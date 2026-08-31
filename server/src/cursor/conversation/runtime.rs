//! Owns the current Run and coordinates the Conversation lifecycle.
use std::sync::Arc;

use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{
    cursor::{
        checkpoint::CheckpointBuilder,
        compile,
        protocol::proto::agent::v1 as pb,
        services::{blob_sync::BlobSynchronizer, context_sync::RequestContextSynchronizer},
        tools::{
            codec,
            runtime::CursorToolRuntime,
            tool_call_result::{tool_result_channel, ToolResultReceiver, ToolResultSender},
            ClientToolEvent, ToolDispatcher,
        },
        transport::{OrderedInbox, TransportHandle},
    },
    run::{CommandResult, RunEngine, RunHandle},
};

use super::{
    CompiledMessages, ConversationDependencies, ConversationOutput, ConversationOutputDependencies,
    ConversationRegistry, MessageDelivery, TransportCommand,
};

pub struct ConversationRuntime;

#[derive(Clone)]
struct RunGeneration {
    superseded: CancellationToken,
    finished: CancellationToken,
    run: Arc<parking_lot::Mutex<Option<RunHandle>>>,
    results: ToolResultSender,
    runtime_actions: mpsc::UnboundedSender<compile::RuntimeAction>,
    tool_runtime: CursorToolRuntime,
    tools: ToolDispatcher,
}

struct FinishGeneration(CancellationToken);

impl Drop for FinishGeneration {
    fn drop(&mut self) {
        self.0.cancel();
    }
}

impl ConversationRuntime {
    pub(crate) fn spawn(
        registry: ConversationRegistry,
        handle: TransportHandle,
        mut receiver: mpsc::Receiver<TransportCommand>,
    ) {
        tokio::spawn(async move {
            let dependencies = registry.dependencies().clone();
            let blob_sync = BlobSynchronizer::new(
                handle.request_id().into(),
                dependencies.store.clone(),
                handle.clone(),
            );
            let mut inbox = OrderedInbox::starting_at(0);
            let tool_runtime_factory = CursorToolRuntime::default();
            let context_sync =
                RequestContextSynchronizer::new(handle.clone(), dependencies.store.clone());
            let mut current = None::<RunGeneration>;
            loop {
                let command = match receiver.recv().await {
                    Some(command) => command,
                    None => {
                        handle.mark_disconnected();
                        if let Some(generation) = current.as_ref() {
                            generation.superseded.cancel();
                            if let Some(run) = generation.run.lock().clone() {
                                run.cancel();
                            }
                        }
                        super::finish_cancelled(&handle).ok();
                        break;
                    }
                };
                match command {
                    TransportCommand::Disconnect => {
                        handle.mark_disconnected();
                        if let Some(generation) = current.as_ref() {
                            generation.superseded.cancel();
                            if let Some(run) = generation.run.lock().clone() {
                                run.cancel();
                            }
                            for id in generation.tool_runtime.drain_running().await {
                                let _ = handle.emit(&codec::abort(id));
                            }
                        }
                        super::finish_cancelled(&handle).ok();
                        break;
                    }
                    TransportCommand::Close => {
                        break;
                    }
                    TransportCommand::Append { seqno, message } => {
                        for (_seqno, message) in inbox.push(seqno, *message) {
                            {
                                match message.message {
                                    Some(pb::agent_client_message::Message::RunRequest(
                                        request,
                                    )) => {
                                        if let Some(conversation_id) =
                                            request.conversation_id.as_deref()
                                        {
                                            if let Err(error) =
                                                handle.set_conversation_id(conversation_id)
                                            {
                                                tracing::error!(
                                                    request_id = handle.request_id(),
                                                    %error,
                                                    "invalid Cursor conversation id"
                                                );
                                                let _ = super::finish_failed(&handle, &error);
                                                let _ =
                                                    handle.command(TransportCommand::Close).await;
                                                return;
                                            }
                                        }
                                        let previous_finished =
                                            if let Some(previous) = current.take() {
                                                previous.superseded.cancel();
                                                if let Some(run) = previous.run.lock().clone() {
                                                    run.cancel();
                                                }
                                                for id in previous
                                                    .tool_runtime
                                                    .interrupt_for_run_replacement()
                                                    .await
                                                {
                                                    let _ = handle.emit(&codec::abort(id));
                                                }
                                                Some(previous.finished.clone())
                                            } else {
                                                None
                                            };
                                        let (results, result_receiver) = tool_result_channel();
                                        let (runtime_actions, runtime_action_receiver) =
                                            mpsc::unbounded_channel::<compile::RuntimeAction>();
                                        let tool_runtime = tool_runtime_factory.next_run();
                                        let tools = ToolDispatcher::with_results(
                                            tool_runtime.clone(),
                                            results.clone(),
                                            dependencies.store.clone(),
                                            dependencies.web_cache.clone(),
                                        );
                                        let generation = RunGeneration {
                                            superseded: CancellationToken::new(),
                                            finished: CancellationToken::new(),
                                            run: Arc::new(parking_lot::Mutex::new(None)),
                                            results,
                                            runtime_actions,
                                            tool_runtime,
                                            tools,
                                        };
                                        current = Some(generation.clone());
                                        spawn_run_request(
                                            registry.clone(),
                                            handle.clone(),
                                            request,
                                            dependencies.clone(),
                                            blob_sync.clone(),
                                            context_sync.clone(),
                                            generation,
                                            previous_finished,
                                            result_receiver,
                                            runtime_action_receiver,
                                        );
                                    }
                                    Some(pb::agent_client_message::Message::ExecClientMessage(
                                        message,
                                    )) => {
                                        if context_sync.handle_client(&message).await {
                                            continue;
                                        }
                                        let Some(generation) = current.as_ref() else {
                                            continue;
                                        };
                                        match codec::client_event(
                                            &message,
                                            &generation.tool_runtime,
                                        )
                                        .await
                                        {
                                            Ok(codec::ClientExecEvent::Delta(message)) => {
                                                let _ = handle.emit(&message);
                                            }
                                            Ok(codec::ClientExecEvent::Message(message)) => {
                                                let _ = handle.emit(&message);
                                            }
                                            Ok(codec::ClientExecEvent::Completed(result)) => {
                                                generation.results.send(*result)
                                            }
                                            Ok(codec::ClientExecEvent::Pending) => {}
                                            Err(error) => generation.results.send_error(error),
                                        }
                                    }
                                    Some(
                                        pb::agent_client_message::Message::ExecClientControlMessage(
                                            message,
                                        ),
                                    ) => {
                                        use pb::exec_client_control_message::Message;
                                        match message.message {
                                            Some(Message::StreamClose(close)) => {
                                                if context_sync.handle_stream_close(close.id).await
                                                {
                                                    continue;
                                                }
                                                let Some(generation) = current.as_ref() else {
                                                    continue;
                                                };
                                                match codec::stream_closed(
                                                    close.id,
                                                    &generation.tool_runtime,
                                                )
                                                .await
                                                {
                                                    Ok(Some(completion)) => {
                                                        generation.results.send(completion)
                                                    }
                                                    Ok(None) => {}
                                                    Err(error) => {
                                                        generation.results.send_error(error)
                                                    }
                                                }
                                            }
                                            Some(Message::Throw(throw)) => {
                                                if context_sync
                                                    .handle_throw(
                                                        throw.id,
                                                        format!(
                                                            "Cursor request context failed: {}",
                                                            throw.error
                                                        ),
                                                    )
                                                    .await
                                                {
                                                    continue;
                                                }
                                                let Some(generation) = current.as_ref() else {
                                                    continue;
                                                };
                                                if generation
                                                    .tool_runtime
                                                    .is_interrupted(throw.id)
                                                    .await
                                                {
                                                    generation
                                                        .tool_runtime
                                                        .discard_exec(throw.id)
                                                        .await;
                                                    continue;
                                                }
                                                match generation
                                                    .tool_runtime
                                                    .take_exec(throw.id)
                                                    .await
                                                {
                                                    Some(pending) => generation.results.send_error(
                                                        crate::Error::Protocol(format!(
                                                            "Exec {} failed: {}",
                                                            pending.call.call_id, throw.error
                                                        )),
                                                    ),
                                                    None => generation.results.send_error(
                                                        crate::Error::Protocol(format!(
                                                            "unknown ExecClientThrow id: {}",
                                                            throw.id
                                                        )),
                                                    ),
                                                }
                                            }
                                            Some(Message::Heartbeat(_)) | None => {}
                                        }
                                    }
                                    Some(
                                        pb::agent_client_message::Message::InteractionResponse(
                                            message,
                                        ),
                                    ) => {
                                        let Some(generation) = current.as_ref() else {
                                            continue;
                                        };
                                        match generation.tools.interaction_response(&message).await
                                        {
                                            Ok(ClientToolEvent::Completed(completion)) => {
                                                generation.results.send(*completion)
                                            }
                                            Ok(ClientToolEvent::Pending) => {}
                                            Err(error) => generation.results.send_error(error),
                                        }
                                    }
                                    Some(pb::agent_client_message::Message::KvClientMessage(
                                        message,
                                    )) => {
                                        let _ = blob_sync.handle_client(message).await;
                                    }
                                    // TODO: ConversationAction has two different delivery paths that
                                    // must not be conflated:
                                    //
                                    // 1. AgentRunRequest.action starts/resumes a Run. compile::prepare
                                    //    consumes UserMessageAction, BackgroundTaskCompletionAction,
                                    //    SummarizeAction, ExecutePlanAction and ResumeAction. Explicit
                                    //    ResumeAction also consumes its request_context and projects a
                                    //    continuation input when no pending tool result exists.
                                    // 2. AgentClientMessage::ConversationAction arrives while a Bidi Run
                                    //    is already active and needs a runtime dispatcher here. Supporting
                                    //    an Action in compile::prepare does not mean this path supports it.
                                    //
                                    // Cursor 3.16 sends a queued follow-up as InjectContextAction.
                                    // It targets expected_run_id and asks the active Run to yield to the
                                    // queued message. The session owns this path because interruption must
                                    // abort active execs and publish a recoverable checkpoint before the
                                    // old Run ends. It must not be reduced to handle.cancel() here.
                                    //
                                    // The remaining unimplemented Action variants are
                                    // ShellCommandAction, StartPlanAction,
                                    // AsyncAskQuestionCompletionAction, BackgroundShellAction,
                                    // BackgroundSubagentAction,
                                    // SubscriptionNotificationAction and GoalContinuationAction.
                                    // Variants whose wire behavior is not captured yet need evidence
                                    // before assigning semantics. Every unsupported runtime Action must
                                    // return an explicit Protocol Error rather than falling through silently.
                                    Some(
                                        pb::agent_client_message::Message::ConversationAction(
                                            action,
                                        ),
                                    ) => match action.action {
                                        Some(
                                            pb::conversation_action::Action::UserMessageAction(
                                                action,
                                            ),
                                        ) => {
                                            let Some(generation) = current.as_ref() else {
                                                continue;
                                            };
                                            if generation
                                                .runtime_actions
                                                .send(compile::RuntimeAction::UserMessage(action))
                                                .is_err()
                                            {
                                                generation.results.send_error(crate::Error::Protocol(
                                                    "UserMessageAction arrived without an active Run"
                                                        .into(),
                                                ));
                                            }
                                        }
                                        Some(pb::conversation_action::Action::CancelAction(_)) => {
                                            if let Some(generation) = current.as_ref() {
                                                if let Some(run) = generation.run.lock().clone() {
                                                    run.cancel();
                                                }
                                                for id in
                                                    generation.tool_runtime.drain_running().await
                                                {
                                                    let _ = handle.emit(&codec::abort(id));
                                                }
                                            }
                                        }
                                        Some(
                                            pb::conversation_action::Action::InjectContextAction(
                                                action,
                                            ),
                                        ) => {
                                            let Some(generation) = current.as_ref() else {
                                                continue;
                                            };
                                            if generation
                                                .runtime_actions
                                                .send(compile::RuntimeAction::Inject(action))
                                                .is_err()
                                            {
                                                generation.results.send_error(crate::Error::Protocol(
                                                    "InjectContextAction arrived without an active Run"
                                                        .into(),
                                                ));
                                            }
                                        }
                                        Some(
                                            pb::conversation_action::Action::CancelSubagentAction(
                                                action,
                                            ),
                                        ) => {
                                            if let Some(generation) = current.as_ref() {
                                                if let Some(id) = generation
                                                    .tool_runtime
                                                    .running_task_exec_id(&action.subagent_id)
                                                    .await
                                                {
                                                    let _ = handle.emit(&codec::abort(id));
                                                }
                                            }
                                        }
                                        Some(action) => {
                                            tracing::warn!(
                                                request_id = handle.request_id(),
                                                action = runtime_action_name(&action),
                                                "ignoring unsupported runtime ConversationAction"
                                            );
                                        }
                                        None => {
                                            if let Some(generation) = current.as_ref() {
                                                generation.results.send_error(
                                                    crate::Error::Protocol(
                                                        "runtime ConversationAction has no action"
                                                            .into(),
                                                    ),
                                                );
                                            }
                                        }
                                    },
                                    _ => {}
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_run_request(
    registry: ConversationRegistry,
    handle: TransportHandle,
    request: pb::AgentRunRequest,
    dependencies: ConversationDependencies,
    blob_sync: BlobSynchronizer,
    context_sync: RequestContextSynchronizer,
    generation: RunGeneration,
    previous_finished: Option<CancellationToken>,
    results: ToolResultReceiver,
    runtime_actions: mpsc::UnboundedReceiver<compile::RuntimeAction>,
) {
    tokio::spawn(async move {
        let _finished = FinishGeneration(generation.finished.clone());
        if let Some(previous_finished) = previous_finished {
            tokio::select! {
                biased;
                _ = generation.superseded.cancelled() => return,
                _ = previous_finished.cancelled() => {}
            }
        }
        if generation.superseded.is_cancelled() {
            return;
        }

        let mut checkpoint = CheckpointBuilder::new(
            dependencies.store.clone(),
            blob_sync.clone(),
            handle.parent().map(|parent| parent.tool_call_id.clone()),
            request.conversation_state.clone(),
        );
        let prepared = tokio::select! {
            biased;
            _ = generation.superseded.cancelled() => return,
            prepared = compile::prepare(
                handle.request_id(),
                &request,
                compile::PrepareDependencies {
                    compiler: &dependencies.compiler,
                    store: &dependencies.store,
                    checkpoint: &checkpoint,
                    blob_sync: &blob_sync,
                    context_sync: &context_sync,
                    local_rules_dir: dependencies.local_rules_dir.as_deref(),
                },
            ) => prepared,
        };
        let (mut prepared, context) = match prepared {
            Ok(prepared) => prepared,
            Err(error) => {
                if generation.superseded.is_cancelled() {
                    return;
                }
                tracing::error!(
                    request_id = handle.request_id(),
                    %error,
                    "failed to prepare Cursor Run"
                );
                let _ = super::finish_failed(&handle, &error);
                let _ = handle.command(TransportCommand::Close).await;
                return;
            }
        };
        checkpoint.configure(
            prepared.model.model_id.clone(),
            prepared.model.context_window_tokens,
            context.checkpoint_prompt.instructions.clone(),
            context.checkpoint_prompt.tools.clone(),
            context.dynamic_tools.keys().cloned().collect(),
            context.turn_user.clone(),
        );
        if context.background_completion {
            let event_id = prepared
                .initial_messages
                .first()
                .and_then(|message| message.runtime_event_id.clone())
                .unwrap_or_else(|| format!("background:{}", prepared.run_id));
            match registry
                .deliver(
                    &prepared.conversation_id,
                    CompiledMessages {
                        event_id,
                        target_run_id: None,
                        messages: prepared.initial_messages.clone(),
                        delivery: MessageDelivery::InsertMessages,
                    },
                )
                .await
            {
                CommandResult::Applied | CommandResult::Duplicate => {
                    if !generation.superseded.is_cancelled() {
                        super::finish_success(&handle);
                        let _ = handle.command(TransportCommand::Close).await;
                    }
                    return;
                }
                CommandResult::RunClosing => {
                    prepared.initial_messages.clear();
                    tokio::select! {
                        biased;
                        _ = generation.superseded.cancelled() => return,
                        _ = registry.wait_until_idle(&prepared.conversation_id) => {}
                    }
                    if let Ok(checkpoint) = dependencies
                        .store
                        .ensure_conversation(&prepared.conversation_id)
                        .await
                    {
                        prepared.base_checkpoint_id = checkpoint;
                    }
                }
                CommandResult::RunEnded => {
                    prepared.initial_messages.clear();
                }
                CommandResult::StaleTarget => {
                    if !generation.superseded.is_cancelled() {
                        super::finish_success(&handle);
                        let _ = handle.command(TransportCommand::Close).await;
                    }
                    return;
                }
            }
        }
        let pending = registry.take_pending(&prepared.conversation_id).await;
        if !pending.is_empty() {
            let mut messages = pending
                .into_iter()
                .flat_map(|pending| pending.messages)
                .collect::<Vec<_>>();
            messages.extend(prepared.initial_messages);
            prepared.initial_messages = messages;
            if let Ok(checkpoint) = dependencies
                .store
                .ensure_conversation(&prepared.conversation_id)
                .await
            {
                prepared.base_checkpoint_id = checkpoint;
            }
        }
        if generation.superseded.is_cancelled() {
            return;
        }

        let run_id = prepared.run_id.clone();
        let conversation_id = prepared.conversation_id.clone();
        let (port, core, run_handle) = crate::run::channel(run_id.clone(), 256);
        *generation.run.lock() = Some(run_handle.clone());
        if generation.superseded.is_cancelled() {
            run_handle.cancel();
            *generation.run.lock() = None;
            return;
        }
        registry
            .activate(conversation_id.clone(), run_id.clone(), run_handle.clone())
            .await;
        let cancellation = run_handle.cancellation();
        let engine = RunEngine::new(dependencies.store.clone(), dependencies.provider.clone());
        let core_run = tokio::spawn(async move { engine.run(prepared, port, cancellation).await });
        let output = ConversationOutput::new(
            handle.clone(),
            dependencies.store.clone(),
            context,
            core,
            run_handle,
            registry.clone(),
            ConversationOutputDependencies {
                superseded: generation.superseded.clone(),
                tools: generation.tools.clone(),
                results,
                runtime_actions,
                compiler: dependencies.compiler.clone(),
                blob_sync,
                checkpoint,
                tool_runtime: generation.tool_runtime.clone(),
            },
        );
        if let Err(error) = output.run().await {
            if !generation.superseded.is_cancelled() {
                tracing::error!(
                    request_id = handle.request_id(),
                    %error,
                    "Cursor session failed"
                );
                let _ = super::finish_failed(&handle, &error);
            }
        }
        let _ = core_run.await;
        registry.release(&conversation_id, &run_id).await;
        if generation
            .run
            .lock()
            .as_ref()
            .is_some_and(|run| run.run_id() == &run_id)
        {
            *generation.run.lock() = None;
        }
        if !generation.superseded.is_cancelled() {
            let _ = handle.command(TransportCommand::Close).await;
        }
    });
}

fn runtime_action_name(action: &pb::conversation_action::Action) -> &'static str {
    use pb::conversation_action::Action;

    match action {
        Action::UserMessageAction(_) => "UserMessageAction",
        Action::ResumeAction(_) => "ResumeAction",
        Action::CancelAction(_) => "CancelAction",
        Action::SummarizeAction(_) => "SummarizeAction",
        Action::ShellCommandAction(_) => "ShellCommandAction",
        Action::StartPlanAction(_) => "StartPlanAction",
        Action::ExecutePlanAction(_) => "ExecutePlanAction",
        Action::AsyncAskQuestionCompletionAction(_) => "AsyncAskQuestionCompletionAction",
        Action::CancelSubagentAction(_) => "CancelSubagentAction",
        Action::BackgroundTaskCompletionAction(_) => "BackgroundTaskCompletionAction",
        Action::BackgroundShellAction(_) => "BackgroundShellAction",
        Action::BackgroundSubagentAction(_) => "BackgroundSubagentAction",
        Action::SubscriptionNotificationAction(_) => "SubscriptionNotificationAction",
        Action::GoalContinuationAction(_) => "GoalContinuationAction",
        Action::InjectContextAction(_) => "InjectContextAction",
    }
}
