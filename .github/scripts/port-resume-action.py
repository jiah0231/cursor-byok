from pathlib import Path


def replace_once(path: str, old: str, new: str) -> None:
    p = Path(path)
    text = p.read_text(encoding="utf-8")
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"expected exactly one match in {path}, got {count}")
    p.write_text(text.replace(old, new, 1), encoding="utf-8")


# ResumeAction carries a fresh request_context and must participate in hydration.
replace_once(
    "server/src/cursor/compile/context.rs",
    """            pb::conversation_action::Action::ExecutePlanAction(action) => {
                action.request_context.as_ref()
            }
            _ => None,""",
    """            pb::conversation_action::Action::ExecutePlanAction(action) => {
                action.request_context.as_ref()
            }
            pb::conversation_action::Action::ResumeAction(action) => {
                action.request_context.as_ref()
            }
            _ => None,""",
)

# Explicit ResumeAction without a pending tool result needs a model-visible continuation input.
replace_once(
    "server/src/cursor/compile/break_messages.rs",
    """use super::{context, images};

pub(crate) enum RuntimeAction {""",
    """use super::{context, images};

const RESUME_PROMPT: &str = "<resume>\\nContinue the unfinished task from the current conversation state. Use the latest workspace context, make concrete progress toward the user's request, and do not repeat work that is already complete.\\n</resume>";

pub(crate) enum RuntimeAction {""",
)
replace_once(
    "server/src/cursor/compile/break_messages.rs",
    """    Ok(should_project_request_context(history, &message).then_some(message))
}

fn should_project_request_context(""",
    """    Ok(should_project_request_context(history, &message).then_some(message))
}

pub(super) fn compile_resume_messages(
    request_id: &str,
    request_context: &pb::RequestContext,
    history: &[CanonicalMessage],
    has_pending_tool_round: bool,
) -> Result<Vec<CanonicalMessage>> {
    let event_id = format!("resume:{request_id}");
    let mut messages = compile_request_context(&event_id, request_context, history)?
        .into_iter()
        .collect::<Vec<_>>();
    if !has_pending_tool_round {
        messages.push(CanonicalMessage::text(
            format!("resume-prompt:{request_id}"),
            Role::User,
            Origin::Prompt,
            RESUME_PROMPT,
        ));
    }
    Ok(messages)
}

fn should_project_request_context(""",
)

# Decode the pending tool round before projecting ResumeAction messages.
replace_once(
    "server/src/cursor/compile/run.rs",
    """    let request_context = request_context;
    let ActionProjection {""",
    """    let request_context = request_context;
    let explicit_resume = matches!(
        request
            .action
            .as_ref()
            .and_then(|action| action.action.as_ref()),
        Some(pb::conversation_action::Action::ResumeAction(_))
    );
    let ActionProjection {""",
)
replace_once(
    "server/src/cursor/compile/run.rs",
    """        background_completion,
    } = action(request)?;
    let checkpoint_mode = if request.subagent_type_name.is_some() {""",
    """        background_completion,
    } = action(request)?;
    let pending_tool_round = if !starts_turn && !compacting {
        match request
            .conversation_state
            .as_ref()
            .map(|state| state.pending_tool_calls.as_slice())
            .unwrap_or_default()
        {
            [] => None,
            [pending] => Some(messages::decode_pending(pending)?),
            pending => {
                return Err(Error::Protocol(format!(
                    "Cursor resume contains {} pending assistant messages",
                    pending.len()
                )))
            }
        }
    } else {
        None
    };
    let checkpoint_mode = if request.subagent_type_name.is_some() {""",
)
replace_once(
    "server/src/cursor/compile/run.rs",
    """    let (base_checkpoint_id, reused) = store
        .match_checkpoint_prefix(&conversation_id, base_checkpoint_id, &initial_messages)
        .await?;""",
    """    if explicit_resume {
        initial_messages.extend(break_messages::compile_resume_messages(
            request_id,
            &request_context,
            base_messages.as_deref().unwrap_or_default(),
            pending_tool_round.is_some(),
        )?);
    }
    let (base_checkpoint_id, reused) = store
        .match_checkpoint_prefix(&conversation_id, base_checkpoint_id, &initial_messages)
        .await?;""",
)
replace_once(
    "server/src/cursor/compile/run.rs",
    """    let action = if compacting {
        RunAction::Compact
    } else if starts_turn {
        RunAction::Start
    } else {
        let pending_tool_round = match request
            .conversation_state
            .as_ref()
            .map(|state| state.pending_tool_calls.as_slice())
            .unwrap_or_default()
        {
            [] => None,
            [pending] => Some(messages::decode_pending(pending)?),
            pending => {
                return Err(Error::Protocol(format!(
                    "Cursor resume contains {} pending assistant messages",
                    pending.len()
                )))
            }
        };
        RunAction::Resume { pending_tool_round }
    };""",
    """    let action = if compacting {
        RunAction::Compact
    } else if starts_turn {
        RunAction::Start
    } else {
        RunAction::Resume { pending_tool_round }
    };""",
)

# Keep the architecture comment accurate.
replace_once(
    "server/src/cursor/conversation/runtime.rs",
    """                                    // 1. AgentRunRequest.action starts/resumes a Run. compile::prepare
                                    //    currently consumes UserMessageAction,
                                    //    BackgroundTaskCompletionAction, SummarizeAction and
                                    //    ExecutePlanAction. ResumeAction only works indirectly through
                                    //    the absence of a new runtime event and still needs an explicit
                                    //    implementation that consumes ResumeAction.request_context.""",
    """                                    // 1. AgentRunRequest.action starts/resumes a Run. compile::prepare
                                    //    consumes UserMessageAction, BackgroundTaskCompletionAction,
                                    //    SummarizeAction, ExecutePlanAction and ResumeAction. Explicit
                                    //    ResumeAction also consumes its request_context and projects a
                                    //    continuation input when no pending tool result exists.""",
)

# End-to-end regression adapted to the new Transport/Conversation architecture.
Path("server/tests/resume_progress.rs").write_text(
    r'''#[path = "support/fake_provider.rs"]
mod fake_provider;
#[path = "support/fixtures.rs"]
mod fixtures;

use std::sync::Arc;

use cursor_server::{
    cursor::prompting::{PromptAssets, PromptCompiler},
    cursor::protocol::{connect, proto::agent::v1 as pb},
    cursor::{TransportCommand, TransportHandle, TransportRegistry},
    model::{ContentPart, ProjectedContent, Role},
    provider::{FinishReason, ModelEvent},
};
use prost::Message;

#[tokio::test]
async fn resume_action_context_and_continuation_reach_the_next_model_call() {
    let (_directory, store) = fixtures::temp_store().await;
    let provider = fake_provider::FakeProvider::default();
    provider.push(text_response("model-initial", "initial response"));
    provider.push(text_response("model-resumed", "continued response"));
    let assets = PromptAssets::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("prompt/cursor")
            .as_path(),
    )
    .unwrap();
    let registry = TransportRegistry::new(
        store,
        Arc::new(provider.clone()),
        PromptCompiler::new(assets),
    );

    let first = registry.get_or_create("initial-request").await.unwrap();
    let mut first_output = first.subscribe();
    first
        .command(TransportCommand::Append {
            seqno: 0,
            message: Box::new(start_request()),
        })
        .await
        .unwrap();
    let state = drive_to_end(&first, &mut first_output, 1).await;
    assert!(state.pending_tool_calls.is_empty());

    let resumed = registry.get_or_create("resume-request").await.unwrap();
    let mut resumed_output = resumed.subscribe();
    resumed
        .command(TransportCommand::Append {
            seqno: 0,
            message: Box::new(resume_request(state)),
        })
        .await
        .unwrap();
    let _ = drive_to_end(&resumed, &mut resumed_output, 1).await;

    let requests = provider.requests();
    assert_eq!(requests.len(), 2);
    let history = &requests[1].history;
    assert_eq!(
        history
            .iter()
            .filter(|message| message.message_id.starts_with("request-context:"))
            .count(),
        2,
        "the changed ResumeAction context must be appended to history"
    );
    let texts = history.iter().filter_map(projected_text).collect::<Vec<_>>();
    assert!(texts.iter().any(|text| text.contains("Shell: pwsh")));
    let last = history.last().expect("resume history must not be empty");
    assert_eq!(last.role, Role::User);
    assert_eq!(last.message_id, "resume-prompt:resume-request");
    assert!(projected_text(last).as_deref().is_some_and(|text| {
        text.contains("<resume>") && text.contains("make concrete progress")
    }));
}

fn text_response(model_call_id: &str, text: &str) -> Vec<ModelEvent> {
    vec![
        ModelEvent::Start {
            model_call_id: model_call_id.into(),
        },
        ModelEvent::TextStart,
        ModelEvent::TextDelta(text.into()),
        ModelEvent::TextEnd,
        ModelEvent::Done(FinishReason::Stop),
    ]
}

fn start_request() -> pb::AgentClientMessage {
    pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::RunRequest(
            pb::AgentRunRequest {
                action: Some(pb::ConversationAction {
                    action: Some(pb::conversation_action::Action::UserMessageAction(
                        pb::UserMessageAction {
                            user_message: Some(pb::UserMessage {
                                text: "perform the task".into(),
                                message_id: "user-1".into(),
                                mode: pb::AgentMode::Agent as i32,
                                ..Default::default()
                            }),
                            request_context: Some(request_context("bash")),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                conversation_id: Some("resume-progress-conversation".into()),
                run_id: Some("initial-wire-run".into()),
                requested_model: Some(pb::RequestedModel {
                    model_id: "test-model".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

fn resume_request(state: pb::ConversationStateStructure) -> pb::AgentClientMessage {
    pb::AgentClientMessage {
        message: Some(pb::agent_client_message::Message::RunRequest(
            pb::AgentRunRequest {
                action: Some(pb::ConversationAction {
                    action: Some(pb::conversation_action::Action::ResumeAction(
                        pb::ResumeAction {
                            request_context: Some(request_context("pwsh")),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                }),
                conversation_state: Some(state),
                conversation_id: Some("resume-progress-conversation".into()),
                run_id: Some("resume-wire-run".into()),
                requested_model: Some(pb::RequestedModel {
                    model_id: "test-model".into(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        )),
    }
}

fn request_context(shell: &str) -> pb::RequestContext {
    pb::RequestContext {
        env: Some(pb::RequestContextEnv {
            os_version: "windows".into(),
            workspace_paths: vec!["C:/workspace".into()],
            shell: shell.into(),
            terminals_folder: "C:/terminals".into(),
            agent_transcripts_folder: "C:/transcripts".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

fn projected_text(message: &cursor_server::model::ProjectedMessage) -> Option<String> {
    let ProjectedContent::Parts(parts) = &message.content else {
        return None;
    };
    Some(
        parts
            .iter()
            .filter_map(|part| match part {
                ContentPart::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

async fn drive_to_end(
    handle: &TransportHandle,
    output: &mut tokio::sync::mpsc::UnboundedReceiver<bytes::Bytes>,
    mut seqno: i64,
) -> pb::ConversationStateStructure {
    let mut latest = None;
    loop {
        let frame = tokio::time::timeout(std::time::Duration::from_secs(10), output.recv())
            .await
            .unwrap()
            .unwrap();
        let (flags, payload) = connect::decode_frames(&frame).unwrap().pop().unwrap();
        if flags & connect::END_STREAM_FLAG != 0 {
            break;
        }
        let server = pb::AgentServerMessage::decode(payload).unwrap();
        match server.message {
            Some(pb::agent_server_message::Message::KvServerMessage(kv)) => {
                assert!(matches!(
                    kv.message,
                    Some(pb::kv_server_message::Message::SetBlobArgs(_))
                ));
                handle
                    .command(TransportCommand::Append {
                        seqno,
                        message: Box::new(pb::AgentClientMessage {
                            message: Some(pb::agent_client_message::Message::KvClientMessage(
                                pb::KvClientMessage {
                                    id: kv.id,
                                    message: Some(pb::kv_client_message::Message::SetBlobResult(
                                        pb::SetBlobResult { error: None },
                                    )),
                                },
                            )),
                        }),
                    })
                    .await
                    .unwrap();
                seqno += 1;
            }
            Some(pb::agent_server_message::Message::ConversationCheckpointUpdate(state)) => {
                latest = Some(state)
            }
            _ => {}
        }
    }
    latest.expect("run must publish a checkpoint")
}
''',
    encoding="utf-8",
)
