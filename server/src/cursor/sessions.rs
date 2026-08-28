use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, OnceLock},
};

use bytes::Bytes;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio_util::sync::CancellationToken;

use crate::{
    cursor::prompting::PromptCompiler,
    cursor::{
        blob_sync::BlobSynchronizer, observability::CursorTraceRecorder, proto::agent::v1 as pb,
    },
    provider::Provider,
    run::RunRegistry,
    store::Store,
    Result,
};

use super::{
    actor::{CursorActor, RunDependencies},
    CursorCommand,
};

#[derive(Clone)]
pub struct CursorSessionHandle {
    request_id: String,
    commands: mpsc::Sender<CursorCommand>,
    output: Arc<OutputHub>,
    cancellation: CancellationToken,
    conversation_id: Arc<OnceLock<String>>,
    cancelled_conversations: Arc<parking_lot::Mutex<HashSet<String>>>,
    parent: Arc<OnceLock<CursorParent>>,
    trace: Option<CursorTraceRecorder>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CursorParent {
    pub request_id: String,
    pub tool_call_id: String,
}

impl CursorSessionHandle {
    pub fn request_id(&self) -> &str {
        &self.request_id
    }
    pub fn set_conversation_id(&self, conversation_id: &str) -> Result<()> {
        if conversation_id.is_empty() {
            return Err(crate::Error::Protocol(
                "Cursor conversation id is required".into(),
            ));
        }
        if self
            .conversation_id
            .get()
            .is_some_and(|current| current != conversation_id)
        {
            return Err(crate::Error::Protocol(format!(
                "conflicting conversation ids for request {}",
                self.request_id
            )));
        }
        let _ = self.conversation_id.set(conversation_id.into());
        Ok(())
    }
    pub fn conversation_id(&self) -> Option<&str> {
        self.conversation_id.get().map(String::as_str)
    }
    pub fn mark_conversation_cancelled(&self) {
        if let Some(conversation_id) = self.conversation_id() {
            self.cancelled_conversations
                .lock()
                .insert(conversation_id.to_owned());
        }
    }
    pub fn subscribe(&self) -> mpsc::UnboundedReceiver<Bytes> {
        self.output.subscribe()
    }
    pub async fn command(&self, command: CursorCommand) -> Result<()> {
        self.commands
            .send(command)
            .await
            .map_err(|_| crate::Error::RunNotFound(self.request_id.clone()))
    }
    pub fn emit_frame(&self, frame: Bytes) {
        self.output.emit(frame);
    }
    pub fn emit(&self, message: &pb::AgentServerMessage) -> Result<()> {
        self.emit_frame(crate::cursor::connect::encode_message(message)?);
        Ok(())
    }
    pub fn cancel(&self) {
        self.cancellation.cancel();
    }
    pub fn close_output(&self) {
        self.output.close();
    }
    pub fn cancellation(&self) -> CancellationToken {
        self.cancellation.clone()
    }
    pub fn set_parent(&self, parent: CursorParent) -> Result<()> {
        if parent.request_id.is_empty() || parent.tool_call_id.is_empty() {
            return Err(crate::Error::Protocol(
                "Cursor parent request and tool call ids are required".into(),
            ));
        }
        if self.parent.get().is_some_and(|current| current != &parent) {
            return Err(crate::Error::Protocol(format!(
                "conflicting parent ids for request {}",
                self.request_id
            )));
        }
        let _ = self.parent.set(parent);
        Ok(())
    }
    pub fn parent(&self) -> Option<&CursorParent> {
        self.parent.get()
    }
    pub(crate) fn trace(&self) -> Option<&CursorTraceRecorder> {
        self.trace.as_ref()
    }
}

#[derive(Default)]
struct OutputHub {
    state: parking_lot::Mutex<OutputState>,
    closed: tokio::sync::Notify,
}

#[derive(Default)]
struct OutputState {
    history: Vec<Bytes>,
    subscribers: Vec<mpsc::UnboundedSender<Bytes>>,
    closed: bool,
}

impl OutputHub {
    fn emit(&self, frame: Bytes) {
        let mut state = self.state.lock();
        if state.closed {
            return;
        }
        state.history.push(frame.clone());
        state
            .subscribers
            .retain(|subscriber| subscriber.send(frame.clone()).is_ok());
    }

    fn subscribe(&self) -> mpsc::UnboundedReceiver<Bytes> {
        let (sender, receiver) = mpsc::unbounded_channel();
        let mut state = self.state.lock();
        for frame in &state.history {
            let _ = sender.send(frame.clone());
        }
        if !state.closed {
            state.subscribers.push(sender);
        }
        receiver
    }

    fn close(&self) {
        let mut state = self.state.lock();
        state.closed = true;
        state.subscribers.clear();
        drop(state);
        self.closed.notify_waiters();
    }

    async fn wait_closed(&self) {
        loop {
            let notified = self.closed.notified();
            if self.state.lock().closed {
                return;
            }
            notified.await;
        }
    }
}

#[derive(Clone)]
pub struct CursorSessionRegistry {
    inner: Arc<RegistryInner>,
}

struct RegistryInner {
    runs: Mutex<HashMap<String, CursorSessionHandle>>,
    upstream_runs: Mutex<HashMap<String, u64>>,
    route_changed: Notify,
    run_registry: RunRegistry,
    store: Store,
    provider: Arc<dyn Provider>,
    compiler: PromptCompiler,
    cancelled_conversations: Arc<parking_lot::Mutex<HashSet<String>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CursorRoute {
    Local,
    Upstream(u64),
}

impl CursorSessionRegistry {
    pub fn store(&self) -> &Store {
        &self.inner.store
    }

    pub fn new(
        store: Store,
        provider: Arc<dyn Provider>,
        compiler: PromptCompiler,
        run_registry: RunRegistry,
    ) -> Self {
        Self {
            inner: Arc::new(RegistryInner {
                runs: Mutex::new(HashMap::new()),
                upstream_runs: Mutex::new(HashMap::new()),
                route_changed: Notify::new(),
                run_registry,
                store,
                provider,
                compiler,
                cancelled_conversations: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            }),
        }
    }

    pub async fn get_or_create(&self, request_id: &str) -> Result<CursorSessionHandle> {
        if let Some(handle) = self.inner.runs.lock().await.get(request_id).cloned() {
            return Ok(handle);
        }
        let (commands, receiver) = mpsc::channel(128);
        let output = Arc::new(OutputHub::default());
        let cancellation = CancellationToken::new();
        let trace = CursorTraceRecorder::resume(self.inner.store.clone(), request_id).await;
        let handle = CursorSessionHandle {
            request_id: request_id.into(),
            commands,
            output,
            cancellation,
            conversation_id: Arc::new(OnceLock::new()),
            cancelled_conversations: self.inner.cancelled_conversations.clone(),
            parent: Arc::new(OnceLock::new()),
            trace,
        };
        let mut runs = self.inner.runs.lock().await;
        if let Some(existing) = runs.get(request_id).cloned() {
            return Ok(existing);
        }
        runs.insert(request_id.into(), handle.clone());
        drop(runs);
        self.inner.route_changed.notify_waiters();
        let blob_sync =
            BlobSynchronizer::new(request_id.into(), self.inner.store.clone(), handle.clone());
        CursorActor::spawn(
            handle.clone(),
            receiver,
            RunDependencies {
                store: self.inner.store.clone(),
                provider: self.inner.provider.clone(),
                compiler: self.inner.compiler.clone(),
                run_registry: self.inner.run_registry.clone(),
            },
            blob_sync,
            0,
        );
        let registry = Arc::downgrade(&self.inner);
        let request_id = request_id.to_string();
        let output = handle.output.clone();
        tokio::spawn(async move {
            output.wait_closed().await;
            let Some(registry) = registry.upgrade() else {
                return;
            };
            registry.runs.lock().await.remove(&request_id);
        });
        Ok(handle)
    }

    pub(crate) async fn local(&self, request_id: &str) -> Option<CursorSessionHandle> {
        self.inner.runs.lock().await.get(request_id).cloned()
    }

    pub(crate) async fn mark_upstream(&self, request_id: &str) {
        let mut runs = self.inner.upstream_runs.lock().await;
        let generation = runs.get(request_id).copied().unwrap_or_default() + 1;
        runs.insert(request_id.into(), generation);
        drop(runs);
        self.inner.route_changed.notify_waiters();
    }

    pub(crate) async fn upstream(&self, request_id: &str) -> bool {
        self.inner
            .upstream_runs
            .lock()
            .await
            .contains_key(request_id)
    }

    pub(crate) fn conversation_cancelled(&self, conversation_id: &str) -> bool {
        self.inner
            .cancelled_conversations
            .lock()
            .contains(conversation_id)
    }

    pub(crate) fn clear_conversation_cancelled(&self, conversation_id: &str) {
        self.inner
            .cancelled_conversations
            .lock()
            .remove(conversation_id);
    }

    pub(crate) async fn wait_route(&self, request_id: &str) -> CursorRoute {
        loop {
            // Create the notification future BEFORE checking state to avoid
            // a race where a notification fires between state check and await.
            let changed = self.inner.route_changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.inner.runs.lock().await.contains_key(request_id) {
                return CursorRoute::Local;
            }
            if let Some(generation) = self
                .inner
                .upstream_runs
                .lock()
                .await
                .get(request_id)
                .copied()
            {
                return CursorRoute::Upstream(generation);
            }
            changed.await;
        }
    }

    pub(crate) fn finish_upstream(&self, request_id: String, generation: u64) {
        let registry = self.clone();
        tokio::spawn(async move {
            let mut runs = registry.inner.upstream_runs.lock().await;
            if runs.get(&request_id) == Some(&generation) {
                runs.remove(&request_id);
            }
        });
    }

    pub async fn shutdown(&self) {
        let handles = {
            let mut runs = self.inner.runs.lock().await;
            runs.drain().map(|(_, handle)| handle).collect::<Vec<_>>()
        };
        self.inner.run_registry.shutdown().await;
        self.inner.upstream_runs.lock().await.clear();
        for handle in handles {
            handle.cancel();
            let _ = crate::cursor::lifecycle::cancel(&handle);
        }
    }
}
