use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use tokio::sync::Mutex;

use crate::store::{BlobId, BufferedCursorTraceChunk, Store};

#[derive(Clone)]
pub struct CursorTraceRecorder {
    store: Store,
    request_id: String,
    chunks: Arc<Mutex<TraceChunkBuffer>>,
    finished: Arc<AtomicBool>,
}

#[derive(Default)]
struct TraceChunkBuffer {
    chunks: Vec<BufferedCursorTraceChunk>,
    bytes: usize,
    first_chunk_at: Option<Instant>,
    generation: u64,
}

const MAX_BUFFERED_CHUNKS: usize = 32;
const MAX_BUFFERED_BYTES: usize = 256 * 1024;
const MAX_BUFFER_AGE: Duration = Duration::from_millis(50);

impl CursorTraceRecorder {
    pub async fn begin(
        store: Store,
        request_id: &str,
        conversation_id: Option<&str>,
        route: &str,
        model_id: Option<&str>,
        reasoning_effort: Option<&str>,
        fast: Option<bool>,
    ) -> Option<Self> {
        match store
            .start_cursor_trace_if_detailed(
                request_id,
                conversation_id,
                route,
                model_id,
                reasoning_effort,
                fast,
            )
            .await
        {
            Ok(true) => Some(Self {
                store,
                request_id: request_id.into(),
                chunks: Arc::new(Mutex::new(TraceChunkBuffer::default())),
                finished: Arc::new(AtomicBool::new(false)),
            }),
            Ok(false) => None,
            Err(error) => {
                tracing::warn!(request_id, %error, "failed to start Cursor trace");
                None
            }
        }
    }

    pub async fn resume(store: Store, request_id: &str) -> Option<Self> {
        match store.cursor_trace_exists(request_id).await {
            Ok(true) => Some(Self {
                store,
                request_id: request_id.into(),
                chunks: Arc::new(Mutex::new(TraceChunkBuffer::default())),
                finished: Arc::new(AtomicBool::new(false)),
            }),
            Ok(false) => None,
            Err(error) => {
                tracing::warn!(request_id, %error, "failed to resume Cursor trace");
                None
            }
        }
    }

    pub fn request_id(&self) -> &str {
        &self.request_id
    }

    pub async fn request(&self, artifact_type: &str, data: &[u8], metadata: serde_json::Value) {
        if let Err(error) = self
            .store
            .append_cursor_trace_artifact(
                &self.request_id,
                artifact_type,
                "cursor_client",
                data,
                &metadata,
            )
            .await
        {
            tracing::warn!(request_id = self.request_id, %error, "failed to record Cursor request artifact");
            return;
        }
        if let Err(error) = self
            .store
            .add_cursor_trace_request_bytes(&self.request_id, data.len())
            .await
        {
            tracing::warn!(request_id = self.request_id, %error, "failed to update Cursor request trace size");
        }
    }

    pub async fn artifact(
        &self,
        artifact_type: &str,
        source: &str,
        data: &[u8],
        metadata: serde_json::Value,
    ) {
        if let Err(error) = self
            .store
            .append_cursor_trace_artifact(&self.request_id, artifact_type, source, data, &metadata)
            .await
        {
            tracing::warn!(request_id = self.request_id, %error, artifact_type, "failed to record Cursor trace artifact");
        }
    }

    pub async fn linked_blob(
        &self,
        artifact_type: &str,
        source: &str,
        blob_id: &BlobId,
        metadata: serde_json::Value,
    ) {
        if let Err(error) = self
            .store
            .link_cursor_trace_artifact(&self.request_id, artifact_type, source, blob_id, &metadata)
            .await
        {
            tracing::warn!(request_id = self.request_id, %error, artifact_type, "failed to link Cursor trace Blob");
        }
    }

    pub async fn response_started(&self, status: u16) {
        if let Err(error) = self
            .store
            .start_cursor_trace_response(&self.request_id, status)
            .await
        {
            tracing::warn!(request_id = self.request_id, %error, "failed to start Cursor response trace");
        }
    }

    pub async fn response_chunk(&self, source: &str, data: &[u8]) {
        let mut buffer = self.chunks.lock().await;
        if self.finished.load(Ordering::Acquire) {
            return;
        }
        let schedule_flush = if buffer.chunks.is_empty() {
            buffer.generation = buffer.generation.wrapping_add(1);
            buffer.first_chunk_at = Some(Instant::now());
            Some(buffer.generation)
        } else {
            None
        };
        buffer.bytes += data.len();
        buffer
            .chunks
            .push(BufferedCursorTraceChunk::new(source, data));
        let expired = buffer
            .first_chunk_at
            .is_some_and(|started| started.elapsed() >= MAX_BUFFER_AGE);
        if buffer.chunks.len() >= MAX_BUFFERED_CHUNKS
            || buffer.bytes >= MAX_BUFFERED_BYTES
            || expired
        {
            if let Err(error) = self.flush_locked(&mut buffer).await {
                tracing::warn!(request_id = self.request_id, %error, "failed to record Cursor response chunk");
            }
        }
        drop(buffer);
        if let Some(generation) = schedule_flush {
            let recorder = self.clone();
            tokio::spawn(async move {
                tokio::time::sleep(MAX_BUFFER_AGE).await;
                let mut buffer = recorder.chunks.lock().await;
                if buffer.generation == generation {
                    if let Err(error) = recorder.flush_locked(&mut buffer).await {
                        tracing::warn!(request_id = recorder.request_id, %error, "failed to flush Cursor response chunks");
                    }
                }
            });
        }
    }

    pub async fn finish(&self, error: Option<&str>) {
        if self.finished.swap(true, Ordering::AcqRel) {
            return;
        }
        let mut buffer = self.chunks.lock().await;
        if let Err(store_error) = self.flush_locked(&mut buffer).await {
            tracing::warn!(request_id = self.request_id, %store_error, "failed to flush Cursor response chunks");
        }
        drop(buffer);
        if let Err(store_error) = self
            .store
            .finish_cursor_trace(&self.request_id, error)
            .await
        {
            tracing::warn!(request_id = self.request_id, %store_error, "failed to finish Cursor trace");
        }
    }

    async fn flush_locked(&self, buffer: &mut TraceChunkBuffer) -> crate::Result<()> {
        if buffer.chunks.is_empty() {
            return Ok(());
        }
        let chunks = std::mem::take(&mut buffer.chunks);
        buffer.bytes = 0;
        buffer.first_chunk_at = None;
        if let Err(error) = self
            .store
            .add_cursor_trace_response_chunks(&self.request_id, &chunks)
            .await
        {
            buffer.bytes = chunks.iter().map(|chunk| chunk.data.len()).sum();
            buffer.first_chunk_at = Some(Instant::now());
            buffer.chunks = chunks;
            return Err(error);
        }
        Ok(())
    }
}
