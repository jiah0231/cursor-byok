use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct CursorRunTraceSummary {
    pub request_id: String,
    pub conversation_id: Option<String>,
    pub route: String,
    pub model_id: Option<String>,
    pub reasoning_effort: Option<String>,
    pub fast: Option<bool>,
    pub status: String,
    pub request_bytes: i64,
    pub response_bytes: i64,
    pub response_event_count: i64,
    pub http_status: Option<i64>,
    pub received_at_ms: i64,
    pub first_response_at_ms: Option<i64>,
    pub finished_at_ms: Option<i64>,
    pub error_message: Option<String>,
}

#[derive(Clone, Debug)]
pub struct CursorRunTraceArtifact {
    pub seq: i64,
    pub artifact_type: String,
    pub source: String,
    pub metadata: serde_json::Value,
    pub created_at_ms: i64,
    pub data: Vec<u8>,
}
