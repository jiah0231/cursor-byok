use base64::{engine::general_purpose::STANDARD_NO_PAD, Engine};
use prost::Message;

use crate::{
    cursor::CursorSessionHandle,
    cursor::{
        connect::{
            encode_end_stream, encode_error_end_stream, ConnectCode, ConnectErrorDetail,
            ConnectStreamError,
        },
        proto::aiserver::v1 as ai,
    },
    Error, Result,
};

pub fn finish_success(handle: &CursorSessionHandle) {
    handle.emit_frame(encode_end_stream());
    handle.close_output();
}

pub fn fail(handle: &CursorSessionHandle, error: &Error) -> Result<()> {
    let stream_error = match error {
        Error::Provider(_) | Error::Http(_) => provider_error(error),
        Error::Protocol(message) => plain_message(ConnectCode::InvalidArgument, message.clone()),
        Error::Decode(_) | Error::Json(_) => plain_error(ConnectCode::InvalidArgument, error),
        Error::RunNotFound(_) => plain_error(ConnectCode::NotFound, error),
        Error::Cancelled => plain_error(ConnectCode::Canceled, error),
        Error::Config(_)
        | Error::Store(_)
        | Error::Database(_)
        | Error::Migration(_)
        | Error::Encode(_)
        | Error::Io(_) => plain_error(ConnectCode::Internal, error),
    };
    // Always close the output even if encoding fails, to prevent silent hangs.
    match encode_error_end_stream(&stream_error) {
        Ok(frame) => handle.emit_frame(frame),
        Err(_) => handle.emit_frame(encode_end_stream()),
    }
    handle.close_output();
    Ok(())
}

pub fn cancel(handle: &CursorSessionHandle) -> Result<()> {
    handle.cancel();
    // Always close the output even if encoding fails, to prevent silent hangs.
    match encode_error_end_stream(&ConnectStreamError {
        code: ConnectCode::Canceled,
        message: "run was cancelled".into(),
        details: Vec::new(),
    }) {
        Ok(frame) => handle.emit_frame(frame),
        Err(_) => handle.emit_frame(encode_end_stream()),
    }
    handle.close_output();
    Ok(())
}

fn plain_error(code: ConnectCode, error: &Error) -> ConnectStreamError {
    plain_message(code, error.to_string())
}

fn plain_message(code: ConnectCode, message: String) -> ConnectStreamError {
    ConnectStreamError {
        code,
        message,
        details: Vec::new(),
    }
}

fn provider_error(error: &Error) -> ConnectStreamError {
    let detail = ai::ErrorDetails {
        error: ai::error_details::Error::ProviderError as i32,
        details: Some(ai::CustomErrorDetails {
            title: "Provider Error".into(),
            detail: error.to_string(),
            allow_command_links_potentially_unsafe_please_only_use_for_handwritten_trusted_markdown:
                Some(true),
            is_retryable: Some(true),
            show_request_id: Some(true),
            should_show_immediate_error: Some(false),
        }),
        is_expected: Some(true),
    };
    ConnectStreamError {
        code: ConnectCode::Unavailable,
        message: error.to_string(),
        details: vec![ConnectErrorDetail {
            type_name: "aiserver.v1.ErrorDetails".into(),
            value: STANDARD_NO_PAD.encode(detail.encode_to_vec()),
        }],
    }
}
