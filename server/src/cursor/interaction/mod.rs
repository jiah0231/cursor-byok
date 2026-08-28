mod query;
mod render;

use std::{collections::BTreeMap, time::Duration};

use crate::{
    cursor::{proto::agent::v1 as pb, tools::compat},
    model::{ToolCall, Usage},
    provider::ModelEvent,
    Error, Result,
};

pub use query::tool_query;
pub(crate) use render::{create_plan_partial, edit_content_delta, edit_path_partial};
pub use render::{dynamic_mcp_placeholder, render_dynamic_mcp, tool_completed};
use render::{
    render_tool_call as render_builtin_tool_call, tool_placeholder as builtin_tool_placeholder,
    tool_started as builtin_tool_started,
};

pub fn tool_placeholder(name: &str, call_id: &str) -> Result<pb::ToolCall> {
    match builtin_tool_placeholder(name, call_id) {
        Ok(tool) => Ok(tool),
        Err(error) if is_unsupported_tool(&error, name) => Ok(compat::placeholder(name, call_id)),
        Err(error) => Err(error),
    }
}

pub fn render_tool_call(call: &ToolCall, completed: bool) -> Result<pb::ToolCall> {
    match render_builtin_tool_call(call, completed) {
        Ok(tool) => Ok(tool),
        Err(error) if is_unsupported_tool(&error, &call.name) => {
            Ok(compat::render(call, completed))
        }
        Err(error) => Err(error),
    }
}

pub fn tool_started(
    call: &ToolCall,
    dynamic_mcp: Option<&pb::McpToolDefinition>,
) -> Result<pb::AgentServerMessage> {
    match builtin_tool_started(call, dynamic_mcp) {
        Ok(message) => Ok(message),
        Err(error) if dynamic_mcp.is_none() && is_unsupported_tool(&error, &call.name) => {
            Ok(server_interaction(
                pb::interaction_update::Message::ToolCallStarted(pb::ToolCallStartedUpdate {
                    call_id: call.call_id.clone(),
                    tool_call: Some(compat::render(call, false)),
                    model_call_id: call.model_call_id.clone(),
                }),
            ))
        }
        Err(error) => Err(error),
    }
}

fn is_unsupported_tool(error: &Error, name: &str) -> bool {
    matches!(error, Error::Protocol(message) if message == &format!("unsupported tool: {name}"))
}

pub fn response_event(
    event: &ModelEvent,
    model_call_id: &str,
    dynamic_mcp: &BTreeMap<String, pb::McpToolDefinition>,
) -> Result<Option<pb::AgentServerMessage>> {
    use pb::interaction_update::Message;
    let message = match event {
        ModelEvent::TextDelta(text) => Message::TextDelta(pb::TextDeltaUpdate {
            text: text.clone(),
            is_server_notice: false,
        }),
        ModelEvent::ThinkingDelta(text) => Message::ThinkingDelta(pb::ThinkingDeltaUpdate {
            text: text.clone(),
            thinking_style: Some(pb::ThinkingStyle::Default as i32),
        }),
        ModelEvent::ToolCallStart { call_id, name, .. } => {
            Message::PartialToolCall(pb::PartialToolCallUpdate {
                call_id: call_id.clone(),
                tool_call: Some(match dynamic_mcp.get(name) {
                    Some(definition) => dynamic_mcp_placeholder(definition, call_id),
                    None => tool_placeholder(name, call_id)?,
                }),
                args_text_delta: String::new(),
                model_call_id: model_call_id.into(),
            })
        }
        ModelEvent::ToolCallArgumentsDelta { .. } => return Ok(None),
        ModelEvent::ToolCallEnd { .. }
        | ModelEvent::Start { .. }
        | ModelEvent::TextStart
        | ModelEvent::TextEnd
        | ModelEvent::ThinkingStart
        | ModelEvent::ThinkingEnd
        | ModelEvent::ProviderReplayState(_)
        | ModelEvent::Usage(_)
        | ModelEvent::Done(_) => return Ok(None),
    };
    Ok(Some(server_interaction(message)))
}

pub fn thinking_completed(elapsed: Duration) -> pb::AgentServerMessage {
    let milliseconds = elapsed.as_millis().clamp(1, i32::MAX as u128) as i32;
    server_interaction(pb::interaction_update::Message::ThinkingCompleted(
        pb::ThinkingCompletedUpdate {
            thinking_duration_ms: milliseconds,
        },
    ))
}

pub fn heartbeat() -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::Heartbeat(
        pb::HeartbeatUpdate {},
    ))
}

pub fn arguments_delta(call: &ToolCall, delta: &str) -> Result<pb::AgentServerMessage> {
    Ok(server_interaction(
        pb::interaction_update::Message::PartialToolCall(pb::PartialToolCallUpdate {
            call_id: call.call_id.clone(),
            tool_call: Some(tool_placeholder(&call.name, &call.call_id)?),
            args_text_delta: delta.into(),
            model_call_id: call.model_call_id.clone(),
        }),
    ))
}

pub fn dynamic_mcp_arguments_delta(
    call: &ToolCall,
    delta: &str,
    definition: &pb::McpToolDefinition,
) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::PartialToolCall(
        pb::PartialToolCallUpdate {
            call_id: call.call_id.clone(),
            tool_call: Some(dynamic_mcp_placeholder(definition, &call.call_id)),
            args_text_delta: delta.into(),
            model_call_id: call.model_call_id.clone(),
        },
    ))
}

pub fn turn_ended(usage: Option<Usage>) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::TurnEnded(
        pb::TurnEndedUpdate {
            input_tokens: usage.and_then(|usage| usage.input_tokens.map(|value| value as i64)),
            output_tokens: usage.and_then(|usage| usage.output_tokens.map(|value| value as i64)),
            cache_read_tokens: usage
                .and_then(|usage| usage.cache_read_tokens.map(|value| value as i64)),
            cache_write_tokens: usage
                .and_then(|usage| usage.cache_write_tokens.map(|value| value as i64)),
            reasoning_tokens: usage
                .and_then(|usage| usage.reasoning_tokens.map(|value| value as i64)),
        },
    ))
}

pub fn token_delta(tokens: u64) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::TokenDelta(
        pb::TokenDeltaUpdate {
            tokens: tokens.min(i32::MAX as u64) as i32,
        },
    ))
}

pub fn summary_started() -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::SummaryStarted(
        pb::SummaryStartedUpdate {},
    ))
}

pub fn summary_delta(summary: String) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::Summary(
        pb::SummaryUpdate { summary },
    ))
}

pub fn summary_completed() -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::SummaryCompleted(
        pb::SummaryCompletedUpdate { hook_message: None },
    ))
}

pub fn context_injection_queued(injection_id: String) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::ContextInjectionState(
        pb::ContextInjectionStateUpdate {
            injection_id,
            state: Some(pb::ContextInjectionState {
                state: Some(pb::context_injection_state::State::Queued(
                    pb::ContextInjectionQueued {},
                )),
            }),
        },
    ))
}

pub fn context_injection_rejected(injection_id: String, reason: String) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::ContextInjectionState(
        pb::ContextInjectionStateUpdate {
            injection_id,
            state: Some(pb::ContextInjectionState {
                state: Some(pb::context_injection_state::State::Rejected(
                    pb::ContextInjectionRejected { reason },
                )),
            }),
        },
    ))
}

pub fn context_injection_delivered(
    injection_id: String,
    delivery_batch_id: String,
    delivered_at_ms: i64,
) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::ContextInjectionState(
        pb::ContextInjectionStateUpdate {
            injection_id,
            state: Some(pb::ContextInjectionState {
                state: Some(pb::context_injection_state::State::Delivered(
                    pb::ContextInjectionDelivered {
                        step: 0,
                        delivery_batch_id,
                        delivered_at_ms,
                    },
                )),
            }),
        },
    ))
}

pub fn user_message_appended(user_message: pb::UserMessage) -> pb::AgentServerMessage {
    server_interaction(pb::interaction_update::Message::UserMessageAppended(
        pb::UserMessageAppendedUpdate {
            user_message: Some(user_message),
        },
    ))
}

pub fn server_interaction(message: pb::interaction_update::Message) -> pb::AgentServerMessage {
    pb::AgentServerMessage {
        ttft_breakdown: None,
        message: Some(pb::agent_server_message::Message::InteractionUpdate(
            pb::InteractionUpdate {
                message: Some(message),
            },
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unknown_tool(name: &str) -> ToolCall {
        let arguments = serde_json::json!({"shell_id": "legacy-shell", "value": 1});
        ToolCall {
            index: 0,
            call_id: "call-1".into(),
            model_call_id: "model-call-1".into(),
            name: name.into(),
            arguments_text: arguments.to_string(),
            arguments,
        }
    }

    #[test]
    fn retired_tool_streaming_uses_a_compatibility_card() {
        let call = unknown_tool("AwaitShell");

        assert!(tool_placeholder(&call.name, &call.call_id).is_ok());
        assert!(render_tool_call(&call, false).is_ok());
        assert!(tool_started(&call, None).is_ok());
        assert!(arguments_delta(&call, "{\"shell_id\":").is_ok());
    }

    #[test]
    fn arbitrary_unknown_tool_start_does_not_fail_the_agent_stream() {
        let event = ModelEvent::ToolCallStart {
            index: 0,
            call_id: "call-1".into(),
            name: "OldTool".into(),
        };

        assert!(response_event(&event, "model-call-1", &BTreeMap::new())
            .unwrap()
            .is_some());
    }
}
