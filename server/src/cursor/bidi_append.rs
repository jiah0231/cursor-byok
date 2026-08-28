use prost::Message;

use crate::{
    cursor::interaction,
    cursor::proto::{agent::v1 as agent, aiserver::v1 as ai},
    cursor::{CursorCommand, CursorParent, CursorSessionRegistry},
    Error, Result,
};

pub struct DecodedAppend {
    pub request_id: String,
    pub seqno: i64,
    pub message: agent::AgentClientMessage,
}

impl DecodedAppend {
    pub fn model_id(&self) -> Option<&str> {
        let agent::agent_client_message::Message::RunRequest(request) =
            self.message.message.as_ref()?
        else {
            return None;
        };
        request
            .requested_model
            .as_ref()
            .map(|model| model.model_id.as_str())
            .filter(|model| !model.is_empty())
            .or_else(|| {
                request
                    .model_details
                    .as_ref()
                    .map(|model| model.model_id.as_str())
                    .filter(|model| !model.is_empty())
            })
    }

    pub fn reasoning_effort(&self) -> Option<&str> {
        let agent::agent_client_message::Message::RunRequest(request) =
            self.message.message.as_ref()?
        else {
            return None;
        };
        request
            .requested_model
            .as_ref()?
            .parameters
            .iter()
            .rev()
            .find(|parameter| matches!(parameter.id.as_str(), "effort" | "reasoning"))
            .and_then(|parameter| {
                let effort = parameter.value.trim();
                (!effort.is_empty() && effort != "none").then_some(effort)
            })
    }

    pub fn fast(&self) -> Option<bool> {
        let agent::agent_client_message::Message::RunRequest(request) =
            self.message.message.as_ref()?
        else {
            return None;
        };
        request
            .requested_model
            .as_ref()?
            .parameters
            .iter()
            .rev()
            .find(|parameter| parameter.id == "fast")
            .and_then(|parameter| match parameter.value.trim() {
                "true" => Some(true),
                "false" => Some(false),
                _ => None,
            })
    }

    pub fn conversation_id(&self) -> Option<&str> {
        let agent::agent_client_message::Message::RunRequest(request) =
            self.message.message.as_ref()?
        else {
            return None;
        };
        request.conversation_id.as_deref()
    }

    pub fn is_background_task_completion(&self) -> bool {
        let Some(agent::agent_client_message::Message::RunRequest(request)) =
            self.message.message.as_ref()
        else {
            return false;
        };
        matches!(
            request
                .action
                .as_ref()
                .and_then(|action| action.action.as_ref()),
            Some(agent::conversation_action::Action::BackgroundTaskCompletionAction(_))
        )
    }

    fn is_runtime_cancellation(&self) -> bool {
        matches!(
            self.message.message.as_ref(),
            Some(agent::agent_client_message::Message::ConversationAction(action))
                if matches!(
                    action.action.as_ref(),
                    Some(agent::conversation_action::Action::CancelAction(_))
                        | Some(agent::conversation_action::Action::UserMessageAction(_))
                )
        )
    }

    pub fn trace_metadata(&self) -> serde_json::Value {
        let Some(message) = self.message.message.as_ref() else {
            return serde_json::json!({
                "append_seqno": self.seqno,
                "message_type": "empty",
            });
        };
        let agent::agent_client_message::Message::RunRequest(request) = message else {
            return serde_json::json!({
                "append_seqno": self.seqno,
                "message_type": client_message_type(message),
            });
        };
        let (action_type, history_messages, history_images) = request
            .action
            .as_ref()
            .and_then(|action| action.action.as_ref())
            .map(|action| match action {
                agent::conversation_action::Action::UserMessageAction(action) => {
                    let history = action.conversation_history.as_ref();
                    (
                        "user_message",
                        history.map_or(0, |history| history.messages.len()),
                        history.map_or(0, history_image_count),
                    )
                }
                agent::conversation_action::Action::BackgroundTaskCompletionAction(_) => {
                    ("background_task_completion", 0, 0)
                }
                agent::conversation_action::Action::ExecutePlanAction(_) => ("execute_plan", 0, 0),
                agent::conversation_action::Action::SummarizeAction(_) => ("summarize", 0, 0),
                _ => ("other", 0, 0),
            })
            .unwrap_or(("none", 0, 0));
        let state = request.conversation_state.as_ref();
        serde_json::json!({
            "append_seqno": self.seqno,
            "message_type": "run_request",
            "conversation_id": request.conversation_id,
            "model_id": self.model_id(),
            "reasoning_effort": self.reasoning_effort(),
            "fast": self.fast(),
            "action_type": action_type,
            "conversation_history_messages": history_messages,
            "conversation_history_images": history_images,
            "root_message_count": state.map_or(0, |state| state.root_prompt_messages_json.len()),
            "turn_count": state.map_or(0, |state| state.turns.len()),
            "prefetched_blob_count": request.pre_fetched_blobs.len(),
        })
    }
}

fn client_message_type(message: &agent::agent_client_message::Message) -> &'static str {
    use agent::agent_client_message::Message;
    match message {
        Message::RunRequest(_) => "run_request",
        Message::ExecClientMessage(_) => "exec_client_message",
        Message::ExecClientControlMessage(_) => "exec_client_control_message",
        Message::KvClientMessage(_) => "kv_client_message",
        Message::ConversationAction(_) => "conversation_action",
        Message::InteractionResponse(_) => "interaction_response",
        Message::ClientHeartbeat(_) => "client_heartbeat",
        Message::PrewarmRequest(_) => "prewarm_request",
    }
}

fn history_image_count(history: &agent::ConversationHistory) -> usize {
    use agent::{
        conversation_history_message::Message,
        conversation_history_tool_result_content::Content as ToolContent,
        conversation_history_user_content::Content as UserContent,
    };
    history
        .messages
        .iter()
        .map(|message| match message.message.as_ref() {
            Some(Message::User(user)) => user
                .content
                .iter()
                .filter(|content| matches!(content.content, Some(UserContent::Image(_))))
                .count(),
            Some(Message::Tool(tool)) => tool
                .content
                .iter()
                .filter(|content| matches!(content.content, Some(ToolContent::Image(_))))
                .count(),
            _ => 0,
        })
        .sum()
}

pub fn decode(request: &ai::BidiAppendRequest) -> Result<DecodedAppend> {
    let request_id = request
        .request_id
        .as_ref()
        .map(|id| id.request_id.as_str())
        .filter(|id| !id.is_empty())
        .ok_or_else(|| Error::Protocol("BidiAppend request_id is required".into()))?;
    if !request.data_binary.is_empty() {
        return Err(Error::Protocol(
            "BidiAppend data_binary is not part of the captured protocol".into(),
        ));
    }
    if request.data.is_empty() {
        return Err(Error::Protocol(
            "BidiAppend contains no AgentClientMessage".into(),
        ));
    }
    let payload = hex::decode(&request.data)
        .map_err(|error| Error::Protocol(format!("invalid BidiAppend hex: {error}")))?;
    Ok(DecodedAppend {
        request_id: request_id.into(),
        seqno: request.append_seqno,
        message: agent::AgentClientMessage::decode(payload.as_slice())?,
    })
}

pub async fn append(
    registry: &CursorSessionRegistry,
    request: DecodedAppend,
    parent: Option<CursorParent>,
) -> Result<ai::BidiAppendResponse> {
    if let Some(conversation_id) = request.conversation_id() {
        if request.is_background_task_completion()
            && registry.conversation_cancelled(conversation_id)
        {
            tracing::info!(
                request_id = %request.request_id,
                %conversation_id,
                "dropping background task completion for cancelled conversation"
            );
            return Ok(ai::BidiAppendResponse {});
        }
        if !request.is_background_task_completion() {
            registry.clear_conversation_cancelled(conversation_id);
        }
    }
    let handle = registry.get_or_create(&request.request_id).await?;
    if let Some(conversation_id) = request.conversation_id() {
        handle.set_conversation_id(conversation_id)?;
    }
    if request.is_runtime_cancellation() {
        handle.mark_conversation_cancelled();
        handle.cancel();
    }
    if let Some(parent) = parent {
        handle.set_parent(parent)?;
    }
    if matches!(
        request.message.message.as_ref(),
        Some(agent::agent_client_message::Message::ClientHeartbeat(_))
    ) {
        handle.emit(&interaction::heartbeat())?;
    }
    handle
        .command(CursorCommand::Append {
            seqno: request.seqno,
            message: Box::new(request.message),
        })
        .await?;
    Ok(ai::BidiAppendResponse {})
}

#[cfg(test)]
mod tests {
    use super::*;

    fn encoded(run: agent::AgentRunRequest) -> ai::BidiAppendRequest {
        let message = agent::AgentClientMessage {
            message: Some(agent::agent_client_message::Message::RunRequest(run)),
        };
        ai::BidiAppendRequest {
            data: hex::encode(message.encode_to_vec()),
            request_id: Some(ai::BidiRequestId {
                request_id: "request".into(),
            }),
            append_seqno: 1,
            data_binary: Vec::new(),
        }
    }

    #[test]
    fn route_model_uses_requested_model_id() {
        let decoded = decode(&encoded(agent::AgentRunRequest {
            requested_model: Some(agent::RequestedModel {
                model_id: "33ceed20".into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .unwrap();
        assert_eq!(decoded.model_id(), Some("33ceed20"));
    }

    #[test]
    fn captures_requested_model_options_for_observability() {
        let decoded = decode(&encoded(agent::AgentRunRequest {
            requested_model: Some(agent::RequestedModel {
                model_id: "grok-4.6".into(),
                parameters: vec![
                    agent::requested_model::ModelParameterValue {
                        id: "effort".into(),
                        value: "high".into(),
                    },
                    agent::requested_model::ModelParameterValue {
                        id: "fast".into(),
                        value: "false".into(),
                    },
                ],
                ..Default::default()
            }),
            ..Default::default()
        }))
        .unwrap();

        assert_eq!(decoded.reasoning_effort(), Some("high"));
        assert_eq!(decoded.fast(), Some(false));
        assert_eq!(decoded.trace_metadata()["reasoning_effort"], "high");
        assert_eq!(decoded.trace_metadata()["fast"], false);
    }

    #[test]
    fn route_model_uses_legacy_model_details_when_needed() {
        let decoded = decode(&encoded(agent::AgentRunRequest {
            model_details: Some(agent::ModelDetails {
                model_id: "grok-4.6".into(),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .unwrap();
        assert_eq!(decoded.model_id(), Some("grok-4.6"));
    }

    #[test]
    fn detects_background_task_completion_actions() {
        let decoded = decode(&encoded(agent::AgentRunRequest {
            action: Some(agent::ConversationAction {
                action: Some(
                    agent::conversation_action::Action::BackgroundTaskCompletionAction(
                        agent::BackgroundTaskCompletionAction::default(),
                    ),
                ),
                ..Default::default()
            }),
            ..Default::default()
        }))
        .unwrap();
        assert!(decoded.is_background_task_completion());
    }
}
