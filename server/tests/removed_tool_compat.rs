use std::collections::{BTreeMap, HashSet};

use cursor_server::{
    cursor::tools::{
        runtime::{CursorToolRuntime, ExecContext},
        ToolBatchState, ToolDispatcher,
    },
    model::ToolCall,
};

fn tool(name: &str) -> ToolCall {
    let arguments = serde_json::json!({
        "shell_id": "legacy-shell",
        "block_until_ms": 30_000
    });
    ToolCall {
        index: 0,
        call_id: "call-1".into(),
        model_call_id: "model-call-1".into(),
        name: name.into(),
        arguments_text: arguments.to_string(),
        arguments,
    }
}

async fn dispatch(
    name: &str,
) -> cursor_server::Result<cursor_server::cursor::tools::DispatchedTool> {
    let dispatcher = ToolDispatcher::new(CursorToolRuntime::default());
    let completed = HashSet::new();
    let started = HashSet::new();
    let call = tool(name);
    let dispatched = dispatcher
        .start_batch(
            &[call],
            ToolBatchState {
                completed: &completed,
                started: &started,
                response_text: "",
                response_thinking: "",
            },
            &[],
            &BTreeMap::new(),
            &ExecContext::default(),
        )
        .await?;
    Ok(dispatched.into_iter().next().expect("one dispatched tool"))
}

#[tokio::test]
async fn resumed_await_shell_becomes_a_failed_tool_result_instead_of_a_protocol_error() {
    let dispatched = dispatch("AwaitShell").await.unwrap();

    assert_eq!(
        dispatched.messages.len(),
        1,
        "started card is still published"
    );
    let completion = dispatched.completion.expect("compatibility completion");
    assert!(completion.result().is_error);
    assert!(completion.result().content.contains("older version"));
}

#[tokio::test]
async fn hallucinated_unknown_tool_becomes_a_failed_tool_result_instead_of_a_protocol_error() {
    let dispatched = dispatch("OldTool").await.unwrap();

    assert_eq!(
        dispatched.messages.len(),
        1,
        "started card is still published"
    );
    let completion = dispatched.completion.expect("compatibility completion");
    assert!(completion.result().is_error);
    assert!(completion.result().content.contains("not available"));
}
