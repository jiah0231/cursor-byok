from pathlib import Path
import yaml

# Start from the established patch that already compiled in CI.
wf = yaml.safe_load(Path('.github/workflows/patch-compaction-runtime.yml').read_text())
steps = wf['jobs']['patch-test-commit']['steps']
exec(compile(next(x['run'] for x in steps if x.get('name') == 'Patch runtime compaction'), '<patch>', 'exec'), {})
exec(compile(next(x['run'] for x in steps if x.get('name') == 'Add Resume compaction regression test'), '<test>', 'exec'), {})

p = Path('server/src/run/engine.rs')
s = p.read_text()

# Make the timeout match exhaustive (the timeout branch normally has None, but Rust
# correctly requires us to handle all representable states).
marker = '(false, true, None) => {'
assert marker in s
before, after = s.split(marker, 1)
usage_marker = '(fallback_summary(&compactable), None)'
assert usage_marker in after
after = after.replace(
    usage_marker,
    '(fallback_summary(&compactable), match timed_out_cycle { Some(Ok(cycle)) => cycle.usage, Some(Err(failure)) => failure.usage, None => None })',
    1,
)
s = before + '(false, true, timed_out_cycle) => {' + after

# Bound real user-visible summary latency and avoid needlessly generating 4K summary tokens.
s = s.replace('Duration::from_secs(45)', 'Duration::from_secs(30)', 1)
s = s.replace('const COMPACTION_OUTPUT_TOKENS: u64 = 4_096;', 'const COMPACTION_OUTPUT_TOKENS: u64 = 2_048;', 1)

# Resume used to be excluded entirely from auto compaction. The base patch enables it.
# However, the first Resume boundary can inherit a completed usage record from the
# pre-compaction run. Use direct estimation until this Resume completes one ordinary
# provider call; after that the latest provider usage belongs to the current run and is
# safe to use for later tool/model loops.
s = s.replace(
    '        let mut last_auto_compaction_revision = None;\n',
    '        let mut last_auto_compaction_revision = None;\n        let mut provider_completed_this_run = false;\n',
    1,
)
start = s.index('            let context_anchor = if can_auto_compact {')
end = s.index('            if can_auto_compact && should_auto_compact', start)
anchor_block = '''            // Do not carry a stale pre-compaction usage anchor across a Resume boundary.
            // Direct estimation still catches a genuinely oversized recovered state.
            let may_use_usage_anchor =
                prepared.action == RunAction::Start || provider_completed_this_run;
            let context_anchor = if can_auto_compact && may_use_usage_anchor {
                match self
                    .store
                    .latest_llm_call_usage_anchor(
                        &prepared.conversation_id,
                        &prepared.model.model_id,
                    )
                    .await
                {
                    Ok(anchor) => anchor.and_then(ContextUsageAnchor::from_llm_call),
                    Err(error) => return (RunOutcome::Failed(error.into()), usage),
                }
            } else {
                None
            };
'''
s = s[:start] + anchor_block + s[end:]

# Only a completed normal model cycle makes the cross-run usage anchor current.
auto_start = s.index('    async fn auto_compact(')
pos = s.rfind('            if let Some(cycle_usage) = cycle.usage {', 0, auto_start)
assert pos != -1
s = s[:pos] + '            provider_completed_this_run = true;\n' + s[pos:]
p.write_text(s)

# Replace the synthetic-anchor Resume regression added by the base patch. That fixture
# intentionally injects stale cross-run usage, which should now be ignored. The replacement
# verifies a genuinely oversized recovered state compacts with no new user message.
p = Path('server/tests/compaction_recovery.rs')
t = p.read_text()
begin = t.index('#[tokio::test]\nasync fn resume_action_auto_compacts_before_the_next_model_call()')
end = t.index('async fn record_threshold_anchor(', begin)
replacement_test = r'''#[tokio::test]
async fn resume_action_auto_compacts_large_recovered_state_without_new_user_message() {
    let (_directory, store) = fixtures::temp_store().await;
    let model = store
        .create_model(&ModelConfigInput {
            sort_order: 0,
            display_name: "Resume Threshold".into(),
            model_type: ModelType::OpenAi,
            base_url: "https://example.com/v1/chat/completions".into(),
            use_full_url: true,
            api_key: "test-key".into(),
            tooltip_data: "Resume Threshold".into(),
            model_id: "resume-threshold".into(),
            reasoning_effort: None,
            openai_endpoint: OPENAI_CHAT_ENDPOINT.into(),
            openai_extra_params_enabled: false,
            openai_extra_params: serde_json::json!({}),
            custom_headers_enabled: false,
            custom_headers: serde_json::json!({}),
            anthropic_extra_params_enabled: false,
            anthropic_extra_params: serde_json::json!({}),
            context_window_tokens: Some(30_000),
            max_completion_tokens: Some(4_000),
            anthropic_max_tokens: None,
            anthropic_thinking_effort: None,
            thinking_budget_tokens: None,
        })
        .await
        .unwrap();
    let provider = fake_provider::FakeProvider::default();
    provider.push(text_response("x".repeat(100_000), 1_000, 25_000));
    provider.push(text_response("resume summary marker", 26_000, 100));
    provider.push(text_response("continued after resume compaction", 2_000, 20));
    let assets = PromptAssets::load(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("prompt/cursor")
            .as_path(),
    )
    .unwrap();
    let registry = CursorSessionRegistry::new(
        store.clone(),
        Arc::new(provider.clone()),
        PromptCompiler::new(assets),
        Default::default(),
    );
    let first = run(
        &registry,
        "resume-large-first",
        user_request(
            "resume-large-conversation",
            "resume-user",
            "start long work",
            &model.model_hash,
            None,
        ),
    )
    .await;
    assert!(first.end_error.is_none());
    let state = first.checkpoints.last().unwrap().clone();
    let resumed = run(
        &registry,
        "resume-large-second",
        resume_request("resume-large-conversation", &model.model_hash, state),
    )
    .await;
    assert!(resumed.end_error.is_none());
    assert_eq!(resumed.summary_started, 1);
    assert_eq!(resumed.summary_completed, 1);
    let requests = provider.requests();
    assert_eq!(requests.len(), 3);
    assert!(requests[1].prompt.tools.is_empty());
    assert!(history_text(&requests[2].history).contains("resume summary marker"));
}

'''
t = t[:begin] + replacement_test + t[end:]
p.write_text(t)
