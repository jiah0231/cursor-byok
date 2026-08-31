import type {
  JsonValue,
  NetworkEventStream,
  PluginContext,
} from "cursor-byok:plugin";
import type { LlmRequest, ModelEvent } from "cursor-byok:provider";
import type { ResourceSnapshot } from "cursor-byok:resource";
import { codexProvider } from "./provider.ts";
import { credentialDraft, RESOURCE_TYPE } from "./resources.ts";

function assert(condition: unknown, message = "assertion failed"): asserts condition {
  if (!condition) throw new Error(message);
}

function assertEquals(actual: unknown, expected: unknown): void {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) throw new Error(`expected ${right}, received ${left}`);
}

function jwt(payload: Record<string, unknown>): string {
  const encoded = btoa(JSON.stringify(payload)).replace(/=/g, "").replace(/\+/g, "-").replace(
    /\//g,
    "_",
  );
  return `header.${encoded}.signature`;
}

function context(stream: () => NetworkEventStream): PluginContext {
  return {
    network: {
      fetch: () => {
        throw new Error("fetch was not expected");
      },
      stream: () => Promise.resolve(stream()),
    },
    signal: new AbortController().signal,
  };
}

function snapshot(privateData: JsonValue): ResourceSnapshot {
  return {
    id: "resource-1",
    type: RESOURCE_TYPE,
    key: "codex:acct-1",
    privateData,
    state: { status: "ready" },
  };
}

async function* sse(lines: string[]): AsyncGenerator<string> {
  for (const line of lines) yield line;
}

function request(): LlmRequest {
  return {
    instructions: "You are a coding assistant.",
    messages: [{ role: "user", content: [{ type: "text", text: "hi" }] }],
    tools: [],
    reasoning: { enabled: true, effort: "medium" },
    latency: "standard",
    maxOutputTokens: null,
    cacheKey: "conversation-1",
  };
}

function overloadedFailure(totalTokens: number): string {
  return `data: ${JSON.stringify({
    type: "response.failed",
    response: {
      id: "resp-overloaded",
      status: "failed",
      error: {
        code: "server_is_overloaded",
        message: "Our servers are currently overloaded. Please try again later.",
      },
      output: [],
      usage: { total_tokens: totalTokens },
    },
  })}`;
}

async function account(): Promise<ResourceSnapshot> {
  const accessToken = jwt({ "https://api.openai.com/auth": { chatgpt_account_id: "acct-1" } });
  const draft = await credentialDraft({
    accessToken,
    refreshToken: null,
    displayName: null,
  });
  return snapshot(draft.privateData);
}

Deno.test("Codex retries a zero-token overloaded Responses failure before output", async () => {
  let calls = 0;
  const events: ModelEvent[] = [];
  const result = await codexProvider.invoke(
    {
      model: {
        id: "gpt-test",
        displayName: "GPT Test",
        privateData: { reasoningEfforts: ["medium"] },
      },
      resource: await account(),
      request: request(),
    },
    { emit: (event) => events.push(event) },
    context(() => {
      calls += 1;
      return calls === 1
        ? { status: 200, headers: {}, lines: sse([overloadedFailure(0)]) }
        : {
          status: 200,
          headers: {},
          lines: sse(['data: {"type":"response.completed","response":{}}']),
        };
    }),
  );

  assertEquals(result, { status: "completed" });
  assertEquals(calls, 2);
  assertEquals(events, [{ type: "done", reason: "stop" }]);
});

Deno.test("Codex never replays a request after visible output has started", async () => {
  let calls = 0;
  const events: ModelEvent[] = [];
  const result = await codexProvider.invoke(
    {
      model: {
        id: "gpt-test",
        displayName: "GPT Test",
        privateData: { reasoningEfforts: ["medium"] },
      },
      resource: await account(),
      request: request(),
    },
    { emit: (event) => events.push(event) },
    context(() => {
      calls += 1;
      return {
        status: 200,
        headers: {},
        lines: sse([
          'data: {"type":"response.output_text.delta","delta":"partial"}',
          overloadedFailure(0),
        ]),
      };
    }),
  );

  assertEquals(calls, 1);
  assert(result.status === "request-error", `expected request-error, received ${result.status}`);
  assert(!result.message.includes('"response"'), "failure message should not dump raw JSON");
  assert(result.message.includes("server_is_overloaded"));
  assertEquals(events, [
    { type: "text-start" },
    { type: "text-delta", text: "partial" },
  ]);
});

Deno.test("Codex does not replay an overloaded failure that reports token usage", async () => {
  let calls = 0;
  const result = await codexProvider.invoke(
    {
      model: {
        id: "gpt-test",
        displayName: "GPT Test",
        privateData: { reasoningEfforts: ["medium"] },
      },
      resource: await account(),
      request: request(),
    },
    { emit: () => {} },
    context(() => {
      calls += 1;
      return { status: 200, headers: {}, lines: sse([overloadedFailure(12)]) };
    }),
  );

  assertEquals(calls, 1);
  assert(result.status === "request-error", `expected request-error, received ${result.status}`);
  assert(!result.message.includes('"response"'), "failure message should not dump raw JSON");
});
