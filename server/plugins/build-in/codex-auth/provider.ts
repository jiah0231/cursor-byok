import type {
  ProviderInvokeInput,
  ProviderOutput,
  ProviderResult,
  ProviderSupport,
} from "cursor-byok:provider";
import type { PluginContext } from "cursor-byok:plugin";
import { HttpError, streamOpenAiResponses } from "cursor-byok:protocol/openai-responses";
import { codexModels, reasoningEfforts } from "./models.ts";
import {
  type AccountData,
  accountData,
  chatGptAccountId,
  quotaExhaustedPatch,
  RESOURCE_TYPE,
} from "./resources.ts";

const RESPONSES_URL = "https://chatgpt.com/backend-api/codex/responses";
const RESPONSES_FAILURE_PREFIX = "OpenAI Responses failed: ";
const TRANSIENT_RETRY_DELAYS_MS = [350, 1200, 3500] as const;
const TRANSIENT_RESPONSE_CODES = new Set([
  "server_is_overloaded",
  "temporarily_unavailable",
  "service_unavailable",
  "server_error",
  "internal_server_error",
  "rate_limit_exceeded",
]);
const TRANSIENT_HTTP_STATUSES = new Set([408, 409, 425, 429, 500, 502, 503, 504]);

type ResponsesFailure = {
  code: string | null;
  message: string | null;
  responseId: string | null;
  totalTokens: number | null;
};

function record(value: unknown): Record<string, unknown> | null {
  return value !== null && typeof value === "object" && !Array.isArray(value)
    ? value as Record<string, unknown>
    : null;
}

function text(value: unknown): string | null {
  return typeof value === "string" && value.length > 0 ? value : null;
}

function count(value: unknown): number | null {
  return typeof value === "number" && Number.isFinite(value) ? value : null;
}

/** 流内错误只有文本可用,按额度关键词分类。 */
export function isQuotaError(error: string): boolean {
  const message = error.toLowerCase();
  return message.includes("insufficient_quota") ||
    message.includes("usage_limit_reached") ||
    message.includes("exceeded your current quota") ||
    message.includes("quota_exceeded") ||
    message.includes("5-hour") ||
    message.includes("5 hour") ||
    (message.includes("429") &&
      (message.includes("quota") || message.includes("usage_limit") ||
        message.includes("insufficient")));
}

/** HTTP 失败携带结构化状态码,429 时放宽响应体的匹配条件。 */
function isQuotaHttpError(error: HttpError): boolean {
  const body = error.body.toLowerCase();
  return body.includes("insufficient_quota") ||
    body.includes("usage_limit_reached") ||
    body.includes("exceeded your current quota") ||
    body.includes("quota_exceeded") ||
    body.includes("5-hour") ||
    body.includes("5 hour") ||
    (error.status === 429 &&
      (body.includes("quota") || body.includes("usage_limit") || body.includes("insufficient")));
}

function responsesFailure(error: unknown): ResponsesFailure | null {
  const message = error instanceof Error ? error.message : String(error);
  if (!message.startsWith(RESPONSES_FAILURE_PREFIX)) return null;
  try {
    const event = record(JSON.parse(message.slice(RESPONSES_FAILURE_PREFIX.length)));
    const response = record(event?.response);
    const failure = record(response?.error);
    return {
      code: text(failure?.code),
      message: text(failure?.message),
      responseId: text(response?.id),
      totalTokens: count(record(response?.usage)?.total_tokens),
    };
  } catch {
    return { code: null, message: null, responseId: null, totalTokens: null };
  }
}

function isTransientFailure(error: unknown): boolean {
  if (error instanceof HttpError) {
    return !isQuotaHttpError(error) && TRANSIENT_HTTP_STATUSES.has(error.status);
  }
  const message = error instanceof Error ? error.message : String(error);
  if (isQuotaError(message)) return false;
  const failure = responsesFailure(error);
  // Only replay a streamed failure when the provider explicitly reports zero usage.
  // This avoids duplicating a request that performed hidden reasoning before failing.
  return failure !== null && failure.code !== null && failure.totalTokens === 0 &&
    TRANSIENT_RESPONSE_CODES.has(failure.code);
}

function compactResponsesFailure(error: unknown): string | null {
  const failure = responsesFailure(error);
  if (!failure) return null;
  const detail = failure.message ?? "OpenAI Responses request failed";
  const diagnostics = [failure.code, failure.responseId].filter((value) => value !== null);
  return diagnostics.length > 0 ? `${detail} (${diagnostics.join(", ")})` : detail;
}

function transientFailureMessage(error: unknown, retries: number): string {
  const attempts = retries + 1;
  const failure = compactResponsesFailure(error);
  if (failure) return `${failure}. Automatic retry failed after ${attempts} attempts.`;
  if (error instanceof HttpError) {
    return `OpenAI request failed after ${attempts} attempts (HTTP ${error.status}).`;
  }
  const message = error instanceof Error ? error.message : String(error);
  return `${message} Automatic retry failed after ${attempts} attempts.`;
}

async function waitForRetry(delayMs: number, signal: AbortSignal): Promise<boolean> {
  if (signal.aborted) return false;
  return await new Promise<boolean>((resolve) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve(true);
    }, delayMs);
    const onAbort = () => {
      clearTimeout(timer);
      signal.removeEventListener("abort", onAbort);
      resolve(false);
    };
    signal.addEventListener("abort", onAbort, { once: true });
    if (signal.aborted) onAbort();
  });
}

function invalidResult(message: string, stateMessage: string): ProviderResult {
  return {
    status: "resource-error",
    message,
    patch: { state: { status: "invalid", message: stateMessage } },
  };
}

function headers(data: AccountData, cacheKey: string | null): Record<string, string> {
  const result: Record<string, string> = {
    authorization: `Bearer ${data.accessToken}`,
    originator: "codex_cli_rs",
  };
  const accountId = chatGptAccountId(data.accessToken);
  if (accountId) result["ChatGPT-Account-Id"] = accountId;
  // Codex 后端的缓存亲和契约:session-id / thread-id / prompt_cache_key
  // 三者同源(见 codex-rs client.rs);缺头会导致请求落在随机分片上。
  if (cacheKey !== null) {
    result["session-id"] = cacheKey;
    result["thread-id"] = cacheKey;
    result["x-client-request-id"] = cacheKey;
  }
  return result;
}

async function invoke(
  input: ProviderInvokeInput,
  output: ProviderOutput,
  context: PluginContext,
): Promise<ProviderResult> {
  if (!input.resource) {
    return { status: "request-error", message: "add a ChatGPT account before calling Codex" };
  }
  let data: AccountData;
  try {
    data = accountData(input.resource);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    return invalidResult(message, message);
  }
  const efforts = reasoningEfforts(input.model);
  const reasoning = input.request.reasoning;
  const effort = reasoning.effort !== null && efforts.includes(reasoning.effort)
    ? reasoning.effort
    : null;

  let retries = 0;
  while (true) {
    let emitted = false;
    try {
      await streamOpenAiResponses(
        {
          url: RESPONSES_URL,
          model: input.model.id,
          // Codex 订阅端点不接受 max_output_tokens;fast 档位经协议库映射为
          // service_tier: "priority" 后透传。
          request: {
            ...input.request,
            reasoning: { enabled: reasoning.enabled, effort },
            maxOutputTokens: null,
          },
          headers: headers(data, input.request.cacheKey),
          extraBody: { store: false },
        },
        {
          emit: (event) => {
            emitted = true;
            output.emit(event);
          },
        },
        context,
      );
      return { status: "completed" };
    } catch (error) {
      if (
        !emitted && retries < TRANSIENT_RETRY_DELAYS_MS.length && isTransientFailure(error)
      ) {
        const shouldContinue = await waitForRetry(
          TRANSIENT_RETRY_DELAYS_MS[retries],
          context.signal,
        );
        if (!shouldContinue) return { status: "request-error", message: "request cancelled" };
        retries += 1;
        continue;
      }
      if (error instanceof HttpError) {
        if (error.status === 401) {
          return invalidResult(error.message, "ChatGPT authorization expired; sign in again");
        }
        if (isQuotaHttpError(error)) {
          return {
            status: "resource-error",
            message: error.message,
            patch: quotaExhaustedPatch(data, error.body),
          };
        }
        if (isTransientFailure(error)) {
          return { status: "request-error", message: transientFailureMessage(error, retries) };
        }
        return { status: "request-error", message: error.message };
      }
      const message = error instanceof Error ? error.message : String(error);
      if (isQuotaError(message)) {
        return { status: "resource-error", message, patch: quotaExhaustedPatch(data, message) };
      }
      if (isTransientFailure(error)) {
        return { status: "request-error", message: transientFailureMessage(error, retries) };
      }
      const responseMessage = compactResponsesFailure(error);
      return { status: "request-error", message: responseMessage === null ? message : `${responseMessage}.` };
    }
  }
}

export const codexProvider: ProviderSupport = {
  id: "codex",
  displayName: "OpenAI Codex",
  description: {
    "en-US": "ChatGPT subscription access through the official Codex Responses API.",
    "zh-CN": "通过官方 Codex Responses API 使用 ChatGPT 订阅。",
  },
  providerType: "openai",
  resourceType: RESOURCE_TYPE,
  models: codexModels,
  invoke,
};
