import { parseOfficialModels } from "./models.ts";

function assertEquals(actual: unknown, expected: unknown): void {
  const left = JSON.stringify(actual);
  const right = JSON.stringify(expected);
  if (left !== right) throw new Error(`expected ${right}, received ${left}`);
}

Deno.test("official Codex reasoning levels enable thinking and preserve supported efforts", () => {
  const models = parseOfficialModels({
    models: [{
      slug: "gpt-5.3-codex",
      display_name: "GPT-5.3-Codex",
      supported_in_api: true,
      visibility: "list",
      supported_reasoning_levels: [
        { effort: "low", description: "Fast answers" },
        { effort: "medium", description: "Balanced reasoning" },
        { effort: "high", description: "Deeper reasoning" },
        { effort: "xhigh", description: "Deepest reasoning" },
      ],
    }],
  });

  assertEquals(models.length, 1);
  assertEquals(models[0].capabilities, { thinking: true, images: true });
  assertEquals(models[0].privateData, {
    reasoningEfforts: ["low", "medium", "high", "xhigh"],
  });
});

Deno.test("legacy reasoning effort aliases remain supported", () => {
  const models = parseOfficialModels({
    models: [{
      slug: "legacy-codex",
      supported_in_api: true,
      visibility: "list",
      supported_reasoning_efforts: ["medium", "high"],
    }],
  });

  assertEquals(models[0].capabilities, { thinking: true, images: true });
  assertEquals(models[0].privateData, { reasoningEfforts: ["medium", "high"] });
});
