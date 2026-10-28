import { ProviderInstanceId, type ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { pickPoolAccount } from "./accountPool.ts";

const nowMs = Date.parse("2026-10-18T15:00:00.000Z");
const a = ProviderInstanceId.make("claude_a");
const b = ProviderInstanceId.make("claude_b");
const model = "claude-opus-5-5";

const account = (instanceId: ProviderInstanceId, weeklyUsed: number, weeklyReset: string) =>
  ({
    instanceId,
    enabled: true,
    status: "ready",
    auth: { status: "authenticated" },
    usageLimits: {
      checkedAt: "2026-10-18T14:59:00.000Z",
      windows: [
        {
          id: "seven_day",
          kind: "weekly",
          label: "Weekly",
          usedPercent: weeklyUsed,
          resetsAt: weeklyReset,
        },
      ],
    },
  }) as unknown as ServerProvider;

describe("pickPoolAccount", () => {
  it("picks the account resetting soonest unless it is at 95%", () => {
    const pick = (aUsed: number) =>
      pickPoolAccount({
        pool: [a, b],
        providers: [
          account(a, aUsed, "2026-10-19T19:00:00.000Z"),
          account(b, 60, "2026-10-21T23:00:00.000Z"),
        ],
        model,
        nowMs,
      });
    expect(pick(94)).toBe(a);
    expect(pick(95)).toBe(b);
  });
});
