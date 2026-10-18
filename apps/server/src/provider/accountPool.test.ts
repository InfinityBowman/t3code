import {
  ProviderInstanceId,
  type ServerProvider,
  type ServerProviderUsageWindow,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { pickPoolAccount, poolAccountBlockedUntil } from "./accountPool.ts";

const nowMs = Date.parse("2026-10-18T15:00:00.000Z");
const a = ProviderInstanceId.make("claude_a");
const b = ProviderInstanceId.make("claude_b");
const pool = [a, b];
const model = "claude-opus-5-5";

const session = (usedPercent: number, resetsAt = "2026-10-18T18:00:00.000Z") =>
  ({ id: "five_hour", kind: "session", label: "Session", usedPercent, resetsAt }) as const;
const weekly = (usedPercent: number, resetsAt: string, id = "seven_day") =>
  ({ id, kind: "weekly", label: "Weekly", usedPercent, resetsAt }) as const;

// Only the fields routing reads; the rest of a snapshot does not bear on it.
const account = (
  instanceId: ProviderInstanceId,
  windows: ReadonlyArray<ServerProviderUsageWindow>,
): ServerProvider =>
  ({
    instanceId,
    enabled: true,
    status: "ready",
    auth: { status: "authenticated" },
    usageLimits: { checkedAt: "2026-10-18T14:59:00.000Z", windows },
  }) as unknown as ServerProvider;

const soonA = account(a, [session(10), weekly(80, "2026-10-19T19:00:00.000Z")]);
const laterB = account(b, [session(0), weekly(5, "2026-10-21T23:00:00.000Z")]);
const pick = (
  providers: ReadonlyArray<ServerProvider>,
  extra: Partial<Parameters<typeof pickPoolAccount>[0]> = {},
) => pickPoolAccount({ pool, providers, activeThreads: [], model, nowMs, ...extra });

describe("pickPoolAccount", () => {
  it("starts new threads on the account whose weekly window resets soonest", () => {
    expect(pick([soonA, laterB])).toBe(a);
  });

  it("skips an account near its weekly or session limit", () => {
    expect(pick([account(a, [weekly(95, "2026-10-19T19:00:00.000Z")]), laterB])).toBe(b);
    expect(pick([account(a, [session(90), weekly(10, "2026-10-19T19:00:00.000Z")]), laterB])).toBe(
      b,
    );
  });

  it("counts each active thread against its account so a busy one hands new threads on", () => {
    const busy = Array.from({ length: 8 }, () => ({ providerInstanceId: a }));
    // 80% plus 8 threads' reserve passes the weekly limit; 2 threads do not.
    expect(pick([soonA, laterB], { activeThreads: busy })).toBe(b);
    expect(pick([soonA, laterB], { activeThreads: busy.slice(0, 2) })).toBe(a);
  });

  it("lets a used-up model window bind only threads on that model", () => {
    const fableFull = account(a, [
      weekly(20, "2026-10-19T19:00:00.000Z"),
      weekly(100, "2026-10-19T19:00:00.000Z", "seven_day_fable"),
    ]);
    expect(pick([fableFull, laterB])).toBe(a);
    expect(pick([fableFull, laterB], { model: "claude-fable-5-1" })).toBe(b);
  });

  it("treats a window whose reset has passed as started over", () => {
    const renewed = account(a, [weekly(100, "2026-10-18T14:00:00.000Z")]);
    expect(pick([renewed, laterB], { exclude: b })).toBe(a);
  });

  it("leaves the choice to the caller when no other account has room", () => {
    expect(pick([soonA, laterB], { exclude: b })).toBe(a);
    expect(
      pick([soonA, account(b, [weekly(99, "2026-10-21T23:00:00.000Z")])], { exclude: a }),
    ).toBe(undefined);
  });
});

describe("poolAccountBlockedUntil", () => {
  it("reports when a used-up window lifts", () => {
    const resetsAt = "2026-10-19T19:00:00.000Z";
    expect(poolAccountBlockedUntil(account(a, [weekly(100, resetsAt)]), model, nowMs)).toBe(
      Date.parse(resetsAt),
    );
    expect(poolAccountBlockedUntil(soonA, model, nowMs)).toBeUndefined();
  });
});
