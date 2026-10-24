// @effect-diagnostics nodeBuiltinImport:off - writes a temporary Claude home for the session count.
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import {
  ProviderInstanceId,
  type ServerProvider,
  type ServerProviderUsageWindow,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  activeSessions,
  pickPoolAccount,
  poolAccountBlockedUntil,
  poolAccountOverrun,
} from "./accountPool.ts";

const nowMs = Date.parse("2026-10-18T15:00:00.000Z");
const a = ProviderInstanceId.make("claude_a");
const b = ProviderInstanceId.make("claude_b");
const pool = [a, b];
const model = "claude-opus-5-5";

const session = (usedPercent: number, resetsAt = "2026-10-18T18:00:00.000Z") =>
  ({
    id: "five_hour",
    kind: "session",
    label: "Session",
    usedPercent,
    resetsAt,
    windowDurationMins: 300,
  }) as const;
const weekly = (usedPercent: number, resetsAt: string, id = "seven_day") =>
  ({
    id,
    kind: "weekly",
    label: "Weekly",
    usedPercent,
    resetsAt,
    windowDurationMins: 7 * 24 * 60,
  }) as const;

// Only the fields routing reads; the rest of a snapshot does not bear on it.
const account = (
  instanceId: ProviderInstanceId,
  windows: ReadonlyArray<ServerProviderUsageWindow>,
  checkedAt = "2026-10-18T14:59:00.000Z",
): ServerProvider =>
  ({
    instanceId,
    enabled: true,
    status: "ready",
    auth: { status: "authenticated" },
    usageLimits: { checkedAt, windows },
  }) as unknown as ServerProvider;

// A's weekly window resets in 28 hours, B's in 80.
const aWeek = "2026-10-19T19:00:00.000Z";
const soonA = account(a, [session(10), weekly(70, aWeek)]);
const laterB = account(b, [session(0), weekly(5, "2026-10-21T23:00:00.000Z")]);
const pick = (
  providers: ReadonlyArray<ServerProvider>,
  extra: Partial<Parameters<typeof pickPoolAccount>[0]> & { sessionsOnA?: number } = {},
) =>
  pickPoolAccount({
    pool,
    providers,
    load: { sessions: new Map([[a, extra.sessionsOnA ?? 0]]) },
    model,
    nowMs,
    ...extra,
  });

describe("pickPoolAccount", () => {
  it("starts new threads on the account whose weekly window resets soonest", () => {
    expect(pick([soonA, laterB])).toBe(a);
  });

  it("skips an account that would end past its weekly or session room", () => {
    expect(pick([account(a, [weekly(95, aWeek)]), laterB])).toBe(b);
    expect(pick([account(a, [session(90), weekly(10, aWeek)]), laterB])).toBe(b);
  });

  it("counts every session on an account against the use left before its reset", () => {
    // 70% plus 28 hours at 0.05 points a session an hour: sixteen sessions and the new thread fit under
    // 95%, seventeen do not.
    const weekOnly = account(a, [weekly(70, aWeek)]);
    expect(pick([weekOnly, laterB], { sessionsOnA: 16 })).toBe(a);
    expect(pick([weekOnly, laterB], { sessionsOnA: 17 })).toBe(b);
  });

  it("projects a weekly window at its average so far, not a busy few hours", () => {
    // Oct 24: A at 65% after 117 hours, 51 to its reset. Its average, 0.56 points an hour, ends it at 93%.
    const busyA = account(a, [weekly(65, "2026-10-20T18:00:00.000Z")]);
    expect(poolAccountOverrun(busyA, model, nowMs, { sessions: new Map() }, 0)).toBeCloseTo(
      65 + (65 / 117) * 51 - 95,
      5,
    );
  });

  it("averages a window's first hours over a day, so a heavy start still reads as heavy", () => {
    // Three hours into the week: 15% over a day is 0.63 points an hour, past 95% by the reset; 2% is not.
    const week = "2026-10-25T12:00:00.000Z";
    const overrun = (used: number) =>
      poolAccountOverrun(
        account(a, [weekly(used, week)]),
        model,
        nowMs,
        { sessions: new Map() },
        0,
      );
    expect(overrun(15)).toBeGreaterThan(0);
    expect(overrun(2)).toBeLessThan(0);
  });

  it("lets a used-up model window bind only threads on that model", () => {
    const fableFull = account(a, [weekly(20, aWeek), weekly(100, aWeek, "seven_day_fable")]);
    expect(pick([fableFull, laterB])).toBe(a);
    expect(pick([fableFull, laterB], { model: "claude-fable-5-1" })).toBe(b);
  });

  it("treats a window whose reset has passed as started over", () => {
    const renewed = account(a, [weekly(100, "2026-10-18T14:00:00.000Z")]);
    expect(pick([renewed, laterB], { exclude: [b] })).toBe(a);
  });

  it("projects a busy account's unopened five-hour window over its whole length", () => {
    // B's week resets first, but its nine sessions would use its next five hours past their room.
    const unopened = {
      id: "five_hour",
      kind: "session",
      label: "Session",
      usedPercent: 0,
      windowDurationMins: 300,
    } as const;
    const busyB = account(b, [unopened, weekly(20, "2026-10-19T23:00:00.000Z")]);
    const quietA = account(a, [unopened, weekly(0, "2026-10-25T19:00:00.000Z")]);
    const load = (sessionsOnB: number) => ({
      sessions: new Map([[b, sessionsOnB]]),
    });
    expect(pickPoolAccount({ pool, providers: [quietA, busyB], load: load(9), model, nowMs })).toBe(
      a,
    );
    expect(pickPoolAccount({ pool, providers: [quietA, busyB], load: load(3), model, nowMs })).toBe(
      b,
    );
  });

  it("finds no account without room, unless asked for the one that overruns least", () => {
    const fullB = account(b, [weekly(99, "2026-10-21T23:00:00.000Z")]);
    const fullA = account(a, [weekly(97, aWeek)]);
    expect(pick([fullA, fullB])).toBe(undefined);
    expect(pick([fullA, fullB], { fallback: true })).toBe(a);
  });
});

describe("activeSessions", () => {
  it("counts transcripts written in the last 15 minutes", () => {
    const home = fs.mkdtempSync(path.join(os.tmpdir(), "pool-home-"));
    const project = path.join(home, "projects", "-repo");
    fs.mkdirSync(path.join(project, "subagents"), { recursive: true });
    for (const [file, ageMin] of [
      ["live.jsonl", 2],
      ["idle.jsonl", 40],
      ["subagents/agent.jsonl", 1],
    ] as const) {
      fs.writeFileSync(path.join(project, file), "{}\n");
      const atSeconds = (nowMs - ageMin * 60_000) / 1000;
      fs.utimesSync(path.join(project, file), atSeconds, atSeconds);
    }
    expect(activeSessions(home, nowMs)).toBe(1);
    expect(activeSessions(path.join(home, "missing"), nowMs)).toBe(0);
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
