// @effect-diagnostics nodeBuiltinImport:off - empty temporary Claude homes for the session count.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { assert, describe, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
  type OrchestrationV2DomainEvent,
  type ServerProvider,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as UsageLimitRecoveryWorker from "./UsageLimitRecoveryWorker.ts";
import { limitRecoveryCommand } from "./UsageLimitRecoveryWorker.ts";

const now = Date.parse("2026-10-18T15:00:00.000Z");
const a = ProviderInstanceId.make("claude_a");
const b = ProviderInstanceId.make("claude_b");
const threadId = ThreadId.make("thread:limited");
const runId = RunId.make("run:limited");
const modelSelection = { instanceId: a, model: "claude-opus-5-5" };
// Empty homes, so the session counts never read this machine's transcripts.
const home = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pool-sweep-"));
const providerInstances = Object.fromEntries(
  [a, b].map((instanceId) => [
    instanceId,
    {
      driver: ProviderDriverKind.make("claudeAgent"),
      config: { homePath: NodePath.join(home, instanceId) },
    },
  ]),
);

const account = (instanceId: ProviderInstanceId, usedPercent: number, resetsAt: string) =>
  ({
    instanceId,
    enabled: true,
    status: "ready",
    auth: { status: "authenticated" },
    usageLimits: {
      checkedAt: "2026-10-18T14:59:00.000Z",
      windows: [{ id: "seven_day", kind: "weekly", label: "Weekly", usedPercent, resetsAt }],
    },
  }) as unknown as ServerProvider;

const sweepOnce = (input: {
  readonly aResetsAt: string;
  readonly aUsed?: number;
  readonly running: boolean;
  readonly failed: boolean;
  // A thread kept apart from this one, on B.
  readonly apartOnB?: boolean;
}) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(now);
    const dispatched: Array<OrchestrationV2Command> = [];
    const candidate = {
      id: threadId,
      providerInstanceId: a,
      modelSelection,
      status: "failed",
      lastErrorClass: "usage_limit",
      latestRunId: runId,
      usageLimitResetAt: input.aResetsAt,
      archivedAt: null,
      settledOverride: null,
      pendingRuntimeRequest: null,
      latestRunCompletedAt: DateTime.makeUnsafe(now - 60_000),
      updatedAt: DateTime.makeUnsafe(now - 60_000),
      limitRecovery: null,
      snoozedUntil: null,
    } as unknown as ProjectionStore.ProjectionLimitRecoveryCandidate;
    const layer = Layer.mergeAll(
      ServerSettings.layerTest({
        accountPool: [a, b],
        accountPoolApart: input.apartOnB ? [threadId, ThreadId.make("thread:apart")] : [],
        providerInstances,
      }),
      Layer.mock(ProviderRegistry.ProviderRegistry)({
        getProviders: Effect.succeed([
          account(a, input.aUsed ?? 100, input.aResetsAt),
          account(b, 5, "2026-10-21T23:00:00.000Z"),
        ]),
      }),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getActiveThreads: () =>
          Effect.succeed([
            {
              id: threadId,
              providerInstanceId: a,
              modelSelection,
              createdAt: DateTime.makeUnsafe(now - 3_600_000),
              runningRunId: input.running ? runId : null,
            },
            ...(input.apartOnB
              ? [
                  {
                    id: ThreadId.make("thread:apart"),
                    providerInstanceId: b,
                    modelSelection: { instanceId: b, model: "claude-opus-5-5" },
                    createdAt: DateTime.makeUnsafe(now - 3_600_000),
                    runningRunId: RunId.make("run:apart"),
                  },
                ]
              : []),
          ]),
        getLimitRecoveryCandidates: () => Effect.succeed(input.failed ? [candidate] : []),
        getLimitRecoveryWakeAt: () => Effect.succeed(null),
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({ streamDomainEvents: Stream.empty }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command as OrchestrationV2Command);
            return { sequence: dispatched.length, storedEvents: [] };
          }),
      }),
    );
    const sweep = yield* UsageLimitRecoveryWorker.makeSweep.pipe(
      Effect.provide(layer),
      Effect.scoped,
    );
    yield* sweep().pipe(Effect.provide(layer));
    return dispatched;
  });

it.effect("moves a turn held by a used-up account to the account with room", () =>
  Effect.gen(function* () {
    const dispatched = yield* sweepOnce({
      aResetsAt: "2026-10-19T19:00:00.000Z",
      running: true,
      failed: false,
    });
    assert.deepEqual(
      dispatched.map((command) => command.type),
      ["provider.switch", "message.dispatch"],
    );
    const [switched, message] = dispatched;
    assert.equal(switched?.type === "provider.switch" && switched.modelSelection.instanceId, b);
    assert.deepEqual(message?.type === "message.dispatch" && message.dispatchMode, {
      type: "queue_after_active",
    });
  }),
);

it.effect("shares an account with a thread kept apart when no other account has room", () =>
  Effect.gen(function* () {
    const dispatched = yield* sweepOnce({
      aResetsAt: "2026-10-19T19:00:00.000Z",
      running: true,
      failed: false,
      apartOnB: true,
    });
    const [switched] = dispatched;
    assert.equal(switched?.type === "provider.switch" && switched.modelSelection.instanceId, b);
  }),
);

it.effect("moves a thread whose turn failed on a long limit and starts it again", () =>
  Effect.gen(function* () {
    const dispatched = yield* sweepOnce({
      aResetsAt: "2026-10-19T19:00:00.000Z",
      running: false,
      failed: true,
    });
    const message = dispatched.find((command) => command.type === "message.dispatch");
    assert.deepEqual(message?.type === "message.dispatch" && message.dispatchMode, {
      type: "queue_after_active",
    });
  }),
);

it.effect("waits out a limit that lifts within half an hour", () =>
  Effect.gen(function* () {
    const dispatched = yield* sweepOnce({
      aResetsAt: "2026-10-18T15:20:00.000Z",
      running: true,
      failed: true,
    });
    assert.deepEqual(dispatched, []);
  }),
);

it.effect("moves a thread off a nearly used-up account between turns, never cutting one", () =>
  Effect.gen(function* () {
    const at = (aUsed: number, running: boolean) =>
      sweepOnce({ aResetsAt: "2026-10-19T19:00:00.000Z", aUsed, running, failed: false });
    assert.deepEqual(yield* at(99, true), []);
    // 97% would pass the 90% room by the reset, but a projection never moves a thread.
    assert.deepEqual(yield* at(97, false), []);
    const [switched, message] = yield* at(99, false);
    assert.equal(switched?.type === "provider.switch" && switched.modelSelection.instanceId, b);
    assert.match(
      message?.type === "message.dispatch" ? message.text : "",
      /nearly at its usage limit/,
    );
  }),
);

// B holds six running threads and sits at 50% of a five-hour window that resets in three hours.
const busyHome = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "pool-busy-"));
const busyInstances = Object.fromEntries(
  [a, b].map((instanceId) => [
    instanceId,
    {
      driver: ProviderDriverKind.make("claudeAgent"),
      config: { homePath: NodePath.join(busyHome, instanceId) },
    },
  ]),
);
NodeFS.mkdirSync(NodePath.join(busyHome, b, "projects", "repo"), { recursive: true });
for (let index = 0; index < 6; index++) {
  const transcript = NodePath.join(busyHome, b, "projects", "repo", `${index}.jsonl`);
  NodeFS.writeFileSync(transcript, "{}\n");
  NodeFS.utimesSync(transcript, (now - 60_000) / 1000, (now - 60_000) / 1000);
}
const withWindows = (instanceId: ProviderInstanceId, windows: ReadonlyArray<unknown>) =>
  ({
    instanceId,
    enabled: true,
    status: "ready",
    auth: { status: "authenticated" },
    usageLimits: { checkedAt: "2026-10-18T14:59:00.000Z", windows },
  }) as unknown as ServerProvider;
const busyB = withWindows(b, [
  {
    id: "five_hour",
    kind: "session",
    label: "Session",
    usedPercent: 50,
    resetsAt: "2026-10-18T18:00:00.000Z",
    windowDurationMins: 300,
  },
  {
    id: "seven_day",
    kind: "weekly",
    label: "Weekly",
    usedPercent: 20,
    resetsAt: "2026-10-21T23:00:00.000Z",
    windowDurationMins: 7 * 24 * 60,
  },
]);
const quietA = withWindows(a, [
  { id: "five_hour", kind: "session", label: "Session", usedPercent: 0, windowDurationMins: 300 },
  {
    id: "seven_day",
    kind: "weekly",
    label: "Weekly",
    usedPercent: 0,
    resetsAt: "2026-10-25T19:00:00.000Z",
    windowDurationMins: 7 * 24 * 60,
  },
]);

const sweepBusy = (
  threads: ReadonlyArray<{ id: string; running: boolean; ageHours: number }>,
  apart: ReadonlyArray<string>,
) =>
  Effect.gen(function* () {
    yield* TestClock.setTime(now);
    const dispatched: Array<OrchestrationV2Command> = [];
    const layer = Layer.mergeAll(
      ServerSettings.layerTest({
        accountPool: [a, b],
        accountPoolApart: apart.map((id) => ThreadId.make(id)),
        providerInstances: busyInstances,
      }),
      Layer.mock(ProviderRegistry.ProviderRegistry)({
        getProviders: Effect.succeed([quietA, busyB]),
      }),
      Layer.mock(ProjectionStore.ProjectionStoreV2)({
        getActiveThreads: () =>
          Effect.succeed(
            threads.map((thread) => ({
              id: ThreadId.make(thread.id),
              providerInstanceId: b,
              modelSelection: { instanceId: b, model: "claude-opus-5-5" },
              createdAt: DateTime.makeUnsafe(now - thread.ageHours * 3_600_000),
              runningRunId: thread.running ? RunId.make(`run:${thread.id}`) : null,
            })),
          ),
        getLimitRecoveryCandidates: () => Effect.succeed([]),
        getLimitRecoveryWakeAt: () => Effect.succeed(null),
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({ streamDomainEvents: Stream.empty }),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command as OrchestrationV2Command);
            return { sequence: dispatched.length, storedEvents: [] };
          }),
      }),
    );
    const sweep = yield* UsageLimitRecoveryWorker.makeSweep.pipe(
      Effect.provide(layer),
      Effect.scoped,
    );
    yield* sweep().pipe(Effect.provide(layer));
    return dispatched.flatMap((command) =>
      command.type === "provider.switch" ? [command.threadId] : [],
    );
  });

it.effect("moves an idle thread kept apart off the account it shares, never a running one", () =>
  Effect.gen(function* () {
    const moved = yield* sweepBusy(
      [
        { id: "thread:ltm", running: false, ageHours: 9 },
        { id: "thread:infra", running: true, ageHours: 8 },
        { id: "thread:worker", running: true, ageHours: 1 },
      ],
      ["thread:ltm", "thread:infra"],
    );
    // The running infra lead stays put.
    assert.deepEqual(moved, ["thread:ltm"]);
  }),
);

it.effect("idle sweeps skip SQL until the next wake time or a relevant event", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(Date.parse("2026-07-01T09:00:00.000Z"));
    const startMs = Date.parse("2026-07-01T09:00:00.000Z");
    const queries = yield* Queue.unbounded<number>();
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const layer = UsageLimitRecoveryWorker.layer.pipe(
      Layer.provide(
        Layer.mergeAll(
          Scheduler.layer,
          Layer.mock(ProjectionStore.ProjectionStoreV2)({
            getLimitRecoveryCandidates: () =>
              Clock.currentTimeMillis.pipe(
                Effect.flatMap((nowMs) => Queue.offer(queries, nowMs - startMs)),
                Effect.as([]),
              ),
            // One armed thread resets at 60s.
            getLimitRecoveryWakeAt: (now) =>
              Effect.succeed(
                DateTime.toEpochMillis(now) < startMs + 60_000 ? startMs + 60_000 : null,
              ),
          }),
          Layer.mock(ThreadManagement.ThreadManagementService)({}),
          Layer.mock(ProviderRegistry.ProviderRegistry)({}),
          Layer.mock(Orchestrator.OrchestratorV2)({
            streamDomainEvents: Stream.fromQueue(events),
          }),
          Layer.mock(ServerSettings.ServerSettingsService)({
            getSettings: Effect.succeed({
              autoResumeLimitedThreads: true,
              snoozeLimitedThreads: false,
              accountPool: [],
            } as never),
            streamChanges: Stream.never,
          }),
        ),
      ),
    );
    yield* Effect.gen(function* () {
      assert.equal(yield* Queue.take(queries), 0);
      // Eleven idle ticks before the recorded wake time, then the wake tick itself.
      yield* TestClock.adjust("60 seconds");
      assert.equal(yield* Queue.take(queries), 60_000);
      yield* Queue.offer(events, {
        type: "turn-item.updated",
        payload: { type: "assistant_message" },
      } as never);
      yield* TestClock.adjust("30 seconds");
      yield* Queue.offer(events, { type: "run.updated", payload: { status: "failed" } } as never);
      yield* TestClock.adjust("5 seconds");
      // A streaming item at 90s does not wake it; the failed run does on the 95s tick.
      assert.equal(yield* Queue.take(queries), 95_000);
    }).pipe(Effect.provide(layer));
  }),
);

const HOUR_MS = 60 * 60 * 1000;
const nowMs = Date.parse("2026-10-19T12:00:00.000Z");

const failedThread = (failedAgoMs: number, resetAgoMs: number) => {
  const failedAt = DateTime.makeUnsafe(nowMs - failedAgoMs);
  const candidate: ProjectionStore.ProjectionLimitRecoveryCandidate = {
    id: ThreadId.make("thread:limited"),
    providerInstanceId: a,
    modelSelection,
    status: "failed",
    lastErrorClass: "usage_limit",
    usageLimitResetAt: DateTime.formatIso(DateTime.makeUnsafe(nowMs - resetAgoMs)),
    latestRunId: RunId.make("run:limited"),
    latestRunCompletedAt: failedAt,
    updatedAt: failedAt,
    archivedAt: null,
    settledOverride: null,
    pendingRuntimeRequest: null,
    limitRecovery: null,
    snoozedUntil: null,
  };
  return candidate;
};

describe("limitRecoveryCommand arming", () => {
  it("does not freshly arm a failure whose reset passed days ago", () => {
    const thread = failedThread(5 * 24 * HOUR_MS, 4 * 24 * HOUR_MS);
    assert.isNull(limitRecoveryCommand(thread, true, nowMs));
    assert.isNull(limitRecoveryCommand(thread, true, nowMs, true));
  });

  it("still arms a failure whose reset only just passed", () => {
    const thread = failedThread(6 * HOUR_MS, HOUR_MS);
    const arm = limitRecoveryCommand(thread, true, nowMs);
    assert.equal(arm?.type, "thread.metadata.update");
  });

  it("arms a reset that is exactly 24 hours overdue", () => {
    const thread = failedThread(30 * HOUR_MS, 24 * HOUR_MS);
    const arm = limitRecoveryCommand(thread, true, nowMs);
    assert.equal(arm?.type, "thread.metadata.update");
  });

  it("does not arm a reset that is 24 hours and 1ms overdue", () => {
    const thread = failedThread(30 * HOUR_MS, 24 * HOUR_MS + 1);
    assert.isNull(limitRecoveryCommand(thread, true, nowMs));
  });
});
