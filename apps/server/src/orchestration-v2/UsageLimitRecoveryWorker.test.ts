// @effect-diagnostics nodeBuiltinImport:off - empty temporary Claude homes for the session count.
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import { assert, it } from "@effect/vitest";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  type OrchestrationV2Command,
  type ServerProvider,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as TestClock from "effect/testing/TestClock";

import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as UsageLimitRecoveryWorker from "./UsageLimitRecoveryWorker.ts";

const now = Date.parse("2026-10-18T15:00:00.000Z");
const a = ProviderInstanceId.make("claude_a");
const b = ProviderInstanceId.make("claude_b");
const threadId = ThreadId.make("thread:limited");
const runId = RunId.make("run:limited");
const modelSelection = { instanceId: a, model: "claude-opus-5-5" };
// Empty homes, so the session counts never read this machine's transcripts.
const home = fs.mkdtempSync(path.join(os.tmpdir(), "pool-sweep-"));
const providerInstances = Object.fromEntries(
  [a, b].map((instanceId) => [
    instanceId,
    {
      driver: ProviderDriverKind.make("claudeAgent"),
      config: { homePath: path.join(home, instanceId) },
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
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({}),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command as OrchestrationV2Command);
            return { sequence: dispatched.length, storedEvents: [] };
          }),
      }),
    );
    const sweep = yield* UsageLimitRecoveryWorker.makeSweep.pipe(Effect.provide(layer));
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
const busyHome = fs.mkdtempSync(path.join(os.tmpdir(), "pool-busy-"));
const busyInstances = Object.fromEntries(
  [a, b].map((instanceId) => [
    instanceId,
    {
      driver: ProviderDriverKind.make("claudeAgent"),
      config: { homePath: path.join(busyHome, instanceId) },
    },
  ]),
);
fs.mkdirSync(path.join(busyHome, b, "projects", "repo"), { recursive: true });
for (let index = 0; index < 6; index++) {
  const transcript = path.join(busyHome, b, "projects", "repo", `${index}.jsonl`);
  fs.writeFileSync(transcript, "{}\n");
  fs.utimesSync(transcript, (now - 60_000) / 1000, (now - 60_000) / 1000);
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
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({}),
      Layer.mock(ThreadManagement.ThreadManagementService)({
        dispatch: (command) =>
          Effect.sync(() => {
            dispatched.push(command as OrchestrationV2Command);
            return { sequence: dispatched.length, storedEvents: [] };
          }),
      }),
    );
    const sweep = yield* UsageLimitRecoveryWorker.makeSweep.pipe(Effect.provide(layer));
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
