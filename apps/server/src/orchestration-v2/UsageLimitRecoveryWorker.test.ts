import { assert, it } from "@effect/vitest";
import {
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
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as UsageLimitRecoveryWorker from "./UsageLimitRecoveryWorker.ts";

const now = Date.parse("2026-10-18T15:00:00.000Z");
const a = ProviderInstanceId.make("claude_a");
const b = ProviderInstanceId.make("claude_b");
const threadId = ThreadId.make("thread:limited");
const runId = RunId.make("run:limited");
const modelSelection = { instanceId: a, model: "claude-opus-5-5" };

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
  readonly running: boolean;
  readonly failed: boolean;
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
      ServerSettings.layerTest({ accountPool: [a, b] }),
      Layer.mock(ProviderRegistry.ProviderRegistry)({
        getProviders: Effect.succeed([
          account(a, 100, input.aResetsAt),
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
              runningRunId: input.running ? runId : null,
            },
          ]),
        getLimitRecoveryCandidates: () => Effect.succeed(input.failed ? [candidate] : []),
      }),
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
