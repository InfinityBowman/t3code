import { describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ServerCommand,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { TestClock } from "effect/testing";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  idleCompactionWindow,
  KEEP_WARM_TEXT,
  type IdleCompactionRecords,
} from "./IdleCompaction.ts";
import * as IdleCompactionService from "./IdleCompactionService.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const NOW_MS = Date.parse("2026-10-19T12:00:00.000Z");
const MINUTE_MS = 60_000;
const at = (offset = 0) => DateTime.makeUnsafe(NOW_MS + offset);
const threadId = ThreadId.make("idle-thread");
const runId = RunId.make("run-1");
const instanceId = ProviderInstanceId.make("claudeAgent");
const providerThreadId = ProviderThreadId.make("provider-thread");
const modelSelection = { instanceId, model: "claude-opus-5-5" };

function records(
  usage: {
    readonly usedTokens?: number;
    readonly cacheExpiresInMs?: number;
    readonly cacheTtlMs?: number;
  } = {},
): { -readonly [Key in keyof IdleCompactionRecords]: IdleCompactionRecords[Key] } {
  const messageId = MessageId.make("message-1");
  const nodeId = NodeId.make("root-node");
  return {
    thread: {
      id: threadId,
      projectId: ProjectId.make("project"),
      title: "Idle thread",
      createdBy: "user",
      creationSource: "web",
      providerInstanceId: instanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: providerThreadId,
      lineage: { rootThreadId: threadId, parentThreadId: null, relationshipToParent: null },
      forkedFrom: null,
      createdAt: at(-MINUTE_MS),
      updatedAt: at(),
      archivedAt: null,
      settledOverride: null,
      settledAt: null,
      lastVisitedAt: at(),
      deletedAt: null,
    },
    runs: [
      {
        id: runId,
        threadId,
        ordinal: 1,
        providerInstanceId: instanceId,
        modelSelection,
        providerThreadId,
        userMessageId: messageId,
        rootNodeId: nodeId,
        activeAttemptId: null,
        status: "completed",
        requestedAt: at(-MINUTE_MS),
        startedAt: at(-MINUTE_MS),
        completedAt: at(),
        checkpointId: null,
        contextHandoffId: null,
      },
    ],
    messages: [
      {
        id: messageId,
        threadId,
        runId,
        nodeId,
        role: "user",
        text: "Explain this code",
        attachments: [],
        streaming: false,
        createdBy: "user",
        creationSource: "web",
        createdAt: at(-MINUTE_MS),
        updatedAt: at(-MINUTE_MS),
      },
    ],
    providerThreads: [
      {
        id: providerThreadId,
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: nodeId,
        nativeThreadRef: null,
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt: at(-MINUTE_MS),
        updatedAt: at(),
      },
    ],
    providerTurns: [
      {
        id: ProviderTurnId.make("provider-turn-1"),
        providerThreadId,
        nodeId,
        runAttemptId: null,
        nativeTurnRef: null,
        ordinal: 1,
        status: "completed",
        startedAt: at(-MINUTE_MS),
        completedAt: at(),
        tokenUsage: {
          usedTokens: usage.usedTokens ?? 250_000,
          ...(usage.cacheExpiresInMs === undefined
            ? {}
            : {
                promptCacheTtlMs: usage.cacheTtlMs ?? 60 * MINUTE_MS,
                promptCacheExpiresAt: DateTime.formatIso(at(usage.cacheExpiresInMs)),
              }),
          updatedAt: DateTime.formatIso(at()),
        },
      },
    ],
    runtimeRequests: [],
    turnItems: [],
    contextTransfers: [],
  };
}

describe("idleCompactionWindow", () => {
  it("starts two minutes before the cache Claude reported expires", () => {
    expect(idleCompactionWindow(records({ cacheExpiresInMs: 60 * MINUTE_MS }), NOW_MS)).toEqual({
      action: "compact",
      runId,
      startMs: NOW_MS + 58 * MINUTE_MS,
      endMs: NOW_MS + 60 * MINUTE_MS,
    });
    // A 5m cache whose last request started 3 minutes before the run ended.
    const late = records({ cacheExpiresInMs: 2 * MINUTE_MS });
    expect(idleCompactionWindow(late, NOW_MS)).toMatchObject({ startMs: NOW_MS });
    expect(idleCompactionWindow(late, NOW_MS + 2 * MINUTE_MS)).toBeNull();
  });

  it("skips unknown expiry, other providers, switched threads, and compactions", () => {
    const cache = { cacheExpiresInMs: 60 * MINUTE_MS };
    expect(idleCompactionWindow(records(), NOW_MS)).toBeNull();
    const codex = records(cache);
    codex.providerThreads = [
      { ...codex.providerThreads[0]!, driver: ProviderDriverKind.make("codex") },
    ];
    expect(idleCompactionWindow(codex, NOW_MS)).toBeNull();
    const switched = records(cache);
    switched.thread = { ...switched.thread, providerInstanceId: ProviderInstanceId.make("codex") };
    expect(idleCompactionWindow(switched, NOW_MS)).toBeNull();
    // Below 200k too: a compaction is never followed by a keep-warm chain.
    for (const usedTokens of [250_000, 30_000]) {
      const compacted = records({ ...cache, usedTokens });
      compacted.messages = [{ ...compacted.messages[0]!, text: "/compact" }];
      expect(idleCompactionWindow(compacted, NOW_MS)).toBeNull();
    }
    const archived = records(cache);
    archived.thread = { ...archived.thread, archivedAt: at() };
    expect(idleCompactionWindow(archived, NOW_MS)).toBeNull();
  });
});

describe("keep-warm", () => {
  const cache = { cacheExpiresInMs: 60 * MINUTE_MS };

  it("pings below 200k at the moment it would compact", () => {
    expect(idleCompactionWindow(records({ ...cache, usedTokens: 199_999 }), NOW_MS)).toEqual({
      action: "keep_warm",
      runId,
      startMs: NOW_MS + 58 * MINUTE_MS,
      endMs: NOW_MS + 60 * MINUTE_MS,
    });
    // A five-minute cache would need a ping every three minutes, more than a cold re-read costs.
    expect(
      idleCompactionWindow(
        records({ usedTokens: 50_000, cacheExpiresInMs: 5 * MINUTE_MS, cacheTtlMs: 5 * MINUTE_MS }),
        NOW_MS,
      ),
    ).toBeNull();
  });

  it("pings instead of compacting while background work would report into the next turn", () => {
    const waiting = records(cache);
    waiting.providerThreads = [
      {
        ...waiting.providerThreads[0]!,
        pendingBackgroundTasks: [{ taskId: "agent-1", kind: "subagent" }],
      },
    ];
    expect(idleCompactionWindow(waiting, NOW_MS)).toMatchObject({ action: "keep_warm" });
  });

  it("stops six hours after the last run that wasn't a ping", () => {
    // Pings every 58 minutes after a real run that ended at `at(0)`.
    const chain = (pings: number) => {
      const value = records({ usedTokens: 30_000 });
      const real = value.runs[0]!;
      const pingAt = pings * 58 * MINUTE_MS;
      const pingRunId = RunId.make(`ping-${pings}`);
      const pingMessageId = MessageId.make(`keep-warm:${threadId}:ping-${pings}`);
      value.runs = [
        real,
        {
          ...real,
          id: pingRunId,
          ordinal: pings + 1,
          userMessageId: pingMessageId,
          requestedAt: at(pingAt),
          startedAt: at(pingAt),
          completedAt: at(pingAt),
        },
      ];
      value.messages = [
        { ...value.messages[0]!, id: pingMessageId, runId: pingRunId, text: KEEP_WARM_TEXT },
      ];
      value.providerTurns = [
        {
          ...value.providerTurns[0]!,
          tokenUsage: {
            usedTokens: 30_000,
            promptCacheTtlMs: 60 * MINUTE_MS,
            promptCacheExpiresAt: DateTime.formatIso(at(pingAt + 60 * MINUTE_MS)),
            updatedAt: DateTime.formatIso(at(pingAt)),
          },
        },
      ];
      return idleCompactionWindow(value, NOW_MS + pingAt);
    };
    expect(chain(5)).toMatchObject({ action: "keep_warm", startMs: NOW_MS + 348 * MINUTE_MS });
    expect(chain(6)).toBeNull();
  });
});

const seed = Effect.fn("seedIdleThread")(function* (value: IdleCompactionRecords) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const event = { threadId, occurredAt: at() };
  yield* store.apply({
    ...event,
    id: EventId.make("e1"),
    type: "thread.created",
    payload: value.thread,
  });
  yield* store.apply({
    ...event,
    id: EventId.make("e2"),
    type: "run.created",
    payload: value.runs[0]!,
  });
  yield* store.apply({
    ...event,
    id: EventId.make("e3"),
    type: "message.updated",
    payload: value.messages[0]!,
  });
  yield* store.apply({
    ...event,
    id: EventId.make("e4"),
    type: "provider-thread.updated",
    payload: value.providerThreads[0]!,
  });
  yield* store.apply({
    ...event,
    id: EventId.make("e5"),
    type: "provider-turn.updated",
    payload: value.providerTurns[0]!,
  });
});

it.effect("sends /compact as T3 Code once the cache is about to expire", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(NOW_MS);
    const value = records({ cacheExpiresInMs: 5 * MINUTE_MS });
    yield* seed(value);
    const events = yield* Queue.unbounded<OrchestrationV2DomainEvent>();
    const commands = yield* Queue.unbounded<OrchestrationV2ServerCommand>();
    const reads = yield* Queue.unbounded<void>();
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const dependencies = Layer.mergeAll(
      Layer.succeed(ProjectionStore.ProjectionStoreV2, {
        ...store,
        getThreadRecords: (...args) =>
          store.getThreadRecords(...args).pipe(Effect.tap(() => Queue.offer(reads, undefined))),
      }),
      Layer.mock(ProviderRegistry.ProviderRegistry)({ getProviders: Effect.succeed([]) }),
      Layer.mock(ServerSettings.ServerSettingsService)({
        getSettings: Effect.succeed({ ...DEFAULT_SERVER_SETTINGS, compactIdleThreads: true }),
      }),
      Layer.mock(Orchestrator.OrchestratorV2)({
        streamDomainEvents: Stream.fromQueue(events),
        dispatch: (command) =>
          Queue.offer(commands, command).pipe(Effect.as({ sequence: 1, storedEvents: [] })),
      }),
    );
    yield* Effect.gen(function* () {
      const service = yield* IdleCompactionService.make;
      yield* service.start();
      yield* Queue.offer(events, {
        id: EventId.make("completed"),
        type: "run.updated",
        threadId,
        occurredAt: at(),
        payload: value.runs[0]!,
      });
      yield* Queue.take(reads);
      yield* TestClock.adjust("2 minutes");
      expect(yield* Queue.size(commands)).toBe(0);
      yield* TestClock.adjust("1 minute");
      expect(yield* Queue.take(commands)).toMatchObject({
        type: "message.dispatch",
        threadId,
        text: "/compact",
        createdBy: "agent",
        creationSource: "server",
      });
    }).pipe(Effect.provide(dependencies));
  }).pipe(
    Effect.provide(ProjectionStore.layer.pipe(Layer.provide(SqlitePersistence.layerMemory))),
    Effect.scoped,
  ),
);
