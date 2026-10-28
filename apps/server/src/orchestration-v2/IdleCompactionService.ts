import {
  CommandId,
  IDLE_COMPACTION_TEXT,
  MessageId,
  resolveIdleCompaction,
  resolveProjectProviderInstanceEnabled,
  type OrchestrationV2Run,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FiberMap from "effect/FiberMap";
import * as Stream from "effect/Stream";

import { poolAccountBlockedUntil } from "../provider/accountPool.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import { forkParked } from "../serverActivation.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  idleCompactionWindow,
  isKeepWarmRun,
  KEEP_WARM_MESSAGE_ID_PREFIX,
  KEEP_WARM_TEXT,
} from "./IdleCompaction.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

/**
 * Beta: when a run completes on a Claude instance set to compact before the
 * cache expires, waits until shortly before its prompt cache expires, then sends
 * `/compact` to a long thread, or a keep-warm ping to a shorter one or one waiting
 * on background work, as a visible T3 Code turn. Timers live in memory, so a
 * server restart skips the ones still waiting.
 */
export const make = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const settings = yield* ServerSettings.ServerSettingsService;
  const providers = yield* ProviderRegistry.ProviderRegistry;

  const compactWhenDue = Effect.fn("IdleCompactionService.compactWhenDue")(
    function* (threadId: ThreadId, runId: RunId) {
      // Loops once after the wait: the thread may have moved on.
      while (true) {
        const current = yield* settings.getSettings;
        const records = yield* projections.getThreadRecords(
          threadId,
          [
            "runs",
            "messages",
            "providerThreads",
            "providerTurns",
            "runtimeRequests",
            "turnItems",
            "contextTransfers",
          ],
          { messageRunIds: [runId], turnItemStatuses: ["pending", "running", "waiting"] },
        );
        if (
          resolveIdleCompaction(current, records.thread.providerInstanceId) !==
            "beforeCacheExpires" ||
          !resolveProjectProviderInstanceEnabled(
            current,
            records.thread.projectId,
            records.thread.providerInstanceId,
          )
        )
          return;
        const nowMs = DateTime.toEpochMillis(yield* DateTime.now);
        const window = idleCompactionWindow(records, nowMs);
        if (window?.runId !== runId) return;
        if (nowMs < window.startMs) {
          yield* Effect.sleep(Duration.millis(window.startMs - nowMs));
          continue;
        }
        if (window.action === "compact") {
          yield* orchestrator.dispatch({
            type: "message.dispatch",
            // One compaction per completed run, even across duplicate events.
            commandId: CommandId.make(`server:idle-compact:${threadId}:${runId}`),
            messageId: MessageId.make(`idle-compact:${threadId}:${runId}`),
            threadId,
            text: IDLE_COMPACTION_TEXT,
            attachments: [],
            createdBy: "agent",
            creationSource: "server",
            idleCompaction: "beforeCacheExpires",
            dispatchMode: { type: "start_immediately" },
          });
          return;
        }
        // A ping on a used-up account fails, notifies the phone and sends the thread to limit recovery.
        const provider = (yield* providers.getProviders).find(
          (candidate) => candidate.instanceId === records.thread.providerInstanceId,
        );
        if (
          provider !== undefined &&
          poolAccountBlockedUntil(provider, records.thread.modelSelection.model, nowMs) !==
            undefined
        )
          return;
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`server:keep-warm:${threadId}:${runId}`),
          messageId: MessageId.make(`${KEEP_WARM_MESSAGE_ID_PREFIX}${threadId}:${runId}`),
          threadId,
          text: KEEP_WARM_TEXT,
          attachments: [],
          createdBy: "agent",
          creationSource: "server",
          dispatchMode: { type: "start_immediately" },
        });
        return;
      }
    },
    (effect, threadId) =>
      effect.pipe(
        Effect.catch((cause) => Effect.logWarning("idle compaction skipped", { threadId, cause })),
      ),
  );

  // A ping's completion is nobody's news: a thread read before it stays read.
  const keepPingRead = Effect.fn("IdleCompactionService.keepPingRead")(
    function* (run: OrchestrationV2Run) {
      if (run.completedAt === null) return;
      const { thread, runs } = yield* projections.getThreadRecords(run.threadId, ["runs"]);
      const previous = runs
        .filter((candidate) => candidate.ordinal < run.ordinal && candidate.completedAt !== null)
        .reduce<OrchestrationV2Run | null>(
          (latest, candidate) =>
            latest === null || candidate.ordinal > latest.ordinal ? candidate : latest,
          null,
        );
      if (
        thread.lastVisitedAt === null ||
        previous?.completedAt == null ||
        DateTime.toEpochMillis(previous.completedAt) > DateTime.toEpochMillis(thread.lastVisitedAt)
      )
        return;
      yield* orchestrator.dispatch({
        type: "thread.visit",
        commandId: CommandId.make(`server:keep-warm-read:${run.id}`),
        threadId: run.threadId,
        visitedAt: DateTime.formatIso(run.completedAt),
      });
    },
    (effect, run) =>
      effect.pipe(
        Effect.catch((cause) =>
          Effect.logWarning("keep-warm read mark skipped", { threadId: run.threadId, cause }),
        ),
      ),
  );

  const start = Effect.fn("IdleCompactionService.start")(function* () {
    // Keyed by thread: a newer completed run replaces the waiting timer.
    const timers = yield* FiberMap.make<ThreadId>();
    yield* forkParked(
      Stream.runForEach(orchestrator.streamDomainEvents, (event) =>
        event.type === "run.updated" && event.payload.status === "completed"
          ? FiberMap.run(
              timers,
              event.threadId,
              Effect.andThen(
                isKeepWarmRun(event.payload) ? keepPingRead(event.payload) : Effect.void,
                compactWhenDue(event.threadId, event.payload.id),
              ),
            )
          : Effect.void,
      ),
    );
  });
  return { start };
});
