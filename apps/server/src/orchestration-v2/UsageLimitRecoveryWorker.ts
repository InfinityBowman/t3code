import {
  CommandId,
  MessageId,
  type ModelSelection,
  type OrchestrationV2Command,
  type ProviderInstanceId,
  type RunId,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  ACCOUNT_MOVE_MIN_WAIT_MS,
  pickPoolAccount,
  poolAccountBlockedUntil,
} from "../provider/accountPool.ts";
import * as ProviderRegistry from "../provider/ProviderRegistry.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";

// Usage is probed only on demand, so an idle pooled account is re-probed this often to route on.
const POOL_USAGE_MAX_AGE_MS = 10 * 60 * 1000;
// A thread between turns leaves an account this full, before a limit would stop it mid-turn.
const NEARLY_FULL_PERCENT = 99;

const MOVE_TEXT = {
  limit:
    "The previous account reached its usage limit, so this thread moved to another account. Continue where you left off.",
  nearlyFull:
    "The previous account was nearly at its usage limit, so this thread moved to another account. Continue where you left off.",
  apart:
    "This thread moved to another account, so it never shares one with the other threads kept apart from it. Continue where you left off.",
} as const;
// The move starts a new process, which takes none of the old one's session state.
const MOVE_AFTER =
  " Your crons, watchers and background jobs ended with the previous process: re-create them from your notes and check them with CronList.";

/**
 * Moves a thread to another account in place. The switch ends a turn the old account still
 * holds and hands the thread's context to the new one; the queued message starts the next turn
 * there as soon as no turn is active.
 */
export function accountMoveCommands(input: {
  readonly threadId: ThreadId;
  readonly modelSelection: ModelSelection;
  readonly targetInstanceId: ProviderInstanceId;
  /** Names this move among the thread's moves: the run it cuts, or when it was made. */
  readonly key: string;
  readonly reason?: keyof typeof MOVE_TEXT;
}): readonly [OrchestrationV2Command, OrchestrationV2Command] {
  const identity = `${input.threadId}:${input.key}:${input.targetInstanceId}`;
  return [
    {
      type: "provider.switch",
      commandId: CommandId.make(`account-move-switch:${identity}`),
      threadId: input.threadId,
      modelSelection: { ...input.modelSelection, instanceId: input.targetInstanceId },
    },
    {
      type: "message.dispatch",
      commandId: CommandId.make(`account-move:${identity}`),
      messageId: MessageId.make(`account-move:${identity}`),
      threadId: input.threadId,
      text: MOVE_TEXT[input.reason ?? "limit"] + MOVE_AFTER,
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "user",
      creationSource: "server",
    },
  ];
}

export function limitRecoveryCommand(
  thread: ProjectionStore.ProjectionLimitRecoveryCandidate,
  autoResume: boolean,
  nowMs: number,
  snooze = false,
): OrchestrationV2Command | null {
  if (
    thread.status !== "failed" ||
    thread.lastErrorClass !== "usage_limit" ||
    !thread.latestRunId ||
    !thread.usageLimitResetAt ||
    thread.archivedAt !== null ||
    thread.settledOverride === "settled" ||
    thread.pendingRuntimeRequest !== null
  )
    return null;
  const resetMs = Date.parse(thread.usageLimitResetAt);
  // An already-expired window reported with a fresh failure cannot start a retry loop.
  if (
    !Number.isFinite(resetMs) ||
    resetMs <= DateTime.toEpochMillis(thread.latestRunCompletedAt ?? thread.updatedAt)
  )
    return null;
  const identity = `${thread.id}:${thread.latestRunId}:${resetMs}`;
  const recovery = thread.limitRecovery;
  if (recovery?.runId !== thread.latestRunId || recovery.resetAt !== thread.usageLimitResetAt) {
    if (!autoResume && (!snooze || resetMs <= nowMs)) return null;
    return {
      type: "thread.metadata.update",
      commandId: CommandId.make(`limit-arm:${identity}`),
      threadId: thread.id,
      limitRecovery: {
        runId: thread.latestRunId,
        resetAt: thread.usageLimitResetAt,
        autoResume,
        snooze: snooze && resetMs > nowMs,
      },
    };
  }
  if (
    !recovery.autoResume ||
    resetMs > nowMs ||
    (thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs)
  )
    return null;
  const deliveryIdentity = `${identity}:${recovery.requestId ?? "legacy"}`;
  return {
    type: "message.dispatch",
    commandId: CommandId.make(`limit-resume:${deliveryIdentity}`),
    messageId: MessageId.make(`limit-resume:${deliveryIdentity}`),
    threadId: thread.id,
    usageLimitContinuationOfRunId: thread.latestRunId,
    ...(recovery.requestId === undefined
      ? {}
      : { usageLimitRecoveryRequestId: recovery.requestId }),
    text: "Continue where you left off.",
    attachments: [],
    dispatchMode: { type: "start_immediately" },
    createdBy: "user",
    creationSource: "server",
  };
}

export const makeSweep = Effect.gen(function* () {
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const settings = yield* ServerSettings.ServerSettingsService;
  const registry = yield* ProviderRegistry.ProviderRegistry;
  const probedAt = new Map<ProviderInstanceId, number>();

  const dispatchAll = (threadId: ThreadId, commands: ReadonlyArray<OrchestrationV2Command>) =>
    Effect.forEach(commands, (command) => threads.dispatch(command), { discard: true }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("orchestration-v2.limit-recovery.dispatch-failed", { threadId, cause }),
      ),
    );

  const poolSnapshots = Effect.fn("UsageLimitRecoveryWorker.poolSnapshots")(function* (
    pool: ReadonlyArray<ProviderInstanceId>,
    nowMs: number,
  ) {
    const current = yield* registry.getProviders;
    for (const instanceId of pool) {
      const checkedAt = current.find((provider) => provider.instanceId === instanceId)?.usageLimits
        ?.checkedAt;
      const lastMs = Math.max(checkedAt ? Date.parse(checkedAt) : 0, probedAt.get(instanceId) ?? 0);
      if (nowMs - lastMs < POOL_USAGE_MAX_AGE_MS) continue;
      probedAt.set(instanceId, nowMs);
      yield* registry.refreshInstance(instanceId);
    }
    return yield* registry.getProviders;
  });

  return Effect.fn("UsageLimitRecoveryWorker.sweep")(function* () {
    const preferences = yield* settings.getSettings;
    const now = yield* DateTime.now;
    const nowMs = DateTime.toEpochMillis(now);
    const pool = preferences.accountPool.length > 1 ? preferences.accountPool : [];
    const moved = new Set<ThreadId>();
    if (pool.length > 0) {
      const providers = yield* poolSnapshots(pool, nowMs);
      const active = yield* projections.getActiveThreads({
        since: DateTime.subtract(now, { hours: 1 }),
      });
      const placed = new Map<ThreadId, ProviderInstanceId>();
      const accountOf = (thread: (typeof active)[number]) =>
        placed.get(thread.id) ?? thread.providerInstanceId;
      const apart = new Set(preferences.accountPoolApart);
      // The accounts holding another thread kept apart from this one.
      const apartFrom = (thread: (typeof active)[number]) =>
        apart.has(thread.id)
          ? active.filter((other) => other.id !== thread.id && apart.has(other.id)).map(accountOf)
          : [];
      const pick = (thread: (typeof active)[number], exclude: ProviderInstanceId) =>
        pickPoolAccount({
          pool,
          providers,
          model: thread.modelSelection.model,
          nowMs,
          exclude: [exclude, ...apartFrom(thread)],
        });
      const move = Effect.fn("move")(function* (
        thread: (typeof active)[number] | ProjectionStore.ProjectionLimitRecoveryCandidate,
        targetInstanceId: ProviderInstanceId,
        key: string,
        reason?: "nearlyFull" | "apart",
      ) {
        yield* dispatchAll(
          thread.id,
          accountMoveCommands({
            threadId: thread.id,
            modelSelection: thread.modelSelection,
            targetInstanceId,
            key,
            ...(reason === undefined ? {} : { reason }),
          }),
        );
        moved.add(thread.id);
        placed.set(thread.id, targetInstanceId);
      });
      // A limit that lifts soon is waited out; a long one moves the thread to an account with room.
      const longLimit = (current: ProviderInstanceId, resetMs: number) =>
        pool.includes(current) && resetMs - nowMs > ACCOUNT_MOVE_MIN_WAIT_MS;
      // Claude Code can hold a turn at a limit rather than fail it, so a running thread on a
      // used-up account is stuck until the reset.
      for (const thread of active) {
        if (thread.runningRunId === null) continue;
        const provider = providers.find((p) => p.instanceId === thread.providerInstanceId);
        const blockedUntil =
          provider && poolAccountBlockedUntil(provider, thread.modelSelection.model, nowMs);
        if (blockedUntil === undefined || !longLimit(thread.providerInstanceId, blockedUntil))
          continue;
        const targetInstanceId = pick(thread, thread.providerInstanceId);
        if (targetInstanceId === undefined) continue;
        yield* move(thread, targetInstanceId, thread.runningRunId);
      }
      const failed = yield* projections.getLimitRecoveryCandidates({
        now,
        autoResume: true,
        snooze: preferences.snoozeLimitedThreads,
      });
      for (const thread of failed) {
        if (moved.has(thread.id) || !thread.latestRunId || !thread.usageLimitResetAt) continue;
        if (!longLimit(thread.providerInstanceId, Date.parse(thread.usageLimitResetAt))) continue;
        const listed = active.find((candidate) => candidate.id === thread.id);
        const targetInstanceId = pickPoolAccount({
          pool,
          providers,
          model: thread.modelSelection.model,
          nowMs,
          exclude: [thread.providerInstanceId, ...(listed ? apartFrom(listed) : [])],
        });
        if (targetInstanceId === undefined) continue;
        yield* move(thread, targetInstanceId, thread.latestRunId);
      }
      // Threads kept apart that share an account: an idle one moves to an account with room and none of the
      // others, so the move cuts no turn.
      for (const thread of active) {
        if (!apart.has(thread.id) || thread.runningRunId !== null || moved.has(thread.id)) continue;
        if (!pool.includes(thread.providerInstanceId)) continue;
        if (!apartFrom(thread).includes(accountOf(thread))) continue;
        const targetInstanceId = pick(thread, thread.providerInstanceId);
        if (targetInstanceId === undefined) continue;
        yield* move(thread, targetInstanceId, `idle:${nowMs}`, "apart");
      }
      // Only a nearly used-up account moves threads, between turns so no turn is cut; 95% only steers new ones.
      for (const thread of active) {
        if (thread.runningRunId !== null || moved.has(thread.id)) continue;
        const provider = providers.find((p) => p.instanceId === thread.providerInstanceId);
        const fullUntil =
          provider &&
          poolAccountBlockedUntil(
            provider,
            thread.modelSelection.model,
            nowMs,
            NEARLY_FULL_PERCENT,
          );
        if (fullUntil === undefined || !longLimit(thread.providerInstanceId, fullUntil)) continue;
        const targetInstanceId = pick(thread, thread.providerInstanceId);
        if (targetInstanceId === undefined) continue;
        yield* move(thread, targetInstanceId, `idle:${nowMs}`, "nearlyFull");
      }
    }
    const candidates = yield* projections.getLimitRecoveryCandidates({
      now,
      autoResume: preferences.autoResumeLimitedThreads,
      snooze: preferences.snoozeLimitedThreads,
    });
    for (const thread of candidates) {
      if (moved.has(thread.id)) continue;
      const command = limitRecoveryCommand(
        thread,
        preferences.autoResumeLimitedThreads,
        nowMs,
        preferences.snoozeLimitedThreads,
      );
      if (command === null) continue;
      yield* Effect.gen(function* () {
        if (command.type === "thread.metadata.update" && command.limitRecovery != null) {
          yield* orchestrator.autoArmLimitRecovery({
            commandId: command.commandId,
            threadId: command.threadId,
            limitRecovery: command.limitRecovery,
          });
        } else {
          yield* threads.dispatch(command);
        }
      }).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("orchestration-v2.limit-recovery.dispatch-failed", {
            threadId: thread.id,
            cause,
          }),
        ),
      );
    }
  });
});

export const layer = Layer.effectDiscard(
  Effect.gen(function* () {
    const sweep = yield* makeSweep;
    const scheduler = yield* Scheduler.Scheduler;
    yield* scheduler.register("usage-limit-recovery", sweep());
  }),
);
