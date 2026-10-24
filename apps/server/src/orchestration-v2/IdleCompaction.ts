import type { OrchestrationV2ThreadProjection } from "@t3tools/contracts";
import { COMPACTION_SUGGESTION_MIN_TOKENS } from "@t3tools/shared/claudeCompaction";
import { derivePendingBackgroundWork } from "@t3tools/shared/orchestrationV2PendingBackgroundWork";
import * as DateTime from "effect/DateTime";

// Claude Code's idleCompaction defaults to 200K and won't go below 100K. Below
// that a cold re-read is cheap, so compacting mostly loses context for little
// saving.
const MIN_CONTEXT_TOKENS = COMPACTION_SUGGESTION_MIN_TOKENS;
// Start this long before the cache expires: enough for the session to resume
// and send the compaction while the cache is still warm.
const LEAD_MS = 2 * 60_000;
// A thread nobody has woken this long lets its cache lapse.
const KEEP_WARM_MAX_IDLE_MS = 6 * 60 * 60_000;
// A ping costs a tenth of the context per cache lifetime: worth it each hour, not every five minutes.
const KEEP_WARM_MIN_TTL_MS = 60 * 60_000;

// The model may also see a background report that landed with the ping, so it must not drop one.
export const KEEP_WARM_TEXT =
  'Keep-warm ping from T3 to hold the prompt cache: reply with just "ok" and take no action. If a background task reports in with this ping, handle that report as usual instead.';
// Marks the ping's message, so its turn never counts as someone's activity.
export const KEEP_WARM_MESSAGE_ID_PREFIX = "keep-warm:";

export function isKeepWarmRun(run: { readonly userMessageId: string }): boolean {
  return run.userMessageId.startsWith(KEEP_WARM_MESSAGE_ID_PREFIX);
}

export type IdleCompactionRecords = Pick<
  OrchestrationV2ThreadProjection,
  | "thread"
  | "runs"
  | "messages"
  | "providerThreads"
  | "providerTurns"
  | "runtimeRequests"
  | "turnItems"
  | "contextTransfers"
>;

/**
 * When an idle Claude thread should compact or have its cache kept warm, or
 * null when neither. The window ends when the prompt cache of Claude's last
 * request expires (1h on subscriptions, 5m on API keys); after that the next
 * turn pays a full uncached read.
 */
export function idleCompactionWindow(
  {
    thread,
    runs,
    messages,
    providerThreads,
    providerTurns,
    runtimeRequests,
    turnItems,
    contextTransfers,
  }: IdleCompactionRecords,
  nowMs: number,
): {
  readonly action: "compact" | "keep_warm";
  readonly runId: (typeof runs)[number]["id"];
  readonly startMs: number;
  readonly endMs: number;
} | null {
  if (thread.deletedAt !== null || thread.archivedAt !== null) return null;
  if (thread.settledAt !== null || thread.settledOverride === "settled") return null;
  if (thread.snoozedUntil != null && DateTime.toEpochMillis(thread.snoozedUntil) > nowMs)
    return null;
  if (thread.lineage.relationshipToParent === "subagent") return null;
  const latestRun = runs.reduce<(typeof runs)[number] | null>(
    (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
    null,
  );
  if (latestRun?.status !== "completed" || latestRun.completedAt === null) return null;
  const message = messages.find((candidate) => candidate.id === latestRun.userMessageId);
  // Never follow a compaction: one idle spell compacts at most once, and its
  // summary isn't cached yet, so a ping would pay the write coming back pays.
  if (message === undefined || message.text.trim().toLowerCase() === "/compact") return null;
  if (
    runtimeRequests.some((request) => request.status === "pending") ||
    // A pending merge-back belongs to the user's next message.
    contextTransfers.some(
      (transfer) =>
        transfer.type === "merge_back" &&
        transfer.targetThreadId === thread.id &&
        transfer.status === "pending",
    )
  )
    return null;
  const providerThread = providerThreads.find(
    (candidate) => candidate.id === thread.activeProviderThreadId,
  );
  // The thread's current selection receives the turn, so it must still be this Claude session.
  if (
    providerThread?.driver !== "claudeAgent" ||
    providerThread.id !== latestRun.providerThreadId ||
    providerThread.providerInstanceId !== thread.providerInstanceId ||
    latestRun.providerInstanceId !== thread.providerInstanceId
  )
    return null;
  const usage = providerTurns
    .filter((turn) => turn.providerThreadId === providerThread.id && turn.tokenUsage !== undefined)
    .map((turn) => turn.tokenUsage!)
    .toSorted((left, right) => Date.parse(right.updatedAt) - Date.parse(left.updatedAt))[0];
  // A request served fully from cache creates no cache entry and reports no
  // TTL, but it still refreshes the cache. Treat it as unknown and skip.
  const endMs =
    usage?.promptCacheExpiresAt === undefined ? Number.NaN : Date.parse(usage.promptCacheExpiresAt);
  if (!Number.isFinite(endMs) || nowMs >= endMs) return null;
  const startMs = endMs - LEAD_MS;
  // Background work reports into the next turn and compaction would swallow
  // it, but a ping leaves it to arrive as usual.
  const backgroundWork =
    derivePendingBackgroundWork({
      latestRun,
      runs,
      providerThreads,
      turnItems,
      activeProviderThreadId: thread.activeProviderThreadId,
    }).length > 0;
  if (!backgroundWork && (usage?.usedTokens ?? 0) >= MIN_CONTEXT_TOKENS)
    return { action: "compact", runId: latestRun.id, startMs, endMs };
  const lastRealRun = runs
    .filter((run) => !isKeepWarmRun(run))
    .reduce<(typeof runs)[number] | null>(
      (latest, run) => (latest === null || run.ordinal > latest.ordinal ? run : latest),
      null,
    );
  const idleSince = lastRealRun?.completedAt ?? lastRealRun?.requestedAt;
  if (
    idleSince === undefined ||
    startMs - DateTime.toEpochMillis(idleSince) >= KEEP_WARM_MAX_IDLE_MS ||
    (usage?.promptCacheTtlMs ?? 0) < KEEP_WARM_MIN_TTL_MS
  )
    return null;
  return { action: "keep_warm", runId: latestRun.id, startMs, endMs };
}
