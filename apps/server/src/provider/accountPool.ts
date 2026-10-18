import type {
  ProviderInstanceId,
  ServerProvider,
  ServerProviderUsageWindow,
} from "@t3tools/contracts";

// The last few percent stay with the threads already on an account, so a new one starts elsewhere.
const SESSION_ROOM_PERCENT = 90;
const WEEKLY_ROOM_PERCENT = 95;
// Each active thread will keep spending, so a busy account counts as fuller than it reads and new
// threads go elsewhere before its threads would have to move. First guesses, to tune on real burn.
const SESSION_RESERVE_PER_ACTIVE_THREAD = 5;
const WEEKLY_RESERVE_PER_ACTIVE_THREAD = 2;

/** A limit lifting sooner than this is waited out: a move costs the thread its prompt cache. */
export const ACCOUNT_MOVE_MIN_WAIT_MS = 30 * 60 * 1000;

// Model-scoped weekly windows (`seven_day_fable`) bind only threads on that model.
function bindsModel(window: ServerProviderUsageWindow, model: string): boolean {
  const scope = /^seven_day_(.+)$/.exec(window.id)?.[1];
  return window.id === "seven_day" || scope === undefined || model.includes(scope);
}

// A window whose reset has passed has started over, whatever the snapshot last said.
function liveWindows(provider: ServerProvider, model: string, nowMs: number) {
  return (provider.usageLimits?.windows ?? []).filter(
    (window) =>
      bindsModel(window, model) &&
      (window.resetsAt === undefined || Date.parse(window.resetsAt) > nowMs),
  );
}

/** When the account can serve this model again, if a window is used up; otherwise undefined. */
export function poolAccountBlockedUntil(
  provider: ServerProvider,
  model: string,
  nowMs: number,
): number | undefined {
  const resets = liveWindows(provider, model, nowMs)
    .filter((window) => window.usedPercent >= 100 && window.resetsAt !== undefined)
    .map((window) => Date.parse(window.resetsAt!));
  return resets.length === 0 ? undefined : Math.max(...resets);
}

/**
 * The pooled account a thread on `model` should run on: of those signed in with room once
 * their active threads' reserve is counted, the one whose weekly window resets soonest, so
 * budget that is about to expire goes first.
 */
export function pickPoolAccount(input: {
  readonly pool: ReadonlyArray<ProviderInstanceId>;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly activeThreads: ReadonlyArray<{ readonly providerInstanceId: ProviderInstanceId }>;
  readonly model: string;
  readonly nowMs: number;
  readonly exclude?: ProviderInstanceId;
}): ProviderInstanceId | undefined {
  const withRoom = input.pool.flatMap((instanceId) => {
    const provider = input.providers.find((candidate) => candidate.instanceId === instanceId);
    if (
      instanceId === input.exclude ||
      provider === undefined ||
      !provider.enabled ||
      provider.status !== "ready" ||
      provider.auth.status !== "authenticated"
    )
      return [];
    const windows = liveWindows(provider, input.model, input.nowMs);
    const active = input.activeThreads.filter(
      (thread) => thread.providerInstanceId === instanceId,
    ).length;
    const full = windows.some((window) =>
      window.kind === "session"
        ? window.usedPercent + active * SESSION_RESERVE_PER_ACTIVE_THREAD >= SESSION_ROOM_PERCENT
        : window.usedPercent + active * WEEKLY_RESERVE_PER_ACTIVE_THREAD >= WEEKLY_ROOM_PERCENT,
    );
    if (full) return [];
    const weeklyReset = windows.find((window) => window.id === "seven_day")?.resetsAt;
    return [
      { instanceId, resetMs: weeklyReset === undefined ? Infinity : Date.parse(weeklyReset) },
    ];
  });
  // Stable sort keeps pool order among accounts with no known reset.
  return withRoom.toSorted((left, right) => left.resetMs - right.resetMs)[0]?.instanceId;
}
