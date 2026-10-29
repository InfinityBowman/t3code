import type {
  ProviderInstanceId,
  ServerProvider,
  ServerProviderUsageWindow,
} from "@t3tools/contracts";

// An account with any window at this use is skipped while another has room.
const FULL_PERCENT = 90;

/** A limit lifting sooner than this is waited out: a move costs the thread its prompt cache. */
export const ACCOUNT_MOVE_MIN_WAIT_MS = 30 * 60 * 1000;

// Model-scoped weekly windows (`seven_day_fable`) bind only threads on that model.
function bindsModel(window: ServerProviderUsageWindow, model: string): boolean {
  const scope = /^seven_day_(.+)$/.exec(window.id)?.[1];
  return window.id === "seven_day" || scope === undefined || model.includes(scope);
}

// A window whose reset has passed has started over, whatever the snapshot last said: empty, and not yet opened.
function liveWindows(provider: ServerProvider, model: string, nowMs: number) {
  return (provider.usageLimits?.windows ?? [])
    .filter((window) => bindsModel(window, model))
    .map((window) =>
      window.resetsAt !== undefined && Date.parse(window.resetsAt) <= nowMs
        ? { ...window, usedPercent: 0, resetsAt: undefined }
        : window,
    );
}

/** When a window at `atPercent` or past it resets (by default, when a used-up account can serve again). */
export function poolAccountBlockedUntil(
  provider: ServerProvider,
  model: string,
  nowMs: number,
  atPercent = 100,
): number | undefined {
  const resets = liveWindows(provider, model, nowMs)
    .filter((window) => window.usedPercent >= atPercent && window.resetsAt !== undefined)
    .map((window) => Date.parse(window.resetsAt!));
  return resets.length === 0 ? undefined : Math.max(...resets);
}

function servable(provider: ServerProvider | undefined): provider is ServerProvider {
  return (
    provider !== undefined &&
    provider.enabled &&
    provider.status === "ready" &&
    provider.auth.status === "authenticated"
  );
}

/**
 * The pooled account a thread on `model` should go to: of those with every window under 90%, the one whose
 * weekly window resets soonest, so budget about to expire goes first. With `fallback`, when none is under
 * 90%, the one whose fullest window is lowest; otherwise undefined.
 */
export function pickPoolAccount(input: {
  readonly pool: ReadonlyArray<ProviderInstanceId>;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly model: string;
  readonly nowMs: number;
  readonly exclude?: ReadonlyArray<ProviderInstanceId>;
  readonly fallback?: boolean;
}): ProviderInstanceId | undefined {
  const candidates = input.pool.flatMap((instanceId) => {
    const provider = input.providers.find((candidate) => candidate.instanceId === instanceId);
    if (input.exclude?.includes(instanceId) || !servable(provider)) return [];
    const windows = liveWindows(provider, input.model, input.nowMs);
    const weeklyReset = windows.find((window) => window.id === "seven_day")?.resetsAt;
    return [
      {
        instanceId,
        used: Math.max(0, ...windows.map((window) => window.usedPercent)),
        resetMs: weeklyReset === undefined ? Infinity : Date.parse(weeklyReset),
      },
    ];
  });
  // Stable sorts keep pool order among accounts with no known reset.
  const withRoom = candidates.filter((candidate) => candidate.used < FULL_PERCENT);
  if (withRoom.length > 0)
    return withRoom.toSorted((left, right) => left.resetMs - right.resetMs)[0]!.instanceId;
  return input.fallback === true
    ? candidates.toSorted((left, right) => left.used - right.used)[0]?.instanceId
    : undefined;
}
