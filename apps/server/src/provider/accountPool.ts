// @effect-diagnostics nodeBuiltinImport:off - session counting stats Claude transcripts synchronously inside pure routing.
import * as fs from "node:fs";
import * as os from "node:os";
import * as path from "node:path";

import type {
  ProviderInstanceId,
  ServerProvider,
  ServerProviderUsageWindow,
} from "@t3tools/contracts";

// An account has room while its projected use at each window's reset stays at or under these.
const ROOM_PERCENT = { session: 90, weekly: 95 } as const;
// Burn per active session (points an hour), a floor under a window's average. On Oct 19, account B spent about
// 20 five-hour points an hour across some 13 sessions, and about 0.6 weekly points an hour across some 10; a busy
// Oct 18 on A ran 1.5 across nine. A weekly floor near the busy rate would fill a freshly reset account after
// three sessions, a week out from its reset.
const SESSION_BURN_PER_HOUR = { session: 2, weekly: 0.05 } as const;
// A window burns at its use so far over the hours it has been open, at least these: a heavy start still reads
// as heavy, but a first hour isn't projected to a reset days out as the rate.
const MIN_OPEN_HOURS = { session: 1, weekly: 24 } as const;
// A session counts as active while its transcript was written this recently.
const ACTIVE_SESSION_MS = 15 * 60 * 1000;
const SESSION_COUNT_TTL_MS = 60 * 1000;

/** A limit lifting sooner than this is waited out: a move costs the thread its prompt cache. */
export const ACCOUNT_MOVE_MIN_WAIT_MS = 30 * 60 * 1000;

type Kind = keyof typeof ROOM_PERCENT;
const kindOf = (window: ServerProviderUsageWindow): Kind =>
  window.kind === "session" ? "session" : "weekly";

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

/** A Claude instance's config folder: its homePath, else Claude Code's default. */
export function claudeHome(homePath: unknown): string {
  const configured = typeof homePath === "string" ? homePath.trim() : "";
  if (configured === "") return path.join(os.homedir(), ".claude");
  return path.resolve(configured.replace(/^~(?=$|\/)/, os.homedir()));
}

const sessionCounts = new Map<string, { readonly atMs: number; readonly count: number }>();
/**
 * Claude sessions active on the account whose config folder is `home`, T3 threads and terminal tabs alike:
 * every top-level transcript written in the last 15 minutes.
 */
export function activeSessions(home: string, nowMs: number): number {
  const cached = sessionCounts.get(home);
  if (cached !== undefined && nowMs - cached.atMs < SESSION_COUNT_TTL_MS) return cached.count;
  let count = 0;
  const projects = path.join(home, "projects");
  for (const project of fs.existsSync(projects) ? fs.readdirSync(projects) : []) {
    const directory = path.join(projects, project);
    let files: Array<string> = [];
    try {
      files = fs.readdirSync(directory);
    } catch {
      continue;
    }
    for (const file of files) {
      if (!file.endsWith(".jsonl")) continue;
      try {
        if (nowMs - fs.statSync(path.join(directory, file)).mtimeMs <= ACTIVE_SESSION_MS) count++;
      } catch {}
    }
  }
  sessionCounts.set(home, { atMs: nowMs, count });
  return count;
}

export interface PoolLoad {
  readonly sessions: ReadonlyMap<ProviderInstanceId, number>;
}

/** The pool's load now, from each instance's configured home. */
export function poolLoad(
  pool: ReadonlyArray<ProviderInstanceId>,
  instances: Readonly<Record<string, { readonly config?: unknown } | undefined>>,
  nowMs: number,
): PoolLoad {
  const homePath = (instanceId: ProviderInstanceId) =>
    (instances[instanceId]?.config as { readonly homePath?: unknown } | undefined)?.homePath;
  return {
    sessions: new Map(
      pool.map((instanceId) => [
        instanceId,
        activeSessions(claudeHome(homePath(instanceId)), nowMs),
      ]),
    ),
  };
}

/**
 * How far past its room this account ends at each window's reset, with `extra` more sessions (fewer when
 * negative): the worst window's projected use minus its room, so a value over zero means it runs past it.
 * Burn is the window's average so far, never under its sessions' floor; each extra session adds an average
 * session's share. A window not yet opened opens with the next request and runs its full length.
 */
export function poolAccountOverrun(
  provider: ServerProvider,
  model: string,
  nowMs: number,
  load: PoolLoad,
  extra: number,
): number {
  const sessions = load.sessions.get(provider.instanceId) ?? 0;
  const overruns = liveWindows(provider, model, nowMs).map((window) => {
    const kind = kindOf(window);
    const hours =
      window.resetsAt !== undefined
        ? (Date.parse(window.resetsAt) - nowMs) / 3_600_000
        : (window.windowDurationMins ?? 0) / 60;
    const openHours = (window.windowDurationMins ?? 0) / 60 - hours;
    const average = window.usedPercent / Math.max(openHours, MIN_OPEN_HOURS[kind]);
    const floor = SESSION_BURN_PER_HOUR[kind];
    const burn = Math.max(average, sessions * floor);
    const perSession = Math.max(floor, sessions > 0 ? average / sessions : 0);
    return window.usedPercent + Math.max(0, burn + extra * perSession) * hours - ROOM_PERCENT[kind];
  });
  return Math.max(-Infinity, ...overruns);
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
 * The pooled account a new thread on `model` should run on: of those that stay within their room at every
 * reset with it added, the one whose weekly window resets soonest, so budget about to expire goes first.
 * `added` counts sessions already moved onto an account this sweep. With `fallback`, when none has room,
 * the one that overruns least; otherwise undefined.
 */
export function pickPoolAccount(input: {
  readonly pool: ReadonlyArray<ProviderInstanceId>;
  readonly providers: ReadonlyArray<ServerProvider>;
  readonly load: PoolLoad;
  readonly model: string;
  readonly nowMs: number;
  readonly exclude?: ReadonlyArray<ProviderInstanceId>;
  readonly added?: ReadonlyMap<ProviderInstanceId, number>;
  readonly fallback?: boolean;
}): ProviderInstanceId | undefined {
  const candidates = input.pool.flatMap((instanceId) => {
    const provider = input.providers.find((candidate) => candidate.instanceId === instanceId);
    if (input.exclude?.includes(instanceId) || !servable(provider)) return [];
    const weeklyReset = liveWindows(provider, input.model, input.nowMs).find(
      (window) => window.id === "seven_day",
    )?.resetsAt;
    return [
      {
        instanceId,
        overrun: poolAccountOverrun(
          provider,
          input.model,
          input.nowMs,
          input.load,
          1 + (input.added?.get(instanceId) ?? 0),
        ),
        resetMs: weeklyReset === undefined ? Infinity : Date.parse(weeklyReset),
      },
    ];
  });
  // Stable sorts keep pool order among accounts with no known reset.
  const withRoom = candidates.filter((candidate) => candidate.overrun <= 0);
  if (withRoom.length > 0)
    return withRoom.toSorted((left, right) => left.resetMs - right.resetMs)[0]!.instanceId;
  return input.fallback === true
    ? candidates.toSorted((left, right) => left.overrun - right.overrun)[0]?.instanceId
    : undefined;
}
