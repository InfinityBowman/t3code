// @effect-diagnostics nodeBuiltinImport:off - the SDK's custom spawn contract is Node streams and events.
// @effect-diagnostics globalDate:off - dates keeper processes from ps, outside any Effect runtime.
/**
 * Client for scripts/claude-keeper.ts, which holds Claude CLI processes so a server restart leaves them running.
 * Set T3CODE_CLAUDE_KEEPER_SOCKET to its socket to launch Claude through it; unset, Claude runs as the server's
 * own child.
 */
import * as NodeChildProcess from "node:child_process";
import { EventEmitter } from "node:events";
import * as fs from "node:fs";
import * as net from "node:net";
import { Writable } from "node:stream";

import type { SpawnedProcess, SpawnOptions } from "@anthropic-ai/claude-agent-sdk";

export function claudeKeeperSocket(): string | undefined {
  const socket = process.env.T3CODE_CLAUDE_KEEPER_SOCKET?.trim();
  return socket === undefined || socket === "" || !fs.existsSync(socket) ? undefined : socket;
}

// Set once the server starts shutting down: closing its queries then only drops their connections, so the
// processes outlive it and the next server attaches to them.
let detaching = false;
export function detachClaudeKeeperClients(): void {
  detaching = true;
}

function sendControl(socketPath: string, message: Record<string, unknown>): void {
  const socket = net.connect(socketPath);
  socket.on("error", () => {});
  socket.end(JSON.stringify(message) + "\n");
}

/** Ends the keeper's process for one session, and the process group it leads. */
export function stopClaudeKeeperSession(socketPath: string, key: string): void {
  sendControl(socketPath, { op: "kill", key, signal: "SIGTERM" });
}

/**
 * The session ids the keeper holds a live process for, each with when that process started (0 if unknown). A
 * session can run in several processes over its life, and work from an earlier one did not survive it.
 */
export function listClaudeKeeperSessions(socketPath: string): Promise<ReadonlyMap<string, number>> {
  return new Promise((resolve) => {
    const socket = net.connect(socketPath);
    let answer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => (answer += chunk));
    socket.on("error", () => resolve(new Map()));
    socket.on("close", () => {
      let live: Array<{ readonly key: string; readonly pid: number }>;
      try {
        live = JSON.parse(answer);
      } catch {
        resolve(new Map());
        return;
      }
      void processStarts(live.map((entry) => entry.pid)).then((starts) =>
        resolve(new Map(live.map((entry) => [entry.key, starts.get(entry.pid) ?? 0]))),
      );
    });
    socket.end(JSON.stringify({ op: "list" }) + "\n");
  });
}

// The keeper reports pids only; ps's elapsed time, [[dd-]hh:]mm:ss, dates them on macOS and Linux alike.
function processStarts(pids: ReadonlyArray<number>): Promise<ReadonlyMap<number, number>> {
  if (pids.length === 0) return Promise.resolve(new Map());
  return new Promise((resolve) => {
    NodeChildProcess.execFile(
      "ps",
      ["-o", "pid=,etime=", "-p", pids.join(",")],
      (error, stdout) => {
        const now = Date.now();
        const starts = new Map<number, number>();
        for (const line of error === null ? stdout.split("\n") : []) {
          const match = /^\s*(\d+)\s+(?:(\d+)-)?(?:(\d+):)?(\d+):(\d+)\s*$/.exec(line);
          if (match === null) continue;
          const [, pid, days = "0", hours = "0", minutes, seconds] = match;
          const elapsed =
            ((Number(days) * 24 + Number(hours)) * 60 + Number(minutes)) * 60 + Number(seconds);
          starts.set(Number(pid), now - elapsed * 1000);
        }
        resolve(starts);
      },
    );
  });
}

/** The sessions the keeper holds now; none without a keeper. */
export function claudeKeeperSessions(): Promise<ReadonlyMap<string, number>> {
  const socket = claudeKeeperSocket();
  return socket === undefined ? Promise.resolve(new Map()) : listClaudeKeeperSessions(socket);
}

let sessionsAtStartup: Promise<ReadonlyMap<string, number>> | undefined;
/**
 * The sessions the keeper held when this server started, read once before the server opens any: their
 * processes, turns, background work and crons carried over from the previous server.
 */
export function claudeKeeperSessionsAtStartup(): Promise<ReadonlyMap<string, number>> {
  sessionsAtStartup ??= claudeKeeperSessions();
  return sessionsAtStartup;
}

const reattached = new Set<string>();
/** True the first time this server opens a session the keeper held at its start. */
export async function takeClaudeKeeperReattach(key: string): Promise<boolean> {
  if (!(await claudeKeeperSessionsAtStartup()).has(key) || reattached.has(key)) return false;
  reattached.add(key);
  return true;
}

/**
 * The SDK's spawn for one Claude session, keyed by its session id: it joins the keeper's live process for that
 * session, or has the keeper start one from these options.
 */
export function spawnThroughClaudeKeeper(socketPath: string, key: string) {
  return (options: SpawnOptions): SpawnedProcess => {
    const socket = net.connect({ path: socketPath, allowHalfOpen: true });
    const events = new EventEmitter();
    let exitCode: number | null = null;
    let signalCode: NodeJS.Signals | null = null;
    let killed = false;
    socket.write(
      JSON.stringify({
        op: "attach",
        key,
        command: options.command,
        args: options.args,
        cwd: options.cwd,
        env: options.env,
      }) + "\n",
    );
    socket.on("error", (error) => events.emit("error", error));
    // The keeper ends the connection once the process exits.
    socket.on("end", () => socket.destroy());
    socket.on("close", () => {
      exitCode = signalCode === null ? 0 : null;
      events.emit("exit", exitCode, signalCode);
    });
    // A session the server ends stops at once: ending its stdin alone lets one busy with subagents or
    // background jobs work on unseen, beside the session that replaced it.
    const end = () => {
      if (!detaching) sendControl(socketPath, { op: "kill", key, signal: "SIGTERM" });
    };
    // Its own stream, so the SDK ending stdin closes the session through the keeper instead of half-closing the
    // connection, which a crashing server does too.
    const stdin = new Writable({
      write(chunk, _encoding, callback) {
        socket.write(chunk, callback);
      },
      final(callback) {
        end();
        callback();
      },
    });
    options.signal?.addEventListener("abort", () => {
      if (!detaching) sendControl(socketPath, { op: "kill", key, signal: "SIGTERM" });
    });
    return {
      stdin,
      stdout: socket,
      get killed() {
        return killed;
      },
      get exitCode() {
        return exitCode;
      },
      get signalCode() {
        return signalCode;
      },
      kill(signal) {
        killed = true;
        signalCode = signal;
        if (detaching) socket.destroy();
        else sendControl(socketPath, { op: "kill", key, signal });
        return true;
      },
      on: events.on.bind(events),
      once: events.once.bind(events),
      off: events.off.bind(events),
    } as SpawnedProcess;
  };
}
