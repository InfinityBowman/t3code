// @effect-diagnostics nodeBuiltinImport:off - the SDK's custom spawn contract is Node streams and events.
/**
 * Client for scripts/claude-keeper.ts, which holds Claude CLI processes so a server restart leaves them running.
 * Set T3CODE_CLAUDE_KEEPER_SOCKET to its socket to launch Claude through it; unset, Claude runs as the server's
 * own child.
 */
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

/** The session ids the keeper holds a live process for. */
export function listClaudeKeeperSessions(socketPath: string): Promise<ReadonlySet<string>> {
  return new Promise((resolve) => {
    const socket = net.connect(socketPath);
    let answer = "";
    socket.setEncoding("utf8");
    socket.on("data", (chunk: string) => (answer += chunk));
    socket.on("error", () => resolve(new Set()));
    socket.on("close", () => {
      try {
        const live = JSON.parse(answer) as Array<{ readonly key: string }>;
        resolve(new Set(live.map((entry) => entry.key)));
      } catch {
        resolve(new Set());
      }
    });
    socket.end(JSON.stringify({ op: "list" }) + "\n");
  });
}

/** The sessions the keeper holds now; none without a keeper. */
export function claudeKeeperSessions(): Promise<ReadonlySet<string>> {
  const socket = claudeKeeperSocket();
  return socket === undefined ? Promise.resolve(new Set()) : listClaudeKeeperSessions(socket);
}

let sessionsAtStartup: Promise<ReadonlySet<string>> | undefined;
/**
 * The sessions the keeper held when this server started, read once before the server opens any: their
 * processes, turns, background work and crons carried over from the previous server.
 */
export function claudeKeeperSessionsAtStartup(): Promise<ReadonlySet<string>> {
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
    const end = () => {
      if (!detaching) sendControl(socketPath, { op: "close", key });
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
