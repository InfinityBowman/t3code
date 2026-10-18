// @effect-diagnostics nodeBuiltinImport:off - a standalone daemon with no Effect runtime, kept small so it rarely restarts.
// @effect-diagnostics globalDate:off - log timestamps only.
// @effect-diagnostics globalConsole:off - its log is launchd's stdout file.
/**
 * Holds Claude CLI processes so they outlive the T3 server: a server restart drops its connections, and the
 * sessions keep running, their output waiting here until the server attaches again.
 *
 * Usage: node apps/server/scripts/claude-keeper.ts <socket path>
 *
 * Each connection starts with one JSON line:
 * - {op:"attach", key, command, args, cwd, env}: joins the live process for `key`, or starts it from the rest.
 *   The connection then carries the process's stdin and stdout; the process's exit closes it.
 * - {op:"close", key}: ends the process's stdin, as a client closing the session on purpose does.
 * - {op:"kill", key, signal}: signals the process's group.
 * - {op:"list"}: answers one JSON line, [{key, pid}], for the live processes.
 * A connection that drops without one of these leaves its process running.
 */
import { spawn, type ChildProcess } from "node:child_process";
import * as fs from "node:fs";
import * as net from "node:net";

// Output waiting for a client past this is dropped; a restart takes seconds, so only a stuck server reaches it.
const MAX_PENDING_BYTES = 64 * 1024 * 1024;

interface Kept {
  readonly key: string;
  readonly child: ChildProcess;
  socket: net.Socket | null;
  pending: Array<string>;
  pendingBytes: number;
  partial: string;
}

const socketPath = process.argv[2];
if (socketPath === undefined) throw new Error("usage: claude-keeper.ts <socket path>");
const kept = new Map<string, Kept>();
const log = (...parts: Array<unknown>) => console.log(new Date().toISOString(), ...parts);

function signalGroup(entry: Kept, signal: NodeJS.Signals) {
  try {
    process.kill(-(entry.child.pid ?? 0), signal);
  } catch {}
}

function start(key: string, header: Record<string, unknown>): Kept {
  // Its own group, so the MCP servers' children it leaves behind can be killed with it.
  const child = spawn(String(header.command), header.args as Array<string>, {
    cwd: header.cwd as string | undefined,
    env: header.env as NodeJS.ProcessEnv,
    stdio: ["pipe", "pipe", "ignore"],
    detached: true,
  });
  const entry: Kept = { key, child, socket: null, pending: [], pendingBytes: 0, partial: "" };
  child.stdout!.setEncoding("utf8");
  // Whole lines only, so a client that attaches mid-stream never reads half a message.
  child.stdout!.on("data", (data: string) => {
    const text = entry.partial + data;
    const cut = text.lastIndexOf("\n");
    entry.partial = text.slice(cut + 1);
    if (cut < 0) return;
    const lines = text.slice(0, cut + 1);
    if (entry.socket !== null && !entry.socket.destroyed) {
      entry.socket.write(lines);
    } else if (entry.pendingBytes + lines.length <= MAX_PENDING_BYTES) {
      entry.pending.push(lines);
      entry.pendingBytes += lines.length;
    }
  });
  child.stdin!.on("error", () => {});
  child.once("exit", (code, signal) => {
    log("exited", key, child.pid, code ?? signal);
    if (kept.get(key) === entry) kept.delete(key);
    signalGroup(entry, "SIGKILL");
    entry.socket?.end();
  });
  child.once("error", (error) => {
    log("failed to start", key, error.message);
    if (kept.get(key) === entry) kept.delete(key);
    entry.socket?.destroy();
  });
  kept.set(key, entry);
  log("started", key, child.pid);
  return entry;
}

function attach(socket: net.Socket, header: Record<string, unknown>, rest: Buffer) {
  const key = String(header.key);
  const entry = kept.get(key) ?? start(key, header);
  if (entry.socket !== null && entry.socket !== socket) entry.socket.destroy();
  entry.socket = socket;
  if (entry.pending.length > 0)
    log("attached", key, entry.child.pid, "replaying", entry.pendingBytes, "bytes");
  for (const lines of entry.pending.splice(0)) socket.write(lines);
  entry.pendingBytes = 0;
  if (rest.length > 0) entry.child.stdin!.write(rest);
  socket.on("data", (chunk) => entry.child.stdin!.write(chunk));
  socket.on("close", () => {
    if (entry.socket === socket) entry.socket = null;
  });
}

function control(socket: net.Socket, header: Record<string, unknown>) {
  const entry = kept.get(String(header.key));
  if (header.op === "list") {
    socket.end(
      JSON.stringify(
        Array.from(kept.values(), (live) => ({ key: live.key, pid: live.child.pid })),
      ) + "\n",
    );
    return;
  }
  if (entry !== undefined && header.op === "close") {
    // A later attach for this key starts a new process rather than joining the one that is ending.
    kept.delete(entry.key);
    entry.child.stdin!.end();
  }
  if (entry !== undefined && header.op === "kill") {
    kept.delete(entry.key);
    signalGroup(entry, (header.signal as NodeJS.Signals | undefined) ?? "SIGTERM");
  }
  socket.end();
}

try {
  fs.unlinkSync(socketPath);
} catch {}
net
  .createServer({ allowHalfOpen: true }, (socket) => {
    let header = Buffer.alloc(0);
    socket.on("error", () => {});
    const readHeader = (chunk: Buffer) => {
      header = Buffer.concat([header, chunk]);
      const newline = header.indexOf(10);
      if (newline < 0) return;
      socket.off("data", readHeader);
      let parsed: Record<string, unknown>;
      try {
        parsed = JSON.parse(header.subarray(0, newline).toString("utf8"));
      } catch {
        socket.destroy();
        return;
      }
      if (parsed.op === "attach") attach(socket, parsed, header.subarray(newline + 1));
      else control(socket, parsed);
    };
    socket.on("data", readHeader);
  })
  .listen(socketPath, () => {
    fs.chmodSync(socketPath, 0o600);
    log("listening", socketPath);
  });
