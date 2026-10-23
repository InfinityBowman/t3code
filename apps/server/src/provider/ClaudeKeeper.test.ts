// @effect-diagnostics nodeBuiltinImport:off - drives the keeper daemon over its socket, as the server does.
// @effect-diagnostics globalDate:off - checks the wall-clock start the keeper client reports.
import { spawn } from "node:child_process";
import * as fs from "node:fs";
import type * as net from "node:net";
import * as os from "node:os";
import * as path from "node:path";
import type { Readable } from "node:stream";

import { expect, it } from "vite-plus/test";

import { listClaudeKeeperSessions, spawnThroughClaudeKeeper } from "./ClaudeKeeper.ts";

const KEEPER = path.join(import.meta.dirname, "../../scripts/claude-keeper.ts");

const readLine = (stream: Readable) =>
  new Promise<string>((resolve) => {
    let text = "";
    const onData = (chunk: Buffer | string) => {
      text += String(chunk);
      const newline = text.indexOf("\n");
      if (newline < 0) return;
      stream.off("data", onData);
      resolve(text.slice(0, newline));
    };
    stream.on("data", onData);
  });

it("keeps a session's process through a dropped connection and stops it when closed", async () => {
  const socket = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "keeper-")), "k.sock");
  const keeper = spawn(process.execPath, [KEEPER, socket], {
    stdio: ["ignore", "pipe", "inherit"],
  });
  try {
    await new Promise<void>((resolve) =>
      keeper.stdout.on("data", (chunk) => String(chunk).includes("listening") && resolve()),
    );
    // Like a session busy with subagents, it keeps working after its stdin ends.
    const options = {
      command: "sh",
      args: ["-c", "cat; sleep 30"],
      env: process.env,
      cwd: os.tmpdir(),
      signal: new AbortController().signal,
    };

    const first = spawnThroughClaudeKeeper(socket, "session")(options);
    first.stdin.write("one\n");
    expect(await readLine(first.stdout)).toBe("one");
    // A server that dies sends nothing first.
    (first.stdout as net.Socket).destroy();
    const listed = await listClaudeKeeperSessions(socket);
    expect([...listed.keys()]).toEqual(["session"]);
    expect(Math.abs(Date.now() - listed.get("session")!)).toBeLessThan(10_000);

    const second = spawnThroughClaudeKeeper(socket, "session")(options);
    second.stdin.write("two\n");
    expect(await readLine(second.stdout)).toBe("two");

    const exited = new Promise((resolve) => second.once("exit", resolve));
    second.stdin.end();
    await exited;
    expect((await listClaudeKeeperSessions(socket)).size).toBe(0);
  } finally {
    keeper.kill();
  }
});
