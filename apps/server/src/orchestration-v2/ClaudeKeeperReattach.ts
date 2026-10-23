import { CommandId, MessageId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";

import {
  claudeKeeperSessionsAtStartup,
  claudeKeeperSocket,
  listClaudeKeeperSessions,
  stopClaudeKeeperSession,
} from "../provider/ClaudeKeeper.ts";
import * as Orchestrator from "./Orchestrator.ts";

/**
 * After a restart, each thread whose Claude session the keeper held gets a provider continuation, which prompts
 * nothing: it attaches the server to the live process, takes in what the session produced meanwhile, and routes
 * its later output (the rest of a cut turn, a cron firing, a background task ending) back to the thread.
 */
export const reattachClaudeKeeperSessions = Effect.gen(function* () {
  const kept = yield* Effect.promise(claudeKeeperSessionsAtStartup);
  if (kept.size === 0) return;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const startedAt = DateTime.toEpochMillis(yield* DateTime.now);
  const { threads } = yield* orchestrator.getShellSnapshot();
  for (const shell of threads) {
    if (shell.activeProviderThreadId === null || shell.archivedAt !== null) continue;
    const { providerThreads } = yield* orchestrator.getThreadRecords(shell.id, ["providerThreads"]);
    const nativeId = providerThreads.find(
      (candidate) => candidate.id === shell.activeProviderThreadId,
    )?.nativeThreadRef?.nativeId;
    if (nativeId == null || !kept.has(nativeId)) continue;
    const key = `${shell.id}:${startedAt}`;
    yield* orchestrator
      .dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(`command:keeper-reattach:${key}`),
        threadId: shell.id,
        messageId: MessageId.make(`message:keeper-reattach:${key}`),
        text: "Reconnected after a T3 Code restart.",
        attachments: [],
        dispatchMode: { type: "queue_after_active" },
        createdBy: "agent",
        // "provider" marks a continuation, which ClaudeAdapterV2 runs without prompting the session.
        creationSource: "provider",
      })
      .pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("orchestration-v2.keeper-reattach-failed", {
            threadId: shell.id,
            cause,
          }),
        ),
      );
  }
});

/**
 * Stops keeper processes that no thread runs on, such as a session a move to another account left behind
 * because it had already ended at a limit: alive, it keeps answering messages and firing crons under the
 * thread's name. One must look orphaned on two passes in a row, so a session just starting isn't taken.
 */
export const reapClaudeKeeperOrphans = Effect.gen(function* () {
  const socket = claudeKeeperSocket();
  if (socket === undefined) return;
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  let suspects = new Set<string>();
  const pass = Effect.gen(function* () {
    const kept = yield* Effect.promise(() => listClaudeKeeperSessions(socket));
    const live = new Set<string>();
    const { threads } = yield* orchestrator.getShellSnapshot();
    for (const shell of threads) {
      if (shell.activeProviderThreadId === null || shell.archivedAt !== null) continue;
      const { providerThreads } = yield* orchestrator.getThreadRecords(shell.id, [
        "providerThreads",
      ]);
      const nativeId = providerThreads.find(
        (candidate) => candidate.id === shell.activeProviderThreadId,
      )?.nativeThreadRef?.nativeId;
      if (nativeId != null) live.add(nativeId);
    }
    const orphans = new Set([...kept.keys()].filter((key) => !live.has(key)));
    for (const key of orphans) {
      if (!suspects.has(key)) continue;
      stopClaudeKeeperSession(socket, key);
      yield* Effect.logWarning("orchestration-v2.keeper-orphan-stopped", { key });
    }
    suspects = orphans;
  }).pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("orchestration-v2.keeper-reap-failed", { cause }),
    ),
  );
  yield* pass.pipe(Effect.repeat(Schedule.spaced("5 minutes")));
});
