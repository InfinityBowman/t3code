import { CommandId, MessageId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";

import { claudeKeeperSessionsAtStartup } from "../provider/ClaudeKeeper.ts";
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
