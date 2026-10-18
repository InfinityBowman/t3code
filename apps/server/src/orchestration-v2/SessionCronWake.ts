import { CommandId, MessageId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";

import * as Orchestrator from "./Orchestrator.ts";

// A local command: Claude answers it without the model, so the session reopens, restoring its crons, and the
// model has nothing to act on.
const REOPEN_COMMAND = "/cost";

// Restart continuations dispatch first; a thread one of them already resumed needs no wake.
const WAKE_DELAY = Duration.minutes(1);

/**
 * A Claude session's crons live in its CLI process, and resuming the session restores them. A server
 * restart ends every process, so after startup each thread whose session had crons is reopened.
 */
export const wakeSessionCrons = (startedAt: DateTime.Utc) =>
  Effect.gen(function* () {
    yield* Effect.sleep(WAKE_DELAY);
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const { threads } = yield* orchestrator.getShellSnapshot();
    for (const shell of threads) {
      if ((shell.sessionCrons ?? []).length === 0 || shell.archivedAt !== null) continue;
      const requestedAt = shell.latestRunRequestedAt;
      if (requestedAt != null && DateTime.isGreaterThanOrEqualTo(requestedAt, startedAt)) continue;
      const key = `${shell.id}:${DateTime.toEpochMillis(startedAt)}`;
      yield* orchestrator
        .dispatch({
          type: "message.dispatch",
          commandId: CommandId.make(`command:session-cron-wake:${key}`),
          threadId: shell.id,
          messageId: MessageId.make(`message:session-cron-wake:${key}`),
          text: REOPEN_COMMAND,
          attachments: [],
          modelSelection: shell.modelSelection,
          dispatchMode: { type: "start_immediately" },
          createdBy: "agent",
          creationSource: "server",
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("orchestration-v2.session-cron-wake-failed", {
              threadId: shell.id,
              cause,
            }),
          ),
        );
    }
  });
