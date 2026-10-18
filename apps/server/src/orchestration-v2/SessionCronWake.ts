import { CommandId, MessageId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";

import * as Orchestrator from "./Orchestrator.ts";

const WAKE_TEXT =
  "T3 Code restarted, which ended this session's process. This message reopens it so its crons fire again; nothing to do unless one is due.";

// Restart continuations dispatch first; a thread one of them already resumed needs no wake.
const WAKE_DELAY = Duration.minutes(1);

/**
 * A Claude session's crons live in its CLI process, and resuming the session restores them. A server
 * restart ends every process, so each thread whose session had crons gets one message after startup
 * to open it again.
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
          text: WAKE_TEXT,
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
