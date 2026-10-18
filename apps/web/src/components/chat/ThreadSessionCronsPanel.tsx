import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { describeCron, nextCronRun } from "@t3tools/client-runtime/state/thread-execution";
import { AlarmClockIcon, Repeat2Icon } from "lucide-react";

import { useNowMinute } from "../../hooks/useNowMinute";
import { cn } from "../../lib/utils";
import { useQuietThreadShell } from "../../state/entities";
import { ScheduledRelativeLabel } from "../settings/ScheduledTasksSettings";
import { ThreadDetailsSection } from "./ThreadDetailsSection";
import {
  THREAD_DETAILS_PANEL_ICON_CLASS,
  THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS,
} from "./threadDetailsPanelStyles";

const onceFormat = new Intl.DateTimeFormat(undefined, {
  weekday: "short",
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

/** The crons the thread's Claude session scheduled for itself, as it last reported them. */
export function ThreadSessionCronsPanel(props: {
  readonly environmentId: EnvironmentId;
  readonly threadId: ThreadId;
}) {
  const shell = useQuietThreadShell({
    environmentId: props.environmentId,
    threadId: props.threadId,
  });
  const nowMinute = useNowMinute();
  const crons = shell?.sessionCrons ?? [];
  if (crons.length === 0) return null;
  const now = new Date(Date.parse(`${nowMinute}:00.000Z`));
  const rows = crons
    .map((cron) => ({ cron, next: nextCronRun(cron.schedule, now) }))
    .sort(
      (left, right) => (left.next?.getTime() ?? Infinity) - (right.next?.getTime() ?? Infinity),
    );

  return (
    <ThreadDetailsSection
      headingId="thread-details-session-crons-heading"
      title={`Crons (${crons.length})`}
      data-thread-session-crons-panel
    >
      <ul className="m-0 list-none p-0">
        {rows.map(({ cron, next }) => {
          const Icon = cron.recurring ? Repeat2Icon : AlarmClockIcon;
          return (
            <li
              key={cron.id}
              title={`${cron.schedule}\n\n${cron.prompt}`}
              className={cn(
                "flex items-center rounded-lg py-1.5",
                THREAD_DETAILS_PANEL_ROW_CONTENT_CLASS,
              )}
            >
              <Icon className={THREAD_DETAILS_PANEL_ICON_CLASS} />
              <div className="min-w-0 flex-1">
                <span className="block truncate text-sm font-medium text-foreground/80">
                  {cron.prompt.split("\n")[0]}
                </span>
                <p className="truncate text-2xs text-muted-foreground">
                  {cron.recurring
                    ? describeCron(cron.schedule)
                    : next === null
                      ? cron.schedule
                      : `Once, ${onceFormat.format(next)}`}
                  {next === null ? null : (
                    <>
                      {" · "}
                      {cron.recurring ? "next " : null}
                      <ScheduledRelativeLabel value={next.toISOString()} />
                    </>
                  )}
                </p>
              </div>
            </li>
          );
        })}
      </ul>
    </ThreadDetailsSection>
  );
}
