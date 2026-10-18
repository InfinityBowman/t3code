// @effect-diagnostics globalDate:off -- Cron fields are local wall-clock time, which Date's local accessors read directly.
// Claude session crons are five-field expressions (minute hour day-of-month month day-of-week)
// read in the machine's local time.

const WEEKDAY_NAMES = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const SEARCH_DAYS = 400;

function parseField(field: string, min: number, max: number): ReadonlySet<number> | null {
  const values = new Set<number>();
  for (const part of field.split(",")) {
    const match = /^(\*|(\d+)(?:-(\d+))?)(?:\/(\d+))?$/.exec(part);
    if (match === null) return null;
    const step = match[4] === undefined ? 1 : Number(match[4]);
    const from = match[1] === "*" ? min : Number(match[2]);
    const to =
      match[1] === "*" ? max : match[3] === undefined ? (match[4] ? max : from) : Number(match[3]);
    if (step < 1 || from < min || to > max || from > to) return null;
    for (let value = from; value <= to; value += step) values.add(value);
  }
  return values;
}

interface ParsedCron {
  readonly minutes: ReadonlyArray<number>;
  readonly hours: ReadonlyArray<number>;
  readonly days: ReadonlySet<number>;
  readonly months: ReadonlySet<number>;
  readonly weekdays: ReadonlySet<number>;
  readonly anyDay: boolean;
  readonly anyWeekday: boolean;
}

function parseCron(schedule: string): ParsedCron | null {
  const fields = schedule.trim().split(/\s+/);
  if (fields.length !== 5) return null;
  const [minute, hour, day, month, weekday] = fields as [string, string, string, string, string];
  const minutes = parseField(minute, 0, 59);
  const hours = parseField(hour, 0, 23);
  const days = parseField(day, 1, 31);
  const months = parseField(month, 1, 12);
  const weekdays = parseField(weekday, 0, 7);
  if (!minutes || !hours || !days || !months || !weekdays) return null;
  return {
    minutes: [...minutes].sort((a, b) => a - b),
    hours: [...hours].sort((a, b) => a - b),
    days,
    months,
    weekdays: new Set([...weekdays].map((value) => value % 7)),
    anyDay: day === "*",
    anyWeekday: weekday === "*",
  };
}

function dayMatches(cron: ParsedCron, date: Date): boolean {
  if (!cron.months.has(date.getMonth() + 1)) return false;
  const day = cron.days.has(date.getDate());
  const weekday = cron.weekdays.has(date.getDay());
  // Standard cron: when both day fields are restricted, either one matching is enough.
  if (!cron.anyDay && !cron.anyWeekday) return day || weekday;
  return day && weekday;
}

/** The first time after `after` the cron fires, or null when it never does within a year. */
export function nextCronRun(schedule: string, after: Date): Date | null {
  const cron = parseCron(schedule);
  if (cron === null) return null;
  const day = new Date(after.getFullYear(), after.getMonth(), after.getDate());
  for (let offset = 0; offset < SEARCH_DAYS; offset++) {
    if (dayMatches(cron, day)) {
      for (const hour of cron.hours) {
        for (const minute of cron.minutes) {
          const run = new Date(day.getFullYear(), day.getMonth(), day.getDate(), hour, minute);
          if (run.getTime() > after.getTime()) return run;
        }
      }
    }
    day.setDate(day.getDate() + 1);
  }
  return null;
}

const pad = (value: number) => String(value).padStart(2, "0");

/** A short reading of the schedule, such as "Every 30 min" or "Weekdays at 09:00". */
export function describeCron(schedule: string): string {
  const fields = schedule.trim().split(/\s+/);
  const cron = parseCron(schedule);
  if (cron === null) return schedule;
  const [minute, hour, day, month, weekday] = fields as [string, string, string, string, string];
  const everyMinutes = /^\*\/(\d+)$/.exec(minute)?.[1];
  const singleMinute = /^\d+$/.test(minute) ? Number(minute) : undefined;
  if (hour === "*" && day === "*" && month === "*" && weekday === "*") {
    if (minute === "*") return "Every minute";
    if (everyMinutes !== undefined) return `Every ${everyMinutes} min`;
    if (singleMinute !== undefined) return `Hourly at :${pad(singleMinute)}`;
  }
  if (singleMinute === undefined || day !== "*" || month !== "*") return schedule;
  const everyHours = /^\*\/(\d+)$/.exec(hour)?.[1];
  const days =
    weekday === "*"
      ? "Daily"
      : weekday === "1-5"
        ? "Weekdays"
        : [...cron.weekdays]
            .sort((a, b) => a - b)
            .map((value) => WEEKDAY_NAMES[value])
            .join(", ");
  if (everyHours !== undefined) return `${days}, every ${everyHours} h at :${pad(singleMinute)}`;
  if (!/^\d+(,\d+)*$/.test(hour)) return schedule;
  return `${days} at ${cron.hours.map((value) => `${pad(value)}:${pad(singleMinute)}`).join(", ")}`;
}
