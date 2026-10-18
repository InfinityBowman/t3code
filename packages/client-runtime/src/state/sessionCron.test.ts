// @effect-diagnostics globalDate:off -- Fixed local instants keep the next-run assertions deterministic.
import { describe, expect, it } from "vite-plus/test";

import { describeCron, nextCronRun } from "./sessionCron.ts";

// Local time throughout, as Claude reads its crons.
const at = (month: number, day: number, hour: number, minute: number) =>
  new Date(2026, month - 1, day, hour, minute);

describe("nextCronRun", () => {
  it("finds the next fire of steps, lists, ranges and one-shots", () => {
    // Sunday, October 18, 2026, 13:24.
    const now = at(10, 18, 13, 24);
    expect(nextCronRun("*/30 * * * *", now)).toEqual(at(10, 18, 13, 30));
    expect(nextCronRun("7 * * * *", now)).toEqual(at(10, 18, 14, 7));
    expect(nextCronRun("0 9,17 * * 1-5", now)).toEqual(at(10, 19, 9, 0));
    expect(nextCronRun("23 9 21 10 *", now)).toEqual(at(10, 21, 9, 23));
    expect(nextCronRun("0 0 1 * 0", now)).toEqual(at(10, 25, 0, 0));
    expect(nextCronRun("not a cron", now)).toBeNull();
  });
});

describe("describeCron", () => {
  it("reads common schedules and falls back to the expression", () => {
    expect(describeCron("*/15 * * * *")).toBe("Every 15 min");
    expect(describeCron("7 * * * *")).toBe("Hourly at :07");
    expect(describeCron("0 9 * * 1-5")).toBe("Weekdays at 09:00");
    expect(describeCron("30 8,20 * * *")).toBe("Daily at 08:30, 20:30");
    expect(describeCron("5 */4 * * *")).toBe("Daily, every 4 h at :05");
    expect(describeCron("23 9 21 10 *")).toBe("23 9 21 10 *");
  });
});
