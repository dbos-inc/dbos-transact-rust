//! Cron expressions: parsing one, and walking its firing times on a timezone's wall clock.
//!
//! **`croner` does the work.** What is here is the dialect around it — which of its options this
//! crate turns on, and the two spellings it does not accept that TypeScript does — and the one
//! check it leaves to its caller, that a pattern fires at all.
//!
//! The dialect is documented on [`ScheduleSpec`](crate::ScheduleSpec), where a user meets it.

use jiff::tz::TimeZone;
use jiff::{Timestamp, Zoned};

use croner::Cron;
use croner::parser::{CronParser, Seconds, Year};

/// A parsed cron expression, bound to the timezone whose wall clock it fires on.
#[derive(Debug, Clone)]
pub(crate) struct CronSchedule {
    cron: Cron,
    zone: TimeZone,
}

impl CronSchedule {
    /// Parses `expression` to fire on `timezone`'s wall clock, or on UTC's when there is none.
    ///
    /// The error is a sentence for a caller to wrap, naming what was wrong: the expression, the
    /// timezone, or that the pattern never fires.
    ///
    /// **UTC when unset**, as in Python. TypeScript and Go fall back to the process's local zone,
    /// which makes a schedule's firing times depend on where its executor happens to run — and,
    /// for a fleet spread across regions, makes two executors disagree about when a tick is.
    pub(crate) fn parse(expression: &str, timezone: Option<&str>) -> Result<Self, String> {
        let zone = match timezone {
            None => TimeZone::UTC,
            Some(name) => TimeZone::get(name).map_err(|_| format!("invalid timezone: `{name}`"))?,
        };
        let cron = CronParser::builder()
            .seconds(Seconds::Optional)
            .year(Year::Disallowed)
            .dom_and_dow(true)
            .build()
            .parse(&normalize(expression))
            .map_err(|error| format!("invalid cron schedule `{expression}`: {error}"))?;
        let schedule = Self { cron, zone };
        // **Checked here rather than left to the loop**, because `croner` parses `0 0 31 2 *` without
        // complaint and only fails when asked for a time. Python accepts such a pattern and its
        // schedule thread dies on the first fire; TypeScript refuses it on create, and so does
        // this. Asked from now, because a pattern that has stopped firing is as dead as one that
        // never could.
        if schedule.next_after(Timestamp::now()).is_none() {
            return Err(format!("cron schedule `{expression}` never fires"));
        }
        Ok(schedule)
    }

    /// The first firing strictly after `after`, on this schedule's wall clock.
    ///
    /// `None` when there is none — a pattern that has stopped firing, or one past the end of the
    /// calendar `croner` searches.
    pub(crate) fn next_after(&self, after: Timestamp) -> Option<Zoned> {
        self.cron
            .find_next_occurrence(&after.to_zoned(self.zone.clone()), false)
            .ok()
    }
}

/// The expression with the two spellings `croner` does not take rewritten into ones it does.
///
/// `@midnight` is `@daily` in every dialect that has it. Full month and weekday names are
/// TypeScript's; `croner` takes the three-letter forms only, and turns `Friday` into `5DAY`.
fn normalize(expression: &str) -> String {
    const FULL_NAMES: [(&str, &str); 19] = [
        // Weekdays first, and every name whose short form is a prefix of a longer one is
        // rewritten whole: replacing `MON` inside `MONDAY` would leave `MONDAY` as `MONDAY`.
        ("MONDAY", "MON"),
        ("TUESDAY", "TUE"),
        ("WEDNESDAY", "WED"),
        ("THURSDAY", "THU"),
        ("FRIDAY", "FRI"),
        ("SATURDAY", "SAT"),
        ("SUNDAY", "SUN"),
        ("JANUARY", "JAN"),
        ("FEBRUARY", "FEB"),
        ("MARCH", "MAR"),
        ("APRIL", "APR"),
        ("JUNE", "JUN"),
        ("JULY", "JUL"),
        ("AUGUST", "AUG"),
        ("SEPTEMBER", "SEP"),
        ("OCTOBER", "OCT"),
        ("NOVEMBER", "NOV"),
        ("DECEMBER", "DEC"),
        ("@MIDNIGHT", "@DAILY"),
    ];
    let mut normalized = expression.trim().to_uppercase();
    for (long, short) in FULL_NAMES {
        normalized = normalized.replace(long, short);
    }
    normalized
}

/// The workflow id of a schedule's firing at `at`: `sched-<name>-<time>`.
///
/// **Deterministic**, which is what makes a firing happen once: every executor polling the
/// schedule computes the same id for the same tick, and a backfill over a window the loop already
/// fired finds the ids it would write already taken.
///
/// The time is RFC 3339 to the second, on the schedule's wall clock: `Z` at a zero offset and
/// `±hh:mm` otherwise, with no fraction and no zone name — `2026-10-05T12:00:00Z`,
/// `2026-10-05T08:00:00-04:00`. That is Go's `time.RFC3339`, and what Java's scheduler is moving
/// to; Python writes `+00:00` for UTC and TypeScript always writes UTC with milliseconds, so two
/// SDKs firing one schedule only deduplicate against each other when they agree on this.
pub(crate) fn firing_id(schedule_name: &str, at: &Zoned) -> String {
    format!("sched-{schedule_name}-{}", rfc3339_seconds(at))
}

/// The workflow id of a manual trigger at `now`: `sched-<name>-trigger-<time>`.
///
/// Not deterministic, and not meant to be: two triggers are two runs. The time is UTC to the
/// nanosecond with trailing zeros dropped, which is Go's `time.RFC3339Nano`.
pub(crate) fn trigger_id(schedule_name: &str, now: Timestamp) -> String {
    format!("sched-{schedule_name}-trigger-{now}")
}

/// `at` as RFC 3339 to the second, `Z` for a zero offset.
fn rfc3339_seconds(at: &Zoned) -> String {
    let formatted = at.strftime("%Y-%m-%dT%H:%M:%S%:z").to_string();
    match formatted.strip_suffix("+00:00") {
        Some(local) => format!("{local}Z"),
        None => formatted,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(rfc3339: &str) -> Timestamp {
        rfc3339.parse().unwrap()
    }

    /// The firings after `start`, as UTC instants.
    fn firings(expression: &str, timezone: Option<&str>, start: &str, count: usize) -> Vec<String> {
        let schedule = CronSchedule::parse(expression, timezone).unwrap();
        let mut cursor = at(start);
        let mut out = Vec::new();
        for _ in 0..count {
            let next = schedule.next_after(cursor).unwrap();
            cursor = next.timestamp();
            out.push(cursor.to_string());
        }
        out
    }

    fn refused(expression: &str) -> String {
        CronSchedule::parse(expression, None).unwrap_err()
    }

    #[test]
    fn six_fields_put_seconds_first() {
        assert_eq!(
            firings("*/20 * * * * *", None, "2025-01-01T00:00:00Z", 3),
            [
                "2025-01-01T00:00:20Z",
                "2025-01-01T00:00:40Z",
                "2025-01-01T00:01:00Z"
            ]
        );
    }

    #[test]
    fn five_fields_fire_at_second_zero() {
        assert_eq!(
            firings("*/5 * * * *", None, "2025-01-01T00:00:30Z", 2),
            ["2025-01-01T00:05:00Z", "2025-01-01T00:10:00Z"]
        );
    }

    #[test]
    fn the_next_firing_is_strictly_after_and_on_a_whole_second() {
        assert_eq!(
            firings("* * * * * *", None, "2025-01-01T00:00:00.5Z", 1),
            ["2025-01-01T00:00:01Z"]
        );
        assert_eq!(
            firings("* * * * * *", None, "2025-01-01T00:00:01Z", 1),
            ["2025-01-01T00:00:02Z"]
        );
    }

    #[test]
    fn every_nickname_is_its_pattern() {
        for (nickname, pattern) in [
            ("@yearly", "0 0 1 1 *"),
            ("@annually", "0 0 1 1 *"),
            ("@monthly", "0 0 1 * *"),
            ("@weekly", "0 0 * * 0"),
            ("@daily", "0 0 * * *"),
            ("@midnight", "0 0 * * *"),
            ("@hourly", "0 * * * *"),
            ("@Daily", "0 0 * * *"),
        ] {
            assert_eq!(
                firings(nickname, None, "2025-03-15T12:34:56Z", 3),
                firings(pattern, None, "2025-03-15T12:34:56Z", 3),
                "{nickname}"
            );
        }
    }

    #[test]
    fn a_question_mark_is_a_wildcard_in_either_day_field() {
        assert_eq!(
            firings("0 0 ? * 1", None, "2025-01-01T00:00:00Z", 2),
            firings("0 0 * * 1", None, "2025-01-01T00:00:00Z", 2),
        );
        assert_eq!(
            firings("0 0 1 * ?", None, "2025-01-01T00:00:00Z", 2),
            firings("0 0 1 * *", None, "2025-01-01T00:00:00Z", 2),
        );
    }

    #[test]
    fn names_are_accepted_short_or_full_in_any_case() {
        let start = "2025-01-01T00:00:00Z";
        let mondays = firings("0 0 * * 1", None, start, 3);
        for spelling in ["mon", "MON", "Monday", "MONDAY", "monday"] {
            assert_eq!(
                firings(&format!("0 0 * * {spelling}"), None, start, 3),
                mondays,
                "{spelling}"
            );
        }
        let septembers = firings("0 0 1 9 *", None, start, 2);
        for spelling in ["sep", "September", "SEPTEMBER"] {
            assert_eq!(
                firings(&format!("0 0 1 {spelling} *"), None, start, 2),
                septembers,
                "{spelling}"
            );
        }
        assert_eq!(
            firings("0 0 * * Monday-Wednesday", None, "2025-01-05T00:00:00Z", 4),
            [
                "2025-01-06T00:00:00Z",
                "2025-01-07T00:00:00Z",
                "2025-01-08T00:00:00Z",
                "2025-01-13T00:00:00Z"
            ]
        );
    }

    #[test]
    fn steps_count_from_the_start_of_their_range() {
        assert_eq!(
            firings("1-10/4 * * * * *", None, "2025-01-01T00:00:00Z", 4),
            [
                "2025-01-01T00:00:01Z",
                "2025-01-01T00:00:05Z",
                "2025-01-01T00:00:09Z",
                "2025-01-01T00:01:01Z"
            ]
        );
        // Day-of-month `*/10` is 1, 11, 21, 31 — counted from the field's minimum, which is 1.
        assert_eq!(
            firings("0 0 */10 * *", None, "2025-01-01T00:00:00Z", 4),
            [
                "2025-01-11T00:00:00Z",
                "2025-01-21T00:00:00Z",
                "2025-01-31T00:00:00Z",
                "2025-02-01T00:00:00Z"
            ]
        );
    }

    #[test]
    fn seven_is_sunday_too() {
        assert_eq!(
            firings("0 0 * * 7", None, "2025-01-01T00:00:00Z", 2),
            firings("0 0 * * 0", None, "2025-01-01T00:00:00Z", 2),
        );
        // Friday through Sunday.
        assert_eq!(
            firings("0 0 * * 5-7", None, "2025-01-01T00:00:00Z", 4),
            [
                "2025-01-03T00:00:00Z",
                "2025-01-04T00:00:00Z",
                "2025-01-05T00:00:00Z",
                "2025-01-10T00:00:00Z"
            ]
        );
    }

    #[test]
    fn last_and_nearest_weekday_forms() {
        // The last day of February, in a leap year and out of one.
        assert_eq!(
            firings("0 0 L * *", None, "2024-02-01T00:00:00Z", 1),
            ["2024-02-29T00:00:00Z"]
        );
        assert_eq!(
            firings("0 0 L * *", None, "2025-02-01T00:00:00Z", 1),
            ["2025-02-28T00:00:00Z"]
        );
        // The 15th of November 2025 is a Saturday, so the nearest weekday is Friday the 14th.
        assert_eq!(
            firings("0 0 15W * *", None, "2025-11-01T00:00:00Z", 1),
            ["2025-11-14T00:00:00Z"]
        );
        // The last weekday of May 2025: the 31st is a Saturday.
        assert_eq!(
            firings("0 0 LW * *", None, "2025-05-01T00:00:00Z", 1),
            ["2025-05-30T00:00:00Z"]
        );
        // The last Friday of October 2025.
        assert_eq!(
            firings("0 0 * * 5L", None, "2025-10-01T00:00:00Z", 1),
            ["2025-10-31T00:00:00Z"]
        );
        // The third Tuesday of October 2025.
        assert_eq!(
            firings("0 0 * * 2#3", None, "2025-10-01T00:00:00Z", 1),
            ["2025-10-21T00:00:00Z"]
        );
    }

    #[test]
    fn both_day_fields_must_match_when_both_are_restricted() {
        // Friday the 13th, not every Friday and every 13th.
        assert_eq!(
            firings("0 0 13 * 5", None, "2025-01-01T00:00:00Z", 2),
            ["2025-06-13T00:00:00Z", "2026-02-13T00:00:00Z"]
        );
    }

    #[test]
    fn a_distant_firing_is_found() {
        assert_eq!(
            firings("0 0 29 2 *", None, "2025-01-01T00:00:00Z", 1),
            ["2028-02-29T00:00:00Z"]
        );
        assert_eq!(
            firings(
                "10 19 28-31 * *",
                Some("America/New_York"),
                "2026-09-01T00:00:00Z",
                1
            ),
            ["2026-09-28T23:10:00Z"]
        );
    }

    #[test]
    fn a_timezone_moves_the_wall_clock_the_pattern_is_read_on() {
        // Midnight in New York in winter is 05:00 UTC.
        assert_eq!(
            firings(
                "0 0 * * *",
                Some("America/New_York"),
                "2025-01-01T00:00:00Z",
                2
            ),
            ["2025-01-01T05:00:00Z", "2025-01-02T05:00:00Z"]
        );
        assert_eq!(
            firings(
                "0 0 0 * * *",
                Some("America/Sao_Paulo"),
                "2018-10-10T12:00:00Z",
                1
            ),
            ["2018-10-11T03:00:00Z"]
        );
        assert_eq!(
            firings(
                "0 0 0 * * *",
                Some("Europe/Rome"),
                "2018-10-10T12:00:00Z",
                1
            ),
            ["2018-10-10T22:00:00Z"]
        );
    }

    #[test]
    fn daylight_saving_follows_classic_cron() {
        let ny = Some("America/New_York");
        // 02:30 does not exist on 2025-03-09, so that day fires at 03:00 EDT — the first instant
        // after the gap — and the next day is back at 02:30.
        assert_eq!(
            firings("30 2 * * *", ny, "2025-03-09T04:00:00Z", 2),
            ["2025-03-09T07:00:00Z", "2025-03-10T06:30:00Z"]
        );
        // 01:30 happens twice on 2025-11-02, and a fixed time of day fires on the first only.
        assert_eq!(
            firings("30 1 * * *", ny, "2025-11-02T04:00:00Z", 2),
            ["2025-11-02T05:30:00Z", "2025-11-03T06:30:00Z"]
        );
        // An interval fires in both of the repeated hours: 01:00 and 01:30 EDT, then 01:00 and
        // 01:30 EST.
        assert_eq!(
            firings("0 */30 * * * *", ny, "2025-11-02T04:45:00Z", 4),
            [
                "2025-11-02T05:00:00Z",
                "2025-11-02T05:30:00Z",
                "2025-11-02T06:00:00Z",
                "2025-11-02T06:30:00Z"
            ]
        );
    }

    #[test]
    fn malformed_patterns_are_refused() {
        for pattern in [
            "",
            "* * * *",
            "* * * * * * *",
            "* * 32 * *",
            "* 25 * * *",
            "60 * * * *",
            "63 * * * * *",
            "* * * 13 *",
            "* * * foo *",
            "* * * * 9",
            "* * * * foo",
            "*/0 * * * *",
            "*/someString * * * *",
            "5/10 * * * *",
            "1,2,3/2 * * * *",
            "0 0 1-15W * *",
            "R * * * *",
        ] {
            let error = refused(pattern);
            assert!(
                error.starts_with("invalid cron schedule"),
                "{pattern}: {error}"
            );
        }
    }

    #[test]
    fn forms_typescript_takes_and_this_does_not_are_refused_rather_than_misread() {
        for pattern in ["0 0 L-1 * *", "0 22-2 * * *", "0 0 * * Fri-Mon"] {
            assert!(
                refused(pattern).starts_with("invalid cron schedule"),
                "{pattern}"
            );
        }
    }

    #[test]
    fn a_pattern_that_never_fires_is_refused() {
        for pattern in ["0 0 31 2 *", "0 0 30 2 *"] {
            assert!(refused(pattern).ends_with("never fires"), "{pattern}");
        }
    }

    #[test]
    fn an_unknown_timezone_is_refused() {
        assert_eq!(
            CronSchedule::parse("* * * * *", Some("Mars/Olympus_Mons")).unwrap_err(),
            "invalid timezone: `Mars/Olympus_Mons`"
        );
    }

    #[test]
    fn a_firing_id_is_rfc3339_to_the_second_on_the_schedules_wall_clock() {
        let utc = at("2026-10-05T12:00:00Z").to_zoned(TimeZone::UTC);
        assert_eq!(
            firing_id("nightly", &utc),
            "sched-nightly-2026-10-05T12:00:00Z"
        );

        let ny = at("2026-10-05T12:00:00Z").to_zoned(TimeZone::get("America/New_York").unwrap());
        assert_eq!(
            firing_id("nightly", &ny),
            "sched-nightly-2026-10-05T08:00:00-04:00"
        );

        // A zone at a zero offset writes `Z`, as Go's `RFC3339` does: the offset is the format's
        // business, not the zone's name.
        let london = at("2026-01-05T12:00:00Z").to_zoned(TimeZone::get("Europe/London").unwrap());
        assert_eq!(
            firing_id("nightly", &london),
            "sched-nightly-2026-01-05T12:00:00Z"
        );
    }

    #[test]
    fn a_trigger_id_is_utc_with_the_fraction_trimmed() {
        assert_eq!(
            trigger_id("nightly", at("2026-10-05T12:00:00.250Z")),
            "sched-nightly-trigger-2026-10-05T12:00:00.25Z"
        );
        assert_eq!(
            trigger_id("nightly", at("2026-10-05T12:00:00Z")),
            "sched-nightly-trigger-2026-10-05T12:00:00Z"
        );
    }
}
