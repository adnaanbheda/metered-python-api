//! Calendar-month quota period, same as src/utils/periods.py.

use std::time::{SystemTime, UNIX_EPOCH};

const DAY: i64 = 86_400;

/// Period label ("2026-11") and the unix ts the usage counter expires at:
/// start of next month + 1 day grace.
pub fn current_period() -> (String, i64) {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before 1970")
        .as_secs() as i64;
    period_at(now)
}

pub fn period_at(unix: i64) -> (String, i64) {
    let (y, m, _) = civil_from_days(unix.div_euclid(DAY));
    let (ny, nm) = if m == 12 { (y + 1, 1) } else { (y, m + 1) };
    let next_month = days_from_civil(ny, nm, 1) * DAY;
    (format!("{y:04}-{m:02}"), next_month + DAY)
}

// Howard Hinnant's date algorithms: days since 1970-01-01 <-> proleptic Gregorian date.
fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m as i64 + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d as i64 - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + if m <= 2 { 1 } else { 0 };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mid_month() {
        // 2026-11-01T14:31:00Z
        assert_eq!(period_at(1_793_543_460), ("2026-11".into(), 1_796_169_600));
    }

    #[test]
    fn december_rolls_into_next_year() {
        // 2026-12-31T23:59:59Z -> expires 2027-01-02T00:00:00Z
        assert_eq!(period_at(1_798_761_599), ("2026-12".into(), 1_798_848_000));
    }

    #[test]
    fn first_second_of_month() {
        // 2027-01-01T00:00:00Z
        assert_eq!(period_at(1_798_761_600), ("2027-01".into(), 1_801_526_400));
    }
}
