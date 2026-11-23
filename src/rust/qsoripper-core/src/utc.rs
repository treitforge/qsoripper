//! UTC conversions and fixed project date formats.

use prost_types::Timestamp;
use time::{Date, OffsetDateTime};

/// Convert a date and time to a protobuf timestamp.
#[must_use]
pub fn to_timestamp(value: OffsetDateTime) -> Timestamp {
    Timestamp {
        seconds: value.unix_timestamp(),
        nanos: value.nanosecond().cast_signed(),
    }
}

/// Convert a protobuf timestamp to UTC.
#[must_use]
pub fn from_timestamp(value: &Timestamp) -> Option<OffsetDateTime> {
    let nanos = u32::try_from(value.nanos).ok()?;
    OffsetDateTime::from_unix_timestamp(value.seconds)
        .ok()?
        .replace_nanosecond(nanos)
        .ok()
}

/// Format a UTC date as `YYYY-MM-DD`.
#[must_use]
pub fn format_date(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}-{:02}-{:02}",
        value.year(),
        u8::from(value.month()),
        value.day()
    )
}

/// Format a UTC date as `YYYYMMDD`.
#[must_use]
pub fn format_adif_date(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{:04}{:02}{:02}",
        value.year(),
        u8::from(value.month()),
        value.day()
    )
}

/// Format a UTC time as `HH:MM`.
#[must_use]
pub fn format_minutes(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!("{:02}:{:02}", value.hour(), value.minute())
}

/// Format a UTC time as `HHMMSS`.
#[must_use]
pub fn format_adif_time(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{:02}{:02}{:02}",
        value.hour(),
        value.minute(),
        value.second()
    )
}

/// Format UTC as `YYYY-MM-DD HH:MM:SS`.
#[must_use]
pub fn format_datetime(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{} {:02}:{:02}:{:02}",
        format_date(value),
        value.hour(),
        value.minute(),
        value.second()
    )
}

/// Format UTC as RFC 3339 with milliseconds.
#[must_use]
pub fn format_rfc3339_millis(value: OffsetDateTime) -> String {
    let value = value.to_offset(time::UtcOffset::UTC);
    format!(
        "{}T{:02}:{:02}:{:02}.{:03}Z",
        format_date(value),
        value.hour(),
        value.minute(),
        value.second(),
        value.millisecond()
    )
}

/// Parse a date in `YYYY-MM-DD` format.
/// Numeric fields can omit leading zeros.
///
/// # Errors
///
/// Returns an error when the date is invalid.
pub fn parse_date(value: &str) -> Result<Date, time::error::Parse> {
    Date::parse(
        value,
        time::macros::format_description!("[year]-[month padding:none]-[day padding:none]"),
    )
}

/// Parse a UTC time in `HH:MM` format.
/// Numeric fields can omit leading zeros.
///
/// # Errors
///
/// Returns an error when the time is invalid.
pub fn parse_minutes(value: &str) -> Result<time::Time, time::error::Parse> {
    time::Time::parse(
        value,
        time::macros::format_description!("[hour padding:none]:[minute padding:none]"),
    )
}

#[cfg(test)]
#[expect(clippy::expect_used, reason = "Tests require valid timestamp fixtures")]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn timestamp_round_trip_preserves_negative_fractional_seconds() {
        let timestamp = Timestamp {
            seconds: -1,
            nanos: 500_000_000,
        };
        let datetime = from_timestamp(&timestamp).expect("valid timestamp");
        assert_eq!(to_timestamp(datetime), timestamp);
        assert_eq!(format_rfc3339_millis(datetime), "1969-12-31T23:59:59.500Z");
    }

    #[test]
    fn timestamp_round_trip_preserves_protobuf_limits() {
        for value in [
            datetime!(0001-01-01 0:00 UTC),
            datetime!(9999-12-31 23:59:59.999_999_999 UTC),
        ] {
            let timestamp = to_timestamp(value);
            assert_eq!(from_timestamp(&timestamp), Some(value));
        }
    }

    #[test]
    fn invalid_timestamp_components_are_rejected() {
        for (seconds, nanos) in [(0, -1), (0, 1_000_000_000), (i64::MAX, 0), (i64::MIN, 0)] {
            assert!(from_timestamp(&Timestamp { seconds, nanos }).is_none());
        }
    }

    #[test]
    fn formats_preserve_utc_and_zero_padding() {
        let value = datetime!(2026-02-03 04:05:06.007 UTC);
        assert_eq!(format_date(value), "2026-02-03");
        assert_eq!(format_adif_date(value), "20260203");
        assert_eq!(format_minutes(value), "04:05");
        assert_eq!(format_adif_time(value), "040506");
        assert_eq!(format_datetime(value), "2026-02-03 04:05:06");
        assert_eq!(format_rfc3339_millis(value), "2026-02-03T04:05:06.007Z");
    }

    #[test]
    fn formats_convert_nonzero_offsets_to_utc() {
        let value = datetime!(2026-02-03 01:05:06 -03:00);
        assert_eq!(format_datetime(value), "2026-02-03 04:05:06");
        assert_eq!(format_minutes(value), "04:05");
        assert_eq!(format_adif_time(value), "040506");
        assert_eq!(format_rfc3339_millis(value), "2026-02-03T04:05:06.000Z");
    }

    #[test]
    fn date_parser_checks_leap_years_and_invalid_dates() {
        assert!(parse_date("2024-02-29").is_ok());
        assert_eq!(parse_date("2024-2-9"), parse_date("2024-02-09"));
        assert!(parse_date("2025-02-29").is_err());
        assert!(parse_date("2025-13-01").is_err());
        assert!(parse_date("invalid").is_err());
    }

    #[test]
    fn time_parser_preserves_unpadded_operator_input() {
        assert_eq!(parse_minutes("3:4"), parse_minutes("03:04"));
        assert_eq!(parse_minutes("23:59").expect("valid time").second(), 0);
        assert!(parse_minutes("24:00").is_err());
        assert!(parse_minutes("12:60").is_err());
        assert!(parse_minutes("12:30:00").is_err());
    }
}
