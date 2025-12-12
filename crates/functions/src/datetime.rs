use chrono::{DateTime, Datelike, Duration, NaiveDate, Timelike, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn date(value: &Value) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::null());
    }

    if let Some(ts) = value.as_timestamp() {
        return Ok(Value::date(ts.date_naive()));
    }

    if let Some(d) = value.as_date() {
        return Ok(Value::date(d));
    }

    Err(Error::TypeMismatch {
        expected: "TIMESTAMP or DATE".to_string(),
        actual: format!("{:?}", value),
    })
}

pub fn timestamp_diff(ts1: &DateTime<Utc>, ts2: &DateTime<Utc>, unit: &str) -> Result<i64> {
    let duration = ts1.signed_duration_since(*ts2);

    match unit.to_uppercase().as_str() {
        "SECOND" => Ok(duration.num_seconds()),
        "MINUTE" => Ok(duration.num_minutes()),
        "HOUR" => Ok(duration.num_hours()),
        "DAY" => Ok(duration.num_days()),
        "WEEK" => Ok(duration.num_weeks()),
        _ => Err(Error::invalid_query(format!(
            "Unknown unit for TIMESTAMP_DIFF: {}. Supported: SECOND, MINUTE, HOUR, DAY, WEEK",
            unit
        ))),
    }
}

pub fn timestamp_trunc(ts: &DateTime<Utc>, precision: &str) -> Result<DateTime<Utc>> {
    match precision.to_uppercase().as_str() {
        "YEAR" => truncate_to_year(ts),
        "QUARTER" => truncate_to_quarter(ts),
        "MONTH" => truncate_to_month(ts),
        "WEEK" => truncate_to_week(ts),
        "DAY" => truncate_to_day(ts),
        "HOUR" => truncate_to_hour(ts),
        "MINUTE" => truncate_to_minute(ts),
        "SECOND" => truncate_to_second(ts),
        _ => Err(Error::invalid_query(format!(
            "Unknown precision for TIMESTAMP_TRUNC: {}. Supported: YEAR, QUARTER, MONTH, WEEK, DAY, HOUR, MINUTE, SECOND",
            precision
        ))),
    }
}

fn zero_time_from_level(ts: DateTime<Utc>, level: TruncateLevel) -> Result<DateTime<Utc>> {
    match level {
        TruncateLevel::Second => ts
            .with_nanosecond(0)
            .ok_or_else(|| Error::internal("Failed to set nanosecond to 0")),
        TruncateLevel::Minute => ts
            .with_second(0)
            .and_then(|t| t.with_nanosecond(0))
            .ok_or_else(|| Error::internal("Failed to truncate to minute")),
        TruncateLevel::Hour => ts
            .with_minute(0)
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .ok_or_else(|| Error::internal("Failed to truncate to hour")),
        TruncateLevel::Day => ts
            .with_hour(0)
            .and_then(|t| t.with_minute(0))
            .and_then(|t| t.with_second(0))
            .and_then(|t| t.with_nanosecond(0))
            .ok_or_else(|| Error::internal("Failed to truncate to day")),
    }
}

#[derive(Debug, Copy, Clone)]
enum TruncateLevel {
    Second,
    Minute,
    Hour,
    Day,
}

pub fn truncate_to_year(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    let result = ts
        .with_month(1)
        .and_then(|t| t.with_day(1))
        .ok_or_else(|| Error::internal("Failed to truncate to start of year"))?;
    zero_time_from_level(result, TruncateLevel::Day)
}

fn truncate_to_quarter(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    let quarter_month = ((ts.month() - 1) / 3) * 3 + 1;
    let result = ts
        .with_month(quarter_month)
        .and_then(|t| t.with_day(1))
        .ok_or_else(|| Error::internal("Failed to truncate to start of quarter"))?;
    zero_time_from_level(result, TruncateLevel::Day)
}

pub fn truncate_to_month(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    let result = ts
        .with_day(1)
        .ok_or_else(|| Error::internal("Failed to truncate to start of month"))?;
    zero_time_from_level(result, TruncateLevel::Day)
}

fn truncate_to_week(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    let weekday = ts.weekday().num_days_from_monday();
    let week_start = *ts - Duration::days(weekday as i64);
    zero_time_from_level(week_start, TruncateLevel::Day)
}

pub fn truncate_to_day(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    zero_time_from_level(*ts, TruncateLevel::Day)
}

fn truncate_to_hour(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    zero_time_from_level(*ts, TruncateLevel::Hour)
}

fn truncate_to_minute(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    zero_time_from_level(*ts, TruncateLevel::Minute)
}

fn truncate_to_second(ts: &DateTime<Utc>) -> Result<DateTime<Utc>> {
    zero_time_from_level(*ts, TruncateLevel::Second)
}

pub fn eval_timestamp_diff(ts1: &Value, ts2: &Value, unit: &Value) -> Result<Value> {
    if ts1.is_null() || ts2.is_null() || unit.is_null() {
        return Ok(Value::null());
    }

    if let (Some(t1), Some(t2), Some(u)) = (ts1.as_timestamp(), ts2.as_timestamp(), unit.as_str()) {
        let diff = timestamp_diff(&t1, &t2, u)?;
        return Ok(Value::int64(diff));
    }

    Err(Error::TypeMismatch {
        expected: "TIMESTAMP, TIMESTAMP, STRING".to_string(),
        actual: format!(
            "{}, {}, {}",
            ts1.data_type(),
            ts2.data_type(),
            unit.data_type()
        ),
    })
}

pub fn eval_timestamp_trunc(ts: &Value, precision: &Value) -> Result<Value> {
    if ts.is_null() || precision.is_null() {
        return Ok(Value::null());
    }

    if let (Some(t), Some(p)) = (ts.as_timestamp(), precision.as_str()) {
        let truncated = timestamp_trunc(&t, p)?;
        return Ok(Value::timestamp(truncated));
    }

    Err(Error::TypeMismatch {
        expected: "TIMESTAMP, STRING".to_string(),
        actual: format!("{}, {}", ts.data_type(), precision.data_type()),
    })
}

pub fn generate_date_array(
    start_date: &Value,
    end_date: &Value,
    interval_val: &Value,
    interval_unit: &Value,
) -> Result<Value> {
    use chrono::NaiveDate;

    let start = if start_date.is_null() {
        return Ok(Value::null());
    } else if let Some(d) = start_date.as_date() {
        d
    } else if let Some(s) = start_date.as_str() {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| Error::invalid_query(format!("Invalid start date '{}': {}", s, e)))?
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_DATE_ARRAY start must be DATE or STRING, got {}",
            start_date.data_type()
        )));
    };

    let end = if end_date.is_null() {
        return Ok(Value::null());
    } else if let Some(d) = end_date.as_date() {
        d
    } else if let Some(s) = end_date.as_str() {
        NaiveDate::parse_from_str(s, "%Y-%m-%d")
            .map_err(|e| Error::invalid_query(format!("Invalid end date '{}': {}", s, e)))?
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_DATE_ARRAY end must be DATE or STRING, got {}",
            end_date.data_type()
        )));
    };

    let step = if interval_val.is_null() {
        return Ok(Value::null());
    } else if let Some(i) = interval_val.as_i64() {
        i
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_DATE_ARRAY interval value must be INT64, got {}",
            interval_val.data_type()
        )));
    };

    if step == 0 {
        return Err(Error::invalid_query(
            "GENERATE_DATE_ARRAY interval cannot be 0".to_string(),
        ));
    }

    let unit = if interval_unit.is_null() {
        return Ok(Value::null());
    } else if let Some(s) = interval_unit.as_str() {
        s.to_uppercase()
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_DATE_ARRAY interval unit must be STRING, got {}",
            interval_unit.data_type()
        )));
    };

    let elements = generate_date_array_elements(start, end, step, &unit)?;
    Ok(Value::array(elements))
}

fn generate_date_array_elements(
    start: NaiveDate,
    end: NaiveDate,
    step: i64,
    unit: &str,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();

    let is_ascending = start <= end;
    let step_is_ascending = step > 0;

    if is_ascending != step_is_ascending {
        return Ok(result);
    }

    let mut current = start;

    let should_continue =
        |curr: NaiveDate| -> bool { if step > 0 { curr <= end } else { curr >= end } };

    while should_continue(current) {
        result.push(Value::date(current));

        match add_date_interval(current, step, unit)? {
            Some(next_date) => {
                if !should_continue(next_date) {
                    break;
                }
                current = next_date;
            }
            None => break,
        }
    }

    Ok(result)
}

fn add_date_interval(date: NaiveDate, value: i64, unit: &str) -> Result<Option<NaiveDate>> {
    use chrono::{Datelike, Duration};

    match unit {
        "DAY" => Ok(date.checked_add_signed(Duration::days(value))),
        "WEEK" => Ok(date.checked_add_signed(Duration::weeks(value))),
        "MONTH" => {
            let total_months = (date.year() * 12 + date.month() as i32) as i64 + value;
            let new_year = (total_months / 12) as i32;
            let new_month = (total_months % 12) as u32;
            let new_month = if new_month == 0 { 12 } else { new_month };
            let new_year = if new_month == 12 {
                new_year - 1
            } else {
                new_year
            };

            let max_day = days_in_month(new_year, new_month);
            let new_day = date.day().min(max_day);

            Ok(NaiveDate::from_ymd_opt(new_year, new_month, new_day))
        }
        "QUARTER" => add_date_interval(date, value * 3, "MONTH"),
        "YEAR" => add_date_interval(date, value * 12, "MONTH"),
        _ => Err(Error::invalid_query(format!(
            "Unknown interval unit for GENERATE_DATE_ARRAY: {}. Supported: DAY, WEEK, MONTH, QUARTER, YEAR",
            unit
        ))),
    }
}

fn days_in_month(year: i32, month: u32) -> u32 {
    use chrono::NaiveDate;

    let next_month_first = if month == 12 {
        NaiveDate::from_ymd_opt(year + 1, 1, 1)
    } else {
        NaiveDate::from_ymd_opt(year, month + 1, 1)
    };

    match next_month_first {
        Some(next) => (next - Duration::days(1)).day(),
        None => 31,
    }
}

pub fn generate_timestamp_array(
    start_ts: &Value,
    end_ts: &Value,
    interval_val: &Value,
    interval_unit: &Value,
) -> Result<Value> {
    let start = if start_ts.is_null() {
        return Ok(Value::null());
    } else if let Some(ts) = start_ts.as_timestamp() {
        ts
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_TIMESTAMP_ARRAY start must be TIMESTAMP, got {}",
            start_ts.data_type()
        )));
    };

    let end = if end_ts.is_null() {
        return Ok(Value::null());
    } else if let Some(ts) = end_ts.as_timestamp() {
        ts
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_TIMESTAMP_ARRAY end must be TIMESTAMP, got {}",
            end_ts.data_type()
        )));
    };

    let step = if interval_val.is_null() {
        return Ok(Value::null());
    } else if let Some(i) = interval_val.as_i64() {
        i
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_TIMESTAMP_ARRAY interval value must be INT64, got {}",
            interval_val.data_type()
        )));
    };

    if step == 0 {
        return Err(Error::invalid_query(
            "GENERATE_TIMESTAMP_ARRAY interval cannot be 0".to_string(),
        ));
    }

    let unit = if interval_unit.is_null() {
        return Ok(Value::null());
    } else if let Some(s) = interval_unit.as_str() {
        s.to_uppercase()
    } else {
        return Err(Error::invalid_query(format!(
            "GENERATE_TIMESTAMP_ARRAY interval unit must be STRING, got {}",
            interval_unit.data_type()
        )));
    };

    let elements = generate_timestamp_array_elements(start, end, step, &unit)?;
    Ok(Value::array(elements))
}

fn generate_timestamp_array_elements(
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    step: i64,
    unit: &str,
) -> Result<Vec<Value>> {
    let mut result = Vec::new();

    let is_ascending = start <= end;
    let step_is_ascending = step > 0;

    if is_ascending != step_is_ascending {
        return Ok(result);
    }

    let mut current = start;

    let should_continue =
        |curr: DateTime<Utc>| -> bool { if step > 0 { curr <= end } else { curr >= end } };

    while should_continue(current) {
        result.push(Value::timestamp(current));

        match add_timestamp_interval(current, step, unit)? {
            Some(next_ts) => {
                if !should_continue(next_ts) {
                    break;
                }
                current = next_ts;
            }
            None => break,
        }
    }

    Ok(result)
}

fn add_timestamp_interval(
    ts: DateTime<Utc>,
    value: i64,
    unit: &str,
) -> Result<Option<DateTime<Utc>>> {
    use chrono::Duration;

    match unit {
        "SECOND" => Ok(ts.checked_add_signed(Duration::seconds(value))),
        "MINUTE" => Ok(ts.checked_add_signed(Duration::minutes(value))),
        "HOUR" => Ok(ts.checked_add_signed(Duration::hours(value))),
        "DAY" => Ok(ts.checked_add_signed(Duration::days(value))),
        "WEEK" => Ok(ts.checked_add_signed(Duration::weeks(value))),
        "MONTH" => {
            let date = ts.date_naive();
            let time = ts.time();

            match add_date_interval(date, value, "MONTH")? {
                Some(new_date) => {
                    let naive_dt = new_date.and_time(time);
                    Ok(Some(DateTime::from_naive_utc_and_offset(naive_dt, Utc)))
                }
                None => Ok(None),
            }
        }
        "YEAR" => add_timestamp_interval(ts, value * 12, "MONTH"),
        _ => Err(Error::invalid_query(format!(
            "Unknown interval unit for GENERATE_TIMESTAMP_ARRAY: {}. Supported: SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR",
            unit
        ))),
    }
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;

    #[test]
    fn test_timestamp_diff_seconds() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 30).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let diff = timestamp_diff(&ts1, &ts2, "SECOND").unwrap();
        assert_eq!(diff, 30);
    }

    #[test]
    fn test_timestamp_diff_hours() {
        let ts1 = Utc.with_ymd_and_hms(2024, 1, 1, 15, 0, 0).unwrap();
        let ts2 = Utc.with_ymd_and_hms(2024, 1, 1, 12, 0, 0).unwrap();

        let diff = timestamp_diff(&ts1, &ts2, "HOUR").unwrap();
        assert_eq!(diff, 3);
    }

    #[test]
    fn test_truncate_to_day() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 45).unwrap();
        let truncated = truncate_to_day(&ts).unwrap();

        assert_eq!(truncated.hour(), 0);
        assert_eq!(truncated.minute(), 0);
        assert_eq!(truncated.second(), 0);
        assert_eq!(truncated.day(), 15);
    }

    #[test]
    fn test_truncate_to_month() {
        let ts = Utc.with_ymd_and_hms(2024, 3, 15, 14, 30, 45).unwrap();
        let truncated = truncate_to_month(&ts).unwrap();

        assert_eq!(truncated.day(), 1);
        assert_eq!(truncated.month(), 3);
        assert_eq!(truncated.hour(), 0);
    }

    #[test]
    fn test_truncate_to_year() {
        let ts = Utc.with_ymd_and_hms(2024, 6, 15, 14, 30, 45).unwrap();
        let truncated = truncate_to_year(&ts).unwrap();

        assert_eq!(truncated.month(), 1);
        assert_eq!(truncated.day(), 1);
        assert_eq!(truncated.hour(), 0);
    }

    #[test]
    fn test_generate_timestamp_array_hourly() {
        let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2024, 1, 1, 3, 0, 0).unwrap();
        let result = generate_timestamp_array(
            &Value::timestamp(start),
            &Value::timestamp(end),
            &Value::int64(1),
            &Value::string("HOUR".to_string()),
        )
        .unwrap();

        let array = result.as_array().expect("expected array result");
        assert_eq!(array.len(), 4);
        for (i, value) in array.iter().enumerate() {
            if let Some(ts) = value.as_timestamp() {
                let expected = Utc.with_ymd_and_hms(2024, 1, 1, i as u32, 0, 0).unwrap();
                assert_eq!(ts, expected);
            } else {
                panic!("expected timestamp element, found {:?}", value);
            }
        }
    }
}
