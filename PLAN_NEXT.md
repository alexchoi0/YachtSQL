# PLAN_12: BigQuery DateTime Functions (32 tests)

## Overview
Implement BigQuery date and time manipulation functions.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/bigquery/functions/datetime.rs`

---

## Functions to Implement

### Current Date/Time Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `CURRENT_DATE()` | Current date | DATE |
| `CURRENT_DATETIME()` | Current datetime | DATETIME |
| `CURRENT_TIME()` | Current time | TIME |
| `CURRENT_TIMESTAMP()` | Current timestamp | TIMESTAMP |

### Date Construction

| Function | Description | Return Type |
|----------|-------------|-------------|
| `DATE(year, month, day)` | Construct date | DATE |
| `DATE(timestamp)` | Extract date from timestamp | DATE |
| `DATE(datetime)` | Extract date from datetime | DATE |
| `DATE(string)` | Parse date from string | DATE |
| `DATETIME(year, month, day, hour, min, sec)` | Construct datetime | DATETIME |
| `DATETIME(date, time)` | Combine date and time | DATETIME |
| `TIME(hour, minute, second)` | Construct time | TIME |
| `TIMESTAMP(string)` | Parse timestamp | TIMESTAMP |

### Date Extraction

| Function | Description | Return Type |
|----------|-------------|-------------|
| `EXTRACT(part FROM date)` | Extract date part | INT64 |
| `DATE_TRUNC(date, part)` | Truncate to part | DATE |
| `LAST_DAY(date, part)` | Last day of period | DATE |

Supported parts: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `MICROSECOND`

### Date Arithmetic

| Function | Description | Return Type |
|----------|-------------|-------------|
| `DATE_ADD(date, INTERVAL n part)` | Add interval | DATE |
| `DATE_SUB(date, INTERVAL n part)` | Subtract interval | DATE |
| `DATETIME_ADD(datetime, INTERVAL n part)` | Add interval | DATETIME |
| `DATETIME_SUB(datetime, INTERVAL n part)` | Subtract interval | DATETIME |
| `TIMESTAMP_ADD(timestamp, INTERVAL n part)` | Add interval | TIMESTAMP |
| `TIMESTAMP_SUB(timestamp, INTERVAL n part)` | Subtract interval | TIMESTAMP |
| `DATE_DIFF(date1, date2, part)` | Difference in parts | INT64 |
| `DATETIME_DIFF(dt1, dt2, part)` | Difference in parts | INT64 |
| `TIMESTAMP_DIFF(ts1, ts2, part)` | Difference in parts | INT64 |

### Formatting and Parsing

| Function | Description | Return Type |
|----------|-------------|-------------|
| `FORMAT_DATE(format, date)` | Format date | STRING |
| `FORMAT_DATETIME(format, datetime)` | Format datetime | STRING |
| `FORMAT_TIME(format, time)` | Format time | STRING |
| `FORMAT_TIMESTAMP(format, timestamp)` | Format timestamp | STRING |
| `PARSE_DATE(format, string)` | Parse date | DATE |
| `PARSE_DATETIME(format, string)` | Parse datetime | DATETIME |
| `PARSE_TIME(format, string)` | Parse time | TIME |
| `PARSE_TIMESTAMP(format, string)` | Parse timestamp | TIMESTAMP |

### Unix Timestamp Conversion

| Function | Description | Return Type |
|----------|-------------|-------------|
| `UNIX_DATE(date)` | Days since 1970-01-01 | INT64 |
| `UNIX_SECONDS(timestamp)` | Seconds since epoch | INT64 |
| `UNIX_MILLIS(timestamp)` | Milliseconds since epoch | INT64 |
| `UNIX_MICROS(timestamp)` | Microseconds since epoch | INT64 |
| `TIMESTAMP_SECONDS(seconds)` | Timestamp from seconds | TIMESTAMP |
| `TIMESTAMP_MILLIS(millis)` | Timestamp from millis | TIMESTAMP |
| `TIMESTAMP_MICROS(micros)` | Timestamp from micros | TIMESTAMP |

---

## Implementation Details

### Date Construction

```rust
pub fn date_from_parts(year: i64, month: i64, day: i64) -> Result<Value> {
    let date = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
        .ok_or_else(|| Error::invalid_date(year, month, day))?;
    Ok(Value::date(date))
}

pub fn datetime_from_parts(
    year: i64, month: i64, day: i64,
    hour: i64, minute: i64, second: i64
) -> Result<Value> {
    let date = NaiveDate::from_ymd_opt(year as i32, month as u32, day as u32)
        .ok_or_else(|| Error::invalid_date(year, month, day))?;
    let time = NaiveTime::from_hms_opt(hour as u32, minute as u32, second as u32)
        .ok_or_else(|| Error::invalid_time(hour, minute, second))?;
    Ok(Value::datetime(NaiveDateTime::new(date, time)))
}
```

### EXTRACT Function

```rust
pub fn extract_from_date(part: DatePart, date: NaiveDate) -> Result<Value> {
    let value = match part {
        DatePart::Year => date.year() as i64,
        DatePart::Quarter => ((date.month() - 1) / 3 + 1) as i64,
        DatePart::Month => date.month() as i64,
        DatePart::Week => date.iso_week().week() as i64,
        DatePart::Day => date.day() as i64,
        DatePart::DayOfWeek => date.weekday().num_days_from_sunday() as i64 + 1,
        DatePart::DayOfYear => date.ordinal() as i64,
        _ => return Err(Error::invalid_date_part(part, "DATE")),
    };
    Ok(Value::int64(value))
}

pub fn extract_from_datetime(part: DatePart, dt: NaiveDateTime) -> Result<Value> {
    let value = match part {
        DatePart::Year => dt.year() as i64,
        DatePart::Quarter => ((dt.month() - 1) / 3 + 1) as i64,
        DatePart::Month => dt.month() as i64,
        DatePart::Week => dt.iso_week().week() as i64,
        DatePart::Day => dt.day() as i64,
        DatePart::Hour => dt.hour() as i64,
        DatePart::Minute => dt.minute() as i64,
        DatePart::Second => dt.second() as i64,
        DatePart::Millisecond => (dt.nanosecond() / 1_000_000) as i64,
        DatePart::Microsecond => (dt.nanosecond() / 1_000) as i64,
        DatePart::DayOfWeek => dt.weekday().num_days_from_sunday() as i64 + 1,
        DatePart::DayOfYear => dt.ordinal() as i64,
    };
    Ok(Value::int64(value))
}
```

### DATE_ADD/DATE_SUB

```rust
pub fn date_add(date: NaiveDate, interval: i64, part: DatePart) -> Result<Value> {
    let result = match part {
        DatePart::Day => date + chrono::Duration::days(interval),
        DatePart::Week => date + chrono::Duration::weeks(interval),
        DatePart::Month => add_months(date, interval as i32)?,
        DatePart::Quarter => add_months(date, (interval * 3) as i32)?,
        DatePart::Year => add_months(date, (interval * 12) as i32)?,
        _ => return Err(Error::invalid_interval_part(part, "DATE_ADD")),
    };
    Ok(Value::date(result))
}

fn add_months(date: NaiveDate, months: i32) -> Result<NaiveDate> {
    let total_months = date.year() * 12 + date.month() as i32 - 1 + months;
    let new_year = total_months / 12;
    let new_month = (total_months % 12 + 1) as u32;

    // Handle end-of-month edge cases
    let max_day = days_in_month(new_year, new_month);
    let new_day = date.day().min(max_day);

    NaiveDate::from_ymd_opt(new_year, new_month, new_day)
        .ok_or_else(|| Error::invalid_date(new_year as i64, new_month as i64, new_day as i64))
}
```

### DATE_DIFF

```rust
pub fn date_diff(date1: NaiveDate, date2: NaiveDate, part: DatePart) -> Result<Value> {
    let diff = match part {
        DatePart::Day => (date1 - date2).num_days(),
        DatePart::Week => (date1 - date2).num_weeks(),
        DatePart::Month => {
            let months1 = date1.year() * 12 + date1.month() as i32;
            let months2 = date2.year() * 12 + date2.month() as i32;
            (months1 - months2) as i64
        }
        DatePart::Quarter => {
            let q1 = date1.year() * 4 + ((date1.month() - 1) / 3) as i32;
            let q2 = date2.year() * 4 + ((date2.month() - 1) / 3) as i32;
            (q1 - q2) as i64
        }
        DatePart::Year => (date1.year() - date2.year()) as i64,
        _ => return Err(Error::invalid_date_part(part, "DATE_DIFF")),
    };
    Ok(Value::int64(diff))
}
```

### DATE_TRUNC

```rust
pub fn date_trunc(date: NaiveDate, part: DatePart) -> Result<Value> {
    let result = match part {
        DatePart::Year => NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap(),
        DatePart::Quarter => {
            let quarter_month = ((date.month() - 1) / 3) * 3 + 1;
            NaiveDate::from_ymd_opt(date.year(), quarter_month, 1).unwrap()
        }
        DatePart::Month => NaiveDate::from_ymd_opt(date.year(), date.month(), 1).unwrap(),
        DatePart::Week => {
            let days_from_monday = date.weekday().num_days_from_monday();
            date - chrono::Duration::days(days_from_monday as i64)
        }
        DatePart::Day => date,
        _ => return Err(Error::invalid_date_part(part, "DATE_TRUNC")),
    };
    Ok(Value::date(result))
}
```

### FORMAT_DATE

```rust
pub fn format_date(format: &str, date: NaiveDate) -> Result<Value> {
    // BigQuery format specifiers
    let result = format
        .replace("%Y", &format!("{:04}", date.year()))
        .replace("%m", &format!("{:02}", date.month()))
        .replace("%d", &format!("{:02}", date.day()))
        .replace("%B", &month_name(date.month()))
        .replace("%b", &month_abbrev(date.month()))
        .replace("%A", &weekday_name(date.weekday()))
        .replace("%a", &weekday_abbrev(date.weekday()))
        .replace("%j", &format!("{:03}", date.ordinal()))
        .replace("%W", &format!("{:02}", date.iso_week().week()))
        .replace("%F", &date.format("%Y-%m-%d").to_string());

    Ok(Value::string(result))
}

fn month_name(month: u32) -> String {
    match month {
        1 => "January", 2 => "February", 3 => "March", 4 => "April",
        5 => "May", 6 => "June", 7 => "July", 8 => "August",
        9 => "September", 10 => "October", 11 => "November", 12 => "December",
        _ => "Unknown",
    }.to_string()
}
```

### PARSE_DATE

```rust
pub fn parse_date(format: &str, s: &str) -> Result<Value> {
    // Convert BigQuery format to chrono format
    let chrono_format = format
        .replace("%Y", "%Y")
        .replace("%m", "%m")
        .replace("%d", "%d")
        .replace("%F", "%Y-%m-%d");

    let date = NaiveDate::parse_from_str(s, &chrono_format)
        .map_err(|e| Error::parse_error("DATE", s, e))?;

    Ok(Value::date(date))
}
```

### Unix Timestamp Conversions

```rust
pub fn unix_date(date: NaiveDate) -> Result<Value> {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    let days = (date - epoch).num_days();
    Ok(Value::int64(days))
}

pub fn unix_seconds(ts: DateTime<Utc>) -> Result<Value> {
    Ok(Value::int64(ts.timestamp()))
}

pub fn unix_millis(ts: DateTime<Utc>) -> Result<Value> {
    Ok(Value::int64(ts.timestamp_millis()))
}

pub fn timestamp_seconds(seconds: i64) -> Result<Value> {
    let ts = DateTime::from_timestamp(seconds, 0)
        .ok_or_else(|| Error::invalid_timestamp(seconds))?;
    Ok(Value::timestamp(ts))
}

pub fn timestamp_millis(millis: i64) -> Result<Value> {
    let ts = DateTime::from_timestamp_millis(millis)
        .ok_or_else(|| Error::invalid_timestamp(millis))?;
    Ok(Value::timestamp(ts))
}
```

---

## Key Files to Modify

1. **Functions:** `crates/functions/src/datetime/bigquery.rs`
   - All BigQuery-specific datetime functions

2. **Registry:** `crates/functions/src/registry/datetime_funcs.rs`
   - Register all datetime functions

3. **Parser:** Ensure INTERVAL parsing works for BigQuery syntax

---

## Implementation Order

### Phase 1: Current Functions
1. `CURRENT_DATE()`, `CURRENT_DATETIME()`
2. `CURRENT_TIME()`, `CURRENT_TIMESTAMP()`

### Phase 2: Construction
1. `DATE(year, month, day)`
2. `DATETIME(...)`, `TIME(...)`

### Phase 3: Extraction
1. `EXTRACT(part FROM date/datetime)`
2. `DATE_TRUNC(date, part)`

### Phase 4: Arithmetic
1. `DATE_ADD`, `DATE_SUB`
2. `DATETIME_ADD`, `DATETIME_SUB`
3. `DATE_DIFF`, `DATETIME_DIFF`

### Phase 5: Formatting/Parsing
1. `FORMAT_DATE`, `FORMAT_DATETIME`
2. `PARSE_DATE`, `PARSE_DATETIME`

### Phase 6: Unix Timestamps
1. `UNIX_DATE`, `UNIX_SECONDS`
2. `TIMESTAMP_SECONDS`, `TIMESTAMP_MILLIS`

---

## Testing Pattern

```rust
#[test]
fn test_date_construction() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT DATE(2024, 1, 15)").unwrap();
    assert_batch_eq!(result, [[d(2024, 1, 15)]]);
}

#[test]
fn test_extract() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT EXTRACT(YEAR FROM DATE '2024-03-15')").unwrap();
    assert_batch_eq!(result, [[2024]]);

    let result = executor.execute_sql("SELECT EXTRACT(MONTH FROM DATE '2024-03-15')").unwrap();
    assert_batch_eq!(result, [[3]]);
}

#[test]
fn test_date_add() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT DATE_ADD(DATE '2024-01-15', INTERVAL 10 DAY)"
    ).unwrap();
    assert_batch_eq!(result, [[d(2024, 1, 25)]]);
}

#[test]
fn test_date_diff() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT DATE_DIFF(DATE '2024-03-15', DATE '2024-01-15', DAY)"
    ).unwrap();
    assert_batch_eq!(result, [[59]]);
}

#[test]
fn test_format_date() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT FORMAT_DATE('%Y-%m-%d', DATE '2024-01-15')"
    ).unwrap();
    assert_batch_eq!(result, [["2024-01-15"]]);
}
```

---

## Verification Steps

1. Run: `cargo test --test bigquery -- functions::datetime --ignored`
2. Implement current date/time functions
3. Add construction and extraction
4. Add arithmetic functions
5. Add formatting and parsing
6. Remove `#[ignore = "Implement me!"]` as tests pass
