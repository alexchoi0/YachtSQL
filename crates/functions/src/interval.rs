use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{Interval, Value};

pub fn make_interval(
    years: i32,
    months: i32,
    days: i32,
    hours: i32,
    minutes: i32,
    seconds: f64,
) -> Result<Value> {
    let total_months = years * 12 + months;
    let total_micros = ((hours as i64) * 3_600_000_000)
        + ((minutes as i64) * 60_000_000)
        + (seconds * 1_000_000.0) as i64;

    Ok(Value::interval(Interval {
        months: total_months,
        days,
        micros: total_micros,
    }))
}

pub fn interval_add(interval1: &Value, interval2: &Value) -> Result<Value> {
    if interval1.is_null() || interval2.is_null() {
        return Ok(Value::null());
    }

    let i1 = interval1.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval1.data_type().to_string(),
    })?;

    let i2 = interval2.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval2.data_type().to_string(),
    })?;

    Ok(Value::interval(Interval {
        months: i1.months + i2.months,
        days: i1.days + i2.days,
        micros: i1.micros + i2.micros,
    }))
}

pub fn interval_subtract(interval1: &Value, interval2: &Value) -> Result<Value> {
    if interval1.is_null() || interval2.is_null() {
        return Ok(Value::null());
    }

    let i1 = interval1.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval1.data_type().to_string(),
    })?;

    let i2 = interval2.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval2.data_type().to_string(),
    })?;

    Ok(Value::interval(Interval {
        months: i1.months - i2.months,
        days: i1.days - i2.days,
        micros: i1.micros - i2.micros,
    }))
}

pub fn interval_multiply(interval: &Value, factor: &Value) -> Result<Value> {
    if interval.is_null() || factor.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    let f = factor.as_f64().ok_or_else(|| Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: factor.data_type().to_string(),
    })?;

    Ok(Value::interval(Interval {
        months: (i.months as f64 * f) as i32,
        days: (i.days as f64 * f) as i32,
        micros: (i.micros as f64 * f) as i64,
    }))
}

pub fn interval_divide(interval: &Value, divisor: &Value) -> Result<Value> {
    if interval.is_null() || divisor.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    let d = divisor.as_f64().ok_or_else(|| Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: divisor.data_type().to_string(),
    })?;

    if d == 0.0 {
        return Err(Error::invalid_query("Division by zero".to_string()));
    }

    Ok(Value::interval(Interval {
        months: (i.months as f64 / d) as i32,
        days: (i.days as f64 / d) as i32,
        micros: (i.micros as f64 / d) as i64,
    }))
}

pub fn interval_negate(interval: &Value) -> Result<Value> {
    if interval.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    Ok(Value::interval(Interval {
        months: -i.months,
        days: -i.days,
        micros: -i.micros,
    }))
}

pub fn interval_extract(interval: &Value, field: &str) -> Result<Value> {
    if interval.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    match field.to_lowercase().as_str() {
        "year" => Ok(Value::int64((i.months / 12) as i64)),
        "month" => Ok(Value::int64((i.months % 12) as i64)),
        "day" => Ok(Value::int64(i.days as i64)),
        "hour" => Ok(Value::int64(i.micros / 3_600_000_000)),
        "minute" => Ok(Value::int64((i.micros % 3_600_000_000) / 60_000_000)),
        "second" => Ok(Value::float64(
            ((i.micros % 60_000_000) as f64) / 1_000_000.0,
        )),
        _ => Err(Error::invalid_query(format!(
            "Invalid interval field: {}",
            field
        ))),
    }
}

pub fn interval_to_seconds(interval: &Value) -> Result<Value> {
    if interval.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    let total_seconds = (i.months as f64 * 30.0 * 24.0 * 3600.0)
        + (i.days as f64 * 24.0 * 3600.0)
        + (i.micros as f64 / 1_000_000.0);

    Ok(Value::float64(total_seconds))
}

pub fn justify_days(interval: &Value) -> Result<Value> {
    if interval.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    let extra_months = i.days / 30;
    let remaining_days = i.days % 30;

    Ok(Value::interval(Interval {
        months: i.months + extra_months,
        days: remaining_days,
        micros: i.micros,
    }))
}

pub fn justify_hours(interval: &Value) -> Result<Value> {
    if interval.is_null() {
        return Ok(Value::null());
    }

    let i = interval.as_interval().ok_or_else(|| Error::TypeMismatch {
        expected: "INTERVAL".to_string(),
        actual: interval.data_type().to_string(),
    })?;

    let extra_days = i.micros / (24 * 3_600_000_000);
    let remaining_micros = i.micros % (24 * 3_600_000_000);

    Ok(Value::interval(Interval {
        months: i.months,
        days: i.days + extra_days as i32,
        micros: remaining_micros,
    }))
}

pub fn justify_interval(interval: &Value) -> Result<Value> {
    let hours_justified = justify_hours(interval)?;
    justify_days(&hours_justified)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_interval() {
        let interval = make_interval(1, 2, 3, 4, 5, 6.5).unwrap();
        let i = interval.as_interval().unwrap();
        assert_eq!(i.months, 14);
        assert_eq!(i.days, 3);
        assert_eq!(i.micros, 14_706_500_000);
    }

    #[test]
    fn test_interval_add() {
        let i1 = make_interval(1, 0, 0, 0, 0, 0.0).unwrap();
        let i2 = make_interval(0, 6, 0, 0, 0, 0.0).unwrap();
        let result = interval_add(&i1, &i2).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.months, 18);
    }

    #[test]
    fn test_interval_subtract() {
        let i1 = make_interval(2, 0, 0, 0, 0, 0.0).unwrap();
        let i2 = make_interval(1, 0, 0, 0, 0, 0.0).unwrap();
        let result = interval_subtract(&i1, &i2).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.months, 12);
    }

    #[test]
    fn test_interval_multiply() {
        let i = make_interval(1, 0, 2, 0, 0, 0.0).unwrap();
        let factor = Value::float64(2.0);
        let result = interval_multiply(&i, &factor).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.months, 24);
        assert_eq!(r.days, 4);
    }

    #[test]
    fn test_interval_divide() {
        let i = make_interval(2, 0, 4, 0, 0, 0.0).unwrap();
        let divisor = Value::float64(2.0);
        let result = interval_divide(&i, &divisor).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.months, 12);
        assert_eq!(r.days, 2);
    }

    #[test]
    fn test_interval_negate() {
        let i = make_interval(1, 2, 3, 4, 5, 6.0).unwrap();
        let result = interval_negate(&i).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.months, -14);
        assert_eq!(r.days, -3);
        assert!(r.micros < 0);
    }

    #[test]
    fn test_interval_extract() {
        let i = make_interval(1, 3, 5, 2, 30, 45.5).unwrap();

        let year = interval_extract(&i, "year").unwrap();
        assert_eq!(year.as_i64().unwrap(), 1);

        let month = interval_extract(&i, "month").unwrap();
        assert_eq!(month.as_i64().unwrap(), 3);

        let day = interval_extract(&i, "day").unwrap();
        assert_eq!(day.as_i64().unwrap(), 5);
    }

    #[test]
    fn test_justify_hours() {
        let i = make_interval(0, 0, 0, 25, 0, 0.0).unwrap();
        let result = justify_hours(&i).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.days, 1);
        assert_eq!(r.micros, 3_600_000_000);
    }

    #[test]
    fn test_justify_days() {
        let i = make_interval(0, 0, 31, 0, 0, 0.0).unwrap();
        let result = justify_days(&i).unwrap();
        let r = result.as_interval().unwrap();
        assert_eq!(r.months, 1);
        assert_eq!(r.days, 1);
    }
}
