use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{Range, RangeType, Value};

pub fn make_range(
    range_type: RangeType,
    lower: Option<Value>,
    upper: Option<Value>,
    bounds: &str,
) -> Result<Value> {
    if bounds.len() != 2 {
        return Err(Error::invalid_query(format!(
            "Invalid range bounds specification: {}",
            bounds
        )));
    }

    let lower_inclusive = bounds.starts_with('[');
    let upper_inclusive = bounds.chars().nth(1).unwrap() == ']';

    if let (Some(l), Some(u)) = (&lower, &upper) {
        if !value_less_than_or_equal(l, u)? {
            return Err(Error::invalid_query(
                "Range lower bound must be less than or equal to upper bound".to_string(),
            ));
        }
    }

    Ok(Value::range(Range {
        range_type,
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
    }))
}

pub fn range_contains(range: &Value, value: &Value) -> Result<Value> {
    if range.is_null() || value.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    if let Some(ref lower) = r.lower {
        if r.lower_inclusive {
            if value_less_than(value, lower)? {
                return Ok(Value::bool_val(false));
            }
        } else if value_less_than_or_equal(value, lower)? {
            return Ok(Value::bool_val(false));
        }
    }

    if let Some(ref upper) = r.upper {
        if r.upper_inclusive {
            if value_greater_than(value, upper)? {
                return Ok(Value::bool_val(false));
            }
        } else if value_greater_than_or_equal(value, upper)? {
            return Ok(Value::bool_val(false));
        }
    }

    Ok(Value::bool_val(true))
}

pub fn range_contains_range(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    match (&r1.lower, &r2.lower) {
        (None, _) => {}
        (Some(_), None) => return Ok(Value::bool_val(false)),
        (Some(l1), Some(l2)) => {
            if value_greater_than(l1, l2)? {
                return Ok(Value::bool_val(false));
            }
            if value_equal(l1, l2)? && !r1.lower_inclusive && r2.lower_inclusive {
                return Ok(Value::bool_val(false));
            }
        }
    }

    match (&r1.upper, &r2.upper) {
        (None, _) => {}
        (Some(_), None) => return Ok(Value::bool_val(false)),
        (Some(u1), Some(u2)) => {
            if value_less_than(u1, u2)? {
                return Ok(Value::bool_val(false));
            }
            if value_equal(u1, u2)? && !r1.upper_inclusive && r2.upper_inclusive {
                return Ok(Value::bool_val(false));
            }
        }
    }

    Ok(Value::bool_val(true))
}

pub fn range_overlaps(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    match (&r1.upper, &r2.lower) {
        (Some(u1), Some(l2)) => {
            if value_less_than(u1, l2)? {
                return Ok(Value::bool_val(false));
            }
            if value_equal(u1, l2)? && (!r1.upper_inclusive || !r2.lower_inclusive) {
                return Ok(Value::bool_val(false));
            }
        }
        _ => {}
    }

    match (&r2.upper, &r1.lower) {
        (Some(u2), Some(l1)) => {
            if value_less_than(u2, l1)? {
                return Ok(Value::bool_val(false));
            }
            if value_equal(u2, l1)? && (!r2.upper_inclusive || !r1.lower_inclusive) {
                return Ok(Value::bool_val(false));
            }
        }
        _ => {}
    }

    Ok(Value::bool_val(true))
}

pub fn range_union(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    let overlaps = range_overlaps(range1, range2)?;
    let adjacent = range_adjacent(range1, range2)?;
    if !overlaps.as_bool().unwrap_or(false) && !adjacent.as_bool().unwrap_or(false) {
        return Err(Error::invalid_query(
            "Cannot union non-overlapping and non-adjacent ranges".to_string(),
        ));
    }

    let (new_lower, new_lower_inclusive) = match (&r1.lower, &r2.lower) {
        (None, _) | (_, None) => (None, true),
        (Some(l1), Some(l2)) => {
            if value_less_than(l1, l2)? {
                (Some(l1.clone()), r1.lower_inclusive)
            } else if value_less_than(l2, l1)? {
                (Some(l2.clone()), r2.lower_inclusive)
            } else {
                (Some(l1.clone()), r1.lower_inclusive || r2.lower_inclusive)
            }
        }
    };

    let (new_upper, new_upper_inclusive) = match (&r1.upper, &r2.upper) {
        (None, _) | (_, None) => (None, true),
        (Some(u1), Some(u2)) => {
            if value_greater_than(u1, u2)? {
                (Some(u1.clone()), r1.upper_inclusive)
            } else if value_greater_than(u2, u1)? {
                (Some(u2.clone()), r2.upper_inclusive)
            } else {
                (Some(u1.clone()), r1.upper_inclusive || r2.upper_inclusive)
            }
        }
    };

    Ok(Value::range(Range {
        range_type: r1.range_type.clone(),
        lower: new_lower,
        upper: new_upper,
        lower_inclusive: new_lower_inclusive,
        upper_inclusive: new_upper_inclusive,
    }))
}

pub fn range_intersection(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    let overlaps = range_overlaps(range1, range2)?;
    if !overlaps.as_bool().unwrap_or(false) {
        return Ok(Value::range(Range {
            range_type: r1.range_type.clone(),
            lower: Some(Value::int64(0)),
            upper: Some(Value::int64(0)),
            lower_inclusive: false,
            upper_inclusive: false,
        }));
    }

    let (new_lower, new_lower_inclusive) = match (&r1.lower, &r2.lower) {
        (None, Some(l2)) => (Some(l2.clone()), r2.lower_inclusive),
        (Some(l1), None) => (Some(l1.clone()), r1.lower_inclusive),
        (None, None) => (None, true),
        (Some(l1), Some(l2)) => {
            if value_greater_than(l1, l2)? {
                (Some(l1.clone()), r1.lower_inclusive)
            } else if value_greater_than(l2, l1)? {
                (Some(l2.clone()), r2.lower_inclusive)
            } else {
                (Some(l1.clone()), r1.lower_inclusive && r2.lower_inclusive)
            }
        }
    };

    let (new_upper, new_upper_inclusive) = match (&r1.upper, &r2.upper) {
        (None, Some(u2)) => (Some(u2.clone()), r2.upper_inclusive),
        (Some(u1), None) => (Some(u1.clone()), r1.upper_inclusive),
        (None, None) => (None, true),
        (Some(u1), Some(u2)) => {
            if value_less_than(u1, u2)? {
                (Some(u1.clone()), r1.upper_inclusive)
            } else if value_less_than(u2, u1)? {
                (Some(u2.clone()), r2.upper_inclusive)
            } else {
                (Some(u1.clone()), r1.upper_inclusive && r2.upper_inclusive)
            }
        }
    };

    Ok(Value::range(Range {
        range_type: r1.range_type.clone(),
        lower: new_lower,
        upper: new_upper,
        lower_inclusive: new_lower_inclusive,
        upper_inclusive: new_upper_inclusive,
    }))
}

pub fn range_is_empty(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    if r.lower.is_none() && r.upper.is_none() && !r.lower_inclusive && !r.upper_inclusive {
        return Ok(Value::bool_val(true));
    }

    match (&r.lower, &r.upper) {
        (Some(l), Some(u)) => {
            if value_greater_than(l, u)? {
                return Ok(Value::bool_val(true));
            }
            if value_equal(l, u)? && (!r.lower_inclusive || !r.upper_inclusive) {
                return Ok(Value::bool_val(true));
            }
        }
        _ => {}
    }

    Ok(Value::bool_val(false))
}

pub fn range_lower(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    match &r.lower {
        Some(l) => Ok(l.clone()),
        None => Ok(Value::null()),
    }
}

pub fn range_upper(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    match &r.upper {
        Some(u) => Ok(u.clone()),
        None => Ok(Value::null()),
    }
}

pub fn range_lower_inc(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    Ok(Value::bool_val(r.lower_inclusive))
}

pub fn range_upper_inc(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    Ok(Value::bool_val(r.upper_inclusive))
}

pub fn range_lower_inf(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    Ok(Value::bool_val(r.lower.is_none()))
}

pub fn range_upper_inf(range: &Value) -> Result<Value> {
    if range.is_null() {
        return Ok(Value::null());
    }

    let r = range.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range.data_type().to_string(),
    })?;

    Ok(Value::bool_val(r.upper.is_none()))
}

pub fn range_contains_elem(range: &Value, elem: &Value) -> Result<Value> {
    range_contains(range, elem)
}

pub fn range_adjacent(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    if let (Some(u1), Some(l2)) = (&r1.upper, &r2.lower) {
        if value_equal(u1, l2)? && (r1.upper_inclusive != r2.lower_inclusive) {
            return Ok(Value::bool_val(true));
        }
    }

    if let (Some(u2), Some(l1)) = (&r2.upper, &r1.lower) {
        if value_equal(u2, l1)? && (r2.upper_inclusive != r1.lower_inclusive) {
            return Ok(Value::bool_val(true));
        }
    }

    Ok(Value::bool_val(false))
}

pub fn range_strictly_left(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    match (&r1.upper, &r2.lower) {
        (Some(u1), Some(l2)) => {
            if value_less_than(u1, l2)? {
                return Ok(Value::bool_val(true));
            }
            if value_equal(u1, l2)? && (!r1.upper_inclusive || !r2.lower_inclusive) {
                return Ok(Value::bool_val(true));
            }
            Ok(Value::bool_val(false))
        }
        (None, _) | (_, None) => Ok(Value::bool_val(false)),
    }
}

pub fn range_strictly_right(range1: &Value, range2: &Value) -> Result<Value> {
    range_strictly_left(range2, range1)
}

pub fn range_difference(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() || range2.is_null() {
        return Ok(Value::null());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    let overlaps = range_overlaps(range1, range2)?;
    if !overlaps.as_bool().unwrap_or(false) {
        return Ok(range1.clone());
    }

    let contains = range_contains_range(range2, range1)?;
    if contains.as_bool().unwrap_or(false) {
        return Ok(Value::range(Range {
            range_type: r1.range_type.clone(),
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        }));
    }

    let r2_lower_before_r1_lower = match (&r1.lower, &r2.lower) {
        (None, _) => false,
        (_, None) => true,
        (Some(l1), Some(l2)) => {
            value_less_than(l2, l1)?
                || (value_equal(l2, l1)? && r2.lower_inclusive && !r1.lower_inclusive)
        }
    };

    let r2_upper_after_r1_upper = match (&r1.upper, &r2.upper) {
        (None, _) => false,
        (_, None) => true,
        (Some(u1), Some(u2)) => {
            value_greater_than(u2, u1)?
                || (value_equal(u2, u1)? && r2.upper_inclusive && !r1.upper_inclusive)
        }
    };

    if r2_lower_before_r1_lower && r2_upper_after_r1_upper {
        return Ok(Value::range(Range {
            range_type: r1.range_type.clone(),
            lower: None,
            upper: None,
            lower_inclusive: false,
            upper_inclusive: false,
        }));
    }

    if r2_lower_before_r1_lower {
        let new_lower = r2.upper.clone();
        let new_lower_inclusive = !r2.upper_inclusive;
        return Ok(Value::range(Range {
            range_type: r1.range_type.clone(),
            lower: new_lower,
            upper: r1.upper.clone(),
            lower_inclusive: new_lower_inclusive,
            upper_inclusive: r1.upper_inclusive,
        }));
    }

    if r2_upper_after_r1_upper {
        let new_upper = r2.lower.clone();
        let new_upper_inclusive = !r2.lower_inclusive;
        return Ok(Value::range(Range {
            range_type: r1.range_type.clone(),
            lower: r1.lower.clone(),
            upper: new_upper,
            lower_inclusive: r1.lower_inclusive,
            upper_inclusive: new_upper_inclusive,
        }));
    }

    Err(Error::invalid_query(
        "Range difference would result in a non-contiguous range".to_string(),
    ))
}

pub fn range_merge(range1: &Value, range2: &Value) -> Result<Value> {
    if range1.is_null() {
        return Ok(range2.clone());
    }
    if range2.is_null() {
        return Ok(range1.clone());
    }

    let r1 = range1.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range1.data_type().to_string(),
    })?;

    let r2 = range2.as_range().ok_or_else(|| Error::TypeMismatch {
        expected: "RANGE".to_string(),
        actual: range2.data_type().to_string(),
    })?;

    let (new_lower, new_lower_inclusive) = match (&r1.lower, &r2.lower) {
        (None, _) | (_, None) => (None, true),
        (Some(l1), Some(l2)) => {
            if value_less_than(l1, l2)? {
                (Some(l1.clone()), r1.lower_inclusive)
            } else if value_less_than(l2, l1)? {
                (Some(l2.clone()), r2.lower_inclusive)
            } else {
                (Some(l1.clone()), r1.lower_inclusive || r2.lower_inclusive)
            }
        }
    };

    let (new_upper, new_upper_inclusive) = match (&r1.upper, &r2.upper) {
        (None, _) | (_, None) => (None, true),
        (Some(u1), Some(u2)) => {
            if value_greater_than(u1, u2)? {
                (Some(u1.clone()), r1.upper_inclusive)
            } else if value_greater_than(u2, u1)? {
                (Some(u2.clone()), r2.upper_inclusive)
            } else {
                (Some(u1.clone()), r1.upper_inclusive || r2.upper_inclusive)
            }
        }
    };

    Ok(Value::range(Range {
        range_type: r1.range_type.clone(),
        lower: new_lower,
        upper: new_upper,
        lower_inclusive: new_lower_inclusive,
        upper_inclusive: new_upper_inclusive,
    }))
}

fn value_less_than(v1: &Value, v2: &Value) -> Result<bool> {
    if let (Some(i1), Some(i2)) = (v1.as_i64(), v2.as_i64()) {
        return Ok(i1 < i2);
    }
    if let (Some(f1), Some(f2)) = (v1.as_f64(), v2.as_f64()) {
        return Ok(f1 < f2);
    }
    if let (Some(d1), Some(d2)) = (v1.as_date(), v2.as_date()) {
        return Ok(d1 < d2);
    }
    if let (Some(t1), Some(t2)) = (
        v1.as_timestamp().or_else(|| v1.as_datetime()),
        v2.as_timestamp().or_else(|| v2.as_datetime()),
    ) {
        return Ok(t1 < t2);
    }
    Err(Error::invalid_query("Cannot compare values".to_string()))
}

fn value_less_than_or_equal(v1: &Value, v2: &Value) -> Result<bool> {
    if let (Some(i1), Some(i2)) = (v1.as_i64(), v2.as_i64()) {
        return Ok(i1 <= i2);
    }
    if let (Some(f1), Some(f2)) = (v1.as_f64(), v2.as_f64()) {
        return Ok(f1 <= f2);
    }
    if let (Some(d1), Some(d2)) = (v1.as_date(), v2.as_date()) {
        return Ok(d1 <= d2);
    }
    if let (Some(t1), Some(t2)) = (
        v1.as_timestamp().or_else(|| v1.as_datetime()),
        v2.as_timestamp().or_else(|| v2.as_datetime()),
    ) {
        return Ok(t1 <= t2);
    }
    Err(Error::invalid_query("Cannot compare values".to_string()))
}

fn value_greater_than(v1: &Value, v2: &Value) -> Result<bool> {
    if let (Some(i1), Some(i2)) = (v1.as_i64(), v2.as_i64()) {
        return Ok(i1 > i2);
    }
    if let (Some(f1), Some(f2)) = (v1.as_f64(), v2.as_f64()) {
        return Ok(f1 > f2);
    }
    if let (Some(d1), Some(d2)) = (v1.as_date(), v2.as_date()) {
        return Ok(d1 > d2);
    }
    if let (Some(t1), Some(t2)) = (
        v1.as_timestamp().or_else(|| v1.as_datetime()),
        v2.as_timestamp().or_else(|| v2.as_datetime()),
    ) {
        return Ok(t1 > t2);
    }
    Err(Error::invalid_query("Cannot compare values".to_string()))
}

fn value_greater_than_or_equal(v1: &Value, v2: &Value) -> Result<bool> {
    if let (Some(i1), Some(i2)) = (v1.as_i64(), v2.as_i64()) {
        return Ok(i1 >= i2);
    }
    if let (Some(f1), Some(f2)) = (v1.as_f64(), v2.as_f64()) {
        return Ok(f1 >= f2);
    }
    if let (Some(d1), Some(d2)) = (v1.as_date(), v2.as_date()) {
        return Ok(d1 >= d2);
    }
    if let (Some(t1), Some(t2)) = (
        v1.as_timestamp().or_else(|| v1.as_datetime()),
        v2.as_timestamp().or_else(|| v2.as_datetime()),
    ) {
        return Ok(t1 >= t2);
    }
    Err(Error::invalid_query("Cannot compare values".to_string()))
}

fn value_equal(v1: &Value, v2: &Value) -> Result<bool> {
    Ok(v1 == v2)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_make_range() {
        let range = make_range(
            RangeType::Int4Range,
            Some(Value::int64(1)),
            Some(Value::int64(10)),
            "[)",
        )
        .unwrap();

        let r = range.as_range().unwrap();
        assert_eq!(r.range_type, RangeType::Int4Range);
        assert!(r.lower_inclusive);
        assert!(!r.upper_inclusive);
    }

    #[test]
    fn test_range_contains() {
        let range = make_range(
            RangeType::Int4Range,
            Some(Value::int64(1)),
            Some(Value::int64(10)),
            "[)",
        )
        .unwrap();

        assert!(
            range_contains(&range, &Value::int64(5))
                .unwrap()
                .as_bool()
                .unwrap()
        );
        assert!(
            !range_contains(&range, &Value::int64(10))
                .unwrap()
                .as_bool()
                .unwrap()
        );
        assert!(
            !range_contains(&range, &Value::int64(0))
                .unwrap()
                .as_bool()
                .unwrap()
        );
    }

    #[test]
    fn test_range_overlaps() {
        let r1 = make_range(
            RangeType::Int4Range,
            Some(Value::int64(1)),
            Some(Value::int64(10)),
            "[)",
        )
        .unwrap();

        let r2 = make_range(
            RangeType::Int4Range,
            Some(Value::int64(5)),
            Some(Value::int64(15)),
            "[)",
        )
        .unwrap();

        assert!(range_overlaps(&r1, &r2).unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_range_is_empty() {
        let r = make_range(
            RangeType::Int4Range,
            Some(Value::int64(5)),
            Some(Value::int64(5)),
            "()",
        )
        .unwrap();

        assert!(range_is_empty(&r).unwrap().as_bool().unwrap());
    }
}
