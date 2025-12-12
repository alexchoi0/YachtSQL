use crate::error::Result;
use crate::types::Value;
use crate::types::coercion::CoercionRules;

pub fn evaluate_tuple_in(
    left_tuple: &[Value],
    right_tuples: &[Vec<Value>],
    negated: bool,
) -> Result<Value> {
    if right_tuples.is_empty() {
        return Ok(Value::bool_val(negated));
    }

    if has_null(left_tuple) {
        return Ok(Value::null());
    }

    let common_types = find_common_types_for_columns(left_tuple, right_tuples)?;

    let coerced_left = coerce_tuple(left_tuple, &common_types)?;

    let match_result = find_tuple_match(&coerced_left, right_tuples, &common_types)?;

    Ok(apply_three_valued_logic(match_result, negated))
}

#[inline]
fn has_null(tuple: &[Value]) -> bool {
    tuple.iter().any(|v| v.is_null())
}

fn find_common_types_for_columns(
    left_tuple: &[Value],
    right_tuples: &[Vec<Value>],
) -> Result<Vec<crate::types::DataType>> {
    let num_columns = left_tuple.len();
    let mut common_types = Vec::with_capacity(num_columns);

    for col_idx in 0..num_columns {
        let mut types = Vec::new();

        if !left_tuple[col_idx].is_null() {
            types.push(left_tuple[col_idx].data_type());
        }

        for right_tuple in right_tuples {
            if col_idx < right_tuple.len() && !right_tuple[col_idx].is_null() {
                types.push(right_tuple[col_idx].data_type());
            }
        }

        if types.is_empty() {
            common_types.push(crate::types::DataType::String);
        } else {
            let common_type = CoercionRules::find_common_type(&types)?;
            common_types.push(common_type);
        }
    }

    Ok(common_types)
}

fn coerce_tuple(tuple: &[Value], target_types: &[crate::types::DataType]) -> Result<Vec<Value>> {
    tuple
        .iter()
        .zip(target_types)
        .map(|(val, target_type)| CoercionRules::coerce_value(val.clone(), target_type))
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MatchResult {
    Found,
    Unknown,
    NotFound,
}

fn find_tuple_match(
    coerced_left: &[Value],
    right_tuples: &[Vec<Value>],
    common_types: &[crate::types::DataType],
) -> Result<MatchResult> {
    let mut found_unknown = false;

    for right_tuple in right_tuples {
        let coerced_right = coerce_tuple(right_tuple, common_types)?;

        match compare_tuples(coerced_left, &coerced_right) {
            TupleComparison::Equal => return Ok(MatchResult::Found),
            TupleComparison::NotEqual => continue,
            TupleComparison::Unknown => found_unknown = true,
        }
    }

    if found_unknown {
        Ok(MatchResult::Unknown)
    } else {
        Ok(MatchResult::NotFound)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TupleComparison {
    Equal,
    NotEqual,
    Unknown,
}

#[inline]
fn compare_tuples(left: &[Value], right: &[Value]) -> TupleComparison {
    for (left_val, right_val) in left.iter().zip(right) {
        if left_val.is_null() || right_val.is_null() {
            return TupleComparison::Unknown;
        }
        if left_val != right_val {
            return TupleComparison::NotEqual;
        }
    }
    TupleComparison::Equal
}

#[inline]
fn apply_three_valued_logic(result: MatchResult, negated: bool) -> Value {
    match result {
        MatchResult::Found => Value::bool_val(!negated),
        MatchResult::Unknown => Value::null(),
        MatchResult::NotFound => Value::bool_val(negated),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_in_exact_match() {
        let left = vec![Value::int64(1), Value::int64(2)];
        let right = vec![
            vec![Value::int64(1), Value::int64(2)],
            vec![Value::int64(3), Value::int64(4)],
        ];

        let result = evaluate_tuple_in(&left, &right, false).unwrap();
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn test_tuple_in_no_match() {
        let left = vec![Value::int64(5), Value::int64(6)];
        let right = vec![
            vec![Value::int64(1), Value::int64(2)],
            vec![Value::int64(3), Value::int64(4)],
        ];

        let result = evaluate_tuple_in(&left, &right, false).unwrap();
        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn test_tuple_in_null_in_left() {
        let left = vec![Value::int64(1), Value::null()];
        let right = vec![vec![Value::int64(1), Value::int64(2)]];

        let result = evaluate_tuple_in(&left, &right, false).unwrap();
        assert_eq!(result, Value::null());
    }

    #[test]
    fn test_tuple_in_null_in_right() {
        let left = vec![Value::int64(1), Value::int64(2)];

        let right = vec![vec![Value::int64(1), Value::null()]];

        let result = evaluate_tuple_in(&left, &right, false);

        match result {
            Ok(val) if val == Value::null() => {}
            Err(_) => {}
            _ => panic!("Expected NULL or error"),
        }
    }

    #[test]
    fn test_tuple_in_empty_list() {
        let left = vec![Value::int64(1), Value::int64(2)];
        let right: Vec<Vec<Value>> = vec![];

        let result = evaluate_tuple_in(&left, &right, false).unwrap();
        assert_eq!(result, Value::bool_val(false));
    }

    #[test]
    fn test_tuple_not_in() {
        let left = vec![Value::int64(5), Value::int64(6)];
        let right = vec![vec![Value::int64(1), Value::int64(2)]];

        let result = evaluate_tuple_in(&left, &right, true).unwrap();
        assert_eq!(result, Value::bool_val(true));
    }

    #[test]
    fn test_tuple_in_type_coercion() {
        let left = vec![Value::int64(1), Value::float64(2.0)];
        let right = vec![vec![Value::float64(1.0), Value::int64(2)]];

        let result = evaluate_tuple_in(&left, &right, false).unwrap();
        assert_eq!(result, Value::bool_val(true));
    }
}
