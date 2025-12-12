use std::collections::HashSet;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub fn array_length(array: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        Ok(Value::int64(arr.len() as i64))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_length_dim(array: &Value, dimension: &Value) -> Result<Value> {
    if array.is_null() || dimension.is_null() {
        return Ok(Value::null());
    }

    let dim = dimension.as_i64().ok_or_else(|| Error::TypeMismatch {
        expected: "INT64".to_string(),
        actual: dimension.data_type().to_string(),
    })?;

    if dim != 1 {
        return Ok(Value::null());
    }

    let arr = array.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: array.data_type().to_string(),
    })?;

    if arr.is_empty() {
        return Ok(Value::null());
    }

    Ok(Value::int64(arr.len() as i64))
}

pub fn cardinality(array: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    fn count_elements(arr: &[Value]) -> i64 {
        let mut count = 0i64;
        for elem in arr {
            if let Some(nested) = elem.as_array() {
                count += count_elements(nested);
            } else {
                count += 1;
            }
        }
        count
    }

    let arr = array.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: array.data_type().to_string(),
    })?;

    Ok(Value::int64(count_elements(arr)))
}

pub fn array_dims(array: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    let arr = array.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: array.data_type().to_string(),
    })?;

    if arr.is_empty() {
        return Ok(Value::null());
    }

    fn get_dims(arr: &[Value]) -> Vec<usize> {
        let mut dims = vec![arr.len()];
        if let Some(first) = arr.first() {
            if let Some(nested) = first.as_array() {
                dims.extend(get_dims(nested));
            }
        }
        dims
    }

    let dims = get_dims(arr);
    let dims_str = dims
        .iter()
        .map(|&d| format!("[1:{}]", d))
        .collect::<String>();

    Ok(Value::string(dims_str))
}

pub fn array_concat(arrays: &[Value]) -> Result<Value> {
    let mut result = Vec::new();

    for array in arrays {
        if array.is_null() {
            return Ok(Value::null());
        }

        if let Some(arr) = array.as_array() {
            result.extend(arr.iter().cloned());
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY".to_string(),
                actual: array.data_type().to_string(),
            });
        }
    }

    Ok(Value::array(result))
}

pub fn array_reverse(array: Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let mut reversed = arr.to_vec();
        reversed.reverse();
        Ok(Value::array(reversed))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_append(array: Value, element: Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        if !arr.is_empty() && !element.is_null() {
            let first_elem = &arr[0];
            if !first_elem.is_null() {
                let array_elem_type = first_elem.data_type();
                let new_elem_type = element.data_type();
                if !types_are_compatible(&array_elem_type, &new_elem_type) {
                    return Err(Error::TypeMismatch {
                        expected: array_elem_type.to_string(),
                        actual: new_elem_type.to_string(),
                    });
                }
            }
        }
        let mut result = arr.to_vec();
        result.push(element);
        Ok(Value::array(result))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

fn types_are_compatible(
    type1: &yachtsql_common::types::DataType,
    type2: &yachtsql_common::types::DataType,
) -> bool {
    use yachtsql_common::types::DataType;

    if type1 == type2 {
        return true;
    }

    matches!(
        (type1, type2),
        (DataType::Int64, DataType::Float64) | (DataType::Float64, DataType::Int64)
    )
}

pub fn array_prepend(element: Value, array: Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let mut result = arr.to_vec();
        result.insert(0, element);
        Ok(Value::array(result))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_position(array: &Value, search: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        for (i, elem) in arr.iter().enumerate() {
            if values_equal(elem, search) {
                return Ok(Value::int64((i + 1) as i64));
            }
        }
        Ok(Value::null())
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_contains(array: &Value, search: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        for elem in arr.iter() {
            if values_equal(elem, search) {
                return Ok(Value::bool_val(true));
            }
        }
        Ok(Value::bool_val(false))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_contains_array(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    let left_arr = left.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: left.data_type().to_string(),
    })?;

    let right_arr = right.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: right.data_type().to_string(),
    })?;

    if right_arr.is_empty() {
        return Ok(Value::bool_val(true));
    }

    for right_elem in right_arr.iter() {
        let mut found = false;
        for left_elem in left_arr.iter() {
            if values_equal(left_elem, right_elem) {
                found = true;
                break;
            }
        }
        if !found {
            return Ok(Value::bool_val(false));
        }
    }

    Ok(Value::bool_val(true))
}

pub fn array_contained_by(left: &Value, right: &Value) -> Result<Value> {
    array_contains_array(right, left)
}

pub fn array_overlap(left: &Value, right: &Value) -> Result<Value> {
    if left.is_null() || right.is_null() {
        return Ok(Value::null());
    }

    let left_arr = left.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: left.data_type().to_string(),
    })?;

    let right_arr = right.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: right.data_type().to_string(),
    })?;

    if left_arr.is_empty() || right_arr.is_empty() {
        return Ok(Value::bool_val(false));
    }

    for left_elem in left_arr.iter() {
        for right_elem in right_arr.iter() {
            if values_equal(left_elem, right_elem) {
                return Ok(Value::bool_val(true));
            }
        }
    }

    Ok(Value::bool_val(false))
}

pub fn array_remove(array: Value, element: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let result: Vec<Value> = arr
            .iter()
            .filter(|elem| !values_equal(elem, element))
            .cloned()
            .collect();
        Ok(Value::array(result))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_replace(array: Value, old_value: &Value, new_value: &Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let result: Vec<Value> = arr
            .iter()
            .map(|elem| {
                if values_equal(elem, old_value) {
                    new_value.clone()
                } else {
                    elem.clone()
                }
            })
            .collect();
        Ok(Value::array(result))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_sort(array: Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let mut sorted = arr.to_vec();
        sorted.sort_by(|a, b| {
            use std::cmp::Ordering;
            if a.is_null() && b.is_null() {
                return Ordering::Equal;
            }
            if a.is_null() {
                return Ordering::Greater;
            }
            if b.is_null() {
                return Ordering::Less;
            }

            if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
                return x.cmp(&y);
            }
            if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
                return x.partial_cmp(&y).unwrap_or(Ordering::Equal);
            }
            if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
                return x.cmp(y);
            }
            Ordering::Equal
        });
        Ok(Value::array(sorted))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_distinct(array: Value) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let mut seen = HashSet::new();
        let mut result = Vec::new();

        for elem in arr.iter() {
            let key = format!("{:?}", elem);
            if seen.insert(key) {
                result.push(elem.clone());
            }
        }

        Ok(Value::array(result))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn values_equal(a: &Value, b: &Value) -> bool {
    if a.is_null() && b.is_null() {
        return true;
    }
    if a.is_null() || b.is_null() {
        return false;
    }

    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return (x - y).abs() < f64::EPSILON;
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x == y;
    }

    if let (Some(x), Some(y)) = (a.as_array(), b.as_array()) {
        return x.len() == y.len() && x.iter().zip(y.iter()).all(|(a, b)| values_equal(a, b));
    }

    false
}

fn convert_sql_index_to_zero_based(idx: i64, _len: i64) -> Option<i64> {
    if idx > 0 { Some(idx - 1) } else { None }
}

fn parse_optional_index(
    value: Option<&Value>,
    default: i64,
    param_name: &str,
) -> Result<Option<i64>> {
    match value {
        Some(v) if v.is_null() => Ok(None),
        Some(v) => {
            if let Some(idx) = v.as_i64() {
                Ok(Some(idx))
            } else {
                Err(Error::TypeMismatch {
                    expected: format!("INT64 for {} parameter", param_name),
                    actual: v.data_type().to_string(),
                })
            }
        }
        None => Ok(Some(default)),
    }
}

pub fn array_subscript(array: &Value, index: &Value) -> Result<Value> {
    if array.is_null() || index.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        if let Some(idx) = index.as_i64() {
            let len = arr.len() as i64;

            if len == 0 {
                return Ok(Value::null());
            }

            let zero_based_idx = match convert_sql_index_to_zero_based(idx, len) {
                Some(idx) => idx,
                None => return Ok(Value::null()),
            };

            if zero_based_idx < 0 || zero_based_idx >= len {
                return Ok(Value::null());
            }

            Ok(arr[zero_based_idx as usize].clone())
        } else {
            Err(Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: index.data_type().to_string(),
            })
        }
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

pub fn array_slice(array: &Value, start: Option<&Value>, end: Option<&Value>) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if let Some(arr) = array.as_array() {
        let len = arr.len() as i64;

        if len == 0 {
            return Ok(Value::array(vec![]));
        }

        let start_idx = match parse_optional_index(start, 1, "start")? {
            Some(idx) => idx,
            None => return Ok(Value::null()),
        };

        let end_idx = match parse_optional_index(end, len, "end")? {
            Some(idx) => idx,
            None => return Ok(Value::null()),
        };

        let start_zero = convert_sql_index_to_zero_based(start_idx, len).unwrap_or(0);
        let end_zero = convert_sql_index_to_zero_based(end_idx, len).unwrap_or(-1);

        if start_zero >= len {
            return Ok(Value::array(vec![]));
        }

        if end_zero < 0 {
            return Ok(Value::array(vec![]));
        }

        let start_clamped = start_zero.max(0).min(len - 1);
        let end_clamped = end_zero.max(0).min(len - 1);

        if start_clamped > end_clamped {
            return Ok(Value::array(vec![]));
        }

        let result: Vec<Value> = arr[(start_clamped as usize)..=(end_clamped as usize)].to_vec();

        Ok(Value::array(result))
    } else {
        Err(Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: array.data_type().to_string(),
        })
    }
}

fn extract_optional_i64(value: &Value, param_name: &str) -> Result<Option<i64>> {
    if value.is_null() {
        return Ok(None);
    }

    value
        .as_i64()
        .ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: format!("{} has type {}", param_name, value.data_type()),
        })
        .map(Some)
}

#[inline]
fn default_array_step(start: i64, end: i64) -> i64 {
    if end >= start { 1 } else { -1 }
}

fn generate_array_elements(start: i64, end: i64, step: i64) -> Vec<Value> {
    let mut result = Vec::new();

    if start == end {
        result.push(Value::int64(start));
        return result;
    }

    let is_ascending = start < end;
    let step_is_ascending = step > 0;

    if is_ascending != step_is_ascending {
        return result;
    }

    let mut current = start;

    let should_continue = |curr: i64| -> bool { if step > 0 { curr <= end } else { curr >= end } };

    while should_continue(current) {
        result.push(Value::int64(current));

        match current.checked_add(step) {
            Some(next) => {
                if !should_continue(next) {
                    break;
                }
                current = next;
            }
            None => break,
        }
    }

    result
}

pub fn array_to_string(
    array: &Value,
    delimiter: &Value,
    null_text: Option<&Value>,
) -> Result<Value> {
    if array.is_null() {
        return Ok(Value::null());
    }

    if delimiter.is_null() {
        return Ok(Value::null());
    }

    let arr = array.as_array().ok_or_else(|| Error::TypeMismatch {
        expected: "ARRAY".to_string(),
        actual: array.data_type().to_string(),
    })?;

    let delim = delimiter.as_str().ok_or_else(|| Error::TypeMismatch {
        expected: "STRING".to_string(),
        actual: delimiter.data_type().to_string(),
    })?;

    let null_replacement = null_text.and_then(|v| v.as_str().map(|s| s.to_string()));

    let mut parts: Vec<String> = Vec::new();
    for elem in arr.iter() {
        if elem.is_null() {
            if let Some(ref replacement) = null_replacement {
                parts.push(replacement.clone());
            }
        } else if let Some(s) = elem.as_str() {
            parts.push(s.to_string());
        } else if let Some(i) = elem.as_i64() {
            parts.push(i.to_string());
        } else if let Some(f) = elem.as_f64() {
            parts.push(f.to_string());
        } else if let Some(b) = elem.as_bool() {
            parts.push(b.to_string());
        } else {
            parts.push(format!("{:?}", elem));
        }
    }

    Ok(Value::string(parts.join(delim)))
}

pub fn generate_array(start: &Value, end: &Value, step: Option<&Value>) -> Result<Value> {
    let start_i64 = match extract_optional_i64(start, "start")? {
        Some(v) => v,
        None => return Ok(Value::null()),
    };

    let end_i64 = match extract_optional_i64(end, "end")? {
        Some(v) => v,
        None => return Ok(Value::null()),
    };

    let step_i64 = if let Some(step_val) = step {
        match extract_optional_i64(step_val, "step")? {
            Some(v) => v,
            None => return Ok(Value::null()),
        }
    } else {
        default_array_step(start_i64, end_i64)
    };

    if step_i64 == 0 {
        return Err(Error::invalid_query(
            "GENERATE_ARRAY step cannot be 0".to_string(),
        ));
    }

    let elements = generate_array_elements(start_i64, end_i64, step_i64);
    Ok(Value::array(elements))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_array_length() {
        assert_eq!(
            array_length(&Value::array(vec![
                Value::int64(1),
                Value::int64(2),
                Value::int64(3)
            ]))
            .unwrap(),
            Value::int64(3)
        );

        assert_eq!(
            array_length(&Value::array(vec![])).unwrap(),
            Value::int64(0)
        );

        assert_eq!(array_length(&Value::null()).unwrap(), Value::null());
    }

    #[test]
    fn test_array_concat() {
        let result = array_concat(&[
            Value::array(vec![Value::int64(1), Value::int64(2)]),
            Value::array(vec![Value::int64(3), Value::int64(4)]),
        ])
        .unwrap();

        assert_eq!(
            result,
            Value::array(vec![
                Value::int64(1),
                Value::int64(2),
                Value::int64(3),
                Value::int64(4)
            ])
        );
    }

    #[test]
    fn test_array_reverse() {
        let result = array_reverse(Value::array(vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(3),
        ]))
        .unwrap();

        assert_eq!(
            result,
            Value::array(vec![Value::int64(3), Value::int64(2), Value::int64(1)])
        );
    }

    #[test]
    fn test_values_equal() {
        assert!(values_equal(&Value::int64(42), &Value::int64(42)));
        assert!(!values_equal(&Value::int64(42), &Value::int64(43)));
        assert!(values_equal(&Value::null(), &Value::null()));
        assert!(!values_equal(&Value::null(), &Value::int64(42)));

        let arr1 = Value::array(vec![Value::int64(1), Value::int64(2)]);
        let arr2 = Value::array(vec![Value::int64(1), Value::int64(2)]);
        let arr3 = Value::array(vec![Value::int64(1), Value::int64(3)]);

        assert!(values_equal(&arr1, &arr2));
        assert!(!values_equal(&arr1, &arr3));
    }
}
