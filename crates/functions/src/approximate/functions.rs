use std::cmp::Ordering;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};

use indexmap::IndexMap;
use rust_decimal::prelude::ToPrimitive;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

use super::hyperloglog::HyperLogLogPlusPlus;
use super::tdigest::TDigest;

pub fn approx_count_distinct(values: &[Value]) -> Result<Value> {
    let mut estimator = HyperLogLogPlusPlus::new();
    let mut saw_value = false;

    for value in values {
        if value.is_null() {
            continue;
        }

        saw_value = true;
        estimator.add(&HashableValue(value));
    }

    if !saw_value {
        return Ok(Value::int64(0));
    }

    Ok(Value::int64(estimator.estimate() as i64))
}

pub fn approx_quantiles(values: &[Value], num_quantiles: i64) -> Result<Value> {
    if num_quantiles <= 0 {
        return Err(Error::invalid_query(
            "APPROX_QUANTILES requires num_quantiles > 0; negative or zero values are invalid"
                .to_string(),
        ));
    }

    let mut digest = TDigest::new();
    let mut saw_value = false;

    for value in values {
        if let Some(number) = numeric_value_to_f64(value)? {
            digest.add(number);
            saw_value = true;
        }
    }

    if !saw_value {
        return Ok(Value::null());
    }

    let mut quantiles = Vec::with_capacity((num_quantiles + 1) as usize);
    for i in 0..=num_quantiles {
        let q = i as f64 / num_quantiles as f64;
        quantiles.push(Value::float64(digest.quantile(q)));
    }

    Ok(Value::array(quantiles))
}

pub fn approx_top_count(values: &[Value], k: i64) -> Result<Value> {
    validate_k_positive(k, "APPROX_TOP_COUNT")?;

    let non_null_values = filter_nulls(values);
    if non_null_values.is_empty() {
        return Ok(Value::array(vec![]));
    }

    let counts = space_saving_count(&non_null_values, k as usize);

    let result = counts_to_struct_array(counts, "count");
    Ok(Value::array(result))
}

pub fn approx_top_sum(values: &[Value], weights: &[Value], k: i64) -> Result<Value> {
    validate_k_positive(k, "APPROX_TOP_SUM")?;

    if values.len() != weights.len() {
        return Err(Error::invalid_query(
            "APPROX_TOP_SUM requires values and weights arrays to have the same length".to_string(),
        ));
    }

    let sums = space_saving_weighted_sum(values, weights, k as usize)?;

    let result = sums_to_struct_array(sums, "sum");
    Ok(Value::array(result))
}

fn filter_nulls(values: &[Value]) -> Vec<&Value> {
    values.iter().filter(|v| !v.is_null()).collect()
}

fn validate_k_positive(k: i64, function_name: &str) -> Result<()> {
    if k <= 0 {
        return Err(Error::invalid_query(format!(
            "{} requires k > 0",
            function_name
        )));
    }
    Ok(())
}

fn space_saving_count(values: &[&Value], k: usize) -> HashMap<String, i64> {
    let mut counts: HashMap<String, i64> = HashMap::new();

    for value in values {
        let key = value_to_string(value);
        update_space_saving_map(&mut counts, key, 1, k);
    }

    counts
}

fn space_saving_weighted_sum(
    values: &[Value],
    weights: &[Value],
    k: usize,
) -> Result<HashMap<String, f64>> {
    let mut sums: HashMap<String, f64> = HashMap::new();

    for (value, weight) in values.iter().zip(weights.iter()) {
        if value.is_null() || weight.is_null() {
            continue;
        }

        let weight_f64 = extract_numeric_weight(weight)?;
        let key = value_to_string(value);
        update_space_saving_map(&mut sums, key, weight_f64, k);
    }

    Ok(sums)
}

fn update_space_saving_map<T>(map: &mut HashMap<String, T>, key: String, increment: T, k: usize)
where
    T: std::ops::AddAssign + Copy + PartialOrd,
{
    if let Some(value) = map.get_mut(&key) {
        *value += increment;
    } else if map.len() < k {
        map.insert(key, increment);
    } else if let Some((min_key, min_value)) = find_min_entry(map) {
        map.remove(&min_key);
        let mut new_value = min_value;
        new_value += increment;
        map.insert(key, new_value);
    }
}

fn find_min_entry<T: PartialOrd + Copy>(map: &HashMap<String, T>) -> Option<(String, T)> {
    map.iter()
        .min_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(k, v)| (k.clone(), *v))
}

fn extract_numeric_weight(weight: &Value) -> Result<f64> {
    numeric_value_to_f64(weight)?.ok_or_else(|| Error::TypeMismatch {
        expected: "NUMERIC".to_string(),
        actual: "NULL".to_string(),
    })
}

fn numeric_value_to_f64(value: &Value) -> Result<Option<f64>> {
    if value.is_null() {
        return Ok(None);
    }

    if let Some(i) = value.as_i64() {
        return Ok(Some(i as f64));
    }

    if let Some(f) = value.as_f64() {
        return Ok(Some(f));
    }

    if let Some(d) = value.as_numeric() {
        return d.to_f64().map(Some).ok_or_else(|| Error::TypeMismatch {
            expected: "NUMERIC".to_string(),
            actual: "NUMERIC (out of range)".to_string(),
        });
    }

    Err(Error::TypeMismatch {
        expected: "NUMERIC".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn counts_to_struct_array(counts: HashMap<String, i64>, field_name: &str) -> Vec<Value> {
    hashmap_to_sorted_struct_array(counts, field_name, Value::int64, |a, b| b.cmp(a))
}

fn sums_to_struct_array(sums: HashMap<String, f64>, field_name: &str) -> Vec<Value> {
    hashmap_to_sorted_struct_array(sums, field_name, Value::float64, |a, b| {
        b.partial_cmp(a).unwrap_or(Ordering::Equal)
    })
}

fn hashmap_to_sorted_struct_array<T>(
    map: HashMap<String, T>,
    metric_name: &str,
    to_value: impl Fn(T) -> Value,
    compare_metrics: impl Fn(&T, &T) -> Ordering,
) -> Vec<Value> {
    let mut items: Vec<_> = map.into_iter().collect();

    items.sort_by(|(a_key, a_metric), (b_key, b_metric)| {
        match compare_metrics(a_metric, b_metric) {
            Ordering::Equal => a_key.cmp(b_key),
            ordering => ordering,
        }
    });

    items
        .into_iter()
        .map(|(key, metric)| create_result_struct(key, to_value(metric), metric_name))
        .collect()
}

fn create_result_struct(value_str: String, metric: Value, metric_name: &str) -> Value {
    let mut fields = IndexMap::new();
    fields.insert("value".to_string(), Value::string(value_str));
    fields.insert(metric_name.to_string(), metric);
    Value::struct_val(fields)
}

fn value_to_string(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_string();
    }
    if let Some(i) = value.as_i64() {
        return i.to_string();
    }
    if let Some(f) = value.as_f64() {
        return f.to_string();
    }
    if let Some(b) = value.as_bool() {
        return b.to_string();
    }
    if let Some(d) = value.as_numeric() {
        return d.to_string();
    }
    if let Some(d) = value.as_date() {
        return d.to_string();
    }
    if let Some(ts) = value.as_timestamp() {
        return ts.to_string();
    }

    format!("{:?}", value)
}

struct HashableValue<'a>(&'a Value);

impl<'a> Hash for HashableValue<'a> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        if self.0.is_null() {
            0_u8.hash(state);
            return;
        }

        if let Some(b) = self.0.as_bool() {
            1_u8.hash(state);
            b.hash(state);
            return;
        }

        if let Some(i) = self.0.as_i64() {
            2_u8.hash(state);
            i.hash(state);
            return;
        }

        if let Some(f) = self.0.as_f64() {
            3_u8.hash(state);
            f.to_bits().hash(state);
            return;
        }

        if let Some(s) = self.0.as_str() {
            4_u8.hash(state);
            s.hash(state);
            return;
        }

        if let Some(d) = self.0.as_numeric() {
            5_u8.hash(state);
            d.to_string().hash(state);
            return;
        }

        if let Some(date) = self.0.as_date() {
            6_u8.hash(state);
            date.to_string().hash(state);
            return;
        }

        if let Some(dt) = self.0.as_datetime() {
            7_u8.hash(state);
            dt.timestamp_nanos_opt().unwrap_or(0).hash(state);
            return;
        }

        if let Some(time) = self.0.as_time() {
            8_u8.hash(state);
            time.to_string().hash(state);
            return;
        }

        if let Some(ts) = self.0.as_timestamp() {
            9_u8.hash(state);
            ts.timestamp_nanos_opt().unwrap_or(0).hash(state);
            return;
        }

        if let Some(bytes) = self.0.as_bytes() {
            10_u8.hash(state);
            bytes.hash(state);
            return;
        }

        if let Some(fields) = self.0.as_struct() {
            11_u8.hash(state);
            fields.len().hash(state);
            for (key, value) in fields.iter() {
                key.hash(state);
                HashableValue(value).hash(state);
            }
            return;
        }

        if let Some(items) = self.0.as_array() {
            12_u8.hash(state);
            items.len().hash(state);
            for item in items.iter() {
                HashableValue(item).hash(state);
            }
            return;
        }

        if let Some(wkt) = self.0.as_geography() {
            13_u8.hash(state);
            wkt.hash(state);
            return;
        }

        if let Some(json) = self.0.as_json() {
            14_u8.hash(state);
            json.to_string().hash(state);
            return;
        }

        if let Some(uuid) = self.0.as_uuid() {
            15_u8.hash(state);
            uuid.as_bytes().hash(state);
            return;
        }

        16_u8.hash(state);
        format!("{:?}", self.0).hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_approx_count_distinct_basic() {
        let values = vec![
            Value::int64(1),
            Value::int64(2),
            Value::int64(3),
            Value::int64(1),
            Value::int64(2),
        ];
        let result = approx_count_distinct(&values).unwrap();
        assert_count_in_range(result, 2, 4, "Expected ~3 distinct values");
    }

    #[test]
    fn test_approx_count_distinct_with_nulls() {
        let values = vec![
            Value::int64(1),
            Value::null(),
            Value::int64(2),
            Value::null(),
            Value::int64(3),
        ];
        let result = approx_count_distinct(&values).unwrap();
        assert_count_in_range(result, 2, 4, "Expected ~3 distinct values (NULLs ignored)");
    }

    #[test]
    fn test_approx_count_distinct_empty() {
        let values = vec![Value::null(), Value::null()];
        let result = approx_count_distinct(&values).unwrap();
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn test_approx_quantiles_basic() {
        let values = vec![
            Value::float64(1.0),
            Value::float64(2.0),
            Value::float64(3.0),
            Value::float64(4.0),
            Value::float64(5.0),
        ];
        let result = approx_quantiles(&values, 4).unwrap();

        if let Some(quantiles) = result.as_array() {
            assert_eq!(
                quantiles.len(),
                5,
                "4 quantiles should produce 5 boundaries"
            );
            assert_float_in_range(&quantiles[0], 0.9, 1.1);
            assert_float_in_range(&quantiles[2], 2.9, 3.1);
            assert_float_in_range(&quantiles[4], 4.9, 5.1);
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_approx_quantiles_invalid_k() {
        let values = vec![Value::float64(1.0)];
        let result = approx_quantiles(&values, 0);
        assert!(result.is_err());
    }

    #[test]
    fn test_approx_top_count_basic() {
        let values = vec![
            Value::string("a".to_string()),
            Value::string("b".to_string()),
            Value::string("a".to_string()),
            Value::string("c".to_string()),
            Value::string("a".to_string()),
        ];
        let result = approx_top_count(&values, 2).unwrap();

        if let Some(top) = result.as_array() {
            assert_eq!(top.len(), 2);

            if let Some(fields) = top[0].as_struct() {
                assert_eq!(fields.get("value"), Some(&Value::string("a".to_string())));
                assert_eq!(fields.get("count"), Some(&Value::int64(3)));
            } else {
                panic!("Expected Struct");
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_approx_top_sum_basic() {
        let values = vec![
            Value::string("a".to_string()),
            Value::string("b".to_string()),
            Value::string("a".to_string()),
            Value::string("c".to_string()),
        ];
        let weights = vec![
            Value::float64(10.0),
            Value::float64(5.0),
            Value::float64(15.0),
            Value::float64(3.0),
        ];
        let result = approx_top_sum(&values, &weights, 2).unwrap();

        if let Some(top) = result.as_array() {
            assert_eq!(top.len(), 2);

            if let Some(fields) = top[0].as_struct() {
                assert_eq!(fields.get("value"), Some(&Value::string("a".to_string())));
                if let Some(sum_val) = fields.get("sum") {
                    if let Some(sum) = sum_val.as_f64() {
                        assert!((sum - 25.0).abs() < 0.01, "Expected sum ~25.0, got {}", sum);
                    } else {
                        panic!("Expected Float64 sum");
                    }
                } else {
                    panic!("Expected Float64 sum");
                }
            } else {
                panic!("Expected Struct");
            }
        } else {
            panic!("Expected Array");
        }
    }

    #[test]
    fn test_approx_top_sum_length_mismatch() {
        let values = vec![Value::string("a".to_string())];
        let weights = vec![Value::float64(1.0), Value::float64(2.0)];
        let result = approx_top_sum(&values, &weights, 1);
        assert!(result.is_err());
    }

    fn assert_count_in_range(result: Value, min: i64, max: i64, msg: &str) {
        if let Some(count) = result.as_i64() {
            assert!(count >= min && count <= max, "{}: got {}", msg, count);
        } else {
            panic!("Expected Int64, got {:?}", result);
        }
    }

    fn assert_float_in_range(value: &Value, min: f64, max: f64) {
        if let Some(x) = value.as_f64() {
            assert!(
                x >= min && x <= max,
                "Expected value in [{}, {}], got {}",
                min,
                max,
                x
            );
        } else {
            panic!("Expected Float64, got {:?}", value);
        }
    }
}
