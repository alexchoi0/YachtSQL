use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};

use super::{Accumulator, AggregateFunction};

const DEFAULT_MAX_LENGTH: usize = 1_048_576;

#[derive(Debug, Clone)]
pub enum OverflowBehavior {
    None,
    TruncateWithoutCount(String),
    TruncateWithCount(String),
}

#[derive(Debug, Clone)]
pub struct ListAggAccumulator {
    values: Vec<String>,
    delimiter: String,
    max_length: usize,
    overflow_behavior: OverflowBehavior,
}

impl ListAggAccumulator {
    pub fn new() -> Self {
        Self {
            values: Vec::new(),
            delimiter: String::new(),
            max_length: DEFAULT_MAX_LENGTH,
            overflow_behavior: OverflowBehavior::None,
        }
    }

    pub fn with_delimiter(delimiter: String) -> Self {
        Self {
            values: Vec::new(),
            delimiter,
            max_length: DEFAULT_MAX_LENGTH,
            overflow_behavior: OverflowBehavior::None,
        }
    }

    pub fn with_max_length(mut self, max_length: usize) -> Self {
        self.max_length = max_length;
        self
    }

    pub fn with_overflow_behavior(mut self, behavior: OverflowBehavior) -> Self {
        self.overflow_behavior = behavior;
        self
    }

    fn concatenate_with_overflow(&self) -> String {
        if self.values.is_empty() {
            return String::new();
        }

        if self.values.len() == 1 {
            let value = &self.values[0];
            return self.apply_overflow_truncation(value, 0);
        }

        let mut result = String::new();
        let delimiter_len = self.delimiter.len();

        for (idx, value) in self.values.iter().enumerate() {
            if idx > 0 {
                if result.len() + delimiter_len > self.max_length {
                    return self.finalize_with_overflow(result, idx);
                }
                result.push_str(&self.delimiter);
            }

            if result.len() + value.len() > self.max_length {
                return self.finalize_with_overflow(result, idx);
            }

            result.push_str(value);
        }

        result
    }

    fn apply_overflow_truncation(&self, value: &str, _truncated_count: usize) -> String {
        if value.len() <= self.max_length {
            return value.to_string();
        }

        match &self.overflow_behavior {
            OverflowBehavior::None => value.to_string(),
            OverflowBehavior::TruncateWithoutCount(indicator) => {
                let indicator_len = indicator.len();
                if self.max_length > indicator_len {
                    let keep_len = self.max_length - indicator_len;
                    format!("{}{}", &value[..keep_len], indicator)
                } else {
                    indicator.clone()
                }
            }
            OverflowBehavior::TruncateWithCount(indicator) => {
                let suffix = format!("{} (1)", indicator);
                let suffix_len = suffix.len();
                if self.max_length > suffix_len {
                    let keep_len = self.max_length - suffix_len;
                    format!("{}{}", &value[..keep_len], suffix)
                } else {
                    suffix
                }
            }
        }
    }

    fn finalize_with_overflow(&self, partial_result: String, truncated_from_idx: usize) -> String {
        let truncated_count = self.values.len() - truncated_from_idx;

        match &self.overflow_behavior {
            OverflowBehavior::None => partial_result,
            OverflowBehavior::TruncateWithoutCount(indicator) => {
                let indicator_len = indicator.len();
                if partial_result.len() + indicator_len > self.max_length {
                    if self.max_length > indicator_len {
                        let keep_len = self.max_length - indicator_len;
                        format!("{}{}", &partial_result[..keep_len], indicator)
                    } else {
                        indicator.clone()
                    }
                } else {
                    format!("{}{}", partial_result, indicator)
                }
            }
            OverflowBehavior::TruncateWithCount(indicator) => {
                let suffix = format!("{} ({})", indicator, truncated_count);
                let suffix_len = suffix.len();
                if partial_result.len() + suffix_len > self.max_length {
                    if self.max_length > suffix_len {
                        let keep_len = self.max_length - suffix_len;
                        format!("{}{}", &partial_result[..keep_len], suffix)
                    } else {
                        suffix
                    }
                } else {
                    format!("{}{}", partial_result, suffix)
                }
            }
        }
    }
}

impl Default for ListAggAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl Accumulator for ListAggAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }

        let string_value = if let Some(s) = value.as_str() {
            s.to_string()
        } else if let Some(i) = value.as_i64() {
            i.to_string()
        } else if let Some(f) = value.as_f64() {
            f.to_string()
        } else if let Some(b) = value.as_bool() {
            b.to_string()
        } else if let Some(d) = value.as_numeric() {
            d.to_string()
        } else if let Some(d) = value.as_date() {
            d.to_string()
        } else if let Some(dt) = value.as_datetime() {
            dt.to_string()
        } else if let Some(t) = value.as_time() {
            t.to_string()
        } else if let Some(ts) = value.as_timestamp() {
            ts.to_string()
        } else if let Some(u) = value.as_uuid() {
            u.to_string()
        } else {
            return Err(Error::type_mismatch_value("STRING or scalar type", value));
        };

        self.values.push(string_value);
        Ok(())
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        let other = other
            .as_any()
            .downcast_ref::<ListAggAccumulator>()
            .ok_or_else(|| {
                Error::internal("Cannot merge LISTAGG accumulator with different type")
            })?;

        self.values.extend_from_slice(&other.values);
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            return Ok(Value::null());
        }

        let result = self.concatenate_with_overflow();
        Ok(Value::string(result))
    }

    fn reset(&mut self) {
        self.values.clear();
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

#[derive(Debug)]
pub struct ListAggFunction {
    delimiter: Option<String>,
    max_length: usize,
    overflow_behavior: OverflowBehavior,
}

impl ListAggFunction {
    pub fn new() -> Self {
        Self {
            delimiter: None,
            max_length: DEFAULT_MAX_LENGTH,
            overflow_behavior: OverflowBehavior::None,
        }
    }

    pub fn with_delimiter(delimiter: String) -> Self {
        Self {
            delimiter: Some(delimiter),
            max_length: DEFAULT_MAX_LENGTH,
            overflow_behavior: OverflowBehavior::None,
        }
    }

    pub fn with_max_length(mut self, max_length: usize) -> Self {
        self.max_length = max_length;
        self
    }

    pub fn with_overflow_behavior(mut self, behavior: OverflowBehavior) -> Self {
        self.overflow_behavior = behavior;
        self
    }
}

impl Default for ListAggFunction {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateFunction for ListAggFunction {
    fn name(&self) -> &str {
        "LISTAGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        let mut acc = match &self.delimiter {
            Some(delim) => ListAggAccumulator::with_delimiter(delim.clone()),
            None => ListAggAccumulator::new(),
        };

        acc = acc
            .with_max_length(self.max_length)
            .with_overflow_behavior(self.overflow_behavior.clone());

        Box::new(acc)
    }
}

#[derive(Debug, Clone)]
pub struct StringAggFunction {
    delimiter: String,
}

impl StringAggFunction {
    pub fn new(delimiter: String) -> Self {
        Self { delimiter }
    }
}

impl Default for StringAggFunction {
    fn default() -> Self {
        Self::new(",".to_string())
    }
}

impl AggregateFunction for StringAggFunction {
    fn name(&self) -> &str {
        "STRING_AGG"
    }

    fn arg_types(&self) -> &[DataType] {
        &[DataType::Unknown]
    }

    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType> {
        Ok(DataType::String)
    }

    fn create_accumulator(&self) -> Box<dyn Accumulator> {
        Box::new(ListAggAccumulator::with_delimiter(self.delimiter.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listagg_basic_empty() {
        let acc = ListAggAccumulator::new();
        assert_eq!(acc.finalize().unwrap(), Value::null());
    }

    #[test]
    fn test_listagg_single_value() {
        let mut acc = ListAggAccumulator::new();
        acc.accumulate(&Value::string("Alice".to_string())).unwrap();
        assert_eq!(acc.finalize().unwrap(), Value::string("Alice".to_string()));
    }

    #[test]
    fn test_listagg_multiple_values_no_delimiter() {
        let mut acc = ListAggAccumulator::new();
        acc.accumulate(&Value::string("Alice".to_string())).unwrap();
        acc.accumulate(&Value::string("Bob".to_string())).unwrap();
        acc.accumulate(&Value::string("Charlie".to_string()))
            .unwrap();
        assert_eq!(
            acc.finalize().unwrap(),
            Value::string("AliceBobCharlie".to_string())
        );
    }

    #[test]
    fn test_listagg_with_delimiter() {
        let mut acc = ListAggAccumulator::with_delimiter(", ".to_string());
        acc.accumulate(&Value::string("Alice".to_string())).unwrap();
        acc.accumulate(&Value::string("Bob".to_string())).unwrap();
        acc.accumulate(&Value::string("Charlie".to_string()))
            .unwrap();
        assert_eq!(
            acc.finalize().unwrap(),
            Value::string("Alice, Bob, Charlie".to_string())
        );
    }

    #[test]
    fn test_listagg_skips_nulls() {
        let mut acc = ListAggAccumulator::with_delimiter(", ".to_string());
        acc.accumulate(&Value::string("Alice".to_string())).unwrap();
        acc.accumulate(&Value::null()).unwrap();
        acc.accumulate(&Value::string("Bob".to_string())).unwrap();
        acc.accumulate(&Value::null()).unwrap();
        acc.accumulate(&Value::string("Charlie".to_string()))
            .unwrap();
        assert_eq!(
            acc.finalize().unwrap(),
            Value::string("Alice, Bob, Charlie".to_string())
        );
    }

    #[test]
    fn test_listagg_all_nulls_returns_null() {
        let mut acc = ListAggAccumulator::new();
        acc.accumulate(&Value::null()).unwrap();
        acc.accumulate(&Value::null()).unwrap();
        acc.accumulate(&Value::null()).unwrap();
        assert_eq!(acc.finalize().unwrap(), Value::null());
    }

    #[test]
    fn test_listagg_numeric_values() {
        let mut acc = ListAggAccumulator::with_delimiter(", ".to_string());
        acc.accumulate(&Value::int64(10)).unwrap();
        acc.accumulate(&Value::int64(20)).unwrap();
        acc.accumulate(&Value::int64(30)).unwrap();
        assert_eq!(
            acc.finalize().unwrap(),
            Value::string("10, 20, 30".to_string())
        );
    }

    #[test]
    fn test_listagg_overflow_truncate_without_count() {
        let mut acc = ListAggAccumulator::with_delimiter(", ".to_string())
            .with_max_length(20)
            .with_overflow_behavior(OverflowBehavior::TruncateWithoutCount("...".to_string()));

        acc.accumulate(&Value::string("Alice".to_string())).unwrap();
        acc.accumulate(&Value::string("Bob".to_string())).unwrap();
        acc.accumulate(&Value::string("Charlie".to_string()))
            .unwrap();
        acc.accumulate(&Value::string("Diana".to_string())).unwrap();

        let result = acc.finalize().unwrap();
        if let Some(s) = result.as_str() {
            assert!(s.len() <= 20);
            assert!(s.ends_with("..."));
            assert!(s.starts_with("Alice, Bob"));
        } else {
            panic!("Expected string result");
        }
    }

    #[test]
    fn test_listagg_overflow_truncate_with_count() {
        let mut acc = ListAggAccumulator::with_delimiter(", ".to_string())
            .with_max_length(25)
            .with_overflow_behavior(OverflowBehavior::TruncateWithCount("...".to_string()));

        acc.accumulate(&Value::string("Alice".to_string())).unwrap();
        acc.accumulate(&Value::string("Bob".to_string())).unwrap();
        acc.accumulate(&Value::string("Charlie".to_string()))
            .unwrap();
        acc.accumulate(&Value::string("Diana".to_string())).unwrap();

        let result = acc.finalize().unwrap();
        if let Some(s) = result.as_str() {
            assert!(s.contains("..."));

            assert!(s.contains('(') && s.contains(')'));
        } else {
            panic!("Expected string result");
        }
    }
}
