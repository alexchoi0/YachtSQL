use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("SPLIT", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        if let (Some(s), Some(delimiter)) = (values[0].as_str(), values[1].as_str()) {
            let parts: Vec<Value> = s
                .split(delimiter)
                .map(|part| Value::string(part.to_string()))
                .collect();
            Ok(Value::array(parts))
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING".to_string(),
                actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
            })
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split_by_char(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "SPLITBYCHAR requires at least 2 arguments",
            ));
        }
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let delimiter = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let s = values[1]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[1].data_type().to_string(),
            })?;

        let max_parts = if args.len() >= 3 {
            values[2].as_i64().unwrap_or(0) as usize
        } else {
            0
        };

        if delimiter.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "SPLITBYCHAR: delimiter must be a single character",
            ));
        }

        let parts: Vec<Value> = if max_parts > 0 {
            s.splitn(max_parts, delimiter)
                .map(|part| Value::string(part.to_string()))
                .collect()
        } else {
            s.split(delimiter)
                .map(|part| Value::string(part.to_string()))
                .collect()
        };
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split_by_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "SPLITBYSTRING requires at least 2 arguments",
            ));
        }
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let delimiter = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let s = values[1]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[1].data_type().to_string(),
            })?;

        let max_parts = if args.len() >= 3 {
            values[2].as_i64().unwrap_or(0) as usize
        } else {
            0
        };

        let parts: Vec<Value> = if max_parts > 0 {
            s.splitn(max_parts, delimiter)
                .map(|part| Value::string(part.to_string()))
                .collect()
        } else {
            s.split(delimiter)
                .map(|part| Value::string(part.to_string()))
                .collect()
        };
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split_by_regexp(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(crate::error::Error::invalid_query(
                "SPLITBYREGEXP requires at least 2 arguments",
            ));
        }
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let pattern = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let s = values[1]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[1].data_type().to_string(),
            })?;

        let re = regex::Regex::new(pattern)
            .map_err(|e| crate::error::Error::invalid_query(e.to_string()))?;
        let parts: Vec<Value> = re
            .split(s)
            .map(|part| Value::string(part.to_string()))
            .collect();
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split_by_whitespace(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("SPLITBYWHITESPACE", args, 1)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;

        let parts: Vec<Value> = s
            .split_whitespace()
            .map(|part| Value::string(part.to_string()))
            .collect();
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split_by_non_alpha(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("SPLITBYNONALPHA", args, 1)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;

        let parts: Vec<Value> = s
            .split(|c: char| !c.is_alphabetic())
            .filter(|part| !part.is_empty())
            .map(|part| Value::string(part.to_string()))
            .collect();
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_alpha_tokens(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ALPHATOKENS", args, 1)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;

        let parts: Vec<Value> = s
            .split(|c: char| !c.is_alphabetic())
            .filter(|part| !part.is_empty())
            .map(|part| Value::string(part.to_string()))
            .collect();
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_tokens(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("TOKENS", args, 1)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;

        let parts: Vec<Value> = s
            .split(|c: char| !c.is_alphanumeric())
            .filter(|part| !part.is_empty())
            .map(|part| Value::string(part.to_string()))
            .collect();
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_ngrams(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("NGRAMS", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let n = values[1]
            .as_i64()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: values[1].data_type().to_string(),
            })? as usize;

        if n == 0 || s.len() < n {
            return Ok(Value::array(vec![]));
        }

        let ngrams: Vec<Value> = (0..=s.len() - n)
            .map(|i| Value::string(s[i..i + n].to_string()))
            .collect();
        Ok(Value::array(ngrams))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_string_concat(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "ARRAYSTRINGCONCAT requires at least 1 argument",
            ));
        }
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() {
            return Ok(Value::null());
        }

        let arr = values[0]
            .as_array()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "ARRAY".to_string(),
                actual: values[0].data_type().to_string(),
            })?;

        let separator = if args.len() >= 2 && !values[1].is_null() {
            values[1].as_str().unwrap_or("")
        } else {
            ""
        };

        let strings: Vec<String> = arr
            .iter()
            .filter_map(|v| v.as_str().map(|s| s.to_string()))
            .collect();
        Ok(Value::string(strings.join(separator)))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_extract_all(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("EXTRACTALL", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let pattern = values[1]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[1].data_type().to_string(),
            })?;

        let re = regex::Regex::new(pattern)
            .map_err(|e| crate::error::Error::invalid_query(e.to_string()))?;
        let matches: Vec<Value> = re
            .find_iter(s)
            .map(|m| Value::string(m.as_str().to_string()))
            .collect();
        Ok(Value::array(matches))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_extract_all_groups_horizontal(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("EXTRACTALLGROUPSHORIZONTAL", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let pattern = values[1]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[1].data_type().to_string(),
            })?;

        let re = regex::Regex::new(pattern)
            .map_err(|e| crate::error::Error::invalid_query(e.to_string()))?;

        let num_groups = re.captures_len() - 1;
        let mut groups: Vec<Vec<Value>> = (0..num_groups).map(|_| Vec::new()).collect();

        for caps in re.captures_iter(s) {
            for (i, group) in groups.iter_mut().enumerate() {
                if let Some(m) = caps.get(i + 1) {
                    group.push(Value::string(m.as_str().to_string()));
                }
            }
        }

        let result: Vec<Value> = groups.into_iter().map(|g| Value::array(g)).collect();
        Ok(Value::array(result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_extract_all_groups_vertical(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("EXTRACTALLGROUPSVERTICAL", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let s = values[0]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[0].data_type().to_string(),
            })?;
        let pattern = values[1]
            .as_str()
            .ok_or_else(|| crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: values[1].data_type().to_string(),
            })?;

        let re = regex::Regex::new(pattern)
            .map_err(|e| crate::error::Error::invalid_query(e.to_string()))?;

        let mut result: Vec<Value> = Vec::new();

        for caps in re.captures_iter(s) {
            let mut groups: Vec<Value> = Vec::new();
            for i in 1..caps.len() {
                if let Some(m) = caps.get(i) {
                    groups.push(Value::string(m.as_str().to_string()));
                }
            }
            result.push(Value::array(groups));
        }

        Ok(Value::array(result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_split_part(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("SPLIT_PART", args, 3)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() || values[2].is_null() {
            return Ok(Value::null());
        }

        let s = values[0].as_str();
        let delimiter = values[1].as_str();
        let field_num = values[2].as_i64();

        match (s, delimiter, field_num) {
            (Some(s), Some(delimiter), Some(field_num)) => {
                if field_num <= 0 {
                    return Err(crate::error::Error::invalid_query(
                        "SPLIT_PART: field number must be positive",
                    ));
                }
                let parts: Vec<&str> = s.split(delimiter).collect();
                let idx = (field_num - 1) as usize;
                if idx < parts.len() {
                    Ok(Value::string(parts[idx].to_string()))
                } else {
                    Ok(Value::string(String::new()))
                }
            }
            _ => Err(crate::error::Error::TypeMismatch {
                expected: "STRING, STRING, INT64".to_string(),
                actual: format!(
                    "{}, {}, {}",
                    values[0].data_type(),
                    values[1].data_type(),
                    values[2].data_type()
                ),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema_with_two_strings() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("delim", DataType::String),
        ])
    }

    #[test]
    fn splits_string_by_delimiter() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("a,b,c".into()),
                Value::string(",".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("delim")];
        let result = ProjectionWithExprExec::evaluate_split(&args, &batch, 0).expect("success");
        let arr = result.as_array().expect("Expected Array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Value::string("a".into()));
        assert_eq!(arr[1], Value::string("b".into()));
        assert_eq!(arr[2], Value::string("c".into()));
    }

    #[test]
    fn splits_with_multi_char_delimiter() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("foo::bar::baz".into()),
                Value::string("::".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("delim")];
        let result = ProjectionWithExprExec::evaluate_split(&args, &batch, 0).expect("success");
        let arr = result.as_array().expect("Expected Array");
        assert_eq!(arr.len(), 3);
        assert_eq!(arr[0], Value::string("foo".into()));
        assert_eq!(arr[1], Value::string("bar".into()));
        assert_eq!(arr[2], Value::string("baz".into()));
    }

    #[test]
    fn returns_single_element_when_delimiter_not_found() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![
                Value::string("hello".into()),
                Value::string(",".into()),
            ]],
        );
        let args = vec![Expr::column("str"), Expr::column("delim")];
        let result = ProjectionWithExprExec::evaluate_split(&args, &batch, 0).expect("success");
        let arr = result.as_array().expect("Expected Array");
        assert_eq!(arr.len(), 1);
        assert_eq!(arr[0], Value::string("hello".into()));
    }

    #[test]
    fn propagates_null_string() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::null(), Value::string(",".into())]],
        );
        let args = vec![Expr::column("str"), Expr::column("delim")];
        let result = ProjectionWithExprExec::evaluate_split(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_delimiter() {
        let batch = create_batch(
            schema_with_two_strings(),
            vec![vec![Value::string("a,b".into()), Value::null()]],
        );
        let args = vec![Expr::column("str"), Expr::column("delim")];
        let result = ProjectionWithExprExec::evaluate_split(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hi".into())]]);
        let err = ProjectionWithExprExec::evaluate_split(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "SPLIT");
    }
}
