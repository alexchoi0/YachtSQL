use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_substr(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(crate::error::Error::invalid_query(
                "SUBSTR requires 2 or 3 arguments (string, start, [length])".to_string(),
            ));
        }
        let values = Self::evaluate_args(args, batch, row_idx)?;

        let str_val = values[0]
            .as_fixed_string()
            .map(|fs| fs.to_string_lossy())
            .or_else(|| values[0].as_str().map(|s| s.to_string()));

        if let (Some(s), Some(start)) = (str_val, values[1].as_i64()) {
            let chars: Vec<char> = s.chars().collect();
            let str_len = chars.len() as i64;

            let start_idx = if start > 0 {
                (start - 1) as usize
            } else if start < 0 {
                (str_len + start) as usize
            } else {
                return Ok(Value::string(String::new()));
            };

            if start_idx >= chars.len() {
                return Ok(Value::string(String::new()));
            }

            let substr = if args.len() == 3 {
                if values[2].is_null() {
                    return Ok(Value::null());
                }
                if let Some(length) = values[2].as_i64() {
                    if length <= 0 {
                        String::new()
                    } else {
                        let end_idx = (start_idx + length as usize).min(chars.len());
                        chars[start_idx..end_idx].iter().collect()
                    }
                } else {
                    return Err(crate::error::Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: values[2].data_type().to_string(),
                    });
                }
            } else {
                chars[start_idx..].iter().collect()
            };

            Ok(Value::string(substr))
        } else if values[0].is_null() || values[1].is_null() {
            Ok(Value::null())
        } else if let Some(start) = values[1].as_i64() {
            if let Some(b) = values[0].as_bytes() {
                let bytes_len = b.len() as i64;

                let start_idx = if start > 0 {
                    (start - 1) as usize
                } else if start < 0 {
                    (bytes_len + start) as usize
                } else {
                    return Ok(Value::bytes(Vec::new()));
                };

                if start_idx >= b.len() {
                    return Ok(Value::bytes(Vec::new()));
                }

                let subbytes = if args.len() == 3 {
                    if values[2].is_null() {
                        return Ok(Value::null());
                    }
                    if let Some(length) = values[2].as_i64() {
                        if length <= 0 {
                            Vec::new()
                        } else {
                            let end_idx = (start_idx + length as usize).min(b.len());
                            b[start_idx..end_idx].to_vec()
                        }
                    } else {
                        return Err(crate::error::Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: values[2].data_type().to_string(),
                        });
                    }
                } else {
                    b[start_idx..].to_vec()
                };

                Ok(Value::bytes(subbytes))
            } else {
                Err(crate::error::Error::TypeMismatch {
                    expected: "STRING or BYTES, INT64".to_string(),
                    actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
                })
            }
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING or BYTES, INT64".to_string(),
                actual: format!("{}, {}", values[0].data_type(), values[1].data_type()),
            })
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

    #[test]
    fn extracts_substring_with_start_position() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello world".into()), Value::int64(7)]],
        );
        let args = vec![Expr::column("str"), Expr::column("start")];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("world".into()));
    }

    #[test]
    fn extracts_substring_with_length() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
            Field::nullable("length", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("hello world".into()),
                Value::int64(1),
                Value::int64(5),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("start"),
            Expr::column("length"),
        ];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello".into()));
    }

    #[test]
    fn handles_negative_start_position() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(-3)]],
        );
        let args = vec![Expr::column("str"), Expr::column("start")];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("llo".into()));
    }

    #[test]
    fn returns_empty_string_when_start_is_zero() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(0)]],
        );
        let args = vec![Expr::column("str"), Expr::column("start")];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn returns_empty_string_when_start_exceeds_length() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(100)]],
        );
        let args = vec![Expr::column("str"), Expr::column("start")];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn returns_empty_string_for_negative_length() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
            Field::nullable("length", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("hello".into()),
                Value::int64(1),
                Value::int64(-1),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("start"),
            Expr::column("length"),
        ];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn propagates_null_string() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::null(), Value::int64(1)]]);
        let args = vec![Expr::column("str"), Expr::column("start")];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_start() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::null()]],
        );
        let args = vec![Expr::column("str"), Expr::column("start")];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn propagates_null_length() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("start", DataType::Int64),
            Field::nullable("length", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("hello".into()),
                Value::int64(1),
                Value::null(),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("start"),
            Expr::column("length"),
        ];
        let result = ProjectionWithExprExec::evaluate_substr(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_substr(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "SUBSTR");
    }
}
