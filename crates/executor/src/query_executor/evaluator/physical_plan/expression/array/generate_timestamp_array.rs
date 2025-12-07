use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_generate_timestamp_array(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 || args.len() > 4 {
            return Err(crate::error::Error::invalid_query(
                "GENERATE_TIMESTAMP_ARRAY requires (start_ts, end_ts, INTERVAL step) or explicit value/unit"
                    .to_string(),
            ));
        }

        let start_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let end_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if start_val.is_null() || end_val.is_null() {
            return Ok(Value::null());
        }

        let (interval_value, interval_unit) = if args.len() == 3 {
            match &args[2] {
                Expr::Function {
                    name,
                    args: interval_args,
                } if matches!(name, yachtsql_ir::FunctionName::Custom(s) if s == "INTERVAL_LITERAL") =>
                {
                    if interval_args.len() != 2 {
                        return Err(crate::error::Error::invalid_query(
                            "INTERVAL_LITERAL requires exactly 2 arguments".to_string(),
                        ));
                    }
                    let value = Self::evaluate_expr(&interval_args[0], batch, row_idx)?;
                    let unit = Self::evaluate_expr(&interval_args[1], batch, row_idx)?;
                    if value.is_null() || unit.is_null() {
                        return Ok(Value::null());
                    }
                    if let (Some(v), Some(u)) = (value.as_i64(), unit.as_str()) {
                        (Value::int64(v), Value::string(u.to_string()))
                    } else {
                        return Err(crate::error::Error::invalid_query(
                            "INTERVAL must have INT64 value and STRING unit".to_string(),
                        ));
                    }
                }
                _ => {
                    let value = Self::evaluate_expr(&args[2], batch, row_idx)?;
                    if value.is_null() {
                        return Ok(Value::null());
                    }
                    if let Some(v) = value.as_i64() {
                        (Value::int64(v), Value::string("HOUR".to_string()))
                    } else {
                        return Err(crate::error::Error::TypeMismatch {
                            expected: "INTERVAL or INT64".to_string(),
                            actual: value.data_type().to_string(),
                        });
                    }
                }
            }
        } else {
            let value = Self::evaluate_expr(&args[2], batch, row_idx)?;
            let unit = Self::evaluate_expr(&args[3], batch, row_idx)?;
            if value.is_null() || unit.is_null() {
                return Ok(Value::null());
            }
            if let (Some(v), Some(u)) = (value.as_i64(), unit.as_str()) {
                (Value::int64(v), Value::string(u.to_string()))
            } else {
                return Err(crate::error::Error::invalid_query(
                    "GENERATE_TIMESTAMP_ARRAY interval requires INT64 value and STRING unit"
                        .to_string(),
                ));
            }
        };

        crate::functions::datetime::generate_timestamp_array(
            &start_val,
            &end_val,
            &interval_value,
            &interval_unit,
        )
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_ir::FunctionName;
    use yachtsql_optimizer::expr::{Expr, LiteralValue};
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema_three_args() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("start", DataType::Timestamp),
            Field::nullable("end", DataType::Timestamp),
            Field::nullable("step", DataType::Int64),
        ])
    }

    fn schema_four_args() -> Schema {
        Schema::from_fields(vec![
            Field::nullable("start", DataType::Timestamp),
            Field::nullable("end", DataType::Timestamp),
            Field::nullable("step_value", DataType::Int64),
            Field::nullable("step_unit", DataType::String),
        ])
    }

    fn ts(year: i32, month: u32, day: u32, hour: u32) -> Value {
        Value::timestamp(
            Utc.with_ymd_and_hms(year, month, day, hour, 0, 0)
                .single()
                .expect("valid timestamp"),
        )
    }

    #[test]
    fn generates_timestamps_with_default_unit() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![ts(2024, 1, 1, 0), ts(2024, 1, 1, 2), Value::int64(1)]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step"),
        ];
        let result = ProjectionWithExprExec::evaluate_generate_timestamp_array(&args, &batch, 0)
            .expect("success");

        assert_eq!(
            result,
            Value::array(vec![
                ts(2024, 1, 1, 0),
                ts(2024, 1, 1, 1),
                ts(2024, 1, 1, 2)
            ])
        );
    }

    #[test]
    fn supports_interval_literal_syntax() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![ts(2024, 1, 1, 0), ts(2024, 1, 1, 3), Value::int64(1)]],
        );
        let interval_literal = Expr::Function {
            name: FunctionName::Custom("INTERVAL_LITERAL".to_string()),
            args: vec![
                Expr::literal(LiteralValue::Int64(2)),
                Expr::literal(LiteralValue::String("hour".to_string())),
            ],
        };

        let args = vec![Expr::column("start"), Expr::column("end"), interval_literal];
        let result = ProjectionWithExprExec::evaluate_generate_timestamp_array(&args, &batch, 0)
            .expect("success");

        assert_eq!(
            result,
            Value::array(vec![ts(2024, 1, 1, 0), ts(2024, 1, 1, 2)])
        );
    }

    #[test]
    fn supports_explicit_value_and_unit_arguments() {
        let batch = create_batch(
            schema_four_args(),
            vec![vec![
                ts(2024, 1, 1, 0),
                ts(2024, 1, 1, 2),
                Value::int64(1),
                Value::string("hour".to_string()),
            ]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step_value"),
            Expr::column("step_unit"),
        ];
        let result = ProjectionWithExprExec::evaluate_generate_timestamp_array(&args, &batch, 0)
            .expect("success");

        assert_eq!(
            result,
            Value::array(vec![
                ts(2024, 1, 1, 0),
                ts(2024, 1, 1, 1),
                ts(2024, 1, 1, 2)
            ])
        );
    }

    #[test]
    fn propagates_null_inputs() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![Value::null(), ts(2024, 1, 1, 0), Value::int64(1)]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step"),
        ];
        let result = ProjectionWithExprExec::evaluate_generate_timestamp_array(&args, &batch, 0)
            .expect("success");

        assert_eq!(result, Value::null());
    }

    #[test]
    fn returns_null_when_first_before_second_with_positive_step() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![
                ts(2024, 1, 1, 0),
                ts(2023, 12, 31, 0),
                Value::int64(1),
            ]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::column("step"),
        ];
        let result = ProjectionWithExprExec::evaluate_generate_timestamp_array(&args, &batch, 0)
            .expect("success");
        assert_eq!(result, Value::array(vec![]));
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![ts(2024, 1, 1, 0), ts(2024, 1, 1, 0), Value::int64(1)]],
        );

        let err = ProjectionWithExprExec::evaluate_generate_timestamp_array(
            &[Expr::column("start"), Expr::column("end")],
            &batch,
            0,
        )
        .expect_err("invalid arg count");
        assert_error_contains(&err, "GENERATE_TIMESTAMP_ARRAY");
    }

    #[test]
    fn errors_when_interval_value_wrong_type() {
        let batch = create_batch(
            schema_three_args(),
            vec![vec![ts(2024, 1, 1, 0), ts(2024, 1, 1, 1), Value::int64(1)]],
        );

        let args = vec![
            Expr::column("start"),
            Expr::column("end"),
            Expr::literal(LiteralValue::String("oops".to_string())),
        ];
        let err = ProjectionWithExprExec::evaluate_generate_timestamp_array(&args, &batch, 0)
            .expect_err("type mismatch expected");
        assert_error_contains(&err, "INT64");
    }
}
