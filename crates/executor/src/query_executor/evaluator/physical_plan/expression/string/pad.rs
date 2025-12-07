use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_lpad(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::evaluate_pad_internal("LPAD", args, batch, row_idx, true)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_rpad(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::evaluate_pad_internal("RPAD", args, batch, row_idx, false)
    }

    fn evaluate_pad_internal(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
        is_lpad: bool,
    ) -> Result<Value> {
        if args.len() < 2 || args.len() > 3 {
            return Err(crate::error::Error::invalid_query(format!(
                "{} requires 2 or 3 arguments (string, length, [fill])",
                name
            )));
        }

        let values = Self::evaluate_args(args, batch, row_idx)?;

        let fill_str = if args.len() == 3 {
            if values[2].is_null() {
                return Ok(Value::null());
            }
            if let Some(f) = values[2].as_str() {
                f.to_string()
            } else {
                return Err(crate::error::Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: values[2].data_type().to_string(),
                });
            }
        } else {
            " ".to_string()
        };

        if let (Some(s), Some(target_len)) = (values[0].as_str(), values[1].as_i64()) {
            Ok(Value::string(Self::pad_string(
                s, target_len, &fill_str, is_lpad,
            )?))
        } else if values[0].is_null() || values[1].is_null() {
            Ok(Value::null())
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING, INT64".to_string(),
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
    fn lpad_pads_with_spaces_by_default() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("len", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hi".into()), Value::int64(5)]],
        );
        let args = vec![Expr::column("str"), Expr::column("len")];
        let result = ProjectionWithExprExec::evaluate_lpad(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("   hi".into()));
    }

    #[test]
    fn rpad_pads_with_spaces_by_default() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("len", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hi".into()), Value::int64(5)]],
        );
        let args = vec![Expr::column("str"), Expr::column("len")];
        let result = ProjectionWithExprExec::evaluate_rpad(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hi   ".into()));
    }

    #[test]
    fn lpad_pads_with_custom_fill() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("len", DataType::Int64),
            Field::nullable("fill", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("hi".into()),
                Value::int64(5),
                Value::string("*".into()),
            ]],
        );
        let args = vec![
            Expr::column("str"),
            Expr::column("len"),
            Expr::column("fill"),
        ];
        let result = ProjectionWithExprExec::evaluate_lpad(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("***hi".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("len", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::null(), Value::int64(5)]]);
        let args = vec![Expr::column("str"), Expr::column("len")];
        let result = ProjectionWithExprExec::evaluate_lpad(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hi".into())]]);
        let err = ProjectionWithExprExec::evaluate_lpad(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "LPAD");
    }
}
