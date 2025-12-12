use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_right(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        use yachtsql_common::error::Error;

        if args.len() != 2 {
            return Err(Error::invalid_query("RIGHT requires exactly 2 arguments"));
        }

        let first_arg = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let second_arg = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if first_arg.is_null() || second_arg.is_null() {
            return Ok(Value::null());
        }

        let n = second_arg.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: second_arg.data_type().to_string(),
        })?;

        if let Some(bytes) = first_arg.as_bytes() {
            if n <= 0 {
                return Ok(Value::bytes(vec![]));
            }
            let skip = bytes.len().saturating_sub(n as usize);
            return Ok(Value::bytes(bytes[skip..].to_vec()));
        }

        let s = first_arg.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: first_arg.data_type().to_string(),
        })?;

        if n <= 0 {
            Ok(Value::string(String::new()))
        } else {
            let chars: Vec<char> = s.chars().collect();
            let skip = chars.len().saturating_sub(n as usize);
            Ok(Value::string(chars[skip..].iter().collect()))
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    #[test]
    fn returns_rightmost_characters() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello world".into()), Value::int64(5)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_right(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("world".into()));
    }

    #[test]
    fn returns_entire_string_when_n_exceeds_length() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(100)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_right(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("hello".into()));
    }

    #[test]
    fn returns_empty_string_for_zero_or_negative() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("hello".into()), Value::int64(0)]],
        );
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_right(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("".into()));
    }

    #[test]
    fn propagates_null() {
        let schema = Schema::from_fields(vec![
            Field::nullable("str", DataType::String),
            Field::nullable("n", DataType::Int64),
        ]);
        let batch = create_batch(schema, vec![vec![Value::null(), Value::int64(5)]]);
        let args = vec![Expr::column("str"), Expr::column("n")];
        let result = ProjectionWithExprExec::evaluate_right(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("str", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("hello".into())]]);
        let err = ProjectionWithExprExec::evaluate_right(&[Expr::column("str")], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "RIGHT");
    }
}
