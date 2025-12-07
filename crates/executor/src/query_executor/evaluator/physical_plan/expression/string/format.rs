use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_format(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(crate::error::Error::invalid_query(
                "FORMAT requires at least 1 argument (format string)".to_string(),
            ));
        }

        let format_val = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if format_val.is_null() {
            return Ok(Value::null());
        }

        if let Some(format_str) = format_val.as_str() {
            let mut arg_values = Vec::new();
            for arg in args.iter().skip(1) {
                arg_values.push(Self::evaluate_expr(arg, batch, row_idx)?);
            }

            Self::apply_format_string(format_str, &arg_values)
        } else {
            Err(crate::error::Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: format_val.data_type().to_string(),
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
    fn formats_string_with_no_placeholders() {
        let schema = Schema::from_fields(vec![Field::nullable("fmt", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("Hello".into())]]);
        let args = vec![Expr::column("fmt")];
        let result = ProjectionWithExprExec::evaluate_format(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello".into()));
    }

    #[test]
    fn formats_string_with_percent_s() {
        let schema = Schema::from_fields(vec![
            Field::nullable("fmt", DataType::String),
            Field::nullable("arg", DataType::String),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::string("Hello %s".into()),
                Value::string("World".into()),
            ]],
        );
        let args = vec![Expr::column("fmt"), Expr::column("arg")];
        let result = ProjectionWithExprExec::evaluate_format(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Hello World".into()));
    }

    #[test]
    fn formats_string_with_integer() {
        let schema = Schema::from_fields(vec![
            Field::nullable("fmt", DataType::String),
            Field::nullable("num", DataType::Int64),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string("Number: %s".into()), Value::int64(42)]],
        );
        let args = vec![Expr::column("fmt"), Expr::column("num")];
        let result = ProjectionWithExprExec::evaluate_format(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::string("Number: 42".into()));
    }

    #[test]
    fn propagates_null_format_string() {
        let schema = Schema::from_fields(vec![Field::nullable("fmt", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::null()]]);
        let args = vec![Expr::column("fmt")];
        let result = ProjectionWithExprExec::evaluate_format(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::null());
    }

    #[test]
    fn validates_missing_format_string() {
        let schema = Schema::new();
        let batch = create_batch(schema, vec![vec![]]);
        let err = ProjectionWithExprExec::evaluate_format(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "FORMAT");
    }
}
