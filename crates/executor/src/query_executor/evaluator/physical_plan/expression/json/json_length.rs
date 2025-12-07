use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_json_length(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "JSON_LENGTH requires exactly 1 argument".to_string(),
            ));
        }

        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::json::json_length(&json_val)
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
    fn returns_length_of_json_array() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string(r#"[1,2,3,4,5]"#.into())]]);
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_json_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(5));
    }

    #[test]
    fn returns_length_of_json_object() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(
            schema,
            vec![vec![Value::string(r#"{"a":1,"b":2,"c":3}"#.into())]],
        );
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_json_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(3));
    }

    #[test]
    fn returns_zero_for_empty_array() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("[]".into())]]);
        let args = vec![Expr::column("json")];
        let result =
            ProjectionWithExprExec::evaluate_json_length(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::int64(0));
    }

    #[test]
    fn validates_argument_count() {
        let schema = Schema::from_fields(vec![Field::nullable("json", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string(r#"[1,2,3]"#.into())]]);
        let err =
            ProjectionWithExprExec::evaluate_json_length(&[], &batch, 0).expect_err("no args");
        assert_error_contains(&err, "JSON_LENGTH");
    }
}
