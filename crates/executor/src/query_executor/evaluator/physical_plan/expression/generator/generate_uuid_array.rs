use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_generate_uuid_array(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() != 1 {
            return Err(Error::invalid_query(
                "GENERATE_UUID_ARRAY requires exactly 1 argument (count)".to_string(),
            ));
        }
        let count_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let count = if count_val.is_null() {
            return Err(Error::invalid_query(
                "GENERATE_UUID_ARRAY count cannot be NULL".to_string(),
            ));
        } else if let Some(n) = count_val.as_i64() {
            n
        } else {
            return Err(Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: format!("{:?}", count_val.data_type()),
            });
        };
        crate::functions::generator::generate_uuid_array(count)
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;
    use crate::tests::support::assert_error_contains;

    fn schema_with_int() -> Schema {
        Schema::from_fields(vec![Field::nullable("count", DataType::Int64)])
    }

    #[test]
    fn generates_array_of_uuids() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::int64(3)]]);
        let args = vec![Expr::column("count")];
        let result =
            ProjectionWithExprExec::eval_generate_uuid_array(&args, &batch, 0).expect("success");

        if let Some(items) = result.as_array() {
            assert_eq!(items.len(), 3, "expected 3 UUIDs");
            for item in items {
                if let Some(s) = item.as_str() {
                    Uuid::parse_str(s).expect("valid UUID string");
                } else {
                    panic!("Expected String")
                }
            }
        } else {
            panic!("expected Array value")
        }
    }

    #[test]
    fn generates_empty_array_for_zero_count() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::int64(0)]]);
        let args = vec![Expr::column("count")];
        let result =
            ProjectionWithExprExec::eval_generate_uuid_array(&args, &batch, 0).expect("success");
        assert_eq!(result, Value::array(vec![]));
    }

    #[test]
    fn errors_on_null_count() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::null()]]);
        let args = vec![Expr::column("count")];
        let err = ProjectionWithExprExec::eval_generate_uuid_array(&args, &batch, 0)
            .expect_err("null count");
        assert_error_contains(&err, "NULL");
    }

    #[test]
    fn errors_on_type_mismatch() {
        let schema = Schema::from_fields(vec![Field::nullable("count", DataType::String)]);
        let batch = create_batch(schema, vec![vec![Value::string("not-a-number".into())]]);
        let args = vec![Expr::column("count")];
        let err = ProjectionWithExprExec::eval_generate_uuid_array(&args, &batch, 0)
            .expect_err("type mismatch");
        assert_error_contains(&err, "INT64");
    }

    #[test]
    fn validates_argument_count() {
        let batch = create_batch(schema_with_int(), vec![vec![Value::int64(1)]]);
        let err = ProjectionWithExprExec::eval_generate_uuid_array(&[], &batch, 0)
            .expect_err("missing argument");
        assert_error_contains(&err, "GENERATE_UUID_ARRAY");
    }
}
