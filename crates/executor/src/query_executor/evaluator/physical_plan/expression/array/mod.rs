mod array_append;
mod array_concat;
mod array_contains;
mod array_distinct;
mod array_length;
mod array_position;
mod array_prepend;
mod array_remove;
mod array_replace;
mod array_reverse;
mod array_sort;
mod generate_array;
mod generate_date_array;
mod generate_timestamp_array;
pub mod higher_order;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_array_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "ARRAY_LENGTH" => Self::evaluate_array_length(args, batch, row_idx),
            "ARRAY_CONCAT" | "ARRAY_CAT" => Self::evaluate_array_concat(args, batch, row_idx),
            "ARRAY_REVERSE" => Self::evaluate_array_reverse(args, batch, row_idx),
            "ARRAY_APPEND" => Self::evaluate_array_append(args, batch, row_idx),
            "ARRAY_PREPEND" => Self::evaluate_array_prepend(args, batch, row_idx),
            "ARRAY_POSITION" => Self::evaluate_array_position(args, batch, row_idx),
            "ARRAY_CONTAINS" => Self::evaluate_array_contains(args, batch, row_idx),
            "ARRAY_REMOVE" => Self::evaluate_array_remove(args, batch, row_idx),
            "ARRAY_REPLACE" => Self::evaluate_array_replace(args, batch, row_idx),
            "ARRAY_SORT" => Self::evaluate_array_sort(args, batch, row_idx),
            "ARRAY_DISTINCT" => Self::evaluate_array_distinct(args, batch, row_idx),
            "GENERATE_ARRAY" => Self::evaluate_generate_array(args, batch, row_idx),
            "GENERATE_DATE_ARRAY" => Self::evaluate_generate_date_array(args, batch, row_idx),
            "GENERATE_TIMESTAMP_ARRAY" => {
                Self::evaluate_generate_timestamp_array(args, batch, row_idx)
            }
            _ => Err(Error::unsupported_feature(format!(
                "Unknown array function: {}",
                name
            ))),
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
    fn dispatches_known_function() {
        let schema = Schema::from_fields(vec![Field::nullable(
            "arr",
            DataType::Array(Box::new(DataType::Int64)),
        )]);
        let batch = create_batch(
            schema,
            vec![vec![Value::array(vec![Value::int64(1), Value::int64(2)])]],
        );

        let args = vec![Expr::column("arr")];
        let value =
            ProjectionWithExprExec::evaluate_array_function("ARRAY_LENGTH", &args, &batch, 0)
                .expect("dispatch should succeed");

        assert_eq!(value, Value::int64(2));
    }

    #[test]
    fn dispatches_aliases() {
        let schema = Schema::from_fields(vec![
            Field::nullable("arr1", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("arr2", DataType::Array(Box::new(DataType::Int64))),
        ]);
        let batch = create_batch(
            schema,
            vec![vec![
                Value::array(vec![Value::int64(1)]),
                Value::array(vec![Value::int64(2)]),
            ]],
        );

        let args = vec![Expr::column("arr1"), Expr::column("arr2")];
        let value = ProjectionWithExprExec::evaluate_array_function("ARRAY_CAT", &args, &batch, 0)
            .expect("ARRAY_CAT should use concat evaluator");

        assert_eq!(value, Value::array(vec![Value::int64(1), Value::int64(2)]));
    }

    #[test]
    fn errors_on_unknown_function() {
        let schema = Schema::from_fields(vec![Field::nullable(
            "arr",
            DataType::Array(Box::new(DataType::Int64)),
        )]);
        let batch = create_batch(schema, vec![vec![Value::array(vec![Value::int64(1)])]]);

        let err = ProjectionWithExprExec::evaluate_array_function(
            "ARRAY_DOES_NOT_EXIST",
            &[Expr::column("arr")],
            &batch,
            0,
        )
        .expect_err("unknown function should error");
        assert_error_contains(&err, "Unknown array function");
    }
}
