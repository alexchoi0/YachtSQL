use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_append(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("ARRAY_APPEND", args, 2)?;
        let arr_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let elem_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        crate::functions::array::array_append(arr_val, elem_val)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::{DataType, Value};
    use yachtsql_optimizer::expr::Expr;
    use yachtsql_storage::{Field, Schema};

    use super::*;

    fn make_schema(fields: Vec<Field>) -> Schema {
        Schema::from_fields(fields)
    }

    fn build_batch(schema: Schema, rows: Vec<Vec<Value>>) -> Table {
        Table::from_values(schema, rows).expect("record batch should build")
    }

    #[test]
    fn evaluate_array_append_appends_element() {
        let schema = make_schema(vec![
            Field::nullable("arr", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("elem", DataType::Int64),
        ]);

        let batch = build_batch(
            schema,
            vec![vec![
                Value::array(vec![Value::int64(1), Value::int64(2)]),
                Value::int64(3),
            ]],
        );

        let args = vec![Expr::column("arr"), Expr::column("elem")];
        let result =
            ProjectionWithExprExec::evaluate_array_append(&args, &batch, 0).expect("success");

        assert_eq!(
            result,
            Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)])
        );
    }

    #[test]
    fn evaluate_array_append_propagates_null_array() {
        let schema = make_schema(vec![
            Field::nullable("arr", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("elem", DataType::Int64),
        ]);

        let batch = build_batch(schema, vec![vec![Value::null(), Value::int64(7)]]);

        let args = vec![Expr::column("arr"), Expr::column("elem")];
        let result =
            ProjectionWithExprExec::evaluate_array_append(&args, &batch, 0).expect("success");

        assert_eq!(result, Value::null());
    }

    #[test]
    fn evaluate_array_append_rejects_non_array_first_argument() {
        let schema = make_schema(vec![
            Field::nullable("arr", DataType::Int64),
            Field::nullable("elem", DataType::Int64),
        ]);

        let batch = build_batch(schema, vec![vec![Value::int64(1), Value::int64(2)]]);

        let args = vec![Expr::column("arr"), Expr::column("elem")];
        let err = ProjectionWithExprExec::evaluate_array_append(&args, &batch, 0)
            .expect_err("type mismatch expected");

        assert!(
            err.to_string().contains("ARRAY"),
            "expected ARRAY type error, got {err}"
        );
    }

    #[test]
    fn evaluate_array_append_validates_argument_count() {
        let schema = make_schema(vec![Field::nullable(
            "arr",
            DataType::Array(Box::new(DataType::Int64)),
        )]);
        let batch = build_batch(schema, vec![vec![Value::array(vec![Value::int64(1)])]]);

        let args = vec![Expr::column("arr")];
        assert!(
            ProjectionWithExprExec::evaluate_array_append(&args, &batch, 0).is_err(),
            "expected arg-count validation failure"
        );
    }
}
