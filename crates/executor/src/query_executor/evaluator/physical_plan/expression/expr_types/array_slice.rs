use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_slice(
        array: &Expr,
        start: &Option<Box<Expr>>,
        end: &Option<Box<Expr>>,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let array_value = Self::evaluate_expr(array, batch, row_idx)?;

        let start_value = if let Some(s) = start {
            Some(Self::evaluate_expr(s, batch, row_idx)?)
        } else {
            None
        };

        let end_value = if let Some(e) = end {
            Some(Self::evaluate_expr(e, batch, row_idx)?)
        } else {
            None
        };

        yachtsql_functions::array::array_slice(
            &array_value,
            start_value.as_ref(),
            end_value.as_ref(),
        )
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::LiteralValue;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_batch(
            vec![("arr_col", DataType::Array(Box::new(DataType::Int64)))],
            vec![
                vec![Value::array(vec![
                    Value::int64(10),
                    Value::int64(20),
                    Value::int64(30),
                    Value::int64(40),
                    Value::int64(50),
                ])],
                vec![Value::null()],
            ],
        )
    }

    #[test]
    fn test_array_slice_middle() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let start_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(2))));
        let end_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(4))));

        let result = ProjectionWithExprExec::evaluate_array_slice(
            &array_expr,
            &start_expr,
            &end_expr,
            &batch,
            0,
        );
        assert!(result.is_ok());
        let arr = result.unwrap();
        let elements = arr.as_array().unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], Value::int64(20));
        assert_eq!(elements[1], Value::int64(30));
        assert_eq!(elements[2], Value::int64(40));
    }

    #[test]
    fn test_array_slice_from_start() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let start_expr = None;
        let end_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(3))));

        let result = ProjectionWithExprExec::evaluate_array_slice(
            &array_expr,
            &start_expr,
            &end_expr,
            &batch,
            0,
        );
        assert!(result.is_ok());
        let arr = result.unwrap();
        let elements = arr.as_array().unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], Value::int64(10));
        assert_eq!(elements[1], Value::int64(20));
        assert_eq!(elements[2], Value::int64(30));
    }

    #[test]
    fn test_array_slice_to_end() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let start_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(3))));
        let end_expr = None;

        let result = ProjectionWithExprExec::evaluate_array_slice(
            &array_expr,
            &start_expr,
            &end_expr,
            &batch,
            0,
        );
        assert!(result.is_ok());
        let arr = result.unwrap();
        let elements = arr.as_array().unwrap();
        assert_eq!(elements.len(), 3);
        assert_eq!(elements[0], Value::int64(30));
        assert_eq!(elements[1], Value::int64(40));
        assert_eq!(elements[2], Value::int64(50));
    }

    #[test]
    fn test_array_slice_full() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let start_expr = None;
        let end_expr = None;

        let result = ProjectionWithExprExec::evaluate_array_slice(
            &array_expr,
            &start_expr,
            &end_expr,
            &batch,
            0,
        );
        assert!(result.is_ok());
        let arr = result.unwrap();
        let elements = arr.as_array().unwrap();
        assert_eq!(elements.len(), 5);
    }

    #[test]
    fn test_array_slice_null_array() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let start_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(1))));
        let end_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(3))));

        let result = ProjectionWithExprExec::evaluate_array_slice(
            &array_expr,
            &start_expr,
            &end_expr,
            &batch,
            1,
        );
        assert!(result.is_ok());
        assert!(result.unwrap().is_null());
    }

    #[test]
    fn test_array_slice_empty_result() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };

        let start_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(4))));
        let end_expr = Some(Box::new(Expr::Literal(LiteralValue::Int64(2))));

        let result = ProjectionWithExprExec::evaluate_array_slice(
            &array_expr,
            &start_expr,
            &end_expr,
            &batch,
            0,
        );
        assert!(result.is_ok());
        let arr = result.unwrap();
        let elements = arr.as_array().unwrap();
        assert_eq!(elements.len(), 0);
    }
}
