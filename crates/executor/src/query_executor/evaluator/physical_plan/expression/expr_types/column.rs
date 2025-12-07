use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_column(
        name: &str,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let schema = batch.schema();
        let col_idx = schema
            .field_index(name)
            .ok_or_else(|| Error::column_not_found(name.to_string()))?;
        let column = batch
            .column(col_idx)
            .ok_or_else(|| Error::column_not_found(name.to_string()))?;
        column.get(row_idx)
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_batch(
            vec![
                ("id", DataType::Int64),
                ("name", DataType::String),
                ("score", DataType::Float64),
            ],
            vec![
                vec![
                    Value::int64(1),
                    Value::string("Alice".to_string()),
                    Value::float64(95.5),
                ],
                vec![
                    Value::int64(2),
                    Value::string("Bob".to_string()),
                    Value::float64(87.3),
                ],
                vec![Value::int64(3), Value::null(), Value::float64(92.0)],
            ],
        )
    }

    #[test]
    fn test_evaluate_column_int64() {
        let batch = create_test_batch();
        let result = ProjectionWithExprExec::evaluate_column("id", &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(1));

        let result = ProjectionWithExprExec::evaluate_column("id", &batch, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(2));
    }

    #[test]
    fn test_evaluate_column_string() {
        let batch = create_test_batch();
        let result = ProjectionWithExprExec::evaluate_column("name", &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("Alice".to_string()));

        let result = ProjectionWithExprExec::evaluate_column("name", &batch, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("Bob".to_string()));
    }

    #[test]
    fn test_evaluate_column_null() {
        let batch = create_test_batch();
        let result = ProjectionWithExprExec::evaluate_column("name", &batch, 2);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_evaluate_column_float64() {
        let batch = create_test_batch();
        let result = ProjectionWithExprExec::evaluate_column("score", &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::float64(95.5));
    }

    #[test]
    fn test_evaluate_column_not_found() {
        let batch = create_test_batch();
        let result = ProjectionWithExprExec::evaluate_column("nonexistent", &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::ColumnNotFound(col)) => assert_eq!(col, "nonexistent"),
            _ => panic!("Expected ColumnNotFound error"),
        }
    }
}
