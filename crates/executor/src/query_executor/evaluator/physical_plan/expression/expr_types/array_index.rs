use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_array_index(
        array: &Expr,
        index: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let array_value = Self::evaluate_expr(array, batch, row_idx)?;
        let index_value = Self::evaluate_expr(index, batch, row_idx)?;

        if array_value.is_null() {
            return Ok(Value::null());
        }

        if let Some(json_val) = array_value.as_json() {
            if index_value.is_null() {
                return Ok(Value::null());
            }
            if let Some(i) = index_value.as_i64() {
                return Ok(match json_val.get(i as usize) {
                    Some(v) => Value::json(v.clone()),
                    None => Value::null(),
                });
            } else if let Some(key) = index_value.as_str() {
                return Ok(match json_val.get(key) {
                    Some(v) => Value::json(v.clone()),
                    None => Value::null(),
                });
            } else {
                return Err(Error::TypeMismatch {
                    expected: "INT64 or STRING".to_string(),
                    actual: index_value.data_type().to_string(),
                });
            }
        }

        if let Some(map_entries) = array_value.as_map() {
            if index_value.is_null() {
                return Ok(Value::null());
            }
            for (k, v) in map_entries {
                if Self::values_equal(k, &index_value) {
                    return Ok(v.clone());
                }
            }
            return Ok(Value::null());
        }

        let arr = if let Some(elements) = array_value.as_array() {
            elements
        } else {
            return Err(Error::TypeMismatch {
                expected: "ARRAY".to_string(),
                actual: array_value.data_type().to_string(),
            });
        };

        let idx = if index_value.is_null() {
            return Ok(Value::null());
        } else if let Some(i) = index_value.as_i64() {
            i
        } else {
            return Err(Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: index_value.data_type().to_string(),
            });
        };

        let len = arr.len() as i64;
        let actual_idx = if idx < 0 { len + idx } else { idx - 1 };

        if actual_idx < 0 || actual_idx >= len {
            return Ok(Value::null());
        }

        Ok(arr[actual_idx as usize].clone())
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_common::types::DataType;
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
                ])],
                vec![Value::null()],
            ],
        )
    }

    #[test]
    fn test_array_index_first_element() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(1));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(10));
    }

    #[test]
    fn test_array_index_middle_element() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(2));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(20));
    }

    #[test]
    fn test_array_index_last_element() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(3));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(30));
    }

    #[test]
    fn test_array_index_out_of_bounds_positive() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(10));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_array_index_zero() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(0));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_array_index_negative() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(-1));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(30));
    }

    #[test]
    fn test_array_index_null_array() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Int64(1));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 1);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_array_index_null_index() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::Null);

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_array_index_non_array_type() {
        let batch = create_test_batch();
        let array_expr = Expr::Literal(LiteralValue::String("not_an_array".to_string()));
        let index_expr = Expr::Literal(LiteralValue::Int64(1));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::TypeMismatch { expected, actual }) => {
                assert_eq!(expected, "ARRAY");
                assert_eq!(actual, "STRING");
            }
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[test]
    fn test_array_index_non_int_index() {
        let batch = create_test_batch();
        let array_expr = Expr::Column {
            name: "arr_col".to_string(),
            table: None,
        };
        let index_expr = Expr::Literal(LiteralValue::String("not_an_int".to_string()));

        let result =
            ProjectionWithExprExec::evaluate_array_index(&array_expr, &index_expr, &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::TypeMismatch { expected, actual }) => {
                assert_eq!(expected, "INT64");
                assert_eq!(actual, "STRING");
            }
            _ => panic!("Expected TypeMismatch error"),
        }
    }
}
