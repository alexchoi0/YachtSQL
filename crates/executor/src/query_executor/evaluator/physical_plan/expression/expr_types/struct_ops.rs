use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_struct_literal(
        fields: &[yachtsql_optimizer::expr::StructLiteralField],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let mut struct_map = IndexMap::new();
        for field in fields {
            let value = Self::evaluate_expr(&field.expr, batch, row_idx)?;
            struct_map.insert(field.name.clone(), value);
        }
        Ok(Value::struct_val(struct_map))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_struct_field_access(
        expr: &Expr,
        field: &str,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        let struct_value = Self::evaluate_expr(expr, batch, row_idx)?;
        debug_print::debug_eprintln!(
            "[executor::struct_field] accessing field '{}' from value {:?}",
            field,
            struct_value
        );
        if let Some(map) = struct_value.as_struct() {
            debug_print::debug_eprintln!(
                "[executor::struct_field] struct has fields: {:?}",
                map.keys().collect::<Vec<_>>()
            );
            if let Some(value) = map.get(field) {
                Ok(value.clone())
            } else if let Some((_, value)) = map.iter().find(|(k, _)| k.eq_ignore_ascii_case(field))
            {
                Ok(value.clone())
            } else {
                Err(Error::invalid_query(format!(
                    "Struct does not have field '{}'",
                    field
                )))
            }
        } else if struct_value.is_null() {
            Ok(Value::null())
        } else {
            Err(Error::TypeMismatch {
                expected: "STRUCT".to_string(),
                actual: struct_value.data_type().to_string(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use yachtsql_core::types::DataType;
    use yachtsql_optimizer::expr::{LiteralValue, StructLiteralField};

    use super::*;
    use crate::query_executor::evaluator::physical_plan::expression::test_utils::*;

    fn create_test_batch() -> Table {
        create_batch(
            vec![("col1", DataType::Int64)],
            vec![vec![Value::int64(42)], vec![Value::int64(99)]],
        )
    }

    #[test]
    fn test_evaluate_struct_literal_simple() {
        let batch = create_test_batch();
        let fields = vec![
            StructLiteralField {
                name: "id".to_string(),
                expr: Expr::Literal(LiteralValue::Int64(1)),
                declared_type: None,
            },
            StructLiteralField {
                name: "name".to_string(),
                expr: Expr::Literal(LiteralValue::String("Alice".to_string())),
                declared_type: None,
            },
        ];

        let result = ProjectionWithExprExec::evaluate_struct_literal(&fields, &batch, 0);
        assert!(result.is_ok());

        let value = result.unwrap();
        let map = value.as_struct().expect("Expected Struct value");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("id"), Some(&Value::int64(1)));
        assert_eq!(map.get("name"), Some(&Value::string("Alice".to_string())));
    }

    #[test]
    fn test_evaluate_struct_literal_with_column() {
        let batch = create_test_batch();
        let fields = vec![
            StructLiteralField {
                name: "value".to_string(),
                expr: Expr::Column {
                    name: "col1".to_string(),
                    table: None,
                },
                declared_type: None,
            },
            StructLiteralField {
                name: "doubled".to_string(),
                expr: Expr::BinaryOp {
                    left: Box::new(Expr::Column {
                        name: "col1".to_string(),
                        table: None,
                    }),
                    op: yachtsql_optimizer::BinaryOp::Multiply,
                    right: Box::new(Expr::Literal(LiteralValue::Int64(2))),
                },
                declared_type: None,
            },
        ];

        let result = ProjectionWithExprExec::evaluate_struct_literal(&fields, &batch, 0);
        assert!(result.is_ok());

        let value = result.unwrap();
        let map = value.as_struct().expect("Expected Struct value");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get("value"), Some(&Value::int64(42)));
        assert_eq!(map.get("doubled"), Some(&Value::int64(84)));
    }

    #[test]
    fn test_evaluate_struct_literal_empty() {
        let batch = create_test_batch();
        let fields: Vec<StructLiteralField> = vec![];

        let result = ProjectionWithExprExec::evaluate_struct_literal(&fields, &batch, 0);
        assert!(result.is_ok());

        let value = result.unwrap();
        let map = value.as_struct().expect("Expected Struct value");
        assert_eq!(map.len(), 0);
    }

    #[test]
    fn test_evaluate_struct_field_access_existing_field() {
        let batch = create_test_batch();
        let struct_expr = Expr::StructLiteral {
            fields: vec![
                StructLiteralField {
                    name: "x".to_string(),
                    expr: Expr::Literal(LiteralValue::Int64(10)),
                    declared_type: None,
                },
                StructLiteralField {
                    name: "y".to_string(),
                    expr: Expr::Literal(LiteralValue::String("hello".to_string())),
                    declared_type: None,
                },
            ],
        };

        let result =
            ProjectionWithExprExec::evaluate_struct_field_access(&struct_expr, "x", &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::int64(10));

        let result =
            ProjectionWithExprExec::evaluate_struct_field_access(&struct_expr, "y", &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::string("hello".to_string()));
    }

    #[test]
    fn test_evaluate_struct_field_access_missing_field() {
        let batch = create_test_batch();
        let struct_expr = Expr::StructLiteral {
            fields: vec![StructLiteralField {
                name: "x".to_string(),
                expr: Expr::Literal(LiteralValue::Int64(10)),
                declared_type: None,
            }],
        };

        let result = ProjectionWithExprExec::evaluate_struct_field_access(
            &struct_expr,
            "missing",
            &batch,
            0,
        );
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("missing") && err_msg.contains("field"));
    }

    #[test]
    fn test_evaluate_struct_field_access_null_struct() {
        let batch = create_test_batch();
        let struct_expr = Expr::Literal(LiteralValue::Null);

        let result =
            ProjectionWithExprExec::evaluate_struct_field_access(&struct_expr, "x", &batch, 0);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), Value::null());
    }

    #[test]
    fn test_evaluate_struct_field_access_non_struct() {
        let batch = create_test_batch();
        let non_struct_expr = Expr::Literal(LiteralValue::String("not_a_struct".to_string()));

        let result =
            ProjectionWithExprExec::evaluate_struct_field_access(&non_struct_expr, "x", &batch, 0);
        assert!(result.is_err());
        match result {
            Err(Error::TypeMismatch { expected, actual }) => {
                assert_eq!(expected, "STRUCT");
                assert_eq!(actual, "STRING");
            }
            _ => panic!("Expected TypeMismatch error"),
        }
    }

    #[test]
    fn test_evaluate_struct_field_access_nested() {
        let batch = create_test_batch();
        let struct_expr = Expr::StructLiteral {
            fields: vec![
                StructLiteralField {
                    name: "inner".to_string(),
                    expr: Expr::StructLiteral {
                        fields: vec![StructLiteralField {
                            name: "nested_value".to_string(),
                            expr: Expr::Literal(LiteralValue::Int64(100)),
                            declared_type: None,
                        }],
                    },
                    declared_type: None,
                },
                StructLiteralField {
                    name: "simple".to_string(),
                    expr: Expr::Literal(LiteralValue::String("test".to_string())),
                    declared_type: None,
                },
            ],
        };

        let result =
            ProjectionWithExprExec::evaluate_struct_field_access(&struct_expr, "inner", &batch, 0);
        assert!(result.is_ok());
        let value = result.unwrap();
        let map = value.as_struct().expect("Expected nested Struct value");
        assert_eq!(map.get("nested_value"), Some(&Value::int64(100)));
    }
}
