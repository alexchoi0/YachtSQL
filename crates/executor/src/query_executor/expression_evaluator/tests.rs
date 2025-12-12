use sqlparser::ast::{Expr as SqlExpr, Value as SqlValue};
use yachtsql_common::types::{DataType, Value};
use yachtsql_storage::{Field, Row, Schema};

use super::*;

fn create_test_schema() -> Schema {
    Schema::from_fields(vec![
        Field::nullable("id".to_string(), DataType::Int64),
        Field::nullable("name".to_string(), DataType::String),
        Field::nullable("price".to_string(), DataType::Float64),
        Field::nullable("quantity".to_string(), DataType::Int64),
        Field::nullable("active".to_string(), DataType::Bool),
    ])
}

fn create_test_row() -> Row {
    Row::from_values(vec![
        Value::int64(1),
        Value::string("test".to_string()),
        Value::float64(10.5),
        Value::int64(5),
        Value::bool_val(true),
    ])
}

fn literal_expr(value: SqlValue) -> SqlExpr {
    SqlExpr::Value(value.with_empty_span())
}

fn literal_value(value: SqlValue) -> sqlparser::ast::ValueWithSpan {
    value.with_empty_span()
}

#[test]
fn test_compare_values_integers() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.compare_values(&Value::int64(5), &Value::int64(3));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Greater);

    let result = evaluator.compare_values(&Value::int64(3), &Value::int64(5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Less);

    let result = evaluator.compare_values(&Value::int64(5), &Value::int64(5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Equal);
}

#[test]
fn test_compare_values_with_null() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.compare_values(&Value::null(), &Value::int64(5));
    assert!(result.is_ok());

    let result = evaluator.compare_values(&Value::int64(5), &Value::null());
    assert!(result.is_ok());

    let result = evaluator.compare_values(&Value::null(), &Value::null());
    assert!(result.is_ok());
}

#[test]
fn test_compare_values_floats() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.compare_values(&Value::float64(5.5), &Value::float64(3.2));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Greater);

    let result = evaluator.compare_values(&Value::float64(3.2), &Value::float64(5.5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Less);
}

#[test]
fn test_compare_values_strings() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.compare_values(
        &Value::string("apple".to_string()),
        &Value::string("banana".to_string()),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Less);

    let result = evaluator.compare_values(
        &Value::string("zebra".to_string()),
        &Value::string("apple".to_string()),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), std::cmp::Ordering::Greater);
}

#[test]
fn test_value_to_bool_true_values() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    assert_eq!(
        evaluator.value_to_bool(&Value::bool_val(true)).unwrap(),
        true
    );
    assert_eq!(evaluator.value_to_bool(&Value::int64(1)).unwrap(), true);
    assert_eq!(evaluator.value_to_bool(&Value::int64(-5)).unwrap(), true);
}

#[test]
fn test_value_to_bool_false_values() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    assert_eq!(
        evaluator.value_to_bool(&Value::bool_val(false)).unwrap(),
        false
    );
    assert_eq!(evaluator.value_to_bool(&Value::int64(0)).unwrap(), false);

    assert_eq!(evaluator.value_to_bool(&Value::null()).unwrap(), false);
}

#[test]
fn test_negate_value_integers() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.negate_value(&Value::int64(5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(-5));

    let result = evaluator.negate_value(&Value::int64(-5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(5));

    let result = evaluator.negate_value(&Value::int64(0));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(0));
}

#[test]
fn test_negate_value_floats() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.negate_value(&Value::float64(5.5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(-5.5));

    let result = evaluator.negate_value(&Value::float64(-3.2));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(3.2));
}

#[test]
fn test_negate_value_null() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.negate_value(&Value::null());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_apply_not() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    let result = evaluator.apply_not(&Value::bool_val(true));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(false));

    let result = evaluator.apply_not(&Value::bool_val(false));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(true));
}

#[test]
fn test_value_to_string() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);

    assert_eq!(
        evaluator
            .value_to_string(&Value::string("hello".to_string()))
            .unwrap(),
        "hello"
    );
    assert_eq!(evaluator.value_to_string(&Value::int64(42)).unwrap(), "42");
    assert_eq!(
        evaluator.value_to_string(&Value::float64(3.14)).unwrap(),
        "3.14"
    );
    assert_eq!(
        evaluator.value_to_string(&Value::bool_val(true)).unwrap(),
        "true"
    );
    assert_eq!(evaluator.value_to_string(&Value::null()).unwrap(), "");
}

#[test]
fn test_apply_arithmetic_op_addition() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::BinaryOperator;

    let result =
        evaluator.apply_arithmetic_op(&Value::int64(5), &BinaryOperator::Plus, &Value::int64(3));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(8));

    let result = evaluator.apply_arithmetic_op(
        &Value::float64(5.5),
        &BinaryOperator::Plus,
        &Value::float64(3.2),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(8.7));
}

#[test]
fn test_apply_arithmetic_op_subtraction() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::BinaryOperator;

    let result =
        evaluator.apply_arithmetic_op(&Value::int64(10), &BinaryOperator::Minus, &Value::int64(3));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(7));

    let result = evaluator.apply_arithmetic_op(
        &Value::float64(10.5),
        &BinaryOperator::Minus,
        &Value::float64(3.2),
    );
    assert!(result.is_ok());
    let val = result.unwrap();
    if let Some(f) = val.as_f64() {
        assert!((f - 7.3).abs() < 0.001);
    } else {
        panic!("Expected Float64");
    }
}

#[test]
fn test_apply_arithmetic_op_multiplication() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::BinaryOperator;

    let result = evaluator.apply_arithmetic_op(
        &Value::int64(5),
        &BinaryOperator::Multiply,
        &Value::int64(3),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(15));

    let result = evaluator.apply_arithmetic_op(
        &Value::float64(5.0),
        &BinaryOperator::Multiply,
        &Value::float64(3.0),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(15.0));
}

#[test]
fn test_apply_arithmetic_op_division() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::BinaryOperator;

    let result =
        evaluator.apply_arithmetic_op(&Value::int64(10), &BinaryOperator::Divide, &Value::int64(2));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(5));

    let result = evaluator.apply_arithmetic_op(
        &Value::float64(10.0),
        &BinaryOperator::Divide,
        &Value::float64(4.0),
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(2.5));
}

#[test]
fn test_apply_arithmetic_op_division_by_zero() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::BinaryOperator;

    let result =
        evaluator.apply_arithmetic_op(&Value::int64(10), &BinaryOperator::Divide, &Value::int64(0));
    assert!(result.is_err());

    let result = evaluator.apply_arithmetic_op(
        &Value::float64(10.0),
        &BinaryOperator::Divide,
        &Value::float64(0.0),
    );
    assert!(result.is_ok());
    assert!(result.unwrap().as_f64().unwrap().is_infinite());
}

#[test]
fn test_apply_arithmetic_op_null_propagation() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::BinaryOperator;

    let result =
        evaluator.apply_arithmetic_op(&Value::null(), &BinaryOperator::Plus, &Value::int64(5));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());

    let result =
        evaluator.apply_arithmetic_op(&Value::int64(5), &BinaryOperator::Plus, &Value::null());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());

    let result =
        evaluator.apply_arithmetic_op(&Value::null(), &BinaryOperator::Multiply, &Value::null());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_get_column_value() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = create_test_row();

    let result = evaluator.get_column_value(&row, "id");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(1));

    let result = evaluator.get_column_value(&row, "name");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("test".to_string()));

    let result = evaluator.get_column_value(&row, "price");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(10.5));

    let result = evaluator.get_column_value(&row, "active");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(true));
}

#[test]
fn test_get_column_value_not_found() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = create_test_row();

    let result = evaluator.get_column_value(&row, "nonexistent");
    assert!(result.is_err());
}

#[test]
fn test_sql_value_to_value_number() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::Value as SqlValue;

    let result =
        evaluator.sql_value_to_value(&SqlValue::Number("42".to_string(), false).with_empty_span());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(42));

    let result = evaluator
        .sql_value_to_value(&SqlValue::Number("3.14".to_string(), false).with_empty_span());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(3.14));
}

#[test]
fn test_sql_value_to_value_string() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::Value as SqlValue;

    let result = evaluator
        .sql_value_to_value(&SqlValue::SingleQuotedString("hello".to_string()).with_empty_span());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("hello".to_string()));
}

#[test]
fn test_sql_value_to_value_boolean() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    use sqlparser::ast::Value as SqlValue;

    let result = evaluator.sql_value_to_value(&SqlValue::Boolean(true).with_empty_span());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(true));

    let result = evaluator.sql_value_to_value(&SqlValue::Boolean(false).with_empty_span());
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(false));
}

#[test]
fn test_sql_value_to_value_null() {
    let schema = create_test_schema();
    let evaluator = ExpressionEvaluator::new(&schema);
    let result = evaluator.sql_value_to_value(&literal_value(SqlValue::Null));
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_coalesce_returns_first_non_null() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("COALESCE"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("test".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("default".to_string()),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("test".to_string()));
}

#[test]
fn test_coalesce_all_null_returns_null() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("COALESCE"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_coalesce_with_mixed_types() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("COALESCE"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("42".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(42));
}

#[test]
fn test_nullif_equal_values_returns_null() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("NULLIF"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_nullif_different_values_returns_first() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("NULLIF"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("10".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(5));
}

#[test]
fn test_nullif_with_strings() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("NULLIF"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("hello".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("hello".to_string()),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_nullif_with_nulls() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("NULLIF"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_greatest_returns_maximum() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("GREATEST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("10".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("3".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("8".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(10));
}

#[test]
fn test_greatest_with_nulls() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("GREATEST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("10".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(10));
}

#[test]
fn test_greatest_all_nulls_returns_null() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("GREATEST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_greatest_with_strings() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("GREATEST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("apple".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("zebra".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("banana".to_string()),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("zebra".to_string()));
}

#[test]
fn test_least_returns_minimum() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("LEAST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("10".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("3".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("8".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(3));
}

#[test]
fn test_least_with_nulls() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("LEAST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("3".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(3));
}

#[test]
fn test_least_all_nulls_returns_null() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("LEAST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::null());
}

#[test]
fn test_least_with_strings() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("LEAST"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("apple".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("zebra".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("banana".to_string()),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("apple".to_string()));
}

#[test]
fn test_ifnull_with_null() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("IFNULL"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Null,
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("default".to_string()),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("default".to_string()));
}

#[test]
fn test_nvl_with_value() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("NVL"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("5".to_string(), false),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("10".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(5));
}

#[test]
fn test_isnull_true() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("ISNULL"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Null,
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(true));
}

#[test]
fn test_isnull_false() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("ISNULL"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("5".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::bool_val(false));
}

#[test]
fn test_abs_negative() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("ABS"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("-5".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(5));
}

#[test]
fn test_sign_positive() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("SIGN"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("42".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(1));
}

#[test]
fn test_sign_negative() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("SIGN"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("-42".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(-1));
}

#[test]
fn test_sign_zero() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("SIGN"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("0".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::int64(0));
}

#[test]
fn test_ceil_float() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("CEIL"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("3.2".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(4.0));
}

#[test]
fn test_floor_float() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("FLOOR"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::Number("3.8".to_string(), false),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::float64(3.0));
}

#[test]
fn test_trim_whitespace() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("TRIM"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::SingleQuotedString("  hello  ".to_string()),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("hello".to_string()));
}

#[test]
fn test_ltrim_whitespace() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("LTRIM"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::SingleQuotedString("  hello  ".to_string()),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("hello  ".to_string()));
}

#[test]
fn test_rtrim_whitespace() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("RTRIM"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::SingleQuotedString("  hello  ".to_string()),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("  hello".to_string()));
}

#[test]
fn test_reverse_string() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("REVERSE"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                sqlparser::ast::Value::SingleQuotedString("hello".to_string()),
            )))],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("olleh".to_string()));
}

#[test]
fn test_repeat_string() {
    use sqlparser::ast::{
        Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
        ObjectName,
    };

    let schema = create_test_schema();
    let row = create_test_row();
    let evaluator = ExpressionEvaluator::new(&schema);

    let func = Function {
        name: ObjectName(vec![sqlparser::ast::ObjectNamePart::Identifier(
            Ident::new("REPEAT"),
        )]),
        parameters: sqlparser::ast::FunctionArguments::None,
        args: FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::SingleQuotedString("ab".to_string()),
                ))),
                FunctionArg::Unnamed(FunctionArgExpr::Expr(literal_expr(
                    sqlparser::ast::Value::Number("3".to_string(), false),
                ))),
            ],
            clauses: vec![],
        }),
        filter: None,
        null_treatment: None,
        over: None,
        within_group: vec![],
        uses_odbc_syntax: false,
    };

    let result = evaluator.evaluate_function(&func, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), Value::string("ababab".to_string()));
}
