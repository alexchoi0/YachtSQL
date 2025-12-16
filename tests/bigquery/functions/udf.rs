use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_javascript_udf_simple_addition() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION add_one(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x + 1;'
        "#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT add_one(5)").unwrap();

    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_javascript_udf_string_manipulation() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION reverse_string(s STRING)
            RETURNS STRING
            LANGUAGE JAVASCRIPT
            AS 'return s.split("").reverse().join("");'
        "#,
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT reverse_string('hello')")
        .unwrap();

    assert_table_eq!(result, [["olleh"]]);
}

#[test]
fn test_javascript_udf_multiple_args() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION multiply_add(a INT64, b INT64, c INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return a * b + c;'
        "#,
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT multiply_add(2, 3, 4)")
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_javascript_udf_float_math() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION circle_area(radius FLOAT64)
            RETURNS FLOAT64
            LANGUAGE js
            AS 'return Math.PI * radius * radius;'
        "#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT circle_area(2.0)").unwrap();

    let records = result.to_records().unwrap();
    let value = &records[0].values()[0];
    match value {
        yachtsql_common::types::Value::Float64(f) => {
            let expected = std::f64::consts::PI * 4.0;
            assert!((f.into_inner() - expected).abs() < 0.0001);
        }
        yachtsql_common::types::Value::Null
        | yachtsql_common::types::Value::Bool(_)
        | yachtsql_common::types::Value::Int64(_)
        | yachtsql_common::types::Value::Numeric(_)
        | yachtsql_common::types::Value::String(_)
        | yachtsql_common::types::Value::Bytes(_)
        | yachtsql_common::types::Value::Date(_)
        | yachtsql_common::types::Value::Time(_)
        | yachtsql_common::types::Value::DateTime(_)
        | yachtsql_common::types::Value::Timestamp(_)
        | yachtsql_common::types::Value::Json(_)
        | yachtsql_common::types::Value::Array(_)
        | yachtsql_common::types::Value::Struct(_)
        | yachtsql_common::types::Value::Geography(_)
        | yachtsql_common::types::Value::Interval(_) => panic!("Expected Float64 result"),
    }
}

#[test]
fn test_javascript_udf_null_handling() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION is_null_check(x INT64)
            RETURNS STRING
            LANGUAGE js
            AS 'return x === null ? "was null" : "not null";'
        "#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT is_null_check(NULL)").unwrap();

    assert_table_eq!(result, [["was null"]]);

    let result = executor.execute_sql("SELECT is_null_check(42)").unwrap();

    assert_table_eq!(result, [["not null"]]);
}

#[test]
fn test_javascript_udf_arrow_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION double_value(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS '(x) => x * 2'
        "#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT double_value(21)").unwrap();

    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_javascript_udf_with_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION square(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x * x;'
        "#,
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, square(value) FROM numbers ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 100], [2, 400], [3, 900]]);
}

#[test]
fn test_sql_udf_still_works() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION add_ten(x INT64) RETURNS INT64 AS (x + 10)")
        .unwrap();

    let result = executor.execute_sql("SELECT add_ten(5)").unwrap();

    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_javascript_udf_or_replace() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"
            CREATE FUNCTION my_func(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x + 1;'
        "#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT my_func(5)").unwrap();
    assert_table_eq!(result, [[6]]);

    executor
        .execute_sql(
            r#"
            CREATE OR REPLACE FUNCTION my_func(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x + 100;'
        "#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT my_func(5)").unwrap();
    assert_table_eq!(result, [[105]]);
}
