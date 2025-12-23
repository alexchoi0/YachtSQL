use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_javascript_udf_simple_addition() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION add_one(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x + 1;'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT add_one(5)").unwrap();

    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_javascript_udf_string_manipulation() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION reverse_string(s STRING)
            RETURNS STRING
            LANGUAGE JAVASCRIPT
            AS 'return s.split("").reverse().join("");'
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT reverse_string('hello')")
        .unwrap();

    assert_table_eq!(result, [["olleh"]]);
}

#[test]
fn test_javascript_udf_multiple_args() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION multiply_add(a INT64, b INT64, c INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return a * b + c;'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT multiply_add(2, 3, 4)").unwrap();

    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_javascript_udf_float_math() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION circle_area(radius FLOAT64)
            RETURNS FLOAT64
            LANGUAGE js
            AS 'return Math.PI * radius * radius;'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT circle_area(2.0)").unwrap();

    let records = result.to_records().unwrap();
    let value = &records[0].values()[0];
    let expected = std::f64::consts::PI * 4.0;
    match value {
        yachtsql_common::types::Value::Float64(f) => {
            assert!((f.into_inner() - expected).abs() < 0.0001);
        }
        yachtsql_common::types::Value::String(s) => {
            let f: f64 = s.parse().expect("Expected parseable float");
            assert!((f - expected).abs() < 0.0001);
        }
        other => panic!("Expected Float64 or String result, got {:?}", other),
    }
}

#[test]
fn test_javascript_udf_null_handling() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION is_null_check(x INT64)
            RETURNS STRING
            LANGUAGE js
            AS 'return x === null ? "was null" : "not null";'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT is_null_check(NULL)").unwrap();

    assert_table_eq!(result, [["was null"]]);

    let result = session.execute_sql("SELECT is_null_check(42)").unwrap();

    assert_table_eq!(result, [["not null"]]);
}

#[test]
fn test_javascript_udf_arrow_function() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION double_value(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS '(x) => x * 2'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT double_value(21)").unwrap();

    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_javascript_udf_with_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION square(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x * x;'
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT id, square(value) FROM numbers ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 100], [2, 400], [3, 900]]);
}

#[test]
fn test_sql_udf_still_works() {
    let mut session = create_session();

    session
        .execute_sql("CREATE FUNCTION add_ten(x INT64) RETURNS INT64 AS (x + 10)")
        .unwrap();

    let result = session.execute_sql("SELECT add_ten(5)").unwrap();

    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_javascript_udf_or_replace() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION my_func(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x + 1;'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT my_func(5)").unwrap();
    assert_table_eq!(result, [[6]]);

    session
        .execute_sql(
            r#"
            CREATE OR REPLACE FUNCTION my_func(x INT64)
            RETURNS INT64
            LANGUAGE js
            AS 'return x + 100;'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT my_func(5)").unwrap();
    assert_table_eq!(result, [[105]]);
}

#[test]
fn test_javascript_udf_json_parse() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION extract_name(json_str STRING)
            RETURNS STRING
            LANGUAGE js
            AS 'return JSON.parse(json_str).name;'
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql(r#"SELECT extract_name('{"name": "Alice", "age": 30}')"#)
        .unwrap();

    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_javascript_udf_json_stringify() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION make_person(name STRING, age INT64)
            RETURNS STRING
            LANGUAGE js
            AS 'return JSON.stringify({name: name, age: age});'
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT make_person('Bob', 25)")
        .unwrap();

    assert_table_eq!(result, [[r#"{"name":"Bob","age":25}"#]]);
}

#[test]
fn test_javascript_udf_regex() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION extract_digits(s STRING)
            RETURNS STRING
            LANGUAGE js
            AS 'return s.replace(/[^0-9]/g, "");'
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT extract_digits('abc123def456')")
        .unwrap();

    assert_table_eq!(result, [["123456"]]);
}

#[test]
fn test_javascript_udf_multi_statement() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION fibonacci(n INT64)
            RETURNS INT64
            LANGUAGE js
            AS '''
                if (n <= 1) return n;
                let a = 0, b = 1;
                for (let i = 2; i <= n; i++) {
                    let temp = a + b;
                    a = b;
                    b = temp;
                }
                return b;
            '''
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT fibonacci(10)").unwrap();
    assert_table_eq!(result, [[55]]);
}

#[test]
fn test_javascript_udf_array_operations() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION sum_csv(csv STRING)
            RETURNS INT64
            LANGUAGE js
            AS '''
                return csv.split(",").map(x => parseInt(x.trim())).reduce((a, b) => a + b, 0);
            '''
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT sum_csv('1, 2, 3, 4, 5')")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
fn test_javascript_udf_helper_function() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION is_palindrome(s STRING)
            RETURNS BOOL
            LANGUAGE js
            AS '''
                const normalize = str => str.toLowerCase().replace(/[^a-z0-9]/g, "");
                const cleaned = normalize(s);
                const reversed = cleaned.split("").reverse().join("");
                return cleaned === reversed;
            '''
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT is_palindrome('A man a plan a canal Panama')")
        .unwrap();
    assert_table_eq!(result, [[true]]);

    let result = session
        .execute_sql("SELECT is_palindrome('hello')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_javascript_udf_conditional_logic() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION grade(score INT64)
            RETURNS STRING
            LANGUAGE js
            AS '''
                if (score >= 90) return "A";
                if (score >= 80) return "B";
                if (score >= 70) return "C";
                if (score >= 60) return "D";
                return "F";
            '''
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT grade(95), grade(82), grade(55)")
        .unwrap();
    assert_table_eq!(result, [["A", "B", "F"]]);
}

#[test]
fn test_javascript_udf_string_formatting() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION format_phone(digits STRING)
            RETURNS STRING
            LANGUAGE js
            AS '''
                const d = digits.replace(/\D/g, "");
                if (d.length !== 10) return "Invalid";
                return `(${d.slice(0,3)}) ${d.slice(3,6)}-${d.slice(6)}`;
            '''
        "#,
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT format_phone('5551234567')")
        .unwrap();
    assert_table_eq!(result, [["(555) 123-4567"]]);
}

#[test]
fn test_javascript_udf_math_functions() {
    let mut session = create_session();

    session
        .execute_sql(
            r#"
            CREATE FUNCTION hypotenuse(a FLOAT64, b FLOAT64)
            RETURNS FLOAT64
            LANGUAGE js
            AS 'return Math.sqrt(a * a + b * b);'
        "#,
        )
        .unwrap();

    let result = session.execute_sql("SELECT hypotenuse(3.0, 4.0)").unwrap();
    let records = result.to_records().unwrap();
    let value = &records[0].values()[0];
    match value {
        yachtsql_common::types::Value::Float64(f) => {
            assert!((f.into_inner() - 5.0).abs() < 0.0001);
        }
        yachtsql_common::types::Value::String(s) => {
            let f: f64 = s.parse().expect("Expected parseable float");
            assert!((f - 5.0).abs() < 0.0001);
        }
        other => panic!("Expected Float64 or String result, got {:?}", other),
    }
}
