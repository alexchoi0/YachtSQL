use crate::assert_table_eq;
use crate::common::{create_session, date};

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_sql() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION add_one(x INT64) RETURNS INT64 AS (x + 1)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT add_one(5)").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_string() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION greet(name STRING) RETURNS STRING AS (CONCAT('Hello, ', name))",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT greet('World')").await.unwrap();
    assert_table_eq!(result, [["Hello, World"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_function() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION my_func(x INT64) RETURNS INT64 AS (x * 2)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE OR REPLACE FUNCTION my_func(x INT64) RETURNS INT64 AS (x * 3)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT my_func(10)").await.unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_if_not_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION existing_func(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE FUNCTION IF NOT EXISTS existing_func(x INT64) RETURNS INT64 AS (x * 100)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT existing_func(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_temp_function() {
    let session = create_session();

    session
        .execute_sql("CREATE TEMP FUNCTION temp_add(a INT64, b INT64) RETURNS INT64 AS (a + b)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT temp_add(3, 4)").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_multiple_params() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION calc(a INT64, b INT64, c INT64) RETURNS INT64 AS (a + b * c)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT calc(1, 2, 3)").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_function() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION to_drop(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    session.execute_sql("DROP FUNCTION to_drop").await.unwrap();

    let result = session.execute_sql("SELECT to_drop(1)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_function_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP FUNCTION IF EXISTS nonexistent_func")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_case() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION classify(x INT64) RETURNS STRING AS (
                CASE
                    WHEN x < 0 THEN 'negative'
                    WHEN x = 0 THEN 'zero'
                    ELSE 'positive'
                END
            )",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT classify(-5)").await.unwrap();
    assert_table_eq!(result, [["negative"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_coalesce() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION safe_value(x INT64) RETURNS INT64 AS (COALESCE(x, 0))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT safe_value(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_in_where_clause() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE FUNCTION is_even(x INT64) RETURNS BOOL AS (MOD(x, 2) = 0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT val FROM numbers WHERE is_even(val) ORDER BY val")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_in_select() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (x INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (10), (20)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE FUNCTION double_it(n INT64) RETURNS INT64 AS (n * 2)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, double_it(x) AS doubled FROM data ORDER BY x")
        .await
        .unwrap();
    assert_table_eq!(result, [[10, 20], [20, 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_procedure() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE my_procedure(x INT64, y INT64)
            BEGIN
                SELECT x + y;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CALL my_procedure(3, 4)")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_procedure() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc1()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE PROCEDURE proc1()
            BEGIN
                SELECT 2;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc1()").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_procedure() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE to_drop_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    session
        .execute_sql("DROP PROCEDURE to_drop_proc")
        .await
        .unwrap();

    let result = session.execute_sql("CALL to_drop_proc()").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_procedure_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP PROCEDURE IF EXISTS nonexistent_proc")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_out_param() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE get_sum(IN a INT64, IN b INT64, OUT result INT64)
            BEGIN
                SET result = a + b;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL get_sum(5, 3, @out)").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_inout_param() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE increment(INOUT x INT64)
            BEGIN
                SET x = x + 1;
            END",
        )
        .await
        .unwrap();

    session.execute_sql("SET @val = 10").await.unwrap();
    session.execute_sql("CALL increment(@val)").await.unwrap();

    let result = session.execute_sql("SELECT @val").await.unwrap();
    assert_table_eq!(result, [[11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_struct_return() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION make_point(x INT64, y INT64) RETURNS STRUCT<x INT64, y INT64> AS (STRUCT(x, y))",
        ).await
        .unwrap();

    let result = session
        .execute_sql("SELECT make_point(1, 2).x, make_point(1, 2).y")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_array_return() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION make_range(n INT64) RETURNS ARRAY<INT64> AS (GENERATE_ARRAY(1, n))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(make_range(3))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_nested_call() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION f1(x INT64) RETURNS INT64 AS (x + 1)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE FUNCTION f2(x INT64) RETURNS INT64 AS (f1(x) * 2)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT f2(5)").await.unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_aggregate_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION my_sum(x INT64)
            RETURNS INT64
            AS (
                SUM(x)
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE agg_data (value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO agg_data VALUES (1), (2), (3), (4), (5)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT my_sum(value) FROM agg_data")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_aggregate_function_with_multiple_args() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION weighted_avg(value FLOAT64, weight FLOAT64)
            RETURNS FLOAT64
            AS (
                SUM(value * weight) / SUM(weight)
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE weighted_data (val FLOAT64, wt FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO weighted_data VALUES (10.0, 1.0), (20.0, 2.0), (30.0, 3.0)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT weighted_avg(val, wt) FROM weighted_data")
        .await
        .unwrap();
    assert_table_eq!(result, [[23.333333333333332]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_aggregate_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION custom_count(x INT64)
            RETURNS INT64
            AS (COUNT(x))",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE AGGREGATE FUNCTION custom_count(x INT64)
            RETURNS INT64
            AS (COUNT(DISTINCT x))",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_aggregate_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION to_drop_agg(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .await
        .unwrap();

    session
        .execute_sql("DROP AGGREGATE FUNCTION to_drop_agg")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT to_drop_agg(1)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION my_table_func(x INT64)
            RETURNS TABLE<id INT64, value INT64>
            AS (
                SELECT id, value * x AS value
                FROM UNNEST([STRUCT(1 AS id, 10 AS value), STRUCT(2, 20)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM my_table_func(2) ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 20], [2, 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_function_any_type() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION names_by_year(y INT64)
            RETURNS TABLE<name STRING, year INT64>
            AS (
                SELECT name, year FROM UNNEST([STRUCT('Alice' AS name, 2020 AS year), STRUCT('Bob', 2021)])
                WHERE year = y
            )",
        ).await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM names_by_year(2020)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_table_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION tv_func()
            RETURNS TABLE<x INT64>
            AS (SELECT 1 AS x)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE TABLE FUNCTION tv_func()
            RETURNS TABLE<x INT64>
            AS (SELECT 2 AS x)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x FROM tv_func()")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table_function() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION to_drop_tvf()
            RETURNS TABLE<x INT64>
            AS (SELECT 1 AS x)",
        )
        .await
        .unwrap();

    session
        .execute_sql("DROP TABLE FUNCTION to_drop_tvf")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM to_drop_tvf()").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_options() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION fn_with_opts(x INT64)
            RETURNS INT64
            OPTIONS (description = 'Adds one to input')
            AS (x + 1)",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT fn_with_opts(5)").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_javascript() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE FUNCTION js_multiply(x FLOAT64, y FLOAT64)
            RETURNS FLOAT64
            LANGUAGE js
            AS r"""
                return x * y;
            """"#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT js_multiply(3.0, 4.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[12.0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_remote() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION remote_fn(x INT64)
            RETURNS INT64
            REMOTE WITH CONNECTION `project.region.connection`
            OPTIONS (
                endpoint = 'https://example.com/function'
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_declare() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_with_declare(x INT64)
            BEGIN
                DECLARE y INT64;
                SET y = x * 2;
                SELECT y;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CALL proc_with_declare(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_if() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_with_if(x INT64)
            BEGIN
                IF x > 0 THEN
                    SELECT 'positive';
                ELSE
                    SELECT 'non-positive';
                END IF;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_with_if(5)").await.unwrap();
    assert_table_eq!(result, [["positive"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_loop() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_with_loop()
            BEGIN
                DECLARE i INT64 DEFAULT 0;
                DECLARE sum INT64 DEFAULT 0;
                WHILE i < 5 DO
                    SET sum = sum + i;
                    SET i = i + 1;
                END WHILE;
                SELECT sum;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_with_loop()").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_exception_handling() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_with_exception()
            BEGIN
                BEGIN
                    SELECT 1/0;
                EXCEPTION WHEN ERROR THEN
                    SELECT 'caught';
                END;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CALL proc_with_exception()")
        .await
        .unwrap();
    assert_table_eq!(result, [["caught"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_if_not_exists() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE existing_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE PROCEDURE IF NOT EXISTS existing_proc()
            BEGIN
                SELECT 2;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL existing_proc()").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_deterministic() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION det_func(x INT64)
            RETURNS INT64
            DETERMINISTIC
            AS (x * 2)",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT det_func(5)").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_not_deterministic() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION nondet_func()
            RETURNS INT64
            NOT DETERMINISTIC
            AS (CAST(RAND() * 100 AS INT64))",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT nondet_func()").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_security_definer() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION secure_func(x INT64)
            RETURNS INT64
            SQL SECURITY DEFINER
            AS (x + 1)",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT secure_func(10)").await.unwrap();
    assert_table_eq!(result, [[11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_security_invoker() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION invoker_func(x INT64)
            RETURNS INT64
            SQL SECURITY INVOKER
            AS (x + 1)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT invoker_func(10)")
        .await
        .unwrap();
    assert_table_eq!(result, [[11]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_data_governance() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION data_gov_func(x INT64)
            RETURNS INT64
            OPTIONS (data_governance_type = 'DATA_MASKING')
            AS (x)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_aggregate_function_with_over() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION my_count(x INT64)
            RETURNS INT64
            AS (COUNT(x))",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE count_data (val INT64, category STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO count_data VALUES (1, 'A'), (2, 'A'), (3, 'B')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT category, my_count(val) FROM count_data GROUP BY category ORDER BY category",
        )
        .await
        .unwrap();
    assert_table_eq!(result, [["A", 2], ["B", 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_aggregate_function_with_filter() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION filtered_sum(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE filter_data (val INT64, active BOOL)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO filter_data VALUES (10, true), (20, false), (30, true)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT filtered_sum(val) FROM filter_data WHERE active")
        .await
        .unwrap();
    assert_table_eq!(result, [[40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_function_with_multiple_params() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION multi_param_tvf(start_val INT64, end_val INT64, step INT64)
            RETURNS TABLE<num INT64>
            AS (
                SELECT num FROM UNNEST(GENERATE_ARRAY(start_val, end_val, step)) AS num
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM multi_param_tvf(0, 10, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0], [2], [4], [6], [8], [10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_function_with_struct_output() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION struct_tvf(prefix STRING)
            RETURNS TABLE<person STRUCT<name STRING, age INT64>>
            AS (
                SELECT STRUCT(CONCAT(prefix, name) AS name, age) AS person
                FROM UNNEST([STRUCT('Alice' AS name, 30 AS age), STRUCT('Bob', 25)])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT person.name FROM struct_tvf('Mr. ') ORDER BY person.name")
        .await
        .unwrap();
    assert_table_eq!(result, [["Mr. Alice"], ["Mr. Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_any_type() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION identity_fn(x ANY TYPE)
            AS (x)",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT identity_fn(42)").await.unwrap();
    assert_table_eq!(result, [[42]]);

    let result = session
        .execute_sql("SELECT identity_fn('hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_lambda() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION apply_twice(x INT64, fn ANY TYPE)
            AS (fn(fn(x)))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT apply_twice(5, (x) -> x + 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_in_schema() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA fn_schema")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE FUNCTION fn_schema.qualified_func(x INT64)
            RETURNS INT64
            AS (x * 10)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT fn_schema.qualified_func(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[50]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_function_with_signature() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION overloaded(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    session
        .execute_sql("DROP FUNCTION overloaded(INT64)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT overloaded(1)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_returns_table() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION get_users()
            RETURNS TABLE<id INT64, name STRING>
            AS (
                SELECT * FROM UNNEST([STRUCT(1 AS id, 'Alice' AS name), STRUCT(2, 'Bob')])
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM get_users() ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_for_loop() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_for_loop()
            BEGIN
                DECLARE total INT64 DEFAULT 0;
                FOR i IN (SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x) DO
                    SET total = total + i.x;
                END FOR;
                SELECT total;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_for_loop()").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_repeat() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_repeat()
            BEGIN
                DECLARE i INT64 DEFAULT 1;
                DECLARE sum INT64 DEFAULT 0;
                REPEAT
                    SET sum = sum + i;
                    SET i = i + 1;
                UNTIL i > 5
                END REPEAT;
                SELECT sum;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_repeat()").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_case_statement() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_case(x INT64)
            BEGIN
                CASE x
                    WHEN 1 THEN SELECT 'one';
                    WHEN 2 THEN SELECT 'two';
                    ELSE SELECT 'other';
                END CASE;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_case(2)").await.unwrap();
    assert_table_eq!(result, [["two"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_leave() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_leave()
            BEGIN
                DECLARE i INT64 DEFAULT 0;
                my_loop: LOOP
                    SET i = i + 1;
                    IF i >= 3 THEN
                        LEAVE my_loop;
                    END IF;
                END LOOP;
                SELECT i;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_leave()").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_iterate() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_iterate()
            BEGIN
                DECLARE i INT64 DEFAULT 0;
                DECLARE sum INT64 DEFAULT 0;
                my_loop: LOOP
                    SET i = i + 1;
                    IF i > 5 THEN
                        LEAVE my_loop;
                    END IF;
                    IF MOD(i, 2) = 0 THEN
                        ITERATE my_loop;
                    END IF;
                    SET sum = sum + i;
                END LOOP;
                SELECT sum;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_iterate()").await.unwrap();
    assert_table_eq!(result, [[9]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_raise() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_raise(should_fail BOOL)
            BEGIN
                IF should_fail THEN
                    RAISE USING MESSAGE = 'Custom error message';
                END IF;
                SELECT 'success';
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL proc_raise(true)").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_transaction() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE txn_table (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_txn()
            BEGIN
                BEGIN TRANSACTION;
                INSERT INTO txn_table VALUES (1, 'first');
                INSERT INTO txn_table VALUES (2, 'second');
                COMMIT TRANSACTION;
            END",
        )
        .await
        .unwrap();

    session.execute_sql("CALL proc_txn()").await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM txn_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_rollback() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE rollback_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_rollback()
            BEGIN
                BEGIN TRANSACTION;
                INSERT INTO rollback_table VALUES (1);
                ROLLBACK TRANSACTION;
            END",
        )
        .await
        .unwrap();

    session.execute_sql("CALL proc_rollback()").await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM rollback_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_python() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE FUNCTION py_add(x INT64, y INT64)
            RETURNS INT64
            LANGUAGE python
            AS r"""
def py_add(x, y):
    return x + y
""""#,
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT py_add(3, 4)").await.unwrap();
    assert_table_eq!(result, [[7]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_python_with_dependencies() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE FUNCTION py_calc(x FLOAT64)
            RETURNS FLOAT64
            LANGUAGE python
            OPTIONS (library = ['numpy'])
            AS r"""
import numpy as np
def py_calc(x):
    return np.sqrt(x)
""""#,
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_javascript_with_library() {
    let session = create_session();

    session
        .execute_sql(
            r#"CREATE FUNCTION js_process(json_str STRING)
            RETURNS STRING
            LANGUAGE js
            OPTIONS (library = ['gs://bucket/lodash.min.js'])
            AS r"""
                return JSON.stringify(JSON.parse(json_str));
            """"#,
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_function_set_options() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION alter_fn(x INT64) RETURNS INT64 AS (x)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER FUNCTION alter_fn SET OPTIONS (description = 'Updated function')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_procedure_set_options() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE alter_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    session
        .execute_sql("ALTER PROCEDURE alter_proc SET OPTIONS (description = 'Updated procedure')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_aggregate_function_if_not_exists() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION agg_exists(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE AGGREGATE FUNCTION IF NOT EXISTS agg_exists(x INT64)
            RETURNS INT64
            AS (MAX(x))",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_table_function_if_not_exists() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION tvf_exists()
            RETURNS TABLE<x INT64>
            AS (SELECT 1 AS x)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE FUNCTION IF NOT EXISTS tvf_exists()
            RETURNS TABLE<x INT64>
            AS (SELECT 2 AS x)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM tvf_exists()")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_table_function_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP TABLE FUNCTION IF EXISTS nonexistent_tvf")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_aggregate_function_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP AGGREGATE FUNCTION IF EXISTS nonexistent_agg")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_function_with_qualified_types() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION typed_fn(x BIGNUMERIC(38, 9))
            RETURNS BIGNUMERIC(38, 9)
            AS (x * 2)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_default_parameter() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION fn_with_default(x INT64, y INT64 DEFAULT 10)
            RETURNS INT64
            AS (x + y)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT fn_with_default(5)")
        .await
        .unwrap();
    assert_table_eq!(result, [[15]]);

    let result = session
        .execute_sql("SELECT fn_with_default(5, 20)")
        .await
        .unwrap();
    assert_table_eq!(result, [[25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_with_options() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE proc_with_options()
            OPTIONS (
                strict_mode = true,
                description = 'A procedure with options'
            )
            BEGIN
                SELECT 1;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CALL proc_with_options()")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_float64_return() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION divide(a FLOAT64, b FLOAT64) RETURNS FLOAT64 AS (a / b)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT divide(10.0, 4.0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_bool_return() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION is_positive(x INT64) RETURNS BOOL AS (x > 0)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT is_positive(5)").await.unwrap();
    assert_table_eq!(result, [[true]]);

    let result = session.execute_sql("SELECT is_positive(-3)").await.unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_null_handling() {
    let session = create_session();

    session
        .execute_sql("CREATE FUNCTION null_safe(x INT64) RETURNS INT64 AS (IFNULL(x, -1))")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT null_safe(NULL)").await.unwrap();
    assert_table_eq!(result, [[-1]]);

    let result = session.execute_sql("SELECT null_safe(42)").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_with_date_return() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION next_day(d DATE) RETURNS DATE AS (DATE_ADD(d, INTERVAL 1 DAY))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT next_day(DATE '2024-01-15')")
        .await
        .unwrap();
    assert_table_eq!(result, [[date(2024, 1, 16)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_procedure_returning_multiple_columns() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE PROCEDURE get_info()
            BEGIN
                SELECT 1 AS id, 'Alice' AS name, 30 AS age;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("CALL get_info()").await.unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_function_recursive_call() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE FUNCTION double_add(x INT64, times INT64) RETURNS INT64 AS (
                CASE WHEN times <= 0 THEN x
                ELSE double_add(x + x, times - 1)
                END
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT double_add(1, 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [[8]]);
}
