use crate::assert_table_eq;
use crate::common::{create_executor, date};

#[test]
fn test_create_function_sql() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION add_one(x INT64) RETURNS INT64 AS (x + 1)")
        .unwrap();

    let result = executor.execute_sql("SELECT add_one(5)").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_create_function_string() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION greet(name STRING) RETURNS STRING AS (CONCAT('Hello, ', name))",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT greet('World')").unwrap();
    assert_table_eq!(result, [["Hello, World"]]);
}

#[test]
fn test_create_or_replace_function() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION my_func(x INT64) RETURNS INT64 AS (x * 2)")
        .unwrap();

    executor
        .execute_sql("CREATE OR REPLACE FUNCTION my_func(x INT64) RETURNS INT64 AS (x * 3)")
        .unwrap();

    let result = executor.execute_sql("SELECT my_func(10)").unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_create_function_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION existing_func(x INT64) RETURNS INT64 AS (x)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE FUNCTION IF NOT EXISTS existing_func(x INT64) RETURNS INT64 AS (x * 100)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT existing_func(5)").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_create_temp_function() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TEMP FUNCTION temp_add(a INT64, b INT64) RETURNS INT64 AS (a + b)")
        .unwrap();

    let result = executor.execute_sql("SELECT temp_add(3, 4)").unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_create_function_multiple_params() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION calc(a INT64, b INT64, c INT64) RETURNS INT64 AS (a + b * c)")
        .unwrap();

    let result = executor.execute_sql("SELECT calc(1, 2, 3)").unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_drop_function() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION to_drop(x INT64) RETURNS INT64 AS (x)")
        .unwrap();

    executor.execute_sql("DROP FUNCTION to_drop").unwrap();

    let result = executor.execute_sql("SELECT to_drop(1)");
    assert!(result.is_err());
}

#[test]
fn test_drop_function_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP FUNCTION IF EXISTS nonexistent_func");
    assert!(result.is_ok());
}

#[test]
fn test_create_function_with_case() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION classify(x INT64) RETURNS STRING AS (
                CASE
                    WHEN x < 0 THEN 'negative'
                    WHEN x = 0 THEN 'zero'
                    ELSE 'positive'
                END
            )",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT classify(-5)").unwrap();
    assert_table_eq!(result, [["negative"]]);
}

#[test]
fn test_create_function_with_coalesce() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION safe_value(x INT64) RETURNS INT64 AS (COALESCE(x, 0))")
        .unwrap();

    let result = executor.execute_sql("SELECT safe_value(NULL)").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_function_in_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3), (4), (5)")
        .unwrap();
    executor
        .execute_sql("CREATE FUNCTION is_even(x INT64) RETURNS BOOL AS (MOD(x, 2) = 0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM numbers WHERE is_even(val) ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [[2], [4]]);
}

#[test]
fn test_function_in_select() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE data (x INT64)").unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (10), (20)")
        .unwrap();
    executor
        .execute_sql("CREATE FUNCTION double_it(n INT64) RETURNS INT64 AS (n * 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x, double_it(x) AS doubled FROM data ORDER BY x")
        .unwrap();
    assert_table_eq!(result, [[10, 20], [20, 40]]);
}

#[test]
fn test_create_procedure() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE my_procedure(x INT64, y INT64)
            BEGIN
                SELECT x + y;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL my_procedure(3, 4)").unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
fn test_create_or_replace_procedure() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE proc1()
            BEGIN
                SELECT 1;
            END",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE OR REPLACE PROCEDURE proc1()
            BEGIN
                SELECT 2;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL proc1()").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_drop_procedure() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE to_drop_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .unwrap();

    executor.execute_sql("DROP PROCEDURE to_drop_proc").unwrap();

    let result = executor.execute_sql("CALL to_drop_proc()");
    assert!(result.is_err());
}

#[test]
fn test_drop_procedure_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP PROCEDURE IF EXISTS nonexistent_proc");
    assert!(result.is_ok());
}

#[test]
fn test_procedure_with_out_param() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE get_sum(IN a INT64, IN b INT64, OUT result INT64)
            BEGIN
                SET result = a + b;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL get_sum(5, 3, @out)");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_procedure_with_inout_param() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE increment(INOUT x INT64)
            BEGIN
                SET x = x + 1;
            END",
        )
        .unwrap();

    executor.execute_sql("SET @val = 10").unwrap();
    executor.execute_sql("CALL increment(@val)").unwrap();

    let result = executor.execute_sql("SELECT @val").unwrap();
    assert_table_eq!(result, [[11]]);
}

#[test]
#[ignore]
fn test_function_with_struct_return() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION make_point(x INT64, y INT64) RETURNS STRUCT<x INT64, y INT64> AS (STRUCT(x, y))",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT make_point(1, 2).x, make_point(1, 2).y")
        .unwrap();
    assert_table_eq!(result, [[1, 2]]);
}

#[test]
fn test_function_with_array_return() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION make_range(n INT64) RETURNS ARRAY<INT64> AS (GENERATE_ARRAY(1, n))",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT ARRAY_LENGTH(make_range(3))")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_function_nested_call() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION f1(x INT64) RETURNS INT64 AS (x + 1)")
        .unwrap();
    executor
        .execute_sql("CREATE FUNCTION f2(x INT64) RETURNS INT64 AS (f1(x) * 2)")
        .unwrap();

    let result = executor.execute_sql("SELECT f2(5)").unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
#[ignore]
fn test_create_aggregate_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION my_sum(x INT64)
            RETURNS INT64
            AS (
                SUM(x)
            )",
        )
        .unwrap();

    executor
        .execute_sql("CREATE TABLE agg_data (value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO agg_data VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT my_sum(value) FROM agg_data")
        .unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore]
fn test_create_aggregate_function_with_multiple_args() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION weighted_avg(value FLOAT64, weight FLOAT64)
            RETURNS FLOAT64
            AS (
                SUM(value * weight) / SUM(weight)
            )",
        )
        .unwrap();

    executor
        .execute_sql("CREATE TABLE weighted_data (val FLOAT64, wt FLOAT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO weighted_data VALUES (10.0, 1.0), (20.0, 2.0), (30.0, 3.0)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT weighted_avg(val, wt) FROM weighted_data")
        .unwrap();
    assert_table_eq!(result, [[23.333333333333332]]);
}

#[test]
#[ignore]
fn test_create_or_replace_aggregate_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION custom_count(x INT64)
            RETURNS INT64
            AS (COUNT(x))",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE OR REPLACE AGGREGATE FUNCTION custom_count(x INT64)
            RETURNS INT64
            AS (COUNT(DISTINCT x))",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_aggregate_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION to_drop_agg(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .unwrap();

    executor
        .execute_sql("DROP AGGREGATE FUNCTION to_drop_agg")
        .unwrap();

    let result = executor.execute_sql("SELECT to_drop_agg(1)");
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_create_table_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION my_table_func(x INT64)
            RETURNS TABLE<id INT64, value INT64>
            AS (
                SELECT id, value * x AS value
                FROM UNNEST([STRUCT(1 AS id, 10 AS value), STRUCT(2, 20)])
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM my_table_func(2) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 20], [2, 40]]);
}

#[test]
#[ignore]
fn test_create_table_function_any_type() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION names_by_year(y INT64)
            RETURNS TABLE<name STRING, year INT64>
            AS (
                SELECT name, year FROM UNNEST([STRUCT('Alice' AS name, 2020 AS year), STRUCT('Bob', 2021)])
                WHERE year = y
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM names_by_year(2020)")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
#[ignore]
fn test_create_or_replace_table_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION tv_func()
            RETURNS TABLE<x INT64>
            AS (SELECT 1 AS x)",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE OR REPLACE TABLE FUNCTION tv_func()
            RETURNS TABLE<x INT64>
            AS (SELECT 2 AS x)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT x FROM tv_func()").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore]
fn test_drop_table_function() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION to_drop_tvf()
            RETURNS TABLE<x INT64>
            AS (SELECT 1 AS x)",
        )
        .unwrap();

    executor
        .execute_sql("DROP TABLE FUNCTION to_drop_tvf")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM to_drop_tvf()");
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_create_function_with_options() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION fn_with_opts(x INT64)
            RETURNS INT64
            OPTIONS (description = 'Adds one to input')
            AS (x + 1)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT fn_with_opts(5)").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
#[ignore]
fn test_create_function_javascript() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"CREATE FUNCTION js_multiply(x FLOAT64, y FLOAT64)
            RETURNS FLOAT64
            LANGUAGE js
            AS r"""
                return x * y;
            """"#,
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT js_multiply(3.0, 4.0)")
        .unwrap();
    assert_table_eq!(result, [[12.0]]);
}

#[test]
#[ignore]
fn test_create_function_remote() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION remote_fn(x INT64)
            RETURNS INT64
            REMOTE WITH CONNECTION `project.region.connection`
            OPTIONS (
                endpoint = 'https://example.com/function'
            )",
        )
        .unwrap();
}

#[test]
fn test_procedure_with_declare() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE proc_with_declare(x INT64)
            BEGIN
                DECLARE y INT64;
                SET y = x * 2;
                SELECT y;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL proc_with_declare(5)").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
#[ignore]
fn test_procedure_with_if() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_with_if(5)").unwrap();
    assert_table_eq!(result, [["positive"]]);
}

#[test]
fn test_procedure_with_loop() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_with_loop()").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
#[ignore]
fn test_procedure_with_exception_handling() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_with_exception()").unwrap();
    assert_table_eq!(result, [["caught"]]);
}

#[test]
#[ignore]
fn test_procedure_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE existing_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE PROCEDURE IF NOT EXISTS existing_proc()
            BEGIN
                SELECT 2;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL existing_proc()").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_create_function_deterministic() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION det_func(x INT64)
            RETURNS INT64
            DETERMINISTIC
            AS (x * 2)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT det_func(5)").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
#[ignore]
fn test_create_function_not_deterministic() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION nondet_func()
            RETURNS INT64
            NOT DETERMINISTIC
            AS (CAST(RAND() * 100 AS INT64))",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT nondet_func()");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_create_function_with_security_definer() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION secure_func(x INT64)
            RETURNS INT64
            SQL SECURITY DEFINER
            AS (x + 1)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT secure_func(10)").unwrap();
    assert_table_eq!(result, [[11]]);
}

#[test]
#[ignore]
fn test_create_function_with_security_invoker() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION invoker_func(x INT64)
            RETURNS INT64
            SQL SECURITY INVOKER
            AS (x + 1)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT invoker_func(10)").unwrap();
    assert_table_eq!(result, [[11]]);
}

#[test]
#[ignore]
fn test_create_function_with_data_governance() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION data_gov_func(x INT64)
            RETURNS INT64
            OPTIONS (data_governance_type = 'DATA_MASKING')
            AS (x)",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_aggregate_function_with_over() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION my_count(x INT64)
            RETURNS INT64
            AS (COUNT(x))",
        )
        .unwrap();

    executor
        .execute_sql("CREATE TABLE count_data (val INT64, category STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO count_data VALUES (1, 'A'), (2, 'A'), (3, 'B')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, my_count(val) FROM count_data GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 2], ["B", 1]]);
}

#[test]
#[ignore]
fn test_create_aggregate_function_with_filter() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION filtered_sum(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .unwrap();

    executor
        .execute_sql("CREATE TABLE filter_data (val INT64, active BOOL)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO filter_data VALUES (10, true), (20, false), (30, true)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT filtered_sum(val) FROM filter_data WHERE active")
        .unwrap();
    assert_table_eq!(result, [[40]]);
}

#[test]
#[ignore]
fn test_create_table_function_with_multiple_params() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION multi_param_tvf(start_val INT64, end_val INT64, step INT64)
            RETURNS TABLE<num INT64>
            AS (
                SELECT num FROM UNNEST(GENERATE_ARRAY(start_val, end_val, step)) AS num
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM multi_param_tvf(0, 10, 2)")
        .unwrap();
    assert_table_eq!(result, [[0], [2], [4], [6], [8], [10]]);
}

#[test]
#[ignore]
fn test_create_table_function_with_struct_output() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION struct_tvf(prefix STRING)
            RETURNS TABLE<person STRUCT<name STRING, age INT64>>
            AS (
                SELECT STRUCT(CONCAT(prefix, name) AS name, age) AS person
                FROM UNNEST([STRUCT('Alice' AS name, 30 AS age), STRUCT('Bob', 25)])
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT person.name FROM struct_tvf('Mr. ') ORDER BY person.name")
        .unwrap();
    assert_table_eq!(result, [["Mr. Alice"], ["Mr. Bob"]]);
}

#[test]
#[ignore]
fn test_create_function_with_any_type() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION identity_fn(x ANY TYPE)
            AS (x)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT identity_fn(42)").unwrap();
    assert_table_eq!(result, [[42]]);

    let result = executor.execute_sql("SELECT identity_fn('hello')").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore]
fn test_create_function_with_lambda() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION apply_twice(x INT64, fn ANY TYPE)
            AS (fn(fn(x)))",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT apply_twice(5, (x) -> x + 1)")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
#[ignore]
fn test_create_function_in_schema() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE SCHEMA fn_schema").unwrap();

    executor
        .execute_sql(
            "CREATE FUNCTION fn_schema.qualified_func(x INT64)
            RETURNS INT64
            AS (x * 10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT fn_schema.qualified_func(5)")
        .unwrap();
    assert_table_eq!(result, [[50]]);
}

#[test]
#[ignore]
fn test_drop_function_with_signature() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION overloaded(x INT64) RETURNS INT64 AS (x)")
        .unwrap();

    executor
        .execute_sql("DROP FUNCTION overloaded(INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT overloaded(1)");
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_create_function_returns_table() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION get_users()
            RETURNS TABLE<id INT64, name STRING>
            AS (
                SELECT * FROM UNNEST([STRUCT(1 AS id, 'Alice' AS name), STRUCT(2, 'Bob')])
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM get_users() ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"]]);
}

#[test]
#[ignore]
fn test_procedure_with_for_loop() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_for_loop()").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore]
fn test_procedure_with_repeat() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_repeat()").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore]
fn test_procedure_with_case_statement() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_case(2)").unwrap();
    assert_table_eq!(result, [["two"]]);
}

#[test]
#[ignore]
fn test_procedure_with_leave() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_leave()").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore]
fn test_procedure_with_iterate() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_iterate()").unwrap();
    assert_table_eq!(result, [[9]]);
}

#[test]
#[ignore]
fn test_procedure_with_raise() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE proc_raise(should_fail BOOL)
            BEGIN
                IF should_fail THEN
                    RAISE USING MESSAGE = 'Custom error message';
                END IF;
                SELECT 'success';
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL proc_raise(true)");
    assert!(result.is_err());
}

#[test]
#[ignore]
fn test_procedure_with_transaction() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE txn_table (id INT64, value STRING)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE PROCEDURE proc_txn()
            BEGIN
                BEGIN TRANSACTION;
                INSERT INTO txn_table VALUES (1, 'first');
                INSERT INTO txn_table VALUES (2, 'second');
                COMMIT TRANSACTION;
            END",
        )
        .unwrap();

    executor.execute_sql("CALL proc_txn()").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM txn_table")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore]
fn test_procedure_with_rollback() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE rollback_table (id INT64)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE PROCEDURE proc_rollback()
            BEGIN
                BEGIN TRANSACTION;
                INSERT INTO rollback_table VALUES (1);
                ROLLBACK TRANSACTION;
            END",
        )
        .unwrap();

    executor.execute_sql("CALL proc_rollback()").unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM rollback_table")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore]
fn test_create_function_python() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"CREATE FUNCTION py_add(x INT64, y INT64)
            RETURNS INT64
            LANGUAGE python
            AS r"""
def py_add(x, y):
    return x + y
""""#,
        )
        .unwrap();

    let result = executor.execute_sql("SELECT py_add(3, 4)").unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
#[ignore]
fn test_create_function_python_with_dependencies() {
    let mut executor = create_executor();

    executor
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
        .unwrap();
}

#[test]
#[ignore]
fn test_create_function_javascript_with_library() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            r#"CREATE FUNCTION js_process(json_str STRING)
            RETURNS STRING
            LANGUAGE js
            OPTIONS (library = ['gs://bucket/lodash.min.js'])
            AS r"""
                return JSON.stringify(JSON.parse(json_str));
            """"#,
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_function_set_options() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION alter_fn(x INT64) RETURNS INT64 AS (x)")
        .unwrap();

    executor
        .execute_sql("ALTER FUNCTION alter_fn SET OPTIONS (description = 'Updated function')")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_procedure_set_options() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE alter_proc()
            BEGIN
                SELECT 1;
            END",
        )
        .unwrap();

    executor
        .execute_sql("ALTER PROCEDURE alter_proc SET OPTIONS (description = 'Updated procedure')")
        .unwrap();
}

#[test]
#[ignore]
fn test_create_aggregate_function_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION agg_exists(x INT64)
            RETURNS INT64
            AS (SUM(x))",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE AGGREGATE FUNCTION IF NOT EXISTS agg_exists(x INT64)
            RETURNS INT64
            AS (MAX(x))",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_table_function_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION tvf_exists()
            RETURNS TABLE<x INT64>
            AS (SELECT 1 AS x)",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE FUNCTION IF NOT EXISTS tvf_exists()
            RETURNS TABLE<x INT64>
            AS (SELECT 2 AS x)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM tvf_exists()").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_drop_table_function_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP TABLE FUNCTION IF EXISTS nonexistent_tvf");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_drop_aggregate_function_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP AGGREGATE FUNCTION IF EXISTS nonexistent_agg");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_create_function_with_qualified_types() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION typed_fn(x BIGNUMERIC(38, 9))
            RETURNS BIGNUMERIC(38, 9)
            AS (x * 2)",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_function_with_default_parameter() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION fn_with_default(x INT64, y INT64 DEFAULT 10)
            RETURNS INT64
            AS (x + y)",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT fn_with_default(5)").unwrap();
    assert_table_eq!(result, [[15]]);

    let result = executor
        .execute_sql("SELECT fn_with_default(5, 20)")
        .unwrap();
    assert_table_eq!(result, [[25]]);
}

#[test]
#[ignore]
fn test_procedure_with_options() {
    let mut executor = create_executor();

    executor
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
        .unwrap();

    let result = executor.execute_sql("CALL proc_with_options()").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_function_with_float64_return() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION divide(a FLOAT64, b FLOAT64) RETURNS FLOAT64 AS (a / b)")
        .unwrap();

    let result = executor.execute_sql("SELECT divide(10.0, 4.0)").unwrap();
    assert_table_eq!(result, [[2.5]]);
}

#[test]
fn test_function_with_bool_return() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION is_positive(x INT64) RETURNS BOOL AS (x > 0)")
        .unwrap();

    let result = executor.execute_sql("SELECT is_positive(5)").unwrap();
    assert_table_eq!(result, [[true]]);

    let result = executor.execute_sql("SELECT is_positive(-3)").unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_function_with_null_handling() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE FUNCTION null_safe(x INT64) RETURNS INT64 AS (IFNULL(x, -1))")
        .unwrap();

    let result = executor.execute_sql("SELECT null_safe(NULL)").unwrap();
    assert_table_eq!(result, [[-1]]);

    let result = executor.execute_sql("SELECT null_safe(42)").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_function_with_date_return() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION next_day(d DATE) RETURNS DATE AS (DATE_ADD(d, INTERVAL 1 DAY))",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT next_day(DATE '2024-01-15')")
        .unwrap();
    assert_table_eq!(result, [[date(2024, 1, 16)]]);
}

#[test]
fn test_procedure_returning_multiple_columns() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE PROCEDURE get_info()
            BEGIN
                SELECT 1 AS id, 'Alice' AS name, 30 AS age;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("CALL get_info()").unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[test]
fn test_function_recursive_call() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE FUNCTION double_add(x INT64, times INT64) RETURNS INT64 AS (
                CASE WHEN times <= 0 THEN x
                ELSE double_add(x + x, times - 1)
                END
            )",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT double_add(1, 3)").unwrap();
    assert_table_eq!(result, [[8]]);
}
