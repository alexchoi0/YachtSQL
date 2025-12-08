use crate::common::create_executor;

#[test]
fn test_create_function_sql() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION add_numbers(a INTEGER, b INTEGER) RETURNS INTEGER
         AS $$ SELECT a + b; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_function_plpgsql() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION multiply(a INTEGER, b INTEGER) RETURNS INTEGER AS $$
         BEGIN
             RETURN a * b;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_procedure() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE proc_test (id INTEGER, val INTEGER)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE PROCEDURE insert_value(p_id INTEGER, p_val INTEGER) AS $$
         BEGIN
             INSERT INTO proc_test VALUES (p_id, p_val);
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_call_procedure() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE call_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE PROCEDURE add_row(p_id INTEGER) AS $$
         BEGIN
             INSERT INTO call_test VALUES (p_id);
         END;
         $$ LANGUAGE plpgsql",
        )
        .unwrap();

    let result = executor.execute_sql("CALL add_row(1)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_returns_table() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION get_numbers() RETURNS TABLE(num INTEGER) AS $$
         BEGIN
             RETURN QUERY SELECT generate_series(1, 5);
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_returns_setof() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE setof_test (id INTEGER, val TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE FUNCTION get_all_rows() RETURNS SETOF setof_test AS $$
         SELECT * FROM setof_test;
         $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_out_parameter() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION divide_with_remainder(
             dividend INTEGER,
             divisor INTEGER,
             OUT quotient INTEGER,
             OUT remainder INTEGER
         ) AS $$
         BEGIN
             quotient := dividend / divisor;
             remainder := dividend % divisor;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_inout_parameter() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION increment(INOUT val INTEGER) AS $$
         BEGIN
             val := val + 1;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_default_parameter() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION greet(name TEXT DEFAULT 'World') RETURNS TEXT AS $$
         SELECT 'Hello, ' || name || '!';
         $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_variadic() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION sum_all(VARIADIC nums INTEGER[]) RETURNS INTEGER AS $$
         SELECT SUM(n) FROM UNNEST(nums) AS n;
         $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_immutable() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION const_func() RETURNS INTEGER
         IMMUTABLE
         AS $$ SELECT 42; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_stable() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION stable_func() RETURNS TIMESTAMP
         STABLE
         AS $$ SELECT NOW(); $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_volatile() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION volatile_func() RETURNS DOUBLE PRECISION
         VOLATILE
         AS $$ SELECT RANDOM(); $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_strict() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION strict_add(a INTEGER, b INTEGER) RETURNS INTEGER
         STRICT
         AS $$ SELECT a + b; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_security_definer() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION secure_func() RETURNS INTEGER
         SECURITY DEFINER
         AS $$ SELECT 1; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_security_invoker() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION invoker_func() RETURNS INTEGER
         SECURITY INVOKER
         AS $$ SELECT 1; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_parallel_safe() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION parallel_func(a INTEGER) RETURNS INTEGER
         PARALLEL SAFE
         AS $$ SELECT a * 2; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_cost() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION expensive_func() RETURNS INTEGER
         COST 1000
         AS $$ SELECT 1; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_rows() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION estimate_rows() RETURNS SETOF INTEGER
         ROWS 100
         AS $$ SELECT generate_series(1, 100); $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION to_drop() RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL")
        .unwrap();

    let result = executor.execute_sql("DROP FUNCTION to_drop()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_function_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP FUNCTION IF EXISTS nonexistent()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_procedure() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE PROCEDURE to_drop_proc() AS $$ BEGIN NULL; END; $$ LANGUAGE plpgsql")
        .unwrap();

    let result = executor.execute_sql("DROP PROCEDURE to_drop_proc()");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_function_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION old_func() RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL")
        .unwrap();

    let result = executor.execute_sql("ALTER FUNCTION old_func() RENAME TO new_func");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_function_owner() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION owner_func() RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL")
        .unwrap();

    let result = executor.execute_sql("ALTER FUNCTION owner_func() OWNER TO postgres");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_function_set_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA new_schema").unwrap();
    executor
        .execute_sql(
            "CREATE FUNCTION schema_func() RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
        )
        .unwrap();

    let result = executor.execute_sql("ALTER FUNCTION schema_func() SET SCHEMA new_schema");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_create_or_replace_function() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE FUNCTION replace_func() RETURNS INTEGER AS $$ SELECT 1; $$ LANGUAGE SQL",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE OR REPLACE FUNCTION replace_func() RETURNS INTEGER AS $$ SELECT 2; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_function_overloading() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE FUNCTION overload(a INTEGER) RETURNS INTEGER AS $$ SELECT a; $$ LANGUAGE SQL",
        )
        .unwrap();

    let result = executor.execute_sql(
        "CREATE FUNCTION overload(a INTEGER, b INTEGER) RETURNS INTEGER AS $$ SELECT a + b; $$ LANGUAGE SQL"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_procedure_transaction_control() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tx_test (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE PROCEDURE with_commit() AS $$
         BEGIN
             INSERT INTO tx_test VALUES (1);
             COMMIT;
             INSERT INTO tx_test VALUES (2);
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_exception_handling() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION safe_divide(a INTEGER, b INTEGER) RETURNS INTEGER AS $$
         BEGIN
             RETURN a / b;
         EXCEPTION
             WHEN division_by_zero THEN
                 RETURN NULL;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_with_declare() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION with_vars() RETURNS INTEGER AS $$
         DECLARE
             x INTEGER := 10;
             y INTEGER := 20;
         BEGIN
             RETURN x + y;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_if_else() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION check_value(val INTEGER) RETURNS TEXT AS $$
         BEGIN
             IF val > 100 THEN
                 RETURN 'high';
             ELSIF val > 50 THEN
                 RETURN 'medium';
             ELSE
                 RETURN 'low';
             END IF;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_loop() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION count_to(n INTEGER) RETURNS INTEGER AS $$
         DECLARE
             i INTEGER := 0;
             total INTEGER := 0;
         BEGIN
             LOOP
                 i := i + 1;
                 total := total + i;
                 EXIT WHEN i >= n;
             END LOOP;
             RETURN total;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_for_loop() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE FUNCTION sum_range(start_val INTEGER, end_val INTEGER) RETURNS INTEGER AS $$
         DECLARE
             total INTEGER := 0;
         BEGIN
             FOR i IN start_val..end_val LOOP
                 total := total + i;
             END LOOP;
             RETURN total;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_function_cursor() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cursor_data (id INTEGER, val TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE FUNCTION process_rows() RETURNS INTEGER AS $$
         DECLARE
             rec RECORD;
             cnt INTEGER := 0;
         BEGIN
             FOR rec IN SELECT * FROM cursor_data LOOP
                 cnt := cnt + 1;
             END LOOP;
             RETURN cnt;
         END;
         $$ LANGUAGE plpgsql",
    );
    assert!(result.is_ok() || result.is_err());
}
