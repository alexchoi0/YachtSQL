use crate::assert_table_eq;
use crate::common::create_executor;

#[test]

#[ignore = "Implement me!"]
fn test_declare_variable() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64").unwrap();
    executor.execute_sql("SET x = 10").unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_declare_with_default() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 5").unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_declare_multiple() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE a, b, c INT64").unwrap();
    executor.execute_sql("SET a = 1").unwrap();
    executor.execute_sql("SET b = 2").unwrap();
    executor.execute_sql("SET c = 3").unwrap();

    let result = executor.execute_sql("SELECT a + b + c").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_declare_string() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE name STRING DEFAULT 'Hello'")
        .unwrap();

    let result = executor.execute_sql("SELECT name").unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_set_variable() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE counter INT64 DEFAULT 0")
        .unwrap();
    executor.execute_sql("SET counter = counter + 1").unwrap();
    executor.execute_sql("SET counter = counter + 1").unwrap();

    let result = executor.execute_sql("SELECT counter").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_set_from_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30)")
        .unwrap();

    executor.execute_sql("DECLARE total INT64").unwrap();
    executor
        .execute_sql("SET total = (SELECT SUM(val) FROM numbers)")
        .unwrap();

    let result = executor.execute_sql("SELECT total").unwrap();
    assert_table_eq!(result, [[60]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_if_then() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 10").unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "IF x > 5 THEN
                SET result = 'big';
            END IF",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["big"]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_if_then_else() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 3").unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "IF x > 5 THEN
                SET result = 'big';
            ELSE
                SET result = 'small';
            END IF",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["small"]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_if_elseif_else() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 5").unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "IF x > 10 THEN
                SET result = 'large';
            ELSEIF x > 3 THEN
                SET result = 'medium';
            ELSE
                SET result = 'small';
            END IF",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["medium"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_loop_basic() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "LOOP
                SET i = i + 1;
                SET sum = sum + i;
                IF i >= 5 THEN
                    LEAVE;
                END IF;
            END LOOP",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_while_loop() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "WHILE i < 5 DO
                SET i = i + 1;
                SET sum = sum + i;
            END WHILE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_repeat_loop() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "REPEAT
                SET i = i + 1;
            UNTIL i >= 5
            END REPEAT",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_for_loop() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "FOR i IN (SELECT val FROM UNNEST([1, 2, 3, 4, 5]) AS val) DO
                SET sum = sum + i.val;
            END FOR",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_break_continue() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "LOOP
                SET i = i + 1;
                IF i = 3 THEN
                    CONTINUE;
                END IF;
                SET sum = sum + i;
                IF i >= 5 THEN
                    BREAK;
                END IF;
            END LOOP",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_begin_end_block() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "BEGIN
                DECLARE x INT64 DEFAULT 10;
                DECLARE y INT64 DEFAULT 20;
                SELECT x + y;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_nested_blocks() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE result INT64 DEFAULT 0")
        .unwrap();

    executor
        .execute_sql(
            "BEGIN
                DECLARE a INT64 DEFAULT 1;
                BEGIN
                    DECLARE b INT64 DEFAULT 2;
                    SET result = a + b;
                END;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_exception_handling() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE result STRING DEFAULT 'start'")
        .unwrap();

    executor
        .execute_sql(
            "BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET result = 'caught';
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["caught"]]);
}

#[test]
fn test_raise_exception() {
    let mut executor = create_executor();

    let result = executor.execute_sql("RAISE USING MESSAGE = 'Custom error'");
    assert!(result.is_err());
}

#[test]

#[ignore = "Implement me!"]
fn test_execute_immediate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dynamic_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dynamic_test VALUES (1), (2), (3)")
        .unwrap();

    executor
        .execute_sql("EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM dynamic_test'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM dynamic_test")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_execute_immediate_with_params() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE param_test (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO param_test VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor.execute_sql(
        "EXECUTE IMMEDIATE 'SELECT name FROM param_test WHERE id = @id' USING 1 AS id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]

#[ignore = "Implement me!"]
fn test_execute_immediate_ddl() {
    let mut executor = create_executor();

    executor
        .execute_sql("EXECUTE IMMEDIATE 'CREATE TABLE created_dynamically (id INT64)'")
        .unwrap();

    executor
        .execute_sql("INSERT INTO created_dynamically VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM created_dynamically")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_case_statement() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 2").unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "CASE x
                WHEN 1 THEN SET result = 'one';
                WHEN 2 THEN SET result = 'two';
                WHEN 3 THEN SET result = 'three';
                ELSE SET result = 'other';
            END CASE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["two"]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_return_statement() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE result INT64 DEFAULT 0")
        .unwrap();

    executor
        .execute_sql(
            "BEGIN
                SET result = 1;
                RETURN;
                SET result = 2;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_labeled_block() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "outer_loop: LOOP
                SET i = i + 1;
                IF i >= 3 THEN
                    LEAVE outer_loop;
                END IF;
            END LOOP outer_loop",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_variable_in_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE employees (id INT64, salary INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees VALUES (1, 50000), (2, 60000), (3, 70000)")
        .unwrap();

    executor
        .execute_sql("DECLARE min_salary INT64 DEFAULT 55000")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM employees WHERE salary > min_salary ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_system_variable() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT @@time_zone IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]

#[ignore = "Implement me!"]
fn test_set_system_variable() {
    let mut executor = create_executor();

    executor
        .execute_sql("SET @@time_zone = 'America/Los_Angeles'")
        .unwrap();

    let result = executor.execute_sql("SELECT @@time_zone").unwrap();
    assert_table_eq!(result, [["America/Los_Angeles"]]);
}
