use crate::assert_table_eq;
use crate::common::{create_executor, date};

#[test]
fn test_declare_variable() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64").unwrap();
    executor.execute_sql("SET x = 10").unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_declare_with_default() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 5").unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
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
fn test_declare_string() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE name STRING DEFAULT 'Hello'")
        .unwrap();

    let result = executor.execute_sql("SELECT name").unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[test]
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
#[ignore]
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
fn test_set_system_variable() {
    let mut executor = create_executor();

    executor
        .execute_sql("SET @@time_zone = 'America/Los_Angeles'")
        .unwrap();

    let result = executor.execute_sql("SELECT @@time_zone").unwrap();
    assert_table_eq!(result, [["America/Los_Angeles"]]);
}

#[test]
fn test_declare_type_inference() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x DEFAULT 42").unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_declare_type_inference_string() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE greeting DEFAULT 'Hello World'")
        .unwrap();

    let result = executor.execute_sql("SELECT greeting").unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[test]
fn test_declare_type_inference_from_expression() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE computed DEFAULT 10 + 5 * 2")
        .unwrap();

    let result = executor.execute_sql("SELECT computed").unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_declare_date_default() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE d DATE DEFAULT DATE '2024-01-15'")
        .unwrap();

    let result = executor.execute_sql("SELECT d").unwrap();
    assert_table_eq!(result, [[date(2024, 1, 15)]]);
}

#[test]
fn test_declare_float64() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE pi FLOAT64 DEFAULT 3.14159")
        .unwrap();

    let result = executor.execute_sql("SELECT pi").unwrap();
    assert_table_eq!(result, [[3.14159]]);
}

#[test]
fn test_declare_bool() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE flag BOOL DEFAULT TRUE")
        .unwrap();

    let result = executor.execute_sql("SELECT flag").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_declare_array() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE arr ARRAY<INT64> DEFAULT [1, 2, 3]")
        .unwrap();

    let result = executor.execute_sql("SELECT arr").unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
#[ignore]
fn test_declare_multiple_with_default() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE x, y, z INT64 DEFAULT 0")
        .unwrap();

    let result = executor.execute_sql("SELECT x, y, z").unwrap();
    assert_table_eq!(result, [[0, 0, 0]]);
}

#[test]
#[ignore]
fn test_set_multiple_variables() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE a INT64").unwrap();
    executor.execute_sql("DECLARE b STRING").unwrap();
    executor.execute_sql("DECLARE c BOOL").unwrap();

    executor
        .execute_sql("SET (a, b, c) = (1 + 3, 'foo', FALSE)")
        .unwrap();

    let result = executor.execute_sql("SELECT a, b, c").unwrap();
    assert_table_eq!(result, [[4, "foo", false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_from_struct_query() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE count1 INT64").unwrap();
    executor.execute_sql("DECLARE count2 INT64").unwrap();

    executor
        .execute_sql("SET (count1, count2) = (SELECT AS STRUCT 10, 20)")
        .unwrap();

    let result = executor.execute_sql("SELECT count1, count2").unwrap();
    assert_table_eq!(result, [[10, 20]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_execute_immediate_into() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE books (title STRING, year INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO books VALUES ('Hamlet', 1599), ('Ulysses', 1922)")
        .unwrap();

    executor.execute_sql("DECLARE min_year INT64").unwrap();

    executor
        .execute_sql("EXECUTE IMMEDIATE 'SELECT MIN(year) FROM books' INTO min_year")
        .unwrap();

    let result = executor.execute_sql("SELECT min_year").unwrap();
    assert_table_eq!(result, [[1599]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_execute_immediate_using_positional() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE y INT64").unwrap();

    executor
        .execute_sql("EXECUTE IMMEDIATE 'SELECT ? * (? + 2)' INTO y USING 1, 3")
        .unwrap();

    let result = executor.execute_sql("SELECT y").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_execute_immediate_using_named() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE y INT64").unwrap();

    executor
        .execute_sql("EXECUTE IMMEDIATE 'SELECT @a * (@b + 2)' INTO y USING 1 AS a, 3 AS b")
        .unwrap();

    let result = executor.execute_sql("SELECT y").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_execute_immediate_dml() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE items (name STRING)")
        .unwrap();

    executor
        .execute_sql("DECLARE item_name STRING DEFAULT 'Widget'")
        .unwrap();

    executor
        .execute_sql("EXECUTE IMMEDIATE 'INSERT INTO items (name) VALUES(?)' USING item_name")
        .unwrap();

    let result = executor.execute_sql("SELECT name FROM items").unwrap();
    assert_table_eq!(result, [["Widget"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_exception_error_message() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE error_msg STRING").unwrap();

    executor
        .execute_sql(
            "BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET error_msg = @@error.message;
            END",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT error_msg IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_exception_statement_text() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE stmt_text STRING").unwrap();

    executor
        .execute_sql(
            "BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET stmt_text = @@error.statement_text;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT stmt_text").unwrap();
    assert_table_eq!(result, [["SELECT 1 / 0"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_exception_reraise() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE caught BOOL DEFAULT FALSE")
        .unwrap();

    let result = executor.execute_sql(
        "BEGIN
            BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET caught = TRUE;
                RAISE;
            END;
        EXCEPTION WHEN ERROR THEN
            -- Outer handler catches the re-raised exception
        END",
    );

    assert!(result.is_ok());
    let result = executor.execute_sql("SELECT caught").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_case_conditional() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 15").unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "CASE
                WHEN x > 20 THEN SET result = 'large';
                WHEN x > 10 THEN SET result = 'medium';
                ELSE SET result = 'small';
            END CASE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["medium"]]);
}

#[test]
fn test_case_conditional_no_match() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 5").unwrap();
    executor
        .execute_sql("DECLARE result STRING DEFAULT 'default'")
        .unwrap();

    executor
        .execute_sql(
            "CASE
                WHEN x > 20 THEN SET result = 'large';
                WHEN x > 10 THEN SET result = 'medium';
            END CASE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_case_search_expression_else() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE product_id INT64 DEFAULT 99")
        .unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "CASE product_id
                WHEN 1 THEN SET result = 'Product one';
                WHEN 2 THEN SET result = 'Product two';
                ELSE SET result = 'Invalid product';
            END CASE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["Invalid product"]]);
}

#[test]
fn test_while_with_break() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "WHILE TRUE DO
                SET i = i + 1;
                IF i >= 5 THEN
                    BREAK;
                END IF;
            END WHILE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_while_never_executes() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 100").unwrap();

    executor
        .execute_sql(
            "WHILE i < 10 DO
                SET i = i + 1;
            END WHILE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i").unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_repeat_basic() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "REPEAT
                SET i = i + 1;
                SET sum = sum + i;
            UNTIL i >= 5
            END REPEAT",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_repeat_executes_at_least_once() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE executed BOOL DEFAULT FALSE")
        .unwrap();

    executor
        .execute_sql(
            "REPEAT
                SET executed = TRUE;
            UNTIL TRUE
            END REPEAT",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT executed").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_for_in_basic() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE total INT64 DEFAULT 0")
        .unwrap();

    executor
        .execute_sql(
            "FOR record IN (SELECT n FROM UNNEST([1, 2, 3, 4, 5]) AS n)
            DO
                SET total = total + record.n;
            END FOR",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT total").unwrap();
    assert_table_eq!(result, [[15]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_for_in_with_table() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30)")
        .unwrap();

    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "FOR row IN (SELECT val FROM numbers ORDER BY val)
            DO
                SET sum = sum + row.val;
            END FOR",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[60]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_for_in_with_break() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE last_val INT64 DEFAULT 0")
        .unwrap();

    executor
        .execute_sql(
            "FOR record IN (SELECT n FROM UNNEST([1, 2, 3, 4, 5]) AS n)
            DO
                SET last_val = record.n;
                IF record.n >= 3 THEN
                    BREAK;
                END IF;
            END FOR",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT last_val").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_labeled_loop_leave() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE j INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "outer_loop: LOOP
                SET i = i + 1;
                SET j = 0;
                WHILE j < 3 DO
                    SET j = j + 1;
                    IF i = 2 AND j = 2 THEN
                        LEAVE outer_loop;
                    END IF;
                END WHILE;
            END LOOP outer_loop",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i, j").unwrap();
    assert_table_eq!(result, [[2, 2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_labeled_loop_continue() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor
        .execute_sql("DECLARE skipped INT64 DEFAULT 0")
        .unwrap();

    executor
        .execute_sql(
            "outer_loop: WHILE i < 5 DO
                SET i = i + 1;
                IF i = 3 THEN
                    SET skipped = skipped + 1;
                    CONTINUE outer_loop;
                END IF;
            END WHILE outer_loop",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i, skipped").unwrap();
    assert_table_eq!(result, [[5, 1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_labeled_begin_leave() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE result STRING DEFAULT 'start'")
        .unwrap();

    executor
        .execute_sql(
            "my_block: BEGIN
                SET result = 'in block';
                LEAVE my_block;
                SET result = 'after leave';
            END my_block",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["in block"]]);
}

#[test]
fn test_iterate_synonym() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE sum INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "WHILE i < 5 DO
                SET i = i + 1;
                IF i = 3 THEN
                    ITERATE;
                END IF;
                SET sum = sum + i;
            END WHILE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT sum").unwrap();
    assert_table_eq!(result, [[12]]);
}

#[test]
fn test_leave_synonym() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "LOOP
                SET i = i + 1;
                IF i >= 3 THEN
                    LEAVE;
                END IF;
            END LOOP",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i").unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_begin_transaction_commit() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE accounts (id INT64, balance INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO accounts VALUES (1, 100), (2, 200)")
        .unwrap();

    executor.execute_sql("BEGIN TRANSACTION").unwrap();
    executor
        .execute_sql("UPDATE accounts SET balance = balance - 50 WHERE id = 1")
        .unwrap();
    executor
        .execute_sql("UPDATE accounts SET balance = balance + 50 WHERE id = 2")
        .unwrap();
    executor.execute_sql("COMMIT TRANSACTION").unwrap();

    let result = executor
        .execute_sql("SELECT id, balance FROM accounts ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 50], [2, 250]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_begin_transaction_rollback() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE items (name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('existing')")
        .unwrap();

    executor.execute_sql("BEGIN TRANSACTION").unwrap();
    executor
        .execute_sql("INSERT INTO items VALUES ('new item')")
        .unwrap();
    executor.execute_sql("ROLLBACK TRANSACTION").unwrap();

    let result = executor.execute_sql("SELECT name FROM items").unwrap();
    assert_table_eq!(result, [["existing"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_transaction_with_exception_rollback() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE data (id INT64)")
        .unwrap();
    executor.execute_sql("INSERT INTO data VALUES (1)").unwrap();

    executor
        .execute_sql(
            "BEGIN
                BEGIN TRANSACTION;
                INSERT INTO data VALUES (2);
                SELECT 1 / 0;  -- Error
                COMMIT TRANSACTION;
            EXCEPTION WHEN ERROR THEN
                ROLLBACK TRANSACTION;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM data").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_raise_with_message() {
    let mut executor = create_executor();

    let result = executor.execute_sql("RAISE USING MESSAGE = 'Something went wrong'");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Something went wrong"));
}

#[test]
#[ignore = "Implement me!"]
fn test_raise_and_catch() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE caught_msg STRING").unwrap();

    executor
        .execute_sql(
            "BEGIN
                RAISE USING MESSAGE = 'Custom error';
            EXCEPTION WHEN ERROR THEN
                SET caught_msg = @@error.message;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT caught_msg").unwrap();
    assert_table_eq!(result, [["Custom error"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_return_in_block() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "BEGIN
                SET x = 1;
                RETURN;
                SET x = 2;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_return_in_loop() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "WHILE TRUE DO
                SET i = i + 1;
                IF i = 5 THEN
                    RETURN;
                END IF;
            END WHILE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore]
fn test_variable_scope_in_block() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE outer_var INT64 DEFAULT 10")
        .unwrap();

    executor
        .execute_sql(
            "BEGIN
                DECLARE inner_var INT64 DEFAULT 20;
                SET outer_var = outer_var + inner_var;
            END",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT outer_var").unwrap();
    assert_table_eq!(result, [[30]]);
}

#[test]
fn test_nested_if() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE a INT64 DEFAULT 5").unwrap();
    executor.execute_sql("DECLARE b INT64 DEFAULT 10").unwrap();
    executor.execute_sql("DECLARE result STRING").unwrap();

    executor
        .execute_sql(
            "IF a > 0 THEN
                IF b > a THEN
                    SET result = 'b greater';
                ELSE
                    SET result = 'a greater or equal';
                END IF;
            ELSE
                SET result = 'a not positive';
            END IF",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT result").unwrap();
    assert_table_eq!(result, [["b greater"]]);
}

#[test]
fn test_loop_with_multiple_exits() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE i INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE exit_type STRING").unwrap();

    executor
        .execute_sql(
            "LOOP
                SET i = i + 1;
                IF i = 10 THEN
                    SET exit_type = 'limit';
                    BREAK;
                END IF;
                IF i * i > 50 THEN
                    SET exit_type = 'square';
                    BREAK;
                END IF;
            END LOOP",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT i, exit_type").unwrap();
    assert_table_eq!(result, [[8, "square"]]);
}

#[test]
fn test_complex_while_loop() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE n INT64 DEFAULT 1").unwrap();
    executor
        .execute_sql("DECLARE factorial INT64 DEFAULT 1")
        .unwrap();

    executor
        .execute_sql(
            "WHILE n <= 5 DO
                SET factorial = factorial * n;
                SET n = n + 1;
            END WHILE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT factorial").unwrap();
    assert_table_eq!(result, [[120]]);
}

#[test]
#[ignore]
fn test_if_with_subquery_condition() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')")
        .unwrap();

    executor
        .execute_sql("DECLARE target_id INT64 DEFAULT 1")
        .unwrap();
    executor
        .execute_sql("DECLARE found STRING DEFAULT 'not found'")
        .unwrap();

    executor
        .execute_sql(
            "IF EXISTS(SELECT 1 FROM products WHERE id = target_id) THEN
                SET found = 'found';
            END IF",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT found").unwrap();
    assert_table_eq!(result, [["found"]]);
}

#[test]
#[ignore]
fn test_multiple_elseif() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE score INT64 DEFAULT 75")
        .unwrap();
    executor.execute_sql("DECLARE grade STRING").unwrap();

    executor
        .execute_sql(
            "IF score >= 90 THEN
                SET grade = 'A';
            ELSEIF score >= 80 THEN
                SET grade = 'B';
            ELSEIF score >= 70 THEN
                SET grade = 'C';
            ELSEIF score >= 60 THEN
                SET grade = 'D';
            ELSE
                SET grade = 'F';
            END IF",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT grade").unwrap();
    assert_table_eq!(result, [["C"]]);
}

#[test]
fn test_case_with_multiple_statements() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 1").unwrap();
    executor.execute_sql("DECLARE a INT64 DEFAULT 0").unwrap();
    executor.execute_sql("DECLARE b INT64 DEFAULT 0").unwrap();

    executor
        .execute_sql(
            "CASE x
                WHEN 1 THEN
                    SET a = 10;
                    SET b = 20;
                WHEN 2 THEN
                    SET a = 100;
                    SET b = 200;
                ELSE
                    SET a = -1;
                    SET b = -1;
            END CASE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT a, b").unwrap();
    assert_table_eq!(result, [[10, 20]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_declare_struct() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE person STRUCT<name STRING, age INT64> DEFAULT ('Alice', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT person.name, person.age")
        .unwrap();
    assert_table_eq!(result, [["Alice", 30]]);
}

#[test]
fn test_variable_reassignment() {
    let mut executor = create_executor();

    executor.execute_sql("DECLARE x INT64 DEFAULT 1").unwrap();
    executor.execute_sql("SET x = 2").unwrap();
    executor.execute_sql("SET x = x * 3").unwrap();
    executor.execute_sql("SET x = x + 4").unwrap();

    let result = executor.execute_sql("SELECT x").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_loop_counter() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE counter INT64 DEFAULT 0")
        .unwrap();
    executor
        .execute_sql("DECLARE iterations INT64 DEFAULT 0")
        .unwrap();

    executor
        .execute_sql(
            "LOOP
                SET iterations = iterations + 1;
                SET counter = counter + iterations;
                IF iterations >= 100 THEN
                    BREAK;
                END IF;
            END LOOP",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT counter").unwrap();
    assert_table_eq!(result, [[5050]]);
}
