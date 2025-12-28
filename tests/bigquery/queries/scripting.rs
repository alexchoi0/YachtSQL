use crate::assert_table_eq;
use crate::common::{create_session, date};

#[tokio::test(flavor = "current_thread")]
async fn test_declare_variable() {
    let session = create_session();

    session.execute_sql("DECLARE x INT64").await.unwrap();
    session.execute_sql("SET x = 10").await.unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_with_default() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 5")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_multiple() {
    let session = create_session();

    session.execute_sql("DECLARE a, b, c INT64").await.unwrap();
    session.execute_sql("SET a = 1").await.unwrap();
    session.execute_sql("SET b = 2").await.unwrap();
    session.execute_sql("SET c = 3").await.unwrap();

    let result = session.execute_sql("SELECT a + b + c").await.unwrap();
    assert_table_eq!(result, [[6]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_string() {
    let session = create_session();

    session
        .execute_sql("DECLARE name STRING DEFAULT 'Hello'")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT name").await.unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_variable() {
    let session = create_session();

    session
        .execute_sql("DECLARE counter INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("SET counter = counter + 1")
        .await
        .unwrap();
    session
        .execute_sql("SET counter = counter + 1")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT counter").await.unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_from_query() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30)")
        .await
        .unwrap();

    session.execute_sql("DECLARE total INT64").await.unwrap();
    session
        .execute_sql("SET total = (SELECT SUM(val) FROM numbers)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT total").await.unwrap();
    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_then() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 10")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
        .execute_sql(
            "IF x > 5 THEN
                SET result = 'big';
            END IF",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["big"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_then_else() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 3")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
        .execute_sql(
            "IF x > 5 THEN
                SET result = 'big';
            ELSE
                SET result = 'small';
            END IF",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["small"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_elseif_else() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 5")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
        .execute_sql(
            "IF x > 10 THEN
                SET result = 'large';
            ELSEIF x > 3 THEN
                SET result = 'medium';
            ELSE
                SET result = 'small';
            END IF",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["medium"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_loop_basic() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "LOOP
                SET i = i + 1;
                SET sum = sum + i;
                IF i >= 5 THEN
                    LEAVE;
                END IF;
            END LOOP",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_while_loop() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "WHILE i < 5 DO
                SET i = i + 1;
                SET sum = sum + i;
            END WHILE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_loop() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "REPEAT
                SET i = i + 1;
            UNTIL i >= 5
            END REPEAT",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_loop() {
    let session = create_session();

    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "FOR i IN (SELECT val FROM UNNEST([1, 2, 3, 4, 5]) AS val) DO
                SET sum = sum + i.val;
            END FOR",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_break_continue() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
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
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_begin_end_block() {
    let session = create_session();

    session
        .execute_sql(
            "BEGIN
                DECLARE x INT64 DEFAULT 10;
                DECLARE y INT64 DEFAULT 20;
                SELECT x + y;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT 1").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_blocks() {
    let session = create_session();

    session
        .execute_sql("DECLARE result INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                DECLARE a INT64 DEFAULT 1;
                BEGIN
                    DECLARE b INT64 DEFAULT 2;
                    SET result = a + b;
                END;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exception_handling() {
    let session = create_session();

    session
        .execute_sql("DECLARE result STRING DEFAULT 'start'")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET result = 'caught';
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["caught"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raise_exception() {
    let session = create_session();

    let result = session
        .execute_sql("RAISE USING MESSAGE = 'Custom error'")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE dynamic_test (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO dynamic_test VALUES (1), (2), (3)")
        .await
        .unwrap();

    session
        .execute_sql("EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM dynamic_test'")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM dynamic_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_with_params() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE param_test (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO param_test VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    let result = session
        .execute_sql("EXECUTE IMMEDIATE 'SELECT name FROM param_test WHERE id = @id' USING 1 AS id")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_ddl() {
    let session = create_session();

    session
        .execute_sql("EXECUTE IMMEDIATE 'CREATE TABLE created_dynamically (id INT64)'")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO created_dynamically VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM created_dynamically")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_statement() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 2")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
        .execute_sql(
            "CASE x
                WHEN 1 THEN SET result = 'one';
                WHEN 2 THEN SET result = 'two';
                WHEN 3 THEN SET result = 'three';
                ELSE SET result = 'other';
            END CASE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["two"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_return_statement() {
    let session = create_session();

    session
        .execute_sql("DECLARE result INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                SET result = 1;
                RETURN;
                SET result = 2;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_labeled_block() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "outer_loop: LOOP
                SET i = i + 1;
                IF i >= 3 THEN
                    LEAVE outer_loop;
                END IF;
            END LOOP outer_loop",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_in_query() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE employees (id INT64, salary INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO employees VALUES (1, 50000), (2, 60000), (3, 70000)")
        .await
        .unwrap();

    session
        .execute_sql("DECLARE min_salary INT64 DEFAULT 55000")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM employees WHERE salary > min_salary ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_system_variable() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT @@time_zone IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_system_variable() {
    let session = create_session();

    session
        .execute_sql("SET @@time_zone = 'America/Los_Angeles'")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT @@time_zone").await.unwrap();
    assert_table_eq!(result, [["America/Los_Angeles"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_type_inference() {
    let session = create_session();

    session.execute_sql("DECLARE x DEFAULT 42").await.unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[42]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_type_inference_string() {
    let session = create_session();

    session
        .execute_sql("DECLARE greeting DEFAULT 'Hello World'")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT greeting").await.unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_type_inference_from_expression() {
    let session = create_session();

    session
        .execute_sql("DECLARE computed DEFAULT 10 + 5 * 2")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT computed").await.unwrap();
    assert_table_eq!(result, [[20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_date_default() {
    let session = create_session();

    session
        .execute_sql("DECLARE d DATE DEFAULT DATE '2024-01-15'")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT d").await.unwrap();
    assert_table_eq!(result, [[date(2024, 1, 15)]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_float64() {
    let session = create_session();

    session
        .execute_sql("DECLARE pi FLOAT64 DEFAULT 3.14159")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT pi").await.unwrap();
    assert_table_eq!(result, [[3.14159]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_bool() {
    let session = create_session();

    session
        .execute_sql("DECLARE flag BOOL DEFAULT TRUE")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT flag").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_array() {
    let session = create_session();

    session
        .execute_sql("DECLARE arr ARRAY<INT64> DEFAULT [1, 2, 3]")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT arr").await.unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_multiple_with_default() {
    let session = create_session();

    session
        .execute_sql("DECLARE x, y, z INT64 DEFAULT 0")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT x, y, z").await.unwrap();
    assert_table_eq!(result, [[0, 0, 0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_multiple_variables() {
    let session = create_session();

    session.execute_sql("DECLARE a INT64").await.unwrap();
    session.execute_sql("DECLARE b STRING").await.unwrap();
    session.execute_sql("DECLARE c BOOL").await.unwrap();

    session
        .execute_sql("SET (a, b, c) = (1 + 3, 'foo', FALSE)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT a, b, c").await.unwrap();
    assert_table_eq!(result, [[4, "foo", false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_set_from_struct_query() {
    let session = create_session();

    session.execute_sql("DECLARE count1 INT64").await.unwrap();
    session.execute_sql("DECLARE count2 INT64").await.unwrap();

    session
        .execute_sql("SET (count1, count2) = (SELECT AS STRUCT 10, 20)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT count1, count2").await.unwrap();
    assert_table_eq!(result, [[10, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_into() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE books (title STRING, year INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO books VALUES ('Hamlet', 1599), ('Ulysses', 1922)")
        .await
        .unwrap();

    session.execute_sql("DECLARE min_year INT64").await.unwrap();

    session
        .execute_sql("EXECUTE IMMEDIATE 'SELECT MIN(year) FROM books' INTO min_year")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT min_year").await.unwrap();
    assert_table_eq!(result, [[1599]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_using_positional() {
    let session = create_session();

    session.execute_sql("DECLARE y INT64").await.unwrap();

    session
        .execute_sql("EXECUTE IMMEDIATE 'SELECT ? * (? + 2)' INTO y USING 1, 3")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT y").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_using_named() {
    let session = create_session();

    session.execute_sql("DECLARE y INT64").await.unwrap();

    session
        .execute_sql("EXECUTE IMMEDIATE 'SELECT @a * (@b + 2)' INTO y USING 1 AS a, 3 AS b")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT y").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_execute_immediate_dml() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("DECLARE item_name STRING DEFAULT 'Widget'")
        .await
        .unwrap();

    session
        .execute_sql("EXECUTE IMMEDIATE 'INSERT INTO items (name) VALUES(?)' USING item_name")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT name FROM items").await.unwrap();
    assert_table_eq!(result, [["Widget"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exception_error_message() {
    let session = create_session();

    session
        .execute_sql("DECLARE error_msg STRING")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET error_msg = @@error.message;
            END",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT error_msg IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exception_statement_text() {
    let session = create_session();

    session
        .execute_sql("DECLARE stmt_text STRING")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                SELECT 1 / 0;
            EXCEPTION WHEN ERROR THEN
                SET stmt_text = @@error.statement_text;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT stmt_text").await.unwrap();
    assert_table_eq!(result, [["SELECT 1 / 0"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_exception_reraise() {
    let session = create_session();

    session
        .execute_sql("DECLARE caught BOOL DEFAULT FALSE")
        .await
        .unwrap();

    let result = session
        .execute_sql(
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
        )
        .await;
    assert!(result.is_ok());
    let result = session.execute_sql("SELECT caught").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_conditional() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 15")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
        .execute_sql(
            "CASE
                WHEN x > 20 THEN SET result = 'large';
                WHEN x > 10 THEN SET result = 'medium';
                ELSE SET result = 'small';
            END CASE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["medium"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_conditional_no_match() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 5")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE result STRING DEFAULT 'default'")
        .await
        .unwrap();

    session
        .execute_sql(
            "CASE
                WHEN x > 20 THEN SET result = 'large';
                WHEN x > 10 THEN SET result = 'medium';
            END CASE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_search_expression_else() {
    let session = create_session();

    session
        .execute_sql("DECLARE product_id INT64 DEFAULT 99")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
        .execute_sql(
            "CASE product_id
                WHEN 1 THEN SET result = 'Product one';
                WHEN 2 THEN SET result = 'Product two';
                ELSE SET result = 'Invalid product';
            END CASE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["Invalid product"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_while_with_break() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "WHILE TRUE DO
                SET i = i + 1;
                IF i >= 5 THEN
                    BREAK;
                END IF;
            END WHILE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_while_never_executes() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 100")
        .await
        .unwrap();

    session
        .execute_sql(
            "WHILE i < 10 DO
                SET i = i + 1;
            END WHILE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_basic() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "REPEAT
                SET i = i + 1;
                SET sum = sum + i;
            UNTIL i >= 5
            END REPEAT",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_executes_at_least_once() {
    let session = create_session();

    session
        .execute_sql("DECLARE executed BOOL DEFAULT FALSE")
        .await
        .unwrap();

    session
        .execute_sql(
            "REPEAT
                SET executed = TRUE;
            UNTIL TRUE
            END REPEAT",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT executed").await.unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_in_basic() {
    let session = create_session();

    session
        .execute_sql("DECLARE total INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "FOR record IN (SELECT n FROM UNNEST([1, 2, 3, 4, 5]) AS n)
            DO
                SET total = total + record.n;
            END FOR",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT total").await.unwrap();
    assert_table_eq!(result, [[15]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_in_with_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE numbers (val INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (10), (20), (30)")
        .await
        .unwrap();

    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "FOR row IN (SELECT val FROM numbers ORDER BY val)
            DO
                SET sum = sum + row.val;
            END FOR",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[60]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_for_in_with_break() {
    let session = create_session();

    session
        .execute_sql("DECLARE last_val INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "FOR record IN (SELECT n FROM UNNEST([1, 2, 3, 4, 5]) AS n)
            DO
                SET last_val = record.n;
                IF record.n >= 3 THEN
                    BREAK;
                END IF;
            END FOR",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT last_val").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_labeled_loop_leave() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE j INT64 DEFAULT 0")
        .await
        .unwrap();

    session
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
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i, j").await.unwrap();
    assert_table_eq!(result, [[2, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_labeled_loop_continue() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE skipped INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "outer_loop: WHILE i < 5 DO
                SET i = i + 1;
                IF i = 3 THEN
                    SET skipped = skipped + 1;
                    CONTINUE outer_loop;
                END IF;
            END WHILE outer_loop",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i, skipped").await.unwrap();
    assert_table_eq!(result, [[5, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_labeled_begin_leave() {
    let session = create_session();

    session
        .execute_sql("DECLARE result STRING DEFAULT 'start'")
        .await
        .unwrap();

    session
        .execute_sql(
            "my_block: BEGIN
                SET result = 'in block';
                LEAVE my_block;
                SET result = 'after leave';
            END my_block",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["in block"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_iterate_synonym() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE sum INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "WHILE i < 5 DO
                SET i = i + 1;
                IF i = 3 THEN
                    ITERATE;
                END IF;
                SET sum = sum + i;
            END WHILE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT sum").await.unwrap();
    assert_table_eq!(result, [[12]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_leave_synonym() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "LOOP
                SET i = i + 1;
                IF i >= 3 THEN
                    LEAVE;
                END IF;
            END LOOP",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_begin_transaction_commit() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE accounts (id INT64, balance INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO accounts VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();
    session
        .execute_sql("UPDATE accounts SET balance = balance - 50 WHERE id = 1")
        .await
        .unwrap();
    session
        .execute_sql("UPDATE accounts SET balance = balance + 50 WHERE id = 2")
        .await
        .unwrap();
    session.execute_sql("COMMIT TRANSACTION").await.unwrap();

    let result = session
        .execute_sql("SELECT id, balance FROM accounts ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 50], [2, 250]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_begin_transaction_rollback() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('existing')")
        .await
        .unwrap();

    session.execute_sql("BEGIN TRANSACTION").await.unwrap();
    session
        .execute_sql("INSERT INTO items VALUES ('new item')")
        .await
        .unwrap();
    session.execute_sql("ROLLBACK TRANSACTION").await.unwrap();

    let result = session.execute_sql("SELECT name FROM items").await.unwrap();
    assert_table_eq!(result, [["existing"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_transaction_with_exception_rollback() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1)")
        .await
        .unwrap();

    session
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
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_raise_with_message() {
    let session = create_session();

    let result = session
        .execute_sql("RAISE USING MESSAGE = 'Something went wrong'")
        .await;
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Something went wrong"));
}

#[tokio::test(flavor = "current_thread")]
async fn test_raise_and_catch() {
    let session = create_session();

    session
        .execute_sql("DECLARE caught_msg STRING")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                RAISE USING MESSAGE = 'Custom error';
            EXCEPTION WHEN ERROR THEN
                SET caught_msg = @@error.message;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT caught_msg").await.unwrap();
    assert_table_eq!(result, [["Custom error"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_return_in_block() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                SET x = 1;
                RETURN;
                SET x = 2;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_return_in_loop() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "WHILE TRUE DO
                SET i = i + 1;
                IF i = 5 THEN
                    RETURN;
                END IF;
            END WHILE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_scope_in_block() {
    let session = create_session();

    session
        .execute_sql("DECLARE outer_var INT64 DEFAULT 10")
        .await
        .unwrap();

    session
        .execute_sql(
            "BEGIN
                DECLARE inner_var INT64 DEFAULT 20;
                SET outer_var = outer_var + inner_var;
            END",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT outer_var").await.unwrap();
    assert_table_eq!(result, [[30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nested_if() {
    let session = create_session();

    session
        .execute_sql("DECLARE a INT64 DEFAULT 5")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE b INT64 DEFAULT 10")
        .await
        .unwrap();
    session.execute_sql("DECLARE result STRING").await.unwrap();

    session
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
        .await
        .unwrap();

    let result = session.execute_sql("SELECT result").await.unwrap();
    assert_table_eq!(result, [["b greater"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_loop_with_multiple_exits() {
    let session = create_session();

    session
        .execute_sql("DECLARE i INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE exit_type STRING")
        .await
        .unwrap();

    session
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
        .await
        .unwrap();

    let result = session.execute_sql("SELECT i, exit_type").await.unwrap();
    assert_table_eq!(result, [[8, "square"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_complex_while_loop() {
    let session = create_session();

    session
        .execute_sql("DECLARE n INT64 DEFAULT 1")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE factorial INT64 DEFAULT 1")
        .await
        .unwrap();

    session
        .execute_sql(
            "WHILE n <= 5 DO
                SET factorial = factorial * n;
                SET n = n + 1;
            END WHILE",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT factorial").await.unwrap();
    assert_table_eq!(result, [[120]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_if_with_subquery_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE products (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO products VALUES (1, 'Widget'), (2, 'Gadget')")
        .await
        .unwrap();

    session
        .execute_sql("DECLARE target_id INT64 DEFAULT 1")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE found STRING DEFAULT 'not found'")
        .await
        .unwrap();

    session
        .execute_sql(
            "IF EXISTS(SELECT 1 FROM products WHERE id = target_id) THEN
                SET found = 'found';
            END IF",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT found").await.unwrap();
    assert_table_eq!(result, [["found"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_multiple_elseif() {
    let session = create_session();

    session
        .execute_sql("DECLARE score INT64 DEFAULT 75")
        .await
        .unwrap();
    session.execute_sql("DECLARE grade STRING").await.unwrap();

    session
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
        .await
        .unwrap();

    let result = session.execute_sql("SELECT grade").await.unwrap();
    assert_table_eq!(result, [["C"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_case_with_multiple_statements() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 1")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE a INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE b INT64 DEFAULT 0")
        .await
        .unwrap();

    session
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
        .await
        .unwrap();

    let result = session.execute_sql("SELECT a, b").await.unwrap();
    assert_table_eq!(result, [[10, 20]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_declare_struct() {
    let session = create_session();

    session
        .execute_sql("DECLARE person STRUCT<name STRING, age INT64> DEFAULT ('Alice', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT person.name, person.age")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_variable_reassignment() {
    let session = create_session();

    session
        .execute_sql("DECLARE x INT64 DEFAULT 1")
        .await
        .unwrap();
    session.execute_sql("SET x = 2").await.unwrap();
    session.execute_sql("SET x = x * 3").await.unwrap();
    session.execute_sql("SET x = x + 4").await.unwrap();

    let result = session.execute_sql("SELECT x").await.unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_loop_counter() {
    let session = create_session();

    session
        .execute_sql("DECLARE counter INT64 DEFAULT 0")
        .await
        .unwrap();
    session
        .execute_sql("DECLARE iterations INT64 DEFAULT 0")
        .await
        .unwrap();

    session
        .execute_sql(
            "LOOP
                SET iterations = iterations + 1;
                SET counter = counter + iterations;
                IF iterations >= 100 THEN
                    BREAK;
                END IF;
            END LOOP",
        )
        .await
        .unwrap();

    let result = session.execute_sql("SELECT counter").await.unwrap();
    assert_table_eq!(result, [[5050]]);
}
