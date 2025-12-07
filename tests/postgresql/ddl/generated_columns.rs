use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_generated_column_stored() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_stored (
            id INT64,
            price INT64,
            quantity INT64,
            total INT64 GENERATED ALWAYS AS (price * quantity) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_stored (id, price, quantity) VALUES (1, 100, 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT total FROM gen_stored WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[500]]);
}

#[test]
fn test_generated_column_virtual() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_virtual (
            id INT64,
            first_name STRING,
            last_name STRING,
            full_name STRING GENERATED ALWAYS AS (first_name || ' ' || last_name) VIRTUAL
        )",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO gen_virtual (id, first_name, last_name) VALUES (1, 'John', 'Doe')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT full_name FROM gen_virtual WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [["John Doe"]]);
}

#[test]
fn test_generated_column_expression() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_expr (
            id INT64,
            val INT64,
            doubled INT64 GENERATED ALWAYS AS (val * 2) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_expr (id, val) VALUES (1, 10)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT doubled FROM gen_expr")
        .unwrap();
    assert_table_eq!(result, [[20]]);
}

#[test]
fn test_generated_column_concat() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_concat (
            id INT64,
            prefix STRING,
            code INT64,
            full_code STRING GENERATED ALWAYS AS (prefix || '-' || CAST(code AS STRING)) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_concat (id, prefix, code) VALUES (1, 'ABC', 123)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT full_code FROM gen_concat")
        .unwrap();
    assert_table_eq!(result, [["ABC-123"]]);
}

#[test]
fn test_generated_column_math() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_math (
            id INT64,
            width FLOAT64,
            height FLOAT64,
            area FLOAT64 GENERATED ALWAYS AS (width * height) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_math (id, width, height) VALUES (1, 10.0, 5.0)")
        .unwrap();

    let result = executor.execute_sql("SELECT area FROM gen_math").unwrap();
    assert_table_eq!(result, [[50.0]]);
}

#[test]
fn test_generated_column_case() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_case (
            id INT64,
            score INT64,
            grade STRING GENERATED ALWAYS AS (
                CASE
                    WHEN score >= 90 THEN 'A'
                    WHEN score >= 80 THEN 'B'
                    WHEN score >= 70 THEN 'C'
                    ELSE 'F'
                END
            ) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_case (id, score) VALUES (1, 85)")
        .unwrap();

    let result = executor.execute_sql("SELECT grade FROM gen_case").unwrap();
    assert_table_eq!(result, [["B"]]);
}

#[test]
fn test_generated_column_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_coalesce (
            id INT64,
            name STRING,
            display_name STRING GENERATED ALWAYS AS (COALESCE(name, 'Unknown')) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_coalesce (id, name) VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT display_name FROM gen_coalesce")
        .unwrap();
    assert_table_eq!(result, [["Unknown"]]);
}

#[test]
fn test_generated_column_update_dependency() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_update (
            id INT64,
            val INT64,
            computed INT64 GENERATED ALWAYS AS (val + 100) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_update (id, val) VALUES (1, 50)")
        .unwrap();
    executor
        .execute_sql("UPDATE gen_update SET val = 75 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT computed FROM gen_update")
        .unwrap();
    assert_table_eq!(result, [[175]]);
}

#[test]
fn test_generated_column_multiple() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_multi (
            id INT64,
            a INT64,
            b INT64,
            sum_ab INT64 GENERATED ALWAYS AS (a + b) STORED,
            diff_ab INT64 GENERATED ALWAYS AS (a - b) STORED,
            prod_ab INT64 GENERATED ALWAYS AS (a * b) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_multi (id, a, b) VALUES (1, 10, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sum_ab, diff_ab, prod_ab FROM gen_multi")
        .unwrap();
    assert_table_eq!(result, [[13, 7, 30]]);
}

#[test]
fn test_generated_column_with_function() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_func (
            id INT64,
            text_val STRING,
            upper_val STRING GENERATED ALWAYS AS (UPPER(text_val)) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_func (id, text_val) VALUES (1, 'hello')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT upper_val FROM gen_func")
        .unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[test]
fn test_generated_column_length() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_len (
            id INT64,
            text_val STRING,
            text_length INT64 GENERATED ALWAYS AS (LENGTH(text_val)) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_len (id, text_val) VALUES (1, 'hello world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT text_length FROM gen_len")
        .unwrap();
    assert_table_eq!(result, [[11]]);
}

#[test]
fn test_generated_column_abs() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_abs (
            id INT64,
            val INT64,
            abs_val INT64 GENERATED ALWAYS AS (ABS(val)) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_abs (id, val) VALUES (1, -42)")
        .unwrap();

    let result = executor.execute_sql("SELECT abs_val FROM gen_abs").unwrap();
    assert_table_eq!(result, [[42]]);
}

#[test]
fn test_generated_column_round() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_round (
            id INT64,
            val FLOAT64,
            rounded INT64 GENERATED ALWAYS AS (CAST(ROUND(val) AS INT64)) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_round (id, val) VALUES (1, 3.7)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT rounded FROM gen_round")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_generated_column_boolean() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_bool (
            id INT64,
            val INT64,
            is_positive BOOL GENERATED ALWAYS AS (val > 0) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_bool (id, val) VALUES (1, 10), (2, -5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT is_positive FROM gen_bool ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[true], [false]]);
}

#[test]
fn test_generated_column_select_all() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_select (
            id INT64,
            base INT64,
            computed INT64 GENERATED ALWAYS AS (base * 10) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_select (id, base) VALUES (1, 5)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM gen_select").unwrap();
    assert_table_eq!(result, [[1, 5, 50]]);
}

#[test]
fn test_generated_column_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_where (
            id INT64,
            val INT64,
            doubled INT64 GENERATED ALWAYS AS (val * 2) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_where (id, val) VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM gen_where WHERE doubled > 30")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_generated_column_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_order (
            id INT64,
            val INT64,
            neg_val INT64 GENERATED ALWAYS AS (-val) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_order (id, val) VALUES (1, 10), (2, 5), (3, 15)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM gen_order ORDER BY neg_val")
        .unwrap();
    assert_table_eq!(result, [[3], [1], [2]]);
}

#[test]
fn test_generated_column_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_group (
            id INT64,
            category STRING,
            val INT64,
            doubled INT64 GENERATED ALWAYS AS (val * 2) STORED
        )",
        )
        .unwrap();
    executor.execute_sql("INSERT INTO gen_group (id, category, val) VALUES (1, 'A', 10), (2, 'A', 20), (3, 'B', 15)").unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, SUM(doubled) FROM gen_group GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", 60], ["B", 30]]);
}

#[test]
fn test_generated_column_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE gen_main (
            id INT64,
            val INT64,
            key INT64 GENERATED ALWAYS AS (val % 10) STORED
        )",
        )
        .unwrap();
    executor
        .execute_sql("CREATE TABLE gen_ref (key INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_main (id, val) VALUES (1, 25)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO gen_ref VALUES (5, 'Five')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT m.id, r.name FROM gen_main m JOIN gen_ref r ON m.key = r.key")
        .unwrap();
    assert_table_eq!(result, [[1, "Five"]]);
}
