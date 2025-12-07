use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_insert_returning_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_insert (id INT64, name STRING)")
        .unwrap();

    let result = executor
        .execute_sql("INSERT INTO ret_insert VALUES (1, 'Alice') RETURNING *")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_insert_returning_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_cols (id INT64, name STRING, age INT64)")
        .unwrap();

    let result = executor
        .execute_sql("INSERT INTO ret_cols VALUES (1, 'Alice', 30) RETURNING id, name")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_insert_returning_expression() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_expr (id INT64, val INT64)")
        .unwrap();

    let result = executor
        .execute_sql("INSERT INTO ret_expr VALUES (1, 100) RETURNING id, val * 2 AS doubled")
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}

#[test]
fn test_insert_returning_multiple_rows() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_multi (id INT64, name STRING)")
        .unwrap();

    let result = executor
        .execute_sql(
            "INSERT INTO ret_multi VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie') RETURNING *",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"], [2, "Bob"], [3, "Charlie"]]);
}

#[test]
fn test_update_returning_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_upd (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_upd VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("UPDATE ret_upd SET val = val + 10 WHERE id < 3 RETURNING *")
        .unwrap();
    assert_table_eq!(result, [[1, 110], [2, 210]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_update_returning_old_new() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_old_new (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_upd VALUES (1, 100)")
        .unwrap();

    let result = executor
        .execute_sql("UPDATE ret_old_new SET val = 200 WHERE id = 1 RETURNING id, val AS new_val")
        .unwrap();
    let _ = result;
}

#[test]
fn test_delete_returning_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_del (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_del VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();

    let result = executor
        .execute_sql("DELETE FROM ret_del WHERE id > 1 RETURNING *")
        .unwrap();
    assert_table_eq!(result, [[2, "Bob"], [3, "Charlie"]]);
}

#[test]
fn test_delete_returning_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_del_cols (id INT64, name STRING, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_del_cols VALUES (1, 'Alice', TRUE), (2, 'Bob', FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("DELETE FROM ret_del_cols WHERE active = FALSE RETURNING id, name")
        .unwrap();
    assert_table_eq!(result, [[2, "Bob"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_insert_returning_with_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_def (id INT64, name STRING DEFAULT 'Unknown')")
        .unwrap();

    let result = executor
        .execute_sql("INSERT INTO ret_def (id) VALUES (1) RETURNING *")
        .unwrap();
    assert_table_eq!(result, [[1, "Unknown"]]);
}

#[test]
fn test_insert_returning_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_func (id INT64, name STRING)")
        .unwrap();

    let result = executor
        .execute_sql(
            "INSERT INTO ret_func VALUES (1, 'alice') RETURNING id, UPPER(name) AS upper_name",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "ALICE"]]);
}

#[test]
fn test_upsert_returning() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_upsert (id INT64 PRIMARY KEY, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_upsert VALUES (1, 100)")
        .unwrap();

    let result = executor
        .execute_sql(
            "INSERT INTO ret_upsert VALUES (1, 200)
             ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val
             RETURNING *",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 200]]);
}

#[test]
fn test_insert_returning_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_null (id INT64, opt_val STRING)")
        .unwrap();

    let result = executor
        .execute_sql("INSERT INTO ret_null VALUES (1, NULL) RETURNING *")
        .unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[test]
fn test_update_returning_case() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_case (id INT64, status STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_case VALUES (1, 'pending'), (2, 'active')")
        .unwrap();

    let result = executor
        .execute_sql(
            "UPDATE ret_case SET status = 'completed'
             RETURNING id, CASE WHEN status = 'completed' THEN 'done' ELSE 'other' END AS label",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "done"], [2, "done"]]);
}

#[test]
fn test_delete_returning_with_join_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_del_cond (id INT64, category STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_del_cond VALUES (1, 'A'), (2, 'B'), (3, 'A')")
        .unwrap();

    let result = executor
        .execute_sql("DELETE FROM ret_del_cond WHERE category = 'A' RETURNING id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_insert_returning_concat() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_concat (id INT64, first_name STRING, last_name STRING)")
        .unwrap();

    let result = executor
        .execute_sql(
            "INSERT INTO ret_concat VALUES (1, 'John', 'Doe')
             RETURNING id, first_name || ' ' || last_name AS full_name",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "John Doe"]]);
}

#[test]
fn test_insert_returning_arithmetic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_arith (id INT64, quantity INT64, price INT64)")
        .unwrap();

    let result = executor
        .execute_sql(
            "INSERT INTO ret_arith VALUES (1, 5, 100)
             RETURNING id, quantity * price AS total",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 500]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_update_returning_aggregate_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_agg (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_agg VALUES (1, 10), (2, 20)")
        .unwrap();

    let result = executor
        .execute_sql(
            "UPDATE ret_agg SET val = val + 5 WHERE id = 1
             RETURNING id, val, (SELECT SUM(val) FROM ret_agg) AS total",
        )
        .unwrap();
    let _ = result;
}

#[test]
fn test_delete_returning_count() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_count (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_count VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("DELETE FROM ret_count WHERE id <= 3 RETURNING id")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_insert_returning_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_coal (id INT64, val STRING)")
        .unwrap();

    let result = executor
        .execute_sql(
            "INSERT INTO ret_coal VALUES (1, NULL)
             RETURNING id, COALESCE(val, 'default') AS val",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "default"]]);
}

#[test]
fn test_update_returning_boolean() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ret_bool (id INT64, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ret_bool VALUES (1, FALSE)")
        .unwrap();

    let result = executor
        .execute_sql("UPDATE ret_bool SET active = TRUE WHERE id = 1 RETURNING *")
        .unwrap();
    assert_table_eq!(result, [[1, true]]);
}
