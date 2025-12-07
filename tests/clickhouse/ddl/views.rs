use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE base_table (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO base_table VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW name_view AS SELECT name FROM base_table")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM name_view ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["alice"], ["bob"]]);
}

#[test]
fn test_create_view_with_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW high_values AS SELECT * FROM data WHERE value > 15")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM high_values ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_create_view_with_aggregation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sales VALUES ('A', 100), ('A', 200), ('B', 150)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW sales_summary AS SELECT product, SUM(amount) AS total FROM sales GROUP BY product")
        .unwrap();

    let result = executor
        .execute_sql("SELECT product, total FROM sales_summary ORDER BY product")
        .unwrap();
    assert_table_eq!(result, [["A", 300], ["B", 150]]);
}

#[test]
fn test_create_view_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, user_id INT64, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW user_orders AS SELECT u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, amount FROM user_orders ORDER BY name, amount")
        .unwrap();
    assert_table_eq!(result, [["alice", 100], ["alice", 200], ["bob", 150]]);
}

#[test]
fn test_create_or_replace_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE numbers (n INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO numbers VALUES (1), (2), (3)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW num_view AS SELECT n FROM numbers WHERE n > 0")
        .unwrap();

    executor
        .execute_sql("CREATE OR REPLACE VIEW num_view AS SELECT n FROM numbers WHERE n > 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT n FROM num_view ORDER BY n")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_drop_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW drop_view AS SELECT * FROM drop_test")
        .unwrap();

    let result = executor.execute_sql("DROP VIEW drop_view");
    assert!(result.is_ok());
}

#[test]
fn test_drop_view_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP VIEW IF EXISTS non_existent_view");
    assert!(result.is_ok());
}

#[test]
fn test_view_reflects_base_changes() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dynamic (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dynamic VALUES (1, 100)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW dynamic_view AS SELECT * FROM dynamic")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT COUNT(*) FROM dynamic_view")
        .unwrap();
    assert_table_eq!(result1, [[1]]);

    executor
        .execute_sql("INSERT INTO dynamic VALUES (2, 200)")
        .unwrap();

    let result2 = executor
        .execute_sql("SELECT COUNT(*) FROM dynamic_view")
        .unwrap();
    assert_table_eq!(result2, [[2]]);
}

#[test]
fn test_create_materialized_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE mat_base (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO mat_base VALUES (1, 10), (2, 20)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE MATERIALIZED VIEW mat_view AS SELECT id, value * 2 AS doubled FROM mat_base",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, doubled FROM mat_view ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 20], [2, 40]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_view_with_column_aliases() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE aliased (id INT64, first_name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO aliased VALUES (1, 'alice')")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VIEW aliased_view (user_id, user_name) AS SELECT id, first_name FROM aliased",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT user_id, user_name FROM aliased_view")
        .unwrap();
    assert_table_eq!(result, [[1, "alice"]]);
}

#[test]
fn test_view_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sub_base (id INT64, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sub_base VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    executor
        .execute_sql("CREATE VIEW sub_view AS SELECT id, val FROM sub_base WHERE val > 150")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM sub_base WHERE id IN (SELECT id FROM sub_view) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}
