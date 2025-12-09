use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_enum8_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum8_test (id INT64, status Enum8('active' = 1, 'inactive' = 2, 'pending' = 3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum8_test VALUES (1, 'active'), (2, 'inactive'), (3, 'pending')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, status FROM enum8_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "active"], [2, "inactive"], [3, "pending"]]);
}

#[test]
fn test_enum16_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum16_test (id INT64, priority Enum16('low' = 1, 'medium' = 2, 'high' = 3, 'critical' = 4))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum16_test VALUES (1, 'low'), (2, 'high'), (3, 'critical')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, priority FROM enum16_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "low"], [2, "high"], [3, "critical"]]);
}

#[test]
fn test_enum_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE enum_filter (id INT64, status Enum8('active' = 1, 'inactive' = 2))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_filter VALUES (1, 'active'), (2, 'inactive'), (3, 'active')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM enum_filter WHERE status = 'active' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_enum_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE enum_order (id INT64, level Enum8('low' = 1, 'medium' = 2, 'high' = 3))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_order VALUES (1, 'high'), (2, 'low'), (3, 'medium')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM enum_order ORDER BY level")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_enum_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum_group (id INT64, category Enum8('A' = 1, 'B' = 2, 'C' = 3), value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO enum_group VALUES (1, 'A', 10), (2, 'B', 20), (3, 'A', 30), (4, 'B', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) AS total FROM enum_group GROUP BY category ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 40], ["B", 60]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_to_int_from_enum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum_to_int (status Enum8('active' = 1, 'inactive' = 2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_to_int VALUES ('active'), ('inactive')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT toInt8(status) FROM enum_to_int ORDER BY status")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_enum_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE enum_cmp (id INT64, level Enum8('low' = 1, 'medium' = 2, 'high' = 3))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_cmp VALUES (1, 'low'), (2, 'medium'), (3, 'high')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM enum_cmp WHERE level > 'low' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_enum_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum_distinct (status Enum8('A' = 1, 'B' = 2, 'C' = 3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_distinct VALUES ('A'), ('B'), ('A'), ('C'), ('B')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT status FROM enum_distinct ORDER BY status")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_enum_insert_by_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum_by_val (status Enum8('active' = 1, 'inactive' = 2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_by_val VALUES (1), (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT status FROM enum_by_val ORDER BY status")
        .unwrap();
    assert_table_eq!(result, [["active"], ["inactive"]]);
}

#[test]
fn test_enum_case_sensitive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enum_case (status Enum8('Active' = 1, 'INACTIVE' = 2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO enum_case VALUES ('Active'), ('INACTIVE')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT status FROM enum_case ORDER BY status")
        .unwrap();
    assert_table_eq!(result, [["Active"], ["INACTIVE"]]);
}
