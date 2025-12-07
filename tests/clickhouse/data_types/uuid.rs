use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_uuid_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toUUID('61f0c404-5cb3-11e7-907b-a6006ad3dba0')")
        .unwrap();
    assert_table_eq!(result, [["61f0c404-5cb3-11e7-907b-a6006ad3dba0"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_column_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_table (id UUID, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO uuid_table VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'test')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name FROM uuid_table WHERE id = '61f0c404-5cb3-11e7-907b-a6006ad3dba0'",
        )
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_order (id UUID, val INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_order VALUES ('00000000-0000-0000-0000-000000000003', 3), ('00000000-0000-0000-0000-000000000001', 1), ('00000000-0000-0000-0000-000000000002', 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM uuid_order ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_dup (id UUID)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_dup VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('61f0c404-5cb3-11e7-907b-a6006ad3dba0'), ('71f0c404-5cb3-11e7-907b-a6006ad3dba0')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT id) FROM uuid_dup")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_nullable (id UUID, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_nullable VALUES (NULL, 'null_id'), ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 'has_id')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM uuid_nullable WHERE id IS NULL")
        .unwrap();
    assert_table_eq!(result, [["null_id"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_uuid_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_group (id UUID, amount INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_group VALUES ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 10), ('61f0c404-5cb3-11e7-907b-a6006ad3dba0', 20), ('71f0c404-5cb3-11e7-907b-a6006ad3dba0', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(amount) FROM uuid_group GROUP BY id ORDER BY SUM(amount)")
        .unwrap();
    assert_table_eq!(result, [[30], [30]]);
}
