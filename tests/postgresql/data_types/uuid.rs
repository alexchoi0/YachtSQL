use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_uuid_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID")
        .unwrap();
    assert_table_eq!(result, [["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]]);
}

#[test]
fn test_uuid_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users_uuid (id UUID, name TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO users_uuid VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'Alice')",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT name FROM users_uuid").unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_uuid_comparison() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID"
    ).unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_uuid_not_equal() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID <> 'b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID"
    ).unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_uuid_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_null (id INTEGER, uuid_col UUID)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_null VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT uuid_col IS NULL FROM uuid_null")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_uuid_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_order (id UUID)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_order VALUES ('c0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_order VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_order VALUES ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM uuid_order ORDER BY id LIMIT 1")
        .unwrap();
    assert_table_eq!(result, [["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]]);
}

#[test]
fn test_gen_random_uuid() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT GEN_RANDOM_UUID()").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_uuid_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_where (id UUID, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_where VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_where VALUES ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 200)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT value FROM uuid_where WHERE id = 'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'::UUID",
        )
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_uuid_uppercase() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 'A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11'::UUID")
        .unwrap();
    assert_table_eq!(result, [["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]]);
}

#[test]
fn test_uuid_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_dup (id UUID)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_dup VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_dup VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_dup VALUES ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT id FROM uuid_dup ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"],
            ["b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"]
        ]
    );
}

#[test]
fn test_uuid_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uuid_group (id UUID, amount INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_group VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_group VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 50)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uuid_group VALUES ('b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 200)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, SUM(amount) FROM uuid_group GROUP BY id ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", 150],
            ["b0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11", 200]
        ]
    );
}
