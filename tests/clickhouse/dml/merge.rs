use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_merge_insert_only() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 'alice', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (2, 'bob', 200)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.name, source.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "alice"], [2, "bob"]]);
}

#[test]
fn test_merge_update_only() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 'alice', 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 'alice_updated', 150)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name, value = source.value",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target")
        .unwrap();
    assert_table_eq!(result, [[1, "alice_updated", 150]]);
}

#[test]
fn test_merge_delete_only() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1), (3)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN DELETE",
        )
        .unwrap();

    let result = executor.execute_sql("SELECT id, name FROM target").unwrap();
    assert_table_eq!(result, [[2, "bob"]]);
}

#[test]
fn test_merge_upsert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 'alice', 100), (2, 'bob', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (2, 'bob_updated', 250), (3, 'charlie', 300)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET name = source.name, value = source.value WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.name, source.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "alice", 100],
            [2, "bob_updated", 250],
            [3, "charlie", 300]
        ]
    );
}

#[test]
fn test_merge_with_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 'alice', 100), (2, 'bob', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 'alice_new', 50), (2, 'bob_new', 300)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED AND source.value > target.value THEN UPDATE SET name = source.name, value = source.value",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "alice", 100], [2, "bob_new", 300]]);
}

#[test]
fn test_merge_multiple_when_clauses() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, status STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, status STRING, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO source VALUES (1, 'inactive', 150), (2, 'deleted', 0), (4, 'active', 400)",
        )
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id
             WHEN MATCHED AND source.status = 'deleted' THEN DELETE
             WHEN MATCHED THEN UPDATE SET status = source.status, value = source.value
             WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.status, source.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, status, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "inactive", 150], [3, "active", 300], [4, "active", 400]]
    );
}

#[test]
fn test_merge_with_subquery_source() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source_raw (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source_raw VALUES (1, 150), (2, 200), (3, 50)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING (SELECT id, value FROM source_raw WHERE value > 100) AS source ON target.id = source.id WHEN MATCHED THEN UPDATE SET value = source.value WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 150], [2, 200]]);
}

#[test]
fn test_merge_empty_source() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 100), (2, 200)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET value = source.value WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
fn test_merge_empty_target() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO source VALUES (1, 100), (2, 200)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN NOT MATCHED THEN INSERT VALUES (source.id, source.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
fn test_merge_all_rows_match() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO target VALUES (1, 100), (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 150), (2, 250)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target USING source ON target.id = source.id WHEN MATCHED THEN UPDATE SET value = source.value",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 150], [2, 250]]);
}
