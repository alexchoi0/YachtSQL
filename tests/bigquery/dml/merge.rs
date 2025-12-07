use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_tables(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO source VALUES (2, 'b_updated', 25), (3, 'c_updated', 35), (4, 'd', 40)",
        )
        .unwrap();
}

#[test]
fn test_merge_update_when_matched() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "a", 10], [2, "b_updated", 25], [3, "c_updated", 35],]
    );
}

#[test]
fn test_merge_insert_when_not_matched() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[test]
fn test_merge_delete_when_matched() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN DELETE")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_merge_update_and_insert() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "a"], [2, "b_updated"], [3, "c_updated"], [4, "d"],]
    );
}

#[test]
fn test_merge_with_condition() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED AND S.value > 30 THEN UPDATE SET name = S.name, value = S.value")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target WHERE id = 3")
        .unwrap();
    assert_table_eq!(result, [[3, 35]]);
}

#[test]
fn test_merge_update_delete_insert() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED AND S.value > 30 THEN DELETE WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [4]]);
}

#[test]
fn test_merge_with_subquery_source() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (2, 25), (3, 30)")
        .unwrap();

    executor
        .execute_sql("MERGE INTO target T USING (SELECT id, value FROM source WHERE value > 20) S ON T.id = S.id WHEN MATCHED THEN UPDATE SET value = S.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (S.id, S.value)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 25], [3, 30]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_merge_insert_row() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 'new', 100)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target T USING source S ON T.id = S.id WHEN NOT MATCHED THEN INSERT ROW",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target")
        .unwrap();
    assert_table_eq!(result, [[1, "new", 100]]);
}

#[test]
fn test_merge_when_not_matched_by_source() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (2, 25)")
        .unwrap();

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN NOT MATCHED BY SOURCE THEN DELETE")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_merge_all_clauses() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (2, 25), (4, 40)")
        .unwrap();

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET value = S.value WHEN NOT MATCHED BY TARGET THEN INSERT (id, value) VALUES (S.id, S.value) WHEN NOT MATCHED BY SOURCE THEN DELETE")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2, 25], [4, 40]]);
}

#[test]
fn test_merge_with_constants() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INT64, status STRING, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 'old', 10)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 20), (2, 30)")
        .unwrap();

    executor
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET status = 'updated', value = S.value WHEN NOT MATCHED THEN INSERT (id, status, value) VALUES (S.id, 'new', S.value)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, status FROM target ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "updated"], [2, "new"]]);
}
