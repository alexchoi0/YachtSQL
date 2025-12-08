use yachtsql::QueryExecutor;

use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_merge_tables(executor: &mut QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE target (id INTEGER, name TEXT, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INTEGER, name TEXT, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 'A', 100), (2, 'B', 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (2, 'B_updated', 250), (3, 'C', 300)")
        .unwrap();
}

#[test]
fn test_merge_matched_update() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name, value FROM target WHERE id = 2")
        .unwrap();

    assert_table_eq!(result, [["B_updated", 250]]);
}

#[test]
fn test_merge_not_matched_insert() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target WHERE id = 3")
        .unwrap();

    assert_table_eq!(result, [[3, "C", 300]]);
}

#[test]
fn test_merge_matched_and_not_matched() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target ORDER BY id")
        .unwrap();

    assert_table_eq!(
        result,
        [[1, "A", 100], [2, "B_updated", 250], [3, "C", 300],]
    );
}

#[test]
fn test_merge_matched_delete() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED THEN DELETE",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM target ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_merge_with_condition_on_matched() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED AND s.value > 200 THEN UPDATE SET value = s.value",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target WHERE id = 2")
        .unwrap();

    assert_table_eq!(result, [[2, 250]]);
}

#[test]
fn test_merge_update_and_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INTEGER, name TEXT, active BOOL)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE source (id INTEGER, name TEXT, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 'A', TRUE), (2, 'B', TRUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO source VALUES (1, 'A_updated', TRUE), (2, 'B', FALSE)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED AND s.active = FALSE THEN DELETE
             WHEN MATCHED THEN UPDATE SET name = s.name",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name FROM target ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, "A_updated"]]);
}

#[test]
fn test_merge_with_subquery_source() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE target (id INTEGER, value INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE updates (id INTEGER, delta INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO target VALUES (1, 100), (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO updates VALUES (1, 10), (1, 20), (2, 30)")
        .unwrap();

    executor
        .execute_sql(
            "MERGE INTO target t
             USING (SELECT id, SUM(delta) AS total_delta FROM updates GROUP BY id) s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET value = t.value + s.total_delta",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, 130], [2, 230],]);
}

#[test]
fn test_merge_not_matched_by_source() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM target ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_merge_all_clauses() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
             WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (s.id, s.name, s.value)
             WHEN NOT MATCHED BY SOURCE THEN DELETE",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, name, value FROM target ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2, "B_updated", 250], [3, "C", 300],]);
}

#[test]
fn test_merge_returning() {
    let mut executor = create_executor();
    setup_merge_tables(&mut executor);

    let result = executor
        .execute_sql(
            "MERGE INTO target t
             USING source s ON t.id = s.id
             WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
             RETURNING t.id, t.name",
        )
        .unwrap();

    assert_table_eq!(result, [[2, "B_updated"]]);
}
