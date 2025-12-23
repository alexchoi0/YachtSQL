use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_create_snapshot_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE base_table (id INT64, name STRING, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
        .unwrap();

    let result = session.execute_sql("CREATE SNAPSHOT TABLE snapshot_table CLONE base_table");
    assert!(result.is_ok());
}

#[test]
fn test_create_snapshot_if_not_exists() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE source_table (id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO source_table VALUES (1), (2)")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE snap1 CLONE source_table")
        .unwrap();

    let result =
        session.execute_sql("CREATE SNAPSHOT TABLE IF NOT EXISTS snap1 CLONE source_table");
    assert!(result.is_ok());
}

#[test]
fn test_create_snapshot_with_expiration() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE expiring_source (id INT64, data STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO expiring_source VALUES (1, 'test')")
        .unwrap();

    let result = session.execute_sql(
        "CREATE SNAPSHOT TABLE expiring_snapshot
        CLONE expiring_source
        OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))",
    );
    assert!(result.is_ok());
}

#[test]
fn test_create_snapshot_for_system_time() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE time_travel_source (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO time_travel_source VALUES (1, 100)")
        .unwrap();

    let result = session.execute_sql(
        "CREATE SNAPSHOT TABLE time_snapshot
        CLONE time_travel_source
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
    );
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_table_query() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE original (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO original VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE snapshot_copy CLONE original")
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM snapshot_copy ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[test]
fn test_snapshot_isolation() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE isolation_test (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO isolation_test VALUES (1, 100), (2, 200)")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE isolated_snap CLONE isolation_test")
        .unwrap();

    session
        .execute_sql("UPDATE isolation_test SET value = 999 WHERE id = 1")
        .unwrap();

    let snap_result = session
        .execute_sql("SELECT value FROM isolated_snap WHERE id = 1")
        .unwrap();
    assert_table_eq!(snap_result, [[100]]);

    let original_result = session
        .execute_sql("SELECT value FROM isolation_test WHERE id = 1")
        .unwrap();
    assert_table_eq!(original_result, [[999]]);
}

#[test]
fn test_drop_snapshot_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE drop_source (id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO drop_source VALUES (1)")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE drop_snap CLONE drop_source")
        .unwrap();

    let result = session.execute_sql("DROP SNAPSHOT TABLE drop_snap");
    assert!(result.is_ok());

    let query_result = session.execute_sql("SELECT * FROM drop_snap");
    assert!(query_result.is_err());
}

#[test]
fn test_drop_snapshot_if_exists() {
    let mut session = create_session();

    let result = session.execute_sql("DROP SNAPSHOT TABLE IF EXISTS nonexistent_snapshot");
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_with_description() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE desc_source (id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO desc_source VALUES (1)")
        .unwrap();

    let result = session.execute_sql(
        "CREATE SNAPSHOT TABLE desc_snapshot
        CLONE desc_source
        OPTIONS(description='Daily backup snapshot')",
    );
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_with_labels() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE label_source (id INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO label_source VALUES (1)")
        .unwrap();

    let result = session.execute_sql(
        "CREATE SNAPSHOT TABLE label_snapshot
        CLONE label_source
        OPTIONS(labels=[('env', 'prod'), ('team', 'analytics')]",
    );
    assert!(result.is_ok());
}

#[test]
fn test_restore_from_snapshot() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE restore_source (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO restore_source VALUES (1, 100), (2, 200)")
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE restore_snap CLONE restore_source")
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE restore_source")
        .unwrap();

    let result = session.execute_sql("INSERT INTO restore_source SELECT * FROM restore_snap");
    assert!(result.is_ok());

    let count = session
        .execute_sql("SELECT COUNT(*) FROM restore_source")
        .unwrap();
    assert_table_eq!(count, [[2]]);
}

#[test]
fn test_snapshot_complex_table() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE complex_source (
                id INT64,
                data STRUCT<name STRING, values ARRAY<INT64>>,
                created_at TIMESTAMP
            )",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO complex_source VALUES
            (1, STRUCT('test', [1, 2, 3]), CURRENT_TIMESTAMP())",
        )
        .unwrap();

    let result = session.execute_sql("CREATE SNAPSHOT TABLE complex_snap CLONE complex_source");
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_partitioned_table() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE partitioned_source (
                id INT64,
                dt DATE,
                value INT64
            ) PARTITION BY dt",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO partitioned_source VALUES
            (1, DATE '2024-01-01', 100),
            (2, DATE '2024-01-02', 200)",
        )
        .unwrap();

    let result =
        session.execute_sql("CREATE SNAPSHOT TABLE partitioned_snap CLONE partitioned_source");
    assert!(result.is_ok());
}

#[test]
fn test_snapshot_clustered_table() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE clustered_source (
                id INT64,
                category STRING,
                value INT64
            ) CLUSTER BY category",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO clustered_source VALUES
            (1, 'A', 100),
            (2, 'B', 200)",
        )
        .unwrap();

    let result = session.execute_sql("CREATE SNAPSHOT TABLE clustered_snap CLONE clustered_source");
    assert!(result.is_ok());
}
