use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE base_table (id INT64, name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO base_table VALUES (1, 'Alice', 100), (2, 'Bob', 200)")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE snapshot_table CLONE base_table")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_if_not_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE source_table (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source_table VALUES (1), (2)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE snap1 CLONE source_table")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE IF NOT EXISTS snap1 CLONE source_table")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_with_expiration() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE expiring_source (id INT64, data STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO expiring_source VALUES (1, 'test')")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE SNAPSHOT TABLE expiring_snapshot
        CLONE expiring_source
        OPTIONS(expiration_timestamp=TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_snapshot_for_system_time() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE time_travel_source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO time_travel_source VALUES (1, 100)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE SNAPSHOT TABLE time_snapshot
        CLONE time_travel_source
        FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_table_query() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE original (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO original VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE snapshot_copy CLONE original")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM snapshot_copy ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"], ["Bob"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_isolation() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE isolation_test (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO isolation_test VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE isolated_snap CLONE isolation_test")
        .await
        .unwrap();

    session
        .execute_sql("UPDATE isolation_test SET value = 999 WHERE id = 1")
        .await
        .unwrap();

    let snap_result = session
        .execute_sql("SELECT value FROM isolated_snap WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(snap_result, [[100]]);

    let original_result = session
        .execute_sql("SELECT value FROM isolation_test WHERE id = 1")
        .await
        .unwrap();
    assert_table_eq!(original_result, [[999]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_snapshot_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE drop_source (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO drop_source VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE drop_snap CLONE drop_source")
        .await
        .unwrap();

    let result = session.execute_sql("DROP SNAPSHOT TABLE drop_snap").await;
    assert!(result.is_ok());

    let query_result = session.execute_sql("SELECT * FROM drop_snap").await;
    assert!(query_result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_snapshot_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP SNAPSHOT TABLE IF EXISTS nonexistent_snapshot")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_with_description() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE desc_source (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO desc_source VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE SNAPSHOT TABLE desc_snapshot
        CLONE desc_source
        OPTIONS(description='Daily backup snapshot')",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_with_labels() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE label_source (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO label_source VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "CREATE SNAPSHOT TABLE label_snapshot
        CLONE label_source
        OPTIONS(labels=[('env', 'prod'), ('team', 'analytics')]",
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_restore_from_snapshot() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE restore_source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO restore_source VALUES (1, 100), (2, 200)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SNAPSHOT TABLE restore_snap CLONE restore_source")
        .await
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE restore_source")
        .await
        .unwrap();

    let result = session
        .execute_sql("INSERT INTO restore_source SELECT * FROM restore_snap")
        .await;
    assert!(result.is_ok());

    let count = session
        .execute_sql("SELECT COUNT(*) FROM restore_source")
        .await
        .unwrap();
    assert_table_eq!(count, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_complex_table() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE complex_source (
                id INT64,
                data STRUCT<name STRING, values ARRAY<INT64>>,
                created_at TIMESTAMP
            )",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO complex_source VALUES
            (1, STRUCT('test', [1, 2, 3]), CURRENT_TIMESTAMP())",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE complex_snap CLONE complex_source")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_partitioned_table() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE partitioned_source (
                id INT64,
                dt DATE,
                value INT64
            ) PARTITION BY dt",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO partitioned_source VALUES
            (1, DATE '2024-01-01', 100),
            (2, DATE '2024-01-02', 200)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE partitioned_snap CLONE partitioned_source")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_snapshot_clustered_table() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE clustered_source (
                id INT64,
                category STRING,
                value INT64
            ) CLUSTER BY category",
        )
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO clustered_source VALUES
            (1, 'A', 100),
            (2, 'B', 200)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SNAPSHOT TABLE clustered_snap CLONE clustered_source")
        .await;
    assert!(result.is_ok());
}
