use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_detach_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE detach_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO detach_part VALUES (1, '2023-01-15'), (2, '2023-02-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE detach_part DETACH PARTITION '202301'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM detach_part")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_attach_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE attach_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO attach_part VALUES (1, '2023-01-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE attach_part DETACH PARTITION '202301'")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE attach_part ATTACH PARTITION '202301'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM attach_part")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE drop_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO drop_part VALUES (1, '2023-01-15'), (2, '2023-02-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE drop_part DROP PARTITION '202301'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM drop_part")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_freeze_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE freeze_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO freeze_part VALUES (1, '2023-01-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE freeze_part FREEZE PARTITION '202301'")
        .unwrap();
}

#[test]
fn test_unfreeze_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE unfreeze_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO unfreeze_part VALUES (1, '2023-01-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE unfreeze_part FREEZE PARTITION '202301'")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE unfreeze_part UNFREEZE PARTITION '202301'")
        .unwrap();
}

#[test]
fn test_move_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE move_src (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE move_dst (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO move_src VALUES (1, '2023-01-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE move_src MOVE PARTITION '202301' TO TABLE move_dst")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM move_dst")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_replace_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE replace_src (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE replace_dst (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO replace_src VALUES (1, '2023-01-15')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO replace_dst VALUES (2, '2023-01-20')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE replace_dst REPLACE PARTITION '202301' FROM replace_src")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM replace_dst").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_clear_column_in_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE clear_col_part (
                id INT64,
                dt Date,
                value Nullable(Int64)
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO clear_col_part VALUES (1, '2023-01-15', 100)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE clear_col_part CLEAR COLUMN value IN PARTITION '202301'")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM clear_col_part WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_fetch_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE fetch_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("ALTER TABLE fetch_part FETCH PARTITION '202301' FROM '/path/to/replica'")
        .unwrap();
}

#[test]
fn test_update_in_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE update_part (
                id INT64,
                dt Date,
                value Int64
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO update_part VALUES (1, '2023-01-15', 100), (2, '2023-02-15', 200)",
        )
        .unwrap();
    executor
        .execute_sql(
            "ALTER TABLE update_part UPDATE value = 150 IN PARTITION '202301' WHERE id = 1",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM update_part WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[150]]);
}

#[test]
fn test_delete_in_partition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE delete_part (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO delete_part VALUES (1, '2023-01-15'), (2, '2023-01-20'), (3, '2023-02-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE delete_part DELETE IN PARTITION '202301' WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) FROM delete_part WHERE dt >= '2023-01-01' AND dt < '2023-02-01'",
        )
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_attach_part() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE attach_part_test (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("ALTER TABLE attach_part_test ATTACH PART 'all_1_1_0'")
        .unwrap();
}

#[test]
fn test_detach_part() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE detach_part_test (
                id INT64,
                dt Date
            ) ENGINE = MergeTree
            PARTITION BY toYYYYMM(dt)
            ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO detach_part_test VALUES (1, '2023-01-15')")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE detach_part_test DETACH PART 'all_1_1_0'")
        .unwrap();
}
