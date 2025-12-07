use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_table_ttl_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_basic (
                event_time DateTime,
                data String
            ) ENGINE = MergeTree
            ORDER BY event_time
            TTL event_time + INTERVAL 1 MONTH",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_column_ttl() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_column (
                event_time DateTime,
                name String,
                details String TTL event_time + INTERVAL 1 WEEK
            ) ENGINE = MergeTree
            ORDER BY event_time",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_delete() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_delete (
                ts DateTime,
                data String
            ) ENGINE = MergeTree
            ORDER BY ts
            TTL ts + INTERVAL 30 DAY DELETE",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_to_disk() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_disk (
                created DateTime,
                content String
            ) ENGINE = MergeTree
            ORDER BY created
            TTL created + INTERVAL 7 DAY TO DISK 'cold'",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_to_volume() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_volume (
                timestamp DateTime,
                payload String
            ) ENGINE = MergeTree
            ORDER BY timestamp
            TTL timestamp + INTERVAL 1 MONTH TO VOLUME 'archive'",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_group_by() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_group (
                event_date Date,
                user_id Int64,
                count Int64,
                sum Int64
            ) ENGINE = SummingMergeTree((count, sum))
            ORDER BY (event_date, user_id)
            TTL event_date + INTERVAL 90 DAY GROUP BY event_date
                SET count = sum(count), sum = sum(sum)",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_recompress() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_recompress (
                ts DateTime,
                data String
            ) ENGINE = MergeTree
            ORDER BY ts
            TTL ts + INTERVAL 1 DAY RECOMPRESS CODEC(ZSTD(3)),
                ts + INTERVAL 7 DAY RECOMPRESS CODEC(ZSTD(6))",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_multiple_ttl_rules() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE multi_ttl (
                event_time DateTime,
                level String,
                message String
            ) ENGINE = MergeTree
            ORDER BY event_time
            TTL event_time + INTERVAL 1 WEEK TO DISK 'warm',
                event_time + INTERVAL 1 MONTH TO DISK 'cold',
                event_time + INTERVAL 1 YEAR DELETE",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_where() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_where (
                ts DateTime,
                level String,
                msg String
            ) ENGINE = MergeTree
            ORDER BY ts
            TTL ts + INTERVAL 1 DAY DELETE WHERE level = 'DEBUG',
                ts + INTERVAL 7 DAY DELETE WHERE level = 'INFO',
                ts + INTERVAL 30 DAY DELETE",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_alter_table_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE alter_ttl (
                ts DateTime,
                data String
            ) ENGINE = MergeTree ORDER BY ts",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE alter_ttl MODIFY TTL ts + INTERVAL 14 DAY")
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_alter_column_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE alter_col_ttl (
                ts DateTime,
                temp_data String
            ) ENGINE = MergeTree ORDER BY ts",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "ALTER TABLE alter_col_ttl
            MODIFY COLUMN temp_data String TTL ts + INTERVAL 1 DAY",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_remove_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE remove_ttl (
                ts DateTime,
                data String
            ) ENGINE = MergeTree ORDER BY ts
            TTL ts + INTERVAL 7 DAY",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE remove_ttl REMOVE TTL")
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_materialize_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE mat_ttl (
                ts DateTime,
                data String
            ) ENGINE = MergeTree ORDER BY ts
            TTL ts + INTERVAL 1 DAY",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO mat_ttl VALUES (now(), 'test')")
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE mat_ttl MATERIALIZE TTL")
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_ttl_with_date_expression() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ttl_date_expr (
                date Date,
                hour UInt8,
                data String
            ) ENGINE = MergeTree
            ORDER BY (date, hour)
            TTL toDateTime(date) + INTERVAL hour HOUR + INTERVAL 1 DAY",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_show_table_with_ttl() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE show_ttl (
                ts DateTime,
                data String
            ) ENGINE = MergeTree ORDER BY ts
            TTL ts + INTERVAL 7 DAY",
        )
        .unwrap();

    let result = executor.execute_sql("SHOW CREATE TABLE show_ttl").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
