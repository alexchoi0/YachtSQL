use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_lz4_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE lz4_test (
                id UInt64,
                data String CODEC(LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_lz4hc_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE lz4hc_test (
                id UInt64,
                data String CODEC(LZ4HC)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_lz4hc_level_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE lz4hc_level_test (
                id UInt64,
                data String CODEC(LZ4HC(9))
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_zstd_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE zstd_test (
                id UInt64,
                data String CODEC(ZSTD)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_zstd_level_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE zstd_level_test (
                id UInt64,
                data String CODEC(ZSTD(5))
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_delta_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE delta_test (
                id UInt64,
                timestamp DateTime CODEC(Delta, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_delta_bytes_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE delta_bytes_test (
                id UInt64,
                value UInt32 CODEC(Delta(4), ZSTD)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_double_delta_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE ddelta_test (
                id UInt64,
                ts DateTime CODEC(DoubleDelta, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_gorilla_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE gorilla_test (
                id UInt64,
                value Float64 CODEC(Gorilla, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_fpc_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE fpc_test (
                id UInt64,
                value Float64 CODEC(FPC, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_t64_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE t64_test (
                id UInt64,
                value Int64 CODEC(T64, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_none_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE none_codec_test (
                id UInt64,
                data String CODEC(NONE)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_multiple_columns_different_codecs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE multi_codec (
                id UInt64 CODEC(Delta, LZ4),
                timestamp DateTime CODEC(DoubleDelta, ZSTD),
                value Float64 CODEC(Gorilla, LZ4),
                description String CODEC(ZSTD(3))
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_codec_chain() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE codec_chain_test (
                id UInt64,
                ts DateTime64(3) CODEC(Delta, Delta, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_default_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE default_codec_test (
                id UInt64,
                data String CODEC(Default)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_alter_column_codec() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE alter_codec_test (
                id UInt64,
                data String
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    let result = executor
        .execute_sql("ALTER TABLE alter_codec_test MODIFY COLUMN data CODEC(ZSTD)")
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_codec_with_nullable() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE nullable_codec (
                id UInt64,
                value Nullable(Int64) CODEC(Delta, LZ4)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_codec_with_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE array_codec (
                id UInt64,
                values Array(UInt32) CODEC(ZSTD)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_codec_insert_select() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE codec_data (
                id UInt64 CODEC(Delta, LZ4),
                value Int64 CODEC(T64, ZSTD),
                timestamp DateTime CODEC(DoubleDelta)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO codec_data VALUES
            (1, 100, '2023-01-01 00:00:00'),
            (2, 200, '2023-01-01 00:00:01'),
            (3, 300, '2023-01-01 00:00:02')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value FROM codec_data ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200], [3, 300]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_aes_encryption_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE aes_codec_test (
                id UInt64,
                secret String CODEC(AES_128_GCM_SIV)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}

#[ignore = "Implement me!"]
#[test]
fn test_aes_256_codec() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "CREATE TABLE aes256_codec_test (
                id UInt64,
                secret String CODEC(AES_256_GCM_SIV)
            ) ENGINE = MergeTree()
            ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, []);
}
