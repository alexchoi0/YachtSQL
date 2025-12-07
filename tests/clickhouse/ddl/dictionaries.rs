use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_create_dictionary_flat() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY flat_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 1000)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_dictionary_hashed() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY hashed_dict (
                id UInt64,
                value String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(HASHED())
            LIFETIME(300)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_dictionary_cache() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY cache_dict (
                id UInt64,
                data String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(CACHE(SIZE_IN_CELLS 10000))
            LIFETIME(MIN 60 MAX 120)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_dictionary_complex_key() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY complex_dict (
                key1 String,
                key2 UInt64,
                value String
            )
            PRIMARY KEY key1, key2
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(COMPLEX_KEY_HASHED())
            LIFETIME(300)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_dictionary_range_hashed() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY range_dict (
                id UInt64,
                start_date Date,
                end_date Date,
                value String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(RANGE_HASHED())
            RANGE(MIN start_date MAX end_date)
            LIFETIME(300)",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_dictionary() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY drop_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();
    executor.execute_sql("DROP DICTIONARY drop_dict").unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_dictionary_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP DICTIONARY IF EXISTS nonexistent_dict")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_dictionary_get() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dict_source (id UInt64, name String) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dict_source VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY get_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGet('get_dict', 'name', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_dictionary_get_or_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dict_source2 (id UInt64, value Int64) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dict_source2 VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY default_dict (
                id UInt64,
                value Int64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source2'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetOrDefault('default_dict', 'value', toUInt64(999), toInt64(-1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_dictionary_has() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE dict_source3 (id UInt64, name String) ENGINE = Memory")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dict_source3 VALUES (1, 'test')")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY has_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source3'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictHas('has_dict', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_dictionary_hierarchy() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE hier_source (id UInt64, parent_id UInt64, name String) ENGINE = Memory",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO hier_source VALUES (1, 0, 'Root'), (2, 1, 'Child')")
        .unwrap();
    executor
        .execute_sql(
            "CREATE DICTIONARY hier_dict (
                id UInt64,
                parent_id UInt64 HIERARCHICAL,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'hier_source'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetHierarchy('hier_dict', toUInt64(2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_show_dictionaries() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY show_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'source_table'))
            LAYOUT(FLAT())
            LIFETIME(300)",
        )
        .unwrap();

    let result = executor.execute_sql("SHOW DICTIONARIES").unwrap();
    assert!(result.num_rows() >= 1);
}
