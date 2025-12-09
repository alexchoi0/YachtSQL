use crate::common::create_executor;

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY test_dict (
                id UInt64,
                name String,
                value Float64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source'))
            LAYOUT(FLAT())
            LIFETIME(MIN 0 MAX 1000)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGet('test_dict', 'name', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_or_default() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY default_dict (
                id UInt64,
                value Int64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source2'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetOrDefault('default_dict', 'value', toUInt64(999), toInt64(-1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_or_null() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY null_dict (
                id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source3'))
            LAYOUT(HASHED())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetOrNull('null_dict', 'name', toUInt64(999))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_has() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY has_dict (
                id UInt64,
                data String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source4'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictHas('has_dict', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_uint64() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY uint_dict (
                id UInt64,
                count UInt64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source5'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetUInt64('uint_dict', 'count', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_int64() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY int_dict (
                id UInt64,
                amount Int64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source6'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetInt64('int_dict', 'amount', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_float64() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY float_dict (
                id UInt64,
                price Float64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source7'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetFloat64('float_dict', 'price', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_string() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY string_dict (
                id UInt64,
                label String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source8'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetString('string_dict', 'label', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_date() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY date_dict (
                id UInt64,
                event_date Date
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source9'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetDate('date_dict', 'event_date', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_date_time() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY datetime_dict (
                id UInt64,
                created_at DateTime
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source10'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetDateTime('datetime_dict', 'created_at', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_uuid() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY uuid_dict (
                id UInt64,
                external_id UUID
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source11'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetUUID('uuid_dict', 'external_id', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_hierarchy() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY hierarchy_dict (
                id UInt64,
                parent_id UInt64,
                name String
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source12'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetHierarchy('hierarchy_dict', toUInt64(5))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_is_in() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY isin_dict (
                id UInt64,
                parent_id UInt64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source13'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictIsIn('isin_dict', toUInt64(5), toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_children() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY children_dict (
                id UInt64,
                parent_id UInt64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source14'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetChildren('children_dict', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_descendant() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY desc_dict (
                id UInt64,
                parent_id UInt64
            )
            PRIMARY KEY id
            SOURCE(CLICKHOUSE(TABLE 'dict_source15'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetDescendants('desc_dict', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Requires CREATE DICTIONARY DDL support"]
#[test]
fn test_dict_get_all() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE DICTIONARY all_dict (
                id UInt64,
                tags Array(String)
            )
            PRIMARY KEY id
                SOURCE(CLICKHOUSE(TABLE 'dict_source16'))
            LAYOUT(FLAT())
            LIFETIME(0)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT dictGetAll('all_dict', 'tags', toUInt64(1))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
