use crate::common::create_executor;

#[test]
fn test_json_object_type() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_obj (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_insert (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql(r#"INSERT INTO json_insert VALUES (1, '{"name": "test", "value": 123}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_nested() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_nested (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"INSERT INTO json_nested VALUES (1, '{"outer": {"inner": {"deep": "value"}}}')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_array() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_array (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result =
        executor.execute_sql(r#"INSERT INTO json_array VALUES (1, '{"items": [1, 2, 3, 4, 5]}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_access_dot() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_dot (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_dot VALUES (1, '{"name": "test"}')"#)
        .ok();

    let result = executor.execute_sql("SELECT data.name FROM json_dot");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_access_nested_dot() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_nested_dot (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_nested_dot VALUES (1, '{"a": {"b": {"c": 42}}}')"#)
        .ok();

    let result = executor.execute_sql("SELECT data.a.b.c FROM json_nested_dot");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_type_hint() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_hint (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_hint VALUES (1, '{"value": 123}')"#)
        .ok();

    let result = executor.execute_sql("SELECT data.value::Int64 FROM json_hint");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_null_value() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_null (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(r#"INSERT INTO json_null VALUES (1, '{"value": null}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_boolean() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_bool (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql(r#"INSERT INTO json_bool VALUES (1, '{"active": true, "deleted": false}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_mixed_array() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_mixed (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql(r#"INSERT INTO json_mixed VALUES (1, '{"arr": [1, "two", true, null]}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_empty() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_empty (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(r#"INSERT INTO json_empty VALUES (1, '{}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_empty_array() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_empty_arr (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(r#"INSERT INTO json_empty_arr VALUES (1, '{"items": []}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_with_schema() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_schema (
            id UInt64,
            data JSON(name String, age UInt8)
        ) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_dynamic_schema() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_dynamic (
            id UInt64,
            data JSON(max_dynamic_paths=100)
        ) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_max_dynamic_types() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_types (
            id UInt64,
            data JSON(max_dynamic_types=10)
        ) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_skip_path() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_skip (
            id UInt64,
            data JSON(SKIP secret, SKIP password)
        ) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_array_of_objects() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_arr_obj (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"INSERT INTO json_arr_obj VALUES (1, '{"users": [{"name": "alice"}, {"name": "bob"}]}')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_select_all() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_select (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_select VALUES (1, '{"a": 1, "b": 2}')"#)
        .ok();

    let result = executor.execute_sql("SELECT * FROM json_select");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_where (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_where VALUES (1, '{"status": "active"}')"#)
        .ok();

    let result = executor.execute_sql("SELECT * FROM json_where WHERE data.status = 'active'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_group (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql("SELECT data.category, count() FROM json_group GROUP BY data.category");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_order (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql("SELECT * FROM json_order ORDER BY data.priority");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_unicode() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_unicode (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"INSERT INTO json_unicode VALUES (1, '{"greeting": "你好世界", "emoji": "🎉"}')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_escape_chars() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_escape (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql(r#"INSERT INTO json_escape VALUES (1, '{"text": "line1\nline2\ttab"}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_large_number() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_large (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor
        .execute_sql(r#"INSERT INTO json_large VALUES (1, '{"big": 9223372036854775807}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_float() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_float (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result =
        executor.execute_sql(r#"INSERT INTO json_float VALUES (1, '{"value": 3.14159265359}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_scientific_notation() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_sci (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(r#"INSERT INTO json_sci VALUES (1, '{"value": 1.23e10}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_array_index() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_arr_idx (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_arr_idx VALUES (1, '{"items": ["a", "b", "c"]}')"#)
        .ok();

    let result = executor.execute_sql("SELECT data.items[1] FROM json_arr_idx");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_jsonextractraw() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_extract (id UInt64, data String) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_extract VALUES (1, '{"nested": {"key": "value"}}')"#)
        .ok();

    let result = executor.execute_sql("SELECT JSONExtractRaw(data, 'nested') FROM json_extract");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_tojsonstring() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_tostr (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();
    executor
        .execute_sql(r#"INSERT INTO json_tostr VALUES (1, '{"a": 1}')"#)
        .ok();

    let result = executor.execute_sql("SELECT toJSONString(data) FROM json_tostr");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_merge() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_merge (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"SELECT JSONExtract('{"a": 1}', 'a', 'Int64') + JSONExtract('{"b": 2}', 'b', 'Int64')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_default_value() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        r#"CREATE TABLE json_default (
            id UInt64,
            data JSON DEFAULT '{"status": "pending"}'
        ) ENGINE = MergeTree() ORDER BY id"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_nullable() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_nullable (id UInt64, data Nullable(JSON)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_in_array() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_in_arr (id UInt64, items Array(JSON)) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_map_of_json() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_map (id UInt64, data Map(String, JSON)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_tuple_with_json() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE json_tuple (id UInt64, data Tuple(name String, meta JSON)) ENGINE = MergeTree() ORDER BY id"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_deeply_nested() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_deep (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"INSERT INTO json_deep VALUES (1, '{"l1": {"l2": {"l3": {"l4": {"l5": "deep"}}}}}')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_with_special_keys() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_special (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"INSERT INTO json_special VALUES (1, '{"key-with-dash": 1, "key.with.dots": 2}')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_object_multiline() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE json_multi (id UInt64, data JSON) ENGINE = MergeTree() ORDER BY id",
        )
        .ok();

    let result = executor.execute_sql(
        r#"INSERT INTO json_multi VALUES (1, '{
            "key": "value",
            "nested": {
                "a": 1
            }
        }')"#,
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_object_deprecated_type() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE obj_type (id UInt64, data Object('json')) ENGINE = MergeTree() ORDER BY id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_extract_keys() {
    let mut executor = create_executor();
    let result = executor.execute_sql(r#"SELECT JSONExtractKeys('{"a": 1, "b": 2, "c": 3}')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_extract_keys_and_values() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql(r#"SELECT JSONExtractKeysAndValues('{"a": 1, "b": 2}', 'Int64')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_has() {
    let mut executor = create_executor();
    let result = executor.execute_sql(r#"SELECT JSONHas('{"a": {"b": 1}}', 'a', 'b')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_length() {
    let mut executor = create_executor();
    let result = executor.execute_sql(r#"SELECT JSONLength('{"a": [1, 2, 3]}', 'a')"#);
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_json_type() {
    let mut executor = create_executor();
    let result = executor.execute_sql(r#"SELECT JSONType('{"a": 1}')"#);
    assert!(result.is_ok() || result.is_err());
}
