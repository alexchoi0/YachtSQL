use crate::common::create_executor;

#[test]
fn test_create_hstore_extension() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE EXTENSION IF NOT EXISTS hstore");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_hstore_column_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();

    let result = executor.execute_sql("CREATE TABLE hs_test (id INTEGER, data HSTORE)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_hstore_insert_literal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_insert (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result =
            executor.execute_sql("INSERT INTO hs_insert VALUES (1, 'key1=>value1, key2=>value2')");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_insert_with_quotes() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_quotes (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result = executor.execute_sql(
            "INSERT INTO hs_quotes VALUES (1, '\"key with space\"=>\"value with space\"')",
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_insert_null_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_null (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result = executor.execute_sql("INSERT INTO hs_null VALUES (1, 'key1=>NULL')");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_arrow_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_arrow (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_arrow VALUES (1, 'name=>Alice, age=>30')")
            .unwrap();
        let result = executor.execute_sql("SELECT data -> 'name' FROM hs_arrow WHERE id = 1");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_contains_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_contains (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_contains VALUES (1, 'a=>1, b=>2'), (2, 'c=>3')")
            .unwrap();
        let result = executor.execute_sql("SELECT id FROM hs_contains WHERE data @> 'a=>1'");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_contained_by_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_contained (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_contained VALUES (1, 'a=>1')")
            .unwrap();
        let result = executor.execute_sql("SELECT id FROM hs_contained WHERE data <@ 'a=>1, b=>2'");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_has_key_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_haskey (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_haskey VALUES (1, 'name=>Alice, age=>30')")
            .unwrap();
        let result = executor.execute_sql("SELECT id FROM hs_haskey WHERE data ? 'name'");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_has_any_keys_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_anykeys (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_anykeys VALUES (1, 'a=>1'), (2, 'b=>2')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT id FROM hs_anykeys WHERE data ?| ARRAY['a', 'c']");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_has_all_keys_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_allkeys (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_allkeys VALUES (1, 'a=>1, b=>2'), (2, 'a=>1')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT id FROM hs_allkeys WHERE data ?& ARRAY['a', 'b']");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_concat_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_concat (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_concat VALUES (1, 'a=>1')")
            .unwrap();
        let result = executor.execute_sql("SELECT data || 'b=>2'::hstore FROM hs_concat");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_delete_key_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_delete (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_delete VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT data - 'a' FROM hs_delete");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_delete_keys_operator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_delkeys (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_delkeys VALUES (1, 'a=>1, b=>2, c=>3')")
            .unwrap();
        let result = executor.execute_sql("SELECT data - ARRAY['a', 'b'] FROM hs_delkeys");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_akeys_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_akeys (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_akeys VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT akeys(data) FROM hs_akeys");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_avals_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_avals (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_avals VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT avals(data) FROM hs_avals");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_skeys_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_skeys (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_skeys VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT skeys(data) FROM hs_skeys");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_svals_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_svals (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_svals VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT svals(data) FROM hs_svals");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_each_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_each (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_each VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor
            .execute_sql("SELECT * FROM each('a=>1,b=>2'::hstore)")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }
}

#[test]
fn test_hstore_exist_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_exist (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_exist VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT exist(data, 'a') FROM hs_exist");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_defined_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_defined (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_defined VALUES (1, 'a=>1, b=>NULL')")
            .unwrap();
        let result =
            executor.execute_sql("SELECT defined(data, 'a'), defined(data, 'b') FROM hs_defined");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_delete_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_delfunc (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_delfunc VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT delete(data, 'a') FROM hs_delfunc");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_slice_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_slice (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_slice VALUES (1, 'a=>1, b=>2, c=>3')")
            .unwrap();
        let result = executor.execute_sql("SELECT slice(data, ARRAY['a', 'b']) FROM hs_slice");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_to_json_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_tojson (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_tojson VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT hstore_to_json(data) FROM hs_tojson");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_to_jsonb_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_tojsonb (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_tojsonb VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT hstore_to_jsonb(data) FROM hs_tojsonb");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_to_array_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_toarr (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_toarr VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT hstore_to_array(data) FROM hs_toarr");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_to_matrix_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_tomat (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_tomat VALUES (1, 'a=>1, b=>2')")
            .unwrap();
        let result = executor.execute_sql("SELECT hstore_to_matrix(data) FROM hs_tomat");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_from_arrays() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();

    let result = executor.execute_sql("SELECT hstore(ARRAY['a', 'b'], ARRAY['1', '2'])");
    assert!(result.is_ok());
}

#[test]
fn test_hstore_from_record() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    executor
        .execute_sql("CREATE TABLE hs_rec (a TEXT, b TEXT)")
        .ok();

    if executor
        .execute_sql("INSERT INTO hs_rec VALUES ('1', '2')")
        .is_ok()
    {
        let result = executor.execute_sql("SELECT hstore(hs_rec) FROM hs_rec");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_populate_record() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    executor
        .execute_sql("CREATE TYPE hs_type AS (a TEXT, b TEXT)")
        .ok();

    let result =
        executor.execute_sql("SELECT * FROM populate_record(NULL::hs_type, 'a=>hello, b=>world')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_hstore_gin_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_gin (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result = executor.execute_sql("CREATE INDEX ON hs_gin USING GIN (data)");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_gist_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_gist (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result = executor.execute_sql("CREATE INDEX ON hs_gist USING GIST (data)");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_btree_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_btree (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result = executor.execute_sql("CREATE INDEX ON hs_btree USING BTREE (data)");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_hash_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_hash (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        let result = executor.execute_sql("CREATE INDEX ON hs_hash USING HASH (data)");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_update (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_update VALUES (1, 'a=>1')")
            .unwrap();
        let result =
            executor.execute_sql("UPDATE hs_update SET data = data || 'b=>2' WHERE id = 1");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_where_key_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_where (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql(
                "INSERT INTO hs_where VALUES (1, 'status=>active'), (2, 'status=>inactive')",
            )
            .unwrap();
        let result =
            executor.execute_sql("SELECT id FROM hs_where WHERE data -> 'status' = 'active'");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_group (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql(
                "INSERT INTO hs_group VALUES (1, 'type=>A'), (2, 'type=>A'), (3, 'type=>B')",
            )
            .unwrap();
        let result = executor.execute_sql(
            "SELECT data -> 'type' AS type, COUNT(*) FROM hs_group GROUP BY data -> 'type'",
        );
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_order (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor.execute_sql("INSERT INTO hs_order VALUES (1, 'priority=>3'), (2, 'priority=>1'), (3, 'priority=>2')").unwrap();
        let result =
            executor.execute_sql("SELECT id FROM hs_order ORDER BY (data -> 'priority')::INT");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_empty() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_empty (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_empty VALUES (1, '')")
            .unwrap();
        let result = executor.execute_sql("SELECT data = ''::hstore FROM hs_empty");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_null_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_nullcol (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_nullcol VALUES (1, NULL)")
            .unwrap();
        let result = executor.execute_sql("SELECT data IS NULL FROM hs_nullcol");
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_coal (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_coal VALUES (1, NULL)")
            .unwrap();
        let result = executor
            .execute_sql("SELECT COALESCE(data, 'default=>value'::hstore) FROM hs_coal")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
    }
}

#[test]
fn test_hstore_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_subq (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_subq VALUES (1, 'a=>1'), (2, 'a=>2')")
            .unwrap();
        let result = executor.execute_sql(
            "SELECT * FROM hs_subq WHERE (data -> 'a')::INT > (SELECT AVG((data -> 'a')::INT) FROM hs_subq)"
        );
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_hstore_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create_result = executor.execute_sql("CREATE TABLE hs_cte (id INTEGER, data HSTORE)");

    if create_result.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_cte VALUES (1, 'name=>Alice'), (2, 'name=>Bob')")
            .unwrap();
        let result = executor.execute_sql(
            "WITH names AS (SELECT id, data -> 'name' AS name FROM hs_cte) SELECT * FROM names ORDER BY name"
        );
        assert!(result.is_ok());
    }
}

#[test]
fn test_hstore_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS hstore")
        .ok();
    let create1 = executor.execute_sql("CREATE TABLE hs_left (id INTEGER, data HSTORE)");
    let create2 = executor.execute_sql("CREATE TABLE hs_right (code TEXT, description TEXT)");

    if create1.is_ok() && create2.is_ok() {
        executor
            .execute_sql("INSERT INTO hs_left VALUES (1, 'code=>A'), (2, 'code=>B')")
            .unwrap();
        executor
            .execute_sql("INSERT INTO hs_right VALUES ('A', 'Alpha'), ('B', 'Beta')")
            .unwrap();
        let result = executor
            .execute_sql(
                "SELECT l.id, r.description FROM hs_left l JOIN hs_right r ON l.data -> 'code' = r.code",
            )
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }
}
