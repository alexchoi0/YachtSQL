use yachtsql_executor::QueryExecutor;

fn create_test_executor() -> QueryExecutor {
    let executor = QueryExecutor::new();
    {
        let mut storage = executor.storage.borrow_mut();
        let _ = storage.create_dataset("default".to_string());
    }
    executor
}

#[test]
fn test_unique_index_with_duplicate_nulls_in_composite_key() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test (id INT64, val STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_test ON default.test(id, val)")
        .unwrap();

    let result1 = executor.execute_sql("INSERT INTO default.test VALUES (1, NULL)");
    assert!(result1.is_ok(), "First (1, NULL) should succeed");

    let result2 = executor.execute_sql("INSERT INTO default.test VALUES (1, NULL)");
    assert!(
        result2.is_ok(),
        "Second (1, NULL) should succeed: {:?}",
        result2.err()
    );

    executor
        .execute_sql("INSERT INTO default.test VALUES (1, 'A')")
        .unwrap();
    let result3 = executor.execute_sql("INSERT INTO default.test VALUES (1, 'A')");
    assert!(
        result3.is_err(),
        "Duplicate (1, 'A') should fail with unique constraint"
    );
}

#[test]
fn test_all_null_composite_keys_allowed() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test (a STRING, b STRING, c STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_test ON default.test(a, b, c)")
        .unwrap();

    for i in 1..=3 {
        let result = executor.execute_sql("INSERT INTO default.test VALUES (NULL, NULL, NULL)");
        assert!(
            result.is_ok(),
            "All-NULL insert {} should succeed: {:?}",
            i,
            result.err()
        );
    }
}

#[test]
fn test_partial_null_composite_variations() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_test ON default.test(a, b)")
        .unwrap();

    let test_cases = vec![
        ("(NULL, NULL)", "NULL, NULL"),
        ("(NULL, 1)", "NULL, 1"),
        ("(1, NULL)", "1, NULL"),
        ("(NULL, NULL) again", "NULL, NULL"),
        ("(NULL, 1) again", "NULL, 1"),
        ("(1, NULL) again", "1, NULL"),
    ];

    for (description, values) in test_cases {
        let result = executor.execute_sql(&format!("INSERT INTO default.test VALUES ({})", values));
        assert!(
            result.is_ok(),
            "{} should succeed: {:?}",
            description,
            result.err()
        );
    }
}

#[test]
fn test_null_in_btree_vs_hash_index() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test_hash (id INT64, val STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_hash ON default.test_hash USING HASH (val)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test_hash VALUES (1, NULL)")
        .unwrap();
    let result_hash = executor.execute_sql("INSERT INTO default.test_hash VALUES (2, NULL)");
    assert!(
        result_hash.is_ok(),
        "HASH index should allow multiple NULLs"
    );

    executor
        .execute_sql("CREATE TABLE default.test_btree (id INT64, val STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_btree ON default.test_btree USING BTREE (val)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test_btree VALUES (1, NULL)")
        .unwrap();
    let result_btree = executor.execute_sql("INSERT INTO default.test_btree VALUES (2, NULL)");
    assert!(
        result_btree.is_ok(),
        "BTREE index should allow multiple NULLs"
    );
}

#[test]
fn test_mixed_null_and_non_null_unique_violations() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test (id INT64, email STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_email ON default.test(email)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test VALUES (1, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.test VALUES (2, NULL)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test VALUES (3, 'test@example.com')")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test VALUES (4, NULL)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO default.test VALUES (5, 'test@example.com')");
    assert!(
        result.is_err(),
        "Duplicate non-NULL should still fail even with NULLs present"
    );
}

#[test]
fn test_update_to_null_in_unique_index() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test (id INT64, val STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_val ON default.test(val)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test VALUES (1, 'A')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.test VALUES (2, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO default.test VALUES (3, 'B')")
        .unwrap();

    let result = executor.execute_sql("UPDATE default.test SET val = NULL WHERE id = 1");
    assert!(
        result.is_ok(),
        "Updating to NULL should succeed: {:?}",
        result.err()
    );
}

#[test]
fn test_delete_and_reinsert_null() {
    let mut executor = create_test_executor();

    executor
        .execute_sql("CREATE TABLE default.test (id INT64, val STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE UNIQUE INDEX idx_val ON default.test(val)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO default.test VALUES (1, NULL)")
        .unwrap();

    executor
        .execute_sql("DELETE FROM default.test WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO default.test VALUES (2, NULL)");
    assert!(result.is_ok(), "Reinserting NULL should succeed");
}
