use yachtsql::DialectType;
use yachtsql_executor::QueryExecutor;

#[test]
fn test_create_table_with_single_column_primary_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        name STRING,
        PRIMARY KEY (id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some(), "PRIMARY KEY should be set");
    assert_eq!(
        primary_key.unwrap(),
        &["id"],
        "PRIMARY KEY should be on 'id' column"
    );
}

#[test]
fn test_create_table_with_multi_column_primary_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE order_items (
        order_id INT64,
        item_id INT64,
        quantity INT64,
        PRIMARY KEY (order_id, item_id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset
        .get_table("order_items")
        .expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some(), "PRIMARY KEY should be set");
    assert_eq!(
        primary_key.unwrap(),
        &["order_id", "item_id"],
        "PRIMARY KEY should be on both columns"
    );
}

#[test]
fn test_create_table_with_single_column_unique() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        email STRING,
        UNIQUE (email)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(
        unique_constraints.len(),
        1,
        "Should have 1 UNIQUE constraint"
    );
    assert_eq!(
        unique_constraints[0].columns,
        vec!["email"],
        "UNIQUE should be on 'email' column"
    );
}

#[test]
fn test_create_table_with_multi_column_unique() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        first_name STRING,
        last_name STRING,
        email STRING,
        UNIQUE (first_name, last_name)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(
        unique_constraints.len(),
        1,
        "Should have 1 UNIQUE constraint"
    );
    assert_eq!(
        unique_constraints[0].columns,
        vec!["first_name", "last_name"],
        "UNIQUE should be on both columns"
    );
}

#[test]
fn test_create_table_with_multiple_unique_constraints() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        email STRING,
        username STRING,
        UNIQUE (email),
        UNIQUE (username)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(
        unique_constraints.len(),
        2,
        "Should have 2 UNIQUE constraints"
    );
    assert_eq!(
        unique_constraints[0].columns,
        vec!["email"],
        "First UNIQUE should be on 'email'"
    );
    assert_eq!(
        unique_constraints[1].columns,
        vec!["username"],
        "Second UNIQUE should be on 'username'"
    );
}

#[test]
fn test_create_table_with_primary_key_and_unique() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        email STRING,
        username STRING,
        PRIMARY KEY (id),
        UNIQUE (email),
        UNIQUE (username)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some(), "PRIMARY KEY should be set");
    assert_eq!(
        primary_key.unwrap(),
        &["id"],
        "PRIMARY KEY should be on 'id' column"
    );

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(
        unique_constraints.len(),
        2,
        "Should have 2 UNIQUE constraints"
    );
    assert_eq!(
        unique_constraints[0].columns,
        vec!["email"],
        "First UNIQUE should be on 'email'"
    );
    assert_eq!(
        unique_constraints[1].columns,
        vec!["username"],
        "Second UNIQUE should be on 'username'"
    );
}

#[test]
fn test_create_table_primary_key_nonexistent_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        name STRING,
        PRIMARY KEY (nonexistent_column)
    )";

    let result = executor.execute_sql(sql);
    assert!(
        result.is_err(),
        "CREATE TABLE should fail with nonexistent column"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("PRIMARY KEY column") && err_msg.contains("does not exist"),
        "Error should mention PRIMARY KEY column doesn't exist, got: {}",
        err_msg
    );
}

#[test]
fn test_create_table_unique_nonexistent_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        name STRING,
        UNIQUE (nonexistent_column)
    )";

    let result = executor.execute_sql(sql);
    assert!(
        result.is_err(),
        "CREATE TABLE should fail with nonexistent column"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("UNIQUE constraint column") && err_msg.contains("does not exist"),
        "Error should mention UNIQUE constraint column doesn't exist, got: {}",
        err_msg
    );
}

#[test]
fn test_create_table_multi_column_primary_key_partial_invalid() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE order_items (
        order_id INT64,
        item_id INT64,
        PRIMARY KEY (order_id, nonexistent_column)
    )";

    let result = executor.execute_sql(sql);
    assert!(
        result.is_err(),
        "CREATE TABLE should fail with nonexistent column"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("PRIMARY KEY column") && err_msg.contains("does not exist"),
        "Error should mention PRIMARY KEY column doesn't exist, got: {}",
        err_msg
    );
}

#[test]
fn test_create_table_primary_key_with_check_constraint() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        id INT64,
        age INT64,
        PRIMARY KEY (id),
        CHECK (age >= 18)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some(), "PRIMARY KEY should be set");
    assert_eq!(
        primary_key.unwrap(),
        &["id"],
        "PRIMARY KEY should be on 'id' column"
    );

    let check_constraints = table.schema().check_constraints();
    assert_eq!(check_constraints.len(), 1, "Should have 1 CHECK constraint");
}

#[test]
fn test_create_table_all_constraint_types() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE products (
        id INT64,
        name STRING,
        sku STRING,
        price FLOAT64,
        PRIMARY KEY (id),
        UNIQUE (sku),
        CHECK (price > 0)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("products").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some(), "PRIMARY KEY should be set");
    assert_eq!(primary_key.unwrap(), &["id"]);

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(unique_constraints.len(), 1);
    assert_eq!(unique_constraints[0].columns, vec!["sku"]);

    let check_constraints = table.schema().check_constraints();
    assert_eq!(check_constraints.len(), 1);
}

#[test]
fn test_create_table_primary_key_case_sensitivity() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE users (
        UserId INT64,
        Name STRING,
        PRIMARY KEY (UserId)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some(), "PRIMARY KEY should be set");
    assert_eq!(
        primary_key.unwrap(),
        &["UserId"],
        "PRIMARY KEY should preserve case"
    );
}

#[test]
fn test_create_table_empty_table_name_with_constraints() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE testdb.users (
        id INT64,
        email STRING,
        PRIMARY KEY (id),
        UNIQUE (email)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("testdb").expect("Dataset should exist");
    let table = dataset.get_table("users").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some());
    assert_eq!(primary_key.unwrap(), &["id"]);

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(unique_constraints.len(), 1);
    assert_eq!(unique_constraints[0].columns, vec!["email"]);
}
