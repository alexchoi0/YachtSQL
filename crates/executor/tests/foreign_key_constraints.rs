use yachtsql_capability::DialectType;
use yachtsql_executor::QueryExecutor;
use yachtsql_storage::ReferentialAction;

#[test]
fn test_create_table_with_simple_foreign_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64, name STRING)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        name STRING,
        FOREIGN KEY (dept_id) REFERENCES departments(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(
        foreign_keys.len(),
        1,
        "Should have 1 FOREIGN KEY constraint"
    );

    let fk = &foreign_keys[0];
    assert_eq!(
        fk.child_columns,
        vec!["dept_id"],
        "Child column should be dept_id"
    );
    assert_eq!(
        fk.parent_table, "departments",
        "Parent table should be departments"
    );
    assert_eq!(fk.parent_columns, vec!["id"], "Parent column should be id");
    assert_eq!(
        fk.on_delete,
        ReferentialAction::NoAction,
        "Default ON DELETE is NO ACTION"
    );
    assert_eq!(
        fk.on_update,
        ReferentialAction::NoAction,
        "Default ON UPDATE is NO ACTION"
    );
}

#[test]
fn test_create_table_with_named_foreign_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        CONSTRAINT fk_emp_dept FOREIGN KEY (dept_id) REFERENCES departments(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(foreign_keys.len(), 1);

    let fk = &foreign_keys[0];
    assert_eq!(
        fk.name,
        Some("fk_emp_dept".to_string()),
        "Foreign key should be named"
    );
}

#[test]
fn test_create_table_with_composite_foreign_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE order_items (order_id INT64, product_id INT64, quantity INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE shipments (
        id INT64,
        order_id INT64,
        product_id INT64,
        FOREIGN KEY (order_id, product_id) REFERENCES order_items(order_id, product_id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("shipments").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(foreign_keys.len(), 1);

    let fk = &foreign_keys[0];
    assert_eq!(
        fk.child_columns,
        vec!["order_id", "product_id"],
        "Should have 2 child columns"
    );
    assert_eq!(
        fk.parent_columns,
        vec!["order_id", "product_id"],
        "Should have 2 parent columns"
    );
    assert!(fk.is_composite(), "Foreign key should be composite");
}

#[test]
fn test_create_table_with_cascade_delete() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE CASCADE
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::Cascade,
        "ON DELETE should be CASCADE"
    );
}

#[test]
fn test_create_table_with_set_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE SET NULL
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::SetNull,
        "ON DELETE should be SET NULL"
    );
}

#[test]
fn test_create_table_with_set_default() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE SET DEFAULT
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::SetDefault,
        "ON DELETE should be SET DEFAULT"
    );
}

#[test]
fn test_create_table_with_restrict() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE RESTRICT
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::Restrict,
        "ON DELETE should be RESTRICT"
    );
}

#[test]
fn test_create_table_with_update_cascade() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id) ON UPDATE CASCADE
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_update,
        ReferentialAction::Cascade,
        "ON UPDATE should be CASCADE"
    );
}

#[test]
fn test_create_table_with_both_delete_and_update_actions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id)
            ON DELETE CASCADE
            ON UPDATE SET NULL
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::Cascade,
        "ON DELETE should be CASCADE"
    );
    assert_eq!(
        fk.on_update,
        ReferentialAction::SetNull,
        "ON UPDATE should be SET NULL"
    );
}

#[test]
fn test_create_table_with_multiple_foreign_keys() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");
    executor
        .execute_sql("CREATE TABLE managers (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        manager_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id),
        FOREIGN KEY (manager_id) REFERENCES managers(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(
        foreign_keys.len(),
        2,
        "Should have 2 FOREIGN KEY constraints"
    );

    assert_eq!(foreign_keys[0].child_columns, vec!["dept_id"]);
    assert_eq!(foreign_keys[0].parent_table, "departments");

    assert_eq!(foreign_keys[1].child_columns, vec!["manager_id"]);
    assert_eq!(foreign_keys[1].parent_table, "managers");
}

#[test]
fn test_create_table_foreign_key_nonexistent_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        name STRING,
        FOREIGN KEY (dept_id) REFERENCES departments(id)
    )";

    let result = executor.execute_sql(sql);
    assert!(
        result.is_err(),
        "CREATE TABLE should fail with nonexistent column"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("FOREIGN KEY column") && err_msg.contains("does not exist"),
        "Error should mention FOREIGN KEY column doesn't exist, got: {}",
        err_msg
    );
}

#[test]
fn test_create_table_foreign_key_column_count_mismatch() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64, code INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES departments(id, code)
    )";

    let result = executor.execute_sql(sql);
    assert!(
        result.is_err(),
        "CREATE TABLE should fail with column count mismatch"
    );

    let err = result.unwrap_err();
    let err_msg = err.to_string();
    assert!(
        err_msg.contains("column count mismatch"),
        "Error should mention column count mismatch, got: {}",
        err_msg
    );
}

#[test]
fn test_create_table_self_referencing_foreign_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    let sql = "CREATE TABLE employees (
        id INT64,
        manager_id INT64,
        name STRING,
        FOREIGN KEY (manager_id) REFERENCES employees(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(foreign_keys.len(), 1);

    let fk = &foreign_keys[0];
    assert_eq!(fk.child_columns, vec!["manager_id"]);
    assert_eq!(fk.parent_table, "employees");
    assert_eq!(fk.parent_columns, vec!["id"]);
    assert!(
        fk.is_self_referencing("employees"),
        "Foreign key should be self-referencing"
    );
}

#[test]
fn test_create_table_all_constraint_types_with_foreign_key() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64, PRIMARY KEY (id))")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64,
        email STRING,
        salary FLOAT64,
        PRIMARY KEY (id),
        UNIQUE (email),
        FOREIGN KEY (dept_id) REFERENCES departments(id) ON DELETE CASCADE,
        CHECK (salary > 0)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let primary_key = table.schema().primary_key();
    assert!(primary_key.is_some());
    assert_eq!(primary_key.unwrap(), &["id"]);

    let unique_constraints = table.schema().unique_constraints();
    assert_eq!(unique_constraints.len(), 1);
    assert_eq!(unique_constraints[0].columns, vec!["email"]);

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(foreign_keys.len(), 1);
    assert_eq!(foreign_keys[0].child_columns, vec!["dept_id"]);
    assert_eq!(foreign_keys[0].on_delete, ReferentialAction::Cascade);

    let check_constraints = table.schema().check_constraints();
    assert_eq!(check_constraints.len(), 1);
}

#[test]
fn test_create_table_foreign_key_with_qualified_table_name() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE testdb.departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE testdb.employees (
        id INT64,
        dept_id INT64,
        FOREIGN KEY (dept_id) REFERENCES testdb.departments(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage.get_dataset("testdb").expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(foreign_keys.len(), 1);
    assert_eq!(foreign_keys[0].parent_table, "testdb.departments");
}

#[test]
fn test_column_level_foreign_key_simple() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64 REFERENCES departments(id),
        name STRING
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(
        foreign_keys.len(),
        1,
        "Should have 1 FOREIGN KEY constraint"
    );

    let fk = &foreign_keys[0];
    assert_eq!(
        fk.child_columns,
        vec!["dept_id"],
        "Child column should be dept_id"
    );
    assert_eq!(
        fk.parent_table, "departments",
        "Parent table should be departments"
    );
    assert_eq!(fk.parent_columns, vec!["id"], "Parent column should be id");
}

#[test]
fn test_column_level_foreign_key_with_on_delete_cascade() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64 REFERENCES departments(id) ON DELETE CASCADE
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::Cascade,
        "ON DELETE should be CASCADE"
    );
}

#[test]
fn test_column_level_foreign_key_with_on_update_set_null() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64 REFERENCES departments(id) ON UPDATE SET NULL
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_update,
        ReferentialAction::SetNull,
        "ON UPDATE should be SET NULL"
    );
}

#[test]
fn test_column_level_foreign_key_with_both_actions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64 REFERENCES departments(id) ON DELETE CASCADE ON UPDATE RESTRICT
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    let fk = &foreign_keys[0];
    assert_eq!(
        fk.on_delete,
        ReferentialAction::Cascade,
        "ON DELETE should be CASCADE"
    );
    assert_eq!(
        fk.on_update,
        ReferentialAction::Restrict,
        "ON UPDATE should be RESTRICT"
    );
}

#[test]
fn test_column_level_multiple_foreign_keys() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");
    executor
        .execute_sql("CREATE TABLE managers (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64 REFERENCES departments(id),
        manager_id INT64 REFERENCES managers(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(
        foreign_keys.len(),
        2,
        "Should have 2 FOREIGN KEY constraints"
    );

    assert_eq!(foreign_keys[0].child_columns, vec!["dept_id"]);
    assert_eq!(foreign_keys[0].parent_table, "departments");

    assert_eq!(foreign_keys[1].child_columns, vec!["manager_id"]);
    assert_eq!(foreign_keys[1].parent_table, "managers");
}

#[test]
fn test_column_level_and_table_level_foreign_keys_mixed() {
    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    executor
        .execute_sql("CREATE TABLE departments (id INT64)")
        .expect("CREATE parent table should succeed");
    executor
        .execute_sql("CREATE TABLE managers (id INT64)")
        .expect("CREATE parent table should succeed");

    let sql = "CREATE TABLE employees (
        id INT64,
        dept_id INT64 REFERENCES departments(id),
        manager_id INT64,
        FOREIGN KEY (manager_id) REFERENCES managers(id)
    )";

    executor
        .execute_sql(sql)
        .expect("CREATE TABLE should succeed");

    let storage = executor.storage.borrow_mut();
    let dataset = storage
        .get_dataset("default")
        .expect("Dataset should exist");
    let table = dataset.get_table("employees").expect("Table should exist");

    let foreign_keys = table.schema().foreign_keys();
    assert_eq!(
        foreign_keys.len(),
        2,
        "Should have 2 FOREIGN KEY constraints"
    );

    let dept_fk = foreign_keys
        .iter()
        .find(|fk| fk.child_columns == vec!["dept_id"])
        .expect("dept_id FK should exist");
    assert_eq!(dept_fk.parent_table, "departments");

    let mgr_fk = foreign_keys
        .iter()
        .find(|fk| fk.child_columns == vec!["manager_id"])
        .expect("manager_id FK should exist");
    assert_eq!(mgr_fk.parent_table, "managers");
}
