use crate::assert_table_eq;
use crate::common::{create_session, numeric};

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ADD COLUMN age INT64")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM users ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice", null], [2, "Bob", 30],]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users DROP COLUMN age")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_rename_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users RENAME COLUMN name TO full_name")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, full_name FROM users")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_rename_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE old_name (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO old_name VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE old_name RENAME TO new_name")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM new_name").await.unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_default() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ADD COLUMN status STRING DEFAULT 'active'")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();

    assert_table_eq!(result, [[1, "Alice", "active"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_constraint() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, email STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ADD CONSTRAINT unique_email UNIQUE (email)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'test@example.com')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();

    assert_table_eq!(result, [[1, "test@example.com"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_not_null() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ALTER COLUMN name SET NOT NULL")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_not_null() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING NOT NULL)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ALTER COLUMN name DROP NOT NULL")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").await.unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_options() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE options_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE options_table SET OPTIONS (description = 'Updated description')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO options_table VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM options_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_multiple_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE multi_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE multi_add ADD COLUMN name STRING, ADD COLUMN age INT64")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_add VALUES (1, 'Alice', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM multi_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_alter_column_set_default() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE default_test (id INT64, status STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE default_test ALTER COLUMN status SET DEFAULT 'pending'")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO default_test (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM default_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_alter_column_drop_default() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE drop_default (id INT64, status STRING DEFAULT 'active')")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_default ALTER COLUMN status DROP DEFAULT")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO drop_default (id) VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM drop_default")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("ALTER TABLE IF EXISTS nonexistent ADD COLUMN x INT64")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_constraint() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE constraint_drop (id INT64, email STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE constraint_drop ADD CONSTRAINT unique_email UNIQUE (email)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE constraint_drop DROP CONSTRAINT unique_email")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO constraint_drop VALUES (1, 'a@b.com'), (2, 'a@b.com')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM constraint_drop")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_alter_column_set_data_type() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE type_change (id INT64, value STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE type_change ALTER COLUMN value SET DATA TYPE STRING(100)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_primary_key() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE add_pk (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE add_pk ADD PRIMARY KEY (id) NOT ENFORCED")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_primary_key() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE drop_pk (id INT64 PRIMARY KEY NOT ENFORCED, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_pk DROP PRIMARY KEY")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_view_set_options() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE base_view_table (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW my_view AS SELECT * FROM base_view_table")
        .await
        .unwrap();

    session
        .execute_sql("ALTER VIEW my_view SET OPTIONS (description = 'Updated view')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_materialized_view_set_options() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE mv_base (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE MATERIALIZED VIEW my_mv AS SELECT id, SUM(value) as total FROM mv_base GROUP BY id").await
        .unwrap();

    session
        .execute_sql("ALTER MATERIALIZED VIEW my_mv SET OPTIONS (enable_refresh = true)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_if_not_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE if_not_exists_test (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE if_not_exists_test ADD COLUMN IF NOT EXISTS name STRING")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE if_not_exists_test ADD COLUMN IF NOT EXISTS age INT64")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO if_not_exists_test VALUES (1, 'Alice', 30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM if_not_exists_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_column_if_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE drop_if_exists (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_if_exists DROP COLUMN IF EXISTS nonexistent")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_if_exists DROP COLUMN IF EXISTS name")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO drop_if_exists VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM drop_if_exists")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_rename_multiple_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE rename_multi (a INT64, b STRING, c FLOAT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE rename_multi RENAME COLUMN a TO x, RENAME COLUMN b TO y")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO rename_multi VALUES (1, 'test', 3.14)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y, c FROM rename_multi")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "test", 3.14]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_struct() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE struct_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE struct_add ADD COLUMN address STRUCT<street STRING, city STRING, zip STRING>").await
        .unwrap();

    session
        .execute_sql("INSERT INTO struct_add VALUES (1, STRUCT('123 Main St', 'NYC', '10001'))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, address.city FROM struct_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "NYC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_array() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE array_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE array_add ADD COLUMN tags ARRAY<STRING>")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO array_add VALUES (1, ['rust', 'sql', 'bigquery'])")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, tags[OFFSET(0)] FROM array_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "rust"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_foreign_key() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE parent_fk (id INT64 PRIMARY KEY NOT ENFORCED)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE child_fk (id INT64, parent_id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE child_fk ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent_fk(id) NOT ENFORCED").await
        .unwrap();

    session
        .execute_sql("INSERT INTO parent_fk VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO child_fk VALUES (1, 1)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM child_fk").await.unwrap();
    assert_table_eq!(result, [[1, 1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_drop_foreign_key() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE parent_drop_fk (id INT64 PRIMARY KEY NOT ENFORCED)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE child_drop_fk (
                id INT64,
                parent_id INT64,
                CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent_drop_fk(id) NOT ENFORCED
            )",
        ).await
        .unwrap();

    session
        .execute_sql("ALTER TABLE child_drop_fk DROP CONSTRAINT fk_parent")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_default_collate() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE collate_test (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE collate_test SET DEFAULT COLLATE 'und:ci'")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE collate_test ADD COLUMN description STRING")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO collate_test VALUES (1, 'Test', 'Description')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM collate_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Test", "Description"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_column_set_options() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE col_options (id INT64, amount NUMERIC)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE col_options ALTER COLUMN amount SET OPTIONS (rounding_mode = 'ROUND_HALF_EVEN')").await
        .unwrap();

    session
        .execute_sql("INSERT INTO col_options VALUES (1, NUMERIC '123.456')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM col_options")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, numeric("123.456")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_options_multiple() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE multi_options (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE multi_options SET OPTIONS (
                description = 'Test table',
                labels = [('env', 'test'), ('team', 'data')],
                expiration_timestamp = TIMESTAMP '2030-01-01 00:00:00 UTC'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_options VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM multi_options")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_geography() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE geo_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE geo_add ADD COLUMN location GEOGRAPHY")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO geo_add VALUES (1, ST_GEOGPOINT(-122.4194, 37.7749))")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM geo_add").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_json() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE json_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE json_add ADD COLUMN metadata JSON")
        .await
        .unwrap();

    session
        .execute_sql(r#"INSERT INTO json_add VALUES (1, JSON '{"key": "value"}')"#)
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, JSON_VALUE(metadata, '$.key') FROM json_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "value"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_position() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE position_test (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE position_test ADD COLUMN age INT64 AFTER id")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO position_test VALUES (1, 30, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM position_test")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 30, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_alter_column_set_data_type_widening() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE type_widen (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO type_widen VALUES (1, 100)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE type_widen ALTER COLUMN value SET DATA TYPE NUMERIC")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM type_widen")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, numeric("100")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_with_qualified_name() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA test_schema")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE test_schema.qualified_alter (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE test_schema.qualified_alter ADD COLUMN name STRING")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO test_schema.qualified_alter VALUES (1, 'Test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM test_schema.qualified_alter")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_check_constraint() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE check_add (id INT64, age INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE check_add ADD CONSTRAINT age_positive CHECK (age > 0)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO check_add VALUES (1, 25)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM check_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 25]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_alter_column_collate() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE col_collate (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE col_collate ALTER COLUMN name SET OPTIONS (collate = 'und:ci')")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO col_collate VALUES (1, 'Test')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM col_collate WHERE name = 'TEST'")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_nested_struct_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE nested_struct_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE nested_struct_add ADD COLUMN profile STRUCT<
                personal STRUCT<first_name STRING, last_name STRING>,
                contact STRUCT<email STRING, phone STRING>
            >",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO nested_struct_add VALUES (
                1,
                STRUCT(STRUCT('John', 'Doe'), STRUCT('john@example.com', '555-1234'))
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, profile.personal.first_name FROM nested_struct_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "John"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_array_of_struct_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE array_struct_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE array_struct_add ADD COLUMN orders ARRAY<STRUCT<product STRING, quantity INT64, price NUMERIC>>",
        ).await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO array_struct_add VALUES (
                1,
                [STRUCT('Widget', 5, 19.99), STRUCT('Gadget', 2, 49.99)]
            )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, orders[OFFSET(0)].product FROM array_struct_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Widget"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_partition_expiration() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE partition_exp (id INT64, created DATE)
            PARTITION BY created",
        )
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE partition_exp SET OPTIONS (partition_expiration_days = 30)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_set_require_partition_filter() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE require_filter (id INT64, created DATE)
            PARTITION BY created",
        )
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE require_filter SET OPTIONS (require_partition_filter = true)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_composite_primary_key() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE composite_pk_add (tenant_id INT64, user_id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE composite_pk_add ADD PRIMARY KEY (tenant_id, user_id) NOT ENFORCED",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO composite_pk_add VALUES (1, 1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM composite_pk_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_composite_foreign_key() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE composite_parent (
                tenant_id INT64,
                user_id INT64,
                PRIMARY KEY (tenant_id, user_id) NOT ENFORCED
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE composite_child (
                id INT64,
                tenant_id INT64,
                user_id INT64
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE composite_child ADD CONSTRAINT fk_composite
            FOREIGN KEY (tenant_id, user_id) REFERENCES composite_parent(tenant_id, user_id) NOT ENFORCED",
        ).await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_view_alter_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE view_base (id INT64, name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VIEW alter_view AS SELECT id, name FROM view_base")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER VIEW alter_view ALTER COLUMN name SET OPTIONS (description = 'User name')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_materialized_view_enable_refresh() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE mv_refresh_base (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_refresh
            OPTIONS (enable_refresh = false)
            AS SELECT id, SUM(value) as total FROM mv_refresh_base GROUP BY id",
        )
        .await
        .unwrap();

    session
        .execute_sql("ALTER MATERIALIZED VIEW mv_refresh SET OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_range() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE range_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE range_add ADD COLUMN date_range RANGE<DATE>")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO range_add VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-12-31'))",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM range_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_table_add_column_with_interval() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE interval_add (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("ALTER TABLE interval_add ADD COLUMN duration INTERVAL")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO interval_add VALUES (1, INTERVAL 1 DAY)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM interval_add")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}
