use crate::assert_table_eq;
use crate::common::{create_session, numeric};

#[test]
fn test_alter_table_add_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ADD COLUMN age INT64")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 30)")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM users ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice", null], [2, "Bob", 30],]);
}

#[test]
fn test_alter_table_drop_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users DROP COLUMN age")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_rename_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users RENAME COLUMN name TO full_name")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, full_name FROM users")
        .unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_rename_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE old_name (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO old_name VALUES (1, 'Alice')")
        .unwrap();

    session
        .execute_sql("ALTER TABLE old_name RENAME TO new_name")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM new_name").unwrap();

    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_add_column_with_default() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ADD COLUMN status STRING DEFAULT 'active'")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, [[1, "Alice", "active"]]);
}

#[test]
fn test_alter_table_add_constraint() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, email STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ADD CONSTRAINT unique_email UNIQUE (email)")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'test@example.com')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();

    assert_table_eq!(result, [[1, "test@example.com"]]);
}

#[test]
fn test_alter_table_set_not_null() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ALTER COLUMN name SET NOT NULL")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, 'Alice')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_alter_table_drop_not_null() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE users (id INT64, name STRING NOT NULL)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE users ALTER COLUMN name DROP NOT NULL")
        .unwrap();

    session
        .execute_sql("INSERT INTO users VALUES (1, NULL)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[test]
#[ignore]
fn test_alter_table_set_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE options_table (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE options_table SET OPTIONS (description = 'Updated description')")
        .unwrap();

    session
        .execute_sql("INSERT INTO options_table VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM options_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_alter_table_add_multiple_columns() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE multi_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE multi_add ADD COLUMN name STRING, ADD COLUMN age INT64")
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_add VALUES (1, 'Alice', 30)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM multi_add").unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[test]
#[ignore]
fn test_alter_table_alter_column_set_default() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE default_test (id INT64, status STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE default_test ALTER COLUMN status SET DEFAULT 'pending'")
        .unwrap();

    session
        .execute_sql("INSERT INTO default_test (id) VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM default_test").unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[test]
#[ignore]
fn test_alter_table_alter_column_drop_default() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE drop_default (id INT64, status STRING DEFAULT 'active')")
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_default ALTER COLUMN status DROP DEFAULT")
        .unwrap();

    session
        .execute_sql("INSERT INTO drop_default (id) VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM drop_default").unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[test]
#[ignore]
fn test_alter_table_if_exists() {
    let mut session = create_session();

    let result = session.execute_sql("ALTER TABLE IF EXISTS nonexistent ADD COLUMN x INT64");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_alter_table_drop_constraint() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE constraint_drop (id INT64, email STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE constraint_drop ADD CONSTRAINT unique_email UNIQUE (email)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE constraint_drop DROP CONSTRAINT unique_email")
        .unwrap();

    session
        .execute_sql("INSERT INTO constraint_drop VALUES (1, 'a@b.com'), (2, 'a@b.com')")
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM constraint_drop")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore]
fn test_alter_table_alter_column_set_data_type() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE type_change (id INT64, value STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE type_change ALTER COLUMN value SET DATA TYPE STRING(100)")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_add_primary_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE add_pk (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE add_pk ADD PRIMARY KEY (id) NOT ENFORCED")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_drop_primary_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE drop_pk (id INT64 PRIMARY KEY NOT ENFORCED, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_pk DROP PRIMARY KEY")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_view_set_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE base_view_table (id INT64)")
        .unwrap();

    session
        .execute_sql("CREATE VIEW my_view AS SELECT * FROM base_view_table")
        .unwrap();

    session
        .execute_sql("ALTER VIEW my_view SET OPTIONS (description = 'Updated view')")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_materialized_view_set_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE mv_base (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("CREATE MATERIALIZED VIEW my_mv AS SELECT id, SUM(value) as total FROM mv_base GROUP BY id")
        .unwrap();

    session
        .execute_sql("ALTER MATERIALIZED VIEW my_mv SET OPTIONS (enable_refresh = true)")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_add_column_if_not_exists() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE if_not_exists_test (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE if_not_exists_test ADD COLUMN IF NOT EXISTS name STRING")
        .unwrap();

    session
        .execute_sql("ALTER TABLE if_not_exists_test ADD COLUMN IF NOT EXISTS age INT64")
        .unwrap();

    session
        .execute_sql("INSERT INTO if_not_exists_test VALUES (1, 'Alice', 30)")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM if_not_exists_test")
        .unwrap();
    assert_table_eq!(result, [[1, "Alice", 30]]);
}

#[test]
#[ignore]
fn test_alter_table_drop_column_if_exists() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE drop_if_exists (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_if_exists DROP COLUMN IF EXISTS nonexistent")
        .unwrap();

    session
        .execute_sql("ALTER TABLE drop_if_exists DROP COLUMN IF EXISTS name")
        .unwrap();

    session
        .execute_sql("INSERT INTO drop_if_exists VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM drop_if_exists").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_alter_table_rename_multiple_columns() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE rename_multi (a INT64, b STRING, c FLOAT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE rename_multi RENAME COLUMN a TO x, RENAME COLUMN b TO y")
        .unwrap();

    session
        .execute_sql("INSERT INTO rename_multi VALUES (1, 'test', 3.14)")
        .unwrap();

    let result = session
        .execute_sql("SELECT x, y, c FROM rename_multi")
        .unwrap();
    assert_table_eq!(result, [[1, "test", 3.14]]);
}

#[test]
#[ignore]
fn test_alter_table_add_column_with_struct() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE struct_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE struct_add ADD COLUMN address STRUCT<street STRING, city STRING, zip STRING>")
        .unwrap();

    session
        .execute_sql("INSERT INTO struct_add VALUES (1, STRUCT('123 Main St', 'NYC', '10001'))")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, address.city FROM struct_add")
        .unwrap();
    assert_table_eq!(result, [[1, "NYC"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_column_with_array() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE array_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE array_add ADD COLUMN tags ARRAY<STRING>")
        .unwrap();

    session
        .execute_sql("INSERT INTO array_add VALUES (1, ['rust', 'sql', 'bigquery'])")
        .unwrap();

    let result = session
        .execute_sql("SELECT id, tags[OFFSET(0)] FROM array_add")
        .unwrap();
    assert_table_eq!(result, [[1, "rust"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_foreign_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE parent_fk (id INT64 PRIMARY KEY NOT ENFORCED)")
        .unwrap();

    session
        .execute_sql("CREATE TABLE child_fk (id INT64, parent_id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE child_fk ADD CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent_fk(id) NOT ENFORCED")
        .unwrap();

    session
        .execute_sql("INSERT INTO parent_fk VALUES (1)")
        .unwrap();

    session
        .execute_sql("INSERT INTO child_fk VALUES (1, 1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM child_fk").unwrap();
    assert_table_eq!(result, [[1, 1]]);
}

#[test]
#[ignore]
fn test_alter_table_drop_foreign_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE parent_drop_fk (id INT64 PRIMARY KEY NOT ENFORCED)")
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE child_drop_fk (
                id INT64,
                parent_id INT64,
                CONSTRAINT fk_parent FOREIGN KEY (parent_id) REFERENCES parent_drop_fk(id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql("ALTER TABLE child_drop_fk DROP CONSTRAINT fk_parent")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_set_default_collate() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE collate_test (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE collate_test SET DEFAULT COLLATE 'und:ci'")
        .unwrap();

    session
        .execute_sql("ALTER TABLE collate_test ADD COLUMN description STRING")
        .unwrap();

    session
        .execute_sql("INSERT INTO collate_test VALUES (1, 'Test', 'Description')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM collate_test").unwrap();
    assert_table_eq!(result, [[1, "Test", "Description"]]);
}

#[test]
#[ignore]
fn test_alter_column_set_options() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE col_options (id INT64, amount NUMERIC)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE col_options ALTER COLUMN amount SET OPTIONS (rounding_mode = 'ROUND_HALF_EVEN')")
        .unwrap();

    session
        .execute_sql("INSERT INTO col_options VALUES (1, 123.456)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM col_options").unwrap();
    assert_table_eq!(result, [[1, numeric("123.456")]]);
}

#[test]
#[ignore]
fn test_alter_table_set_options_multiple() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE multi_options (id INT64)")
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE multi_options SET OPTIONS (
                description = 'Test table',
                labels = [('env', 'test'), ('team', 'data')],
                expiration_timestamp = TIMESTAMP '2030-01-01 00:00:00 UTC'
            )",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO multi_options VALUES (1)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM multi_options").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_alter_table_add_column_with_geography() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE geo_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE geo_add ADD COLUMN location GEOGRAPHY")
        .unwrap();

    session
        .execute_sql("INSERT INTO geo_add VALUES (1, ST_GEOGPOINT(-122.4194, 37.7749))")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM geo_add").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_alter_table_add_column_with_json() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE json_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE json_add ADD COLUMN metadata JSON")
        .unwrap();

    session
        .execute_sql(r#"INSERT INTO json_add VALUES (1, JSON '{"key": "value"}')"#)
        .unwrap();

    let result = session
        .execute_sql("SELECT id, JSON_VALUE(metadata, '$.key') FROM json_add")
        .unwrap();
    assert_table_eq!(result, [[1, "value"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_column_position() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE position_test (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE position_test ADD COLUMN age INT64 AFTER id")
        .unwrap();

    session
        .execute_sql("INSERT INTO position_test VALUES (1, 30, 'Alice')")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM position_test").unwrap();
    assert_table_eq!(result, [[1, 30, "Alice"]]);
}

#[test]
#[ignore]
fn test_alter_table_alter_column_set_data_type_widening() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE type_widen (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql("INSERT INTO type_widen VALUES (1, 100)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE type_widen ALTER COLUMN value SET DATA TYPE NUMERIC")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM type_widen").unwrap();
    assert_table_eq!(result, [[1, numeric("100")]]);
}

#[test]
#[ignore]
fn test_alter_table_with_qualified_name() {
    let mut session = create_session();

    session.execute_sql("CREATE SCHEMA test_schema").unwrap();

    session
        .execute_sql("CREATE TABLE test_schema.qualified_alter (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE test_schema.qualified_alter ADD COLUMN name STRING")
        .unwrap();

    session
        .execute_sql("INSERT INTO test_schema.qualified_alter VALUES (1, 'Test')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM test_schema.qualified_alter")
        .unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_check_constraint() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE check_add (id INT64, age INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE check_add ADD CONSTRAINT age_positive CHECK (age > 0)")
        .unwrap();

    session
        .execute_sql("INSERT INTO check_add VALUES (1, 25)")
        .unwrap();

    let result = session.execute_sql("SELECT * FROM check_add").unwrap();
    assert_table_eq!(result, [[1, 25]]);
}

#[test]
#[ignore]
fn test_alter_table_alter_column_collate() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE col_collate (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE col_collate ALTER COLUMN name SET OPTIONS (collate = 'und:ci')")
        .unwrap();

    session
        .execute_sql("INSERT INTO col_collate VALUES (1, 'Test')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM col_collate WHERE name = 'TEST'")
        .unwrap();
    assert_table_eq!(result, [[1, "Test"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_nested_struct_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nested_struct_add (id INT64)")
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE nested_struct_add ADD COLUMN profile STRUCT<
                personal STRUCT<first_name STRING, last_name STRING>,
                contact STRUCT<email STRING, phone STRING>
            >",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO nested_struct_add VALUES (
                1,
                STRUCT(STRUCT('John', 'Doe'), STRUCT('john@example.com', '555-1234'))
            )",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT id, profile.personal.first_name FROM nested_struct_add")
        .unwrap();
    assert_table_eq!(result, [[1, "John"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_array_of_struct_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE array_struct_add (id INT64)")
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE array_struct_add ADD COLUMN orders ARRAY<STRUCT<product STRING, quantity INT64, price NUMERIC>>",
        )
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO array_struct_add VALUES (
                1,
                [STRUCT('Widget', 5, 19.99), STRUCT('Gadget', 2, 49.99)]
            )",
        )
        .unwrap();

    let result = session
        .execute_sql("SELECT id, orders[OFFSET(0)].product FROM array_struct_add")
        .unwrap();
    assert_table_eq!(result, [[1, "Widget"]]);
}

#[test]
#[ignore]
fn test_alter_table_set_partition_expiration() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE partition_exp (id INT64, created DATE)
            PARTITION BY created",
        )
        .unwrap();

    session
        .execute_sql("ALTER TABLE partition_exp SET OPTIONS (partition_expiration_days = 30)")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_set_require_partition_filter() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE require_filter (id INT64, created DATE)
            PARTITION BY created",
        )
        .unwrap();

    session
        .execute_sql("ALTER TABLE require_filter SET OPTIONS (require_partition_filter = true)")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_add_composite_primary_key() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE composite_pk_add (tenant_id INT64, user_id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE composite_pk_add ADD PRIMARY KEY (tenant_id, user_id) NOT ENFORCED",
        )
        .unwrap();

    session
        .execute_sql("INSERT INTO composite_pk_add VALUES (1, 1, 'Alice')")
        .unwrap();

    let result = session
        .execute_sql("SELECT * FROM composite_pk_add")
        .unwrap();
    assert_table_eq!(result, [[1, 1, "Alice"]]);
}

#[test]
#[ignore]
fn test_alter_table_add_composite_foreign_key() {
    let mut session = create_session();

    session
        .execute_sql(
            "CREATE TABLE composite_parent (
                tenant_id INT64,
                user_id INT64,
                PRIMARY KEY (tenant_id, user_id) NOT ENFORCED
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE composite_child (
                id INT64,
                tenant_id INT64,
                user_id INT64
            )",
        )
        .unwrap();

    session
        .execute_sql(
            "ALTER TABLE composite_child ADD CONSTRAINT fk_composite
            FOREIGN KEY (tenant_id, user_id) REFERENCES composite_parent(tenant_id, user_id) NOT ENFORCED",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_view_alter_column() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE view_base (id INT64, name STRING)")
        .unwrap();

    session
        .execute_sql("CREATE VIEW alter_view AS SELECT id, name FROM view_base")
        .unwrap();

    session
        .execute_sql(
            "ALTER VIEW alter_view ALTER COLUMN name SET OPTIONS (description = 'User name')",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_materialized_view_enable_refresh() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE mv_refresh_base (id INT64, value INT64)")
        .unwrap();

    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_refresh
            OPTIONS (enable_refresh = false)
            AS SELECT id, SUM(value) as total FROM mv_refresh_base GROUP BY id",
        )
        .unwrap();

    session
        .execute_sql("ALTER MATERIALIZED VIEW mv_refresh SET OPTIONS (enable_refresh = true, refresh_interval_minutes = 60)")
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_table_add_column_with_range() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE range_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE range_add ADD COLUMN date_range RANGE<DATE>")
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO range_add VALUES (1, RANGE(DATE '2024-01-01', DATE '2024-12-31'))",
        )
        .unwrap();

    let result = session.execute_sql("SELECT id FROM range_add").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_alter_table_add_column_with_interval() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE interval_add (id INT64)")
        .unwrap();

    session
        .execute_sql("ALTER TABLE interval_add ADD COLUMN duration INTERVAL")
        .unwrap();

    session
        .execute_sql("INSERT INTO interval_add VALUES (1, INTERVAL 1 DAY)")
        .unwrap();

    let result = session.execute_sql("SELECT id FROM interval_add").unwrap();
    assert_table_eq!(result, [[1]]);
}
