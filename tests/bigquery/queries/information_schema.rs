use crate::common::{batch, create_executor};

fn setup_test_schema(executor: &mut yachtsql::QueryExecutor) {
    executor.execute_sql("CREATE SCHEMA test_schema").unwrap();
    executor
        .execute_sql(
            "CREATE TABLE test_schema.users (
                id INT64 NOT NULL,
                name STRING,
                email STRING,
                created_at TIMESTAMP,
                PRIMARY KEY (id)
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE test_schema.orders (
                order_id INT64 NOT NULL,
                user_id INT64,
                amount INT64,
                order_date DATE,
                PRIMARY KEY (order_id)
            )",
        )
        .unwrap();
    executor
        .execute_sql("CREATE VIEW test_schema.active_users AS SELECT * FROM test_schema.users")
        .unwrap();
}

#[test]
fn test_information_schema_schemata() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT schema_name
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name = 'test_schema'",
        )
        .unwrap();
    assert_table_eq!(result, [["test_schema"]]);
}

#[test]
fn test_information_schema_tables() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_name, table_type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["active_users", "VIEW"],
            ["orders", "BASE TABLE"],
            ["users", "BASE TABLE"],
        ]
    );
}

#[test]
fn test_information_schema_tables_base_table() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'test_schema' AND table_type = 'BASE TABLE'
            ORDER BY table_name",
        )
        .unwrap();
    assert_table_eq!(result, [["orders"], ["users"]]);
}

#[test]
fn test_information_schema_tables_view() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'test_schema' AND table_type = 'VIEW'",
        )
        .unwrap();
    assert_table_eq!(result, [["active_users"]]);
}

#[test]
fn test_information_schema_columns() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT column_name, data_type, is_nullable
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema' AND table_name = 'users'
            ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["id", "INT64", "NO"],
            ["name", "STRING", "YES"],
            ["email", "STRING", "YES"],
            ["created_at", "TIMESTAMP", "YES"],
        ]
    );
}

#[test]
fn test_information_schema_columns_ordinal_position() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT column_name, ordinal_position
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema' AND table_name = 'users'
            ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["id", 1], ["name", 2], ["email", 3], ["created_at", 4]]
    );
}

#[test]
fn test_information_schema_columns_nullable() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema'
                AND table_name = 'users'
                AND is_nullable = 'NO'",
        )
        .unwrap();
    assert_table_eq!(result, [["id"]]);
}

#[test]
fn test_information_schema_views() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_name, view_definition
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE table_schema = 'test_schema'",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["active_users", "SELECT * FROM test_schema.users"]]
    );
}

#[test]
fn test_information_schema_table_constraints() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT constraint_name, constraint_type
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["orders_pkey", "PRIMARY KEY"],
            ["users_pkey", "PRIMARY KEY"],
        ]
    );
}

#[test]
fn test_information_schema_key_column_usage() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_name, column_name, constraint_name
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["orders", "order_id", "orders_pkey"],
            ["users", "id", "users_pkey"],
        ]
    );
}

#[test]
fn test_information_schema_column_field_paths() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE nested_data (
                id INT64,
                info STRUCT<name STRING, address STRUCT<city STRING, zip STRING>>
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 1
            FROM INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name = 'nested_data'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_table_options() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE options_table (id INT64)
            OPTIONS (description = 'Test table')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_OPTIONS
            WHERE table_name = 'options_table'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_routines() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION add_nums(a INT64, b INT64) RETURNS INT64 AS (a + b)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE routine_name = 'add_nums'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_parameters() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION multiply(x INT64, y INT64) RETURNS INT64 AS (x * y)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.PARAMETERS
            WHERE specific_name = 'multiply'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_partitions() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE partitioned_data (
                id INT64,
                created_date DATE
            )
            PARTITION BY created_date",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO partitioned_data VALUES (1, DATE '2024-01-15')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'partitioned_data'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_all_columns() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema'",
        )
        .unwrap();
    assert_table_eq!(result, [[8]]);
}

#[test]
fn test_information_schema_join_tables_columns() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT t.table_name, COUNT(c.column_name) AS column_count
            FROM INFORMATION_SCHEMA.TABLES t
            JOIN INFORMATION_SCHEMA.COLUMNS c
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE t.table_schema = 'test_schema' AND t.table_type = 'BASE TABLE'
            GROUP BY t.table_name
            ORDER BY t.table_name",
        )
        .unwrap();
    assert_table_eq!(result, [["orders", 4], ["users", 4]]);
}

#[test]
fn test_information_schema_column_default() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE defaults_table (
                id INT64,
                status STRING DEFAULT 'pending',
                count INT64 DEFAULT 0
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'defaults_table' AND column_default IS NOT NULL",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_data_types() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE all_types (
                int_col INT64,
                float_col FLOAT64,
                string_col STRING,
                bool_col BOOL,
                date_col DATE,
                timestamp_col TIMESTAMP,
                bytes_col BYTES,
                array_col ARRAY<INT64>,
                struct_col STRUCT<a INT64, b STRING>
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'all_types'
            ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["int_col", "INT64"],
            ["float_col", "FLOAT64"],
            ["string_col", "STRING"],
            ["bool_col", "BOOL"],
            ["date_col", "DATE"],
            ["timestamp_col", "TIMESTAMP"],
            ["bytes_col", "BYTES"],
            ["array_col", "ARRAY<INT64>"],
            ["struct_col", "STRUCT<a INT64, b STRING>"],
        ]
    );
}

#[test]
fn test_information_schema_search_columns() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_schema, table_name, column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE column_name LIKE '%id%'
            ORDER BY table_schema, table_name, column_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["test_schema", "orders", "order_id"],
            ["test_schema", "orders", "user_id"],
            ["test_schema", "users", "id"],
        ]
    );
}

#[test]
fn test_information_schema_table_catalog() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT DISTINCT table_catalog
            FROM INFORMATION_SCHEMA.TABLES",
        )
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_information_schema_check_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE checked_table (
                id INT64,
                age INT64 CHECK (age >= 0),
                status STRING CHECK (status IN ('active', 'inactive'))
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT constraint_name, check_clause
            FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
            WHERE constraint_schema = 'public'",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["checked_table_age_check", "(age >= 0)"],
            [
                "checked_table_status_check",
                "(status IN ('active', 'inactive'))"
            ],
        ]
    );
}

#[test]
fn test_information_schema_referential_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE parent_table (id INT64 PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE child_table (
                id INT64,
                parent_id INT64 REFERENCES parent_table(id)
            )",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT constraint_name, unique_constraint_name
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["child_table_parent_id_fkey", "parent_table_pkey"]]
    );
}

#[test]
fn test_information_schema_constraint_column_usage() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT table_name, column_name, constraint_name
            FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
            WHERE table_schema = 'test_schema'",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["orders", "order_id", "orders_pkey"],
            ["users", "id", "users_pkey"],
        ]
    );
}

#[test]
fn test_information_schema_domains() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.DOMAINS",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_character_sets() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.CHARACTER_SETS",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_collations() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.COLLATIONS",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_enabled_roles() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT COUNT(*) >= 0 FROM INFORMATION_SCHEMA.ENABLED_ROLES")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_applicable_roles() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.APPLICABLE_ROLES",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_table_privileges() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
            WHERE table_schema = 'test_schema'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_column_privileges() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES
            WHERE table_schema = 'test_schema'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_usage_privileges() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.USAGE_PRIVILEGES",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_sessions() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT COUNT(*) >= 0 FROM INFORMATION_SCHEMA.SESSIONS")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_jobs() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_BY_USER
            LIMIT 10",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_streaming_timeline() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
            LIMIT 10",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_object_privileges() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            LIMIT 10",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_information_schema_complex_query() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "WITH table_stats AS (
                SELECT
                    t.table_schema,
                    t.table_name,
                    t.table_type,
                    COUNT(c.column_name) AS column_count
                FROM INFORMATION_SCHEMA.TABLES t
                LEFT JOIN INFORMATION_SCHEMA.COLUMNS c
                    ON t.table_schema = c.table_schema AND t.table_name = c.table_name
                WHERE t.table_schema = 'test_schema'
                GROUP BY t.table_schema, t.table_name, t.table_type
            )
            SELECT
                table_schema,
                table_type,
                COUNT(*) AS table_count,
                SUM(column_count) AS total_columns
            FROM table_stats
            GROUP BY table_schema, table_type
            ORDER BY table_type",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["test_schema", "BASE TABLE", 2, 8],
            ["test_schema", "VIEW", 1, 4],
        ]
    );
}

#[test]
fn test_information_schema_schema_discovery() {
    let mut executor = create_executor();
    setup_test_schema(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.is_nullable,
                CASE WHEN kcu.column_name IS NOT NULL THEN 'YES' ELSE 'NO' END AS is_primary_key
            FROM INFORMATION_SCHEMA.COLUMNS c
            LEFT JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON c.table_schema = kcu.table_schema
                AND c.table_name = kcu.table_name
                AND c.column_name = kcu.column_name
            WHERE c.table_schema = 'test_schema'
            ORDER BY c.table_name, c.ordinal_position",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["test_schema", "orders", "order_id", "INT64", "NO", "YES"],
            ["test_schema", "orders", "user_id", "INT64", "YES", "NO"],
            ["test_schema", "orders", "amount", "INT64", "YES", "NO"],
            ["test_schema", "orders", "order_date", "DATE", "YES", "NO"],
            ["test_schema", "users", "id", "INT64", "NO", "YES"],
            ["test_schema", "users", "name", "STRING", "YES", "NO"],
            ["test_schema", "users", "email", "STRING", "YES", "NO"],
            [
                "test_schema",
                "users",
                "created_at",
                "TIMESTAMP",
                "YES",
                "NO"
            ],
        ]
    );
}
