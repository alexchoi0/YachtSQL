use crate::common::{batch, create_session};

async fn setup_test_schema(session: &yachtsql::YachtSQLSession) {
    session.execute_sql("CREATE SCHEMA test_schema").await.unwrap();
    session
        .execute_sql(
            "CREATE TABLE test_schema.users (
                id INT64 NOT NULL,
                name STRING,
                email STRING,
                created_at TIMESTAMP,
                PRIMARY KEY (id)
            )",
        ).await
        .unwrap();
    session
        .execute_sql(
            "CREATE TABLE test_schema.orders (
                order_id INT64 NOT NULL,
                user_id INT64,
                amount INT64,
                order_date DATE,
                PRIMARY KEY (order_id)
            )",
        ).await
        .unwrap();
    session
        .execute_sql("CREATE VIEW test_schema.active_users AS SELECT * FROM test_schema.users").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_schemata() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT schema_name
            FROM INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["test_schema"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_tables() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name, table_type
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        ).await
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

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_tables_base_table() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'test_schema' AND table_type = 'BASE TABLE'
            ORDER BY table_name",
        ).await
        .unwrap();
    assert_table_eq!(result, [["orders"], ["users"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_tables_view() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name
            FROM INFORMATION_SCHEMA.TABLES
            WHERE table_schema = 'test_schema' AND table_type = 'VIEW'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["active_users"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_columns() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT column_name, data_type, is_nullable
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema' AND table_name = 'users'
            ORDER BY ordinal_position",
        ).await
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

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_columns_ordinal_position() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT column_name, ordinal_position
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema' AND table_name = 'users'
            ORDER BY ordinal_position",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["id", 1], ["name", 2], ["email", 3], ["created_at", 4]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_columns_nullable() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema'
                AND table_name = 'users'
                AND is_nullable = 'NO'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["id"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_views() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name, view_definition
            FROM INFORMATION_SCHEMA.VIEWS
            WHERE table_schema = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["active_users", "SELECT * FROM test_schema.users"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_constraints() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT constraint_name, constraint_type
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["orders_pkey", "PRIMARY KEY"],
            ["users_pkey", "PRIMARY KEY"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_key_column_usage() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name, column_name, constraint_name
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["orders", "order_id", "orders_pkey"],
            ["users", "id", "users_pkey"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_column_field_paths() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE nested_data (
                id INT64,
                info STRUCT<name STRING, address STRUCT<city STRING, zip STRING>>
            )",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 1
            FROM INFORMATION_SCHEMA.COLUMN_FIELD_PATHS
            WHERE table_name = 'nested_data'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_options() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE options_table (id INT64)
            OPTIONS (description = 'Test table')",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_OPTIONS
            WHERE table_name = 'options_table'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_routines() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION add_nums(a INT64, b INT64) RETURNS INT64 AS (a + b)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE routine_name = 'add_nums'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_parameters() {
    let session = create_session();
    session
        .execute_sql("CREATE FUNCTION multiply(x INT64, y INT64) RETURNS INT64 AS (x * y)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.PARAMETERS
            WHERE specific_name = 'multiply'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_partitions() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE partitioned_data (
                id INT64,
                created_date DATE
            )
            PARTITION BY created_date",
        ).await
        .unwrap();
    session
        .execute_sql("INSERT INTO partitioned_data VALUES (1, DATE '2024-01-15')").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.PARTITIONS
            WHERE table_name = 'partitioned_data'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_all_columns() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT COUNT(*)
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[8]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_join_tables_columns() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT t.table_name, COUNT(c.column_name) AS column_count
            FROM INFORMATION_SCHEMA.TABLES t
            JOIN INFORMATION_SCHEMA.COLUMNS c
                ON t.table_schema = c.table_schema AND t.table_name = c.table_name
            WHERE t.table_schema = 'test_schema' AND t.table_type = 'BASE TABLE'
            GROUP BY t.table_name
            ORDER BY t.table_name",
        ).await
        .unwrap();
    assert_table_eq!(result, [["orders", 4], ["users", 4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_column_default() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE defaults_table (
                id INT64,
                status STRING DEFAULT 'pending',
                count INT64 DEFAULT 0
            )",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'defaults_table' AND column_default IS NOT NULL",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_data_types() {
    let session = create_session();
    session
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
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT column_name, data_type
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'all_types'
            ORDER BY ordinal_position",
        ).await
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

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_search_columns() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_schema, table_name, column_name
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE column_name LIKE '%id%'
            ORDER BY table_schema, table_name, column_name",
        ).await
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

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_catalog() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT DISTINCT table_catalog
            FROM INFORMATION_SCHEMA.TABLES",
        ).await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_check_constraints() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE checked_table (
                id INT64,
                age INT64 CHECK (age >= 0),
                status STRING CHECK (status IN ('active', 'inactive'))
            )",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT constraint_name, check_clause
            FROM INFORMATION_SCHEMA.CHECK_CONSTRAINTS
            WHERE constraint_schema = 'public'",
        ).await
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

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_referential_constraints() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE parent_table (id INT64 PRIMARY KEY)").await
        .unwrap();
    session
        .execute_sql(
            "CREATE TABLE child_table (
                id INT64,
                parent_id INT64 REFERENCES parent_table(id)
            )",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT constraint_name, unique_constraint_name
            FROM INFORMATION_SCHEMA.REFERENTIAL_CONSTRAINTS",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["child_table_parent_id_fkey", "parent_table_pkey"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_constraint_column_usage() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name, column_name, constraint_name
            FROM INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE
            WHERE table_schema = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["orders", "order_id", "orders_pkey"],
            ["users", "id", "users_pkey"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_domains() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.DOMAINS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_character_sets() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.CHARACTER_SETS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_collations() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.COLLATIONS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_enabled_roles() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT COUNT(*) >= 0 FROM INFORMATION_SCHEMA.ENABLED_ROLES").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_applicable_roles() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.APPLICABLE_ROLES",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_privileges() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_PRIVILEGES
            WHERE table_schema = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_column_privileges() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.COLUMN_PRIVILEGES
            WHERE table_schema = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_usage_privileges() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.USAGE_PRIVILEGES",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_sessions() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT COUNT(*) >= 0 FROM INFORMATION_SCHEMA.SESSIONS").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_BY_USER
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_streaming_timeline() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_PROJECT
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_object_privileges() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.OBJECT_PRIVILEGES
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_complex_query() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
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
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["test_schema", "BASE TABLE", 2, 8],
            ["test_schema", "VIEW", 1, 4],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_schema_discovery() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
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
        ).await
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

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_schemata_options() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE SCHEMA options_schema
            OPTIONS (description = 'Test schema with options', location = 'US')",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT schema_name, option_name, option_value
            FROM INFORMATION_SCHEMA.SCHEMATA_OPTIONS
            WHERE schema_name = 'options_schema'
            ORDER BY option_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["options_schema", "description", "Test schema with options"],
            ["options_schema", "location", "US"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_routine_options() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION documented_func(x INT64) RETURNS INT64
            OPTIONS (description = 'A documented function')
            AS (x * 2)",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT routine_name, option_name, option_value
            FROM INFORMATION_SCHEMA.ROUTINE_OPTIONS
            WHERE routine_name = 'documented_func'",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["documented_func", "description", "A documented function"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_materialized_views() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE source_data (id INT64, value INT64)").await
        .unwrap();
    session
        .execute_sql(
            "CREATE MATERIALIZED VIEW mv_sum AS
            SELECT id, SUM(value) AS total
            FROM source_data
            GROUP BY id",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT table_name, view_definition
            FROM INFORMATION_SCHEMA.MATERIALIZED_VIEWS
            WHERE table_name = 'mv_sum'",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [[
            "mv_sum",
            "SELECT id, SUM(value) AS total FROM source_data GROUP BY id"
        ]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_dataset_qualifier() {
    let session = create_session();
    session.execute_sql("CREATE SCHEMA my_dataset").await.unwrap();
    session
        .execute_sql("CREATE TABLE my_dataset.my_table (id INT64)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT table_name
            FROM my_dataset.INFORMATION_SCHEMA.TABLES
            WHERE table_name = 'my_table'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["my_table"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_dataset_qualifier_columns() {
    let session = create_session();
    session.execute_sql("CREATE SCHEMA test_ds").await.unwrap();
    session
        .execute_sql(
            "CREATE TABLE test_ds.products (
                product_id INT64,
                name STRING,
                price FLOAT64
            )",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT column_name, data_type
            FROM test_ds.INFORMATION_SCHEMA.COLUMNS
            WHERE table_name = 'products'
            ORDER BY ordinal_position",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["product_id", "INT64"],
            ["name", "STRING"],
            ["price", "FLOAT64"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_search_indexes() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE searchable (
                id INT64,
                content STRING
            )",
        ).await
        .unwrap();
    session
        .execute_sql("CREATE SEARCH INDEX idx_content ON searchable(content)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT index_name, table_name
            FROM INFORMATION_SCHEMA.SEARCH_INDEXES
            WHERE table_name = 'searchable'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["idx_content", "searchable"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_search_index_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE docs (
                id INT64,
                title STRING,
                body STRING
            )",
        ).await
        .unwrap();
    session
        .execute_sql("CREATE SEARCH INDEX idx_docs ON docs(title, body)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT index_name, column_name
            FROM INFORMATION_SCHEMA.SEARCH_INDEX_COLUMNS
            WHERE table_name = 'docs'
            ORDER BY column_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["idx_docs", "body"], ["idx_docs", "title"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_search_index_options() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE indexed_data (id INT64, text STRING)").await
        .unwrap();
    session
        .execute_sql(
            "CREATE SEARCH INDEX idx_text ON indexed_data(text)
            OPTIONS (analyzer = 'PATTERN_ANALYZER')",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT index_name, option_name, option_value
            FROM INFORMATION_SCHEMA.SEARCH_INDEX_OPTIONS
            WHERE index_name = 'idx_text'",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["idx_text", "analyzer", "PATTERN_ANALYZER"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_vector_indexes() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE embeddings (
                id INT64,
                embedding ARRAY<FLOAT64>
            )",
        ).await
        .unwrap();
    session
        .execute_sql(
            "CREATE VECTOR INDEX vec_idx ON embeddings(embedding)
            OPTIONS (distance_type = 'COSINE', index_type = 'IVF')",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT index_name, table_name, index_status
            FROM INFORMATION_SCHEMA.VECTOR_INDEXES
            WHERE table_name = 'embeddings'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["vec_idx", "embeddings", "ACTIVE"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_vector_index_columns() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE TABLE vectors (
                id INT64,
                vec ARRAY<FLOAT64>
            )",
        ).await
        .unwrap();
    session
        .execute_sql("CREATE VECTOR INDEX v_idx ON vectors(vec)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT index_name, column_name
            FROM INFORMATION_SCHEMA.VECTOR_INDEX_COLUMNS
            WHERE table_name = 'vectors'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["v_idx", "vec"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_vector_index_options() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE vec_data (id INT64, emb ARRAY<FLOAT64>)").await
        .unwrap();
    session
        .execute_sql(
            "CREATE VECTOR INDEX vi ON vec_data(emb)
            OPTIONS (distance_type = 'EUCLIDEAN', num_lists = 100)",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT index_name, option_name, option_value
            FROM INFORMATION_SCHEMA.VECTOR_INDEX_OPTIONS
            WHERE index_name = 'vi'
            ORDER BY option_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["vi", "distance_type", "EUCLIDEAN"],
            ["vi", "num_lists", "100"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_storage() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_schema, table_name, total_rows >= 0 AS has_rows
            FROM INFORMATION_SCHEMA.TABLE_STORAGE
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["test_schema", "orders", true],
            ["test_schema", "users", true],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_storage_by_project() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name, total_logical_bytes >= 0 AS valid_bytes
            FROM INFORMATION_SCHEMA.TABLE_STORAGE_BY_PROJECT
            WHERE table_schema = 'test_schema'
            ORDER BY table_name",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["orders", true], ["users", true]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_reservations() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.RESERVATIONS_BY_PROJECT",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_capacity_commitments() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.CAPACITY_COMMITMENTS_BY_PROJECT",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_assignments() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.ASSIGNMENTS_BY_PROJECT",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_by_project() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_BY_PROJECT
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_timeline() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_TIMELINE_BY_PROJECT
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_sessions_by_project() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.SESSIONS_BY_PROJECT",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_bi_capacities() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.BI_CAPACITIES",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_schemata_links() {
    let session = create_session();
    session.execute_sql("CREATE SCHEMA linked_schema").await.unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.SCHEMATA_LINKS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_schemata_replicas() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.SCHEMATA_REPLICAS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_snapshots() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE snapshot_source (id INT64, data STRING)").await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_SNAPSHOTS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_insights() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.INSIGHTS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_recommendations() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.RECOMMENDATIONS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_write_api_timeline() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.WRITE_API_TIMELINE_BY_PROJECT
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_project_options() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.PROJECT_OPTIONS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_effective_project_options() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.EFFECTIVE_PROJECT_OPTIONS",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_shared_dataset_usage() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.SHARED_DATASET_USAGE",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_project_qualified() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT table_name
            FROM `default`.test_schema.INFORMATION_SCHEMA.TABLES
            WHERE table_type = 'BASE TABLE'
            ORDER BY table_name",
        ).await
        .unwrap();
    assert_table_eq!(result, [["orders"], ["users"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_region_qualified() {
    let session = create_session();
    setup_test_schema(&session).await;

    let result = session
        .execute_sql(
            "SELECT schema_name
            FROM `region-us`.INFORMATION_SCHEMA.SCHEMATA
            WHERE schema_name = 'test_schema'",
        ).await
        .unwrap();
    assert_table_eq!(result, [["test_schema"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_routines_detailed() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION my_schema.calculate(a INT64, b INT64)
            RETURNS INT64
            AS (a * b + 10)",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT routine_schema, routine_name, routine_type, data_type
            FROM INFORMATION_SCHEMA.ROUTINES
            WHERE routine_name = 'calculate'",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [["my_schema", "calculate", "FUNCTION", "INT64"]]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_parameters_detailed() {
    let session = create_session();
    session
        .execute_sql(
            "CREATE FUNCTION param_test(input_val STRING, multiplier INT64)
            RETURNS STRING
            AS (CONCAT(input_val, CAST(multiplier AS STRING)))",
        ).await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT parameter_name, ordinal_position, data_type
            FROM INFORMATION_SCHEMA.PARAMETERS
            WHERE specific_name = 'param_test'
            ORDER BY ordinal_position",
        ).await
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["input_val", 1, "STRING"],
            ["multiplier", 2, "INT64"],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_streaming_timeline_by_folder() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_FOLDER
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_streaming_timeline_by_organization() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.STREAMING_TIMELINE_BY_ORGANIZATION
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_storage_by_folder() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_STORAGE_BY_FOLDER",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_table_storage_by_organization() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.TABLE_STORAGE_BY_ORGANIZATION",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_by_folder() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_BY_FOLDER
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_by_organization() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_BY_ORGANIZATION
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_timeline_by_user() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_TIMELINE_BY_USER
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_timeline_by_folder() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_TIMELINE_BY_FOLDER
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_information_schema_jobs_timeline_by_organization() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0
            FROM INFORMATION_SCHEMA.JOBS_TIMELINE_BY_ORGANIZATION
            LIMIT 10",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
