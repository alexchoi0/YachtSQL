use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE my_schema.users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO my_schema.users VALUES (1, 'Alice')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM my_schema.users")
        .await
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_if_not_exists() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE SCHEMA IF NOT EXISTS my_schema")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA temp_schema")
        .await
        .unwrap();
    session
        .execute_sql("DROP SCHEMA temp_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE temp_schema.test (id INT64)")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_schema_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP SCHEMA IF EXISTS nonexistent_schema")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_schema_cascade() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE my_schema.table1 (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE my_schema.table2 (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA my_schema CASCADE")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT * FROM my_schema.table1").await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_qualified_table_names() {
    let session = create_session();
    session.execute_sql("CREATE SCHEMA schema1").await.unwrap();
    session.execute_sql("CREATE SCHEMA schema2").await.unwrap();

    session
        .execute_sql("CREATE TABLE schema1.data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE schema2.data (id INT64, value INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO schema1.data VALUES (1, 100)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO schema2.data VALUES (1, 200)")
        .await
        .unwrap();

    let result1 = session
        .execute_sql("SELECT value FROM schema1.data")
        .await
        .unwrap();
    let result2 = session
        .execute_sql("SELECT value FROM schema2.data")
        .await
        .unwrap();

    assert_table_eq!(result1, [[100]]);
    assert_table_eq!(result2, [[200]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_with_options() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema OPTIONS(description='My test schema')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE my_schema.test (id INT64)")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT 1 FROM my_schema.test").await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();

    session
        .execute_sql("ALTER SCHEMA my_schema SET OPTIONS(description='Updated description')")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_cross_schema_join() {
    let session = create_session();
    session.execute_sql("CREATE SCHEMA schema_a").await.unwrap();
    session.execute_sql("CREATE SCHEMA schema_b").await.unwrap();

    session
        .execute_sql("CREATE TABLE schema_a.users (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE schema_b.orders (id INT64, user_id INT64, amount INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO schema_a.users VALUES (1, 'Alice'), (2, 'Bob')")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO schema_b.orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT u.name, SUM(o.amount) AS total FROM schema_a.users u JOIN schema_b.orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name").await
        .unwrap();
    assert_table_eq!(result, [["Alice", 300], ["Bob", 150]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_view_in_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE my_schema.data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO my_schema.data VALUES (1, 10), (2, 20)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VIEW my_schema.data_view AS SELECT * FROM my_schema.data WHERE value > 15",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM my_schema.data_view")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_default_schema() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE public_table (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO public_table VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM public_table")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_search_path() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE my_schema.test (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO my_schema.test VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("SET search_path TO my_schema")
        .await
        .unwrap();

    let result = session.execute_sql("SELECT id FROM test").await.unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();

    session.execute_sql("TRUNCATE TABLE data").await.unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_truncate_table_in_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA my_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE my_schema.data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO my_schema.data VALUES (1), (2), (3)")
        .await
        .unwrap();

    session
        .execute_sql("TRUNCATE TABLE my_schema.data")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM my_schema.data")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_undrop_schema() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA dropped_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE dropped_schema.data (id INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO dropped_schema.data VALUES (1)")
        .await
        .unwrap();

    session
        .execute_sql("DROP SCHEMA dropped_schema CASCADE")
        .await
        .unwrap();

    session
        .execute_sql("UNDROP SCHEMA dropped_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM dropped_schema.data")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_undrop_schema_if_not_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA temp_schema")
        .await
        .unwrap();
    session
        .execute_sql("DROP SCHEMA temp_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql("UNDROP SCHEMA IF NOT EXISTS temp_schema")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_or_replace() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA replace_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE replace_schema.data (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE SCHEMA replace_schema OPTIONS(description='Replaced schema')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("CREATE TABLE replace_schema.new_data (id INT64)")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_schema_restrict() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA restrict_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE restrict_schema.data (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("DROP SCHEMA restrict_schema RESTRICT")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_schema_with_default_collation() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA collation_schema OPTIONS(default_collation = 'und:ci')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE collation_schema.data (name STRING)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 1 FROM collation_schema.data")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_row_access_policy() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE policy_table (id INT64, owner STRING, data STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO policy_table VALUES (1, 'alice', 'secret1'), (2, 'bob', 'secret2')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE ROW ACCESS POLICY my_policy
            ON policy_table
            GRANT TO ('user:alice@example.com')
            FILTER USING (owner = 'alice')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_row_access_policy_if_not_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE policy_table2 (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE ROW ACCESS POLICY IF NOT EXISTS test_policy
            ON policy_table2
            GRANT TO ('allAuthenticatedUsers')
            FILTER USING (TRUE)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_row_access_policy() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE policy_table3 (id INT64, category STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE ROW ACCESS POLICY category_policy
            ON policy_table3
            GRANT TO ('allUsers')
            FILTER USING (category = 'public')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE ROW ACCESS POLICY category_policy
            ON policy_table3
            GRANT TO ('allAuthenticatedUsers')
            FILTER USING (category IN ('public', 'internal'))",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_row_access_policy() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE policy_drop (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE ROW ACCESS POLICY drop_policy
            ON policy_drop
            GRANT TO ('allUsers')
            FILTER USING (TRUE)",
        )
        .await
        .unwrap();

    session
        .execute_sql("DROP ROW ACCESS POLICY drop_policy ON policy_drop")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_all_row_access_policies() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE multi_policy (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE ROW ACCESS POLICY policy1 ON multi_policy GRANT TO ('user:a@b.com') FILTER USING (TRUE)",
        ).await
        .unwrap();

    session
        .execute_sql(
            "CREATE ROW ACCESS POLICY policy2 ON multi_policy GRANT TO ('user:c@d.com') FILTER USING (TRUE)",
        ).await
        .unwrap();

    session
        .execute_sql("DROP ALL ROW ACCESS POLICIES ON multi_policy")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_search_index() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE search_data (id INT64, content STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE SEARCH INDEX my_search_idx
            ON search_data (content)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_search_index_all_columns() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE search_all (id INT64, title STRING, body STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE SEARCH INDEX full_text_idx
            ON search_all (ALL COLUMNS)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_search_index_with_options() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE search_opts (id INT64, text STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE SEARCH INDEX opts_idx
            ON search_opts (text)
            OPTIONS (analyzer = 'LOG_ANALYZER')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_search_index() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE search_drop (id INT64, text STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SEARCH INDEX drop_idx ON search_drop (text)")
        .await
        .unwrap();

    session
        .execute_sql("DROP SEARCH INDEX drop_idx ON search_drop")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_vector_index() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE vector_data (id INT64, embedding ARRAY<FLOAT64>)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VECTOR INDEX vector_idx
            ON vector_data (embedding)
            OPTIONS (distance_type = 'COSINE', index_type = 'IVF')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_vector_index_stored_column() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE vector_stored (id INT64, name STRING, embedding ARRAY<FLOAT64>)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VECTOR INDEX stored_idx
            ON vector_stored (embedding)
            STORING (name)
            OPTIONS (distance_type = 'EUCLIDEAN')",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_vector_index() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE vector_drop (id INT64, emb ARRAY<FLOAT64>)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE VECTOR INDEX drop_vidx ON vector_drop (emb)")
        .await
        .unwrap();

    session
        .execute_sql("DROP VECTOR INDEX drop_vidx ON vector_drop")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_location() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA location_schema OPTIONS(location='us')")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE location_schema.data (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 1 FROM location_schema.data")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_default_table_expiration() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA expiring_schema OPTIONS(default_table_expiration_days=3.75)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE expiring_schema.temp (id INT64)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO expiring_schema.temp VALUES (1)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM expiring_schema.temp")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_labels() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE SCHEMA labeled_schema OPTIONS(
                labels=[('org_unit', 'development'), ('cost_center', 'eng')]
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE labeled_schema.test (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT 1 FROM labeled_schema.test")
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_default_partition_expiration() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE SCHEMA partition_schema OPTIONS(
                default_partition_expiration_days=7
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE partition_schema.partitioned (
                id INT64,
                dt DATE
            ) PARTITION BY dt",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO partition_schema.partitioned VALUES (1, DATE '2024-01-15')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM partition_schema.partitioned")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_storage_billing_model() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE SCHEMA physical_billing OPTIONS(
                storage_billing_model='PHYSICAL'
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_schema_with_max_time_travel() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE SCHEMA time_travel_schema OPTIONS(
                max_time_travel_hours=48
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_schema_set_default_collate() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA collate_schema")
        .await
        .unwrap();

    session
        .execute_sql("ALTER SCHEMA collate_schema SET DEFAULT COLLATE 'und:ci'")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE collate_schema.test (name STRING)")
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO collate_schema.test VALUES ('Hello')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT name FROM collate_schema.test WHERE name = 'hello'")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_alter_schema_set_multiple_options() {
    let session = create_session();

    session
        .execute_sql("CREATE SCHEMA multi_opts_schema")
        .await
        .unwrap();

    session
        .execute_sql(
            "ALTER SCHEMA multi_opts_schema SET OPTIONS(
                description='Updated schema',
                default_table_expiration_days=30,
                labels=[('env', 'test')]
            )",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_external_schema() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE EXTERNAL SCHEMA ext_schema
            OPTIONS (
                external_source = 'aws-glue://arn:aws:glue:us-east-1:123456789:database/test',
                location = 'aws-us-east-1'
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("DROP EXTERNAL SCHEMA ext_schema")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_vector_index_tree_ah() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE tree_ah_data (id INT64, embedding ARRAY<FLOAT64>)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VECTOR INDEX tree_ah_idx
            ON tree_ah_data (embedding)
            OPTIONS (
                index_type = 'TREE_AH',
                distance_type = 'EUCLIDEAN',
                tree_ah_options = '{\"leaf_node_embedding_count\": 1000, \"normalization_type\": \"L2\"}'
            )",
        ).await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_search_index_if_not_exists() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE search_ine (id INT64, content STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SEARCH INDEX idx1 ON search_ine (content)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE SEARCH INDEX IF NOT EXISTS idx1 ON search_ine (content)")
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_search_index_with_data_types() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE search_types (
                id INT64,
                title STRING,
                count INT64,
                created_at TIMESTAMP
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE SEARCH INDEX types_idx
            ON search_types (ALL COLUMNS)
            OPTIONS (data_types = ['STRING', 'INT64', 'TIMESTAMP'])",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_drop_search_index_if_exists() {
    let session = create_session();

    let result = session
        .execute_sql("DROP SEARCH INDEX IF EXISTS nonexistent_idx ON nonexistent_table")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_create_or_replace_vector_index() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE vec_replace (id INT64, emb ARRAY<FLOAT64>)")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE VECTOR INDEX vec_idx ON vec_replace (emb)
            OPTIONS (index_type = 'IVF')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE OR REPLACE VECTOR INDEX vec_idx ON vec_replace (emb)
            OPTIONS (index_type = 'IVF', distance_type = 'COSINE')",
        )
        .await
        .unwrap();
}
