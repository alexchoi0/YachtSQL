use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();

    executor
        .execute_sql("CREATE TABLE my_schema.users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.users VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM my_schema.users")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_create_schema_if_not_exists() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();

    let result = executor.execute_sql("CREATE SCHEMA IF NOT EXISTS my_schema");
    assert!(result.is_ok());
}

#[test]
fn test_drop_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA temp_schema").unwrap();
    executor.execute_sql("DROP SCHEMA temp_schema").unwrap();

    let result = executor.execute_sql("CREATE TABLE temp_schema.test (id INT64)");
    assert!(result.is_err());
}

#[test]
fn test_drop_schema_if_exists() {
    let mut executor = create_executor();

    let result = executor.execute_sql("DROP SCHEMA IF EXISTS nonexistent_schema");
    assert!(result.is_ok());
}

#[test]
fn test_drop_schema_cascade() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.table1 (id INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.table2 (id INT64)")
        .unwrap();

    executor
        .execute_sql("DROP SCHEMA my_schema CASCADE")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM my_schema.table1");
    assert!(result.is_err());
}

#[test]
fn test_schema_qualified_table_names() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA schema1").unwrap();
    executor.execute_sql("CREATE SCHEMA schema2").unwrap();

    executor
        .execute_sql("CREATE TABLE schema1.data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema2.data (id INT64, value INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO schema1.data VALUES (1, 100)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema2.data VALUES (1, 200)")
        .unwrap();

    let result1 = executor
        .execute_sql("SELECT value FROM schema1.data")
        .unwrap();
    let result2 = executor
        .execute_sql("SELECT value FROM schema2.data")
        .unwrap();

    assert_table_eq!(result1, [[100]]);
    assert_table_eq!(result2, [[200]]);
}

#[test]
fn test_schema_with_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SCHEMA my_schema OPTIONS(description='My test schema')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE my_schema.test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT 1 FROM my_schema.test");
    assert!(result.is_ok());
}

#[test]
fn test_alter_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();

    executor
        .execute_sql("ALTER SCHEMA my_schema SET OPTIONS(description='Updated description')")
        .unwrap();
}

#[test]
fn test_cross_schema_join() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA schema_a").unwrap();
    executor.execute_sql("CREATE SCHEMA schema_b").unwrap();

    executor
        .execute_sql("CREATE TABLE schema_a.users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema_b.orders (id INT64, user_id INT64, amount INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO schema_a.users VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema_b.orders VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT u.name, SUM(o.amount) AS total FROM schema_a.users u JOIN schema_b.orders o ON u.id = o.user_id GROUP BY u.name ORDER BY u.name")
        .unwrap();
    assert_table_eq!(result, [["Alice", 300], ["Bob", 150]]);
}

#[test]
fn test_create_view_in_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.data VALUES (1, 10), (2, 20)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VIEW my_schema.data_view AS SELECT * FROM my_schema.data WHERE value > 15",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM my_schema.data_view")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_default_schema() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE public_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO public_table VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM public_table").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_schema_search_path() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.test (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.test VALUES (1)")
        .unwrap();

    executor
        .execute_sql("SET search_path TO my_schema")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM test").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_truncate_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10), (2, 20), (3, 30)")
        .unwrap();

    executor.execute_sql("TRUNCATE TABLE data").unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM data").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_truncate_table_in_schema() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA my_schema").unwrap();
    executor
        .execute_sql("CREATE TABLE my_schema.data (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO my_schema.data VALUES (1), (2), (3)")
        .unwrap();

    executor
        .execute_sql("TRUNCATE TABLE my_schema.data")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM my_schema.data")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore]
fn test_undrop_schema() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA dropped_schema")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE dropped_schema.data (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dropped_schema.data VALUES (1)")
        .unwrap();

    executor
        .execute_sql("DROP SCHEMA dropped_schema CASCADE")
        .unwrap();

    executor
        .execute_sql("UNDROP SCHEMA dropped_schema")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM dropped_schema.data")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_undrop_schema_if_not_exists() {
    let mut executor = create_executor();

    executor.execute_sql("CREATE SCHEMA temp_schema").unwrap();
    executor.execute_sql("DROP SCHEMA temp_schema").unwrap();

    let result = executor.execute_sql("UNDROP SCHEMA IF NOT EXISTS temp_schema");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_create_schema_or_replace() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA replace_schema")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE replace_schema.data (id INT64)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE OR REPLACE SCHEMA replace_schema OPTIONS(description='Replaced schema')",
        )
        .unwrap();

    let result = executor.execute_sql("CREATE TABLE replace_schema.new_data (id INT64)");
    assert!(result.is_ok());
}

#[test]
fn test_drop_schema_restrict() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA restrict_schema")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE restrict_schema.data (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP SCHEMA restrict_schema RESTRICT");
    assert!(result.is_err());
}

#[test]
fn test_schema_with_default_collation() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA collation_schema OPTIONS(default_collation = 'und:ci')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE collation_schema.data (name STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT 1 FROM collation_schema.data");
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_create_row_access_policy() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE policy_table (id INT64, owner STRING, data STRING)")
        .unwrap();

    executor
        .execute_sql(
            "INSERT INTO policy_table VALUES (1, 'alice', 'secret1'), (2, 'bob', 'secret2')",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE ROW ACCESS POLICY my_policy
            ON policy_table
            GRANT TO ('user:alice@example.com')
            FILTER USING (owner = 'alice')",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_row_access_policy_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE policy_table2 (id INT64)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE ROW ACCESS POLICY IF NOT EXISTS test_policy
            ON policy_table2
            GRANT TO ('allAuthenticatedUsers')
            FILTER USING (TRUE)",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_or_replace_row_access_policy() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE policy_table3 (id INT64, category STRING)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE ROW ACCESS POLICY category_policy
            ON policy_table3
            GRANT TO ('allUsers')
            FILTER USING (category = 'public')",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE OR REPLACE ROW ACCESS POLICY category_policy
            ON policy_table3
            GRANT TO ('allAuthenticatedUsers')
            FILTER USING (category IN ('public', 'internal'))",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_row_access_policy() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE policy_drop (id INT64)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE ROW ACCESS POLICY drop_policy
            ON policy_drop
            GRANT TO ('allUsers')
            FILTER USING (TRUE)",
        )
        .unwrap();

    executor
        .execute_sql("DROP ROW ACCESS POLICY drop_policy ON policy_drop")
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_all_row_access_policies() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE multi_policy (id INT64)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE ROW ACCESS POLICY policy1 ON multi_policy GRANT TO ('user:a@b.com') FILTER USING (TRUE)",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE ROW ACCESS POLICY policy2 ON multi_policy GRANT TO ('user:c@d.com') FILTER USING (TRUE)",
        )
        .unwrap();

    executor
        .execute_sql("DROP ALL ROW ACCESS POLICIES ON multi_policy")
        .unwrap();
}

#[test]
#[ignore]
fn test_create_search_index() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE search_data (id INT64, content STRING)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE SEARCH INDEX my_search_idx
            ON search_data (content)",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_search_index_all_columns() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE search_all (id INT64, title STRING, body STRING)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE SEARCH INDEX full_text_idx
            ON search_all (ALL COLUMNS)",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_search_index_with_options() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE search_opts (id INT64, text STRING)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE SEARCH INDEX opts_idx
            ON search_opts (text)
            OPTIONS (analyzer = 'LOG_ANALYZER')",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_search_index() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE search_drop (id INT64, text STRING)")
        .unwrap();

    executor
        .execute_sql("CREATE SEARCH INDEX drop_idx ON search_drop (text)")
        .unwrap();

    executor
        .execute_sql("DROP SEARCH INDEX drop_idx ON search_drop")
        .unwrap();
}

#[test]
#[ignore]
fn test_create_vector_index() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE vector_data (id INT64, embedding ARRAY<FLOAT64>)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VECTOR INDEX vector_idx
            ON vector_data (embedding)
            OPTIONS (distance_type = 'COSINE', index_type = 'IVF')",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_vector_index_stored_column() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE vector_stored (id INT64, name STRING, embedding ARRAY<FLOAT64>)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VECTOR INDEX stored_idx
            ON vector_stored (embedding)
            STORING (name)
            OPTIONS (distance_type = 'EUCLIDEAN')",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_vector_index() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE vector_drop (id INT64, emb ARRAY<FLOAT64>)")
        .unwrap();

    executor
        .execute_sql("CREATE VECTOR INDEX drop_vidx ON vector_drop (emb)")
        .unwrap();

    executor
        .execute_sql("DROP VECTOR INDEX drop_vidx ON vector_drop")
        .unwrap();
}

#[test]
fn test_create_schema_with_location() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA location_schema OPTIONS(location='us')")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE location_schema.data (id INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT 1 FROM location_schema.data");
    assert!(result.is_ok());
}

#[test]
fn test_create_schema_with_default_table_expiration() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA expiring_schema OPTIONS(default_table_expiration_days=3.75)")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE expiring_schema.temp (id INT64)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO expiring_schema.temp VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM expiring_schema.temp")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_schema_with_labels() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE SCHEMA labeled_schema OPTIONS(
                labels=[('org_unit', 'development'), ('cost_center', 'eng')]
            )",
        )
        .unwrap();

    executor
        .execute_sql("CREATE TABLE labeled_schema.test (id INT64)")
        .unwrap();

    let result = executor.execute_sql("SELECT 1 FROM labeled_schema.test");
    assert!(result.is_ok());
}

#[test]
fn test_create_schema_with_default_partition_expiration() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE SCHEMA partition_schema OPTIONS(
                default_partition_expiration_days=7
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE partition_schema.partitioned (
                id INT64,
                dt DATE
            ) PARTITION BY dt",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO partition_schema.partitioned VALUES (1, DATE '2024-01-15')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM partition_schema.partitioned")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_create_schema_with_storage_billing_model() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE SCHEMA physical_billing OPTIONS(
                storage_billing_model='PHYSICAL'
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_schema_with_max_time_travel() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE SCHEMA time_travel_schema OPTIONS(
                max_time_travel_hours=48
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_alter_schema_set_default_collate() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA collate_schema")
        .unwrap();

    executor
        .execute_sql("ALTER SCHEMA collate_schema SET DEFAULT COLLATE 'und:ci'")
        .unwrap();

    executor
        .execute_sql("CREATE TABLE collate_schema.test (name STRING)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO collate_schema.test VALUES ('Hello')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM collate_schema.test WHERE name = 'hello'")
        .unwrap();
    assert_table_eq!(result, [["Hello"]]);
}

#[test]
fn test_alter_schema_set_multiple_options() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE SCHEMA multi_opts_schema")
        .unwrap();

    executor
        .execute_sql(
            "ALTER SCHEMA multi_opts_schema SET OPTIONS(
                description='Updated schema',
                default_table_expiration_days=30,
                labels=[('env', 'test')]
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_external_schema() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE EXTERNAL SCHEMA ext_schema
            OPTIONS (
                external_source = 'aws-glue://arn:aws:glue:us-east-1:123456789:database/test',
                location = 'aws-us-east-1'
            )",
        )
        .unwrap();

    executor
        .execute_sql("DROP EXTERNAL SCHEMA ext_schema")
        .unwrap();
}

#[test]
#[ignore]
fn test_create_vector_index_tree_ah() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE tree_ah_data (id INT64, embedding ARRAY<FLOAT64>)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VECTOR INDEX tree_ah_idx
            ON tree_ah_data (embedding)
            OPTIONS (
                index_type = 'TREE_AH',
                distance_type = 'EUCLIDEAN',
                tree_ah_options = '{\"leaf_node_embedding_count\": 1000, \"normalization_type\": \"L2\"}'
            )",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_create_search_index_if_not_exists() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE search_ine (id INT64, content STRING)")
        .unwrap();

    executor
        .execute_sql("CREATE SEARCH INDEX idx1 ON search_ine (content)")
        .unwrap();

    executor
        .execute_sql("CREATE SEARCH INDEX IF NOT EXISTS idx1 ON search_ine (content)")
        .unwrap();
}

#[test]
#[ignore]
fn test_create_search_index_with_data_types() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE search_types (
                id INT64,
                title STRING,
                count INT64,
                created_at TIMESTAMP
            )",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE SEARCH INDEX types_idx
            ON search_types (ALL COLUMNS)
            OPTIONS (data_types = ['STRING', 'INT64', 'TIMESTAMP'])",
        )
        .unwrap();
}

#[test]
#[ignore]
fn test_drop_search_index_if_exists() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("DROP SEARCH INDEX IF EXISTS nonexistent_idx ON nonexistent_table");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore]
fn test_create_or_replace_vector_index() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE vec_replace (id INT64, emb ARRAY<FLOAT64>)")
        .unwrap();

    executor
        .execute_sql(
            "CREATE VECTOR INDEX vec_idx ON vec_replace (emb)
            OPTIONS (index_type = 'IVF')",
        )
        .unwrap();

    executor
        .execute_sql(
            "CREATE OR REPLACE VECTOR INDEX vec_idx ON vec_replace (emb)
            OPTIONS (index_type = 'IVF', distance_type = 'COSINE')",
        )
        .unwrap();
}
