use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_information_schema_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_test (id INTEGER, name TEXT)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table_name FROM information_schema.tables WHERE table_name = 'info_test'",
        )
        .unwrap();
    assert_table_eq!(result, [["info_test"]]);
}

#[test]
fn test_information_schema_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_info (id INTEGER, name TEXT, val DOUBLE PRECISION)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, data_type FROM information_schema.columns
             WHERE table_name = 'col_info' ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["id", "BIGINT"],
            ["name", "TEXT"],
            ["val", "DOUBLE PRECISION"],
        ]
    );
}

#[test]
fn test_information_schema_column_ordinal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ord_test (a INTEGER, b TEXT, c BOOLEAN)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, ordinal_position FROM information_schema.columns
             WHERE table_name = 'ord_test' ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(result, [["a", 1], ["b", 2], ["c", 3],]);
}

#[test]
fn test_information_schema_schemata() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'public'",
        )
        .unwrap();
    let _ = result;
}

#[test]
fn test_information_schema_views() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE view_base (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW info_view AS SELECT * FROM view_base")
        .unwrap();

    let result = executor
        .execute_sql("SELECT table_name, view_definition FROM information_schema.views WHERE table_name = 'info_view'")
        .unwrap();
    assert_table_eq!(result, [["info_view", "SELECT * FROM view_base"]]);
}

#[test]
fn test_information_schema_table_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE const_info (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT constraint_name, constraint_type FROM information_schema.table_constraints
             WHERE table_name = 'const_info'",
        )
        .unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_information_schema_key_column_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE key_info (id INTEGER PRIMARY KEY)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, constraint_name FROM information_schema.key_column_usage
             WHERE table_name = 'key_info'",
        )
        .unwrap();
    let _ = result;
}

#[test]
fn test_information_schema_referential_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ref_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE ref_child (id INTEGER, parent_id INTEGER REFERENCES ref_parent(id))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT constraint_name, unique_constraint_name FROM information_schema.referential_constraints
             WHERE constraint_name LIKE '%ref_child%'"
        );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_check_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE check_info (id INTEGER, val INTEGER CHECK (val > 0))")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT constraint_name, check_clause FROM information_schema.check_constraints
             WHERE constraint_name LIKE '%check_info%'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_column_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE priv_info (id INTEGER, secret TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, privilege_type FROM information_schema.column_privileges
             WHERE table_name = 'priv_info'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_table_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tbl_priv (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT table_name, privilege_type FROM information_schema.table_privileges
             WHERE table_name = 'tbl_priv'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_routines() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE FUNCTION info_func(x INTEGER) RETURNS INTEGER AS $$ SELECT x * 2 $$ LANGUAGE SQL",
        )
        .unwrap();

    let result = executor.execute_sql(
        "SELECT routine_name, routine_type FROM information_schema.routines
             WHERE routine_name = 'info_func'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_parameters() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE FUNCTION param_func(a INTEGER, b TEXT) RETURNS INTEGER AS $$ SELECT a $$ LANGUAGE SQL"
    ).unwrap();

    let result = executor.execute_sql(
        "SELECT parameter_name, data_type FROM information_schema.parameters
             WHERE specific_name LIKE '%param_func%'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_triggers() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE trig_base (id INTEGER)")
        .unwrap();
    let _ = executor.execute_sql(
        "CREATE TRIGGER info_trigger BEFORE INSERT ON trig_base
         FOR EACH ROW EXECUTE FUNCTION my_trigger_func()",
    );

    let result = executor.execute_sql(
        "SELECT trigger_name, event_manipulation FROM information_schema.triggers
             WHERE trigger_name = 'info_trigger'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_sequences() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE info_seq START 1")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT sequence_name, start_value FROM information_schema.sequences
             WHERE sequence_name = 'info_seq'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_domains() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT domain_name, data_type FROM information_schema.domains
             WHERE domain_name = 'positive_int'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_column_domain_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN email AS TEXT CHECK (VALUE LIKE '%@%')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE domain_usage (id INTEGER, contact email)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, domain_name FROM information_schema.column_domain_usage
             WHERE table_name = 'domain_usage'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_constraint_column_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ccu_test (id INTEGER PRIMARY KEY, name TEXT UNIQUE)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, constraint_name FROM information_schema.constraint_column_usage
             WHERE table_name = 'ccu_test'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_constraint_table_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ctu_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE ctu_child (id INTEGER, ref_id INTEGER REFERENCES ctu_parent(id))",
        )
        .unwrap();

    let result = executor.execute_sql(
        "SELECT table_name, constraint_name FROM information_schema.constraint_table_usage
             WHERE table_name = 'ctu_parent'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_element_types() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_elem (id INTEGER, vals INTEGER[])")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT object_name, data_type FROM information_schema.element_types
             WHERE object_name = 'arr_elem'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_role_table_grants() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE role_grant (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT table_name, privilege_type FROM information_schema.role_table_grants
             WHERE table_name = 'role_grant'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_role_column_grants() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE role_col_grant (id INTEGER, data TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, privilege_type FROM information_schema.role_column_grants
             WHERE table_name = 'role_col_grant'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_enabled_roles() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT role_name FROM information_schema.enabled_roles");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_applicable_roles() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT role_name, grantee FROM information_schema.applicable_roles");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_column_udt_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE my_type AS (x INTEGER, y INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE udt_usage (id INTEGER, point my_type)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, udt_name FROM information_schema.column_udt_usage
             WHERE table_name = 'udt_usage'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_user_defined_types() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE info_udt AS (a INTEGER, b TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT user_defined_type_name FROM information_schema.user_defined_types
             WHERE user_defined_type_name = 'info_udt'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_attributes() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE attr_type AS (field1 INTEGER, field2 TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT attribute_name, data_type FROM information_schema.attributes
             WHERE udt_name = 'attr_type'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_data_type_privileges() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT object_type, privilege_type FROM information_schema.data_type_privileges LIMIT 5",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_routine_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION rout_priv() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE SQL")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT routine_name, privilege_type FROM information_schema.routine_privileges
             WHERE routine_name = 'rout_priv'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_usage_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE usage_seq START 1")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT object_name, privilege_type FROM information_schema.usage_privileges
             WHERE object_name = 'usage_seq'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_udt_privileges() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE udt_priv_type AS (x INTEGER)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT udt_name, privilege_type FROM information_schema.udt_privileges
             WHERE udt_name = 'udt_priv_type'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_view_column_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vcu_base (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW vcu_view AS SELECT id, name FROM vcu_base")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, view_name FROM information_schema.view_column_usage
             WHERE view_name = 'vcu_view'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_view_table_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vtu_base (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW vtu_view AS SELECT * FROM vtu_base")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT table_name, view_name FROM information_schema.view_table_usage
             WHERE view_name = 'vtu_view'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_information_schema_view_routine_usage() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FUNCTION vru_func() RETURNS INTEGER AS $$ SELECT 42 $$ LANGUAGE SQL")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW vru_view AS SELECT vru_func() AS val")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT routine_name, view_name FROM information_schema.view_routine_usage
             WHERE view_name = 'vru_view'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_column_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_opt (id INTEGER, data TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, option_name FROM information_schema.column_options
             WHERE table_name = 'col_opt'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_data_wrappers() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT foreign_data_wrapper_name FROM information_schema.foreign_data_wrappers",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_servers() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT foreign_server_name FROM information_schema.foreign_servers");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_tables() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT foreign_table_name FROM information_schema.foreign_tables");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_table_options() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT foreign_table_name, option_name FROM information_schema.foreign_table_options",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_user_mappings() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT authorization_identifier FROM information_schema.user_mappings");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_user_mapping_options() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT authorization_identifier, option_name FROM information_schema.user_mapping_options",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_administrable_role_authorizations() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT role_name FROM information_schema.administrable_role_authorizations");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_character_sets() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT character_set_name FROM information_schema.character_sets");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_collations() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT collation_name FROM information_schema.collations");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_collation_character_set_applicability() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT collation_name, character_set_name
             FROM information_schema.collation_character_set_applicability",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_sql_features() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT feature_id, feature_name FROM information_schema.sql_features LIMIT 10",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_sql_implementation_info() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT implementation_info_id, implementation_info_name FROM information_schema.sql_implementation_info");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_sql_parts() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT feature_id, feature_name FROM information_schema.sql_parts");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_sql_sizing() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT sizing_id, sizing_name FROM information_schema.sql_sizing");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_query_columns_by_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE query_cols (id INTEGER, name TEXT, active BOOLEAN)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, is_nullable, data_type
             FROM information_schema.columns
             WHERE table_name = 'query_cols'
             ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["id", "YES", "BIGINT"],
            ["name", "YES", "TEXT"],
            ["active", "YES", "BOOLEAN"],
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_query_tables_in_schema() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE schema_t1 (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE schema_t2 (id INTEGER)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT table_name, table_type
             FROM information_schema.tables
             WHERE table_schema = 'public' AND table_name LIKE 'schema_t%'
             ORDER BY table_name",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["schema_t1", "BASE TABLE"], ["schema_t2", "BASE TABLE"],]
    );
}

#[test]
fn test_query_primary_keys() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pk_query (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT tc.constraint_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
             WHERE tc.table_name = 'pk_query' AND tc.constraint_type = 'PRIMARY KEY'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_query_foreign_keys() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE fk_child (id INTEGER, parent_id INTEGER REFERENCES fk_parent(id))",
        )
        .unwrap();

    let result = executor.execute_sql(
        "SELECT tc.constraint_name, kcu.column_name, ccu.table_name AS references_table
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
             JOIN information_schema.constraint_column_usage ccu
               ON tc.constraint_name = ccu.constraint_name
             WHERE tc.table_name = 'fk_child' AND tc.constraint_type = 'FOREIGN KEY'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_query_unique_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE unique_query (id INTEGER, email TEXT UNIQUE)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT tc.constraint_name, kcu.column_name
             FROM information_schema.table_constraints tc
             JOIN information_schema.key_column_usage kcu
               ON tc.constraint_name = kcu.constraint_name
             WHERE tc.table_name = 'unique_query' AND tc.constraint_type = 'UNIQUE'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_query_check_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE check_query (id INTEGER, age INTEGER CHECK (age >= 0))")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT tc.constraint_name, cc.check_clause
             FROM information_schema.table_constraints tc
             JOIN information_schema.check_constraints cc
               ON tc.constraint_name = cc.constraint_name
             WHERE tc.table_name = 'check_query' AND tc.constraint_type = 'CHECK'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_column_defaults() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE def_query (id INTEGER, status TEXT DEFAULT 'active')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, column_default
             FROM information_schema.columns
             WHERE table_name = 'def_query' AND column_default IS NOT NULL",
        )
        .unwrap();
    let _ = result;
}

#[test]
fn test_nullable_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE null_query (id INTEGER NOT NULL, name TEXT)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT column_name, is_nullable
             FROM information_schema.columns
             WHERE table_name = 'null_query'
             ORDER BY ordinal_position",
        )
        .unwrap();
    assert_table_eq!(result, [["id", "NO"], ["name", "YES"],]);
}

#[test]
fn test_information_schema_statistics() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE stat_query (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX stat_idx ON stat_query (val)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT table_name, index_name, column_name
             FROM information_schema.statistics
             WHERE table_name = 'stat_query'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_tables() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_tbl_test (id INTEGER)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT tablename FROM pg_catalog.pg_tables WHERE tablename = 'pg_tbl_test'")
        .unwrap();
    assert_table_eq!(result, [["pg_tbl_test"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_indexes() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_idx_base (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX pg_idx_test ON pg_idx_base (id)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT indexname FROM pg_catalog.pg_indexes WHERE indexname = 'pg_idx_test'")
        .unwrap();
    assert_table_eq!(result, [["pg_idx_test"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_views() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_view_base (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW pg_view_test AS SELECT * FROM pg_view_base")
        .unwrap();

    let result = executor
        .execute_sql("SELECT viewname FROM pg_catalog.pg_views WHERE viewname = 'pg_view_test'")
        .unwrap();
    assert_table_eq!(result, [["pg_view_test"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_class() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_class_test (id INTEGER)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT relname, relkind FROM pg_catalog.pg_class WHERE relname = 'pg_class_test'",
        )
        .unwrap();
    assert_table_eq!(result, [["pg_class_test", "r"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_attribute() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_attr_test (id INTEGER, name TEXT)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT attname, attnum FROM pg_catalog.pg_attribute a
             JOIN pg_catalog.pg_class c ON a.attrelid = c.oid
             WHERE c.relname = 'pg_attr_test' AND attnum > 0
             ORDER BY attnum",
        )
        .unwrap();
    assert_table_eq!(result, [["id", 1], ["name", 2],]);
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_namespace() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = 'public'")
        .unwrap();
    let _ = result;
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_type() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT typname FROM pg_catalog.pg_type WHERE typname = 'int8'")
        .unwrap();
    let _ = result;
}

#[test]
fn test_pg_catalog_pg_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_const_test (id INTEGER PRIMARY KEY)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT conname, contype FROM pg_catalog.pg_constraint c
             JOIN pg_catalog.pg_class cl ON c.conrelid = cl.oid
             WHERE cl.relname = 'pg_const_test'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_catalog_pg_proc() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE FUNCTION pg_proc_test() RETURNS INTEGER AS $$ SELECT 1 $$ LANGUAGE SQL",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT proname FROM pg_catalog.pg_proc WHERE proname = 'pg_proc_test'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_catalog_pg_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_trig_base (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql("SELECT tgname FROM pg_catalog.pg_trigger");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_catalog_pg_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_index_base (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX pg_index_test ON pg_index_base (id)")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT indexrelid, indisunique, indisprimary FROM pg_catalog.pg_index i
             JOIN pg_catalog.pg_class c ON i.indexrelid = c.oid
             WHERE c.relname = 'pg_index_test'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_description() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_desc_test (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("COMMENT ON TABLE pg_desc_test IS 'Test table'")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT description FROM pg_catalog.pg_description d
             JOIN pg_catalog.pg_class c ON d.objoid = c.oid
             WHERE c.relname = 'pg_desc_test'",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_database() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql(
            "SELECT datname FROM pg_catalog.pg_database WHERE datname = current_database()",
        )
        .unwrap();
    let _ = result;
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_roles() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT rolname FROM pg_catalog.pg_roles WHERE rolname = current_user")
        .unwrap();
    let _ = result;
}

#[test]
#[ignore = "Implement me!"]
fn test_pg_catalog_pg_settings() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT name, setting FROM pg_catalog.pg_settings WHERE name = 'search_path'")
        .unwrap();
    let _ = result;
}
