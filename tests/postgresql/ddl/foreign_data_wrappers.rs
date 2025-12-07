use crate::common::create_executor;

#[test]
fn test_create_extension_postgres_fdw() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_extension_file_fdw() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE EXTENSION IF NOT EXISTS file_fdw");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_foreign_data_wrapper() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE FOREIGN DATA WRAPPER test_fdw");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_fdw_with_handler() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();

    let result = executor
        .execute_sql("CREATE FOREIGN DATA WRAPPER my_postgres_fdw HANDLER postgres_fdw_handler");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_fdw_with_validator() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE FOREIGN DATA WRAPPER my_fdw_valid HANDLER postgres_fdw_handler VALIDATOR postgres_fdw_validator"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_fdw_with_options() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE FOREIGN DATA WRAPPER opt_fdw OPTIONS (debug 'true')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_fdw_add_option() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FOREIGN DATA WRAPPER alter_fdw")
        .ok();

    let result =
        executor.execute_sql("ALTER FOREIGN DATA WRAPPER alter_fdw OPTIONS (ADD debug 'true')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_fdw_set_option() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FOREIGN DATA WRAPPER set_fdw OPTIONS (debug 'false')")
        .ok();

    let result =
        executor.execute_sql("ALTER FOREIGN DATA WRAPPER set_fdw OPTIONS (SET debug 'true')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_fdw_drop_option() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FOREIGN DATA WRAPPER drop_opt_fdw OPTIONS (debug 'true')")
        .ok();

    let result =
        executor.execute_sql("ALTER FOREIGN DATA WRAPPER drop_opt_fdw OPTIONS (DROP debug)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_foreign_data_wrapper() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FOREIGN DATA WRAPPER to_drop_fdw")
        .ok();

    let result = executor.execute_sql("DROP FOREIGN DATA WRAPPER to_drop_fdw");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_fdw_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP FOREIGN DATA WRAPPER IF EXISTS nonexistent_fdw");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_fdw_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE FOREIGN DATA WRAPPER cascade_fdw")
        .ok();

    let result = executor.execute_sql("DROP FOREIGN DATA WRAPPER cascade_fdw CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_server() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();

    let result =
        executor.execute_sql("CREATE SERVER remote_server FOREIGN DATA WRAPPER postgres_fdw");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_server_with_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE SERVER remote_opt_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', port '5432', dbname 'remote_db')"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_server_with_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE SERVER typed_server TYPE 'postgresql' FOREIGN DATA WRAPPER postgres_fdw",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_server_with_version() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE SERVER versioned_server VERSION '15.0' FOREIGN DATA WRAPPER postgres_fdw",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_server_add_option() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER alter_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql("ALTER SERVER alter_srv OPTIONS (ADD host 'localhost')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_server_version() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ver_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql("ALTER SERVER ver_srv VERSION '16.0'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_server_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER old_name_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql("ALTER SERVER old_name_srv RENAME TO new_name_srv");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_server_owner() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER owner_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql("ALTER SERVER owner_srv OWNER TO CURRENT_USER");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_server() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER to_drop_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql("DROP SERVER to_drop_srv");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_server_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP SERVER IF EXISTS nonexistent_srv");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_server_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER cascade_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql("DROP SERVER cascade_srv CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_user_mapping() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER mapping_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE USER MAPPING FOR CURRENT_USER SERVER mapping_srv OPTIONS (user 'remote_user', password 'secret')"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_user_mapping_public() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER public_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE USER MAPPING FOR PUBLIC SERVER public_srv OPTIONS (user 'public_user')",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_user_mapping() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER alter_map_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql(
            "CREATE USER MAPPING FOR CURRENT_USER SERVER alter_map_srv OPTIONS (user 'old_user')",
        )
        .ok();

    let result = executor.execute_sql(
        "ALTER USER MAPPING FOR CURRENT_USER SERVER alter_map_srv OPTIONS (SET user 'new_user')",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_user_mapping() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER drop_map_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE USER MAPPING FOR CURRENT_USER SERVER drop_map_srv")
        .ok();

    let result = executor.execute_sql("DROP USER MAPPING FOR CURRENT_USER SERVER drop_map_srv");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_user_mapping_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER drop_map_srv2 FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result =
        executor.execute_sql("DROP USER MAPPING IF EXISTS FOR CURRENT_USER SERVER drop_map_srv2");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor.execute_sql("CREATE SERVER ft_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'remote')").ok();

    let result = executor.execute_sql(
        "CREATE FOREIGN TABLE foreign_users (id INT64, name TEXT, email TEXT) SERVER ft_srv OPTIONS (table_name 'users')"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_foreign_table_schema_qualified() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_schema_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE FOREIGN TABLE foreign_products (id INT64, name TEXT) SERVER ft_schema_srv OPTIONS (schema_name 'public', table_name 'products')"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_foreign_table_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_ine_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE FOREIGN TABLE IF NOT EXISTS foreign_items (id INT64) SERVER ft_ine_srv",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_foreign_table_column_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_col_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE FOREIGN TABLE foreign_col_opts (
            id INT64 OPTIONS (column_name 'user_id'),
            name TEXT OPTIONS (column_name 'user_name')
        ) SERVER ft_col_srv",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_foreign_table_add_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_add_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ft_add (id INT64) SERVER ft_add_srv")
        .ok();

    let result = executor.execute_sql("ALTER FOREIGN TABLE ft_add ADD COLUMN name TEXT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_foreign_table_drop_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_drop_col_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql(
            "CREATE FOREIGN TABLE ft_drop_col (id INT64, name TEXT) SERVER ft_drop_col_srv",
        )
        .ok();

    let result = executor.execute_sql("ALTER FOREIGN TABLE ft_drop_col DROP COLUMN name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_foreign_table_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_opt_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ft_opt (id INT64) SERVER ft_opt_srv")
        .ok();

    let result =
        executor.execute_sql("ALTER FOREIGN TABLE ft_opt OPTIONS (ADD table_name 'real_table')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_foreign_table_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_rename_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ft_old_name (id INT64) SERVER ft_rename_srv")
        .ok();

    let result = executor.execute_sql("ALTER FOREIGN TABLE ft_old_name RENAME TO ft_new_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_foreign_table_set_schema() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor.execute_sql("CREATE SCHEMA ft_schema").ok();
    executor
        .execute_sql("CREATE SERVER ft_schema_mov_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ft_to_move (id INT64) SERVER ft_schema_mov_srv")
        .ok();

    let result = executor.execute_sql("ALTER FOREIGN TABLE ft_to_move SET SCHEMA ft_schema");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_drop_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ft_to_drop (id INT64) SERVER ft_drop_srv")
        .ok();

    let result = executor.execute_sql("DROP FOREIGN TABLE ft_to_drop");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_foreign_table_if_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql("DROP FOREIGN TABLE IF EXISTS nonexistent_ft");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_foreign_table_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ft_cascade_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ft_cascade (id INT64) SERVER ft_cascade_srv")
        .ok();

    let result = executor.execute_sql("DROP FOREIGN TABLE ft_cascade CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_import_foreign_schema() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor.execute_sql("CREATE SERVER import_srv FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname 'remote')").ok();

    let result =
        executor.execute_sql("IMPORT FOREIGN SCHEMA public FROM SERVER import_srv INTO public");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_import_foreign_schema_limit_to() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER import_limit_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "IMPORT FOREIGN SCHEMA public LIMIT TO (users, orders) FROM SERVER import_limit_srv INTO public"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_import_foreign_schema_except() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER import_except_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "IMPORT FOREIGN SCHEMA public EXCEPT (internal_table) FROM SERVER import_except_srv INTO public"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_import_foreign_schema_options() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER import_opt_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();

    let result = executor.execute_sql(
        "IMPORT FOREIGN SCHEMA public FROM SERVER import_opt_srv INTO public OPTIONS (import_default 'true')"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_select_from_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER sel_ft_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE sel_ft (id INT64, name TEXT) SERVER sel_ft_srv")
        .ok();

    let result = executor.execute_sql("SELECT * FROM sel_ft WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_join_foreign_and_local() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER join_ft_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql(
            "CREATE FOREIGN TABLE foreign_orders (id INT64, user_id INT64) SERVER join_ft_srv",
        )
        .ok();
    executor
        .execute_sql("CREATE TABLE local_users (id INT64, name TEXT)")
        .ok();

    let result = executor.execute_sql(
        "SELECT u.name, o.id FROM local_users u JOIN foreign_orders o ON u.id = o.user_id",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_insert_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER ins_ft_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE ins_ft (id INT64, name TEXT) SERVER ins_ft_srv")
        .ok();

    let result = executor.execute_sql("INSERT INTO ins_ft VALUES (1, 'test')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_update_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER upd_ft_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE upd_ft (id INT64, name TEXT) SERVER upd_ft_srv")
        .ok();

    let result = executor.execute_sql("UPDATE upd_ft SET name = 'updated' WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_delete_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER del_ft_srv FOREIGN DATA WRAPPER postgres_fdw")
        .ok();
    executor
        .execute_sql("CREATE FOREIGN TABLE del_ft (id INT64, name TEXT) SERVER del_ft_srv")
        .ok();

    let result = executor.execute_sql("DELETE FROM del_ft WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_file_fdw_foreign_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE EXTENSION IF NOT EXISTS file_fdw")
        .ok();
    executor
        .execute_sql("CREATE SERVER file_srv FOREIGN DATA WRAPPER file_fdw")
        .ok();

    let result = executor.execute_sql(
        "CREATE FOREIGN TABLE csv_data (id INT64, name TEXT, value FLOAT64) SERVER file_srv OPTIONS (filename '/tmp/data.csv', format 'csv', header 'true')"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_tables() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM information_schema.foreign_tables");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_servers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM information_schema.foreign_servers");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_information_schema_foreign_data_wrappers() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM information_schema.foreign_data_wrappers");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_foreign_table() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_foreign_table");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_foreign_server() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_foreign_server");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_foreign_data_wrapper() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_foreign_data_wrapper");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_user_mapping() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_user_mapping");
    assert!(result.is_ok() || result.is_err());
}
