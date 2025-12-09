use crate::common::create_executor;

#[test]
fn test_create_settings_profile() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SETTINGS PROFILE test_profile")
        .unwrap();
}

#[test]
fn test_create_settings_profile_with_settings() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE settings_profile
            SETTINGS max_memory_usage = 10000000000",
        )
        .unwrap();
}

#[test]
fn test_create_settings_profile_multiple_settings() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE multi_settings_profile
            SETTINGS
                max_memory_usage = 10000000000,
                max_execution_time = 60",
        )
        .unwrap();
}

#[test]
fn test_create_settings_profile_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SETTINGS PROFILE IF NOT EXISTS exists_profile")
        .unwrap();
    executor
        .execute_sql("CREATE SETTINGS PROFILE IF NOT EXISTS exists_profile")
        .unwrap();
}

#[test]
fn test_drop_settings_profile() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SETTINGS PROFILE drop_profile")
        .unwrap();
    executor
        .execute_sql("DROP SETTINGS PROFILE drop_profile")
        .unwrap();
}

#[test]
fn test_drop_settings_profile_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP SETTINGS PROFILE IF EXISTS nonexistent_profile")
        .unwrap();
}

#[test]
fn test_alter_settings_profile() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SETTINGS PROFILE alter_profile")
        .unwrap();
    executor
        .execute_sql(
            "ALTER SETTINGS PROFILE alter_profile
            SETTINGS max_memory_usage = 20000000000",
        )
        .unwrap();
}

#[test]
fn test_settings_profile_min_max() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE minmax_profile
            SETTINGS max_memory_usage MIN 1000000 MAX 10000000000",
        )
        .unwrap();
}

#[test]
fn test_settings_profile_readonly() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE readonly_profile
            SETTINGS max_memory_usage = 10000000000 READONLY",
        )
        .unwrap();
}

#[test]
fn test_settings_profile_writable() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE writable_profile
            SETTINGS max_memory_usage = 10000000000 WRITABLE",
        )
        .unwrap();
}

#[test]
fn test_apply_settings_profile_to_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER profile_user").unwrap();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE user_apply_profile
            SETTINGS max_memory_usage = 10000000000
            TO profile_user",
        )
        .unwrap();
}

#[test]
fn test_apply_settings_profile_to_role() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE ROLE profile_role").unwrap();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE role_apply_profile
            SETTINGS max_memory_usage = 10000000000
            TO profile_role",
        )
        .unwrap();
}

#[test]
fn test_inherit_settings_profile() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE base_profile
            SETTINGS max_memory_usage = 10000000000",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE derived_profile
            SETTINGS INHERIT base_profile",
        )
        .unwrap();
}

#[ignore = "Fix me!"]
#[test]
fn test_show_settings_profiles() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SETTINGS PROFILE show_profile")
        .unwrap();
    let result = executor.execute_sql("SHOW SETTINGS PROFILES").unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_settings_profile_constraints() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE SETTINGS PROFILE constraints_profile
            SETTINGS
                max_memory_usage = 10000000000 MIN 1000000 MAX 100000000000,
                max_execution_time = 60 READONLY",
        )
        .unwrap();
}
