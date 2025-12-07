use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_create_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER test_user").unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_user_with_password() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE USER pwd_user IDENTIFIED BY 'password123'")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_user_sha256() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE USER sha_user IDENTIFIED WITH sha256_password BY 'secure_password'")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_user_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE USER IF NOT EXISTS exists_user")
        .unwrap();
    executor
        .execute_sql("CREATE USER IF NOT EXISTS exists_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER drop_user").unwrap();
    executor.execute_sql("DROP USER drop_user").unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_user_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP USER IF EXISTS nonexistent_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_alter_user_password() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER alter_user").unwrap();
    executor
        .execute_sql("ALTER USER alter_user IDENTIFIED BY 'new_password'")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_role() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE ROLE test_role").unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_role_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE ROLE IF NOT EXISTS exists_role")
        .unwrap();
    executor
        .execute_sql("CREATE ROLE IF NOT EXISTS exists_role")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_role() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE ROLE drop_role").unwrap();
    executor.execute_sql("DROP ROLE drop_role").unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_role_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP ROLE IF EXISTS nonexistent_role")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_grant_select() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER grant_user").unwrap();
    executor
        .execute_sql("CREATE TABLE grant_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("GRANT SELECT ON grant_table TO grant_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_grant_insert() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER insert_user").unwrap();
    executor
        .execute_sql("CREATE TABLE insert_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("GRANT INSERT ON insert_table TO insert_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_grant_multiple() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER multi_user").unwrap();
    executor
        .execute_sql("CREATE TABLE multi_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("GRANT SELECT, INSERT, UPDATE, DELETE ON multi_table TO multi_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_grant_role_to_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER role_user").unwrap();
    executor.execute_sql("CREATE ROLE user_role").unwrap();
    executor
        .execute_sql("GRANT user_role TO role_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_revoke_select() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER revoke_user").unwrap();
    executor
        .execute_sql("CREATE TABLE revoke_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("GRANT SELECT ON revoke_table TO revoke_user")
        .unwrap();
    executor
        .execute_sql("REVOKE SELECT ON revoke_table FROM revoke_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_revoke_role() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE USER revoke_role_user")
        .unwrap();
    executor.execute_sql("CREATE ROLE revoke_role").unwrap();
    executor
        .execute_sql("GRANT revoke_role TO revoke_role_user")
        .unwrap();
    executor
        .execute_sql("REVOKE revoke_role FROM revoke_role_user")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_show_users() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER show_user").unwrap();
    let result = executor.execute_sql("SHOW USERS").unwrap();
    assert!(result.num_rows() >= 1);
}

#[ignore = "Implement me!"]
#[test]
fn test_show_roles() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE ROLE show_role").unwrap();
    let result = executor.execute_sql("SHOW ROLES").unwrap();
    assert!(result.num_rows() >= 1);
}

#[ignore = "Implement me!"]
#[test]
fn test_show_grants() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER grants_user").unwrap();
    executor
        .execute_sql("CREATE TABLE grants_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("GRANT SELECT ON grants_table TO grants_user")
        .unwrap();
    let result = executor.execute_sql("SHOW GRANTS FOR grants_user").unwrap();
    assert!(result.num_rows() >= 1);
}

#[ignore = "Implement me!"]
#[test]
fn test_set_role() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER set_role_user").unwrap();
    executor.execute_sql("CREATE ROLE set_role").unwrap();
    executor
        .execute_sql("GRANT set_role TO set_role_user")
        .unwrap();
    executor.execute_sql("SET ROLE set_role").unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_default_role() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE USER default_role_user")
        .unwrap();
    executor.execute_sql("CREATE ROLE default_role").unwrap();
    executor
        .execute_sql("GRANT default_role TO default_role_user")
        .unwrap();
    executor
        .execute_sql("SET DEFAULT ROLE default_role TO default_role_user")
        .unwrap();
}
