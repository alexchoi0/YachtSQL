use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_create_row_policy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_table (id INT64, department String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY test_policy ON policy_table
            FOR SELECT USING department = 'Engineering'",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_create_row_policy_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE exists_table (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY IF NOT EXISTS exists_policy ON exists_table
            FOR SELECT USING id > 0",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY IF NOT EXISTS exists_policy ON exists_table
            FOR SELECT USING id > 0",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_row_policy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_table (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY drop_policy ON drop_table
            FOR SELECT USING id > 0",
        )
        .unwrap();
    executor
        .execute_sql("DROP ROW POLICY drop_policy ON drop_table")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_drop_row_policy_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_exists_table (id INT64)")
        .unwrap();
    executor
        .execute_sql("DROP ROW POLICY IF EXISTS nonexistent_policy ON drop_exists_table")
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_restrictive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE restrict_table (id INT64, active UInt8)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY restrict_policy ON restrict_table
            AS RESTRICTIVE
            FOR SELECT USING active = 1",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_permissive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE permissive_table (id INT64, status String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY permissive_policy ON permissive_table
            AS PERMISSIVE
            FOR SELECT USING status = 'active'",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_for_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER policy_user").unwrap();
    executor
        .execute_sql("CREATE TABLE user_policy_table (id INT64, owner String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY user_policy ON user_policy_table
            FOR SELECT USING owner = 'policy_user'
            TO policy_user",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_for_role() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE ROLE policy_role").unwrap();
    executor
        .execute_sql("CREATE TABLE role_policy_table (id INT64, department String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY role_policy ON role_policy_table
            FOR SELECT USING department = 'Sales'
            TO policy_role",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_all_users() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE all_users_table (id INT64, public UInt8)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY all_users_policy ON all_users_table
            FOR SELECT USING public = 1
            TO ALL",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_except_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER admin_user").unwrap();
    executor
        .execute_sql("CREATE TABLE except_table (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY except_policy ON except_table
            FOR SELECT USING 1
            TO ALL EXCEPT admin_user",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_alter_row_policy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE alter_policy_table (id INT64, status String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY alter_policy ON alter_policy_table
            FOR SELECT USING status = 'active'",
        )
        .unwrap();
    executor
        .execute_sql(
            "ALTER ROW POLICY alter_policy ON alter_policy_table
            FOR SELECT USING status IN ('active', 'pending')",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_show_row_policies() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE show_policy_table (id INT64)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY show_policy ON show_policy_table
            FOR SELECT USING id > 0",
        )
        .unwrap();
    let result = executor.execute_sql("SHOW ROW POLICIES").unwrap();
    assert!(result.num_rows() >= 1);
}

#[ignore = "Implement me!"]
#[test]
fn test_row_policy_complex_condition() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE complex_policy_table (
                id INT64,
                department String,
                level INT64,
                active UInt8
            )",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY complex_policy ON complex_policy_table
            FOR SELECT USING (department = 'Engineering' AND level >= 3) OR active = 1",
        )
        .unwrap();
}

#[ignore = "Implement me!"]
#[test]
fn test_multiple_row_policies() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER multi_user").unwrap();
    executor.execute_sql("CREATE ROLE multi_role").unwrap();
    executor
        .execute_sql("CREATE TABLE multi_policy_table (id INT64, type String)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY policy1 ON multi_policy_table
            FOR SELECT USING type = 'A'
            TO multi_user",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE ROW POLICY policy2 ON multi_policy_table
            FOR SELECT USING type = 'B'
            TO multi_role",
        )
        .unwrap();
}
