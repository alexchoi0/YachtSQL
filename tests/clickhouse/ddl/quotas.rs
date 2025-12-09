use crate::common::create_executor;

#[test]
fn test_create_quota() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE QUOTA test_quota").unwrap();
}

#[test]
fn test_create_quota_with_limits() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA limits_quota
            FOR INTERVAL 1 hour MAX queries = 100, errors = 10, result_rows = 1000000",
        )
        .unwrap();
}

#[test]
fn test_create_quota_multiple_intervals() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA multi_interval_quota
            FOR INTERVAL 1 hour MAX queries = 100
            FOR INTERVAL 1 day MAX queries = 1000",
        )
        .unwrap();
}

#[test]
fn test_create_quota_if_not_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE QUOTA IF NOT EXISTS exists_quota")
        .unwrap();
    executor
        .execute_sql("CREATE QUOTA IF NOT EXISTS exists_quota")
        .unwrap();
}

#[test]
fn test_drop_quota() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE QUOTA drop_quota").unwrap();
    executor.execute_sql("DROP QUOTA drop_quota").unwrap();
}

#[test]
fn test_drop_quota_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP QUOTA IF EXISTS nonexistent_quota")
        .unwrap();
}

#[test]
fn test_alter_quota() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE QUOTA alter_quota").unwrap();
    executor
        .execute_sql(
            "ALTER QUOTA alter_quota
            FOR INTERVAL 1 hour MAX queries = 200",
        )
        .unwrap();
}

#[test]
fn test_create_quota_keyed_by_user() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA user_keyed_quota
            KEYED BY user_name
            FOR INTERVAL 1 hour MAX queries = 100",
        )
        .unwrap();
}

#[test]
fn test_create_quota_keyed_by_ip() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA ip_keyed_quota
            KEYED BY ip_address
            FOR INTERVAL 1 hour MAX queries = 100",
        )
        .unwrap();
}

#[test]
fn test_create_quota_keyed_by_client() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA client_keyed_quota
            KEYED BY client_key
            FOR INTERVAL 1 hour MAX queries = 100",
        )
        .unwrap();
}

#[test]
fn test_apply_quota_to_user() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE USER quota_user").unwrap();
    executor
        .execute_sql(
            "CREATE QUOTA apply_quota
            FOR INTERVAL 1 hour MAX queries = 100
            TO quota_user",
        )
        .unwrap();
}

#[test]
fn test_apply_quota_to_role() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE ROLE quota_role").unwrap();
    executor
        .execute_sql(
            "CREATE QUOTA role_quota
            FOR INTERVAL 1 hour MAX queries = 100
            TO quota_role",
        )
        .unwrap();
}

#[ignore = "Fix me!"]
#[test]
fn test_show_quotas() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE QUOTA show_quota").unwrap();
    let result = executor.execute_sql("SHOW QUOTAS").unwrap();
    assert!(result.num_rows() >= 1);
}

#[test]
fn test_quota_execution_time() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA exec_time_quota
            FOR INTERVAL 1 hour MAX execution_time = 3600",
        )
        .unwrap();
}

#[test]
fn test_quota_read_rows() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA read_rows_quota
            FOR INTERVAL 1 hour MAX read_rows = 1000000000",
        )
        .unwrap();
}

#[test]
fn test_quota_result_bytes() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE QUOTA result_bytes_quota
            FOR INTERVAL 1 hour MAX result_bytes = 1000000000",
        )
        .unwrap();
}
