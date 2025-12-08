use crate::common::create_executor;

fn setup_tables(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE accounts (id INTEGER, balance INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO accounts VALUES (1, 1000), (2, 2000), (3, 3000)")
        .unwrap();
}

#[test]
fn test_select_for_update() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE id = 1 FOR UPDATE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_share() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE id = 1 FOR SHARE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_no_key_update() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE id = 1 FOR NO KEY UPDATE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_key_share() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE id = 1 FOR KEY SHARE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_update_nowait() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE id = 1 FOR UPDATE NOWAIT")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_update_skip_locked() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE id = 1 FOR UPDATE SKIP LOCKED")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_update_of_table() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts a FOR UPDATE OF a")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_select_for_update_multiple_rows() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts WHERE balance > 1500 FOR UPDATE")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_select_for_update_with_order() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts ORDER BY balance DESC FOR UPDATE")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_select_for_update_with_limit() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts ORDER BY id LIMIT 2 FOR UPDATE")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_select_for_update_subquery() {
    let mut executor = create_executor();
    setup_tables(&mut executor);
    executor
        .execute_sql("CREATE TABLE transactions (id INTEGER, account_id INTEGER, amount INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO transactions VALUES (1, 1, 100), (2, 1, 200)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM accounts WHERE id IN (SELECT account_id FROM transactions) FOR UPDATE",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_share_with_join() {
    let mut executor = create_executor();
    setup_tables(&mut executor);
    executor
        .execute_sql("CREATE TABLE owners (id INTEGER, account_id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO owners VALUES (1, 1, 'Alice'), (2, 2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a.id, a.balance, o.name
             FROM accounts a
             JOIN owners o ON a.id = o.account_id
             FOR SHARE",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_update_of_specific_table() {
    let mut executor = create_executor();
    setup_tables(&mut executor);
    executor
        .execute_sql("CREATE TABLE refs (id INTEGER, account_id INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO refs VALUES (1, 1), (2, 2)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a.*, r.id AS ref_id
             FROM accounts a
             JOIN refs r ON a.id = r.account_id
             FOR UPDATE OF a",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
#[ignore = "Implement me!"]
fn test_select_for_share_of_multiple_tables() {
    let mut executor = create_executor();
    setup_tables(&mut executor);
    executor
        .execute_sql("CREATE TABLE details (id INTEGER, account_id INTEGER, info TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO details VALUES (1, 1, 'info1')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT a.*, d.info
             FROM accounts a
             JOIN details d ON a.id = d.account_id
             FOR SHARE OF a, d",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_select_for_update_cte() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql(
            "WITH high_balance AS (SELECT * FROM accounts WHERE balance > 1500)
             SELECT * FROM high_balance FOR UPDATE",
        )
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}

#[test]
fn test_select_for_update_all_rows() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts FOR UPDATE")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}

#[test]
fn test_select_for_share_skip_locked() {
    let mut executor = create_executor();
    setup_tables(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM accounts FOR SHARE SKIP LOCKED")
        .unwrap();
    assert_eq!(result.num_rows(), 3);
}
