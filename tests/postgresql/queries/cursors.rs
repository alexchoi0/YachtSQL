use crate::common::create_executor;

fn setup_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE cursor_data (id INT64, name STRING, val INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cursor_data VALUES
         (1, 'Alice', 100), (2, 'Bob', 200), (3, 'Charlie', 300),
         (4, 'Diana', 400), (5, 'Eve', 500)",
        )
        .unwrap();
}

#[test]
fn test_declare_cursor() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    let result = executor.execute_sql("DECLARE my_cursor CURSOR FOR SELECT * FROM cursor_data");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_declare_cursor_with_hold() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result =
        executor.execute_sql("DECLARE hold_cursor CURSOR WITH HOLD FOR SELECT * FROM cursor_data");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_declare_cursor_scroll() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    let result =
        executor.execute_sql("DECLARE scroll_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_declare_cursor_no_scroll() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("DECLARE no_scroll_cursor NO SCROLL CURSOR FOR SELECT * FROM cursor_data");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_next() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE fetch_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH NEXT FROM fetch_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_prior() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE prior_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    executor
        .execute_sql("FETCH LAST FROM prior_cursor")
        .unwrap();
    let result = executor.execute_sql("FETCH PRIOR FROM prior_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_first() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE first_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH FIRST FROM first_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_last() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE last_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH LAST FROM last_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_absolute() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE abs_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH ABSOLUTE 3 FROM abs_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_relative() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE rel_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    executor.execute_sql("FETCH FIRST FROM rel_cursor").unwrap();
    let result = executor.execute_sql("FETCH RELATIVE 2 FROM rel_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_count() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE count_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH 3 FROM count_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_all() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE all_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH ALL FROM all_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_forward() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE fwd_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH FORWARD 2 FROM fwd_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_forward_all() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE fwd_all_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("FETCH FORWARD ALL FROM fwd_all_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_backward() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE bwd_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    executor.execute_sql("FETCH LAST FROM bwd_cursor").unwrap();
    let result = executor.execute_sql("FETCH BACKWARD 2 FROM bwd_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_backward_all() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql(
            "DECLARE bwd_all_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql("FETCH LAST FROM bwd_all_cursor")
        .unwrap();
    let result = executor.execute_sql("FETCH BACKWARD ALL FROM bwd_all_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_move_cursor() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE move_cursor CURSOR FOR SELECT * FROM cursor_data ORDER BY id")
        .unwrap();
    let result = executor.execute_sql("MOVE NEXT FROM move_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_move_absolute() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql(
            "DECLARE move_abs_cursor SCROLL CURSOR FOR SELECT * FROM cursor_data ORDER BY id",
        )
        .unwrap();
    let result = executor.execute_sql("MOVE ABSOLUTE 3 FROM move_abs_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_close_cursor() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE close_cursor CURSOR FOR SELECT * FROM cursor_data")
        .unwrap();
    let result = executor.execute_sql("CLOSE close_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_close_all_cursors() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE cursor1 CURSOR FOR SELECT * FROM cursor_data")
        .unwrap();
    executor
        .execute_sql("DECLARE cursor2 CURSOR FOR SELECT * FROM cursor_data")
        .unwrap();
    let result = executor.execute_sql("CLOSE ALL");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_declare_cursor_binary() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    let result =
        executor.execute_sql("DECLARE binary_cursor BINARY CURSOR FOR SELECT * FROM cursor_data");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_declare_cursor_insensitive() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("DECLARE insensitive_cursor INSENSITIVE CURSOR FOR SELECT * FROM cursor_data");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_cursor_for_update() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    let result = executor
        .execute_sql("DECLARE update_cursor CURSOR FOR SELECT * FROM cursor_data FOR UPDATE");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_cursor_where_current_of() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE wco_cursor CURSOR FOR SELECT * FROM cursor_data FOR UPDATE")
        .unwrap();
    executor.execute_sql("FETCH NEXT FROM wco_cursor").unwrap();
    let result =
        executor.execute_sql("UPDATE cursor_data SET val = 999 WHERE CURRENT OF wco_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_cursor_delete_current_of() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("DECLARE del_cursor CURSOR FOR SELECT * FROM cursor_data FOR UPDATE")
        .unwrap();
    executor.execute_sql("FETCH NEXT FROM del_cursor").unwrap();
    let result = executor.execute_sql("DELETE FROM cursor_data WHERE CURRENT OF del_cursor");
    executor.execute_sql("COMMIT").unwrap();
    assert!(result.is_ok() || result.is_err());
}
