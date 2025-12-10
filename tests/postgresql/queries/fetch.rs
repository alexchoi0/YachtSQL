use crate::assert_table_eq;
use crate::common::create_executor;

fn setup_table(executor: &mut yachtsql::QueryExecutor) {
    executor
        .execute_sql("CREATE TABLE items (id INTEGER, name TEXT, price INTEGER)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO items VALUES
         (1, 'Apple', 100), (2, 'Banana', 50), (3, 'Cherry', 200),
         (4, 'Date', 150), (5, 'Elderberry', 300), (6, 'Fig', 75)",
        )
        .unwrap();
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_first_n_rows() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH FIRST 3 ROWS ONLY")
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "Apple", 100], [2, "Banana", 50], [3, "Cherry", 200]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_first_row() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH FIRST ROW ONLY")
        .unwrap();
    assert_table_eq!(result, [[1, "Apple", 100]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_first_1_row() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH FIRST 1 ROW ONLY")
        .unwrap();
    assert_table_eq!(result, [[1, "Apple", 100]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_next_n_rows() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH NEXT 2 ROWS ONLY")
        .unwrap();
    assert_table_eq!(result, [[1, "Apple", 100], [2, "Banana", 50]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_offset_fetch() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id OFFSET 2 FETCH FIRST 2 ROWS ONLY")
        .unwrap();
    assert_table_eq!(result, [[3, "Cherry", 200], [4, "Date", 150]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_offset_rows_fetch() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id OFFSET 2 ROWS FETCH FIRST 2 ROWS ONLY")
        .unwrap();
    assert_table_eq!(result, [[3, "Cherry", 200], [4, "Date", 150]]);
}

#[test]
fn test_fetch_with_ties() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scores (id INTEGER, score INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (1, 100), (2, 90), (3, 90), (4, 80), (5, 80)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM scores ORDER BY score DESC FETCH FIRST 2 ROWS WITH TIES")
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_first_percent() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH FIRST 50 PERCENT ROWS ONLY")
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "Apple", 100], [2, "Banana", 50], [3, "Cherry", 200]]
    );
}

#[test]
#[ignore = "FETCH FIRST PERCENT is not supported yet"]
fn test_fetch_first_percent_with_ties() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE percent_ties (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO percent_ties VALUES (1, 10), (2, 20), (3, 20), (4, 30)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM percent_ties ORDER BY val FETCH FIRST 50 PERCENT ROWS WITH TIES",
        )
        .unwrap();
    assert!(result.num_rows() >= 2);
}

#[test]
fn test_fetch_more_than_available() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH FIRST 100 ROWS ONLY")
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Apple", 100],
            [2, "Banana", 50],
            [3, "Cherry", 200],
            [4, "Date", 150],
            [5, "Elderberry", 300],
            [6, "Fig", 75]
        ]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_zero_rows() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id FETCH FIRST 0 ROWS ONLY")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_offset_beyond_rows() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items ORDER BY id OFFSET 10 FETCH FIRST 5 ROWS ONLY")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_with_where() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql("SELECT * FROM items WHERE price > 100 ORDER BY id FETCH FIRST 2 ROWS ONLY")
        .unwrap();
    assert_table_eq!(result, [[3, "Cherry", 200], [4, "Date", 150]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_with_aggregate() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT name, price
             FROM items
             ORDER BY price DESC
             FETCH FIRST 3 ROWS ONLY",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [["Elderberry", 300], ["Cherry", 200], ["Date", 150]]
    );
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_subquery() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT * FROM (
                 SELECT * FROM items ORDER BY price DESC FETCH FIRST 3 ROWS ONLY
             ) AS top_items
             ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [[3, "Cherry", 200], [4, "Date", 150], [5, "Elderberry", 300]]
    );
}

#[test]
fn test_fetch_union() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "(SELECT id, name FROM items WHERE price < 100)
             UNION ALL
             (SELECT id, name FROM items WHERE price > 200)
             FETCH FIRST 3 ROWS ONLY",
        )
        .unwrap();
    assert_table_eq!(result, [[2, "Banana"], [6, "Fig"], [5, "Elderberry"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_with_join() {
    let mut executor = create_executor();
    setup_table(&mut executor);
    executor
        .execute_sql("CREATE TABLE categories (item_id INTEGER, category TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO categories VALUES (1, 'Fruit'), (2, 'Fruit'), (3, 'Fruit')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT i.name, c.category
             FROM items i
             JOIN categories c ON i.id = c.item_id
             ORDER BY i.id
             FETCH FIRST 2 ROWS ONLY",
        )
        .unwrap();
    assert_table_eq!(result, [["Apple", "Fruit"], ["Banana", "Fruit"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_with_cte() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "WITH expensive AS (SELECT * FROM items WHERE price > 100)
             SELECT * FROM expensive ORDER BY price DESC FETCH FIRST 2 ROWS ONLY",
        )
        .unwrap();
    assert_table_eq!(result, [[5, "Elderberry", 300], [3, "Cherry", 200]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_fetch_window_function() {
    let mut executor = create_executor();
    setup_table(&mut executor);

    let result = executor
        .execute_sql(
            "SELECT id, name, price, ROW_NUMBER() OVER (ORDER BY price DESC) as rank
             FROM items
             FETCH FIRST 3 ROWS ONLY",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Apple", 100, 1],
            [2, "Banana", 50, 2],
            [3, "Cherry", 200, 3]
        ]
    );
}
