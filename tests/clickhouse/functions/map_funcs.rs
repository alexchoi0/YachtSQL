use crate::common::create_executor;

#[test]
fn test_map_constructor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT map('a', 1, 'b', 2, 'c', 3)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_from_arrays() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapFromArrays(['a', 'b', 'c'], [1, 2, 3])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_keys() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapKeys(map('a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_values() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapValues(map('a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapContains(map('a', 1, 'b', 2), 'a')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_contains_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapContains(map('a', 1, 'b', 2), 'x')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_subscript() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT map('a', 1, 'b', 2)['a']")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapAdd(map('a', 1, 'b', 2), map('b', 3, 'c', 4))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_subtract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapSubtract(map('a', 10, 'b', 20), map('a', 3, 'b', 5))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_populate_series() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapPopulateSeries(map(1, 10, 5, 50), 6)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_filter() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapFilter((k, v) -> v > 5, map('a', 3, 'b', 7, 'c', 10))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_apply() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapApply((k, v) -> (k, v * 2), map('a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_update() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapUpdate(map('a', 1, 'b', 2), map('b', 20, 'c', 30))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapConcat(map('a', 1), map('b', 2), map('c', 3))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_exists() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapExists((k, v) -> v > 5, map('a', 3, 'b', 7))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapAll((k, v) -> v > 0, map('a', 3, 'b', 7))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_sort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapSort(map('c', 3, 'a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_reverse_sort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapReverseSort(map('c', 3, 'a', 1, 'b', 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_partial_sort() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT mapPartialSort((k, v) -> v, 2, map('c', 30, 'a', 10, 'b', 20))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE map_data (id UInt32, attrs Map(String, Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO map_data VALUES
            (1, map('x', 10, 'y', 20)),
            (2, map('x', 30, 'z', 40)),
            (3, map('y', 50))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, attrs, mapKeys(attrs), mapValues(attrs)
            FROM map_data
            ORDER BY id",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_element_access() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE config (settings Map(String, String))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO config VALUES
            (map('host', 'localhost', 'port', '8080')),
            (map('host', 'example.com', 'port', '443'))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT settings['host'] AS host, settings['port'] AS port
            FROM config",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_map_contains_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE products (name String, props Map(String, String))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO products VALUES
            ('Widget', map('color', 'red', 'size', 'large')),
            ('Gadget', map('color', 'blue')),
            ('Thing', map('material', 'wood'))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT name
            FROM products
            WHERE mapContains(props, 'color')
            ORDER BY name",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}
