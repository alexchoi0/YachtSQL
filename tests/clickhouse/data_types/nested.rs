use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_nested_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_test (id INT64, attrs Nested(key String, value Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_test VALUES (1, ['a', 'b'], [10, 20])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, attrs.key, attrs.value FROM nested_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_access_key() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_keys (id INT64, data Nested(name String, score Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_keys VALUES (1, ['alice', 'bob'], [100, 200])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT data.name FROM nested_keys")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_access_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_vals (id INT64, items Nested(sku String, qty Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_vals VALUES (1, ['A001', 'B002'], [5, 10])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT items.qty FROM nested_vals")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_array_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_join (id INT64, tags Nested(name String, count Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_join VALUES (1, ['tag1', 'tag2', 'tag3'], [1, 2, 3])")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, tags.name, tags.count FROM nested_join ARRAY JOIN tags ORDER BY tags.count",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_filter (id INT64, props Nested(key String, val Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO nested_filter VALUES (1, ['a', 'b'], [10, 20]), (2, ['c', 'd'], [30, 40])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM nested_filter WHERE has(props.key, 'a')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_multiple_rows() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_multi (id INT64, attrs Nested(k String, v Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO nested_multi VALUES (1, ['x'], [100]), (2, ['y', 'z'], [200, 300])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, length(attrs.k) AS cnt FROM nested_multi ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, 1], [2, 2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_empty() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_empty (id INT64, data Nested(a String, b Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_empty VALUES (1, [], [])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, length(data.a) FROM nested_empty")
        .unwrap();
    assert_table_eq!(result, [[1, 0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE nested_agg (id INT64, scores Nested(subject String, score Int64))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_agg VALUES (1, ['math', 'english'], [90, 85]), (2, ['math', 'english'], [80, 95])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT SUM(arraySum(scores.score)) FROM nested_agg")
        .unwrap();
    assert_table_eq!(result, [[350]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_flatten() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_flat (id INT64, items Nested(name String, price Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_flat VALUES (1, ['a', 'b'], [10, 20])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, n, p FROM nested_flat ARRAY JOIN items.name AS n, items.price AS p ORDER BY p")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_subcolumn() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_sub (id INT64, meta Nested(key String, value String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_sub VALUES (1, ['color', 'size'], ['red', 'large'])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT meta.key[1], meta.value[1] FROM nested_sub")
        .unwrap();
    assert_table_eq!(result, [["color", "red"]]);
}
