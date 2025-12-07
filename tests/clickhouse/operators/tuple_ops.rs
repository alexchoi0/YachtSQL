use crate::common::create_executor;
use crate::assert_table_eq;

#[test]
fn test_tuple_access_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_idx (t Tuple(Int64, String, Float64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_idx VALUES ((1, 'test', 3.14))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT t.1, t.2, t.3 FROM tuple_idx")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_access_named() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_named (t Tuple(id Int64, name String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_named VALUES ((1, 'test'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT t.id, t.name FROM tuple_named")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_comparison_equal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_eq (id INT64, t Tuple(Int64, Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_eq VALUES (1, (1, 2)), (2, (3, 4)), (3, (1, 2))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM tuple_eq WHERE t = (1, 2) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_tuple_comparison_less() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_less (id INT64, t Tuple(Int64, Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_less VALUES (1, (1, 2)), (2, (2, 1)), (3, (1, 3))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM tuple_less WHERE t < (2, 0) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_tuple_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_where (x Int64, y Int64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_where VALUES (1, 2), (3, 4), (5, 6)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT x FROM tuple_where WHERE (x, y) IN ((1, 2), (5, 6)) ORDER BY x")
        .unwrap();
    assert_table_eq!(result, [[1], [5]]);
}

#[test]
fn test_tuple_element_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_elem (t Tuple(Int64, String, Float64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_elem VALUES ((1, 'test', 3.14))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT tupleElement(t, 1), tupleElement(t, 2) FROM tuple_elem")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_creation() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuple(1, 'hello', 3.14)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_order (id INT64, t Tuple(Int64, Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_order VALUES (1, (2, 3)), (2, (1, 5)), (3, (2, 1))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM tuple_order ORDER BY t")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_tuple_concat() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_concat (a Tuple(Int64, Int64), b Tuple(String, String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_concat VALUES ((1, 2), ('a', 'b'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT tupleConcat(a, b) FROM tuple_concat")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_untuple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_untuple (t Tuple(Int64, String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_untuple VALUES ((1, 'test'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT untuple(t) FROM tuple_untuple")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_group (key Tuple(Int64, String), value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO tuple_group VALUES ((1, 'a'), 10), ((1, 'a'), 20), ((2, 'b'), 30)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT key, SUM(value) FROM tuple_group GROUP BY key ORDER BY key")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_nested_tuple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_tuple (t Tuple(Tuple(Int64, Int64), String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_tuple VALUES (((1, 2), 'test'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT t.1.1, t.1.2, t.2 FROM nested_tuple")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_tuple_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_distinct (t Tuple(Int64, Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_distinct VALUES ((1, 2)), ((1, 2)), ((3, 4))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT t) FROM tuple_distinct")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}
