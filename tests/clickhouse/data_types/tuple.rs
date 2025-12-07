use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_tuple_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuple(1, 'hello', 3.14)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_access() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT tuple(1, 'hello').1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_access_second() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT tuple(1, 'hello').2").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_element() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleElement(tuple(1, 'hello'), 1)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_element_by_name() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleElement(tuple(1 AS id, 'hello' AS name), 'name')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_named_tuple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuple(1 AS id, 'hello' AS name)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_table (id INT64, data Tuple(x INT64, y INT64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_table VALUES (1, tuple(10, 20)), (2, tuple(30, 40))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, data.x FROM tuple_table ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleConcat(tuple(1, 2), tuple(3, 4))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_plus() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuplePlus(tuple(1, 2), tuple(3, 4))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_minus() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleMinus(tuple(5, 6), tuple(1, 2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_multiply() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleMultiply(tuple(2, 3), tuple(4, 5))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_divide() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleDivide(tuple(10, 20), tuple(2, 4))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_negate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleNegate(tuple(1, -2))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_untuple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT untuple(tuple(1, 'hello'))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_to_name_value_pairs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleToNameValuePairs(tuple(1 AS id, 'hello' AS name))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_names() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleNames(tuple(1 AS id, 'hello' AS name))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuple(1, 2) < tuple(1, 3)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_tuple_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_filter (id INT64, point Tuple(x INT64, y INT64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tuple_filter VALUES (1, tuple(0, 0)), (2, tuple(10, 10)), (3, tuple(5, 5))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM tuple_filter WHERE point.x > 0 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}
