use crate::assert_table_eq;
use crate::common::{create_executor, i64, numeric, str, tuple};

#[test]
fn test_tuple_constructor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuple(1, 'hello', 3.14)")
        .unwrap();
    assert_table_eq!(
        result,
        [[tuple(vec![i64(1), str("hello"), numeric("3.14")])]]
    );
}

#[test]
fn test_tuple_element_index() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleElement((1, 'hello', 3.14), 2)")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_tuple_element_name() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleElement((a: 1, b: 'hello'), 'a')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_tuple_dot_access() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT (a: 1, b: 'hello').a").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_untuple() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT untuple((1, 'hello'))")
        .unwrap();
    assert_table_eq!(result, [[1, "hello"]]);
}

#[test]
fn test_tuple_hamming_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleHammingDistance((1, 2, 3), (1, 2, 4))")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_tuple_plus() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tuplePlus((1, 2, 3), (10, 20, 30))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(11), i64(22), i64(33)])]]);
}

#[test]
fn test_tuple_minus() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleMinus((10, 20, 30), (1, 2, 3))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(9), i64(18), i64(27)])]]);
}

#[test]
fn test_tuple_multiply() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleMultiply((1, 2, 3), (2, 3, 4))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(2), i64(6), i64(12)])]]);
}

#[test]
fn test_tuple_divide() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleDivide((10, 20, 30), (2, 4, 5))")
        .unwrap();
    assert_table_eq!(result, [[(5.0, 5.0, 6.0)]]);
}

#[test]
fn test_tuple_negate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleNegate((1, -2, 3))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(-1), i64(2), i64(-3)])]]);
}

#[test]
fn test_tuple_multiply_by_number() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleMultiplyByNumber((1, 2, 3), 10)")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(10), i64(20), i64(30)])]]);
}

#[test]
fn test_tuple_divide_by_number() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleDivideByNumber((10, 20, 30), 10)")
        .unwrap();
    assert_table_eq!(result, [[(1.0, 2.0, 3.0)]]);
}

#[test]
fn test_tuple_concat() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleConcat((1, 2), ('a', 'b'), (3.14,))")
        .unwrap();
    assert_table_eq!(
        result,
        [[tuple(vec![
            i64(1),
            i64(2),
            str("a"),
            str("b"),
            numeric("3.14")
        ])]]
    );
}

#[test]
fn test_tuple_int_div() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleIntDiv((10, 21, 35), (3, 4, 6))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(3), i64(5), i64(5)])]]);
}

#[test]
fn test_tuple_int_div_or_zero() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleIntDivOrZero((10, 20), (3, 0))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(3), i64(0)])]]);
}

#[test]
fn test_tuple_modulo() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleModulo((10, 21, 35), (3, 4, 6))")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(1), i64(1), i64(5)])]]);
}

#[test]
fn test_tuple_modulo_by_number() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleModuloByNumber((10, 21, 35), 4)")
        .unwrap();
    assert_table_eq!(result, [[tuple(vec![i64(2), i64(1), i64(3)])]]);
}

#[test]
fn test_tuple_to_name_value_pairs() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleToNameValuePairs((a: 1, b: 2, c: 3))")
        .unwrap();
    assert_table_eq!(result, [[[("a", 1), ("b", 2), ("c", 3)]]]);
}

#[test]
fn test_tuple_names() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tupleNames((a: 1, b: 2, c: 3))")
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
fn test_tuple_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE coords (point Tuple(x Float64, y Float64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO coords VALUES
            ((1.0, 2.0)),
            ((3.0, 4.0)),
            ((5.0, 6.0))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT point.x, point.y
            FROM coords
            ORDER BY point.x",
        )
        .unwrap();
    assert_table_eq!(result, [[1.0, 2.0], [3.0, 4.0], [5.0, 6.0],]);
}

#[test]
fn test_nested_tuple() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ((1, 2), (3, 4)).1").unwrap();
    assert_table_eq!(result, [[(1, 2)]]);
}

#[test]
fn test_tuple_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_cmp (t Tuple(Int64, Int64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO tuple_cmp VALUES
            ((1, 2)),
            ((1, 3)),
            ((2, 1))",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT t
            FROM tuple_cmp
            WHERE t > (1, 2)
            ORDER BY t",
        )
        .unwrap();
    assert_table_eq!(result, [[(1, 3)], [(2, 1)]]);
}

#[test]
fn test_tuple_array_operations() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE tuple_arr (items Array(Tuple(name String, value Int64)))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO tuple_arr VALUES
            ([('a', 1), ('b', 2)]),
            ([('c', 3), ('d', 4), ('e', 5)])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT length(items)
            FROM tuple_arr
            ORDER BY length(items)",
        )
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}
