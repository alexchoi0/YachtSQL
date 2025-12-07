use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_group_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_test (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO group_arr_test VALUES ('A', 1), ('A', 2), ('A', 3), ('B', 4), ('B', 5)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, groupArray(value)
            FROM group_arr_test
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_limit (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_arr_limit VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArray(3)(value) FROM group_arr_limit")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_sorted() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_sorted (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_arr_sorted VALUES (3), (1), (4), (1), (5), (9), (2), (6)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArraySorted(5)(value) FROM group_arr_sorted")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_sample() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_sample (value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO group_arr_sample VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArraySample(3, 12345)(value) FROM group_arr_sample")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_insert_at() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_insert (idx UInt32, value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_arr_insert VALUES (0, 'a'), (2, 'c'), (1, 'b')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArrayInsertAt(value, idx) FROM group_arr_insert")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_moving_avg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_mavg (value Float64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO group_arr_mavg VALUES (1), (2), (3), (4), (5), (6), (7), (8), (9), (10)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArrayMovingAvg(3)(value) FROM group_arr_mavg")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_moving_sum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_msum (value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_arr_msum VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArrayMovingSum(3)(value) FROM group_arr_msum")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_uniq_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_uniq_arr (category String, value String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO group_uniq_arr VALUES
            ('A', 'x'), ('A', 'y'), ('A', 'x'),
            ('B', 'y'), ('B', 'z'), ('B', 'y')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT category, groupUniqArray(value)
            FROM group_uniq_arr
            GROUP BY category
            ORDER BY category",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_uniq_array_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_uniq_limit (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_uniq_limit VALUES ('a'), ('b'), ('c'), ('a'), ('d'), ('e')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupUniqArray(3)(value) FROM group_uniq_limit")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_bit_and() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_bit_and (value UInt8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_bit_and VALUES (0b1111), (0b1110), (0b1100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupBitAnd(value) FROM group_bit_and")
        .unwrap();
    assert_table_eq!(result, [[0b1100]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_group_bit_or() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_bit_or (value UInt8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_bit_or VALUES (0b0001), (0b0010), (0b0100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupBitOr(value) FROM group_bit_or")
        .unwrap();
    assert_table_eq!(result, [[0b0111]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_group_bit_xor() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_bit_xor (value UInt8)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_bit_xor VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupBitXor(value) FROM group_bit_xor")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_group_bitmap() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_bitmap_test (value UInt32)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_bitmap_test VALUES (1), (2), (3), (4), (5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT bitmapCardinality(groupBitmap(value)) FROM group_bitmap_test")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sum_map_test (keys Array(String), values Array(INT64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sum_map_test VALUES
            (['a', 'b'], [1, 2]),
            (['b', 'c'], [3, 4]),
            (['a', 'c'], [5, 6])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumMap(keys, values) FROM sum_map_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_map_with_overflow() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sum_map_overflow (keys Array(String), values Array(INT64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sum_map_overflow VALUES (['a'], [1000000000000]), (['a'], [2000000000000])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumMapWithOverflow(keys, values) FROM sum_map_overflow")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_min_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE min_map_test (keys Array(String), values Array(INT64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO min_map_test VALUES
            (['a', 'b'], [10, 20]),
            (['a', 'c'], [5, 15]),
            (['b', 'c'], [8, 12])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT minMap(keys, values) FROM min_map_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_max_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE max_map_test (keys Array(String), values Array(INT64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO max_map_test VALUES
            (['a', 'b'], [10, 20]),
            (['a', 'c'], [15, 5]),
            (['b', 'c'], [8, 25])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT maxMap(keys, values) FROM max_map_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_avg_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE avg_map_test (keys Array(String), values Array(Float64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO avg_map_test VALUES
            (['a', 'b'], [10, 20]),
            (['a', 'b'], [20, 30]),
            (['a', 'b'], [30, 40])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT avgMap(keys, values) FROM avg_map_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_count_map() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE count_map_test (keys Array(String), values Array(INT64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO count_map_test VALUES
            (['a', 'b'], [1, 1]),
            (['a', 'c'], [1, 1]),
            (['b', 'd'], [1, 1])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumMap(keys, values) FROM count_map_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_grouped() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_grouped (dept String, name String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO group_arr_grouped VALUES
            ('eng', 'alice'), ('eng', 'bob'),
            ('sales', 'charlie'), ('sales', 'david'), ('sales', 'eve')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT dept, groupArray(name), length(groupArray(name)) AS count
            FROM group_arr_grouped
            GROUP BY dept
            ORDER BY dept",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_last() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_arr_last (ts DateTime, event String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO group_arr_last VALUES
            ('2023-01-01 10:00:00', 'start'),
            ('2023-01-01 11:00:00', 'middle'),
            ('2023-01-01 12:00:00', 'end')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArrayLast(2)(event) FROM group_arr_last ORDER BY ts")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sum_map_filtered() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sum_map_filtered (active UInt8, keys Array(String), values Array(INT64))",
        )
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO sum_map_filtered VALUES
            (1, ['a', 'b'], [10, 20]),
            (0, ['a', 'b'], [100, 200]),
            (1, ['b', 'c'], [30, 40])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT sumMapFilteredIf(keys, values, active = 1) FROM sum_map_filtered")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_array_concat_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_concat_test (arr Array(Int64))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO arr_concat_test VALUES ([1, 2]), ([3, 4]), ([5])")
        .unwrap();

    let result = executor
        .execute_sql("SELECT arrayFlatten(groupArray(arr)) FROM arr_concat_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_array_intersect() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE arr_intersect_test (arr Array(String))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO arr_intersect_test VALUES
            (['a', 'b', 'c']),
            (['b', 'c', 'd']),
            (['c', 'd', 'e'])",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupArrayIntersect(arr) FROM arr_intersect_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_group_concat() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE group_concat_test (value String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO group_concat_test VALUES ('hello'), ('world'), ('test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT groupConcat(', ')(value) FROM group_concat_test")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
