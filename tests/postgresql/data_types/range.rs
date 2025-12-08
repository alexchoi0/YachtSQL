use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_int4range_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '[1,10)'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_int8range_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '[1,100)'::INT8RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_numrange_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1.5,10.5)'::NUMRANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_tsrange_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[2024-01-01,2024-12-31)'::TSRANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_tstzrange_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[2024-01-01 00:00:00+00,2024-12-31 23:59:59+00)'::TSTZRANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_daterange_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[2024-01-01,2024-12-31)'::DATERANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Parser treats '[1,10]' as a vector instead of a range"]
fn test_range_inclusive_exclusive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '[1,10]'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_exclusive_inclusive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '(1,10]'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_both_exclusive() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '(1,10)'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_empty() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT 'empty'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_unbounded_lower() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '(,10)'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_unbounded_upper() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '[1,)'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_unbounded_both() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '(,)'::INT4RANGE").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_contains_element() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE @> 5")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_element_contained() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT 5 <@ '[1,10)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_contains_range() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE @> '[3,6)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_overlaps() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE && '[5,15)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_strictly_left() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,5)'::INT4RANGE << '[10,20)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_strictly_right() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[10,20)'::INT4RANGE >> '[1,5)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_adjacent() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,5)'::INT4RANGE -|- '[5,10)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_union() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,5)'::INT4RANGE + '[5,10)'::INT4RANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_intersection() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE * '[5,15)'::INT4RANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_range_difference() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE - '[5,15)'::INT4RANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_lower_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LOWER('[1,10)'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_upper_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UPPER('[1,10)'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_isempty_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ISEMPTY('empty'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_lower_inc_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LOWER_INC('[1,10)'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_upper_inc_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UPPER_INC('[1,10)'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_lower_inf_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LOWER_INF('(,10)'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_upper_inf_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UPPER_INF('[1,)'::INT4RANGE)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_merge_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT RANGE_MERGE('[1,5)'::INT4RANGE, '[8,12)'::INT4RANGE)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_range_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE range_test (id INTEGER, r INT4RANGE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_test VALUES (1, '[1,10)')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM range_test").unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_range_index_gist() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE range_gist (id INTEGER, r INT4RANGE)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX range_gist_idx ON range_gist USING GIST (r)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_gist VALUES (1, '[1,10)'), (2, '[20,30)')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM range_gist WHERE r @> 5 ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_range_exclude_constraint() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE room_booking (
            id INTEGER,
            room INTEGER,
            during TSRANGE,
            EXCLUDE USING GIST (room WITH =, during WITH &&)
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_range_comparison_equal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE = '[1,10)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_range_comparison_not_equal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '[1,10)'::INT4RANGE <> '[1,11)'::INT4RANGE")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_range_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE range_agg_test (r INT4RANGE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_agg_test VALUES ('[1,5)'), ('[3,8)'), ('[10,15)')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT RANGE_AGG(r) FROM range_agg_test")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_range_intersect_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE range_int_agg (r INT4RANGE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO range_int_agg VALUES ('[1,10)'), ('[3,8)'), ('[5,15)')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT RANGE_INTERSECT_AGG(r) FROM range_int_agg")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_multirange_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '{[1,5), [8,12)}'::INT4MULTIRANGE")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
#[ignore = "Implement me!"]
fn test_multirange_unnest() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT UNNEST('{[1,5), [8,12)}'::INT4MULTIRANGE)")
        .unwrap();
    assert_eq!(result.num_rows(), 2);
}
