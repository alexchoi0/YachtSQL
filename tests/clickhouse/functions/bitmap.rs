use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_build() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapBuild([1, 2, 3, 4, 5])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_to_array() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapToArray(bitmapBuild([1, 2, 3]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapCardinality(bitmapBuild([1, 2, 3, 4, 5]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_and() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT bitmapToArray(bitmapAnd(bitmapBuild([1, 2, 3]), bitmapBuild([2, 3, 4])))",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_or() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapToArray(bitmapOr(bitmapBuild([1, 2]), bitmapBuild([3, 4])))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_xor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT bitmapToArray(bitmapXor(bitmapBuild([1, 2, 3]), bitmapBuild([2, 3, 4])))",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_and_not() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapToArray(bitmapAndnot(bitmapBuild([1, 2, 3]), bitmapBuild([2])))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapContains(bitmapBuild([1, 2, 3]), 2)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_has_any() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapHasAny(bitmapBuild([1, 2, 3]), bitmapBuild([3, 4, 5]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_has_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapHasAll(bitmapBuild([1, 2, 3, 4]), bitmapBuild([2, 3]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_and_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapAndCardinality(bitmapBuild([1, 2, 3]), bitmapBuild([2, 3, 4]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_or_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapOrCardinality(bitmapBuild([1, 2]), bitmapBuild([3, 4]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_xor_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapXorCardinality(bitmapBuild([1, 2, 3]), bitmapBuild([2, 3, 4]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_andnot_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapAndnotCardinality(bitmapBuild([1, 2, 3]), bitmapBuild([2]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_min() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapMin(bitmapBuild([3, 1, 4, 1, 5]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_max() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapMax(bitmapBuild([3, 1, 4, 1, 5]))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_subset_in_range() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT bitmapToArray(bitmapSubsetInRange(bitmapBuild([1, 2, 3, 4, 5]), 2, 4))",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_subset_limit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapToArray(bitmapSubsetLimit(bitmapBuild([1, 2, 3, 4, 5]), 2, 3))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_transform() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT bitmapToArray(bitmapTransform(bitmapBuild([1, 2, 3]), [1, 2], [10, 20]))",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sub_bitmap() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT bitmapToArray(subBitmap(bitmapBuild([1, 2, 3, 4, 5]), 1, 3))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_bitmap_column_aggregate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bitmap_agg_test (user_id UInt32, page_id UInt32)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO bitmap_agg_test VALUES (1, 100), (1, 101), (2, 100), (2, 102), (3, 101)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT bitmapCardinality(groupBitmapState(page_id)) AS unique_pages
            FROM bitmap_agg_test",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
