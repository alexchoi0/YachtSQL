use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_l1_norm() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT L1Norm([1, 2, 3])").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_l2_norm() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT L2Norm([3, 4])").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_linf_norm() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LinfNorm([1, -5, 3])").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_lp_norm() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LpNorm([1, 2, 3], 2)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_l1_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT L1Distance([1, 2, 3], [4, 5, 6])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_l2_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT L2Distance([0, 0], [3, 4])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_linf_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LinfDistance([1, 2, 3], [4, 6, 5])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_lp_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LpDistance([1, 2, 3], [4, 5, 6], 2)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_l1_normalize() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT L1Normalize([1, 2, 3])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_l2_normalize() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT L2Normalize([3, 4])").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_linf_normalize() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LinfNormalize([1, -5, 3])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_lp_normalize() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT LpNormalize([1, 2, 3], 2)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_cosine_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT cosineDistance([1, 2], [2, 4])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_dot_product() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT dotProduct([1, 2, 3], [4, 5, 6])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_l2_squared_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT L2SquaredDistance([0, 0], [3, 4])")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_distance_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE vectors (id UInt32, vec Array(Float64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO vectors VALUES
            (1, [1.0, 0.0, 0.0]),
            (2, [0.0, 1.0, 0.0]),
            (3, [0.0, 0.0, 1.0]),
            (4, [1.0, 1.0, 0.0])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, L2Distance(vec, [1.0, 0.0, 0.0]) AS dist
            FROM vectors
            ORDER BY dist",
        )
        .unwrap();
    assert!(result.num_rows() == 4); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_cosine_similarity() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE embeddings (id UInt32, embedding Array(Float32))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO embeddings VALUES
            (1, [1.0, 0.0]),
            (2, [0.707, 0.707]),
            (3, [0.0, 1.0])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, 1 - cosineDistance(embedding, [1.0, 0.0]) AS similarity
            FROM embeddings
            ORDER BY similarity DESC",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_knn_search_simulation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items (id UInt32, features Array(Float64))")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO items VALUES
            (1, [1.0, 2.0, 3.0]),
            (2, [1.1, 2.1, 3.1]),
            (3, [5.0, 6.0, 7.0]),
            (4, [1.2, 2.0, 2.9])",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, L2Distance(features, [1.0, 2.0, 3.0]) AS distance
            FROM items
            ORDER BY distance
            LIMIT 3",
        )
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_normalize_and_distance() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT
                L2Normalize([3, 4]) AS normalized,
                L2Norm(L2Normalize([3, 4])) AS norm_of_normalized",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
