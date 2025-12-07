use crate::common::create_executor;

#[test]
fn test_sample_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_basic (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_basic VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM sample_basic SAMPLE 0.1")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_sample_fraction() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_frac (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=1000 {
        executor
            .execute_sql(&format!("INSERT INTO sample_frac VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM sample_frac SAMPLE 1/10")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "Implement me!"]
#[test]
fn test_sample_rows() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_rows (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_rows VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT * FROM sample_rows SAMPLE 10")
        .unwrap();
    assert!(result.num_rows() <= 20);
}

#[test]
fn test_sample_offset() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_offset (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_offset VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM sample_offset SAMPLE 0.1 OFFSET 0.5")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_sample_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sample_where (id INT64, category String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    for i in 1..=100 {
        let cat = if i % 2 == 0 { "even" } else { "odd" };
        executor
            .execute_sql(&format!(
                "INSERT INTO sample_where VALUES ({}, '{}')",
                i, cat
            ))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM sample_where SAMPLE 0.2 WHERE category = 'even'")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_sample_with_order() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_order (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_order VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT id FROM sample_order SAMPLE 0.1 ORDER BY id LIMIT 5")
        .unwrap();
    assert!(result.num_rows() <= 5);
}

#[test]
fn test_sample_with_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_group (category INT64, value INT64) ENGINE = MergeTree ORDER BY category")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!(
                "INSERT INTO sample_group VALUES ({}, {})",
                i % 5,
                i
            ))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT category, COUNT(*) FROM sample_group SAMPLE 0.5 GROUP BY category ORDER BY category")
        .unwrap();
    assert!(result.num_rows() <= 5);
}

#[ignore = "Implement me!"]
#[test]
fn test_sample_virtual_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_virtual (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_virtual VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT id, _sample_factor FROM sample_virtual SAMPLE 0.1 LIMIT 5")
        .unwrap();
    assert!(result.num_rows() <= 5);
}

#[test]
fn test_sample_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sample_left (id INT64, value INT64) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE sample_right (id INT64, name String) ENGINE = MergeTree ORDER BY id",
        )
        .unwrap();
    for i in 1..=50 {
        executor
            .execute_sql(&format!(
                "INSERT INTO sample_left VALUES ({}, {})",
                i,
                i * 10
            ))
            .unwrap();
        executor
            .execute_sql(&format!(
                "INSERT INTO sample_right VALUES ({}, 'name{}')",
                i, i
            ))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT l.id, l.value, r.name FROM sample_left l SAMPLE 0.2 JOIN sample_right r ON l.id = r.id")
        .unwrap();
    assert!(result.num_rows() > 0);
}

#[ignore = "Implement me!"]
#[test]
fn test_sample_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sample_sub (id INT64) ENGINE = MergeTree ORDER BY id")
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_sub VALUES ({})", i))
            .unwrap();
    }

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM (SELECT id FROM sample_sub SAMPLE 0.1)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "Implement me!"]
#[test]
fn test_sample_deterministic() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE sample_det (id INT64) ENGINE = MergeTree ORDER BY id SAMPLE BY id",
        )
        .unwrap();
    for i in 1..=100 {
        executor
            .execute_sql(&format!("INSERT INTO sample_det VALUES ({})", i))
            .unwrap();
    }

    let result1 = executor
        .execute_sql("SELECT COUNT(*) FROM sample_det SAMPLE 0.1")
        .unwrap();
    let result2 = executor
        .execute_sql("SELECT COUNT(*) FROM sample_det SAMPLE 0.1")
        .unwrap();
    assert_eq!(result1.num_rows(), result2.num_rows());
}
