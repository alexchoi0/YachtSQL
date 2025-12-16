use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_row_number_no_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE sales (id INT64, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO sales VALUES (1, 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES (2, 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO sales VALUES (3, 150)")
        .expect("INSERT failed");

    let result =
        executor.execute_sql("SELECT id, ROW_NUMBER() OVER (ORDER BY amount) AS rn FROM sales");
    assert!(result.is_ok(), "ROW_NUMBER failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 rows");
}

#[test]
fn test_row_number_with_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE orders (customer STRING, amount INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Alice', 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Bob', 150)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO orders VALUES ('Bob', 250)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT customer, amount, ROW_NUMBER() OVER (PARTITION BY customer ORDER BY amount) AS rn FROM orders"
    );
    assert!(
        result.is_ok(),
        "ROW_NUMBER with PARTITION failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 4, "Expected 4 rows");
}

#[test]
fn test_rank_no_ties() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (student STRING, score INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 85)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Charlie', 95)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT student, score, RANK() OVER (ORDER BY score DESC) AS rank FROM scores",
    );
    assert!(result.is_ok(), "RANK failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3, "Expected 3 rows");
}

#[test]
fn test_rank_with_ties() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (student STRING, score INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Charlie', 85)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('David', 95)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT student, score, RANK() OVER (ORDER BY score DESC) AS rank FROM scores",
    );
    assert!(result.is_ok(), "RANK with ties failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 4, "Expected 4 rows");
}

#[test]
fn test_dense_rank_with_ties() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (student STRING, score INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO scores VALUES ('Alice', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Bob', 90)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('Charlie', 85)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES ('David', 95)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT student, score, DENSE_RANK() OVER (ORDER BY score DESC) AS dense_rank FROM scores",
    );
    assert!(result.is_ok(), "DENSE_RANK failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 4, "Expected 4 rows");
}

#[test]
fn test_ntile_basic() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .expect("CREATE failed");

    for i in 1..=10 {
        executor
            .execute_sql(&format!("INSERT INTO data VALUES ({}, {})", i, i * 10))
            .expect("INSERT failed");
    }

    let result =
        executor.execute_sql("SELECT id, NTILE(4) OVER (ORDER BY value) AS quartile FROM data");
    assert!(result.is_ok(), "NTILE failed: {:?}", result);
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 10, "Expected 10 rows");
}

#[test]
fn test_multiple_window_functions() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE employees (dept STRING, salary INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO employees VALUES ('Engineering', 80000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Engineering', 90000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Sales', 60000)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO employees VALUES ('Sales', 70000)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT dept, salary,
         ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary) AS rn,
         RANK() OVER (PARTITION BY dept ORDER BY salary) AS rank
         FROM employees",
    );
    assert!(
        result.is_ok(),
        "Multiple window functions failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 4, "Expected 4 rows");
}

#[test]
fn test_window_function_with_nulls() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO data VALUES (1, 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (3, 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (4, NULL)")
        .expect("INSERT failed");

    let result = executor
        .execute_sql("SELECT id, value, ROW_NUMBER() OVER (ORDER BY value) AS rn FROM data");
    assert!(
        result.is_ok(),
        "Window function with NULLs failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 4, "Expected 4 rows");
}

#[test]
fn test_window_function_empty_partition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .expect("CREATE failed");

    executor
        .execute_sql("INSERT INTO data VALUES ('A', 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES ('A', 200)")
        .expect("INSERT failed");

    let result = executor.execute_sql(
        "SELECT category, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) AS rn FROM data"
    );
    assert!(
        result.is_ok(),
        "Window function with single partition failed: {:?}",
        result
    );
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows");
}
