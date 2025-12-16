use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_update_where_single_row() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, age INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', 30)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 25)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie', 30)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE users SET age = 26 WHERE id = 2");
    assert!(result.is_ok(), "UPDATE WHERE failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM users WHERE id = 2");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);

    let result = executor.execute_sql("SELECT * FROM users WHERE age = 30");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows with age=30");
}

#[test]
fn test_update_where_multiple_rows() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE products (id INT64, category STRING, price FLOAT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO products VALUES (1, 'A', 10.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (2, 'B', 20.0)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO products VALUES (3, 'A', 15.0)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE products SET price = 12.0 WHERE category = 'A'");
    assert!(
        result.is_ok(),
        "UPDATE WHERE multiple rows failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM products WHERE category = 'A'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);

    let result = executor.execute_sql("SELECT * FROM products WHERE price = 20.0");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_update_where_no_matches() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE items (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO items VALUES (1, 10)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO items VALUES (2, 20)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE items SET value = 999 WHERE value = 999");
    assert!(
        result.is_ok(),
        "UPDATE WHERE no matches failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM items WHERE value = 10");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1, "Row with value=10 should be unchanged");
}

#[test]
fn test_update_where_greater_than() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE scores (id INT64, score INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO scores VALUES (1, 85)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES (2, 92)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO scores VALUES (3, 78)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE scores SET score = 95 WHERE score > 80");
    assert!(result.is_ok(), "UPDATE WHERE > failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM scores WHERE score = 95");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2, "Expected 2 rows with score=95");

    let result = executor.execute_sql("SELECT * FROM scores WHERE score = 78");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_update_where_and_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE data (id INT64, x INT64, y INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO data VALUES (1, 10, 20)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (2, 15, 25)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO data VALUES (3, 10, 30)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE data SET x = 99 WHERE x = 10 AND y > 25");
    assert!(result.is_ok(), "UPDATE WHERE AND failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM data WHERE x = 99");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);

    let result = executor.execute_sql("SELECT * FROM data WHERE x = 10 AND y = 20");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_update_where_or_condition() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE flags (id INT64, flag STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (1, 'A')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (2, 'B')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO flags VALUES (3, 'C')")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE flags SET flag = 'X' WHERE flag = 'A' OR flag = 'C'");
    assert!(result.is_ok(), "UPDATE WHERE OR failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM flags WHERE flag = 'X'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);

    let result = executor.execute_sql("SELECT * FROM flags WHERE flag = 'B'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_update_where_null_handling() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE nullable (id INT64, value STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (1, 'test')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (2, NULL)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO nullable VALUES (3, 'data')")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE nullable SET value = 'updated' WHERE value IS NULL");
    assert!(result.is_ok(), "UPDATE WHERE IS NULL failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM nullable WHERE value = 'updated'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);

    let result = executor.execute_sql("SELECT * FROM nullable WHERE value IS NULL");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 0);
}

#[test]
fn test_update_where_multiple_columns() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE multi (id INT64, name STRING, age INT64, city STRING)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO multi VALUES (1, 'Alice', 30, 'NYC')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO multi VALUES (2, 'Bob', 25, 'LA')")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO multi VALUES (3, 'Charlie', 35, 'NYC')")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE multi SET age = 40, city = 'SF' WHERE city = 'NYC'");
    assert!(
        result.is_ok(),
        "UPDATE multiple columns WHERE failed: {:?}",
        result
    );

    let result = executor.execute_sql("SELECT * FROM multi WHERE city = 'SF'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 2);

    let result = executor.execute_sql("SELECT * FROM multi WHERE city = 'LA'");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 1);
}

#[test]
fn test_update_where_preserves_order() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE ordered (id INT64, value INT64)")
        .expect("CREATE TABLE failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (1, 100)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (2, 200)")
        .expect("INSERT failed");
    executor
        .execute_sql("INSERT INTO ordered VALUES (3, 300)")
        .expect("INSERT failed");

    let result = executor.execute_sql("UPDATE ordered SET value = 250 WHERE value = 200");
    assert!(result.is_ok(), "UPDATE WHERE failed: {:?}", result);

    let result = executor.execute_sql("SELECT * FROM ordered");
    assert!(result.is_ok());
    let batch = result.unwrap();
    assert_eq!(batch.num_rows(), 3);
}
