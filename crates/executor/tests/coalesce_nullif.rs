use yachtsql::{DialectType, QueryExecutor};

#[test]
fn test_coalesce_with_null_column() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING, nickname STRING)")
        .expect("create table");

    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice', NULL)")
        .expect("insert");
    executor
        .execute_sql("INSERT INTO users VALUES (2, 'Bob', 'Bobby')")
        .expect("insert");
    executor
        .execute_sql("INSERT INTO users VALUES (3, 'Charlie', NULL)")
        .expect("insert");

    let result = executor
        .execute_sql("SELECT id, name, COALESCE(nickname, 'No nickname') AS display_name FROM users ORDER BY id")
        .expect("select with COALESCE");

    assert_eq!(result.num_rows(), 3, "Expected 3 rows");
}

#[test]
fn test_coalesce_multiple_arguments() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE contacts (id INT64, primary_email STRING, secondary_email STRING, backup_email STRING)")
        .expect("create table");

    executor
        .execute_sql("INSERT INTO contacts VALUES (1, NULL, 'bob@secondary.com', 'bob@backup.com')")
        .expect("insert");
    executor
        .execute_sql("INSERT INTO contacts VALUES (2, NULL, NULL, 'charlie@backup.com')")
        .expect("insert");
    executor
        .execute_sql("INSERT INTO contacts VALUES (3, NULL, NULL, NULL)")
        .expect("insert");

    let result = executor
        .execute_sql("SELECT id, COALESCE(primary_email, secondary_email, backup_email, 'no-email@example.com') AS email FROM contacts ORDER BY id")
        .expect("select with COALESCE");

    assert_eq!(result.num_rows(), 3, "Expected 3 rows");
}

#[test]
fn test_nullif_division_by_zero_prevention() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE metrics (id INT64, total INT64, count INT64)")
        .expect("create table");

    executor
        .execute_sql("INSERT INTO metrics VALUES (1, 100, 10)")
        .expect("insert");
    executor
        .execute_sql("INSERT INTO metrics VALUES (2, 50, 0)")
        .expect("insert");
    executor
        .execute_sql("INSERT INTO metrics VALUES (3, 75, 5)")
        .expect("insert");

    let result = executor
        .execute_sql(
            "SELECT id, total, count, total / NULLIF(count, 0) AS average FROM metrics ORDER BY id",
        )
        .expect("select with NULLIF");

    assert_eq!(result.num_rows(), 3, "Expected 3 rows");
}
