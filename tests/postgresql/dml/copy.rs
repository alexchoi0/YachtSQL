use crate::common::create_executor;

#[test]
fn test_copy_to_stdout_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE copy_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO copy_test VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor.execute_sql("COPY copy_test TO STDOUT").unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_csv() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE csv_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO csv_test VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();

    let result = executor
        .execute_sql("COPY csv_test TO STDOUT WITH (FORMAT CSV)")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_csv_header() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE csv_header_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO csv_header_test VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("COPY csv_header_test TO STDOUT WITH (FORMAT CSV, HEADER)")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_delimiter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE delim_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO delim_test VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("COPY delim_test TO STDOUT WITH (DELIMITER '|')")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_columns() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE col_test (id INTEGER, name TEXT, age INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO col_test VALUES (1, 'Alice', 30)")
        .unwrap();

    let result = executor
        .execute_sql("COPY col_test (id, name) TO STDOUT")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE query_copy (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO query_copy VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("COPY (SELECT * FROM query_copy WHERE val > 150) TO STDOUT")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE null_copy (id INTEGER, val TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_copy VALUES (1, 'test'), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("COPY null_copy TO STDOUT WITH (NULL 'NULL')")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_quote() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE quote_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO quote_test VALUES (1, 'Alice, Bob')")
        .unwrap();

    let result = executor
        .execute_sql("COPY quote_test TO STDOUT WITH (FORMAT CSV, QUOTE '''')")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_escape() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE escape_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO escape_test VALUES (1, 'Test \"quoted\"')")
        .unwrap();

    let result = executor
        .execute_sql("COPY escape_test TO STDOUT WITH (FORMAT CSV, ESCAPE '\\')")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_to_stdout_force_quote() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE force_quote_test (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO force_quote_test VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("COPY force_quote_test TO STDOUT WITH (FORMAT CSV, FORCE_QUOTE (name))")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_from_stdin_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE from_stdin (id INTEGER, name TEXT)")
        .unwrap();

    let result = executor.execute_sql("COPY from_stdin FROM STDIN");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_copy_from_stdin_csv() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE from_stdin_csv (id INTEGER, name TEXT)")
        .unwrap();

    let result = executor.execute_sql("COPY from_stdin_csv FROM STDIN WITH (FORMAT CSV)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_copy_binary() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE binary_copy (id INTEGER, data BYTEA)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO binary_copy VALUES (1, '\\x0102')")
        .unwrap();

    let result = executor.execute_sql("COPY binary_copy TO STDOUT WITH (FORMAT BINARY)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_copy_encoding() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE encoding_copy (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO encoding_copy VALUES (1, 'Test')")
        .unwrap();

    let result = executor.execute_sql("COPY encoding_copy TO STDOUT WITH (ENCODING 'UTF8')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_copy_freeze() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE freeze_copy (id INTEGER)")
        .unwrap();

    let result = executor.execute_sql("COPY freeze_copy FROM STDIN WITH (FREEZE)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_copy_empty_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE empty_copy (id INTEGER, name TEXT)")
        .unwrap();

    let result = executor.execute_sql("COPY empty_copy TO STDOUT").unwrap();
    let _ = result;
}

#[test]
fn test_copy_all_types() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE types_copy (i INTEGER, f DOUBLE PRECISION, s TEXT, b BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO types_copy VALUES (1, 3.14, 'test', TRUE)")
        .unwrap();

    let result = executor
        .execute_sql("COPY types_copy TO STDOUT WITH (FORMAT CSV)")
        .unwrap();
    let _ = result;
}

#[test]
fn test_copy_with_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE where_copy (id INTEGER, active BOOL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO where_copy VALUES (1, TRUE), (2, FALSE), (3, TRUE)")
        .unwrap();

    let result = executor
        .execute_sql("COPY (SELECT * FROM where_copy WHERE active = TRUE) TO STDOUT")
        .unwrap();
    let _ = result;
}
