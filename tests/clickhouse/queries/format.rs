use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_format_tabseparated() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_tsv (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_tsv VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_tsv FORMAT TabSeparated")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_csv() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_csv (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_csv VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_csv FORMAT CSV")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_csvwithnames() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_csvn (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_csvn VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_csvn FORMAT CSVWithNames")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_json() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_json (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_json VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_json FORMAT JSON")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_jsoncompact() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_jsonc (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_jsonc VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_jsonc FORMAT JSONCompact")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_jsoneachrow() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_jsonr (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_jsonr VALUES (1, 'test'), (2, 'test2')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_jsonr FORMAT JSONEachRow")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_pretty() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_pretty (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_pretty VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_pretty FORMAT Pretty")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_prettycompact() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_prettyc (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_prettyc VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_prettyc FORMAT PrettyCompact")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_vertical() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_vert (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_vert VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_vert FORMAT Vertical")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_values() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_values (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_values VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_values FORMAT Values")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_rowbinary() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_rowbin (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_rowbin VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_rowbin FORMAT RowBinary")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_native() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_native (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_native VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_native FORMAT Native")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_null (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_null VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_null FORMAT Null")
        .unwrap();
    assert_table_eq!(result, []);
}

#[test]
fn test_format_xml() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_xml (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_xml VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_xml FORMAT XML")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_format_markdown() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE format_md (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO format_md VALUES (1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM format_md FORMAT Markdown")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
