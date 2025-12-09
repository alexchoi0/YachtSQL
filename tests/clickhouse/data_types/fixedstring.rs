use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_fixedstring_create() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_test (id INT64, code FixedString(5))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_test VALUES (1, 'ABCDE'), (2, 'XY')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, code FROM fixed_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
#[ignore = "FixedString zero-padding not yet implemented"]
fn test_fixedstring_padding() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_pad (code FixedString(10))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_pad VALUES ('hello')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT length(code) FROM fixed_pad")
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_fixedstring_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_cmp (id INT64, code FixedString(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_cmp VALUES (1, 'AAA'), (2, 'BBB'), (3, 'AAA')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM fixed_cmp WHERE code = 'AAA' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_fixedstring_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_ord (id INT64, code FixedString(2))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_ord VALUES (1, 'CC'), (2, 'AA'), (3, 'BB')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM fixed_ord ORDER BY code")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
#[ignore = "FixedString zero-padding not yet implemented"]
fn test_fixedstring_concat() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_concat (a FixedString(3), b FixedString(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_concat VALUES ('ABC', 'XYZ')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT concat(a, b) FROM fixed_concat")
        .unwrap();
    assert_table_eq!(result, [["ABC\0\0\0XYZ\0\0\0"]]);
}

#[test]
fn test_fixedstring_substring() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_sub (code FixedString(10))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_sub VALUES ('ABCDEFGHIJ')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT substring(code, 1, 3) FROM fixed_sub")
        .unwrap();
    assert_table_eq!(result, [["ABC"]]);
}

#[test]
fn test_fixedstring_hex() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_hex (data FixedString(4))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_hex VALUES ('test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT hex(data) FROM fixed_hex")
        .unwrap();
    assert_table_eq!(result, [["74657374"]]);
}

#[test]
fn test_fixedstring_unhex() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toFixedString(unhex('74657374'), 4)")
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[test]
fn test_fixedstring_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_null (id INT64, code Nullable(FixedString(5)))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_null VALUES (1, 'AAAAA'), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM fixed_null WHERE code IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_fixedstring_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_group (category FixedString(2), value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_group VALUES ('AA', 10), ('BB', 20), ('AA', 30)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, SUM(value) AS total FROM fixed_group GROUP BY category ORDER BY category")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_fixedstring_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_distinct (code FixedString(3))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_distinct VALUES ('AAA'), ('BBB'), ('AAA'), ('CCC')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT code) FROM fixed_distinct")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
#[ignore = "FixedString zero-padding not yet implemented"]
fn test_fixedstring_binary_data() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fixed_binary (data FixedString(16))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fixed_binary VALUES (unhex('00112233445566778899AABBCCDDEEFF'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT hex(data) FROM fixed_binary")
        .unwrap();
    assert_table_eq!(result, [["00112233445566778899AABBCCDDEEFF"]]);
}
