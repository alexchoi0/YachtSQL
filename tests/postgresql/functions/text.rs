use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_format_basic() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('Hello, %s!', 'World')")
        .unwrap();
    assert_table_eq!(result, [["Hello, World!"]]);
}

#[test]
fn test_format_multiple_args() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%s has %s apples', 'Alice', 5)")
        .unwrap();
    assert_table_eq!(result, [["Alice has 5 apples"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_format_positional() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%2$s, %1$s!', 'World', 'Hello')")
        .unwrap();
    assert_table_eq!(result, [["Hello, World!"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_format_identifier() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%I', 'my table')")
        .unwrap();
    assert_table_eq!(result, [["\"my table\""]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_format_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FORMAT('%L', 'it''s')")
        .unwrap();
    assert_table_eq!(result, [["'it''s'"]]);
}

#[test]
fn test_lpad() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LPAD('hi', 5, '*')").unwrap();
    assert_table_eq!(result, [["***hi"]]);
}

#[test]
fn test_lpad_default() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT LPAD('hi', 5)").unwrap();
    assert_table_eq!(result, [["   hi"]]);
}

#[test]
fn test_rpad() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RPAD('hi', 5, '*')").unwrap();
    assert_table_eq!(result, [["hi***"]]);
}

#[test]
fn test_rpad_default() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT RPAD('hi', 5)").unwrap();
    assert_table_eq!(result, [["hi   "]]);
}

#[test]
fn test_split_part() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SPLIT_PART('a,b,c', ',', 2)")
        .unwrap();
    assert_table_eq!(result, [["b"]]);
}

#[test]
fn test_split_part_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SPLIT_PART('a,b,c', ',', 5)")
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_initcap() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INITCAP('hello world')")
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[test]
fn test_chr() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CHR(65)").unwrap();
    assert_table_eq!(result, [["A"]]);
}

#[test]
fn test_ascii() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ASCII('A')").unwrap();
    assert_table_eq!(result, [[65]]);
}

#[test]
fn test_md5() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MD5('hello')").unwrap();
    assert_table_eq!(result, [["5d41402abc4b2a76b9719d911017c592"]]);
}

#[test]
fn test_translate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRANSLATE('12345', '143', 'ax')")
        .unwrap();
    assert_table_eq!(result, [["a2x5"]]);
}

#[test]
fn test_position() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POSITION('lo' IN 'hello')")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_position_not_found() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT POSITION('xyz' IN 'hello')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_strpos() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STRPOS('hello', 'lo')")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_overlay() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT OVERLAY('Txxxxas' PLACING 'hom' FROM 2 FOR 4)")
        .unwrap();
    assert_table_eq!(result, [["Thomas"]]);
}

#[test]
fn test_quote_ident() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT QUOTE_IDENT('my column')")
        .unwrap();
    assert_table_eq!(result, [["\"my column\""]]);
}

#[test]
fn test_quote_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT QUOTE_LITERAL('hello')")
        .unwrap();
    assert_table_eq!(result, [["'hello'"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_quote_nullable() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT QUOTE_NULLABLE(NULL)").unwrap();
    assert_table_eq!(result, [["NULL"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_btrim() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BTRIM('xyxtrimyyx', 'xy')")
        .unwrap();
    assert_table_eq!(result, [["trim"]]);
}

#[test]
fn test_char_length() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT CHAR_LENGTH('hello')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_character_length() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CHARACTER_LENGTH('hello')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_bit_length() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BIT_LENGTH('hello')").unwrap();
    assert_table_eq!(result, [[40]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_octet_length_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT OCTET_LENGTH('hello')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_starts_with() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STARTS_WITH('hello', 'hel')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_starts_with_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT STARTS_WITH('hello', 'world')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_string_agg() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE names (name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO names VALUES ('Alice'), ('Bob'), ('Charlie')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT STRING_AGG(name, ', ' ORDER BY name) FROM names")
        .unwrap();
    assert_table_eq!(result, [["Alice, Bob, Charlie"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regexp_count() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_COUNT('hello world hello', 'hello')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regexp_instr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_INSTR('hello world', 'world')")
        .unwrap();
    assert_table_eq!(result, [[7]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_regexp_substr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT REGEXP_SUBSTR('hello 123 world', '[0-9]+')")
        .unwrap();
    assert_table_eq!(result, [["123"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_to_hex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT TO_HEX(255)").unwrap();
    assert_table_eq!(result, [["ff"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_parse_ident() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT PARSE_IDENT('myschema.mytable')")
        .unwrap();
    assert_table_eq!(result, [[["myschema", "mytable"]]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_normalize() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NORMALIZE('hello', NFC)")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_is_normalized() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IS_NORMALIZED('hello', NFC)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_concat_ws() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CONCAT_WS(',', 'a', 'b', 'c')")
        .unwrap();
    assert_table_eq!(result, [["a,b,c"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_concat_ws_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT CONCAT_WS(',', 'a', NULL, 'c')")
        .unwrap();
    assert_table_eq!(result, [["a,c"]]);
}
