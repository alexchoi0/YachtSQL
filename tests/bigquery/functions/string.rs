use crate::assert_table_eq;
use crate::common::create_session;

// =============================================================================
// ASCII
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_ascii() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASCII('abcd')").await.unwrap();
    assert_table_eq!(result, [[97]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ascii_single_char() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASCII('a')").await.unwrap();
    assert_table_eq!(result, [[97]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ascii_empty_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASCII('')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ascii_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT ASCII(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// BYTE_LENGTH
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BYTE_LENGTH('hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length_utf8() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT BYTE_LENGTH('абвгд')")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_byte_length_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT BYTE_LENGTH('')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

// =============================================================================
// CHAR_LENGTH / CHARACTER_LENGTH
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_char_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CHAR_LENGTH('hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_char_length_utf8() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CHAR_LENGTH('абвгд')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_character_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CHARACTER_LENGTH('абвгд')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_char_length_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHAR_LENGTH('')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

// =============================================================================
// CHR
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_chr() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHR(65)").await.unwrap();
    assert_table_eq!(result, [["A"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chr_lowercase() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHR(97)").await.unwrap();
    assert_table_eq!(result, [["a"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chr_zero() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHR(0)").await.unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chr_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHR(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chr_extended_ascii() {
    let session = create_session();
    let result = session.execute_sql("SELECT CHR(255)").await.unwrap();
    assert_table_eq!(result, [["ÿ"]]);
}

// =============================================================================
// CODE_POINTS_TO_STRING
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_code_points_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([65, 66, 67])")
        .await
        .unwrap();
    assert_table_eq!(result, [["ABC"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_code_points_to_string_extended() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024])")
        .await
        .unwrap();
    assert_table_eq!(result, [["AÿȁЀ"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_code_points_to_string_empty() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([])")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_code_points_to_string_with_null_element() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([65, 255, NULL, 1024])")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// TO_CODE_POINTS
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_to_code_points() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_CODE_POINTS('foo')")
        .await
        .unwrap();
    assert_table_eq!(result, [[[102, 111, 111]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_code_points_empty() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_CODE_POINTS('')")
        .await
        .unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_to_code_points_utf8() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_CODE_POINTS('Ā')")
        .await
        .unwrap();
    assert_table_eq!(result, [[[256]]]);
}

// =============================================================================
// CODE_POINTS_TO_BYTES
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_code_points_to_bytes() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(CODE_POINTS_TO_BYTES([65, 98, 67, 100]))")
        .await
        .unwrap();
    assert_table_eq!(result, [["AbCd"]]);
}

// =============================================================================
// COLLATE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_collate() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT COLLATE('a', 'und:ci') < COLLATE('Z', 'und:ci')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// CONCAT
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_concat() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT CONCAT('hello', ' ', 'world')")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello world"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_two_args() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('T.P.', ' Bar')")
        .await
        .unwrap();
    assert_table_eq!(result, [["T.P. Bar"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_number() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('Summer', ' ', CAST(1923 AS STRING))")
        .await
        .unwrap();
    assert_table_eq!(result, [["Summer 1923"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('hello', NULL, 'world')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_operator() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

// =============================================================================
// CONTAINS_SUBSTR
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_contains_substr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR('the blue house', 'Blue house')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_contains_substr_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR('the red house', 'blue')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_contains_substr_normalized() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR('\\u2168', 'IX')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_contains_substr_struct() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR((23, 35, 41), '35')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// EDIT_DISTANCE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_edit_distance() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('a', 'b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_edit_distance_two_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('aa', 'b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_edit_distance_one_diff() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('aa', 'ba')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_edit_distance_max_distance() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('abcdefg', 'a', max_distance => 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

// =============================================================================
// ENDS_WITH
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_ends_with() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('apple', 'e')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ends_with_full_suffix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('apple', 'ple')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ends_with_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('apple', 'app')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ends_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH(NULL, 'e')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// STARTS_WITH
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_starts_with() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('bar', 'b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_starts_with_full_prefix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('apple', 'app')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_starts_with_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('apple', 'ple')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_starts_with_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH(NULL, 'b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// FORMAT
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_format_integer() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%d', 10)")
        .await
        .unwrap();
    assert_table_eq!(result, [["10"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('-%s-', 'abcd efg')")
        .await
        .unwrap();
    assert_table_eq!(result, [["-abcd efg-"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_float() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%f', 1.1)")
        .await
        .unwrap();
    assert_table_eq!(result, [["1.100000"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_padded() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('|%10d|', 11)")
        .await
        .unwrap();
    assert_table_eq!(result, [["|        11|"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_zero_padded() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('+%010d+', 12)")
        .await
        .unwrap();
    assert_table_eq!(result, [["+0000000012+"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_with_commas() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT(\"%'d\", 123456789)")
        .await
        .unwrap();
    assert_table_eq!(result, [["123,456,789"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_scientific() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('%E', 2.2)")
        .await
        .unwrap();
    assert_table_eq!(result, [["2.200000E+00"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_percent() {
    let session = create_session();
    let result = session.execute_sql("SELECT FORMAT('%%')").await.unwrap();
    assert_table_eq!(result, [["%"]]);
}

// =============================================================================
// FROM_BASE64 / TO_BASE64
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_to_base64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_BASE64(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["aGVsbG8="]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_base64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64('aGVsbG8='))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_base64_roundtrip() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(TO_BASE64(b'test')))")
        .await
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

// =============================================================================
// FROM_BASE32 / TO_BASE32
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_to_base32() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_BASE32(b'abcde')")
        .await
        .unwrap();
    assert_table_eq!(result, [["MFRGGZDF"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_base32() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE32('MFRGGZDF'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["abcde"]]);
}

// =============================================================================
// FROM_HEX / TO_HEX
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_to_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["68656c6c6f"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_hex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('68656c6c6f'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_hex_uppercase() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48454C4C4F'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_from_hex_odd_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TO_HEX(FROM_HEX('0af'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["00af"]]);
}

// =============================================================================
// INITCAP
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_initcap() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('hello world')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_initcap_mixed_case() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('Hello World-everyone!')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Hello World-Everyone!"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_initcap_custom_delimiters() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('Apples1oranges2pears', '12')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Apples1Oranges2Pears"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_initcap_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT INITCAP(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// INSTR
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_instr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an')")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_instr_occurrence() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an', 1, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_instr_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'ann')")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_instr_from_position() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an', 3, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_instr_negative_position() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an', -1, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_instr_overlapping() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('helloooo', 'oo', 1, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_instr_overlapping_second() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('helloooo', 'oo', 1, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

// =============================================================================
// LEFT
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_left() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT LEFT('hello', 3)")
        .await
        .unwrap();

    assert_table_eq!(result, [["hel"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_banana() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LEFT('banana', 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["ban"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LEFT('hello', 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_left_exceeds_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LEFT('hello', 100)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

// =============================================================================
// LENGTH
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_length() {
    let session = create_session();

    let result = session.execute_sql("SELECT LENGTH('hello')").await.unwrap();

    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_length_utf8() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH('абвгд')").await.unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_length_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH('')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_length_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LENGTH(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// LOWER
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_lower() {
    let session = create_session();

    let result = session.execute_sql("SELECT LOWER('HELLO')").await.unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lower_mixed() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LOWER('FOO BAR BAZ')")
        .await
        .unwrap();
    assert_table_eq!(result, [["foo bar baz"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lower_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LOWER(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// LPAD
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_lpad() {
    let session = create_session();
    let result = session.execute_sql("SELECT LPAD('c', 5)").await.unwrap();
    assert_table_eq!(result, [["    c"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lpad_with_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LPAD('b', 5, 'a')")
        .await
        .unwrap();
    assert_table_eq!(result, [["aaaab"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lpad_with_long_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LPAD('abc', 10, 'ghd')")
        .await
        .unwrap();
    assert_table_eq!(result, [["ghdghdgabc"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lpad_truncate() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LPAD('abc', 2, 'd')")
        .await
        .unwrap();
    assert_table_eq!(result, [["ab"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_lpad_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT LPAD(NULL, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// LTRIM
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_ltrim() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT LTRIM('  hello')")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ltrim_with_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LTRIM('***apple***', '*')")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple***"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ltrim_multiple_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT LTRIM('xxxapplexxx', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [["applexxx"]]);
}

// =============================================================================
// NORMALIZE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_normalize() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('\\u00ea') = NORMALIZE('\\u0065\\u0302')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_normalize_nfkc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('Raha\\u2004Mahan', NFKC)")
        .await
        .unwrap();
    assert_table_eq!(result, [["Raha Mahan"]]);
}

// =============================================================================
// NORMALIZE_AND_CASEFOLD
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_normalize_and_casefold() {
    let session = create_session();
    let result = session
        .execute_sql(
            "SELECT NORMALIZE_AND_CASEFOLD('The red barn') = NORMALIZE_AND_CASEFOLD('The Red Barn')",
        ).await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// OCTET_LENGTH (alias for BYTE_LENGTH)
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_octet_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT OCTET_LENGTH('hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_octet_length_utf8() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT OCTET_LENGTH('абвгд')")
        .await
        .unwrap();
    assert_table_eq!(result, [[10]]);
}

// =============================================================================
// REPEAT
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_repeat() {
    let session = create_session();

    let result = session.execute_sql("SELECT REPEAT('ab', 3)").await.unwrap();

    assert_table_eq!(result, [["ababab"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REPEAT('abc', 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_null_value() {
    let session = create_session();
    let result = session.execute_sql("SELECT REPEAT(NULL, 3)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_repeat_null_count() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REPEAT('abc', NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// REPLACE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_replace() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT REPLACE('hello world', 'world', 'there')")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello there"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_replace_multiple() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REPLACE('apple pie', 'p', 'x')")
        .await
        .unwrap();
    assert_table_eq!(result, [["axxle xie"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_replace_empty_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REPLACE('hello', '', 'x')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_replace_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT REPLACE('hello', 'xyz', 'abc')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

// =============================================================================
// REVERSE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_reverse() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT REVERSE('hello')")
        .await
        .unwrap();

    assert_table_eq!(result, [["olleh"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_reverse_abc() {
    let session = create_session();
    let result = session.execute_sql("SELECT REVERSE('abc')").await.unwrap();
    assert_table_eq!(result, [["cba"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_reverse_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT REVERSE('')").await.unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_reverse_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT REVERSE(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// RIGHT
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_right() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RIGHT('hello', 3)")
        .await
        .unwrap();

    assert_table_eq!(result, [["llo"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_apple() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RIGHT('apple', 3)")
        .await
        .unwrap();
    assert_table_eq!(result, [["ple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_zero() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RIGHT('hello', 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_right_exceeds_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RIGHT('hello', 100)")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

// =============================================================================
// RPAD
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_rpad() {
    let session = create_session();
    let result = session.execute_sql("SELECT RPAD('c', 5)").await.unwrap();
    assert_table_eq!(result, [["c    "]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rpad_with_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RPAD('b', 5, 'a')")
        .await
        .unwrap();
    assert_table_eq!(result, [["baaaa"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rpad_with_long_pattern() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RPAD('abc', 10, 'ghd')")
        .await
        .unwrap();
    assert_table_eq!(result, [["abcghdghdg"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rpad_truncate() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RPAD('abc', 2, 'd')")
        .await
        .unwrap();
    assert_table_eq!(result, [["ab"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rpad_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT RPAD(NULL, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// RTRIM
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_rtrim() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT RTRIM('hello  ')")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rtrim_with_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RTRIM('***apple***', '*')")
        .await
        .unwrap();
    assert_table_eq!(result, [["***apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_rtrim_multiple_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT RTRIM('applexxz', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

// =============================================================================
// SAFE_CONVERT_BYTES_TO_STRING
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_safe_convert_bytes_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .await
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_convert_bytes_to_string_invalid_utf8() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\xc2')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// SOUNDEX
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_soundex() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SOUNDEX('Ashcraft')")
        .await
        .unwrap();
    assert_table_eq!(result, [["A261"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_soundex_similar_names() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SOUNDEX('Robert') = SOUNDEX('Rupert')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// SPLIT
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_split() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SPLIT('a,b,c', ',')")
        .await
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_split_default_delimiter() {
    let session = create_session();
    let result = session.execute_sql("SELECT SPLIT('a,b,c')").await.unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_split_space() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SPLIT('b c d', ' ')")
        .await
        .unwrap();
    assert_table_eq!(result, [[["b", "c", "d"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_split_empty_string() {
    let session = create_session();
    let result = session.execute_sql("SELECT SPLIT('')").await.unwrap();
    assert_table_eq!(result, [[[""]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_split_no_delimiter() {
    let session = create_session();
    let result = session.execute_sql("SELECT SPLIT('a')").await.unwrap();
    assert_table_eq!(result, [[["a"]]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_split_empty_delimiter() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SPLIT('abc', '')")
        .await
        .unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

// =============================================================================
// STRPOS
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_strpos() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS('foo@example.com', '@')")
        .await
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_strpos_not_found() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS('hello', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_strpos_beginning() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS('hello', 'h')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_strpos_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS(NULL, 'a')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// SUBSTR / SUBSTRING
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_substring() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', 1, 5)")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substr() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('apple', 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [["pple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substr_with_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('apple', 2, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [["pp"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substr_negative_position() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('apple', -2)")
        .await
        .unwrap();
    assert_table_eq!(result, [["le"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substr_exceeds_length() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('apple', 1, 123)")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_substr_beyond_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('apple', 123)")
        .await
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

// =============================================================================
// TRANSLATE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_translate() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRANSLATE('This is a cookie', 'sco', 'zku')")
        .await
        .unwrap();
    assert_table_eq!(result, [["Thiz iz a kuukie"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_translate_remove_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRANSLATE('abcdef', 'abc', 'xy')")
        .await
        .unwrap();
    assert_table_eq!(result, [["xydef"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_translate_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRANSLATE(NULL, 'a', 'b')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// TRIM
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_trim() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT TRIM('  hello  ')")
        .await
        .unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_with_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('***apple***', '*')")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_multiple_chars() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('xzxapplexxy', 'xyz')")
        .await
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_trim_concat() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('#', TRIM('   apple   '), '#')")
        .await
        .unwrap();
    assert_table_eq!(result, [["#apple#"]]);
}

// =============================================================================
// UNICODE
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_unicode() {
    let session = create_session();
    let result = session.execute_sql("SELECT UNICODE('âbcd')").await.unwrap();
    assert_table_eq!(result, [[226]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unicode_single() {
    let session = create_session();
    let result = session.execute_sql("SELECT UNICODE('â')").await.unwrap();
    assert_table_eq!(result, [[226]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unicode_empty() {
    let session = create_session();
    let result = session.execute_sql("SELECT UNICODE('')").await.unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unicode_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT UNICODE(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_unicode_ascii() {
    let session = create_session();
    let result = session.execute_sql("SELECT UNICODE('A')").await.unwrap();
    assert_table_eq!(result, [[65]]);
}

// =============================================================================
// UPPER
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_upper() {
    let session = create_session();

    let result = session.execute_sql("SELECT UPPER('hello')").await.unwrap();

    assert_table_eq!(result, [["HELLO"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_upper_mixed() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT UPPER('foo bar baz')")
        .await
        .unwrap();
    assert_table_eq!(result, [["FOO BAR BAZ"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_upper_null() {
    let session = create_session();
    let result = session.execute_sql("SELECT UPPER(NULL)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// COALESCE (included for completeness - conditional function)
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT COALESCE(NULL, 'default')")
        .await
        .unwrap();

    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_coalesce_first_non_null() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT COALESCE('first', 'second')")
        .await
        .unwrap();

    assert_table_eq!(result, [["first"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULLIF('a', 'a')")
        .await
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif_not_equal() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT NULLIF('a', 'b')")
        .await
        .unwrap();

    assert_table_eq!(result, [["a"]]);
}

// =============================================================================
// Integration tests
// =============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_string_functions_on_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE words (word STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO words VALUES ('Hello'), ('World')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT UPPER(word), LENGTH(word) FROM words ORDER BY word")
        .await
        .unwrap();

    assert_table_eq!(result, [["HELLO", 5], ["WORLD", 5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_concat_with_table() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE people (first_name STRING, last_name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO people VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM people ORDER BY first_name").await
        .unwrap();

    assert_table_eq!(result, [["Jane Smith"], ["John Doe"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_split_and_array() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(SPLIT('a,b,c,d'))")
        .await
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_chained_string_functions() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT UPPER(TRIM('  hello world  '))")
        .await
        .unwrap();

    assert_table_eq!(result, [["HELLO WORLD"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_format_in_query() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT FORMAT('Item %d: %s', id, name) FROM items ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [["Item 1: apple"], ["Item 2: banana"]]);
}
