use crate::assert_table_eq;
use crate::common::create_session;

// =============================================================================
// ASCII
// =============================================================================

#[test]
fn test_ascii() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ASCII('abcd')").unwrap();
    assert_table_eq!(result, [[97]]);
}

#[test]
fn test_ascii_single_char() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ASCII('a')").unwrap();
    assert_table_eq!(result, [[97]]);
}

#[test]
fn test_ascii_empty_string() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ASCII('')").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_ascii_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ASCII(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// BYTE_LENGTH
// =============================================================================

#[test]
fn test_byte_length() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT BYTE_LENGTH('hello')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_byte_length_utf8() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT BYTE_LENGTH('абвгд')").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_byte_length_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT BYTE_LENGTH('')").unwrap();
    assert_table_eq!(result, [[0]]);
}

// =============================================================================
// CHAR_LENGTH / CHARACTER_LENGTH
// =============================================================================

#[test]
fn test_char_length() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHAR_LENGTH('hello')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_char_length_utf8() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHAR_LENGTH('абвгд')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_character_length() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CHARACTER_LENGTH('абвгд')")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_char_length_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHAR_LENGTH('')").unwrap();
    assert_table_eq!(result, [[0]]);
}

// =============================================================================
// CHR
// =============================================================================

#[test]
fn test_chr() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHR(65)").unwrap();
    assert_table_eq!(result, [["A"]]);
}

#[test]
fn test_chr_lowercase() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHR(97)").unwrap();
    assert_table_eq!(result, [["a"]]);
}

#[test]
#[ignore]
fn test_chr_zero() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHR(0)").unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_chr_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHR(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_chr_extended_ascii() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT CHR(255)").unwrap();
    assert_table_eq!(result, [["ÿ"]]);
}

// =============================================================================
// CODE_POINTS_TO_STRING
// =============================================================================

#[test]
#[ignore]
fn test_code_points_to_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([65, 66, 67])")
        .unwrap();
    assert_table_eq!(result, [["ABC"]]);
}

#[test]
#[ignore]
fn test_code_points_to_string_extended() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([65, 255, 513, 1024])")
        .unwrap();
    assert_table_eq!(result, [["AÿȁЀ"]]);
}

#[test]
#[ignore]
fn test_code_points_to_string_empty() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([])")
        .unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
#[ignore]
fn test_code_points_to_string_with_null_element() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CODE_POINTS_TO_STRING([65, 255, NULL, 1024])")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// TO_CODE_POINTS
// =============================================================================

#[test]
#[ignore]
fn test_to_code_points() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_CODE_POINTS('foo')").unwrap();
    assert_table_eq!(result, [[[102, 111, 111]]]);
}

#[test]
#[ignore]
fn test_to_code_points_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_CODE_POINTS('')").unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[test]
#[ignore]
fn test_to_code_points_utf8() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_CODE_POINTS('Ā')").unwrap();
    assert_table_eq!(result, [[[256]]]);
}

// =============================================================================
// CODE_POINTS_TO_BYTES
// =============================================================================

#[test]
#[ignore]
fn test_code_points_to_bytes() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(CODE_POINTS_TO_BYTES([65, 98, 67, 100]))")
        .unwrap();
    assert_table_eq!(result, [["AbCd"]]);
}

// =============================================================================
// COLLATE
// =============================================================================

#[test]
#[ignore]
fn test_collate() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT COLLATE('a', 'und:ci') < COLLATE('Z', 'und:ci')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// CONCAT
// =============================================================================

#[test]
fn test_concat() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT CONCAT('hello', ' ', 'world')")
        .unwrap();

    assert_table_eq!(result, [["hello world"]]);
}

#[test]
fn test_concat_two_args() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('T.P.', ' Bar')")
        .unwrap();
    assert_table_eq!(result, [["T.P. Bar"]]);
}

#[test]
fn test_concat_with_number() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('Summer', ' ', CAST(1923 AS STRING))")
        .unwrap();
    assert_table_eq!(result, [["Summer 1923"]]);
}

#[test]
fn test_concat_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('hello', NULL, 'world')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_concat_operator() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT 'hello' || ' ' || 'world'")
        .unwrap();
    assert_table_eq!(result, [["hello world"]]);
}

// =============================================================================
// CONTAINS_SUBSTR
// =============================================================================

#[test]
#[ignore]
fn test_contains_substr() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR('the blue house', 'Blue house')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore]
fn test_contains_substr_not_found() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR('the red house', 'blue')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore]
fn test_contains_substr_normalized() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR('\\u2168', 'IX')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore]
fn test_contains_substr_struct() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONTAINS_SUBSTR((23, 35, 41), '35')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// EDIT_DISTANCE
// =============================================================================

#[test]
#[ignore]
fn test_edit_distance() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('a', 'b')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_edit_distance_two_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('aa', 'b')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore]
fn test_edit_distance_one_diff() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('aa', 'ba')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore]
fn test_edit_distance_max_distance() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT EDIT_DISTANCE('abcdefg', 'a', max_distance => 2)")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

// =============================================================================
// ENDS_WITH
// =============================================================================

#[test]
fn test_ends_with() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('apple', 'e')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ends_with_full_suffix() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('apple', 'ple')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ends_with_false() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ENDS_WITH('apple', 'app')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_ends_with_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ENDS_WITH(NULL, 'e')").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// STARTS_WITH
// =============================================================================

#[test]
fn test_starts_with() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('bar', 'b')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_starts_with_full_prefix() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('apple', 'app')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_starts_with_false() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH('apple', 'ple')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
fn test_starts_with_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STARTS_WITH(NULL, 'b')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// FORMAT
// =============================================================================

#[test]
fn test_format_integer() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT FORMAT('%d', 10)").unwrap();
    assert_table_eq!(result, [["10"]]);
}

#[test]
fn test_format_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT('-%s-', 'abcd efg')")
        .unwrap();
    assert_table_eq!(result, [["-abcd efg-"]]);
}

#[test]
#[ignore]
fn test_format_float() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT FORMAT('%f', 1.1)").unwrap();
    assert_table_eq!(result, [["1.100000"]]);
}

#[test]
#[ignore]
fn test_format_padded() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT FORMAT('|%10d|', 11)").unwrap();
    assert_table_eq!(result, [["|        11|"]]);
}

#[test]
#[ignore]
fn test_format_zero_padded() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT FORMAT('+%010d+', 12)").unwrap();
    assert_table_eq!(result, [["+0000000012+"]]);
}

#[test]
#[ignore]
fn test_format_with_commas() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT FORMAT(\"%'d\", 123456789)")
        .unwrap();
    assert_table_eq!(result, [["123,456,789"]]);
}

#[test]
#[ignore]
fn test_format_scientific() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT FORMAT('%E', 2.2)").unwrap();
    assert_table_eq!(result, [["2.200000E+00"]]);
}

#[test]
fn test_format_percent() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT FORMAT('%%')").unwrap();
    assert_table_eq!(result, [["%"]]);
}

// =============================================================================
// FROM_BASE64 / TO_BASE64
// =============================================================================

#[test]
fn test_to_base64() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_BASE64(b'hello')").unwrap();
    assert_table_eq!(result, [["aGVsbG8="]]);
}

#[test]
fn test_from_base64() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64('aGVsbG8='))")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_base64_roundtrip() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE64(TO_BASE64(b'test')))")
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

// =============================================================================
// FROM_BASE32 / TO_BASE32
// =============================================================================

#[test]
#[ignore]
fn test_to_base32() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_BASE32(b'abcde')").unwrap();
    assert_table_eq!(result, [["MFRGGZDF"]]);
}

#[test]
#[ignore]
fn test_from_base32() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_BASE32('MFRGGZDF'))")
        .unwrap();
    assert_table_eq!(result, [["abcde"]]);
}

// =============================================================================
// FROM_HEX / TO_HEX
// =============================================================================

#[test]
fn test_to_hex() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_HEX(b'hello')").unwrap();
    assert_table_eq!(result, [["68656c6c6f"]]);
}

#[test]
fn test_from_hex() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('68656c6c6f'))")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_from_hex_uppercase() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('48454C4C4F'))")
        .unwrap();
    assert_table_eq!(result, [["HELLO"]]);
}

#[test]
#[ignore]
fn test_from_hex_odd_length() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(FROM_HEX('0AF'))")
        .unwrap();
    assert_table_eq!(result, [["\n\u{00af}"]]);
}

// =============================================================================
// INITCAP
// =============================================================================

#[test]
fn test_initcap() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('hello world')")
        .unwrap();
    assert_table_eq!(result, [["Hello World"]]);
}

#[test]
#[ignore]
fn test_initcap_mixed_case() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('Hello World-everyone!')")
        .unwrap();
    assert_table_eq!(result, [["Hello World-Everyone!"]]);
}

#[test]
#[ignore]
fn test_initcap_custom_delimiters() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INITCAP('Apples1oranges2pears', '12')")
        .unwrap();
    assert_table_eq!(result, [["Apples1Oranges2Pears"]]);
}

#[test]
fn test_initcap_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT INITCAP(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// INSTR
// =============================================================================

#[test]
fn test_instr() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT INSTR('banana', 'an')").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
#[ignore]
fn test_instr_occurrence() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an', 1, 2)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_instr_not_found() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'ann')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore]
fn test_instr_from_position() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an', 3, 1)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore]
fn test_instr_negative_position() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('banana', 'an', -1, 1)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_instr_overlapping() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('helloooo', 'oo', 1, 1)")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore]
fn test_instr_overlapping_second() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT INSTR('helloooo', 'oo', 1, 2)")
        .unwrap();
    assert_table_eq!(result, [[6]]);
}

// =============================================================================
// LEFT
// =============================================================================

#[test]
fn test_left() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LEFT('hello', 3)").unwrap();

    assert_table_eq!(result, [["hel"]]);
}

#[test]
fn test_left_banana() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LEFT('banana', 3)").unwrap();
    assert_table_eq!(result, [["ban"]]);
}

#[test]
fn test_left_zero() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LEFT('hello', 0)").unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_left_exceeds_length() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LEFT('hello', 100)").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

// =============================================================================
// LENGTH
// =============================================================================

#[test]
fn test_length() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LENGTH('hello')").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_length_utf8() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LENGTH('абвгд')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_length_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LENGTH('')").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_length_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LENGTH(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// LOWER
// =============================================================================

#[test]
fn test_lower() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LOWER('HELLO')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_lower_mixed() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LOWER('FOO BAR BAZ')").unwrap();
    assert_table_eq!(result, [["foo bar baz"]]);
}

#[test]
fn test_lower_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LOWER(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// LPAD
// =============================================================================

#[test]
fn test_lpad() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LPAD('c', 5)").unwrap();
    assert_table_eq!(result, [["    c"]]);
}

#[test]
fn test_lpad_with_pattern() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LPAD('b', 5, 'a')").unwrap();
    assert_table_eq!(result, [["aaaab"]]);
}

#[test]
#[ignore]
fn test_lpad_with_long_pattern() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LPAD('abc', 10, 'ghd')")
        .unwrap();
    assert_table_eq!(result, [["ghdghdgabc"]]);
}

#[test]
fn test_lpad_truncate() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LPAD('abc', 2, 'd')").unwrap();
    assert_table_eq!(result, [["ab"]]);
}

#[test]
fn test_lpad_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT LPAD(NULL, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// LTRIM
// =============================================================================

#[test]
fn test_ltrim() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT LTRIM('  hello')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore]
fn test_ltrim_with_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LTRIM('***apple***', '*')")
        .unwrap();
    assert_table_eq!(result, [["apple***"]]);
}

#[test]
#[ignore]
fn test_ltrim_multiple_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LTRIM('xxxapplexxx', 'xyz')")
        .unwrap();
    assert_table_eq!(result, [["applexxx"]]);
}

// =============================================================================
// NORMALIZE
// =============================================================================

#[test]
#[ignore]
fn test_normalize() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('\\u00ea') = NORMALIZE('\\u0065\\u0302')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore]
fn test_normalize_nfkc() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT NORMALIZE('Raha\\u2004Mahan', NFKC)")
        .unwrap();
    assert_table_eq!(result, [["Raha Mahan"]]);
}

// =============================================================================
// NORMALIZE_AND_CASEFOLD
// =============================================================================

#[test]
#[ignore]
fn test_normalize_and_casefold() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT NORMALIZE_AND_CASEFOLD('The red barn') = NORMALIZE_AND_CASEFOLD('The Red Barn')",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// OCTET_LENGTH (alias for BYTE_LENGTH)
// =============================================================================

#[test]
#[ignore]
fn test_octet_length() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT OCTET_LENGTH('hello')").unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore]
fn test_octet_length_utf8() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT OCTET_LENGTH('абвгд')").unwrap();
    assert_table_eq!(result, [[10]]);
}

// =============================================================================
// REPEAT
// =============================================================================

#[test]
fn test_repeat() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT REPEAT('ab', 3)").unwrap();

    assert_table_eq!(result, [["ababab"]]);
}

#[test]
fn test_repeat_zero() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT REPEAT('abc', 0)").unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_repeat_null_value() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT REPEAT(NULL, 3)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_repeat_null_count() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT REPEAT('abc', NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// REPLACE
// =============================================================================

#[test]
fn test_replace() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT REPLACE('hello world', 'world', 'there')")
        .unwrap();

    assert_table_eq!(result, [["hello there"]]);
}

#[test]
fn test_replace_multiple() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT REPLACE('apple pie', 'p', 'x')")
        .unwrap();
    assert_table_eq!(result, [["axxle xie"]]);
}

#[test]
#[ignore]
fn test_replace_empty_pattern() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT REPLACE('hello', '', 'x')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_replace_not_found() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT REPLACE('hello', 'xyz', 'abc')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

// =============================================================================
// REVERSE
// =============================================================================

#[test]
fn test_reverse() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT REVERSE('hello')").unwrap();

    assert_table_eq!(result, [["olleh"]]);
}

#[test]
fn test_reverse_abc() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT REVERSE('abc')").unwrap();
    assert_table_eq!(result, [["cba"]]);
}

#[test]
fn test_reverse_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT REVERSE('')").unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_reverse_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT REVERSE(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// RIGHT
// =============================================================================

#[test]
fn test_right() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT RIGHT('hello', 3)").unwrap();

    assert_table_eq!(result, [["llo"]]);
}

#[test]
fn test_right_apple() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RIGHT('apple', 3)").unwrap();
    assert_table_eq!(result, [["ple"]]);
}

#[test]
fn test_right_zero() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RIGHT('hello', 0)").unwrap();
    assert_table_eq!(result, [[""]]);
}

#[test]
fn test_right_exceeds_length() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RIGHT('hello', 100)").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

// =============================================================================
// RPAD
// =============================================================================

#[test]
fn test_rpad() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RPAD('c', 5)").unwrap();
    assert_table_eq!(result, [["c    "]]);
}

#[test]
fn test_rpad_with_pattern() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RPAD('b', 5, 'a')").unwrap();
    assert_table_eq!(result, [["baaaa"]]);
}

#[test]
#[ignore]
fn test_rpad_with_long_pattern() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT RPAD('abc', 10, 'ghd')")
        .unwrap();
    assert_table_eq!(result, [["abcghdghdg"]]);
}

#[test]
fn test_rpad_truncate() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RPAD('abc', 2, 'd')").unwrap();
    assert_table_eq!(result, [["ab"]]);
}

#[test]
fn test_rpad_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT RPAD(NULL, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// RTRIM
// =============================================================================

#[test]
fn test_rtrim() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT RTRIM('hello  ')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore]
fn test_rtrim_with_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT RTRIM('***apple***', '*')")
        .unwrap();
    assert_table_eq!(result, [["***apple"]]);
}

#[test]
#[ignore]
fn test_rtrim_multiple_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT RTRIM('applexxz', 'xyz')")
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

// =============================================================================
// SAFE_CONVERT_BYTES_TO_STRING
// =============================================================================

#[test]
fn test_safe_convert_bytes_to_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'hello')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore]
fn test_safe_convert_bytes_to_string_invalid_utf8() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\xc2')")
        .unwrap();
    assert_table_eq!(result, [["�"]]);
}

// =============================================================================
// SOUNDEX
// =============================================================================

#[test]
#[ignore]
fn test_soundex() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SOUNDEX('Ashcraft')").unwrap();
    assert_table_eq!(result, [["A261"]]);
}

#[test]
#[ignore]
fn test_soundex_similar_names() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SOUNDEX('Robert') = SOUNDEX('Rupert')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

// =============================================================================
// SPLIT
// =============================================================================

#[test]
fn test_split() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SPLIT('a,b,c', ',')").unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
fn test_split_default_delimiter() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SPLIT('a,b,c')").unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

#[test]
fn test_split_space() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SPLIT('b c d', ' ')").unwrap();
    assert_table_eq!(result, [[["b", "c", "d"]]]);
}

#[test]
fn test_split_empty_string() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SPLIT('')").unwrap();
    assert_table_eq!(result, [[[""]]]);
}

#[test]
fn test_split_no_delimiter() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SPLIT('a')").unwrap();
    assert_table_eq!(result, [[["a"]]]);
}

#[test]
#[ignore]
fn test_split_empty_delimiter() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SPLIT('abc', '')").unwrap();
    assert_table_eq!(result, [[["a", "b", "c"]]]);
}

// =============================================================================
// STRPOS
// =============================================================================

#[test]
fn test_strpos() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS('foo@example.com', '@')")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_strpos_not_found() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT STRPOS('hello', 'xyz')")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_strpos_beginning() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT STRPOS('hello', 'h')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_strpos_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT STRPOS(NULL, 'a')").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// SUBSTR / SUBSTRING
// =============================================================================

#[test]
fn test_substring() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT SUBSTRING('hello world', 1, 5)")
        .unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_substr() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SUBSTR('apple', 2)").unwrap();
    assert_table_eq!(result, [["pple"]]);
}

#[test]
fn test_substr_with_length() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SUBSTR('apple', 2, 2)").unwrap();
    assert_table_eq!(result, [["pp"]]);
}

#[test]
#[ignore]
fn test_substr_negative_position() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SUBSTR('apple', -2)").unwrap();
    assert_table_eq!(result, [["le"]]);
}

#[test]
fn test_substr_exceeds_length() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT SUBSTR('apple', 1, 123)")
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

#[test]
fn test_substr_beyond_string() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT SUBSTR('apple', 123)").unwrap();
    assert_table_eq!(result, [[""]]);
}

// =============================================================================
// TRANSLATE
// =============================================================================

#[test]
#[ignore]
fn test_translate() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TRANSLATE('This is a cookie', 'sco', 'zku')")
        .unwrap();
    assert_table_eq!(result, [["Thiz iz a kuukie"]]);
}

#[test]
#[ignore]
fn test_translate_remove_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TRANSLATE('abcdef', 'abc', 'xy')")
        .unwrap();
    assert_table_eq!(result, [["xydef"]]);
}

#[test]
#[ignore]
fn test_translate_null() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TRANSLATE(NULL, 'a', 'b')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// TRIM
// =============================================================================

#[test]
fn test_trim() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT TRIM('  hello  ')").unwrap();

    assert_table_eq!(result, [["hello"]]);
}

#[test]
#[ignore]
fn test_trim_with_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('***apple***', '*')")
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

#[test]
#[ignore]
fn test_trim_multiple_chars() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT TRIM('xzxapplexxy', 'xyz')")
        .unwrap();
    assert_table_eq!(result, [["apple"]]);
}

#[test]
fn test_trim_concat() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT CONCAT('#', TRIM('   apple   '), '#')")
        .unwrap();
    assert_table_eq!(result, [["#apple#"]]);
}

// =============================================================================
// UNICODE
// =============================================================================

#[test]
#[ignore]
fn test_unicode() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UNICODE('âbcd')").unwrap();
    assert_table_eq!(result, [[226]]);
}

#[test]
#[ignore]
fn test_unicode_single() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UNICODE('â')").unwrap();
    assert_table_eq!(result, [[226]]);
}

#[test]
#[ignore]
fn test_unicode_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UNICODE('')").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore]
fn test_unicode_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UNICODE(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore]
fn test_unicode_ascii() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UNICODE('A')").unwrap();
    assert_table_eq!(result, [[65]]);
}

// =============================================================================
// UPPER
// =============================================================================

#[test]
fn test_upper() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT UPPER('hello')").unwrap();

    assert_table_eq!(result, [["HELLO"]]);
}

#[test]
fn test_upper_mixed() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UPPER('foo bar baz')").unwrap();
    assert_table_eq!(result, [["FOO BAR BAZ"]]);
}

#[test]
fn test_upper_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT UPPER(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

// =============================================================================
// COALESCE (included for completeness - conditional function)
// =============================================================================

#[test]
fn test_coalesce() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT COALESCE(NULL, 'default')")
        .unwrap();

    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_coalesce_first_non_null() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT COALESCE('first', 'second')")
        .unwrap();

    assert_table_eq!(result, [["first"]]);
}

#[test]
fn test_nullif() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT NULLIF('a', 'a')").unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_nullif_not_equal() {
    let mut session = create_session();

    let result = session.execute_sql("SELECT NULLIF('a', 'b')").unwrap();

    assert_table_eq!(result, [["a"]]);
}

// =============================================================================
// Integration tests
// =============================================================================

#[test]
fn test_string_functions_on_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE words (word STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO words VALUES ('Hello'), ('World')")
        .unwrap();

    let result = session
        .execute_sql("SELECT UPPER(word), LENGTH(word) FROM words ORDER BY word")
        .unwrap();

    assert_table_eq!(result, [["HELLO", 5], ["WORLD", 5]]);
}

#[test]
fn test_concat_with_table() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE people (first_name STRING, last_name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO people VALUES ('John', 'Doe'), ('Jane', 'Smith')")
        .unwrap();

    let result = session
        .execute_sql("SELECT CONCAT(first_name, ' ', last_name) AS full_name FROM people ORDER BY first_name")
        .unwrap();

    assert_table_eq!(result, [["Jane Smith"], ["John Doe"]]);
}

#[test]
fn test_split_and_array() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(SPLIT('a,b,c,d'))")
        .unwrap();

    assert_table_eq!(result, [[4]]);
}

#[test]
fn test_chained_string_functions() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT UPPER(TRIM('  hello world  '))")
        .unwrap();

    assert_table_eq!(result, [["HELLO WORLD"]]);
}

#[test]
fn test_format_in_query() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE items (id INT64, name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, 'apple'), (2, 'banana')")
        .unwrap();

    let result = session
        .execute_sql("SELECT FORMAT('Item %d: %s', id, name) FROM items ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [["Item 1: apple"], ["Item 2: banana"]]);
}
