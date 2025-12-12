use yachtsql_common::types::Value;
use yachtsql_functions::FunctionRegistry;

#[test]
fn test_basic_string_functions() {
    let registry = FunctionRegistry::new();

    let upper_fn = registry
        .get_scalar("UPPER")
        .expect("UPPER function not found");
    let result = upper_fn
        .evaluate(&[Value::string("hello".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("HELLO"));

    let lower_fn = registry
        .get_scalar("LOWER")
        .expect("LOWER function not found");
    let result = lower_fn
        .evaluate(&[Value::string("HELLO".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello"));

    let length_fn = registry
        .get_scalar("LENGTH")
        .expect("LENGTH function not found");
    let result = length_fn
        .evaluate(&[Value::string("hello".to_string())])
        .unwrap();
    assert_eq!(result.as_i64(), Some(5));

    let trim_fn = registry
        .get_scalar("TRIM")
        .expect("TRIM function not found");
    let result = trim_fn
        .evaluate(&[Value::string("  hello  ".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello"));

    let ltrim_fn = registry
        .get_scalar("LTRIM")
        .expect("LTRIM function not found");
    let result = ltrim_fn
        .evaluate(&[Value::string("  hello  ".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello  "));

    let rtrim_fn = registry
        .get_scalar("RTRIM")
        .expect("RTRIM function not found");
    let result = rtrim_fn
        .evaluate(&[Value::string("  hello  ".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("  hello"));

    let concat_fn = registry
        .get_scalar("CONCAT")
        .expect("CONCAT function not found");
    let result = concat_fn
        .evaluate(&[
            Value::string("hello".to_string()),
            Value::string(" ".to_string()),
            Value::string("world".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello world"));

    let reverse_fn = registry
        .get_scalar("REVERSE")
        .expect("REVERSE function not found");
    let result = reverse_fn
        .evaluate(&[Value::string("hello".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("olleh"));

    println!("✓ All basic string function tests passed!");
}

#[test]
fn test_trim_chars_functions() {
    let registry = FunctionRegistry::new();

    let trim_chars_fn = registry
        .get_scalar("TRIM_CHARS")
        .expect("TRIM_CHARS function not found");
    let result = trim_chars_fn
        .evaluate(&[
            Value::string("xxxhelloxxx".to_string()),
            Value::string("x".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello"));

    let ltrim_chars_fn = registry
        .get_scalar("LTRIM_CHARS")
        .expect("LTRIM_CHARS function not found");
    let result = ltrim_chars_fn
        .evaluate(&[
            Value::string("xxxhello".to_string()),
            Value::string("x".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello"));

    let rtrim_chars_fn = registry
        .get_scalar("RTRIM_CHARS")
        .expect("RTRIM_CHARS function not found");
    let result = rtrim_chars_fn
        .evaluate(&[
            Value::string("helloxxx".to_string()),
            Value::string("x".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello"));

    println!("✓ All TRIM_CHARS function tests passed!");
}

#[test]
fn test_substring_functions() {
    let registry = FunctionRegistry::new();

    let left_fn = registry
        .get_scalar("LEFT")
        .expect("LEFT function not found");
    let result = left_fn
        .evaluate(&[Value::string("hello world".to_string()), Value::int64(5)])
        .unwrap();
    assert_eq!(result.as_str(), Some("hello"));

    let right_fn = registry
        .get_scalar("RIGHT")
        .expect("RIGHT function not found");
    let result = right_fn
        .evaluate(&[Value::string("hello world".to_string()), Value::int64(5)])
        .unwrap();
    assert_eq!(result.as_str(), Some("world"));

    let substring_fn = registry
        .get_scalar("SUBSTRING")
        .expect("SUBSTRING function not found");
    let result = substring_fn
        .evaluate(&[
            Value::string("hello world".to_string()),
            Value::int64(7),
            Value::int64(5),
        ])
        .unwrap();
    assert_eq!(result.as_str(), Some("world"));

    println!("✓ All substring function tests passed!");
}

#[test]
fn test_character_functions() {
    let registry = FunctionRegistry::new();

    let ascii_fn = registry
        .get_scalar("ASCII")
        .expect("ASCII function not found");
    let result = ascii_fn
        .evaluate(&[Value::string("a".to_string())])
        .unwrap();
    assert_eq!(result.as_i64(), Some(97));

    let chr_fn = registry.get_scalar("CHR").expect("CHR function not found");
    let result = chr_fn.evaluate(&[Value::int64(97)]).unwrap();
    assert_eq!(result.as_str(), Some("a"));

    let char_len_fn = registry
        .get_scalar("CHAR_LENGTH")
        .expect("CHAR_LENGTH function not found");
    let result = char_len_fn
        .evaluate(&[Value::string("hello".to_string())])
        .unwrap();
    assert_eq!(result.as_i64(), Some(5));

    println!("✓ All character function tests passed!");
}

#[test]
fn test_search_functions() {
    let registry = FunctionRegistry::new();

    let position_fn = registry
        .get_scalar("POSITION")
        .expect("POSITION function not found");
    let result = position_fn
        .evaluate(&[
            Value::string("world".to_string()),
            Value::string("hello world".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_i64(), Some(7));

    let strpos_fn = registry
        .get_scalar("STRPOS")
        .expect("STRPOS function not found");
    let result = strpos_fn
        .evaluate(&[
            Value::string("hello world".to_string()),
            Value::string("world".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_i64(), Some(7));

    println!("✓ All search function tests passed!");
}

#[test]
fn test_formatting_functions() {
    let registry = FunctionRegistry::new();

    let format_fn = registry
        .get_scalar("FORMAT")
        .expect("FORMAT function not found");
    let result = format_fn
        .evaluate(&[
            Value::string("Hello, %s!".to_string()),
            Value::string("World".to_string()),
        ])
        .unwrap();
    assert_eq!(result.as_str(), Some("Hello, World!"));

    let quote_ident_fn = registry
        .get_scalar("QUOTE_IDENT")
        .expect("QUOTE_IDENT function not found");
    let result = quote_ident_fn
        .evaluate(&[Value::string("column_name".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("\"column_name\""));

    let quote_literal_fn = registry
        .get_scalar("QUOTE_LITERAL")
        .expect("QUOTE_LITERAL function not found");
    let result = quote_literal_fn
        .evaluate(&[Value::string("hello".to_string())])
        .unwrap();
    assert_eq!(result.as_str(), Some("'hello'"));

    println!("✓ All formatting function tests passed!");
}
