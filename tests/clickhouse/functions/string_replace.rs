use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_replace_one() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceOne('hello world', 'world', 'there')")
        .unwrap();
    assert_table_eq!(result, [["hello there"]]);
}

#[test]
fn test_replace_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceAll('hello hello hello', 'hello', 'hi')")
        .unwrap();
    assert_table_eq!(result, [["hi hi hi"]]);
}

#[test]
fn test_replace() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replace('abcabc', 'bc', 'XY')")
        .unwrap();
    assert_table_eq!(result, [["aXYaXY"]]);
}

#[test]
fn test_replace_regexp_one() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceRegexpOne('hello 123 world', '[0-9]+', 'NUM')")
        .unwrap();
    assert_table_eq!(result, [["hello NUM world"]]);
}

#[test]
fn test_replace_regexp_all() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceRegexpAll('a1b2c3', '[0-9]', 'X')")
        .unwrap();
    assert_table_eq!(result, [["aXbXcX"]]);
}

#[test]
fn test_replace_regexp_with_groups() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT replaceRegexpAll('John Smith', '([A-Z][a-z]+) ([A-Z][a-z]+)', '\\\\2, \\\\1')",
        )
        .unwrap();
    assert_table_eq!(result, [["Smith, John"]]);
}

#[test]
fn test_translate() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT translate('hello', 'el', 'ip')")
        .unwrap();
    assert_table_eq!(result, [["hippo"]]);
}

#[test]
fn test_translate_utf8() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT translateUTF8('héllo', 'é', 'e')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_regexp_quote_meta() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT regexpQuoteMeta('a.b*c?d')")
        .unwrap();
    assert_table_eq!(result, [["a\\.b\\*c\\?d"]]);
}

#[test]
fn test_trim() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT trim('  hello  ')").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_trim_left() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT trimLeft('  hello  ')")
        .unwrap();
    assert_table_eq!(result, [["hello  "]]);
}

#[test]
fn test_trim_right() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT trimRight('  hello  ')")
        .unwrap();
    assert_table_eq!(result, [["  hello"]]);
}

#[test]
fn test_trim_both() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT trimBoth('xxhelloxx', 'x')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_ltrim() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT ltrim('  hello')").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_rtrim() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT rtrim('hello  ')").unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_pad_left() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT leftPad('hi', 5, '*')")
        .unwrap();
    assert_table_eq!(result, [["***hi"]]);
}

#[test]
fn test_pad_right() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT rightPad('hi', 5, '*')")
        .unwrap();
    assert_table_eq!(result, [["hi***"]]);
}

#[test]
fn test_lpad() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT lpad('42', 5, '0')").unwrap();
    assert_table_eq!(result, [["00042"]]);
}

#[test]
fn test_rpad() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT rpad('hi', 5)").unwrap();
    assert_table_eq!(result, [["hi   "]]);
}

#[test]
fn test_normalize_utf8() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT normalizeUTF8NFC('café')")
        .unwrap();
    assert_table_eq!(result, [["café"]]);
}

#[test]
fn test_normalize_utf8_nfd() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT normalizeUTF8NFD('café')")
        .unwrap();
    assert_table_eq!(result, [["cafe\u{301}"]]);
}

#[test]
fn test_normalize_utf8_nfkc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT normalizeUTF8NFKC('ﬁ')")
        .unwrap();
    assert_table_eq!(result, [["fi"]]);
}

#[test]
fn test_normalize_utf8_nfkd() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT normalizeUTF8NFKD('ﬁ')")
        .unwrap();
    assert_table_eq!(result, [["fi"]]);
}

#[test]
fn test_replace_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE templates (template String)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO templates VALUES
            ('Hello, {name}!'),
            ('Welcome to {place}'),
            ('Today is {day}')",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT template,
                replaceOne(template, '{name}', 'Alice') AS replaced
            FROM templates
            ORDER BY template",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["Hello, {name}!", "Hello, Alice!"],
            ["Today is {day}", "Today is {day}"],
            ["Welcome to {place}", "Welcome to {place}"]
        ]
    );
}

#[test]
fn test_sanitize_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT replaceRegexpAll('<script>alert(1)</script>', '<[^>]+>', '')")
        .unwrap();
    assert_table_eq!(result, [["alert(1)"]]);
}

#[test]
fn test_multiple_replacements() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT replaceAll(replaceAll(replaceAll('a-b-c', 'a', '1'), 'b', '2'), 'c', '3')",
        )
        .unwrap();
    assert_table_eq!(result, [["1-2-3"]]);
}
