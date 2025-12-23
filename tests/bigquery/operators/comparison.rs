use yachtsql::YachtSQLSession;

use super::super::common::create_session;
use crate::assert_table_eq;

fn setup_numbers_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .unwrap();
}

fn setup_strings_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE strings (id INT64, text STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO strings VALUES (1, 'apple'), (2, 'banana'), (3, 'cherry')")
        .unwrap();
}

fn setup_nullable_numbers(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE nullable_numbers (id INT64, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable_numbers VALUES (1, 10), (2, NULL), (3, 30)")
        .unwrap();
}

#[test]
fn test_equals_integer() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value = 20")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_equals_string() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text = 'banana'")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_not_equals_integer() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value != 20 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3], [4], [5]]);
}

#[test]
fn test_not_equals_alternate_syntax() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value <> 20 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3], [4], [5]]);
}

#[test]
fn test_less_than() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value < 25 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_less_than_or_equal() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value <= 20 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_greater_than() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value > 30 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[4], [5]]);
}

#[test]
fn test_greater_than_or_equal() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value >= 40 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[4], [5]]);
}

#[test]
fn test_comparison_with_null_equals() {
    let mut session = create_session();
    setup_nullable_numbers(&mut session);

    let result = session
        .execute_sql("SELECT id FROM nullable_numbers WHERE value = NULL")
        .unwrap();

    assert_table_eq!(result, []);
}

#[test]
fn test_is_null() {
    let mut session = create_session();
    setup_nullable_numbers(&mut session);

    let result = session
        .execute_sql("SELECT id FROM nullable_numbers WHERE value IS NULL")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_is_not_null() {
    let mut session = create_session();
    setup_nullable_numbers(&mut session);

    let result = session
        .execute_sql("SELECT id FROM nullable_numbers WHERE value IS NOT NULL ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_between() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value BETWEEN 20 AND 40 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3], [4]]);
}

#[test]
fn test_not_between() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value NOT BETWEEN 20 AND 40 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [5]]);
}

#[test]
fn test_in_list() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value IN (10, 30, 50) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3], [5]]);
}

#[test]
fn test_not_in_list() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value NOT IN (10, 30, 50) ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [4]]);
}

#[test]
fn test_in_string_list() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text IN ('apple', 'cherry') ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_like_prefix() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE 'a%'")
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_like_suffix() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE '%a'")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_like_contains() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE '%an%'")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_like_single_char() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text LIKE '_____'")
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_not_like() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text NOT LIKE '%rry' ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_comparison_expression_result() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT 10 > 5, 10 < 5, 10 = 10, 10 != 10")
        .unwrap();

    assert_table_eq!(result, [[true, false, true, false]]);
}

#[test]
fn test_null_comparison_expression() {
    let mut session = create_session();

    let result = session
        .execute_sql("SELECT NULL = NULL, NULL IS NULL, NULL IS NOT NULL")
        .unwrap();

    assert_table_eq!(result, [[null, true, false]]);
}

#[test]
fn test_string_comparison_operators() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text < 'banana' ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_string_greater_than() {
    let mut session = create_session();
    setup_strings_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM strings WHERE text > 'banana' ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_chained_comparison() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value > 10 AND value < 40 ORDER BY id")
        .unwrap();

    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_comparison_with_expression() {
    let mut session = create_session();
    setup_numbers_table(&mut session);

    let result = session
        .execute_sql("SELECT id FROM numbers WHERE value * 2 = 40")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}
