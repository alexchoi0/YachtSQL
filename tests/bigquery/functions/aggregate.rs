use yachtsql::YachtSQLSession;

use crate::assert_table_eq;
use crate::common::create_session;

fn setup_table(session: &mut YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE numbers (id INT64, value INT64, category STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO numbers VALUES (1, 10, 'A'), (2, 20, 'A'), (3, 30, 'B'), (4, 40, 'B'), (5, 50, 'B')")
        .unwrap();
}

#[test]
fn test_count_star() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session.execute_sql("SELECT COUNT(*) FROM numbers").unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_count_column() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[5]]);
}

#[test]
fn test_count_with_nulls() {
    let mut session = create_session();

    session
        .execute_sql("CREATE TABLE nullable (value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO nullable VALUES (1), (NULL), (3), (NULL)")
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*), COUNT(value) FROM nullable")
        .unwrap();

    assert_table_eq!(result, [[4, 2]]);
}

#[test]
fn test_sum() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[150]]);
}

#[test]
fn test_avg() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT AVG(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[30.0]]);
}

#[test]
fn test_min() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT MIN(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_max() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT MAX(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[50]]);
}

#[test]
fn test_multiple_aggregates() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*), SUM(value), MIN(value), MAX(value) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[5, 150, 10, 50]]);
}

#[test]
fn test_aggregate_with_where() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 20")
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_aggregate_empty_result() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT SUM(value) FROM numbers WHERE value > 100")
        .unwrap();

    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_count_empty_result() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM numbers WHERE value > 100")
        .unwrap();

    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_sum_with_expression() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT SUM(value * 2) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[300]]);
}

#[test]
fn test_count_distinct() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(DISTINCT category) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_count_if() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT_IF(value > 20) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_countif() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNTIF(value > 20) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_sum_if() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT SUM_IF(value, value > 20) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_sumif() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT SUMIF(value, value > 20) FROM numbers")
        .unwrap();

    assert_table_eq!(result, [[120]]);
}

#[test]
fn test_listagg() {
    let mut session = create_session();
    setup_table(&mut session);

    let result = session
        .execute_sql("SELECT LISTAGG(category, ',') FROM numbers")
        .unwrap();

    assert_table_eq!(result, [["A,A,B,B,B"]]);
}

#[test]
fn test_xmlagg() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE xml_data (content STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO xml_data VALUES ('<item>1</item>'), ('<item>2</item>'), ('<item>3</item>')")
        .unwrap();

    let result = session
        .execute_sql("SELECT XMLAGG(content) FROM xml_data")
        .unwrap();

    assert_table_eq!(result, [["<item>1</item><item>2</item><item>3</item>"]]);
}
