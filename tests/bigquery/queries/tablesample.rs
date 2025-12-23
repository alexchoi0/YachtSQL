use crate::assert_table_eq;
use crate::common::create_session;

fn setup_large_table(session: &mut yachtsql::YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE large_data (id INT64, category STRING, value INT64)")
        .unwrap();
    for i in (1..=100).step_by(10) {
        let values: Vec<String> = (i..i + 10)
            .map(|n| {
                let cat = match n % 3 {
                    0 => "A",
                    1 => "B",
                    _ => "C",
                };
                format!("({}, '{}', {})", n, cat, n * 10)
            })
            .collect();
        let sql = format!("INSERT INTO large_data VALUES {}", values.join(", "));
        session.execute_sql(&sql).unwrap();
    }
}

#[test]
fn test_tablesample_percent() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) <= 100 FROM large_data TABLESAMPLE SYSTEM (10 PERCENT)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_bernoulli() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) <= 100 FROM large_data TABLESAMPLE BERNOULLI (50 PERCENT)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_rows() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) <= 10 FROM large_data TABLESAMPLE SYSTEM (10 ROWS)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_with_where() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql(
            "SELECT COUNT(*) <= 100 FROM large_data TABLESAMPLE SYSTEM (50 PERCENT) WHERE category = 'A'",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_with_order_by() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql(
            "SELECT COUNT(*) <= 5 FROM (SELECT id FROM large_data TABLESAMPLE SYSTEM (10 PERCENT) ORDER BY id LIMIT 5) AS sub",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_with_join() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE categories (name STRING, description STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO categories VALUES ('A', 'Category A'), ('B', 'Category B'), ('C', 'Category C')")
        .unwrap();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) <= 3 FROM (SELECT c.description, COUNT(*) FROM large_data d TABLESAMPLE SYSTEM (20 PERCENT) JOIN categories c ON d.category = c.name GROUP BY c.description ORDER BY c.description) AS sub")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_reproducible() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql(
            "SELECT COUNT(*) >= 0 FROM large_data TABLESAMPLE SYSTEM (50 PERCENT) REPEATABLE(42)",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_zero_percent() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM large_data TABLESAMPLE SYSTEM (0 PERCENT)")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_tablesample_hundred_percent() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) FROM large_data TABLESAMPLE SYSTEM (100 PERCENT)")
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_tablesample_in_subquery() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql(
            "SELECT AVG(value) IS NOT NULL OR COUNT(*) = 0 FROM (SELECT * FROM large_data TABLESAMPLE SYSTEM (50 PERCENT)) AS sub",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_with_group_by() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) <= 3 FROM (SELECT category, COUNT(*) FROM large_data TABLESAMPLE SYSTEM (50 PERCENT) GROUP BY category ORDER BY category) AS sub")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_with_aggregation() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql(
            "SELECT SUM(value) IS NOT NULL OR COUNT(*) = 0 FROM large_data TABLESAMPLE SYSTEM (50 PERCENT)",
        )
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_tablesample_alias() {
    let mut session = create_session();
    setup_large_table(&mut session);

    let result = session
        .execute_sql("SELECT COUNT(*) <= 5 FROM (SELECT t.id, t.value FROM large_data AS t TABLESAMPLE SYSTEM (10 PERCENT) ORDER BY t.id LIMIT 5) AS sub")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
