use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_low_cardinality_string() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_string (id INT64, status LowCardinality(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_string VALUES (1, 'active'), (2, 'inactive'), (3, 'active')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, status FROM lc_string ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "active"], [2, "inactive"], [3, "active"]]);
}

#[test]
fn test_low_cardinality_filter() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_filter (id INT64, category LowCardinality(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_filter VALUES (1, 'A'), (2, 'B'), (3, 'A'), (4, 'C')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM lc_filter WHERE category = 'A' ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}

#[test]
fn test_low_cardinality_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_group (id INT64, type LowCardinality(String), value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO lc_group VALUES (1, 'X', 10), (2, 'Y', 20), (3, 'X', 30), (4, 'Y', 40)",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT type, SUM(value) AS total FROM lc_group GROUP BY type ORDER BY type")
        .unwrap();
    assert_table_eq!(result, [["X", 40], ["Y", 60]]);
}

#[test]
fn test_low_cardinality_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_distinct (tag LowCardinality(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_distinct VALUES ('A'), ('B'), ('A'), ('C'), ('B')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT tag FROM lc_distinct ORDER BY tag")
        .unwrap();
    assert_table_eq!(result, [["A"], ["B"], ["C"]]);
}

#[test]
fn test_low_cardinality_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_left (id INT64, key LowCardinality(String))")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lc_right (key LowCardinality(String), value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_left VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_right VALUES ('A', 100), ('B', 200)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT l.id, r.value FROM lc_left l JOIN lc_right r ON l.key = r.key ORDER BY l.id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 100], [2, 200]]);
}

#[test]
fn test_low_cardinality_nullable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_nullable (id INT64, status LowCardinality(Nullable(String)))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_nullable VALUES (1, 'active'), (2, NULL), (3, 'inactive')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM lc_nullable WHERE status IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_low_cardinality_int() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_int (id INT64, code LowCardinality(Int32))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_int VALUES (1, 100), (2, 200), (3, 100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT code, COUNT(*) AS cnt FROM lc_int GROUP BY code ORDER BY code")
        .unwrap();
    assert_table_eq!(result, [[100, 2], [200, 1]]);
}

#[test]
fn test_to_low_cardinality() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toLowCardinality('hello')")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_low_cardinality_indices_position() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_pos (val LowCardinality(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_pos VALUES ('A'), ('B'), ('A'), ('C')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT lowCardinalityIndices(val) FROM lc_pos ORDER BY val")
        .unwrap();
    assert!(result.num_rows() == 4); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_low_cardinality_keys() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lc_keys (val LowCardinality(String))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lc_keys VALUES ('A'), ('B'), ('A'), ('C')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT lowCardinalityKeys(val) FROM lc_keys LIMIT 1")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
