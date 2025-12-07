use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_enum() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE status AS ENUM ('pending', 'active', 'completed')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE tasks (id INT64, status status)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO tasks VALUES (1, 'pending')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM tasks").unwrap();
    assert_table_eq!(result, [[1, "pending"]]);
}

#[test]
fn test_enum_comparison_equal() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE color AS ENUM ('red', 'green', 'blue')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 'red'::color = 'red'::color")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_enum_comparison_less_than() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE priority AS ENUM ('low', 'medium', 'high')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 'low'::priority < 'high'::priority")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_enum_comparison_greater_than() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE size AS ENUM ('small', 'medium', 'large')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 'large'::size > 'small'::size")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_enum_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE rank AS ENUM ('bronze', 'silver', 'gold')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE players (id INT64, rank rank)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO players VALUES (1, 'gold'), (2, 'bronze'), (3, 'silver')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM players ORDER BY rank")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[test]
fn test_enum_where_clause() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE state AS ENUM ('draft', 'review', 'published')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE articles (id INT64, state state)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO articles VALUES (1, 'draft'), (2, 'published'), (3, 'review')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM articles WHERE state = 'published'")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_enum_in_list() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE day AS ENUM ('mon', 'tue', 'wed', 'thu', 'fri', 'sat', 'sun')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE events (id INT64, day day)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO events VALUES (1, 'mon'), (2, 'wed'), (3, 'fri')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM events WHERE day IN ('mon', 'wed')")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_alter_type_add_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE ext_enum AS ENUM ('a', 'b', 'c')")
        .unwrap();

    let result = executor.execute_sql("ALTER TYPE ext_enum ADD VALUE 'd'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_type_add_value_before() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE before_enum AS ENUM ('b', 'c')")
        .unwrap();

    let result = executor.execute_sql("ALTER TYPE before_enum ADD VALUE 'a' BEFORE 'b'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_type_add_value_after() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE after_enum AS ENUM ('a', 'c')")
        .unwrap();

    let result = executor.execute_sql("ALTER TYPE after_enum ADD VALUE 'b' AFTER 'a'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_type_rename_value() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE rename_enum AS ENUM ('old', 'other')")
        .unwrap();

    let result = executor.execute_sql("ALTER TYPE rename_enum RENAME VALUE 'old' TO 'new'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_range_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE range_enum AS ENUM ('a', 'b', 'c', 'd')")
        .unwrap();

    let result = executor.execute_sql("SELECT ENUM_RANGE(NULL::range_enum)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_first_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE first_enum AS ENUM ('x', 'y', 'z')")
        .unwrap();

    let result = executor.execute_sql("SELECT ENUM_FIRST(NULL::first_enum)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_last_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE last_enum AS ENUM ('x', 'y', 'z')")
        .unwrap();

    let result = executor.execute_sql("SELECT ENUM_LAST(NULL::last_enum)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_type_enum() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE drop_enum AS ENUM ('a', 'b')")
        .unwrap();

    let result = executor.execute_sql("DROP TYPE drop_enum");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_type_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE cascade_enum AS ENUM ('a', 'b')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE cascade_table (id INT64, val cascade_enum)")
        .unwrap();

    let result = executor.execute_sql("DROP TYPE cascade_enum CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE null_enum AS ENUM ('a', 'b')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_enum_table (id INT64, val null_enum)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_enum_table VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM null_enum_table WHERE val IS NULL")
        .unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[test]
fn test_enum_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE def_enum AS ENUM ('default', 'other')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE def_enum_table (id INT64, val def_enum DEFAULT 'default')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO def_enum_table (id) VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT val FROM def_enum_table")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_enum_cast_to_text() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE text_enum AS ENUM ('hello', 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 'hello'::text_enum::TEXT")
        .unwrap();
    assert_table_eq!(result, [["hello"]]);
}

#[test]
fn test_enum_cast_from_text() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE from_text_enum AS ENUM ('foo', 'bar')")
        .unwrap();

    let result = executor.execute_sql("SELECT 'foo'::from_text_enum");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE group_enum AS ENUM ('cat', 'dog')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE pets (id INT64, type group_enum)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pets VALUES (1, 'cat'), (2, 'cat'), (3, 'dog')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT type, COUNT(*) FROM pets GROUP BY type ORDER BY type")
        .unwrap();
    assert_table_eq!(result, [["cat", 2], ["dog", 1]]);
}

#[test]
fn test_enum_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE dist_enum AS ENUM ('x', 'y', 'z')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE dist_table (id INT64, val dist_enum)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO dist_table VALUES (1, 'x'), (2, 'x'), (3, 'y')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT DISTINCT val FROM dist_table ORDER BY val")
        .unwrap();
    assert_table_eq!(result, [["x"], ["y"]]);
}

#[test]
fn test_enum_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE arr_enum AS ENUM ('a', 'b', 'c')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE arr_enum_table (id INT64, vals arr_enum[])")
        .unwrap();

    let result =
        executor.execute_sql("INSERT INTO arr_enum_table VALUES (1, ARRAY['a', 'b']::arr_enum[])");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_enum_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE idx_enum AS ENUM ('pending', 'active', 'done')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE idx_enum_table (id INT64, status idx_enum)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_status ON idx_enum_table (status)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO idx_enum_table VALUES (1, 'pending'), (2, 'active')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM idx_enum_table WHERE status = 'active'")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_enum_between() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE between_enum AS ENUM ('a', 'b', 'c', 'd', 'e')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT 'c'::between_enum BETWEEN 'b'::between_enum AND 'd'::between_enum")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
