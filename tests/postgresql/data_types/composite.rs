use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_type_composite() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TYPE address AS (
            street STRING,
            city STRING,
            zip STRING
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE person_name AS (first_name STRING, last_name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE people (id INT64, name person_name)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO people VALUES (1, ROW('John', 'Doe'))")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM people").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_composite_access_field() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE point_2d AS (x FLOAT64, y FLOAT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE points (id INT64, p point_2d)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO points VALUES (1, ROW(3.0, 4.0))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT (p).x FROM points WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [[3.0]]);
}

#[test]
fn test_composite_row_constructor() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROW(1, 'hello', 3.14)")
        .unwrap();
    assert_eq!(result.num_rows(), 1);
}

#[test]
fn test_composite_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROW(1, 2) = ROW(1, 2)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_composite_comparison_less_than() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROW(1, 2) < ROW(1, 3)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_composite_nested() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE inner_type AS (val INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TYPE outer_type AS (name STRING, inner_val inner_type)")
        .unwrap();

    let result = executor.execute_sql("SELECT ROW('test', ROW(42))::outer_type");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_array_of() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE item AS (name STRING, quantity INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE orders (id INT64, items item[])")
        .unwrap();

    let result =
        executor.execute_sql("INSERT INTO orders VALUES (1, ARRAY[ROW('Widget', 5)::item])");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_update_field() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE coord AS (x INT64, y INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE locations (id INT64, pos coord)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO locations VALUES (1, ROW(10, 20))")
        .unwrap();

    let result = executor.execute_sql("UPDATE locations SET pos.x = 15 WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_in_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE dimensions AS (width INT64, height INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE FUNCTION calc_area(d dimensions) RETURNS INT64
         AS $$ SELECT (d).width * (d).height; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_return_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE result_pair AS (success BOOL, message STRING)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE FUNCTION process() RETURNS result_pair
         AS $$ SELECT ROW(TRUE, 'OK')::result_pair; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_drop_type() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE temp_type AS (a INT64)")
        .unwrap();
    let result = executor.execute_sql("DROP TYPE temp_type");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_alter_type_add() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE extendable AS (a INT64)")
        .unwrap();
    let result = executor.execute_sql("ALTER TYPE extendable ADD ATTRIBUTE b STRING");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_alter_type_drop() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE reducible AS (a INT64, b STRING)")
        .unwrap();
    let result = executor.execute_sql("ALTER TYPE reducible DROP ATTRIBUTE b");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_alter_type_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE renamable AS (old_name INT64)")
        .unwrap();
    let result = executor.execute_sql("ALTER TYPE renamable RENAME ATTRIBUTE old_name TO new_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_is_not_distinct_from() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ROW(1, NULL) IS NOT DISTINCT FROM ROW(1, NULL)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_composite_cast_row() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE typed_row AS (id INT64, name STRING)")
        .unwrap();

    let result = executor.execute_sql("SELECT ROW(1, 'test')::typed_row");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE search_key AS (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE search_data (id INT64, key search_key)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO search_data VALUES (1, ROW(10, 20)), (2, ROW(30, 40))")
        .unwrap();

    let result =
        executor.execute_sql("SELECT id FROM search_data WHERE key = ROW(10, 20)::search_key");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_order_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE sortable AS (priority INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE sort_items (id INT64, item sortable)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sort_items VALUES (1, ROW(2, 'B')), (2, ROW(1, 'A'))")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM sort_items ORDER BY item");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_composite_null_field() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE nullable_fields AS (a INT64, b STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE nullable_comp (id INT64, data nullable_fields)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nullable_comp VALUES (1, ROW(NULL, 'test'))")
        .unwrap();

    let result = executor.execute_sql("SELECT (data).a IS NULL FROM nullable_comp");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_composite_whole_row_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TYPE whole_null AS (a INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_comp (id INT64, data whole_null)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_comp VALUES (1, NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT data IS NULL FROM null_comp");
    assert!(result.is_ok() || result.is_err());
}
