use crate::assert_table_eq;
use crate::common::{create_session, d};

#[test]
fn test_array_length() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_LENGTH([1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_array_length_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ARRAY_LENGTH([])").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_array_concat() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_CONCAT([1, 2], [3, 4])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4]]]);
}

#[test]
fn test_array_reverse() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_REVERSE([1, 2, 3])")
        .unwrap();
    assert_table_eq!(result, [[[3, 2, 1]]]);
}

#[test]
fn test_generate_array() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT GENERATE_ARRAY(1, 5)").unwrap();
    assert_table_eq!(result, [[[1, 2, 3, 4, 5]]]);
}

#[test]
fn test_generate_array_with_step() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_ARRAY(0, 10, 2)")
        .unwrap();
    assert_table_eq!(result, [[[0, 2, 4, 6, 8, 10]]]);
}

#[test]
fn test_array_to_string() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_TO_STRING(['a', 'b', 'c'], ',')")
        .unwrap();
    assert_table_eq!(result, [["a,b,c"]]);
}

#[test]
fn test_array_contains() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (id INT64, tags ARRAY<STRING>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, ['red', 'blue']), (2, ['green', 'yellow'])")
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM data WHERE 'red' IN UNNEST(tags)")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_length_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT ARRAY_LENGTH(NULL)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_array_slice() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_SLICE([1, 2, 3, 4, 5], 2, 4)")
        .unwrap();
    assert_table_eq!(result, [[[2, 3, 4]]]);
}

#[test]
fn test_array_agg() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE data (category STRING, value INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 1), ('A', 2), ('B', 3)")
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT category, ARRAY_AGG(value ORDER BY value) FROM data GROUP BY category ORDER BY category",
        )
        .unwrap();
    assert_table_eq!(result, [["A", [1, 2]], ["B", [3]]]);
}

#[test]
fn test_unnest_from() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT * FROM UNNEST([1, 2, 3]) AS num ORDER BY num")
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3]]);
}

#[test]
fn test_unnest_with_offset() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT num, offset FROM UNNEST(['a', 'b', 'c']) AS num WITH OFFSET AS offset ORDER BY offset")
        .unwrap();
    assert_table_eq!(result, [["a", 0], ["b", 1], ["c", 2]]);
}

#[test]
fn test_unnest_struct_array() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT s.a, s.b FROM UNNEST([STRUCT(1 AS a, 'x' AS b), STRUCT(2 AS a, 'y' AS b)]) AS s ORDER BY s.a")
        .unwrap();
    assert_table_eq!(result, [[1, "x"], [2, "y"]]);
}

#[test]
fn test_unnest_struct_from_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items (id INT64, things ARRAY<STRUCT<name STRING, qty INT64>>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .unwrap();
    let result = session
        .execute_sql("SELECT id, thing.name, thing.qty FROM items, UNNEST(things) AS thing ORDER BY thing.name")
        .unwrap();
    assert_table_eq!(result, [[1, "apple", 5], [1, "banana", 3]]);
}

#[test]
fn test_unnest_struct_from_table_with_alias() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items2 (id INT64, things ARRAY<STRUCT<name STRING, qty INT64>>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items2 VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .unwrap();
    let result = session
        .execute_sql("SELECT i.id, thing.name, thing.qty FROM items2 i, UNNEST(i.things) AS thing ORDER BY thing.name")
        .unwrap();
    assert_table_eq!(result, [[1, "apple", 5], [1, "banana", 3]]);
}

#[test]
fn test_unnest_struct_in_cte() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items3 (id INT64, things ARRAY<STRUCT<name STRING, qty INT64>>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items3 VALUES (1, [STRUCT('apple' AS name, 5 AS qty), STRUCT('banana' AS name, 3 AS qty)])")
        .unwrap();
    let result = session
        .execute_sql(
            "WITH flattened AS (
            SELECT i.id, thing.name, thing.qty
            FROM items3 i, UNNEST(i.things) AS thing
        )
        SELECT * FROM flattened ORDER BY name",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "apple", 5], [1, "banana", 3]]);
}

#[test]
fn test_unnest_struct_positional_fields() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items4 (id INT64, things ARRAY<STRUCT<product_name STRING, quantity INT64>>)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items4 VALUES (1, [STRUCT('Laptop', 1), STRUCT('Mouse', 2)])")
        .unwrap();
    let result = session
        .execute_sql("SELECT i.id, thing.product_name, thing.quantity FROM items4 i, UNNEST(i.things) AS thing ORDER BY thing.quantity")
        .unwrap();
    assert_table_eq!(result, [[1, "Laptop", 1], [1, "Mouse", 2]]);
}

#[test]
fn test_unnest_struct_nested_array_in_cte() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE orders_with_items (
                order_id INT64,
                items ARRAY<STRUCT<name STRING, discounts ARRAY<STRUCT<code STRING, amount FLOAT64>>>>
            )",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO orders_with_items VALUES
            (1, [STRUCT('Laptop', [STRUCT('SAVE10', 100.0), STRUCT('VIP', 50.0)]),
                 STRUCT('Mouse', [STRUCT('BUNDLE', 5.0)])])",
        )
        .unwrap();
    let result = session
        .execute_sql(
            "WITH items_flat AS (
                SELECT o.order_id, item.name, item.discounts
                FROM orders_with_items o, UNNEST(o.items) AS item
            )
            SELECT f.order_id, f.name, d.code, d.amount
            FROM items_flat f, UNNEST(f.discounts) AS d
            ORDER BY f.name, d.code",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Laptop", "SAVE10", 100.0],
            [1, "Laptop", "VIP", 50.0],
            [1, "Mouse", "BUNDLE", 5.0],
        ]
    );
}

#[test]
fn test_array_agg_unqualified_column_in_cte() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE items_with_tags (
                item_id INT64,
                item_name STRING,
                tag STRING
            )",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items_with_tags VALUES
            (1, 'Laptop', 'tech'),
            (1, 'Laptop', 'portable'),
            (2, 'Mouse', 'tech'),
            (2, 'Mouse', 'accessory')",
        )
        .unwrap();
    let result = session
        .execute_sql(
            "WITH tagged AS (
                SELECT item_id, item_name, tag
                FROM items_with_tags
            )
            SELECT item_id, item_name, ARRAY_AGG(tag) AS tags
            FROM tagged
            GROUP BY item_id, item_name
            ORDER BY item_id",
        )
        .unwrap();
    assert_table_eq!(
        result,
        [
            [1, "Laptop", ["tech", "portable"]],
            [2, "Mouse", ["tech", "accessory"]],
        ]
    );
}

#[test]
fn test_array_agg_with_order_by_in_cte() {
    let mut session = create_session();
    session
        .execute_sql(
            "CREATE TABLE items_with_scores (
                item_id INT64,
                item_name STRING,
                score INT64
            )",
        )
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items_with_scores VALUES
            (1, 'Laptop', 90),
            (1, 'Laptop', 85),
            (2, 'Mouse', 70),
            (2, 'Mouse', 95)",
        )
        .unwrap();
    let result = session
        .execute_sql(
            "WITH scored AS (
                SELECT item_id, item_name, score
                FROM items_with_scores
            )
            SELECT item_id, item_name, ARRAY_AGG(score) AS scores
            FROM scored
            GROUP BY item_id, item_name
            ORDER BY item_id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "Laptop", [90, 85]], [2, "Mouse", [70, 95]],]);
}

#[test]
fn test_array_concat_multiple() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY_CONCAT([1], [2], [3])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
fn test_array_first() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3][OFFSET(0)]").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_safe_offset() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT [1, 2, 3][SAFE_OFFSET(10)]")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_array_ordinal() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT [1, 2, 3][ORDINAL(1)]").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_array_safe_ordinal() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT [1, 2, 3][SAFE_ORDINAL(10)]")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_generate_date_array() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT GENERATE_DATE_ARRAY(DATE '2024-01-01', DATE '2024-01-05')")
        .unwrap();
    assert_table_eq!(
        result,
        [[[
            d(2024, 1, 1),
            d(2024, 1, 2),
            d(2024, 1, 3),
            d(2024, 1, 4),
            d(2024, 1, 5)
        ]]]
    );
}

#[test]
fn test_array_filter() {
    let mut session = create_session();
    let result = session
        .execute_sql(
            "SELECT ARRAY(SELECT x FROM UNNEST([1, 2, 3, 4, 5]) AS x WHERE x > 2 ORDER BY x)",
        )
        .unwrap();
    assert_table_eq!(result, [[[3, 4, 5]]]);
}

#[test]
fn test_array_transform() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT ARRAY(SELECT x * 2 FROM UNNEST([1, 2, 3]) AS x ORDER BY x)")
        .unwrap();
    assert_table_eq!(result, [[[2, 4, 6]]]);
}

#[test]
fn test_array_enumerate() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT arrayEnumerate(['a', 'b', 'c'])")
        .unwrap();
    assert_table_eq!(result, [[[1, 2, 3]]]);
}

#[test]
fn test_array_enumerate_empty() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT arrayEnumerate([])").unwrap();
    assert_table_eq!(result, [[[]]]);
}

#[test]
fn test_map_keys() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT mapKeys(map('a', 1, 'b', 2))")
        .unwrap();
    assert_table_eq!(result, [[["a", "b"]]]);
}

#[test]
fn test_map_values() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT mapValues(map('a', 1, 'b', 2))")
        .unwrap();
    assert_table_eq!(result, [[[1, 2]]]);
}

#[test]
fn test_array_agg_with_subscript() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items_agg (id INT64, tag STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items_agg VALUES (1, 'a'), (1, 'b'), (2, 'c')")
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT id, ARRAY_AGG(tag)[OFFSET(0)] AS first_tag FROM items_agg GROUP BY id ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "a"], [2, "c"]]);
}

#[test]
fn test_array_agg_with_order_by_and_limit() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items_agg2 (id INT64, tag STRING, score INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO items_agg2 VALUES (1, 'a', 10), (1, 'b', 20), (2, 'c', 30)")
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT id, ARRAY_AGG(tag ORDER BY score DESC LIMIT 1)[OFFSET(0)] AS top_tag FROM items_agg2 GROUP BY id ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, "b"], [2, "c"]]);
}

#[test]
fn test_array_agg_struct_basic() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE items_struct (id INT64, name STRING, price FLOAT64)")
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO items_struct VALUES (1, 'A', 10.0), (1, 'B', 20.0), (2, 'C', 30.0)",
        )
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT id, ARRAY_AGG(STRUCT(name, price)) as items FROM items_struct GROUP BY id ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [
        [1, [{"A", 10.0}, {"B", 20.0}]],
        [2, [{"C", 30.0}]]
    ]);
}

#[test]
fn test_array_agg_struct_in_correlated_subquery() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE orders_s (order_id INT64, customer STRING)")
        .unwrap();
    session
        .execute_sql("CREATE TABLE items_s (order_id INT64, name STRING, price INT64)")
        .unwrap();
    session
        .execute_sql("INSERT INTO orders_s VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    session
        .execute_sql("INSERT INTO items_s VALUES (1, 'A', 10), (1, 'B', 20), (2, 'C', 30)")
        .unwrap();
    let result = session
        .execute_sql(
            "SELECT o.order_id, o.customer,
                    (SELECT ARRAY_AGG(STRUCT(i.name, i.price)) FROM items_s i WHERE i.order_id = o.order_id) as items
             FROM orders_s o
             ORDER BY o.order_id",
        )
        .unwrap();
    assert_table_eq!(result, [
        [1, "Alice", [{"A", 10}, {"B", 20}]],
        [2, "Bob", [{"C", 30}]]
    ]);
}
