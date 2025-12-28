use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_tables(session: &yachtsql::YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 'a', 10), (2, 'b', 20), (3, 'c', 30)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO source VALUES (2, 'b_updated', 25), (3, 'c_updated', 35), (4, 'd', 40)",
        )
        .await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_update_when_matched() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, value FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "a", 10], [2, "b_updated", 25], [3, "c_updated", 35],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_insert_when_not_matched() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [3], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_delete_when_matched() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN DELETE")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_update_and_insert() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(
        result,
        [[1, "a"], [2, "b_updated"], [3, "c_updated"], [4, "d"],]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_with_condition() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED AND S.value > 30 THEN UPDATE SET name = S.name, value = S.value").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target WHERE id = 3")
        .await
        .unwrap();
    assert_table_eq!(result, [[3, 35]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_update_delete_insert() {
    let session = create_session();
    setup_tables(&session).await;

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED AND S.value > 30 THEN DELETE WHEN MATCHED THEN UPDATE SET name = S.name, value = S.value WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1], [2], [4]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_with_subquery_source() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (2, 25), (3, 30)")
        .await
        .unwrap();

    session
        .execute_sql("MERGE INTO target T USING (SELECT id, value FROM source WHERE value > 20) S ON T.id = S.id WHEN MATCHED THEN UPDATE SET value = S.value WHEN NOT MATCHED THEN INSERT (id, value) VALUES (S.id, S.value)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 10], [2, 25], [3, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_insert_row() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, name STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (1, 'new', 100)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T USING source S ON T.id = S.id WHEN NOT MATCHED THEN INSERT ROW",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name, value FROM target")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "new", 100]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_when_not_matched_by_source() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (2, 25)")
        .await
        .unwrap();

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN NOT MATCHED BY SOURCE THEN DELETE").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_all_clauses() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (2, 25), (4, 40)")
        .await
        .unwrap();

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET value = S.value WHEN NOT MATCHED BY TARGET THEN INSERT (id, value) VALUES (S.id, S.value) WHEN NOT MATCHED BY SOURCE THEN DELETE").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[2, 25], [4, 40]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_with_constants() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, status STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 'old', 10)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (1, 20), (2, 30)")
        .await
        .unwrap();

    session
        .execute_sql("MERGE INTO target T USING source S ON T.id = S.id WHEN MATCHED THEN UPDATE SET status = 'updated', value = S.value WHEN NOT MATCHED THEN INSERT (id, status, value) VALUES (S.id, 'new', S.value)").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, status FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "updated"], [2, "new"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_insert_new_items_with_condition() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                quantity INT64,
                supply_constrained BOOL,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('microwave', 20, NULL, []),
            ('washer', 20, false, [])",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30),
            ('dryer', 30),
            ('oven', 5),
            ('washer', 10)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO detailed_inventory T
            USING inventory S
            ON T.product = S.product
            WHEN NOT MATCHED AND quantity < 20 THEN
              INSERT(product, quantity, supply_constrained, comments)
              VALUES(product, quantity, true, [(DATE '2024-01-01', 'low stock')])
            WHEN NOT MATCHED THEN
              INSERT(product, quantity, supply_constrained)
              VALUES(product, quantity, false)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, supply_constrained
            FROM detailed_inventory
            ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["dishwasher", false],
            ["dryer", false],
            ["microwave", null],
            ["oven", true],
            ["washer", false],
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_update_and_delete_with_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64, warehouse STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 20, 'warehouse #2'),
            ('oven', 30, 'warehouse #3'),
            ('washer', 10, 'warehouse #1')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO new_arrivals T
            USING (SELECT * FROM new_arrivals WHERE warehouse <> 'warehouse #2') S
            ON T.product = S.product
            WHEN MATCHED AND T.warehouse = 'warehouse #1' THEN
              UPDATE SET quantity = T.quantity + 20
            WHEN MATCHED THEN
              DELETE",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, quantity FROM new_arrivals ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer", 20], ["washer", 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_false_predicate_replace() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dishwasher', 30),
            ('dryer', 50),
            ('washer', 20),
            ('microwave', 20),
            ('oven', 35)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('washer', 30)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO inventory T
            USING new_arrivals S
            ON FALSE
            WHEN NOT MATCHED AND product LIKE '%washer%' THEN
              INSERT (product, quantity) VALUES(product, quantity)
            WHEN NOT MATCHED BY SOURCE AND product LIKE '%washer%' THEN
              DELETE",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT product FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(result, [["dryer"], ["microwave"], ["oven"], ["washer"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_with_avg_subquery() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE detailed_inventory (
                product STRING,
                comments ARRAY<STRUCT<created DATE, comment STRING>>
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO detailed_inventory VALUES
            ('dryer', []),
            ('microwave', []),
            ('oven', []),
            ('refrigerator', []),
            ('washer', [])",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dryer', 50),
            ('microwave', 20),
            ('oven', 35),
            ('refrigerator', 25),
            ('washer', 30)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO detailed_inventory T
            USING inventory S
            ON T.product = S.product
            WHEN MATCHED AND S.quantity < (SELECT AVG(quantity) FROM inventory) THEN
              UPDATE SET comments = ARRAY_CONCAT(comments, [(DATE '2024-02-01', 'below average')])",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            "SELECT product, ARRAY_LENGTH(comments)
            FROM detailed_inventory
            ORDER BY product",
        )
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["dryer", 0],
            ["microwave", 1],
            ["oven", 0],
            ["refrigerator", 1],
            ["washer", 1]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_with_joined_source() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE inventory (product STRING, quantity INT64)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE new_arrivals (product STRING, quantity INT64, warehouse STRING)")
        .await
        .unwrap();

    session
        .execute_sql("CREATE TABLE warehouse (warehouse STRING, state STRING)")
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO inventory VALUES
            ('dryer', 50),
            ('microwave', 20),
            ('oven', 35),
            ('refrigerator', 25),
            ('washer', 30)",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO new_arrivals VALUES
            ('dryer', 20, 'warehouse #2'),
            ('refrigerator', 25, 'warehouse #2'),
            ('washer', 30, 'warehouse #1')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO warehouse VALUES
            ('warehouse #1', 'WA'),
            ('warehouse #2', 'CA'),
            ('warehouse #3', 'WA')",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO inventory T
            USING (SELECT product, quantity, state FROM new_arrivals t1 JOIN warehouse t2 ON t1.warehouse = t2.warehouse) S
            ON T.product = S.product
            WHEN MATCHED AND state = 'CA' THEN
              UPDATE SET quantity = T.quantity + S.quantity
            WHEN MATCHED THEN
              DELETE",
        ).await
        .unwrap();

    let result = session
        .execute_sql("SELECT product, quantity FROM inventory ORDER BY product")
        .await
        .unwrap();

    assert_table_eq!(
        result,
        [
            ["dryer", 70],
            ["microwave", 20],
            ["oven", 35],
            ["refrigerator", 50]
        ]
    );
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_without_into_keyword() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 10)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (1, 20), (2, 30)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE target T
            USING source S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET value = S.value
            WHEN NOT MATCHED THEN INSERT (id, value) VALUES (S.id, S.value)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, 20], [2, 30]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_merge_update_by_source_condition() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE target (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE source (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO target VALUES (1, 10), (2, 20), (3, 30)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO source VALUES (2, 25)")
        .await
        .unwrap();

    session
        .execute_sql(
            "MERGE INTO target T
            USING source S
            ON T.id = S.id
            WHEN MATCHED THEN UPDATE SET value = S.value
            WHEN NOT MATCHED BY SOURCE AND T.value > 20 THEN UPDATE SET value = 0",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, value FROM target ORDER BY id")
        .await
        .unwrap();

    assert_table_eq!(result, [[1, 10], [2, 25], [3, 0]]);
}
