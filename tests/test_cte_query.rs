#[test]
fn test_manual_cte_query() {
    use yachtsql::{DialectType, QueryExecutor};

    let mut executor = QueryExecutor::with_dialect(DialectType::PostgreSQL);

    // Create the same table the CTE would create
    executor
        .execute_sql("CREATE TABLE __cte_nums (a INT64, b INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO __cte_nums VALUES (1, 2)")
        .unwrap();

    // Execute the same query
    let result = executor
        .execute_sql("SELECT a + 1, b + 1 FROM __cte_nums WHERE a < 5")
        .unwrap();

    println!("Rows: {}", result.num_rows());
    for i in 0..result.num_rows() {
        let col_a = result.column(0).unwrap().get(i).unwrap();
        let col_b = result.column(1).unwrap().get(i).unwrap();
        println!("Row {}: [{:?}, {:?}]", i, col_a, col_b);
    }

    // Should be [2, 3] not [1, 2]
    let val_a = result.column(0).unwrap().get(0).unwrap().as_i64().unwrap();
    let val_b = result.column(1).unwrap().get(0).unwrap().as_i64().unwrap();
    assert_eq!(val_a, 2, "Expected a+1=2");
    assert_eq!(val_b, 3, "Expected b+1=3");
}
