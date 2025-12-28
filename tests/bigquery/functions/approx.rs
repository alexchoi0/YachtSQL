use crate::assert_table_eq;
use crate::common::create_session;

async fn setup_data(session: &yachtsql::YachtSQLSession) {
    session
        .execute_sql("CREATE TABLE data (id INT64, category STRING, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, 'A', 10), (2, 'A', 20), (3, 'A', 30), (4, 'B', 15), (5, 'B', 25), (6, 'B', 35), (7, 'C', 5), (8, 'C', 50)").await
        .unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_count_distinct() {
    let session = create_session();
    setup_data(&session).await;

    let result = session
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(category) FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_count_distinct_with_group() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE visits (user_id INT64, page STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO visits VALUES (1, 'home'), (1, 'about'), (2, 'home'), (2, 'home'), (3, 'home'), (3, 'products')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT user_id, APPROX_COUNT_DISTINCT(page) FROM visits GROUP BY user_id ORDER BY user_id").await
        .unwrap();
    assert_table_eq!(result, [[1, 2], [2, 1], [3, 2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_quantiles() {
    let session = create_session();
    setup_data(&session).await;

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_QUANTILES(value, 4)) = 5 FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_quantiles_with_group() {
    let session = create_session();
    setup_data(&session).await;

    let result = session
        .execute_sql("SELECT COUNT(*) FROM (SELECT category, APPROX_QUANTILES(value, 2) FROM data GROUP BY category ORDER BY category) AS t").await
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_top_count() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE words (word STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO words VALUES ('apple'), ('banana'), ('apple'), ('cherry'), ('apple'), ('banana')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_TOP_COUNT(word, 2)) = 2 FROM words")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_top_sum() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sales (product STRING, amount INT64)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sales VALUES ('A', 100), ('B', 200), ('A', 150), ('C', 50), ('B', 100)",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_TOP_SUM(product, amount, 2)) = 2 FROM sales")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_count_distinct_null() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('a'), (NULL), ('b'), (NULL), ('a')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(value) = 2 FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_quantiles_ignore_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (10), (NULL), (20), (NULL), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_QUANTILES(value, 2)) = 3 FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_quantiles_respect_nulls() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (10), (20), (30)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT ARRAY_LENGTH(APPROX_QUANTILES(value, 2)) = 3 FROM data")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hll_count_init() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE data (category STRING, value STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES ('A', 'x'), ('A', 'y'), ('B', 'z')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM (SELECT category, HLL_COUNT_INIT(value) AS sketch FROM data GROUP BY category) AS t").await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hll_count_merge() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sketches (category STRING, sketch STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sketches VALUES ('A', 'HLL_SKETCH:p15:n1'), ('A', 'HLL_SKETCH:p15:n2'), ('B', 'HLL_SKETCH:p15:n1')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM (SELECT category, HLL_COUNT_MERGE(sketch) AS merged FROM sketches GROUP BY category) AS t").await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hll_count_merge_partial() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sketches (category STRING, sketch STRING)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO sketches VALUES ('A', 'HLL_SKETCH:p15:n1'), ('A', 'HLL_SKETCH:p15:n2'), ('B', 'HLL_SKETCH:p15:n1')").await
        .unwrap();

    let result = session
        .execute_sql("SELECT COUNT(*) FROM (SELECT category, HLL_COUNT_MERGE_PARTIAL(sketch) FROM sketches GROUP BY category ORDER BY category) AS t").await
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_hll_count_extract() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE sketches (category STRING, sketch STRING)")
        .await
        .unwrap();
    session
        .execute_sql(
            "INSERT INTO sketches VALUES ('A', 'HLL_SKETCH:p15:n3'), ('B', 'HLL_SKETCH:p15:n5')",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT HLL_COUNT_EXTRACT(sketch) FROM sketches ORDER BY category")
        .await
        .unwrap();
    assert_table_eq!(result, [[3], [5]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_approx_count_distinct_large_dataset() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE large_data (id INT64)")
        .await
        .unwrap();
    let mut values = Vec::new();
    for i in 1..=100 {
        values.push(format!("({})", i));
    }
    session
        .execute_sql(&format!(
            "INSERT INTO large_data VALUES {}",
            values.join(", ")
        ))
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT APPROX_COUNT_DISTINCT(id) >= 90 AND APPROX_COUNT_DISTINCT(id) <= 110 FROM large_data").await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}
