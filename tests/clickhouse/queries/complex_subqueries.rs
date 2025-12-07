use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_scalar_subquery_in_select() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scalar_main (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scalar_main VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, value, (SELECT AVG(value) FROM scalar_main) AS avg_val FROM scalar_main ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[test]
fn test_scalar_subquery_in_where() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE scalar_where (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scalar_where VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM scalar_where WHERE value > (SELECT AVG(value) FROM scalar_where)",
        )
        .unwrap();
    assert_table_eq!(result, [[3]]);
}

#[test]
fn test_correlated_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE corr_outer (id INT64, category String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE corr_inner (id INT64, outer_id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_outer VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO corr_inner VALUES (1, 1, 100), (2, 1, 200), (3, 2, 150)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT o.id, o.category, (SELECT SUM(value) FROM corr_inner i WHERE i.outer_id = o.id) AS total FROM corr_outer o ORDER BY o.id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_exists_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE exists_main (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE exists_ref (main_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO exists_main VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO exists_ref VALUES (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM exists_main m WHERE EXISTS (SELECT 1 FROM exists_ref r WHERE r.main_id = m.id) ORDER BY name")
        .unwrap();
    assert_table_eq!(result, [["alice"], ["charlie"]]);
}

#[test]
fn test_not_exists_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE not_exists_main (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE not_exists_ref (main_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_exists_main VALUES (1, 'alice'), (2, 'bob'), (3, 'charlie')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_exists_ref VALUES (1), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM not_exists_main m WHERE NOT EXISTS (SELECT 1 FROM not_exists_ref r WHERE r.main_id = m.id)")
        .unwrap();
    assert_table_eq!(result, [["bob"]]);
}

#[test]
fn test_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE in_main (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE in_filter (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO in_main VALUES (1, 100), (2, 200), (3, 300), (4, 400)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO in_filter VALUES (2), (4)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT value FROM in_main WHERE id IN (SELECT id FROM in_filter) ORDER BY value",
        )
        .unwrap();
    assert_table_eq!(result, [[200], [400]]);
}

#[test]
fn test_not_in_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE not_in_main (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE not_in_filter (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_in_main VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO not_in_filter VALUES (2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT value FROM not_in_main WHERE id NOT IN (SELECT id FROM not_in_filter) ORDER BY value")
        .unwrap();
    assert_table_eq!(result, [[100], [300]]);
}

#[test]
fn test_any_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE any_main (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE any_compare (threshold INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO any_main VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO any_compare VALUES (150), (250)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM any_main WHERE value > ANY (SELECT threshold FROM any_compare) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[test]
fn test_all_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE all_main (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE all_compare (threshold INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO all_main VALUES (1, 100), (2, 200), (3, 300)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO all_compare VALUES (50), (100)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM all_main WHERE value > ALL (SELECT threshold FROM all_compare) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[2], [3]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_derived_table_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE derived_src (category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO derived_src VALUES ('A', 10), ('A', 20), ('B', 30), ('B', 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT category, total FROM (SELECT category, SUM(value) AS total FROM derived_src GROUP BY category) ORDER BY category")
        .unwrap();
    assert_table_eq!(result, [["A", 30], ["B", 70]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_nested_subqueries() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nested_data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nested_data VALUES (1, 10), (2, 20), (3, 30), (4, 40), (5, 50)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id FROM nested_data
            WHERE value > (
                SELECT AVG(value) FROM nested_data
                WHERE id IN (SELECT id FROM nested_data WHERE value < 40)
            ) ORDER BY id",
        )
        .unwrap();
    assert_table_eq!(result, [[3], [4], [5]]);
}

#[test]
fn test_subquery_in_from_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sub_from_a (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE sub_from_b (id INT64, name String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sub_from_a VALUES (1, 100), (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sub_from_b VALUES (1, 'alice'), (2, 'bob')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT s.id, s.doubled, b.name
            FROM (SELECT id, value * 2 AS doubled FROM sub_from_a) s
            JOIN sub_from_b b ON s.id = b.id
            ORDER BY s.id",
        )
        .unwrap();
    assert_table_eq!(result, [[1, 200, "alice"], [2, 400, "bob"]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_lateral_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lateral_main (id INT64, category String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE lateral_detail (main_id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lateral_main VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lateral_detail VALUES (1, 100), (1, 200), (2, 300)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT m.id, m.category, d.top_value
            FROM lateral_main m,
            LATERAL (SELECT MAX(value) AS top_value FROM lateral_detail WHERE main_id = m.id) d
            ORDER BY m.id",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_subquery_with_window_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sub_window (id INT64, category String, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sub_window VALUES (1, 'A', 100), (2, 'A', 200), (3, 'B', 150)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id, category, value, rn FROM (
                SELECT id, category, value, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value DESC) AS rn
                FROM sub_window
            ) WHERE rn = 1 ORDER BY category"
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_subquery_union() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE union_a (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE union_b (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO union_a VALUES (1, 100), (2, 200)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO union_b VALUES (3, 300), (4, 400)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT * FROM (
                SELECT id, value FROM union_a
                UNION ALL
                SELECT id, value FROM union_b
            ) ORDER BY id",
        )
        .unwrap();
    assert!(result.num_rows() == 4); // TODO: use table![[expected_values]]
}

#[test]
fn test_multiple_correlated_subqueries() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE multi_corr_main (id INT64, category String)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE multi_corr_values (main_id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE multi_corr_counts (main_id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_corr_main VALUES (1, 'A'), (2, 'B')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_corr_values VALUES (1, 100), (1, 200), (2, 150)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO multi_corr_counts VALUES (1), (1), (1), (2), (2)")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT m.id, m.category,
                (SELECT SUM(value) FROM multi_corr_values v WHERE v.main_id = m.id) AS total_value,
                (SELECT COUNT(*) FROM multi_corr_counts c WHERE c.main_id = m.id) AS count_val
            FROM multi_corr_main m ORDER BY m.id",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[test]
fn test_subquery_with_cte() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cte_sub_data (id INT64, parent_id Nullable(Int64), value INT64)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO cte_sub_data VALUES (1, NULL, 100), (2, 1, 50), (3, 1, 75), (4, 2, 25)",
        )
        .unwrap();

    let result = executor
        .execute_sql(
            "WITH totals AS (
                SELECT parent_id, SUM(value) AS child_sum
                FROM cte_sub_data
                WHERE parent_id IS NOT NULL
                GROUP BY parent_id
            )
            SELECT d.id, d.value, COALESCE(t.child_sum, 0) AS children_total
            FROM cte_sub_data d
            LEFT JOIN totals t ON d.id = t.parent_id
            WHERE d.parent_id IS NULL
            ORDER BY d.id",
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_subquery_comparison_operators() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sub_cmp (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sub_cmp VALUES (1, 10), (2, 20), (3, 30), (4, 40)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM sub_cmp WHERE value = (SELECT MAX(value) FROM sub_cmp)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_subquery_with_limit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sub_limit (id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO sub_limit VALUES (1, 90), (2, 85), (3, 95), (4, 80)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM sub_limit WHERE id IN (SELECT id FROM sub_limit ORDER BY score DESC LIMIT 2) ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [3]]);
}
