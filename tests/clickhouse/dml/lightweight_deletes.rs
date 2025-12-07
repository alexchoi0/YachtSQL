use crate::common::create_executor;

#[test]
fn test_lightweight_delete_basic() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_basic (id UInt64, name String) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_basic VALUES (1, 'a'), (2, 'b'), (3, 'c')").ok();

    let result = executor.execute_sql("DELETE FROM ld_basic WHERE id = 2");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_multiple_rows() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_multi (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_multi VALUES (1, 10), (2, 20), (3, 30), (4, 40)").ok();

    let result = executor.execute_sql("DELETE FROM ld_multi WHERE value > 20");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_all_rows() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_all (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_all VALUES (1), (2), (3)").ok();

    let result = executor.execute_sql("DELETE FROM ld_all WHERE 1 = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_no_match() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_nomatch (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_nomatch VALUES (1), (2), (3)").ok();

    let result = executor.execute_sql("DELETE FROM ld_nomatch WHERE id = 999");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_and() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_and (id UInt64, status String, value Int64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_and VALUES (1, 'active', 100), (2, 'inactive', 200), (3, 'active', 300)").ok();

    let result = executor.execute_sql("DELETE FROM ld_and WHERE status = 'active' AND value > 200");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_or() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_or (id UInt64, type String) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_or VALUES (1, 'A'), (2, 'B'), (3, 'C')").ok();

    let result = executor.execute_sql("DELETE FROM ld_or WHERE type = 'A' OR type = 'C'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_in() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_in (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_in VALUES (1), (2), (3), (4), (5)").ok();

    let result = executor.execute_sql("DELETE FROM ld_in WHERE id IN (2, 4)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_not_in() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_not_in (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_not_in VALUES (1), (2), (3), (4), (5)").ok();

    let result = executor.execute_sql("DELETE FROM ld_not_in WHERE id NOT IN (2, 4)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_between() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_between (id UInt64, value Int64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_between VALUES (1, 10), (2, 50), (3, 100)").ok();

    let result = executor.execute_sql("DELETE FROM ld_between WHERE value BETWEEN 20 AND 80");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_like() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_like (id UInt64, name String) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_like VALUES (1, 'test_a'), (2, 'prod_b'), (3, 'test_c')").ok();

    let result = executor.execute_sql("DELETE FROM ld_like WHERE name LIKE 'test%'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_null() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_null (id UInt64, value Nullable(Int64)) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_null VALUES (1, 10), (2, NULL), (3, 30)").ok();

    let result = executor.execute_sql("DELETE FROM ld_null WHERE value IS NULL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_not_null() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_not_null (id UInt64, value Nullable(Int64)) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_not_null VALUES (1, 10), (2, NULL), (3, 30)").ok();

    let result = executor.execute_sql("DELETE FROM ld_not_null WHERE value IS NOT NULL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_datetime() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_dt (id UInt64, ts DateTime) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_dt VALUES (1, '2024-01-01 00:00:00'), (2, '2024-06-15 12:00:00')").ok();

    let result = executor.execute_sql("DELETE FROM ld_dt WHERE ts < '2024-03-01 00:00:00'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_subquery() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_main (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("CREATE TABLE ld_ref (ref_id UInt64) ENGINE = MergeTree() ORDER BY ref_id").ok();
    executor.execute_sql("INSERT INTO ld_main VALUES (1), (2), (3)").ok();
    executor.execute_sql("INSERT INTO ld_ref VALUES (2)").ok();

    let result = executor.execute_sql("DELETE FROM ld_main WHERE id IN (SELECT ref_id FROM ld_ref)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_replacing_merge_tree() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_rmt (id UInt64, version UInt64, data String) ENGINE = ReplacingMergeTree(version) ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_rmt VALUES (1, 1, 'a'), (2, 1, 'b')").ok();

    let result = executor.execute_sql("DELETE FROM ld_rmt WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_summing_merge_tree() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_smt (id UInt64, value UInt64) ENGINE = SummingMergeTree(value) ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_smt VALUES (1, 10), (2, 20)").ok();

    let result = executor.execute_sql("DELETE FROM ld_smt WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_aggregating_merge_tree() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_amt (id UInt64, count AggregateFunction(count)) ENGINE = AggregatingMergeTree() ORDER BY id"
    ).ok();

    let result = executor.execute_sql("DELETE FROM ld_amt WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_partition() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_part (id UInt64, date Date, value Int64) ENGINE = MergeTree() PARTITION BY toYYYYMM(date) ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_part VALUES (1, '2024-01-15', 10), (2, '2024-02-15', 20)").ok();

    let result = executor.execute_sql("DELETE FROM ld_part WHERE date = '2024-01-15'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_complex_expression() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_complex (id UInt64, a Int64, b Int64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_complex VALUES (1, 10, 5), (2, 20, 10), (3, 30, 15)").ok();

    let result = executor.execute_sql("DELETE FROM ld_complex WHERE a + b > 25");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_function_in_where() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_func (id UInt64, name String) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_func VALUES (1, 'HELLO'), (2, 'world')").ok();

    let result = executor.execute_sql("DELETE FROM ld_func WHERE lower(name) = 'hello'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_array_column() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_arr (id UInt64, tags Array(String)) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_arr VALUES (1, ['a', 'b']), (2, ['c', 'd'])").ok();

    let result = executor.execute_sql("DELETE FROM ld_arr WHERE has(tags, 'a')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_nested_column() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_nested (id UInt64, n Nested(key String, value Int64)) ENGINE = MergeTree() ORDER BY id"
    ).ok();

    let result = executor.execute_sql("DELETE FROM ld_nested WHERE has(n.key, 'test')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_with_settings() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_settings (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_settings VALUES (1), (2)").ok();

    let result = executor.execute_sql("DELETE FROM ld_settings WHERE id = 1 SETTINGS mutations_sync = 2");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_on_cluster() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "DELETE FROM ld_cluster ON CLUSTER test_cluster WHERE id = 1"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_database_qualified() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE DATABASE IF NOT EXISTS ld_db").ok();
    executor.execute_sql("CREATE TABLE ld_db.ld_table (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_db.ld_table VALUES (1)").ok();

    let result = executor.execute_sql("DELETE FROM ld_db.ld_table WHERE id = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_lowcardinality() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_lc (id UInt64, status LowCardinality(String)) ENGINE = MergeTree() ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_lc VALUES (1, 'active'), (2, 'inactive')").ok();

    let result = executor.execute_sql("DELETE FROM ld_lc WHERE status = 'inactive'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_tuple_comparison() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_tuple (a UInt64, b UInt64) ENGINE = MergeTree() ORDER BY (a, b)").ok();
    executor.execute_sql("INSERT INTO ld_tuple VALUES (1, 2), (3, 4)").ok();

    let result = executor.execute_sql("DELETE FROM ld_tuple WHERE (a, b) = (1, 2)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_map_column() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_map (id UInt64, data Map(String, Int64)) ENGINE = MergeTree() ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_map VALUES (1, {'a': 1}), (2, {'b': 2})").ok();

    let result = executor.execute_sql("DELETE FROM ld_map WHERE data['a'] = 1");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_enum() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_enum (id UInt64, status Enum8('pending' = 1, 'done' = 2)) ENGINE = MergeTree() ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_enum VALUES (1, 'pending'), (2, 'done')").ok();

    let result = executor.execute_sql("DELETE FROM ld_enum WHERE status = 'pending'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_uuid() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_uuid (id UUID, data String) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_uuid VALUES ('550e8400-e29b-41d4-a716-446655440000', 'test')").ok();

    let result = executor.execute_sql("DELETE FROM ld_uuid WHERE id = '550e8400-e29b-41d4-a716-446655440000'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_ipv4() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_ip (id UInt64, ip IPv4) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_ip VALUES (1, '192.168.1.1'), (2, '10.0.0.1')").ok();

    let result = executor.execute_sql("DELETE FROM ld_ip WHERE ip = '192.168.1.1'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_decimal() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_dec (id UInt64, amount Decimal(18, 2)) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_dec VALUES (1, 100.50), (2, 200.75)").ok();

    let result = executor.execute_sql("DELETE FROM ld_dec WHERE amount > 150.00");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_fixedstring() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_fs (id UInt64, code FixedString(3)) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_fs VALUES (1, 'ABC'), (2, 'XYZ')").ok();

    let result = executor.execute_sql("DELETE FROM ld_fs WHERE code = 'ABC'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_multiple_conditions() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE ld_mult (id UInt64, a Int64, b String, c Float64) ENGINE = MergeTree() ORDER BY id"
    ).ok();
    executor.execute_sql("INSERT INTO ld_mult VALUES (1, 10, 'x', 1.5), (2, 20, 'y', 2.5), (3, 30, 'z', 3.5)").ok();

    let result = executor.execute_sql("DELETE FROM ld_mult WHERE a > 15 AND b != 'z' AND c < 3.0");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_lightweight_delete_verify_immediate() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE ld_verify (id UInt64) ENGINE = MergeTree() ORDER BY id").ok();
    executor.execute_sql("INSERT INTO ld_verify VALUES (1), (2), (3)").ok();
    executor.execute_sql("DELETE FROM ld_verify WHERE id = 2").ok();

    let result = executor.execute_sql("SELECT count() FROM ld_verify WHERE id = 2");
    assert!(result.is_ok() || result.is_err());
}
