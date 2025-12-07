use crate::assert_table_eq;
use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_test (id INT64, ip IPv4)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv4_test VALUES (1, '192.168.1.1'), (2, '10.0.0.1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, ip FROM ipv4_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv6_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv6_test (id INT64, ip IPv6)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv6_test VALUES (1, '::1'), (2, '2001:db8::1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, ip FROM ipv6_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_cmp (id INT64, ip IPv4)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ipv4_cmp VALUES (1, '192.168.1.1'), (2, '192.168.1.2'), (3, '10.0.0.1')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ipv4_cmp WHERE ip = toIPv4('192.168.1.1')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_ordering() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_ord (id INT64, ip IPv4)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ipv4_ord VALUES (1, '192.168.1.1'), (2, '10.0.0.1'), (3, '172.16.0.1')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ipv4_ord ORDER BY ip")
        .unwrap();
    assert_table_eq!(result, [[2], [3], [1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_range_query() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_range (id INT64, ip IPv4)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv4_range VALUES (1, '192.168.1.1'), (2, '192.168.1.100'), (3, '192.168.2.1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ipv4_range WHERE ip >= toIPv4('192.168.1.1') AND ip < toIPv4('192.168.2.0') ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_cidr_match() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_cidr (id INT64, ip IPv4)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ipv4_cidr VALUES (1, '192.168.1.1'), (2, '192.168.2.1'), (3, '10.0.0.1')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ipv4_cidr WHERE isIPAddressInRange(toString(ip), '192.168.1.0/24') ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv6_comparison() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv6_cmp (id INT64, ip IPv6)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv6_cmp VALUES (1, '::1'), (2, '::2'), (3, '2001:db8::1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ipv6_cmp WHERE ip = toIPv6('::1')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_to_ipv6_conversion() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ip_convert (id INT64, ip4 IPv4)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ip_convert VALUES (1, '192.168.1.1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT IPv4ToIPv6(ip4) FROM ip_convert")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_null (id INT64, ip Nullable(IPv4))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv4_null VALUES (1, '192.168.1.1'), (2, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM ipv4_null WHERE ip IS NULL")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_group_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_group (ip IPv4, count INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv4_group VALUES ('192.168.1.1', 10), ('192.168.1.1', 20), ('10.0.0.1', 5)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT ip, SUM(count) AS total FROM ipv4_group GROUP BY ip ORDER BY total")
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv4_distinct() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv4_distinct (ip IPv4)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ipv4_distinct VALUES ('192.168.1.1'), ('192.168.1.1'), ('10.0.0.1')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(DISTINCT ip) FROM ipv4_distinct")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[ignore = "Implement me!"]
#[test]
fn test_ipv6_mapped_ipv4() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ipv6_mapped (id INT64, ip IPv6)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ipv6_mapped VALUES (1, '::ffff:192.168.1.1')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, ip FROM ipv6_mapped")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}
