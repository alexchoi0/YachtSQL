use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_inet_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '192.168.1.1'::INET").unwrap();
    assert_table_eq!(result, [["192.168.1.1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cidr_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.0/24'::CIDR")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.0/24"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_macaddr_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '08:00:2b:01:02:03'::MACADDR")
        .unwrap();
    assert_table_eq!(result, [["08:00:2b:01:02:03"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_macaddr8_literal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '08:00:2b:01:02:03:04:05'::MACADDR8")
        .unwrap();
    assert_table_eq!(result, [["08:00:2b:01:02:03:04:05"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE servers (id INT64, ip INET)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO servers VALUES (1, '192.168.1.1')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM servers").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_inet_comparison() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.1'::INET = '192.168.1.1'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_inet_less_than() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.1'::INET < '192.168.1.2'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_contained_by() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.5'::INET << '192.168.1.0/24'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_contains() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.0/24'::INET >> '192.168.1.5'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_contained_or_equal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.0/24'::INET <<= '192.168.1.0/24'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_contains_or_equal() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.0/24'::INET >>= '192.168.1.0/24'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_overlaps() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.0/24'::INET && '192.168.1.128/25'::INET")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_bitwise_not() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ~ '192.168.1.1'::INET")
        .unwrap();
    assert_table_eq!(result, [["63.87.254.254"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_bitwise_and() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.6'::INET & '0.0.0.255'::INET")
        .unwrap();
    assert_table_eq!(result, [["0.0.0.6"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_bitwise_or() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.0'::INET | '0.0.0.5'::INET")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.5"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_add() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.1'::INET + 5")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.6"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_subtract() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.10'::INET - 5")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.5"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_subtract_inet() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '192.168.1.10'::INET - '192.168.1.5'::INET")
        .unwrap();
    assert_table_eq!(result, [[5]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_abbrev_inet() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ABBREV('192.168.1.1/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.1/24"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_abbrev_cidr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT ABBREV('192.168.1.0/24'::CIDR)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.0/24"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_broadcast() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT BROADCAST('192.168.1.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.255/24"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_family() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT FAMILY('192.168.1.1'::INET)")
        .unwrap();
    assert_table_eq!(result, [[4]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_family_ipv6() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT FAMILY('::1'::INET)").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_host() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT HOST('192.168.1.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.5"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_hostmask() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT HOSTMASK('192.168.1.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["0.0.0.255"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_masklen() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MASKLEN('192.168.1.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [[24]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_netmask() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NETMASK('192.168.1.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["255.255.255.0"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_network() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NETWORK('192.168.1.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.0/24"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_masklen_inet() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SET_MASKLEN('192.168.1.5'::INET, 16)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.5/16"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_set_masklen_cidr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT SET_MASKLEN('192.168.1.0/24'::CIDR, 16)")
        .unwrap();
    assert_table_eq!(result, [["192.168.0.0/16"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_text_inet() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TEXT('192.168.1.1'::INET)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_same_family() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INET_SAME_FAMILY('192.168.1.1'::INET, '192.168.2.1'::INET)")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_merge() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT INET_MERGE('192.168.1.5/24'::INET, '192.168.2.5/24'::INET)")
        .unwrap();
    assert_table_eq!(result, [["192.168.0.0/22"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_trunc_macaddr() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT TRUNC('08:00:2b:01:02:03'::MACADDR)")
        .unwrap();
    assert_table_eq!(result, [["08:00:2b:00:00:00"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_macaddr8_set7bit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MACADDR8_SET7BIT('00:00:2b:01:02:03:04:05'::MACADDR8)")
        .unwrap();
    assert_table_eq!(result, [["02:00:2b:01:02:03:04:05"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inet_ipv6() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '2001:db8::1'::INET").unwrap();
    assert_table_eq!(result, [["2001:db8::1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_cidr_ipv6() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT '2001:db8::/32'::CIDR")
        .unwrap();
    assert_table_eq!(result, [["2001:db8::/32"]]);
}
