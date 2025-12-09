use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_ipv4_num_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv4NumToString(3232235777)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.1"]]);
}

#[test]
fn test_ipv4_string_to_num() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv4StringToNum('192.168.1.1')")
        .unwrap();
    assert_table_eq!(result, [[3232235777i64]]);
}

#[test]
fn test_ipv4_num_to_string_class_c() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv4NumToStringClassC(3232235777)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.xxx"]]);
}

#[test]
fn test_ipv4_to_ipv6() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv4ToIPv6(IPv4StringToNum('192.168.1.1'))")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[ignore = "Requires toFixedString function"]
#[test]
fn test_ipv6_num_to_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT IPv6NumToString(toFixedString('\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\x00\\xFF\\xFF\\xC0\\xA8\\x01\\x01', 16))").unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_ipv6_string_to_num() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv6StringToNum('::ffff:192.168.1.1')")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_to_ipv4() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toIPv4('192.168.1.1')")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.1"]]);
}

#[test]
fn test_to_ipv6() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT toIPv6('::1')").unwrap();
    assert_table_eq!(result, [["::1"]]);
}

#[test]
fn test_to_ipv4_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toIPv4OrNull('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_to_ipv6_or_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT toIPv6OrNull('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_is_ipv4_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT isIPv4String('192.168.1.1')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_ipv6_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT isIPv6String('::1')").unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_is_ip_address_in_range() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT isIPAddressInRange('192.168.1.1', '192.168.1.0/24')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_ipv4_cidr_to_range() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv4CIDRToRange(toIPv4('192.168.1.0'), 24)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_ipv6_cidr_to_range() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT IPv6CIDRToRange(toIPv6('2001:db8::'), 32)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_mac_num_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MACNumToString(22222222222222)")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_mac_string_to_num() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MACStringToNum('AA:BB:CC:DD:EE:FF')")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_mac_string_to_oui() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT MACStringToOUI('AA:BB:CC:DD:EE:FF')")
        .unwrap();
    assert!(result.num_rows() == 1);
}

#[test]
fn test_ip_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ip_test (ip STRING, name STRING)")
        .unwrap();
    executor
        .execute_sql(
            "INSERT INTO ip_test VALUES ('192.168.1.1', 'server1'), ('10.0.0.1', 'server2')",
        )
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM ip_test WHERE isIPAddressInRange(ip, '192.168.0.0/16')")
        .unwrap();
    assert_table_eq!(result, [["server1"]]);
}
