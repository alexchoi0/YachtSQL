use crate::assert_table_eq;
use crate::common::{create_executor, ip};

#[test]
#[ignore = "Implement me!"]
fn test_net_ipv4_from_int64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IPV4_FROM_INT64(167772161)")
        .unwrap();
    assert_table_eq!(result, [[ip("10.0.0.1")]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ipv4_to_int64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IPV4_TO_INT64(NET.IP_FROM_STRING('192.168.1.1'))")
        .unwrap();
    assert_table_eq!(result, [[3232235777i64]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_from_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_FROM_STRING('192.168.1.1')")
        .unwrap();
    assert_table_eq!(result, [[ip("192.168.1.1")]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_safe_ip_from_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.SAFE_IP_FROM_STRING('192.168.1.1')")
        .unwrap();
    assert_table_eq!(result, [[ip("192.168.1.1")]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_safe_ip_from_string_invalid() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.SAFE_IP_FROM_STRING('invalid')")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('192.168.1.1'))")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_net_mask() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_NET_MASK(4, 24)")
        .unwrap();
    assert_table_eq!(result, [[ip("255.255.255.0")]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_trunc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 24)")
        .unwrap();
    assert_table_eq!(result, [[ip("192.168.1.0")]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ipv4_from_int64_null() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IPV4_FROM_INT64(NULL)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_host() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.HOST('https://www.example.com:8080/path')")
        .unwrap();
    assert_table_eq!(result, [["www.example.com"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_public_suffix() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.PUBLIC_SUFFIX('www.example.com')")
        .unwrap();
    assert_table_eq!(result, [["com"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_reg_domain() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.REG_DOMAIN('www.example.com')")
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_in_net() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_IN_NET(NET.IP_FROM_STRING('192.168.1.100'), '192.168.1.0/24')")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_in_net_false() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_IN_NET(NET.IP_FROM_STRING('10.0.0.1'), '192.168.1.0/24')")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_make_net() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.MAKE_NET(NET.IP_FROM_STRING('192.168.1.0'), 24)")
        .unwrap();
    assert_table_eq!(result, [["192.168.1.0/24"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_is_private() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_IS_PRIVATE(NET.IP_FROM_STRING('192.168.1.1'))")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ip_is_private_public() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_IS_PRIVATE(NET.IP_FROM_STRING('8.8.8.8'))")
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ipv6_from_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('2001:db8::1'))")
        .unwrap();
    assert_table_eq!(result, [["2001:db8::1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_ipv6_to_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('::ffff:192.168.1.1'))")
        .unwrap();
    assert_table_eq!(result, [["::ffff:192.168.1.1"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_net_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE servers (id INT64, ip_address BYTES)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO servers VALUES (1, NET.IP_FROM_STRING('192.168.1.1')), (2, NET.IP_FROM_STRING('10.0.0.1'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM servers WHERE NET.IP_IN_NET(ip_address, '192.168.0.0/16')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}
