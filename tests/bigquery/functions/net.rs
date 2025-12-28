use crate::assert_table_eq;
use crate::common::{create_session, ip};

#[tokio::test(flavor = "current_thread")]
async fn test_net_ipv4_from_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IPV4_FROM_INT64(167772161)")
        .await
        .unwrap();
    assert_table_eq!(result, [[ip("10.0.0.1")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ipv4_to_int64() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IPV4_TO_INT64(NET.IP_FROM_STRING('192.168.1.1'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[3232235777i64]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_from_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_FROM_STRING('192.168.1.1')")
        .await
        .unwrap();
    assert_table_eq!(result, [[ip("192.168.1.1")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_safe_ip_from_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.SAFE_IP_FROM_STRING('192.168.1.1')")
        .await
        .unwrap();
    assert_table_eq!(result, [[ip("192.168.1.1")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_safe_ip_from_string_invalid() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.SAFE_IP_FROM_STRING('invalid')")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('192.168.1.1'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["192.168.1.1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_net_mask() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_NET_MASK(4, 24)")
        .await
        .unwrap();
    assert_table_eq!(result, [[ip("255.255.255.0")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_trunc() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_TRUNC(NET.IP_FROM_STRING('192.168.1.100'), 24)")
        .await
        .unwrap();
    assert_table_eq!(result, [[ip("192.168.1.0")]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ipv4_from_int64_null() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IPV4_FROM_INT64(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_host() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.HOST('https://www.example.com:8080/path')")
        .await
        .unwrap();
    assert_table_eq!(result, [["www.example.com"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_public_suffix() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.PUBLIC_SUFFIX('www.example.com')")
        .await
        .unwrap();
    assert_table_eq!(result, [["com"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_reg_domain() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.REG_DOMAIN('www.example.com')")
        .await
        .unwrap();
    assert_table_eq!(result, [["example.com"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_in_net() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_IN_NET(NET.IP_FROM_STRING('192.168.1.100'), '192.168.1.0/24')")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_in_net_false() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_IN_NET(NET.IP_FROM_STRING('10.0.0.1'), '192.168.1.0/24')")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_make_net() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.MAKE_NET(NET.IP_FROM_STRING('192.168.1.0'), 24)")
        .await
        .unwrap();
    assert_table_eq!(result, [["192.168.1.0/24"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_is_private() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_IS_PRIVATE(NET.IP_FROM_STRING('192.168.1.1'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ip_is_private_public() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_IS_PRIVATE(NET.IP_FROM_STRING('8.8.8.8'))")
        .await
        .unwrap();
    assert_table_eq!(result, [[false]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ipv6_from_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('2001:db8::1'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["2001:db8::1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_ipv6_to_string() {
    let session = create_session();
    let result = session
        .execute_sql("SELECT NET.IP_TO_STRING(NET.IP_FROM_STRING('::ffff:192.168.1.1'))")
        .await
        .unwrap();
    assert_table_eq!(result, [["::ffff:192.168.1.1"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_net_in_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE servers (id INT64, ip_address BYTES)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO servers VALUES (1, NET.IP_FROM_STRING('192.168.1.1')), (2, NET.IP_FROM_STRING('10.0.0.1'))").await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM servers WHERE NET.IP_IN_NET(ip_address, '192.168.0.0/16')")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}
