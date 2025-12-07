use crate::common::create_executor;

#[ignore = "Implement me!"]
#[test]
fn test_md5() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MD5('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sha1() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA1('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sha224() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA224('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sha256() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA256('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sha384() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA384('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_sha512() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA512('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_blake3() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT BLAKE3('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_hex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT hex('hello')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_unhex() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT unhex('68656C6C6F')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_encrypt_aes_128_ecb() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT encrypt('aes-128-ecb', 'hello', '1234567890123456')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_decrypt_aes_128_ecb() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT decrypt('aes-128-ecb', encrypt('aes-128-ecb', 'hello', '1234567890123456'), '1234567890123456')"
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_encrypt_aes_256_cbc() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT encrypt('aes-256-cbc', 'secret message', '12345678901234567890123456789012', '1234567890123456')"
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aes_encrypt_mysql() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT aes_encrypt_mysql('aes-128-ecb', 'hello', '1234567890123456')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_aes_decrypt_mysql() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql(
            "SELECT aes_decrypt_mysql('aes-128-ecb', aes_encrypt_mysql('aes-128-ecb', 'hello', '1234567890123456'), '1234567890123456')"
        )
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_base64_encode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base64Encode('hello')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_base64_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base64Decode('aGVsbG8=')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_try_base64_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT tryBase64Decode('aGVsbG8=')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_base64_url_encode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base64URLEncode('hello+world')")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_base64_url_decode() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT base64URLDecode(base64URLEncode('hello+world'))")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_md5_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE hash_test (data String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO hash_test VALUES ('hello'), ('world'), ('test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT data, hex(MD5(data)) AS hash FROM hash_test ORDER BY data")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}

#[ignore = "Implement me!"]
#[test]
fn test_encrypt_decrypt_roundtrip() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE secrets (id UInt32, message String)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO secrets VALUES (1, 'secret1'), (2, 'secret2')")
        .unwrap();

    let result = executor
        .execute_sql(
            "SELECT id,
                decrypt('aes-128-ecb',
                    encrypt('aes-128-ecb', message, '1234567890123456'),
                    '1234567890123456'
                ) AS decrypted
            FROM secrets
            ORDER BY id",
        )
        .unwrap();
    assert!(result.num_rows() == 2); // TODO: use table![[expected_values]]
}
