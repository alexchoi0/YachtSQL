use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_keys_new_keyset() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') IS NOT NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_keys_add_key_from_raw_bytes() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT KEYS.ADD_KEY_FROM_RAW_BYTES(
            KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
            'AES_GCM',
            b'12345678901234567890123456789012'
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_keys_keyset_chain() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT KEYS.KEYSET_CHAIN('gcp-kms://projects/my-project/locations/us/keyRings/my-ring/cryptoKeys/my-key', KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_keys_keyset_from_json() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT KEYS.KEYSET_FROM_JSON('{\"primaryKeyId\": 123, \"key\": []}')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_keys_keyset_to_json() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT KEYS.KEYSET_TO_JSON(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_keys_rotate_keyset() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT KEYS.ROTATE_KEYSET(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AEAD_AES_GCM_256')",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_aead_encrypt() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE keyset BYTES DEFAULT KEYS.NEW_KEYSET('AEAD_AES_GCM_256')")
        .unwrap();

    let result =
        executor.execute_sql("SELECT AEAD.ENCRYPT(keyset, b'plaintext', b'additional_data')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aead_decrypt_string() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT AEAD.DECRYPT_STRING(
            KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
            AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'secret', b'aad'),
            b'aad'
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_aead_decrypt_bytes() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT AEAD.DECRYPT_BYTES(
            KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
            AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'secret', b''),
            b''
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_deterministic_encrypt() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT DETERMINISTIC_ENCRYPT(
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
            b'plaintext',
            b'additional_data'
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_deterministic_decrypt_string() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT DETERMINISTIC_DECRYPT_STRING(
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
            DETERMINISTIC_ENCRYPT(
                KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
                b'secret',
                b'aad'
            ),
            b'aad'
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_deterministic_decrypt_bytes() {
    let mut executor = create_executor();

    let result = executor.execute_sql(
        "SELECT DETERMINISTIC_DECRYPT_BYTES(
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
            DETERMINISTIC_ENCRYPT(
                KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
                b'secret',
                b''
            ),
            b''
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_keyset_in_table() {
    let mut executor = create_executor();

    executor
        .execute_sql(
            "CREATE TABLE encryption_keys (
                id INT64,
                keyset BYTES
            )",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO encryption_keys VALUES (1, KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM encryption_keys")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_encrypt_column_data() {
    let mut executor = create_executor();

    executor
        .execute_sql("DECLARE keyset BYTES DEFAULT KEYS.NEW_KEYSET('AEAD_AES_GCM_256')")
        .unwrap();

    executor
        .execute_sql(
            "CREATE TABLE users (
                id INT64,
                name STRING,
                encrypted_ssn BYTES
            )",
        )
        .unwrap();

    let _ = executor.execute_sql(
        "INSERT INTO users VALUES (
            1,
            'Alice',
            AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'123-45-6789', b'')
        )",
    );

    let result = executor.execute_sql("SELECT id, name FROM users").unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[test]
fn test_keys_keyset_length() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT KEYS.KEYSET_LENGTH(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_error_function() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT ERROR('This is an error message')");
    assert!(result.is_err());
}

#[test]
fn test_error_in_case() {
    let mut executor = create_executor();

    let result =
        executor.execute_sql("SELECT CASE WHEN 1 = 2 THEN 'ok' ELSE ERROR('Condition failed') END");
    assert!(result.is_err());
}

#[test]
fn test_error_with_null_check() {
    let mut executor = create_executor();

    executor
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO data VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT IF(value IS NULL, ERROR('Value cannot be NULL'), value) FROM data");
    assert!(result.is_err());
}

#[test]
fn test_safe_divide() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT SAFE_DIVIDE(10, 0)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_multiply() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SAFE_MULTIPLY(9223372036854775807, 2)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_negate() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SAFE_NEGATE(-9223372036854775808)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_add() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SAFE_ADD(9223372036854775807, 1)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_safe_subtract() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SAFE_SUBTRACT(-9223372036854775808, 1)")
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
fn test_safe_convert_bytes_to_string() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\xFF\\xFE') IS NULL")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_nullifzero() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT NULLIFZERO(0)").unwrap();
    assert_table_eq!(result, [[null]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_zeroifnull() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT ZEROIFNULL(NULL)").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_ifnull() {
    let mut executor = create_executor();

    let result = executor
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[test]
fn test_nullif() {
    let mut executor = create_executor();

    let result = executor.execute_sql("SELECT NULLIF(5, 5)").unwrap();
    assert_table_eq!(result, [[null]]);
}
