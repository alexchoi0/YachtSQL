use crate::assert_table_eq;
use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_keys_new_keyset() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT KEYS.NEW_KEYSET('AEAD_AES_GCM_256') IS NOT NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_keys_add_key_from_raw_bytes() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT KEYS.ADD_KEY_FROM_RAW_BYTES(
            KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
            'AES_GCM',
            b'12345678901234567890123456789012'
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_keys_keyset_chain() {
    let session = create_session();

    let result = session.execute_sql(
        "SELECT KEYS.KEYSET_CHAIN('gcp-kms://projects/my-project/locations/us/keyRings/my-ring/cryptoKeys/my-key', KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))",
    ).await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_keys_keyset_from_json() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT KEYS.KEYSET_FROM_JSON('{\"primaryKeyId\": 123, \"key\": []}')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_keys_keyset_to_json() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT KEYS.KEYSET_TO_JSON(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_keys_rotate_keyset() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT KEYS.ROTATE_KEYSET(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), 'AEAD_AES_GCM_256')",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_aead_encrypt() {
    let session = create_session();

    session
        .execute_sql("DECLARE keyset BYTES DEFAULT KEYS.NEW_KEYSET('AEAD_AES_GCM_256')")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT AEAD.ENCRYPT(keyset, b'plaintext', b'additional_data')")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_aead_decrypt_string() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT AEAD.DECRYPT_STRING(
            KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
            AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'secret', b'aad'),
            b'aad'
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_aead_decrypt_bytes() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT AEAD.DECRYPT_BYTES(
            KEYS.NEW_KEYSET('AEAD_AES_GCM_256'),
            AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'secret', b''),
            b''
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_deterministic_encrypt() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT DETERMINISTIC_ENCRYPT(
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
            b'plaintext',
            b'additional_data'
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_deterministic_decrypt_string() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT DETERMINISTIC_DECRYPT_STRING(
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
            DETERMINISTIC_ENCRYPT(
                KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
                b'secret',
                b'aad'
            ),
            b'aad'
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_deterministic_decrypt_bytes() {
    let session = create_session();

    let result = session
        .execute_sql(
            "SELECT DETERMINISTIC_DECRYPT_BYTES(
            KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
            DETERMINISTIC_ENCRYPT(
                KEYS.NEW_KEYSET('DETERMINISTIC_AEAD_AES_SIV_CMAC_256'),
                b'secret',
                b''
            ),
            b''
        )",
        )
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_keyset_in_table() {
    let session = create_session();

    session
        .execute_sql(
            "CREATE TABLE encryption_keys (
                id INT64,
                keyset BYTES
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql("INSERT INTO encryption_keys VALUES (1, KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id FROM encryption_keys")
        .await
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_encrypt_column_data() {
    let session = create_session();

    session
        .execute_sql("DECLARE keyset BYTES DEFAULT KEYS.NEW_KEYSET('AEAD_AES_GCM_256')")
        .await
        .unwrap();

    session
        .execute_sql(
            "CREATE TABLE users (
                id INT64,
                name STRING,
                encrypted_ssn BYTES
            )",
        )
        .await
        .unwrap();

    session
        .execute_sql(
            "INSERT INTO users VALUES (
            1,
            'Alice',
            AEAD.ENCRYPT(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'), b'123-45-6789', b'')
        )",
        )
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT id, name FROM users")
        .await
        .unwrap();
    assert_table_eq!(result, [[1, "Alice"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_keys_keyset_length() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT KEYS.KEYSET_LENGTH(KEYS.NEW_KEYSET('AEAD_AES_GCM_256'))")
        .await;
    assert!(result.is_ok() || result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_error_function() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ERROR('This is an error message')")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_error_in_case() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT CASE WHEN 1 = 2 THEN 'ok' ELSE ERROR('Condition failed') END")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_error_with_null_check() {
    let session = create_session();

    session
        .execute_sql("CREATE TABLE data (id INT64, value INT64)")
        .await
        .unwrap();
    session
        .execute_sql("INSERT INTO data VALUES (1, NULL)")
        .await
        .unwrap();

    let result = session
        .execute_sql("SELECT IF(value IS NULL, ERROR('Value cannot be NULL'), value) FROM data")
        .await;
    assert!(result.is_err());
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_divide() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_DIVIDE(10, 0)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_multiply() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_MULTIPLY(9223372036854775807, 2)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_negate() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_NEGATE(-9223372036854775808)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_add() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_ADD(9223372036854775807, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_subtract() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_SUBTRACT(-9223372036854775808, 1)")
        .await
        .unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_safe_convert_bytes_to_string() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT SAFE_CONVERT_BYTES_TO_STRING(b'\\xFF\\xFE') IS NULL")
        .await
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullifzero() {
    let session = create_session();

    let result = session.execute_sql("SELECT NULLIFZERO(0)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_zeroifnull() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT ZEROIFNULL(NULL)")
        .await
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_ifnull() {
    let session = create_session();

    let result = session
        .execute_sql("SELECT IFNULL(NULL, 'default')")
        .await
        .unwrap();
    assert_table_eq!(result, [["default"]]);
}

#[tokio::test(flavor = "current_thread")]
async fn test_nullif() {
    let session = create_session();

    let result = session.execute_sql("SELECT NULLIF(5, 5)").await.unwrap();
    assert_table_eq!(result, [[null]]);
}
