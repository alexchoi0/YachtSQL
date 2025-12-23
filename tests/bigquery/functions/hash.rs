use crate::assert_table_eq;
use crate::common::create_session;

#[test]
fn test_md5() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT TO_HEX(MD5('hello'))").unwrap();
    assert_table_eq!(result, [["5d41402abc4b2a76b9719d911017c592"]]);
}

#[test]
fn test_sha256() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SHA256('hello'))")
        .unwrap();
    assert_table_eq!(result, [[32]]);
}

#[test]
fn test_sha512() {
    let mut session = create_session();
    let result = session
        .execute_sql("SELECT LENGTH(SHA512('hello'))")
        .unwrap();
    assert_table_eq!(result, [[64]]);
}

#[test]
fn test_md5_with_column() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE users (name STRING)")
        .unwrap();
    session
        .execute_sql("INSERT INTO users VALUES ('alice'), ('bob')")
        .unwrap();

    let result = session
        .execute_sql("SELECT name, TO_HEX(MD5(name)) AS hash FROM users ORDER BY name")
        .unwrap();
    assert_table_eq!(
        result,
        [
            ["alice", "6384e2b2184bcbf58eccf10ca7a6563c"],
            ["bob", "9f9d51bc70ef21ca5c14f307980a29d8"]
        ]
    );
}

#[test]
fn test_hash_null() {
    let mut session = create_session();
    let result = session.execute_sql("SELECT MD5(NULL) IS NULL").unwrap();
    assert_table_eq!(result, [[true]]);
}
