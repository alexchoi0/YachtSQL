use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_md5() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT MD5('hello')").unwrap();
    assert_table_eq!(result, [["5d41402abc4b2a76b9719d911017c592"]]);
}

#[test]
fn test_sha1() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA1('hello')").unwrap();
    assert_table_eq!(result, [["aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"]]);
}

#[test]
fn test_sha256() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA256('hello')").unwrap();
    assert_table_eq!(
        result,
        [["2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"]]
    );
}

#[test]
fn test_sha512() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT SHA512('hello')").unwrap();
    assert_table_eq!(
        result,
        [[
            "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
        ]]
    );
}

#[test]
fn test_xxhash32() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT xxHash32('hello')").unwrap();
    assert_table_eq!(result, [[4211111929i64]]);
}

#[test]
fn test_xxhash64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT xxHash64('hello')").unwrap();
    assert_table_eq!(result, [[2794345569481354659i64]]);
}

#[test]
fn test_city_hash64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT cityHash64('hello')").unwrap();
    assert_table_eq!(result, [[-5436999610281751320i64]]);
}

#[test]
fn test_sip_hash64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT sipHash64('hello')").unwrap();
    assert_table_eq!(result, [[-8304253580878589255i64]]);
}

#[test]
fn test_murmur_hash2_32() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT murmurHash2_32('hello')")
        .unwrap();
    assert_table_eq!(result, [[3848350155i64]]);
}

#[test]
fn test_murmur_hash2_64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT murmurHash2_64('hello')")
        .unwrap();
    assert_table_eq!(result, [[2191231550387646743i64]]);
}

#[test]
fn test_murmur_hash3_32() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT murmurHash3_32('hello')")
        .unwrap();
    assert_table_eq!(result, [[613153351]]);
}

#[test]
fn test_murmur_hash3_64() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT murmurHash3_64('hello')")
        .unwrap();
    assert_table_eq!(result, [[-3758069500696749310i64]]);
}

#[test]
fn test_murmur_hash3_128() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT murmurHash3_128('hello')")
        .unwrap();
    assert_table_eq!(result, [["5b1e906a48ae1d19cbd8a7b341bd9b02"]]);
}

#[test]
fn test_java_hash() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT javaHash('hello')").unwrap();
    assert_table_eq!(result, [[99162322]]);
}

#[test]
fn test_hash_with_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE hash_test (id INT64, value STRING)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO hash_test VALUES (1, 'hello'), (2, 'world')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, cityHash64(value) AS hash FROM hash_test ORDER BY id")
        .unwrap();
    assert_table_eq!(
        result,
        [[1, -5436999610281751320i64], [2, -2010201635338800018i64],]
    );
}

#[test]
fn test_half_md5() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT halfMD5('hello')").unwrap();
    assert_table_eq!(result, [[6719722671305337462i64]]);
}

#[test]
fn test_farm_hash64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT farmHash64('hello')").unwrap();
    assert_table_eq!(result, [[-5436999610281751320i64]]);
}

#[test]
fn test_metro_hash64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT metroHash64('hello')").unwrap();
    assert_table_eq!(result, [[4571129541730210044i64]]);
}
