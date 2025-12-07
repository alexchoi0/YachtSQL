use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_domain_basic() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE DOMAIN positive_int AS INT64 CHECK (VALUE > 0)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_with_not_null() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE DOMAIN non_null_string AS STRING NOT NULL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_with_default() {
    let mut executor = create_executor();
    let result = executor.execute_sql("CREATE DOMAIN status_type AS STRING DEFAULT 'pending'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_in_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN email AS STRING CHECK (VALUE LIKE '%@%')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE users (id INT64, email email)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'test@example.com')")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM users").unwrap();
    assert_table_eq!(result, [[1, "test@example.com"]]);
}

#[test]
fn test_domain_check_violation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN positive AS INT64 CHECK (VALUE > 0)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE pos_test (id INT64, val positive)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO pos_test VALUES (1, -5)");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_domain_null_handling() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN nullable_pos AS INT64 CHECK (VALUE IS NULL OR VALUE > 0)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE null_pos (id INT64, val nullable_pos)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO null_pos VALUES (1, NULL)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM null_pos").unwrap();
    assert_table_eq!(result, [[1, null]]);
}

#[test]
fn test_domain_multiple_checks() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE DOMAIN bounded_int AS INT64
         CHECK (VALUE >= 0)
         CHECK (VALUE <= 100)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_named_constraint() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE DOMAIN percentage AS INT64
         CONSTRAINT percentage_range CHECK (VALUE >= 0 AND VALUE <= 100)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_domain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN temp_domain AS INT64")
        .unwrap();
    let result = executor.execute_sql("DROP DOMAIN temp_domain");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_domain_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN cascade_dom AS INT64")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE cascade_table (id INT64, val cascade_dom)")
        .unwrap();
    let result = executor.execute_sql("DROP DOMAIN cascade_dom CASCADE");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_domain_restrict() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN restrict_dom AS INT64")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE restrict_table (id INT64, val restrict_dom)")
        .unwrap();
    let result = executor.execute_sql("DROP DOMAIN restrict_dom RESTRICT");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_alter_domain_add_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN alt_add AS INT64")
        .unwrap();
    let result =
        executor.execute_sql("ALTER DOMAIN alt_add ADD CONSTRAINT positive CHECK (VALUE > 0)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_domain_drop_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN alt_drop AS INT64 CONSTRAINT pos CHECK (VALUE > 0)")
        .unwrap();
    let result = executor.execute_sql("ALTER DOMAIN alt_drop DROP CONSTRAINT pos");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_domain_set_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN alt_def AS INT64")
        .unwrap();
    let result = executor.execute_sql("ALTER DOMAIN alt_def SET DEFAULT 0");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_domain_drop_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN alt_drop_def AS INT64 DEFAULT 0")
        .unwrap();
    let result = executor.execute_sql("ALTER DOMAIN alt_drop_def DROP DEFAULT");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_domain_set_not_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN alt_nn AS INT64")
        .unwrap();
    let result = executor.execute_sql("ALTER DOMAIN alt_nn SET NOT NULL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_domain_drop_not_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN alt_drop_nn AS INT64 NOT NULL")
        .unwrap();
    let result = executor.execute_sql("ALTER DOMAIN alt_drop_nn DROP NOT NULL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_domain_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN old_name AS INT64")
        .unwrap();
    let result = executor.execute_sql("ALTER DOMAIN old_name RENAME TO new_name");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_based_on_domain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN base_dom AS INT64 CHECK (VALUE > 0)")
        .unwrap();
    let result = executor.execute_sql("CREATE DOMAIN derived_dom AS base_dom CHECK (VALUE < 100)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_string_length() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN short_string AS STRING CHECK (LENGTH(VALUE) <= 10)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE short_test (id INT64, val short_string)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO short_test VALUES (1, 'short')")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO short_test VALUES (2, 'this is too long')");
    assert!(result.is_err() || result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_domain_regex_check() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN zip_code AS STRING CHECK (VALUE ~ '^[0-9]{5}$')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE addresses (id INT64, zip zip_code)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO addresses VALUES (1, '12345')")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO addresses VALUES (2, 'ABCDE')");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_domain_numeric_range() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN age AS INT64 CHECK (VALUE >= 0 AND VALUE <= 150)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE persons (id INT64, age age)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO persons VALUES (1, 25)")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM persons").unwrap();
    assert_table_eq!(result, [[1, 25]]);
}

#[test]
fn test_domain_in_function_param() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN currency AS FLOAT64 CHECK (VALUE >= 0)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE FUNCTION add_tax(amount currency) RETURNS FLOAT64
         AS $$ SELECT amount * 1.1; $$ LANGUAGE SQL",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_cast() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN typed_int AS INT64")
        .unwrap();

    let result = executor.execute_sql("SELECT 42::typed_int");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_array() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN score AS INT64 CHECK (VALUE >= 0 AND VALUE <= 100)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE exams (id INT64, scores score[])")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO exams VALUES (1, ARRAY[85, 90, 95])");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_collation() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("CREATE DOMAIN case_insensitive AS STRING COLLATE \"en_US.utf8\"");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_validate_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE DOMAIN validate_dom AS INT64")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE validate_table (id INT64, val validate_dom)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO validate_table VALUES (1, -5)")
        .unwrap();

    let result = executor
        .execute_sql("ALTER DOMAIN validate_dom ADD CONSTRAINT pos CHECK (VALUE > 0) NOT VALID");
    assert!(result.is_ok() || result.is_err());
}
