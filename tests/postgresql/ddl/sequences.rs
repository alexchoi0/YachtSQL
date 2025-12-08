use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_create_sequence_basic() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_test").unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_test')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_sequence_start_with() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_start START WITH 100")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_start')").unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_create_sequence_increment_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_incr INCREMENT BY 5")
        .unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_incr')").unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_incr')").unwrap();
    assert_table_eq!(result, [[6]]);
}

#[test]
fn test_create_sequence_minvalue() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_min MINVALUE 10 START WITH 10")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_min')").unwrap();
    assert_table_eq!(result, [[10]]);
}

#[test]
fn test_create_sequence_maxvalue() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_max MAXVALUE 100")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_max')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_sequence_cycle() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_cycle MAXVALUE 3 CYCLE")
        .unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_cycle')").unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_cycle')").unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_cycle')").unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_cycle')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_sequence_no_cycle() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_no_cycle NO CYCLE")
        .unwrap();
    let result = executor
        .execute_sql("SELECT NEXTVAL('seq_no_cycle')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_sequence_cache() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_cache CACHE 10")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_cache')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_create_sequence_if_not_exists() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_exists").unwrap();
    executor
        .execute_sql("CREATE SEQUENCE IF NOT EXISTS seq_exists")
        .unwrap();
    let result = executor
        .execute_sql("SELECT NEXTVAL('seq_exists')")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_sequence() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_drop").unwrap();
    executor.execute_sql("DROP SEQUENCE seq_drop").unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_drop_sequence_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("DROP SEQUENCE IF EXISTS nonexistent_seq")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_nextval() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_next").unwrap();
    let r1 = executor.execute_sql("SELECT NEXTVAL('seq_next')").unwrap();
    let r2 = executor.execute_sql("SELECT NEXTVAL('seq_next')").unwrap();
    assert_table_eq!(r1, [[1]]);
    assert_table_eq!(r2, [[2]]);
}

#[test]
fn test_currval() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_curr").unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_curr')").unwrap();
    let result = executor.execute_sql("SELECT CURRVAL('seq_curr')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_setval() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_set").unwrap();
    executor
        .execute_sql("SELECT SETVAL('seq_set', 50)")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_set')").unwrap();
    assert_table_eq!(result, [[51]]);
}

#[test]
fn test_setval_is_called_false() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_set_false")
        .unwrap();
    executor
        .execute_sql("SELECT SETVAL('seq_set_false', 50, FALSE)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT NEXTVAL('seq_set_false')")
        .unwrap();
    assert_table_eq!(result, [[50]]);
}

#[test]
fn test_lastval() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_last").unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_last')").unwrap();
    let result = executor.execute_sql("SELECT LASTVAL()").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_alter_sequence_restart() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SEQUENCE seq_alter").unwrap();
    executor.execute_sql("SELECT NEXTVAL('seq_alter')").unwrap();
    executor
        .execute_sql("ALTER SEQUENCE seq_alter RESTART WITH 100")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_alter')").unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
fn test_alter_sequence_increment() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_alter_incr")
        .unwrap();
    executor
        .execute_sql("ALTER SEQUENCE seq_alter_incr INCREMENT BY 10")
        .unwrap();
    executor
        .execute_sql("SELECT NEXTVAL('seq_alter_incr')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT NEXTVAL('seq_alter_incr')")
        .unwrap();
    assert_table_eq!(result, [[11]]);
}

#[test]
fn test_sequence_owned_by() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE items_seq (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE SEQUENCE seq_owned OWNED BY items_seq.id")
        .unwrap();
    let result = executor.execute_sql("SELECT NEXTVAL('seq_owned')").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_serial_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE serial_test (id SERIAL, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO serial_test (name) VALUES ('Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO serial_test (name) VALUES ('Bob')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM serial_test ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1], [2]]);
}

#[test]
fn test_bigserial_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE bigserial_test (id BIGSERIAL, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO bigserial_test (name) VALUES ('Alice')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM bigserial_test")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_smallserial_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE smallserial_test (id SMALLSERIAL, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO smallserial_test (name) VALUES ('Alice')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM smallserial_test")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_sequence_descending() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE SEQUENCE seq_desc INCREMENT BY -1 START WITH 10 MINVALUE 1")
        .unwrap();
    let r1 = executor.execute_sql("SELECT NEXTVAL('seq_desc')").unwrap();
    let r2 = executor.execute_sql("SELECT NEXTVAL('seq_desc')").unwrap();
    assert_table_eq!(r1, [[10]]);
    assert_table_eq!(r2, [[9]]);
}

#[test]
fn test_generated_always_as_identity() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE identity_test (id INTEGER GENERATED ALWAYS AS IDENTITY, name TEXT)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO identity_test (name) VALUES ('Alice')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM identity_test")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_generated_by_default_as_identity() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE identity_default (id INTEGER GENERATED BY DEFAULT AS IDENTITY, name TEXT)").unwrap();
    executor
        .execute_sql("INSERT INTO identity_default (id, name) VALUES (100, 'Alice')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM identity_default")
        .unwrap();
    assert_table_eq!(result, [[100]]);
}

#[test]
#[ignore = "sqlparser 0.59 doesn't support sequence options in GENERATED AS IDENTITY"]
fn test_generated_identity_with_sequence_options() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE identity_opts (id INTEGER GENERATED ALWAYS AS IDENTITY (START WITH 100 INCREMENT BY 10), name TEXT)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO identity_opts (name) VALUES ('Alice')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO identity_opts (name) VALUES ('Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO identity_opts (name) VALUES ('Charlie')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM identity_opts ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[100], [110], [120]]);
}

#[test]
fn test_drop_table_drops_owned_sequence() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE serial_drop_test (id SERIAL, name TEXT)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO serial_drop_test (name) VALUES ('Test')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT id FROM serial_drop_test")
        .unwrap();
    assert_table_eq!(result, [[1]]);

    executor.execute_sql("DROP TABLE serial_drop_test").unwrap();

    executor
        .execute_sql("CREATE TABLE serial_drop_test (id SERIAL, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO serial_drop_test (name) VALUES ('Test2')")
        .unwrap();
    let result2 = executor
        .execute_sql("SELECT id FROM serial_drop_test")
        .unwrap();
    assert_table_eq!(result2, [[1]]);
}
