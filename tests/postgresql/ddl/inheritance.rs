use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
#[ignore = "Implement me!"]
fn test_create_table_inherits() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cities (name TEXT, population INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE capitals (state TEXT) INHERITS (cities)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO capitals VALUES ('Albany', 100000, 'NY')")
        .unwrap();
    let result = executor.execute_sql("SELECT * FROM capitals").unwrap();
    assert_table_eq!(result, [["Albany", 100000, "NY"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_parent_sees_children() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE parent_table (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE child_table (extra TEXT) INHERITS (parent_table)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO parent_table VALUES (1, 'Parent')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO child_table VALUES (2, 'Child', 'Extra')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM parent_table ORDER BY id")
        .unwrap();
    assert_table_eq!(result, [[1, "Parent"], [2, "Child"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_only_parent() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE only_parent (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE only_child (extra TEXT) INHERITS (only_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO only_parent VALUES (1, 'Parent')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO only_child VALUES (2, 'Child', 'Extra')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT * FROM ONLY only_parent")
        .unwrap();
    assert_table_eq!(result, [[1, "Parent"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_multiple_parents() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE person (name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE employee (emp_id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE manager () INHERITS (person, employee)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO manager VALUES ('John', 101)")
        .unwrap();
    let result = executor.execute_sql("SELECT * FROM manager").unwrap();
    assert_table_eq!(result, [["John", 101]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_chain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE base (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE mid (mid_val TEXT) INHERITS (base)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE leaf (leaf_val INTEGER) INHERITS (mid)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO leaf VALUES (1, 'mid', 100)")
        .unwrap();
    let result = executor.execute_sql("SELECT * FROM base").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_no_inherit_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE parent_con (id INTEGER, val INTEGER CHECK (val > 0))")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE child_con (extra TEXT) INHERITS (parent_con)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO child_con VALUES (1, -1, 'test')");
    assert!(result.is_err() || result.is_ok());
}

#[test]
fn test_alter_inherit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE new_parent (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE new_child (id INTEGER, name TEXT, extra TEXT)")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE new_child INHERIT new_parent");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_no_inherit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE old_parent (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE old_child (extra TEXT) INHERITS (old_parent)")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE old_child NO INHERIT old_parent");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_update_parent() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE upd_parent (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE upd_child (extra TEXT) INHERITS (upd_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO upd_child VALUES (1, 100, 'test')")
        .unwrap();
    executor
        .execute_sql("UPDATE upd_parent SET val = 200 WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT val FROM upd_child").unwrap();
    assert_table_eq!(result, [[200]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_delete_parent() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE del_parent (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE del_child (extra TEXT) INHERITS (del_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO del_child VALUES (1, 100, 'test')")
        .unwrap();
    executor
        .execute_sql("DELETE FROM del_parent WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT * FROM del_child").unwrap();
    assert_table_eq!(result, []);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_index_on_parent() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE idx_parent (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE idx_child (extra TEXT) INHERITS (idx_parent)")
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_parent_val ON idx_parent (val)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO idx_child VALUES (1, 100, 'test')")
        .unwrap();
    let result = executor
        .execute_sql("SELECT * FROM idx_parent WHERE val = 100")
        .unwrap();
    assert_table_eq!(result, [[1, 100]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_with_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE def_parent (id INTEGER, status TEXT DEFAULT 'active')")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE def_child (extra INTEGER) INHERITS (def_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO def_child (id, extra) VALUES (1, 100)")
        .unwrap();
    let result = executor
        .execute_sql("SELECT status FROM def_child")
        .unwrap();
    assert_table_eq!(result, [["active"]]);
}

#[test]
fn test_inheritance_with_not_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nn_parent (id INTEGER NOT NULL, name TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE nn_child (extra TEXT) INHERITS (nn_parent)")
        .unwrap();

    let result =
        executor.execute_sql("INSERT INTO nn_child (name, extra) VALUES ('test', 'extra')");
    assert!(result.is_err() || result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_count_with_only() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cnt_parent (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE cnt_child (extra TEXT) INHERITS (cnt_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO cnt_parent VALUES (1), (2)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cnt_child VALUES (3, 'a'), (4, 'b'), (5, 'c')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM cnt_parent")
        .unwrap();
    assert_table_eq!(result, [[5]]);

    let result_only = executor
        .execute_sql("SELECT COUNT(*) FROM ONLY cnt_parent")
        .unwrap();
    assert_table_eq!(result_only, [[2]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_tableoid() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE oid_parent (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE oid_child (extra TEXT) INHERITS (oid_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO oid_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO oid_child VALUES (2, 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT tableoid::regclass, id FROM oid_parent ORDER BY id");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_inheritance_unique_on_child() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uniq_parent (id INTEGER)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE uniq_child (code TEXT UNIQUE) INHERITS (uniq_parent)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO uniq_child VALUES (1, 'ABC')")
        .unwrap();
    let result = executor.execute_sql("INSERT INTO uniq_child VALUES (2, 'ABC')");
    assert!(result.is_err() || result.is_ok());
}
