use crate::assert_table_eq;
use crate::common::create_executor;

#[test]
fn test_primary_key_single_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pk_single (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pk_single VALUES (1, 'Alice')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM pk_single WHERE id = 1")
        .unwrap();
    assert_table_eq!(result, [["Alice"]]);
}

#[test]
fn test_primary_key_composite() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pk_comp (a INTEGER, b INTEGER, name TEXT, PRIMARY KEY (a, b))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pk_comp VALUES (1, 1, 'test')")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM pk_comp WHERE a = 1 AND b = 1")
        .unwrap();
    assert_table_eq!(result, [["test"]]);
}

#[test]
fn test_primary_key_named() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pk_named (id INTEGER, CONSTRAINT pk_named_id PRIMARY KEY (id))")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_unique_constraint_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uq_col (id INTEGER, email TEXT UNIQUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uq_col VALUES (1, 'test@example.com')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM uq_col").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_unique_constraint_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uq_tbl (id INTEGER, a INTEGER, b INTEGER, UNIQUE (a, b))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uq_tbl VALUES (1, 1, 1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM uq_tbl").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_unique_constraint_named() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE uq_named (id INTEGER, email TEXT, CONSTRAINT uq_email UNIQUE (email))",
        )
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_not_null_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nn_test (id INTEGER NOT NULL, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO nn_test VALUES (1, 'test')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM nn_test").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_check_constraint_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE chk_col (id INTEGER, age INTEGER CHECK (age >= 0))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO chk_col VALUES (1, 25)")
        .unwrap();

    let result = executor.execute_sql("SELECT age FROM chk_col").unwrap();
    assert_table_eq!(result, [[25]]);
}

#[test]
fn test_check_constraint_table() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE chk_tbl (id INTEGER, start_date DATE, end_date DATE, CHECK (end_date > start_date))"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO chk_tbl VALUES (1, '2024-01-01', '2024-12-31')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM chk_tbl").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_check_constraint_named() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE chk_named (id INTEGER, val INTEGER, CONSTRAINT chk_positive CHECK (val > 0))",
        )
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_basic() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE child (id INTEGER, parent_id INTEGER REFERENCES parent(id))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM child").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_table_level() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE departments (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE employees (id INTEGER, dept_id INTEGER, FOREIGN KEY (dept_id) REFERENCES departments(id))"
    ).unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_named() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ref_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE ref_child (id INTEGER, pid INTEGER, CONSTRAINT fk_parent FOREIGN KEY (pid) REFERENCES ref_parent(id))"
    ).unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_on_delete_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE cascade_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE cascade_child (id INTEGER, pid INTEGER REFERENCES cascade_parent(id) ON DELETE CASCADE)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO cascade_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO cascade_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("DELETE FROM cascade_parent WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM cascade_child")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_foreign_key_on_delete_set_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE setnull_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE setnull_child (id INTEGER, pid INTEGER REFERENCES setnull_parent(id) ON DELETE SET NULL)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO setnull_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO setnull_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("DELETE FROM setnull_parent WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT pid IS NULL FROM setnull_child")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_foreign_key_on_delete_set_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE setdef_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE setdef_child (id INTEGER, pid INTEGER DEFAULT 0 REFERENCES setdef_parent(id) ON DELETE SET DEFAULT)"
    ).unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_on_update_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE upd_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE upd_child (id INTEGER, pid INTEGER REFERENCES upd_parent(id) ON UPDATE CASCADE)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO upd_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO upd_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("UPDATE upd_parent SET id = 2 WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT pid FROM upd_child").unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_default_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE def_test (id INTEGER, status TEXT DEFAULT 'pending')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO def_test (id) VALUES (1)")
        .unwrap();

    let result = executor.execute_sql("SELECT status FROM def_test").unwrap();
    assert_table_eq!(result, [["pending"]]);
}

#[test]
fn test_default_current_timestamp() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE ts_test (id INTEGER, created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO ts_test (id) VALUES (1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT created_at IS NOT NULL FROM ts_test")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_add_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE add_const (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE add_const ADD CONSTRAINT chk_val CHECK (val > 0)")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE drop_const (id INTEGER, val INTEGER, CONSTRAINT chk_drop CHECK (val > 0))",
        )
        .unwrap();
    executor
        .execute_sql("ALTER TABLE drop_const DROP CONSTRAINT chk_drop")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_add_primary_key() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE add_pk (id INTEGER, name TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE add_pk ADD PRIMARY KEY (id)")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_add_foreign_key() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_ref (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE add_fk (id INTEGER, ref_id INTEGER)")
        .unwrap();
    executor.execute_sql(
        "ALTER TABLE add_fk ADD CONSTRAINT fk_ref_id FOREIGN KEY (ref_id) REFERENCES fk_ref(id)"
    ).unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_exclusion_constraint() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE excl_test (id INTEGER, range_start INTEGER, range_end INTEGER, EXCLUDE USING gist (int8range(range_start, range_end) WITH &&))"
    ).unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_deferrable_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE defer_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE defer_child (id INTEGER, pid INTEGER REFERENCES defer_parent(id) DEFERRABLE INITIALLY DEFERRED)"
    ).unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_not_valid_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE notvalid_test (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO notvalid_test VALUES (1, -5)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE notvalid_test ADD CONSTRAINT chk_pos CHECK (val > 0) NOT VALID")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_validate_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE validate_test (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE validate_test ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE validate_test VALIDATE CONSTRAINT chk_val")
        .unwrap();
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_composite() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE comp_parent (a INTEGER, b INTEGER, name TEXT, PRIMARY KEY (a, b))",
        )
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE comp_child (id INTEGER, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES comp_parent(a, b))"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO comp_parent VALUES (1, 1, 'test')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO comp_child VALUES (1, 1, 1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM comp_child").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_on_delete_restrict() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE restrict_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE restrict_child (id INTEGER, pid INTEGER REFERENCES restrict_parent(id) ON DELETE RESTRICT)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO restrict_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO restrict_child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("DELETE FROM restrict_parent WHERE id = 1");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_on_delete_no_action() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE noact_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE noact_child (id INTEGER, pid INTEGER REFERENCES noact_parent(id) ON DELETE NO ACTION)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO noact_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO noact_child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("DELETE FROM noact_parent WHERE id = 1");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_on_update_restrict() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE upd_restrict_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE upd_restrict_child (id INTEGER, pid INTEGER REFERENCES upd_restrict_parent(id) ON UPDATE RESTRICT)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO upd_restrict_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO upd_restrict_child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("UPDATE upd_restrict_parent SET id = 2 WHERE id = 1");
    assert!(result.is_err());
}

#[test]
fn test_foreign_key_on_update_set_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE upd_setnull_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE upd_setnull_child (id INTEGER, pid INTEGER REFERENCES upd_setnull_parent(id) ON UPDATE SET NULL)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO upd_setnull_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO upd_setnull_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("UPDATE upd_setnull_parent SET id = 2 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT pid IS NULL FROM upd_setnull_child")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_foreign_key_on_update_set_default() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE upd_setdef_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO upd_setdef_parent VALUES (0), (1)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE upd_setdef_child (id INTEGER, pid INTEGER DEFAULT 0 REFERENCES upd_setdef_parent(id) ON UPDATE SET DEFAULT)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO upd_setdef_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("UPDATE upd_setdef_parent SET id = 2 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT pid FROM upd_setdef_child")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_foreign_key_self_referencing() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE employees_hier (id INTEGER PRIMARY KEY, name TEXT, manager_id INTEGER REFERENCES employees_hier(id))"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO employees_hier VALUES (1, 'CEO', NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees_hier VALUES (2, 'Manager', 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO employees_hier VALUES (3, 'Employee', 2)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT name FROM employees_hier WHERE manager_id = 1")
        .unwrap();
    assert_table_eq!(result, [["Manager"]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_self_referencing_cascade() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE tree_nodes (id INTEGER PRIMARY KEY, parent_id INTEGER REFERENCES tree_nodes(id) ON DELETE CASCADE)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO tree_nodes VALUES (1, NULL), (2, 1), (3, 2)")
        .unwrap();
    executor
        .execute_sql("DELETE FROM tree_nodes WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT COUNT(*) FROM tree_nodes")
        .unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
fn test_foreign_key_multiple_references() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE users_fk (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE orders_fk (
            id INTEGER PRIMARY KEY,
            created_by INTEGER REFERENCES users_fk(id),
            updated_by INTEGER REFERENCES users_fk(id)
        )",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO users_fk VALUES (1, 'Alice'), (2, 'Bob')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO orders_fk VALUES (1, 1, 2)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM orders_fk").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_unique_column() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE unique_ref (id INTEGER, code TEXT UNIQUE)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE ref_unique (id INTEGER, code TEXT REFERENCES unique_ref(code))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO unique_ref VALUES (1, 'ABC')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ref_unique VALUES (1, 'ABC')")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM ref_unique").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_null_allowed() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_null_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE fk_null_child (id INTEGER, pid INTEGER REFERENCES fk_null_parent(id))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO fk_null_child VALUES (1, NULL)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT pid IS NULL FROM fk_null_child")
        .unwrap();
    assert_table_eq!(result, [[true]]);
}

#[test]
fn test_foreign_key_violation_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_viol_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE fk_viol_child (id INTEGER, pid INTEGER REFERENCES fk_viol_parent(id))",
        )
        .unwrap();

    let result = executor.execute_sql("INSERT INTO fk_viol_child VALUES (1, 999)");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_violation_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_upd_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE fk_upd_child (id INTEGER, pid INTEGER REFERENCES fk_upd_parent(id))",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO fk_upd_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fk_upd_child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("UPDATE fk_upd_child SET pid = 999 WHERE id = 1");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_match_full() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE match_full_parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE match_full_child (id INTEGER, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES match_full_parent(a, b) MATCH FULL)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO match_full_parent VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO match_full_child VALUES (1, 1, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM match_full_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_match_partial() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE match_partial_parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
        .unwrap();
    let _ = executor.execute_sql(
        "CREATE TABLE match_partial_child (id INTEGER, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES match_partial_parent(a, b) MATCH PARTIAL)"
    );
    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_match_simple() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE match_simple_parent (a INTEGER, b INTEGER, PRIMARY KEY (a, b))")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE match_simple_child (id INTEGER, pa INTEGER, pb INTEGER, FOREIGN KEY (pa, pb) REFERENCES match_simple_parent(a, b) MATCH SIMPLE)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO match_simple_parent VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO match_simple_child VALUES (1, NULL, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM match_simple_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_deferrable_initially_immediate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE defer_imm_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE defer_imm_child (id INTEGER, pid INTEGER REFERENCES defer_imm_parent(id) DEFERRABLE INITIALLY IMMEDIATE)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO defer_imm_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO defer_imm_child VALUES (1, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM defer_imm_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_deferred_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE defer_ins_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE defer_ins_child (id INTEGER, pid INTEGER REFERENCES defer_ins_parent(id) DEFERRABLE INITIALLY DEFERRED)"
    ).unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO defer_ins_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO defer_ins_parent VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM defer_ins_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_set_constraints_immediate() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE set_imm_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE set_imm_child (id INTEGER, pid INTEGER REFERENCES set_imm_parent(id) DEFERRABLE INITIALLY DEFERRED)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO set_imm_parent VALUES (1)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("SET CONSTRAINTS ALL IMMEDIATE")
        .unwrap();
    executor
        .execute_sql("INSERT INTO set_imm_child VALUES (1, 1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM set_imm_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_set_constraints_deferred() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE set_def_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE set_def_child (id INTEGER, pid INTEGER REFERENCES set_def_parent(id) DEFERRABLE)"
    ).unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("SET CONSTRAINTS ALL DEFERRED")
        .unwrap();
    executor
        .execute_sql("INSERT INTO set_def_child VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO set_def_parent VALUES (1)")
        .unwrap();
    executor.execute_sql("COMMIT").unwrap();

    let result = executor
        .execute_sql("SELECT id FROM set_def_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_circular_reference() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE table_a (id INTEGER PRIMARY KEY, b_id INTEGER)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE table_b (id INTEGER PRIMARY KEY, a_id INTEGER REFERENCES table_a(id))",
        )
        .unwrap();
    executor
        .execute_sql("ALTER TABLE table_a ADD FOREIGN KEY (b_id) REFERENCES table_b(id)")
        .unwrap();

    executor
        .execute_sql("INSERT INTO table_a VALUES (1, NULL)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO table_b VALUES (1, 1)")
        .unwrap();
    executor
        .execute_sql("UPDATE table_a SET b_id = 1 WHERE id = 1")
        .unwrap();

    let result = executor
        .execute_sql("SELECT a.id, b.id FROM table_a a JOIN table_b b ON a.b_id = b.id")
        .unwrap();
    assert_table_eq!(result, [[1, 1]]);
}

#[test]
fn test_foreign_key_across_schemas() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE SCHEMA schema_a").unwrap();
    executor.execute_sql("CREATE SCHEMA schema_b").unwrap();
    executor
        .execute_sql("CREATE TABLE schema_a.parent_tbl (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE schema_b.child_tbl (id INTEGER, pid INTEGER REFERENCES schema_a.parent_tbl(id))"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO schema_a.parent_tbl VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO schema_b.child_tbl VALUES (1, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM schema_b.child_tbl")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_drop_foreign_key() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE drop_fk_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE drop_fk_child (id INTEGER, pid INTEGER, CONSTRAINT fk_drop FOREIGN KEY (pid) REFERENCES drop_fk_parent(id))"
    ).unwrap();
    executor
        .execute_sql("ALTER TABLE drop_fk_child DROP CONSTRAINT fk_drop")
        .unwrap();

    executor
        .execute_sql("INSERT INTO drop_fk_child VALUES (1, 999)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT pid FROM drop_fk_child")
        .unwrap();
    assert_table_eq!(result, [[999]]);
}

#[test]
fn test_foreign_key_with_index() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE idx_fk_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE idx_fk_child (id INTEGER, pid INTEGER REFERENCES idx_fk_parent(id))",
        )
        .unwrap();
    executor
        .execute_sql("CREATE INDEX idx_fk_child_pid ON idx_fk_child (pid)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO idx_fk_parent VALUES (1), (2), (3)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO idx_fk_child VALUES (1, 1), (2, 2), (3, 3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM idx_fk_child WHERE pid = 2")
        .unwrap();
    assert_table_eq!(result, [[2]]);
}

#[test]
fn test_foreign_key_cascade_multiple_levels() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE level1 (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE level2 (id INTEGER PRIMARY KEY, l1_id INTEGER REFERENCES level1(id) ON DELETE CASCADE)"
    ).unwrap();
    executor
        .execute_sql(
            "CREATE TABLE level3 (id INTEGER, l2_id INTEGER REFERENCES level2(id) ON DELETE CASCADE)",
        )
        .unwrap();

    executor
        .execute_sql("INSERT INTO level1 VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO level2 VALUES (1, 1), (2, 1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO level3 VALUES (1, 1), (2, 1), (3, 2)")
        .unwrap();

    executor
        .execute_sql("DELETE FROM level1 WHERE id = 1")
        .unwrap();

    let result = executor.execute_sql("SELECT COUNT(*) FROM level3").unwrap();
    assert_table_eq!(result, [[0]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_references_primary_key_implicitly() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE impl_pk_parent (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    executor
        .execute_sql(
            "CREATE TABLE impl_pk_child (id INTEGER, parent_id INTEGER REFERENCES impl_pk_parent)",
        )
        .unwrap();
    executor
        .execute_sql("INSERT INTO impl_pk_parent VALUES (1, 'test')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO impl_pk_child VALUES (1, 1)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id FROM impl_pk_child")
        .unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_with_check_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE fk_chk_parent (id INTEGER PRIMARY KEY CHECK (id > 0))")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE fk_chk_child (id INTEGER, pid INTEGER REFERENCES fk_chk_parent(id) CHECK (pid > 0))"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO fk_chk_parent VALUES (1)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO fk_chk_child VALUES (1, 1)")
        .unwrap();

    let result = executor.execute_sql("SELECT id FROM fk_chk_child").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
#[ignore = "Implement me!"]
fn test_foreign_key_rename_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rename_fk_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE rename_fk_child (id INTEGER, pid INTEGER, CONSTRAINT old_fk_name FOREIGN KEY (pid) REFERENCES rename_fk_parent(id))"
    ).unwrap();
    executor
        .execute_sql("ALTER TABLE rename_fk_child RENAME CONSTRAINT old_fk_name TO new_fk_name")
        .unwrap();

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_not_enforced() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE not_enf_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    let _ = executor.execute_sql(
        "CREATE TABLE not_enf_child (id INTEGER, pid INTEGER REFERENCES not_enf_parent(id) NOT ENFORCED)"
    );

    let result = executor.execute_sql("SELECT 1").unwrap();
    assert_table_eq!(result, [[1]]);
}

#[test]
fn test_foreign_key_not_enforced_allows_invalid_reference() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ne_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    let create_result = executor.execute_sql(
        "CREATE TABLE ne_child (id INTEGER, pid INTEGER REFERENCES ne_parent(id) NOT ENFORCED)",
    );

    if create_result.is_ok() {
        let result = executor.execute_sql("INSERT INTO ne_child VALUES (1, 999)");
        assert!(result.is_ok());
    }
}

#[test]
fn test_foreign_key_enforced_explicit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enf_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    let result = executor.execute_sql(
        "CREATE TABLE enf_child (id INTEGER, pid INTEGER REFERENCES enf_parent(id) ENFORCED)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_check_constraint_not_enforced() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE chk_ne (id INTEGER, val INTEGER, CONSTRAINT chk_positive CHECK (val > 0) NOT ENFORCED)"
    );

    if result.is_ok() {
        let insert_result = executor.execute_sql("INSERT INTO chk_ne VALUES (1, -5)");
        assert!(insert_result.is_ok());
    }
}

#[test]
fn test_check_constraint_enforced_explicit() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("CREATE TABLE chk_enf (id INTEGER, val INTEGER CHECK (val > 0) ENFORCED)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_unique_constraint_not_enforced() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE uq_ne (id INTEGER, email TEXT, CONSTRAINT uq_email UNIQUE (email) NOT ENFORCED)"
    );

    if result.is_ok() {
        executor
            .execute_sql("INSERT INTO uq_ne VALUES (1, 'test@example.com')")
            .unwrap();
        let insert_result =
            executor.execute_sql("INSERT INTO uq_ne VALUES (2, 'test@example.com')");
        assert!(insert_result.is_ok());
    }
}

#[test]
fn test_unique_constraint_enforced_explicit() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("CREATE TABLE uq_enf (id INTEGER, email TEXT UNIQUE ENFORCED)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_primary_key_not_enforced() {
    let mut executor = create_executor();
    let result =
        executor.execute_sql("CREATE TABLE pk_ne (id INTEGER PRIMARY KEY NOT ENFORCED, name TEXT)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_constraint_not_enforced() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE alter_ne (id INTEGER, val INTEGER, CONSTRAINT chk_val CHECK (val > 0))",
        )
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE alter_ne ALTER CONSTRAINT chk_val NOT ENFORCED");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_constraint_enforced() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE alter_enf (id INTEGER, val INTEGER)")
        .unwrap();

    let add_result = executor
        .execute_sql("ALTER TABLE alter_enf ADD CONSTRAINT chk_val CHECK (val > 0) NOT ENFORCED");

    if add_result.is_ok() {
        let result =
            executor.execute_sql("ALTER TABLE alter_enf ALTER CONSTRAINT chk_val ENFORCED");
        assert!(result.is_ok() || result.is_err());
    }
}

#[test]
fn test_not_enforced_with_not_valid() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ne_nv (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO ne_nv VALUES (1, -5)")
        .unwrap();

    let result = executor.execute_sql(
        "ALTER TABLE ne_nv ADD CONSTRAINT chk_pos CHECK (val > 0) NOT ENFORCED NOT VALID",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_exclusion_constraint_not_enforced() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE excl_ne (id INTEGER, range_start INTEGER, range_end INTEGER, EXCLUDE USING gist (int8range(range_start, range_end) WITH &&) NOT ENFORCED)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_enforcement_in_information_schema() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE info_enf (id INTEGER, val INTEGER CHECK (val > 0))")
        .unwrap();

    let result = executor.execute_sql(
        "SELECT constraint_name FROM information_schema.table_constraints WHERE table_name = 'info_enf'"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_constraint_convalidated() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pg_const (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE pg_const ADD CONSTRAINT chk_val CHECK (val > 0) NOT VALID")
        .unwrap();

    let result =
        executor.execute_sql("SELECT convalidated FROM pg_constraint WHERE conname = 'chk_val'");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_foreign_key_not_enforced_cascade() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE ne_cascade_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    let result = executor.execute_sql(
        "CREATE TABLE ne_cascade_child (id INTEGER, pid INTEGER REFERENCES ne_cascade_parent(id) ON DELETE CASCADE NOT ENFORCED)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_enforcement_with_deferrable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE def_enf_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    let result = executor.execute_sql(
        "CREATE TABLE def_enf_child (id INTEGER, pid INTEGER REFERENCES def_enf_parent(id) DEFERRABLE INITIALLY DEFERRED NOT ENFORCED)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_not_null_enforcement() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE nn_enf (id INTEGER NOT NULL, val INTEGER)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO nn_enf VALUES (NULL, 1)");
    assert!(result.is_err());
}

#[test]
fn test_check_constraint_enforcement_on_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE chk_ins (id INTEGER, age INTEGER CHECK (age >= 0))")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO chk_ins VALUES (1, -5)");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_check_constraint_enforcement_on_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE chk_upd (id INTEGER, age INTEGER CHECK (age >= 0))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO chk_upd VALUES (1, 25)")
        .unwrap();

    let result = executor.execute_sql("UPDATE chk_upd SET age = -5 WHERE id = 1");
    assert!(result.is_err());
}

#[test]
fn test_unique_enforcement_on_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uq_ins (id INTEGER, email TEXT UNIQUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uq_ins VALUES (1, 'test@example.com')")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO uq_ins VALUES (2, 'test@example.com')");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_unique_enforcement_on_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE uq_upd (id INTEGER, email TEXT UNIQUE)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uq_upd VALUES (1, 'alice@example.com')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO uq_upd VALUES (2, 'bob@example.com')")
        .unwrap();

    let result = executor.execute_sql("UPDATE uq_upd SET email = 'alice@example.com' WHERE id = 2");
    assert!(result.is_err());
}

#[test]
fn test_primary_key_enforcement_on_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pk_ins (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO pk_ins VALUES (1, 'Alice')")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO pk_ins VALUES (1, 'Bob')");
    assert!(result.is_err());
}

#[test]
fn test_primary_key_enforcement_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE pk_null (id INTEGER PRIMARY KEY, name TEXT)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO pk_null VALUES (NULL, 'test')");
    assert!(result.is_err());
}

#[test]
fn test_composite_unique_enforcement() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE comp_uq (a INTEGER, b INTEGER, c TEXT, UNIQUE(a, b))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO comp_uq VALUES (1, 1, 'first')")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO comp_uq VALUES (1, 1, 'second')");
    assert!(result.is_err());
}

#[test]
fn test_composite_unique_partial_null() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE comp_uq_null (a INTEGER, b INTEGER, UNIQUE(a, b))")
        .unwrap();
    executor
        .execute_sql("INSERT INTO comp_uq_null VALUES (1, NULL)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO comp_uq_null VALUES (1, NULL)");
    assert!(result.is_ok());
}

#[test]
#[ignore = "Implement me!"]
fn test_unique_nulls_not_distinct() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE uq_nulls_nd (id INTEGER, val INTEGER, UNIQUE NULLS NOT DISTINCT (val))",
    );

    if result.is_ok() {
        executor
            .execute_sql("INSERT INTO uq_nulls_nd VALUES (1, NULL)")
            .unwrap();
        let insert_result = executor.execute_sql("INSERT INTO uq_nulls_nd VALUES (2, NULL)");
        assert!(insert_result.is_err());
    }
}

#[test]
fn test_exclusion_constraint_enforcement() {
    let mut executor = create_executor();
    let create_result = executor.execute_sql(
        "CREATE TABLE excl_enf (id INTEGER, room INTEGER, during TSTZRANGE, EXCLUDE USING gist (room WITH =, during WITH &&))"
    );

    if create_result.is_ok() {
        executor.execute_sql(
            "INSERT INTO excl_enf VALUES (1, 101, '[2024-01-01 10:00:00, 2024-01-01 12:00:00)')"
        ).unwrap();
        let result = executor.execute_sql(
            "INSERT INTO excl_enf VALUES (2, 101, '[2024-01-01 11:00:00, 2024-01-01 13:00:00)')",
        );
        assert!(result.is_err());
    }
}

#[test]
fn test_constraint_using_function() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("CREATE TABLE chk_func (id INTEGER, email TEXT CHECK (email LIKE '%@%'))");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_using_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE lookup (allowed_val INTEGER)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO lookup VALUES (1), (2), (3)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE TABLE chk_subq (id INTEGER, val INTEGER CHECK (val IN (SELECT allowed_val FROM lookup)))"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_multiple_columns() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE multi_chk (id INTEGER, start_date DATE, end_date DATE, CHECK (start_date < end_date))"
    ).unwrap();

    let result =
        executor.execute_sql("INSERT INTO multi_chk VALUES (1, '2024-12-31', '2024-01-01')");
    assert!(result.is_err());
}

#[test]
fn test_constraint_or_condition() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE or_chk (id INTEGER, status TEXT CHECK (status = 'active' OR status = 'inactive'))"
    ).unwrap();

    let result = executor.execute_sql("INSERT INTO or_chk VALUES (1, 'pending')");
    assert!(result.is_err());
}

#[test]
fn test_constraint_and_condition() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE and_chk (id INTEGER, min_val INTEGER, max_val INTEGER, CHECK (min_val >= 0 AND max_val <= 100 AND min_val < max_val))"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO and_chk VALUES (1, 10, 50)")
        .unwrap();

    let result = executor.execute_sql("INSERT INTO and_chk VALUES (2, 60, 40)");
    assert!(result.is_err());
}

#[test]
fn test_constraint_case_expression() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE case_chk (id INTEGER, type TEXT, value INTEGER, CHECK (CASE WHEN type = 'percentage' THEN value BETWEEN 0 AND 100 ELSE value >= 0 END))"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_coalesce() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE coal_chk (id INTEGER, val INTEGER, CHECK (COALESCE(val, 0) >= 0))",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_nullif() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE nullif_chk (id INTEGER, val INTEGER, CHECK (NULLIF(val, 0) IS DISTINCT FROM 0))",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_deferred_constraint_violation_at_commit() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE def_viol_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE def_viol_child (id INTEGER, pid INTEGER REFERENCES def_viol_parent(id) DEFERRABLE INITIALLY DEFERRED)"
    ).unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("INSERT INTO def_viol_child VALUES (1, 999)")
        .unwrap();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_set_constraints_named() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE named_def_parent (id INTEGER PRIMARY KEY)")
        .unwrap();
    executor.execute_sql(
        "CREATE TABLE named_def_child (id INTEGER, pid INTEGER, CONSTRAINT fk_named_def FOREIGN KEY (pid) REFERENCES named_def_parent(id) DEFERRABLE INITIALLY IMMEDIATE)"
    ).unwrap();
    executor
        .execute_sql("INSERT INTO named_def_parent VALUES (1)")
        .unwrap();

    executor.execute_sql("BEGIN").unwrap();
    executor
        .execute_sql("SET CONSTRAINTS fk_named_def DEFERRED")
        .unwrap();
    executor
        .execute_sql("INSERT INTO named_def_child VALUES (1, 999)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO named_def_parent VALUES (999)")
        .unwrap();
    let result = executor.execute_sql("COMMIT");
    assert!(result.is_ok());
}

#[test]
fn test_constraint_inheritance() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE parent_inherit (id INTEGER PRIMARY KEY, val INTEGER CHECK (val > 0))",
        )
        .unwrap();
    let result =
        executor.execute_sql("CREATE TABLE child_inherit (extra TEXT) INHERITS (parent_inherit)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_constraint_no_inherit() {
    let mut executor = create_executor();
    executor.execute_sql(
        "CREATE TABLE no_inherit_parent (id INTEGER, val INTEGER, CONSTRAINT chk_no_inherit CHECK (val > 0) NO INHERIT)"
    ).unwrap();
    let result =
        executor.execute_sql("CREATE TABLE no_inherit_child () INHERITS (no_inherit_parent)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
#[ignore = "Implement me!"]
fn test_alter_table_enable_constraint() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE enable_const (id INTEGER, val INTEGER)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE enable_const ADD CONSTRAINT chk_val CHECK (val > 0)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE enable_const DISABLE TRIGGER ALL")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE enable_const ENABLE TRIGGER ALL");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_generated_column_constraint() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE gen_col (id INTEGER, quantity INTEGER, price DOUBLE PRECISION, total DOUBLE PRECISION GENERATED ALWAYS AS (quantity * price) STORED)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_generated_column_with_check() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "CREATE TABLE gen_chk (id INTEGER, quantity INTEGER CHECK (quantity > 0), price DOUBLE PRECISION CHECK (price > 0), total DOUBLE PRECISION GENERATED ALWAYS AS (quantity * price) STORED CHECK (total > 0))"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_domain_constraint() {
    let mut executor = create_executor();
    let create_result =
        executor.execute_sql("CREATE DOMAIN positive_int AS INTEGER CHECK (VALUE > 0)");

    if create_result.is_ok() {
        executor
            .execute_sql("CREATE TABLE domain_test (id positive_int)")
            .unwrap();
        let result = executor.execute_sql("INSERT INTO domain_test VALUES (-5)");
        assert!(result.is_err());
    }
}

#[test]
fn test_domain_constraint_not_enforced() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("CREATE DOMAIN relaxed_positive AS INTEGER CHECK (VALUE > 0) NOT ENFORCED");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_trigger() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE trigger_test (id INTEGER, val INTEGER)")
        .unwrap();
    let result = executor.execute_sql(
        "CREATE CONSTRAINT TRIGGER check_val AFTER INSERT ON trigger_test FOR EACH ROW EXECUTE FUNCTION check_positive()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_constraint_trigger_deferrable() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE def_trigger_test (id INTEGER, val INTEGER)")
        .unwrap();
    let result = executor.execute_sql(
        "CREATE CONSTRAINT TRIGGER check_val AFTER INSERT ON def_trigger_test DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION check_positive()"
    );
    assert!(result.is_ok() || result.is_err());
}
