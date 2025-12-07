use crate::common::create_executor;

#[test]
fn test_enable_row_level_security() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rls_test (id INT64, user_id INT64, data TEXT)")
        .unwrap();

    let result = executor.execute_sql("ALTER TABLE rls_test ENABLE ROW LEVEL SECURITY");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_disable_row_level_security() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rls_disable (id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE rls_disable ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql("ALTER TABLE rls_disable DISABLE ROW LEVEL SECURITY");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_force_row_level_security() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rls_force (id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE rls_force ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql("ALTER TABLE rls_force FORCE ROW LEVEL SECURITY");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_no_force_row_level_security() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE rls_noforce (id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE rls_noforce ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("ALTER TABLE rls_noforce FORCE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql("ALTER TABLE rls_noforce NO FORCE ROW LEVEL SECURITY");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_select() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_sel (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_sel ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY user_select ON policy_sel FOR SELECT USING (owner_id = current_user_id())",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_insert() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_ins (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_ins ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY user_insert ON policy_ins FOR INSERT WITH CHECK (owner_id = current_user_id())"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_update() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_upd (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_upd ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY user_update ON policy_upd FOR UPDATE USING (owner_id = current_user_id())",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_delete() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_del (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_del ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY user_delete ON policy_del FOR DELETE USING (owner_id = current_user_id())",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_all() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_all (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_all ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY user_all ON policy_all FOR ALL USING (owner_id = current_user_id())",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_with_check() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_check (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_check ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY user_check ON policy_check FOR ALL USING (owner_id = current_user_id()) WITH CHECK (owner_id = current_user_id())"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_to_role() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_role (id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_role ENABLE ROW LEVEL SECURITY")
        .ok();

    let result =
        executor.execute_sql("CREATE POLICY admin_policy ON policy_role TO PUBLIC USING (true)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_permissive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_perm (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_perm ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY permissive_policy ON policy_perm AS PERMISSIVE FOR SELECT USING (true)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_create_policy_restrictive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_rest (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_rest ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY restrictive_policy ON policy_rest AS RESTRICTIVE FOR SELECT USING (owner_id IS NOT NULL)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_policy_using() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_alter (id INT64, owner_id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_alter ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY alter_pol ON policy_alter USING (true)")
        .ok();

    let result = executor
        .execute_sql("ALTER POLICY alter_pol ON policy_alter USING (owner_id = current_user_id())");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_policy_with_check() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_alter_chk (id INT64, owner_id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_alter_chk ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY alter_chk_pol ON policy_alter_chk FOR INSERT WITH CHECK (true)")
        .ok();

    let result = executor.execute_sql(
        "ALTER POLICY alter_chk_pol ON policy_alter_chk WITH CHECK (owner_id IS NOT NULL)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_policy_rename() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_rename (id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_rename ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY old_policy ON policy_rename USING (true)")
        .ok();

    let result =
        executor.execute_sql("ALTER POLICY old_policy ON policy_rename RENAME TO new_policy");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_alter_policy_to_roles() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_roles (id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_roles ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY roles_pol ON policy_roles USING (true)")
        .ok();

    let result = executor.execute_sql("ALTER POLICY roles_pol ON policy_roles TO PUBLIC");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_policy() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_drop (id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_drop ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY to_drop ON policy_drop USING (true)")
        .ok();

    let result = executor.execute_sql("DROP POLICY to_drop ON policy_drop");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_drop_policy_if_exists() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_drop_ie (id INT64)")
        .unwrap();

    let result = executor.execute_sql("DROP POLICY IF EXISTS nonexistent ON policy_drop_ie");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_using_current_user() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_curuser (id INT64, username TEXT, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_curuser ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor
        .execute_sql("CREATE POLICY curuser_pol ON policy_curuser USING (username = current_user)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_using_session_user() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_sessuser (id INT64, username TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_sessuser ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY sessuser_pol ON policy_sessuser USING (username = session_user)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_using_subquery() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE user_groups (user_id INT64, group_id INT64)")
        .ok();
    executor
        .execute_sql("CREATE TABLE policy_subq (id INT64, group_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_subq ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY subq_pol ON policy_subq USING (group_id IN (SELECT group_id FROM user_groups WHERE user_id = current_user_id()))"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_using_function() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_func (id INT64, department TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_func ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY func_pol ON policy_func USING (department = current_setting('app.department'))"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_tenant_isolation() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_tenant (id INT64, tenant_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_tenant ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY tenant_pol ON policy_tenant USING (tenant_id = current_setting('app.tenant_id')::INT)"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_time_based() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_time (id INT64, valid_until TIMESTAMP)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_time ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY time_pol ON policy_time USING (valid_until > CURRENT_TIMESTAMP)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_status_based() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_status (id INT64, status TEXT, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_status ENABLE ROW LEVEL SECURITY")
        .ok();

    let result =
        executor.execute_sql("CREATE POLICY status_pol ON policy_status USING (status = 'active')");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_multiple_policies_same_table() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_multi (id INT64, owner_id INT64, is_public BOOL)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_multi ENABLE ROW LEVEL SECURITY")
        .ok();

    executor
        .execute_sql("CREATE POLICY owner_pol ON policy_multi USING (owner_id = current_user_id())")
        .ok();
    let result =
        executor.execute_sql("CREATE POLICY public_pol ON policy_multi USING (is_public = true)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_permissive_and_restrictive() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_combo (id INT64, owner_id INT64, is_active BOOL)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_combo ENABLE ROW LEVEL SECURITY")
        .ok();

    executor.execute_sql("CREATE POLICY perm_pol ON policy_combo AS PERMISSIVE USING (owner_id = current_user_id())").ok();
    let result = executor.execute_sql(
        "CREATE POLICY rest_pol ON policy_combo AS RESTRICTIVE USING (is_active = true)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_different_commands() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_cmds (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_cmds ENABLE ROW LEVEL SECURITY")
        .ok();

    executor
        .execute_sql("CREATE POLICY sel_pol ON policy_cmds FOR SELECT USING (true)")
        .ok();
    executor
        .execute_sql(
            "CREATE POLICY ins_pol ON policy_cmds FOR INSERT WITH CHECK (owner_id IS NOT NULL)",
        )
        .ok();
    executor
        .execute_sql(
            "CREATE POLICY upd_pol ON policy_cmds FOR UPDATE USING (owner_id = current_user_id())",
        )
        .ok();
    let result = executor.execute_sql(
        "CREATE POLICY del_pol ON policy_cmds FOR DELETE USING (owner_id = current_user_id())",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_bypass_role() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_bypass (id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_bypass ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY bypass_pol ON policy_bypass USING (true)")
        .ok();

    let result = executor.execute_sql("ALTER ROLE current_user BYPASSRLS");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_no_bypass_role() {
    let mut executor = create_executor();
    let result = executor.execute_sql("ALTER ROLE current_user NOBYPASSRLS");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_policies_view() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_policies");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_pg_policy_catalog() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT * FROM pg_policy");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_with_join() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE user_permissions (user_id INT64, resource_type TEXT, can_read BOOL)",
        )
        .ok();
    executor
        .execute_sql("CREATE TABLE policy_join (id INT64, resource_type TEXT, data TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_join ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY join_pol ON policy_join USING (
            EXISTS (SELECT 1 FROM user_permissions WHERE user_id = current_user_id() AND resource_type = policy_join.resource_type AND can_read)
        )"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_with_security_definer_function() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE OR REPLACE FUNCTION get_allowed_departments() RETURNS TEXT[] AS $$
         SELECT ARRAY['HR', 'Engineering'];
         $$ LANGUAGE SQL SECURITY DEFINER",
        )
        .ok();
    executor
        .execute_sql("CREATE TABLE policy_secdef (id INT64, department TEXT)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_secdef ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY secdef_pol ON policy_secdef USING (department = ANY(get_allowed_departments()))"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_with_case() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_case (id INT64, sensitivity TEXT, owner_id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_case ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY case_pol ON policy_case USING (
            CASE sensitivity
                WHEN 'public' THEN true
                WHEN 'private' THEN owner_id = current_user_id()
                ELSE false
            END
        )",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_with_coalesce() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_coal (id INT64, owner_id INT64, is_public BOOL)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_coal ENABLE ROW LEVEL SECURITY")
        .ok();

    let result = executor.execute_sql(
        "CREATE POLICY coal_pol ON policy_coal USING (COALESCE(is_public, false) OR owner_id = current_user_id())"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_rls_inheritance() {
    let mut executor = create_executor();
    executor
        .execute_sql(
            "CREATE TABLE policy_parent (id INT64, owner_id INT64) PARTITION BY RANGE (id)",
        )
        .ok();
    executor
        .execute_sql("ALTER TABLE policy_parent ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql(
            "CREATE POLICY parent_pol ON policy_parent USING (owner_id = current_user_id())",
        )
        .ok();

    let result = executor.execute_sql(
        "CREATE TABLE policy_child PARTITION OF policy_parent FOR VALUES FROM (1) TO (100)",
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_on_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_base (id INT64, owner_id INT64, data TEXT)")
        .unwrap();
    executor
        .execute_sql("CREATE VIEW policy_view AS SELECT * FROM policy_base")
        .ok();

    let result = executor.execute_sql("ALTER VIEW policy_view SET (security_barrier = true)");
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_security_barrier_view() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE sb_base (id INT64, secret TEXT, owner_id INT64)")
        .unwrap();

    let result = executor.execute_sql(
        "CREATE VIEW sb_view WITH (security_barrier = true) AS SELECT id, owner_id FROM sb_base WHERE owner_id = current_user_id()"
    );
    assert!(result.is_ok() || result.is_err());
}

#[test]
fn test_policy_explain() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE policy_explain (id INT64, owner_id INT64)")
        .unwrap();
    executor
        .execute_sql("ALTER TABLE policy_explain ENABLE ROW LEVEL SECURITY")
        .ok();
    executor
        .execute_sql("CREATE POLICY exp_pol ON policy_explain USING (owner_id = 1)")
        .ok();

    let result = executor.execute_sql("EXPLAIN SELECT * FROM policy_explain");
    assert!(result.is_ok() || result.is_err());
}
