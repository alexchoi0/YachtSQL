use crate::common::create_session;

#[test]
#[ignore]
fn test_grant_role_on_schema() {
    let mut session = create_session();
    session
        .execute_sql("CREATE SCHEMA myProject.myDataset")
        .unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA `myProject`.myDataset
           TO "user:raha@example-pet-store.com", "user:sasha@example-pet-store.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_multiple_roles_on_schema() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA test_schema").unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor`
           ON SCHEMA test_schema
           TO "user:alice@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_role_on_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE myTable (id INT64, name STRING)")
        .unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer` ON TABLE myTable
           TO "user:reader@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_to_group() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA group_schema").unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA group_schema
           TO "group:analytics-team@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_to_service_account() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA service_schema").unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA service_schema
           TO "serviceAccount:robot@example.iam.gserviceaccount.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_to_domain() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA domain_schema").unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA domain_schema
           TO "domain:example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_revoke_role_from_schema() {
    let mut session = create_session();
    session
        .execute_sql("CREATE SCHEMA myProject.myDataset")
        .unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.admin` ON SCHEMA `myProject`.myDataset
           FROM "group:example-team@example-pet-store.com", "serviceAccount:user@test-project.iam.gserviceaccount.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_revoke_role_from_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE TABLE revoke_table (id INT64)")
        .unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.dataViewer` ON TABLE revoke_table
           FROM "user:former-employee@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_revoke_multiple_roles() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA multi_revoke").unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor`
           ON SCHEMA multi_revoke
           FROM "user:test@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_on_fully_qualified_table() {
    let mut session = create_session();
    session
        .execute_sql("CREATE SCHEMA project.dataset")
        .unwrap();
    session
        .execute_sql("CREATE TABLE project.dataset.orders (id INT64, amount FLOAT64)")
        .unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.dataViewer` ON TABLE `project.dataset`.orders
           TO "user:analyst@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_revoke_from_multiple_users() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA shared_schema").unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.dataEditor` ON SCHEMA shared_schema
           FROM "user:user1@example.com", "user:user2@example.com", "user:user3@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_admin_role() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA admin_test").unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.admin` ON SCHEMA admin_test
           TO "user:admin@example.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_job_user_role() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA jobs_schema").unwrap();

    let result = session.execute_sql(
        r#"GRANT `roles/bigquery.jobUser` ON SCHEMA jobs_schema
           TO "serviceAccount:batch-processor@example.iam.gserviceaccount.com""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_revoke_from_special_group() {
    let mut session = create_session();
    session.execute_sql("CREATE SCHEMA public_schema").unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.dataViewer` ON SCHEMA public_schema
           FROM "specialGroup:allAuthenticatedUsers""#,
    );
    assert!(result.is_ok());
}

#[test]
#[ignore]
fn test_grant_then_revoke() {
    let mut session = create_session();
    session
        .execute_sql("CREATE SCHEMA lifecycle_schema")
        .unwrap();

    session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA lifecycle_schema
               TO "user:temp@example.com""#,
        )
        .unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.dataViewer` ON SCHEMA lifecycle_schema
           FROM "user:temp@example.com""#,
    );
    assert!(result.is_ok());
}
