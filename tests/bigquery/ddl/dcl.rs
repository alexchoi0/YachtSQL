use crate::common::create_session;

#[tokio::test(flavor = "current_thread")]
async fn test_grant_role_on_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA myProject.myDataset")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA `myProject`.myDataset
           TO "user:raha@example-pet-store.com", "user:sasha@example-pet-store.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_multiple_roles_on_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA test_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor`
           ON SCHEMA test_schema
           TO "user:alice@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_role_on_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE myTable (id INT64, name STRING)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON TABLE myTable
           TO "user:reader@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_to_group() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA group_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA group_schema
           TO "group:analytics-team@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_to_service_account() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA service_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA service_schema
           TO "serviceAccount:robot@example.iam.gserviceaccount.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_to_domain() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA domain_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA domain_schema
           TO "domain:example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_revoke_role_from_schema() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA myProject.myDataset")
        .await
        .unwrap();

    let result = session.execute_sql(
        r#"REVOKE `roles/bigquery.admin` ON SCHEMA `myProject`.myDataset
           FROM "group:example-team@example-pet-store.com", "serviceAccount:user@test-project.iam.gserviceaccount.com""#,
    ).await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_revoke_role_from_table() {
    let session = create_session();
    session
        .execute_sql("CREATE TABLE revoke_table (id INT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"REVOKE `roles/bigquery.dataViewer` ON TABLE revoke_table
           FROM "user:former-employee@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_revoke_multiple_roles() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA multi_revoke")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"REVOKE `roles/bigquery.dataViewer`, `roles/bigquery.dataEditor`
           ON SCHEMA multi_revoke
           FROM "user:test@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_on_fully_qualified_table() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA fq_test_schema")
        .await
        .unwrap();
    session
        .execute_sql("CREATE TABLE fq_test_schema.orders (id INT64, amount FLOAT64)")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON TABLE fq_test_schema.orders
           TO "user:analyst@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_revoke_from_multiple_users() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA shared_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"REVOKE `roles/bigquery.dataEditor` ON SCHEMA shared_schema
           FROM "user:user1@example.com", "user:user2@example.com", "user:user3@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_admin_role() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA admin_test")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.admin` ON SCHEMA admin_test
           TO "user:admin@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_job_user_role() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA jobs_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"GRANT `roles/bigquery.jobUser` ON SCHEMA jobs_schema
           TO "serviceAccount:batch-processor@example.iam.gserviceaccount.com""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_revoke_from_special_group() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA public_schema")
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"REVOKE `roles/bigquery.dataViewer` ON SCHEMA public_schema
           FROM "specialGroup:allAuthenticatedUsers""#,
        )
        .await;
    assert!(result.is_ok());
}

#[tokio::test(flavor = "current_thread")]
async fn test_grant_then_revoke() {
    let session = create_session();
    session
        .execute_sql("CREATE SCHEMA lifecycle_schema")
        .await
        .unwrap();

    session
        .execute_sql(
            r#"GRANT `roles/bigquery.dataViewer` ON SCHEMA lifecycle_schema
               TO "user:temp@example.com""#,
        )
        .await
        .unwrap();

    let result = session
        .execute_sql(
            r#"REVOKE `roles/bigquery.dataViewer` ON SCHEMA lifecycle_schema
           FROM "user:temp@example.com""#,
        )
        .await;
    assert!(result.is_ok());
}
