#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(clippy::unnecessary_unwrap)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::wildcard_enum_match_arm)]

use yachtsql::{DialectType, QueryExecutor};

fn main() {
    let mut executor = QueryExecutor::with_dialect(DialectType::BigQuery);

    executor
        .execute_sql("CREATE TABLE users (id INT64, name STRING)")
        .unwrap();
    executor
        .execute_sql("CREATE TABLE scores (user_id INT64, score INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO users VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')")
        .unwrap();
    executor
        .execute_sql("INSERT INTO scores VALUES (1, 100), (2, NULL), (3, 150)")
        .unwrap();

    let _result = executor.execute_sql("SELECT u.name, s.score FROM users u JOIN scores s ON u.id = s.user_id ORDER BY s.score DESC");
}
