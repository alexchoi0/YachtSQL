use yachtsql::{YachtSQLEngine, YachtSQLSession};

#[path = "../test_helpers.rs"]
mod test_helpers;

pub use test_helpers::*;

pub fn create_session() -> YachtSQLSession {
    let engine = YachtSQLEngine::new();
    engine.create_session()
}
