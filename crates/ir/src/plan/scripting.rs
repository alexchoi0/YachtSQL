use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum RaiseLevel {
    #[default]
    Exception,
    Warning,
    Notice,
}
