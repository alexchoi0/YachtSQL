use super::codes::SUCCESSFUL_COMPLETION;
use super::sqlstate::SqlState;

#[derive(Debug, Clone)]
pub struct DiagnosticArea {
    sqlstate: SqlState,

    message_text: Option<String>,

    row_count: Option<usize>,

    detail: Option<String>,

    hint: Option<String>,

    statement_position: Option<usize>,
}

impl DiagnosticArea {
    pub fn success() -> Self {
        Self::new(SUCCESSFUL_COMPLETION, None)
    }

    pub fn error(sqlstate: SqlState, message: impl Into<String>) -> Self {
        Self::new(sqlstate, Some(message.into()))
    }

    fn new(sqlstate: SqlState, message_text: Option<String>) -> Self {
        Self {
            sqlstate,
            message_text,
            row_count: None,
            detail: None,
            hint: None,
            statement_position: None,
        }
    }

    pub fn with_row_count(mut self, count: usize) -> Self {
        self.row_count = Some(count);
        self
    }

    pub fn with_detail(mut self, detail: impl Into<String>) -> Self {
        self.detail = Some(detail.into());
        self
    }

    pub fn with_hint(mut self, hint: impl Into<String>) -> Self {
        self.hint = Some(hint.into());
        self
    }

    pub fn with_message(mut self, message: impl Into<String>) -> Self {
        self.message_text = Some(message.into());
        self
    }

    pub fn with_position(mut self, position: usize) -> Self {
        self.statement_position = Some(position);
        self
    }

    pub fn sqlstate(&self) -> SqlState {
        self.sqlstate
    }

    pub fn message_text(&self) -> Option<&str> {
        self.message_text.as_deref()
    }

    pub fn row_count(&self) -> Option<usize> {
        self.row_count
    }

    pub fn detail(&self) -> Option<&str> {
        self.detail.as_deref()
    }

    pub fn hint(&self) -> Option<&str> {
        self.hint.as_deref()
    }

    pub fn statement_position(&self) -> Option<usize> {
        self.statement_position
    }

    pub fn is_success(&self) -> bool {
        self.sqlstate.is_success()
    }

    pub fn is_warning(&self) -> bool {
        self.sqlstate.is_warning()
    }

    pub fn is_no_data(&self) -> bool {
        self.sqlstate.is_no_data()
    }

    pub fn is_error(&self) -> bool {
        self.sqlstate.is_error()
    }
}

impl Default for DiagnosticArea {
    fn default() -> Self {
        Self::success()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostics::codes::DIVISION_BY_ZERO;

    #[test]
    fn test_diagnostic_area_success() {
        let diag = DiagnosticArea::success();
        assert!(diag.is_success());
        assert!(!diag.is_error());
        assert_eq!(diag.sqlstate(), SUCCESSFUL_COMPLETION);
    }

    #[test]
    fn test_diagnostic_area_error() {
        let diag = DiagnosticArea::error(DIVISION_BY_ZERO, "Cannot divide by zero");
        assert!(!diag.is_success());
        assert!(diag.is_error());
        assert_eq!(diag.sqlstate(), DIVISION_BY_ZERO);
        assert_eq!(diag.message_text(), Some("Cannot divide by zero"));
    }

    #[test]
    fn test_diagnostic_area_builder() {
        let diag = DiagnosticArea::error(DIVISION_BY_ZERO, "Cannot divide by zero")
            .with_detail("Attempted to divide 10 by 0")
            .with_hint("Check the divisor value")
            .with_position(42);

        assert_eq!(diag.detail(), Some("Attempted to divide 10 by 0"));
        assert_eq!(diag.hint(), Some("Check the divisor value"));
        assert_eq!(diag.statement_position(), Some(42));
    }
}
