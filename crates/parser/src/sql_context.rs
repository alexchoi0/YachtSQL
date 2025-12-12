use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone, Default)]
pub struct SqlContext {
    in_single: bool,

    in_double: bool,

    in_backtick: bool,

    in_bracket: bool,

    in_line_comment: bool,

    in_block_comment: usize,

    in_dollar_quoted: Option<String>,
}

impl SqlContext {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_in_string_or_comment(&self) -> bool {
        self.in_single
            || self.in_double
            || self.in_backtick
            || self.in_bracket
            || self.in_line_comment
            || self.in_block_comment > 0
            || self.in_dollar_quoted.is_some()
    }

    pub fn is_in_dollar_quoted(&self) -> bool {
        self.in_dollar_quoted.is_some()
    }

    pub fn enter_dollar_quoted(&mut self, tag: String) {
        self.in_dollar_quoted = Some(tag);
    }

    pub fn exit_dollar_quoted(&mut self) {
        self.in_dollar_quoted = None;
    }

    pub fn dollar_quote_tag(&self) -> Option<&str> {
        self.in_dollar_quoted.as_deref()
    }

    pub fn is_in_code(&self) -> bool {
        !self.is_in_string_or_comment()
    }

    pub fn process_char(&mut self, ch: char, peek: Option<char>) -> bool {
        if self.in_line_comment {
            if ch == '\n' {
                self.in_line_comment = false;
            }
            return true;
        }

        if self.in_block_comment > 0 {
            if ch == '*' && peek == Some('/') {
                self.in_block_comment -= 1;
            } else if ch == '/' && peek == Some('*') {
                self.in_block_comment += 1;
            }
            return true;
        }

        if ch == '-'
            && peek == Some('-')
            && !self.in_single
            && !self.in_double
            && !self.in_backtick
            && !self.in_bracket
        {
            self.in_line_comment = true;
            return true;
        }

        if ch == '/'
            && peek == Some('*')
            && !self.in_single
            && !self.in_double
            && !self.in_backtick
            && !self.in_bracket
        {
            self.in_block_comment += 1;
            return true;
        }

        match ch {
            '\'' if !self.in_double && !self.in_backtick && !self.in_bracket => {
                self.in_single = !self.in_single;
            }
            '"' if !self.in_single && !self.in_backtick && !self.in_bracket => {
                self.in_double = !self.in_double;
            }
            '`' if !self.in_single && !self.in_double && !self.in_bracket => {
                self.in_backtick = !self.in_backtick;
            }
            '[' if !self.in_single && !self.in_double && !self.in_backtick => {
                self.in_bracket = true;
            }
            ']' if self.in_bracket => {
                self.in_bracket = false;
            }
            _ => return false,
        }

        false
    }
}

pub struct SqlWalker<'a> {
    sql: &'a str,
}

impl<'a> SqlWalker<'a> {
    pub fn new(sql: &'a str) -> Self {
        Self { sql }
    }

    pub fn split_statements(&self) -> Vec<&'a str> {
        let mut statements = Vec::new();
        let mut start = 0usize;
        let mut idx = 0usize;
        let bytes = self.sql.as_bytes();
        let mut context = SqlContext::new();

        while idx < bytes.len() {
            let ch = bytes[idx] as char;
            let peek = bytes.get(idx + 1).map(|&b| b as char);

            if context.is_in_dollar_quoted() {
                if let Some(tag) = context.dollar_quote_tag() {
                    let delimiter = format!("${}$", tag);
                    if self.sql[idx..].starts_with(&delimiter) {
                        context.exit_dollar_quoted();
                        idx += delimiter.len();
                        continue;
                    }
                }
                idx += 1;
                continue;
            }

            if ch == '$'
                && context.is_in_code()
                && let Some((tag, delimiter_len)) = Self::try_parse_dollar_quote(&self.sql[idx..])
            {
                context.enter_dollar_quoted(tag);
                idx += delimiter_len;
                continue;
            }

            if context.process_char(ch, peek) {
                if (ch == '-' && peek == Some('-')) || (ch == '/' && peek == Some('*')) {
                    idx += 2;
                } else {
                    idx += 1;
                }
                continue;
            }

            if ch == ';' && context.is_in_code() {
                let stmt = self.sql[start..idx].trim();
                if !stmt.is_empty() {
                    statements.push(stmt);
                }
                start = idx + 1;
            }

            idx += 1;
        }

        if start < self.sql.len() {
            let stmt = self.sql[start..].trim();
            if !stmt.is_empty() {
                statements.push(stmt);
            }
        }

        if statements.is_empty() && !self.sql.trim().is_empty() {
            vec![self.sql.trim()]
        } else {
            statements
        }
    }

    fn try_parse_dollar_quote(s: &str) -> Option<(String, usize)> {
        if !s.starts_with('$') {
            return None;
        }

        let bytes = s.as_bytes();
        let mut tag_end = 1;

        while tag_end < bytes.len() {
            let b = bytes[tag_end];
            if b == b'$' {
                let tag = &s[1..tag_end];
                if tag.is_empty() || Self::is_valid_dollar_tag(tag) {
                    return Some((tag.to_string(), tag_end + 1));
                }
                return None;
            }
            if !Self::is_dollar_tag_char(b as char, tag_end == 1) {
                return None;
            }
            tag_end += 1;
        }

        None
    }

    fn is_valid_dollar_tag(tag: &str) -> bool {
        if tag.is_empty() {
            return true;
        }
        let mut chars = tag.chars();
        let first = chars.next().unwrap();
        if !first.is_ascii_alphabetic() && first != '_' {
            return false;
        }
        chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }

    fn is_dollar_tag_char(ch: char, is_first: bool) -> bool {
        if is_first {
            ch.is_ascii_alphabetic() || ch == '_'
        } else {
            ch.is_ascii_alphanumeric() || ch == '_'
        }
    }

    pub fn find_keyword(&self, keyword: &str) -> Option<usize> {
        let keyword_upper = keyword.to_ascii_uppercase();
        let keyword_len = keyword_upper.len();
        let bytes = self.sql.as_bytes();
        let mut idx = 0usize;
        let mut context = SqlContext::new();

        while idx < bytes.len() {
            let ch = bytes[idx] as char;
            let peek = bytes.get(idx + 1).map(|&b| b as char);

            if context.is_in_dollar_quoted() {
                if let Some(tag) = context.dollar_quote_tag() {
                    let delimiter = format!("${}$", tag);
                    if self.sql[idx..].starts_with(&delimiter) {
                        context.exit_dollar_quoted();
                        idx += delimiter.len();
                        continue;
                    }
                }
                idx += 1;
                continue;
            }

            if ch == '$'
                && context.is_in_code()
                && let Some((tag, delimiter_len)) = Self::try_parse_dollar_quote(&self.sql[idx..])
            {
                context.enter_dollar_quoted(tag);
                idx += delimiter_len;
                continue;
            }

            if context.process_char(ch, peek) {
                if peek.is_some()
                    && (ch == '-' || ch == '/' || ch == '*')
                    && ((ch == '-' && peek == Some('-'))
                        || (ch == '/' && peek == Some('*'))
                        || (ch == '*' && peek == Some('/')))
                {
                    idx += 2;
                } else {
                    idx += 1;
                }
                continue;
            }

            if context.is_in_code()
                && let Some(substr) = self.sql.get(idx..idx + keyword_len)
                && substr.eq_ignore_ascii_case(keyword)
            {
                let before = if idx == 0 {
                    ' '
                } else {
                    self.sql[..idx].chars().next_back().unwrap_or(' ')
                };
                let after = if idx + keyword_len >= self.sql.len() {
                    ' '
                } else {
                    self.sql[idx + keyword_len..].chars().next().unwrap_or(' ')
                };

                if !Self::is_identifier_part(before) && !Self::is_identifier_part(after) {
                    return Some(idx);
                }
            }

            idx += 1;
        }

        None
    }

    pub fn contains_keyword(&self, keyword: &str) -> bool {
        self.find_keyword(keyword).is_some()
    }

    pub fn find_matching_paren(&self, open_idx: usize) -> Result<usize> {
        if open_idx >= self.sql.len() || self.sql.as_bytes()[open_idx] != b'(' {
            return Err(Error::parse_error(
                "Invalid opening parenthesis index".to_string(),
            ));
        }

        let mut cursor = open_idx + 1;
        let mut depth = 1;
        let mut context = SqlContext::new();

        while cursor < self.sql.len() {
            let ch = self.sql.as_bytes()[cursor] as char;
            let peek = self.sql.as_bytes().get(cursor + 1).map(|&b| b as char);

            if context.is_in_dollar_quoted() {
                if let Some(tag) = context.dollar_quote_tag() {
                    let delimiter = format!("${}$", tag);
                    if self.sql[cursor..].starts_with(&delimiter) {
                        context.exit_dollar_quoted();
                        cursor += delimiter.len();
                        continue;
                    }
                }
                cursor += 1;
                continue;
            }

            if ch == '$'
                && context.is_in_code()
                && let Some((tag, delimiter_len)) =
                    Self::try_parse_dollar_quote(&self.sql[cursor..])
            {
                context.enter_dollar_quoted(tag);
                cursor += delimiter_len;
                continue;
            }

            context.process_char(ch, peek);

            if context.is_in_code() {
                match ch {
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Ok(cursor);
                        }
                    }
                    _ => {}
                }
            }

            cursor += 1;
        }

        Err(Error::parse_error(
            "Unmatched opening parenthesis".to_string(),
        ))
    }

    fn is_identifier_part(ch: char) -> bool {
        ch.is_alphanumeric() || ch == '_' || ch == '$'
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_statements_simple() {
        let walker = SqlWalker::new("SELECT 1; SELECT 2");
        let stmts = walker.split_statements();
        assert_eq!(stmts, vec!["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn test_split_statements_with_string() {
        let walker = SqlWalker::new("SELECT 'a;b'; SELECT 2");
        let stmts = walker.split_statements();
        assert_eq!(stmts, vec!["SELECT 'a;b'", "SELECT 2"]);
    }

    #[test]
    fn test_split_statements_with_comment() {
        let walker = SqlWalker::new("SELECT 1 -- comment;\nSELECT 2");
        let stmts = walker.split_statements();
        assert_eq!(stmts, vec!["SELECT 1 -- comment;\nSELECT 2"]);
    }

    #[test]
    fn test_split_statements_with_block_comment() {
        let walker = SqlWalker::new("SELECT 1 /* comment; */ ; SELECT 2");
        let stmts = walker.split_statements();
        assert_eq!(stmts, vec!["SELECT 1 /* comment; */", "SELECT 2"]);
    }

    #[test]
    fn test_find_keyword_simple() {
        let walker = SqlWalker::new("SELECT * FROM table WHERE id = 1");
        assert_eq!(walker.find_keyword("SELECT"), Some(0));
        assert_eq!(walker.find_keyword("FROM"), Some(9));
        assert_eq!(walker.find_keyword("WHERE"), Some(20));
    }

    #[test]
    fn test_find_keyword_in_string() {
        let walker = SqlWalker::new("SELECT 'SELECT' FROM table");
        assert_eq!(walker.find_keyword("SELECT"), Some(0));
        assert_eq!(walker.find_keyword("FROM"), Some(16));
    }

    #[test]
    fn test_find_keyword_case_insensitive() {
        let walker = SqlWalker::new("select * from TABLE");
        assert_eq!(walker.find_keyword("SELECT"), Some(0));
        assert_eq!(walker.find_keyword("FROM"), Some(9));
    }

    #[test]
    fn test_find_keyword_word_boundary() {
        let walker = SqlWalker::new("SELECT * FROM my_from_table");
        assert_eq!(walker.find_keyword("FROM"), Some(9));
        assert!(walker.find_keyword("ROM").is_none());
    }

    #[test]
    fn test_contains_keyword() {
        let walker = SqlWalker::new("SELECT * FROM table");
        assert!(walker.contains_keyword("SELECT"));
        assert!(walker.contains_keyword("FROM"));
        assert!(!walker.contains_keyword("WHERE"));
    }

    #[test]
    fn test_find_matching_paren_simple() {
        let walker = SqlWalker::new("(1 + 2)");
        assert_eq!(walker.find_matching_paren(0).unwrap(), 6);
    }

    #[test]
    fn test_find_matching_paren_nested() {
        let walker = SqlWalker::new("((1 + 2) * 3)");
        assert_eq!(walker.find_matching_paren(0).unwrap(), 12);
        assert_eq!(walker.find_matching_paren(1).unwrap(), 7);
    }

    #[test]
    fn test_find_matching_paren_with_string() {
        let walker = SqlWalker::new("('a(b)c')");
        assert_eq!(walker.find_matching_paren(0).unwrap(), 8);
    }

    #[test]
    fn test_context_tracking() {
        let mut ctx = SqlContext::new();
        assert!(ctx.is_in_code());

        ctx.process_char('\'', None);
        assert!(!ctx.is_in_code());

        ctx.process_char('\'', None);
        assert!(ctx.is_in_code());
    }

    #[test]
    fn test_nested_block_comments() {
        let walker = SqlWalker::new("SELECT /* /* nested */ */ 1");
        assert_eq!(walker.find_keyword("SELECT"), Some(0));
    }
}
