pub mod clickhouse_extensions;
mod custom_statements;
mod helpers;
mod types;

pub use clickhouse_extensions::ClickHouseIndexType;
use clickhouse_extensions::ClickHouseParser;
pub use custom_statements::CustomStatementParser;
use debug_print::debug_eprintln;
pub use helpers::ParserHelpers;
use sqlparser::ast::Statement as SqlStatement;
use sqlparser::dialect::{
    BigQueryDialect, ClickHouseDialect, Dialect, GenericDialect, PostgreSqlDialect,
};
use sqlparser::parser::Parser as SqlParser;
use sqlparser::tokenizer::{Token, Tokenizer};
use types::JsonValueRewriteResult;
pub use types::{
    DialectType, JSON_VALUE_OPTIONS_PREFIX, JsonValueRewriteOptions, StandardStatement, Statement,
};
use yachtsql_core::error::{Error, Result};

use crate::sql_context::SqlWalker;
use crate::validator::StatementValidator;

pub struct Parser {
    dialect: Box<dyn Dialect>,
    dialect_type: DialectType,
}

impl Parser {
    pub fn new() -> Self {
        Self::with_dialect(DialectType::BigQuery)
    }

    pub fn with_dialect(dialect_type: DialectType) -> Self {
        let dialect: Box<dyn Dialect> = match dialect_type {
            DialectType::BigQuery => Box::new(BigQueryDialect),
            DialectType::ClickHouse => Box::new(ClickHouseDialect {}),
            DialectType::PostgreSQL => Box::new(PostgreSqlDialect {}),
        };

        Self {
            dialect,
            dialect_type,
        }
    }

    pub fn dialect_type(&self) -> DialectType {
        self.dialect_type
    }

    pub fn parse_sql(&self, sql: &str) -> Result<Vec<Statement>> {
        let statements = match self.try_parse_custom_statement(sql)? {
            Some(custom_stmt) => vec![custom_stmt],
            None => self.parse_standard_sql(sql)?,
        };

        self.validate_statements(&statements)?;

        Ok(statements)
    }

    fn parse_standard_sql(&self, sql: &str) -> Result<Vec<Statement>> {
        let (sql_without_returning, merge_returnings) = Self::strip_merge_returning_clauses(sql);

        let sql_without_exclude = Self::strip_exclude_from_window_frames(&sql_without_returning);

        let rewritten_sql = self.rewrite_json_item_methods(&sql_without_exclude)?;
        let parse_result = SqlParser::parse_sql(&*self.dialect, &rewritten_sql);

        let sql_statements = match parse_result {
            Ok(statements) => statements,
            Err(primary_err) if matches!(self.dialect_type, DialectType::BigQuery) => {
                debug_eprintln!("[parser] primary parse error: {}", primary_err);
                if let Ok(fallback) = SqlParser::parse_sql(&PostgreSqlDialect {}, &rewritten_sql) {
                    fallback
                } else if let Ok(fallback) =
                    SqlParser::parse_sql(&GenericDialect {}, &rewritten_sql)
                {
                    fallback
                } else {
                    return Err(Error::parse_error(format!(
                        "SQL parse error: {}",
                        primary_err
                    )));
                }
            }
            Err(primary_err) => {
                if let Ok(fallback) = SqlParser::parse_sql(&GenericDialect {}, &rewritten_sql) {
                    fallback
                } else {
                    return Err(Error::parse_error(format!(
                        "SQL parse error: {}",
                        primary_err
                    )));
                }
            }
        };

        if sql_statements.len() != merge_returnings.len() {
            return Err(Error::parse_error(
                "Parsed statement count does not match RETURNING metadata".to_string(),
            ));
        }

        Ok(sql_statements
            .into_iter()
            .zip(merge_returnings)
            .map(|(stmt, returning)| Statement::Standard(StandardStatement::new(stmt, returning)))
            .collect())
    }

    fn validate_statements(&self, statements: &[Statement]) -> Result<()> {
        let validator = StatementValidator::new(self.dialect_type);

        for stmt in statements {
            match stmt {
                Statement::Custom(custom) => validator.validate_custom(custom)?,
                Statement::Standard(_) => {}
            }
        }

        Ok(())
    }

    pub fn unwrap_standard(stmt: &Statement) -> &SqlStatement {
        match stmt {
            Statement::Standard(standard) => standard.ast(),
            Statement::Custom(_) => panic!("Expected Standard statement, found Custom"),
        }
    }

    fn try_parse_custom_statement(&self, sql: &str) -> Result<Option<Statement>> {
        let tokens = Tokenizer::new(&*self.dialect, sql)
            .tokenize()
            .map_err(|e| Error::parse_error(format!("Tokenization error: {}", e)))?;

        let meaningful_tokens: Vec<&Token> = tokens
            .iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect();

        if meaningful_tokens.is_empty() {
            return Ok(None);
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_get_diagnostics(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_refresh_materialized_view(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_refresh_materialized_view(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_drop_materialized_view(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_drop_materialized_view(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_create_sequence(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_create_sequence(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_alter_sequence(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_alter_sequence(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_drop_sequence(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_drop_sequence(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) =
                CustomStatementParser::parse_alter_table_restart_identity(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_create_domain(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_create_domain(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_create_type(&meaningful_tokens)
            && let Some(custom_stmt) = CustomStatementParser::parse_create_type(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_alter_domain(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_alter_domain(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_drop_domain(&meaningful_tokens)
            && let Some(custom_stmt) = CustomStatementParser::parse_drop_domain(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_drop_type(&meaningful_tokens)
            && let Some(custom_stmt) = CustomStatementParser::parse_drop_type(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_set_constraints(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_set_constraints(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_exists_table(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_exists_table(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_exists_database(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_exists_database(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_abort(&meaningful_tokens)
            && let Some(custom_stmt) = CustomStatementParser::parse_abort(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_begin(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_begin_transaction_with_deferrable(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::ClickHouse
            && ClickHouseParser::is_clickhouse_create_index(&meaningful_tokens)
            && let Some(custom_stmt) = ClickHouseParser::parse_create_index(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        Ok(None)
    }

    fn is_set_constraints(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["SET", "CONSTRAINTS"])
    }

    fn strip_merge_returning_clauses(sql: &str) -> (String, Vec<Option<String>>) {
        let statements = Self::split_sql_statements(sql);
        let mut rewritten_statements = Vec::with_capacity(statements.len());
        let mut returning_metadata = Vec::with_capacity(statements.len());

        for stmt in statements {
            let (rewritten, returning) = Self::strip_returning_from_statement(stmt);
            if rewritten.trim().is_empty() {
                continue;
            }
            rewritten_statements.push(rewritten);
            returning_metadata.push(returning);
        }

        let rewritten_sql = rewritten_statements
            .into_iter()
            .collect::<Vec<_>>()
            .join(";");

        (rewritten_sql, returning_metadata)
    }

    fn strip_exclude_from_window_frames(sql: &str) -> String {
        let mut result = sql.to_string();

        let patterns = [
            ("EXCLUDE CURRENT ROW", ""),
            ("EXCLUDE CURRENT_ROW", ""),
            ("exclude current row", ""),
            ("exclude current_row", ""),
            ("Exclude Current Row", ""),
            ("EXCLUDE NO OTHERS", ""),
            ("EXCLUDE NO_OTHERS", ""),
            ("exclude no others", ""),
            ("exclude no_others", ""),
            ("Exclude No Others", ""),
            ("EXCLUDE GROUP", ""),
            ("exclude group", ""),
            ("Exclude Group", ""),
            ("EXCLUDE TIES", ""),
            ("exclude ties", ""),
            ("Exclude Ties", ""),
        ];

        for (pattern, replacement) in &patterns {
            result = result.replace(pattern, replacement);
        }

        result
    }

    fn strip_returning_from_statement(statement: &str) -> (String, Option<String>) {
        if !Self::contains_keyword(statement, "MERGE") {
            return (statement.to_string(), None);
        }

        if let Some(returning_idx) = Self::find_keyword(statement, "RETURNING") {
            let prefix = statement[..returning_idx].trim_end();
            let clause = statement[returning_idx + "RETURNING".len()..].trim();
            let returning = if clause.is_empty() {
                None
            } else {
                Some(clause.trim_end_matches(';').trim().to_string())
            };
            return (prefix.to_string(), returning);
        }

        (statement.to_string(), None)
    }

    fn split_sql_statements(sql: &str) -> Vec<&str> {
        SqlWalker::new(sql).split_statements()
    }

    fn contains_keyword(sql: &str, keyword: &str) -> bool {
        Self::find_keyword(sql, keyword).is_some()
    }

    fn find_keyword(sql: &str, keyword: &str) -> Option<usize> {
        SqlWalker::new(sql).find_keyword(keyword)
    }

    fn matches_keyword_sequence(&self, tokens: &[&Token], keywords: &[&str]) -> bool {
        if tokens.len() < keywords.len() {
            return false;
        }

        keywords.iter().enumerate().all(|(i, &expected)| {
            matches!(tokens.get(i), Some(Token::Word(w)) if w.value.eq_ignore_ascii_case(expected))
        })
    }

    fn is_refresh_materialized_view(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["REFRESH", "MATERIALIZED", "VIEW"])
    }

    fn is_drop_materialized_view(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["DROP", "MATERIALIZED", "VIEW"])
    }

    fn is_create_sequence(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["CREATE", "SEQUENCE"])
    }

    fn is_alter_sequence(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["ALTER", "SEQUENCE"])
    }

    fn is_drop_sequence(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["DROP", "SEQUENCE"])
    }

    fn is_create_domain(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["CREATE", "DOMAIN"])
    }

    fn is_alter_domain(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["ALTER", "DOMAIN"])
    }

    fn is_drop_domain(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["DROP", "DOMAIN"])
    }

    fn is_create_type(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["CREATE", "TYPE"])
    }

    fn is_drop_type(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["DROP", "TYPE"])
    }

    fn is_exists_table(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["EXISTS", "TABLE"])
    }

    fn is_exists_database(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["EXISTS", "DATABASE"])
    }

    fn is_abort(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["ABORT"])
    }

    fn is_begin(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["BEGIN"])
    }

    fn rewrite_json_item_methods(&self, sql: &str) -> Result<String> {
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;
        let mut changed = false;

        while idx < sql.len() {
            if Self::keyword_at(sql, idx, "JSON_VALUE") {
                let keyword_len = "JSON_VALUE".len();
                let mut cursor = idx + keyword_len;

                while cursor < sql.len() {
                    let Some(ch) = Self::next_char(sql, cursor) else {
                        break;
                    };
                    if ch.is_whitespace() {
                        cursor += ch.len_utf8();
                    } else {
                        break;
                    }
                }

                if cursor < sql.len() && sql[cursor..].starts_with('(') {
                    let open_idx = cursor;
                    let close_idx = Self::find_matching_paren(sql, open_idx)?;
                    let inner = &sql[open_idx + 1..close_idx];
                    if let Some(rewrite_info) = Self::rewrite_json_value_inner(inner)? {
                        changed = true;
                        let encoded = Self::encode_json_value_options(&rewrite_info.options)?;
                        result.push_str("JSON_VALUE");
                        result.push_str(&sql[idx + keyword_len..cursor]);
                        result.push('(');
                        result.push_str(&rewrite_info.arg1);
                        result.push_str(", ");
                        result.push_str(&rewrite_info.arg2);
                        result.push_str(", ");
                        result.push_str(&encoded);
                        result.push(')');
                        idx = close_idx + 1;
                    } else {
                        result.push_str(&sql[idx..close_idx + 1]);
                        idx = close_idx + 1;
                    }
                } else {
                    result.push_str("JSON_VALUE");
                    idx += keyword_len;
                }
            } else {
                let Some(ch) = Self::next_char(sql, idx) else {
                    break;
                };
                result.push(ch);
                idx += ch.len_utf8();
            }
        }

        if changed {
            Ok(result)
        } else {
            Ok(sql.to_string())
        }
    }

    fn rewrite_json_value_inner(inner: &str) -> Result<Option<JsonValueRewriteResult>> {
        let Some(comma_idx) = Self::find_top_level_comma(inner)? else {
            return Ok(None);
        };

        let arg1 = inner[..comma_idx].trim().to_string();
        let rest = inner[comma_idx + 1..].trim_start();
        if rest.is_empty() {
            return Ok(None);
        }

        let (arg2, options) = Self::parse_second_arg_and_options(rest)?;
        if options.is_empty() {
            return Ok(None);
        }

        Ok(Some(JsonValueRewriteResult {
            arg1,
            arg2: arg2.trim().to_string(),
            options,
        }))
    }

    fn skip_whitespace(input: &str, mut idx: usize) -> usize {
        while idx < input.len() {
            let Some(ch) = Self::next_char(input, idx) else {
                break;
            };
            if ch.is_whitespace() {
                idx += ch.len_utf8();
            } else {
                break;
            }
        }
        idx
    }

    fn find_top_level_comma(inner: &str) -> Result<Option<usize>> {
        let mut idx = 0;
        let mut depth = 0;

        while idx < inner.len() {
            let Some(ch) = Self::next_char(inner, idx) else {
                break;
            };
            match ch {
                '\'' | '"' => {
                    idx = Self::advance_quoted(inner, idx, ch)?;
                }
                '(' => {
                    depth += 1;
                    idx += ch.len_utf8();
                }
                ')' => {
                    if depth > 0 {
                        depth -= 1;
                    }
                    idx += ch.len_utf8();
                }
                ',' if depth == 0 => {
                    return Ok(Some(idx));
                }
                _ => {
                    idx += ch.len_utf8();
                }
            }
        }

        Ok(None)
    }

    fn parse_second_arg_and_options(rest: &str) -> Result<(String, JsonValueRewriteOptions)> {
        let options_start = Self::find_options_start(rest)?;
        let (arg2_part, options_part) = rest.split_at(options_start);

        let arg2 = arg2_part.trim().to_string();
        let mut options = JsonValueRewriteOptions::default();
        let mut idx = 0;
        let options_str = options_part.trim();

        while idx < options_str.len() {
            let Some(ch) = Self::next_char(options_str, idx) else {
                break;
            };
            if ch.is_whitespace() {
                idx += ch.len_utf8();
                continue;
            }

            if Self::starts_with_ci(options_str, idx, "RETURNING") {
                idx += "RETURNING".len();
                while idx < options_str.len() {
                    let Some(ch) = Self::next_char(options_str, idx) else {
                        break;
                    };
                    if ch.is_whitespace() {
                        idx += ch.len_utf8();
                    } else {
                        break;
                    }
                }
                let (type_clause, new_idx) = Self::consume_until_next_clause(options_str, idx)?;
                options.returning = Some(type_clause.trim().to_string());
                idx = new_idx;
                continue;
            }

            if Self::starts_with_ci(options_str, idx, "NULL ON EMPTY") {
                options.on_empty = Some("NULL".to_string());
                idx += "NULL ON EMPTY".len();
                continue;
            }

            if Self::starts_with_ci(options_str, idx, "NULL ON ERROR") {
                options.on_error = Some("NULL".to_string());
                idx += "NULL ON ERROR".len();
                continue;
            }

            if Self::starts_with_ci(options_str, idx, "ERROR ON ERROR") {
                options.on_error = Some("ERROR".to_string());
                idx += "ERROR ON ERROR".len();
                continue;
            }

            if Self::starts_with_ci(options_str, idx, "ERROR ON EMPTY") {
                options.on_empty = Some("ERROR".to_string());
                idx += "ERROR ON EMPTY".len();
                continue;
            }

            if Self::starts_with_ci(options_str, idx, "DEFAULT") {
                idx += "DEFAULT".len();
                idx = Self::skip_whitespace(options_str, idx);

                let (expr_sql, new_idx) = Self::consume_until_next_clause(options_str, idx)?;
                let expression = expr_sql.trim();
                if expression.is_empty() {
                    return Err(Error::invalid_query(
                        "JSON_VALUE DEFAULT requires an expression".to_string(),
                    ));
                }

                idx = new_idx;
                idx = Self::skip_whitespace(options_str, idx);

                if Self::starts_with_ci(options_str, idx, "ON EMPTY") {
                    options.on_empty = Some("DEFAULT".to_string());
                    options.on_empty_default = Some(expression.to_string());
                    idx += "ON EMPTY".len();
                    continue;
                }

                if Self::starts_with_ci(options_str, idx, "ON ERROR") {
                    options.on_error = Some("DEFAULT".to_string());
                    options.on_error_default = Some(expression.to_string());
                    idx += "ON ERROR".len();
                    continue;
                }

                return Err(Error::invalid_query(
                    "Expected ON EMPTY or ON ERROR after DEFAULT expression".to_string(),
                ));
            }

            if Self::starts_with_ci(options_str, idx, "ON EMPTY") {
                idx += "ON EMPTY".len();
                idx = Self::skip_whitespace(options_str, idx);

                if Self::starts_with_ci(options_str, idx, "NULL") {
                    options.on_empty = Some("NULL".to_string());
                    idx += "NULL".len();
                    continue;
                }

                if Self::starts_with_ci(options_str, idx, "ERROR") {
                    options.on_empty = Some("ERROR".to_string());
                    idx += "ERROR".len();
                    continue;
                }

                if Self::starts_with_ci(options_str, idx, "DEFAULT") {
                    options.on_empty = Some("DEFAULT".to_string());
                    idx += "DEFAULT".len();
                    let (expr_sql, new_idx) = Self::consume_until_next_clause(options_str, idx)?;
                    let expression = expr_sql.trim();
                    if expression.is_empty() {
                        return Err(Error::invalid_query(
                            "JSON_VALUE ON EMPTY DEFAULT requires an expression".to_string(),
                        ));
                    }
                    options.on_empty_default = Some(expression.to_string());
                    idx = new_idx;
                    continue;
                }

                return Err(Error::invalid_query(
                    "Expected NULL, ERROR, or DEFAULT after ON EMPTY".to_string(),
                ));
            }

            if Self::starts_with_ci(options_str, idx, "ON ERROR") {
                idx += "ON ERROR".len();
                idx = Self::skip_whitespace(options_str, idx);

                if Self::starts_with_ci(options_str, idx, "NULL") {
                    options.on_error = Some("NULL".to_string());
                    idx += "NULL".len();
                    continue;
                }

                if Self::starts_with_ci(options_str, idx, "ERROR") {
                    options.on_error = Some("ERROR".to_string());
                    idx += "ERROR".len();
                    continue;
                }

                if Self::starts_with_ci(options_str, idx, "DEFAULT") {
                    options.on_error = Some("DEFAULT".to_string());
                    idx += "DEFAULT".len();
                    let (expr_sql, new_idx) = Self::consume_until_next_clause(options_str, idx)?;
                    let expression = expr_sql.trim();
                    if expression.is_empty() {
                        return Err(Error::invalid_query(
                            "JSON_VALUE ON ERROR DEFAULT requires an expression".to_string(),
                        ));
                    }
                    options.on_error_default = Some(expression.to_string());
                    idx = new_idx;
                    continue;
                }

                return Err(Error::invalid_query(
                    "Expected NULL, ERROR, or DEFAULT after ON ERROR".to_string(),
                ));
            }

            return Ok((arg2, JsonValueRewriteOptions::default()));
        }

        Ok((arg2, options))
    }

    fn find_options_start(rest: &str) -> Result<usize> {
        let mut idx = 0;
        let mut depth = 0;

        while idx < rest.len() {
            let Some(ch) = Self::next_char(rest, idx) else {
                break;
            };
            match ch {
                '\'' | '"' => {
                    idx = Self::advance_quoted(rest, idx, ch)?;
                }
                '(' => {
                    depth += 1;
                    idx += ch.len_utf8();
                }
                ')' => {
                    if depth > 0 {
                        depth -= 1;
                    }
                    idx += ch.len_utf8();
                }
                _ => {
                    if depth == 0
                        && (Self::starts_with_ci(rest, idx, "RETURNING")
                            || Self::starts_with_ci(rest, idx, "NULL ON")
                            || Self::starts_with_ci(rest, idx, "ERROR ON")
                            || Self::starts_with_ci(rest, idx, "DEFAULT")
                            || Self::starts_with_ci(rest, idx, "ON EMPTY")
                            || Self::starts_with_ci(rest, idx, "ON ERROR"))
                    {
                        return Ok(idx);
                    }
                    idx += ch.len_utf8();
                }
            }
        }

        Ok(rest.len())
    }

    fn consume_until_next_clause(rest: &str, start: usize) -> Result<(String, usize)> {
        let patterns = [
            "NULL ON EMPTY",
            "NULL ON ERROR",
            "ERROR ON EMPTY",
            "ERROR ON ERROR",
            "DEFAULT",
            "ON EMPTY",
            "ON ERROR",
            "RETURNING",
        ];

        let mut end_idx = rest.len();

        for pattern in patterns {
            if let Some(pos) = Self::find_case_insensitive_from(rest, start, pattern)
                && pos > start
                && pos < end_idx
            {
                end_idx = pos;
            }
        }

        let value = rest[start..end_idx].to_string();
        Ok((value, end_idx))
    }

    fn encode_json_value_options(opts: &JsonValueRewriteOptions) -> Result<String> {
        let mut parts = Vec::new();
        if let Some(ret) = &opts.returning {
            parts.push(format!("T={}", ret));
        }
        if let Some(on_empty) = &opts.on_empty {
            if on_empty.eq_ignore_ascii_case("DEFAULT") {
                parts.push("E=DEFAULT".to_string());
                if let Some(expr) = &opts.on_empty_default {
                    let escaped = expr.replace('\'', "''");
                    parts.push(format!("ED={}", escaped));
                }
            } else {
                parts.push(format!("E={}", on_empty));
            }
        }
        if let Some(on_error) = &opts.on_error {
            if on_error.eq_ignore_ascii_case("DEFAULT") {
                parts.push("O=DEFAULT".to_string());
                if let Some(expr) = &opts.on_error_default {
                    let escaped = expr.replace('\'', "''");
                    parts.push(format!("OD={}", escaped));
                }
            } else {
                parts.push(format!("O={}", on_error));
            }
        }

        let payload = parts.join(";");
        Ok(format!("'{}{}'", JSON_VALUE_OPTIONS_PREFIX, payload))
    }

    fn keyword_at(sql: &str, start: usize, keyword: &str) -> bool {
        sql.get(start..start + keyword.len())
            .map(|sub| sub.eq_ignore_ascii_case(keyword))
            .unwrap_or(false)
            && Self::is_ident_boundary(sql, start, keyword)
    }

    fn starts_with_ci(s: &str, idx: usize, pattern: &str) -> bool {
        s.get(idx..idx + pattern.len())
            .map(|sub| sub.eq_ignore_ascii_case(pattern))
            .unwrap_or(false)
    }

    fn find_case_insensitive_from(s: &str, start: usize, needle: &str) -> Option<usize> {
        if start >= s.len() {
            return None;
        }
        let haystack = s[start..].to_ascii_uppercase();
        let needle_upper = needle.to_ascii_uppercase();
        haystack.find(&needle_upper).map(|offset| start + offset)
    }

    fn next_char(s: &str, idx: usize) -> Option<char> {
        s.get(idx..)?.chars().next()
    }

    fn advance_quoted(sql: &str, idx: usize, quote: char) -> Result<usize> {
        let mut cursor = idx + quote.len_utf8();
        while cursor < sql.len() {
            let ch = Self::next_char(sql, cursor).ok_or_else(|| {
                Error::parse_error("Invalid character boundary in string literal")
            })?;
            cursor += ch.len_utf8();
            if ch == quote {
                if cursor < sql.len() && Self::next_char(sql, cursor).is_some_and(|c| c == quote) {
                    cursor += quote.len_utf8();
                    continue;
                }
                return Ok(cursor);
            }
        }
        Err(Error::parse_error(
            "Unterminated string literal in JSON_VALUE clause".to_string(),
        ))
    }

    fn find_matching_paren(sql: &str, open_idx: usize) -> Result<usize> {
        SqlWalker::new(sql).find_matching_paren(open_idx)
    }

    fn is_ident_boundary(sql: &str, start: usize, keyword: &str) -> bool {
        let end = start + keyword.len();

        if start > 0
            && let Some(prev) = sql[..start].chars().next_back()
            && (prev.is_ascii_alphanumeric() || prev == '_')
        {
            return false;
        }

        if end < sql.len()
            && let Some(next) = sql[end..].chars().next()
            && (next.is_ascii_alphanumeric() || next == '_')
        {
            return false;
        }

        true
    }
}

impl Default for Parser {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::validator::CustomStatement;

    #[test]
    fn test_parse_refresh_materialized_view() {
        let parser = Parser::with_dialect(DialectType::PostgreSQL);
        let sql = "REFRESH MATERIALIZED VIEW my_view";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Custom(CustomStatement::RefreshMaterializedView { name, concurrently }) => {
                assert_eq!(name.to_string(), "my_view");
                assert!(!concurrently);
            }
            _ => panic!("Expected RefreshMaterializedView statement"),
        }
    }

    #[test]
    fn test_parse_refresh_materialized_view_with_comment() {
        let parser = Parser::with_dialect(DialectType::PostgreSQL);
        let sql = "/* This is a comment */ REFRESH MATERIALIZED VIEW my_view";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Custom(CustomStatement::RefreshMaterializedView { name, concurrently }) => {
                assert_eq!(name.to_string(), "my_view");
                assert!(!concurrently);
            }
            _ => panic!("Expected RefreshMaterializedView statement"),
        }
    }

    #[test]
    fn test_parse_drop_materialized_view() {
        let parser = Parser::with_dialect(DialectType::PostgreSQL);
        let sql = "DROP MATERIALIZED VIEW my_view";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Custom(CustomStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            }) => {
                assert_eq!(name.to_string(), "my_view");
                assert!(!if_exists);
                assert!(!cascade);
            }
            _ => panic!("Expected DropMaterializedView statement"),
        }
    }

    #[test]
    fn test_parse_drop_materialized_view_if_exists() {
        let parser = Parser::with_dialect(DialectType::PostgreSQL);
        let sql = "DROP MATERIALIZED VIEW IF EXISTS my_view";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Custom(CustomStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            }) => {
                assert_eq!(name.to_string(), "my_view");
                assert!(if_exists);
                assert!(!cascade);
            }
            _ => panic!("Expected DropMaterializedView statement"),
        }
    }

    #[test]
    fn test_parse_drop_materialized_view_cascade() {
        let parser = Parser::with_dialect(DialectType::PostgreSQL);
        let sql = "DROP MATERIALIZED VIEW my_view CASCADE";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Custom(CustomStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            }) => {
                assert_eq!(name.to_string(), "my_view");
                assert!(!if_exists);
                assert!(cascade);
            }
            _ => panic!("Expected DropMaterializedView statement"),
        }
    }

    #[test]
    fn test_parse_drop_materialized_view_with_comment() {
        let parser = Parser::with_dialect(DialectType::PostgreSQL);
        let sql = "/* comment */ DROP MATERIALIZED VIEW IF EXISTS my_view CASCADE";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Custom(CustomStatement::DropMaterializedView {
                name,
                if_exists,
                cascade,
            }) => {
                assert_eq!(name.to_string(), "my_view");
                assert!(if_exists);
                assert!(cascade);
            }
            _ => panic!("Expected DropMaterializedView statement"),
        }
    }

    #[test]
    fn test_refresh_materialized_view_wrong_dialect() {
        let parser = Parser::with_dialect(DialectType::BigQuery);
        let sql = "REFRESH MATERIALIZED VIEW my_view";
        let result = parser.parse_sql(sql);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PostgreSQL dialect")
        );
    }

    #[test]
    fn test_drop_materialized_view_wrong_dialect() {
        let parser = Parser::with_dialect(DialectType::BigQuery);
        let sql = "DROP MATERIALIZED VIEW my_view";
        let result = parser.parse_sql(sql);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("PostgreSQL dialect")
        );
    }

    #[test]
    fn test_standard_statement_wrapped() {
        let parser = Parser::new();
        let sql = "SELECT * FROM users";
        let statements = parser.parse_sql(sql).unwrap();

        assert_eq!(statements.len(), 1);
        match &statements[0] {
            Statement::Standard(_) => {}
            _ => panic!("Expected Standard statement"),
        }
    }
}
