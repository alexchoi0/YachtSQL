mod clickhouse_extensions;
mod custom_statements;
mod helpers;
mod types;

use clickhouse_extensions::ClickHouseParser;
pub use clickhouse_extensions::{ClickHouseExplainVariant, ClickHouseIndexType};
pub use custom_statements::CustomStatementParser;
use debug_print::debug_eprintln;
pub use helpers::ParserHelpers;
use lazy_static::lazy_static;
use regex::Regex;
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

use crate::sql_context::{SqlContext, SqlWalker};
use crate::validator::StatementValidator;

lazy_static! {
    static ref RE_FINAL: Regex = Regex::new(r"(?i)\bFINAL\b").unwrap();
    static ref RE_SETTINGS: Regex =
        Regex::new(r"(?i)\bSETTINGS\s+\w+\s*=\s*'?[^']*'?(\s*,\s*\w+\s*=\s*'?[^']*'?)*").unwrap();
    static ref RE_GLOBAL_IN: Regex = Regex::new(r"(?i)\bGLOBAL\s+IN\b").unwrap();
    static ref RE_NO_KEY_UPDATE: Regex = Regex::new(r"(?i)\bFOR\s+NO\s+KEY\s+UPDATE\b").unwrap();
    static ref RE_KEY_SHARE: Regex = Regex::new(r"(?i)\bFOR\s+KEY\s+SHARE\b").unwrap();
    static ref RE_SKIP_LOCKED: Regex = Regex::new(r"(?i)\bSKIP\s+LOCKED\b").unwrap();
    static ref RE_FK_MATCH: Regex = Regex::new(r"(?i)\bMATCH\s+(FULL|SIMPLE|PARTIAL)\b").unwrap();
    static ref RE_NO_INHERIT: Regex = Regex::new(r"(?i)\bNO\s+INHERIT\b").unwrap();
    static ref RE_VIEW_TABLE_FUNC: Regex =
        Regex::new(r"(?i)\bview\s*\(\s*(SELECT\s+[^)]+)\)").unwrap();
    static ref RE_INPUT_TABLE_FUNC: Regex =
        Regex::new(r"(?i)\binput\s*\(\s*'([^']+)'\s*\)\s+FORMAT\s+Values\s+(.*?)(?:;|$)").unwrap();
    static ref RE_IN_TABLE: Regex =
        Regex::new(r"(?i)\bIN\s+([a-zA-Z_][a-zA-Z0-9_]*)").unwrap();
    static ref RE_PROC_PREFIX: Regex = Regex::new(
        r"(?is)(CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+\S+\s*\([^)]*\)\s*)AS\s+(\$[a-zA-Z_0-9]*\$)"
    )
    .unwrap();
    static ref RE_PROC_LANGUAGE: Regex = Regex::new(r"(?i)^\s*(LANGUAGE\s+\w+)").unwrap();
    static ref RE_BQ_PROC_BEGIN: Regex = Regex::new(
        r"(?is)(CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+\S+\s*\([^)]*\)\s*)\bBEGIN\b"
    )
    .unwrap();
    static ref RE_COLUMNS_APPLY: Regex =
        Regex::new(r"(?i)COLUMNS\s*\(\s*'([^']+)'\s*\)((?:\s+APPLY\s+(?:\([^)]+\)|\S+))+)").unwrap();
    static ref RE_APPLY_PART: Regex = Regex::new(r"(?i)\bAPPLY\s+((?:\([^)]+\)|\S+))").unwrap();
    static ref RE_BIGQUERY_PROC_BEGIN: Regex = Regex::new(
        r"(?is)(CREATE\s+(?:OR\s+REPLACE\s+)?PROCEDURE\s+\S+\s*\([^)]*\)\s*)(BEGIN\b)"
    ).unwrap();
}

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

        let sql_without_codec = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::strip_codec_clauses(&sql_without_exclude)
        } else {
            sql_without_exclude
        };

        let sql_without_ttl = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::strip_ttl_clauses(&sql_without_codec)
        } else {
            sql_without_codec
        };

        let sql_with_paste_join = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_paste_join(&sql_without_ttl)
        } else {
            sql_without_ttl
        };

        let sql_with_asof_left = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_asof_left_to_left_asof(&sql_with_paste_join)
        } else {
            sql_with_paste_join
        };

        let sql_with_asof_join = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_asof_join(&sql_with_asof_left)
        } else {
            sql_with_asof_left
        };

        let sql_with_array_join = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_array_join(&sql_with_asof_join)
        } else {
            sql_with_asof_join
        };

        let sql_without_final = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::strip_final_modifier(&sql_with_array_join)
        } else {
            sql_with_array_join
        };

        let sql_without_settings = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::strip_settings_clause(&sql_without_final)
        } else {
            sql_without_final
        };

        let sql_without_global = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::strip_global_keyword(&sql_without_settings)
        } else {
            sql_without_settings
        };

        let sql_with_tuple_as = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_tuple_as_names(&sql_without_global)
        } else {
            sql_without_global
        };

        let sql_with_named_tuples = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_named_tuples(&sql_with_tuple_as)
        } else {
            sql_with_tuple_as
        };

        let sql_with_single_element_tuples = if matches!(self.dialect_type, DialectType::ClickHouse)
        {
            Self::rewrite_single_element_tuples(&sql_with_named_tuples)
        } else {
            sql_with_named_tuples
        };

        let sql_with_tuple_element_access = if matches!(self.dialect_type, DialectType::ClickHouse)
        {
            Self::rewrite_tuple_element_access(&sql_with_single_element_tuples)
        } else {
            sql_with_single_element_tuples
        };

        let sql_with_view_rewritten = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_view_table_function(&sql_with_tuple_element_access)
        } else {
            sql_with_tuple_element_access
        };

        let sql_with_columns_apply = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_columns_apply(&sql_with_view_rewritten)
        } else {
            sql_with_view_rewritten
        };

        let sql_with_in_table = if matches!(self.dialect_type, DialectType::ClickHouse) {
            Self::rewrite_in_table(&sql_with_columns_apply)
        } else {
            sql_with_columns_apply
        };

        let sql_with_rewritten_locks = if matches!(self.dialect_type, DialectType::PostgreSQL) {
            Self::rewrite_pg_lock_clauses(&sql_with_in_table)
        } else {
            sql_with_in_table
        };

        let sql_without_fk_match = if matches!(self.dialect_type, DialectType::PostgreSQL) {
            Self::strip_fk_match(&sql_with_rewritten_locks)
        } else {
            sql_with_rewritten_locks
        };

        let sql_without_no_inherit = if matches!(self.dialect_type, DialectType::PostgreSQL) {
            Self::strip_no_inherit(&sql_without_fk_match)
        } else {
            sql_without_fk_match
        };

        let sql_without_exclude = if matches!(self.dialect_type, DialectType::PostgreSQL) {
            Self::strip_exclude_constraint(&sql_without_no_inherit)
        } else {
            sql_without_no_inherit
        };

        let (sql_with_rewritten_procedures, is_proc_or_replace) = match self.dialect_type {
            DialectType::PostgreSQL => (
                Self::rewrite_procedure_dollar_quotes(&sql_without_exclude),
                false,
            ),
            DialectType::BigQuery => Self::rewrite_bigquery_procedure_begin(&sql_without_exclude),
            DialectType::ClickHouse => (sql_without_exclude, false),
        };

        let sql_with_bq_procedures = sql_with_rewritten_procedures;

        let rewritten_sql = self.rewrite_json_item_methods(&sql_with_bq_procedures)?;
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

        let sql_statements = if is_proc_or_replace {
            sql_statements
                .into_iter()
                .map(|stmt| {
                    if let SqlStatement::CreateProcedure {
                        name,
                        or_alter: _,
                        params,
                        body,
                        language,
                    } = stmt
                    {
                        SqlStatement::CreateProcedure {
                            name,
                            or_alter: true,
                            params,
                            body,
                            language,
                        }
                    } else {
                        stmt
                    }
                })
                .collect::<Vec<_>>()
        } else {
            sql_statements
        };

        if sql_statements.len() == merge_returnings.len() {
            Ok(sql_statements
                .into_iter()
                .zip(merge_returnings)
                .map(|(stmt, returning)| {
                    Statement::Standard(StandardStatement::new(stmt, returning))
                })
                .collect())
        } else {
            Ok(sql_statements
                .into_iter()
                .map(|stmt| Statement::Standard(StandardStatement::new(stmt, None)))
                .collect())
        }
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
        if let Some(custom_stmt) = CustomStatementParser::parse_loop_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_repeat_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_for_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_leave_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_continue_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_break_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if let Some(custom_stmt) = CustomStatementParser::parse_while_statement(sql)? {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::BigQuery
            && let Some(custom_stmt) = CustomStatementParser::parse_export_data(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::BigQuery
            && let Some(custom_stmt) = CustomStatementParser::parse_load_data(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_create_partition(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_detach_partition(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_attach_partition(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_enable_row_movement(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_disable_row_movement(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_create_rule(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_drop_rule(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.dialect_type == DialectType::PostgreSQL
            && let Some(custom_stmt) = CustomStatementParser::parse_alter_rule(sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

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

        if self.is_alter_schema(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_alter_schema(&meaningful_tokens)?
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

        if self.is_lock_table(&meaningful_tokens)
            && let Some(custom_stmt) = CustomStatementParser::parse_lock_table(&meaningful_tokens)?
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
            && let Some(custom_stmt) = ClickHouseParser::try_parse(&meaningful_tokens, sql)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_create_snapshot_table(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_create_snapshot_table(&meaningful_tokens)?
        {
            return Ok(Some(Statement::Custom(custom_stmt)));
        }

        if self.is_drop_snapshot_table(&meaningful_tokens)
            && let Some(custom_stmt) =
                CustomStatementParser::parse_drop_snapshot_table(&meaningful_tokens)?
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

    fn rewrite_procedure_dollar_quotes(sql: &str) -> String {
        let upper = sql.to_uppercase();
        if !upper.contains("CREATE PROCEDURE") && !upper.contains("CREATE OR REPLACE PROCEDURE") {
            return sql.to_string();
        }

        let Some(caps) = RE_PROC_PREFIX.captures(sql) else {
            return sql.to_string();
        };

        let prefix = &caps[1];
        let opening_delimiter = &caps[2];
        let closing_delimiter = opening_delimiter;

        let prefix_end = caps.get(0).unwrap().end();
        let remaining = &sql[prefix_end..];

        let Some(closing_pos) = remaining.find(closing_delimiter) else {
            return sql.to_string();
        };

        let body = &remaining[..closing_pos];
        let after_body = &remaining[closing_pos + closing_delimiter.len()..];

        let language = RE_PROC_LANGUAGE
            .captures(after_body)
            .map(|c| c.get(1).unwrap().as_str().to_string());
        let after_language = if let Some(ref lang_caps) = RE_PROC_LANGUAGE.captures(after_body) {
            &after_body[lang_caps.get(0).unwrap().end()..]
        } else {
            after_body
        };

        let body_trimmed = body.trim();

        match language {
            Some(lang) => {
                format!(
                    "{} {} AS {}{}",
                    prefix.trim(),
                    lang,
                    body_trimmed,
                    after_language
                )
            }
            None => format!("{} AS {}{}", prefix.trim(), body_trimmed, after_language),
        }
    }

    fn rewrite_bigquery_procedure_begin(sql: &str) -> (String, bool) {
        let upper = sql.to_uppercase();
        if !upper.contains("CREATE PROCEDURE") && !upper.contains("CREATE OR REPLACE PROCEDURE") {
            return (sql.to_string(), false);
        }
        let is_or_replace = upper.contains("CREATE OR REPLACE PROCEDURE");
        let sql_without_or_replace = if is_or_replace {
            lazy_static! {
                static ref RE_OR_REPLACE: Regex =
                    Regex::new(r"(?i)\bCREATE\s+OR\s+REPLACE\s+PROCEDURE\b").unwrap();
            }
            RE_OR_REPLACE.replace(sql, "CREATE PROCEDURE").to_string()
        } else {
            sql.to_string()
        };
        if upper.contains("AS BEGIN")
            || upper.contains("AS\nBEGIN")
            || upper.contains("AS\r\nBEGIN")
        {
            return (sql_without_or_replace, is_or_replace);
        }
        let Some(caps) = RE_BQ_PROC_BEGIN.captures(&sql_without_or_replace) else {
            debug_eprintln!("[parser] BQ proc rewrite: regex didn't match");
            return (sql_without_or_replace, is_or_replace);
        };
        let prefix = &caps[1];
        let prefix_end = caps.get(1).unwrap().end();
        let remaining = &sql_without_or_replace[prefix_end..];
        let result = format!("{} AS {}", prefix.trim(), remaining);
        debug_eprintln!(
            "[parser] BQ proc rewrite: {} -> {} (or_replace={})",
            sql.replace('\n', "\\n"),
            result.replace('\n', "\\n"),
            is_or_replace
        );
        (result, is_or_replace)
    }

    fn strip_codec_clauses(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;

        while idx < sql.len() {
            if Self::is_codec_at(sql, idx) {
                let keyword_end = idx + 5;
                let mut cursor = keyword_end;

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

                if cursor < sql.len()
                    && sql[cursor..].starts_with('(')
                    && let Ok(close_idx) = SqlWalker::new(sql).find_matching_paren(cursor)
                {
                    idx = close_idx + 1;
                    continue;
                }

                result.push_str(&sql[idx..keyword_end]);
                idx = keyword_end;
            } else {
                let Some(ch) = Self::next_char(sql, idx) else {
                    break;
                };
                result.push(ch);
                idx += ch.len_utf8();
            }
        }

        result
    }

    fn strip_ttl_clauses(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;
        let mut context = SqlContext::new();

        while idx < sql.len() {
            let ch = match Self::next_char(sql, idx) {
                Some(c) => c,
                None => break,
            };
            let peek = Self::next_char(sql, idx + ch.len_utf8());

            if context.process_char(ch, peek) {
                result.push(ch);
                idx += ch.len_utf8();
                continue;
            }

            if context.is_in_code() && Self::is_ttl_at(sql, idx) {
                let ttl_end = Self::find_ttl_clause_end(sql, idx);
                idx = ttl_end;
                continue;
            }

            result.push(ch);
            idx += ch.len_utf8();
        }

        result
    }

    fn strip_final_modifier(sql: &str) -> String {
        RE_FINAL.replace_all(sql, "").to_string()
    }

    fn strip_settings_clause(sql: &str) -> String {
        RE_SETTINGS.replace_all(sql, "").to_string()
    }

    fn strip_global_keyword(sql: &str) -> String {
        RE_GLOBAL_IN.replace_all(sql, "IN").to_string()
    }

    fn rewrite_tuple_as_names(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut byte_idx = 0;
        let upper = sql.to_uppercase();

        while byte_idx < sql.len() {
            let ch = match sql[byte_idx..].chars().next() {
                Some(c) => c,
                None => break,
            };
            let ch_len = ch.len_utf8();

            if ch == '\'' || ch == '"' {
                result.push(ch);
                byte_idx += ch_len;
                let quote_char = ch;
                while byte_idx < sql.len() {
                    let inner_ch = match sql[byte_idx..].chars().next() {
                        Some(c) => c,
                        None => break,
                    };
                    result.push(inner_ch);
                    byte_idx += inner_ch.len_utf8();
                    if inner_ch == quote_char {
                        break;
                    }
                }
                continue;
            }

            if byte_idx < upper.len()
                && upper[byte_idx..].starts_with("TUPLE(")
                && (byte_idx == 0 || {
                    let prev = sql[..byte_idx].chars().last().unwrap_or(' ');
                    !prev.is_ascii_alphanumeric() && prev != '_'
                })
            {
                let paren_start = byte_idx + 5;
                if let Some(paren_end) = Self::find_matching_paren_at(sql, paren_start) {
                    let inner = &sql[paren_start + 1..paren_end];

                    if let Some(named_args) = Self::parse_tuple_as_args(inner) {
                        result.push_str("__NAMED_TUPLE__(");
                        for (i, (name, value)) in named_args.iter().enumerate() {
                            if i > 0 {
                                result.push_str(", ");
                            }
                            result.push('\'');
                            result.push_str(name);
                            result.push_str("', ");
                            result.push_str(value);
                        }
                        result.push(')');
                        byte_idx = paren_end + 1;
                        continue;
                    }
                }
            }

            result.push(ch);
            byte_idx += ch_len;
        }

        result
    }

    fn parse_tuple_as_args(inner: &str) -> Option<Vec<(String, String)>> {
        let mut args = Vec::new();
        let mut current_pos = 0;
        let inner_bytes = inner.as_bytes();
        let upper = inner.to_uppercase();
        let mut has_as = false;

        while current_pos < inner.len() {
            while current_pos < inner.len() && inner_bytes[current_pos].is_ascii_whitespace() {
                current_pos += 1;
            }
            if current_pos >= inner.len() {
                break;
            }

            let value_start = current_pos;
            let mut depth = 0;
            let mut in_str = false;
            let mut str_char = b' ';
            let mut as_pos = None;

            while current_pos < inner.len() {
                let c = inner_bytes[current_pos];
                if in_str {
                    if c == str_char {
                        in_str = false;
                    }
                    current_pos += 1;
                } else {
                    match c {
                        b'\'' | b'"' => {
                            in_str = true;
                            str_char = c;
                            current_pos += 1;
                        }
                        b'(' => {
                            depth += 1;
                            current_pos += 1;
                        }
                        b')' => {
                            if depth == 0 {
                                break;
                            }
                            depth -= 1;
                            current_pos += 1;
                        }
                        b',' if depth == 0 => {
                            break;
                        }
                        _ => {
                            if depth == 0
                                && as_pos.is_none()
                                && upper[current_pos..].starts_with("AS ")
                                && (current_pos == 0
                                    || inner_bytes[current_pos - 1].is_ascii_whitespace())
                            {
                                as_pos = Some(current_pos);
                            }
                            current_pos += 1;
                        }
                    }
                }
            }

            if let Some(as_idx) = as_pos {
                let value = inner[value_start..as_idx].trim().to_string();
                let name_start = as_idx + 3;
                let name = inner[name_start..current_pos].trim().to_string();

                if value.is_empty() || name.is_empty() {
                    return None;
                }

                args.push((name, value));
                has_as = true;
            } else {
                return None;
            }

            if current_pos < inner.len() && inner_bytes[current_pos] == b',' {
                current_pos += 1;
            }
        }

        if has_as && !args.is_empty() {
            Some(args)
        } else {
            None
        }
    }

    fn rewrite_named_tuples(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;
        let mut in_string = false;
        let mut string_char = ' ';

        while idx < sql.len() {
            let ch = match sql[idx..].chars().next() {
                Some(c) => c,
                None => break,
            };

            if in_string {
                result.push(ch);
                if ch == string_char {
                    in_string = false;
                }
                idx += ch.len_utf8();
                continue;
            }

            match ch {
                '\'' | '"' => {
                    in_string = true;
                    string_char = ch;
                    result.push(ch);
                    idx += ch.len_utf8();
                }
                '(' => {
                    if let Some((named_tuple_str, end_idx)) = Self::try_parse_named_tuple(sql, idx)
                    {
                        result.push_str(&named_tuple_str);
                        idx = end_idx;
                    } else {
                        result.push(ch);
                        idx += ch.len_utf8();
                    }
                }
                _ => {
                    result.push(ch);
                    idx += ch.len_utf8();
                }
            }
        }

        result
    }

    fn try_parse_named_tuple(sql: &str, start: usize) -> Option<(String, usize)> {
        if !sql[start..].starts_with('(') {
            return None;
        }

        let close_idx = Self::find_matching_paren_at(sql, start)?;
        let inner = &sql[start + 1..close_idx];

        let mut fields: Vec<(String, String)> = Vec::new();
        let mut current_pos = 0;
        let inner_bytes = inner.as_bytes();

        while current_pos < inner.len() {
            while current_pos < inner.len() && inner_bytes[current_pos].is_ascii_whitespace() {
                current_pos += 1;
            }
            if current_pos >= inner.len() {
                break;
            }

            let name_start = current_pos;
            while current_pos < inner.len() {
                let c = inner_bytes[current_pos];
                if c.is_ascii_alphanumeric() || c == b'_' {
                    current_pos += 1;
                } else {
                    break;
                }
            }

            if current_pos == name_start {
                return None;
            }
            let name = inner[name_start..current_pos].to_string();

            while current_pos < inner.len() && inner_bytes[current_pos].is_ascii_whitespace() {
                current_pos += 1;
            }

            if current_pos >= inner.len() || inner_bytes[current_pos] != b':' {
                return None;
            }
            current_pos += 1;

            while current_pos < inner.len() && inner_bytes[current_pos].is_ascii_whitespace() {
                current_pos += 1;
            }

            let value_start = current_pos;
            let mut depth = 0;
            let mut in_str = false;
            let mut str_char = b' ';

            while current_pos < inner.len() {
                let c = inner_bytes[current_pos];
                if in_str {
                    if c == str_char {
                        in_str = false;
                    }
                    current_pos += 1;
                } else {
                    match c {
                        b'\'' | b'"' => {
                            in_str = true;
                            str_char = c;
                            current_pos += 1;
                        }
                        b'(' => {
                            depth += 1;
                            current_pos += 1;
                        }
                        b')' => {
                            if depth == 0 {
                                break;
                            }
                            depth -= 1;
                            current_pos += 1;
                        }
                        b',' if depth == 0 => {
                            break;
                        }
                        _ => {
                            current_pos += 1;
                        }
                    }
                }
            }

            let value = inner[value_start..current_pos].trim().to_string();
            if value.is_empty() {
                return None;
            }

            fields.push((name, value));

            if current_pos < inner.len() && inner_bytes[current_pos] == b',' {
                current_pos += 1;
            }
        }

        if fields.is_empty() {
            return None;
        }

        let mut args = Vec::new();
        for (name, value) in &fields {
            args.push(format!("'{}'", name));
            args.push(value.clone());
        }
        let result = format!("__NAMED_TUPLE__({})", args.join(", "));

        Some((result, close_idx + 1))
    }

    fn rewrite_single_element_tuples(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;
        let mut in_string = false;
        let mut string_char = ' ';

        while idx < sql.len() {
            let ch = match sql[idx..].chars().next() {
                Some(c) => c,
                None => break,
            };

            if in_string {
                result.push(ch);
                if ch == string_char {
                    in_string = false;
                }
                idx += ch.len_utf8();
                continue;
            }

            match ch {
                '\'' | '"' => {
                    in_string = true;
                    string_char = ch;
                    result.push(ch);
                    idx += ch.len_utf8();
                }
                '(' => {
                    if let Some((tuple_str, end_idx)) =
                        Self::try_parse_single_element_tuple(sql, idx)
                    {
                        result.push_str(&tuple_str);
                        idx = end_idx;
                    } else {
                        result.push(ch);
                        idx += ch.len_utf8();
                    }
                }
                _ => {
                    result.push(ch);
                    idx += ch.len_utf8();
                }
            }
        }

        result
    }

    fn try_parse_single_element_tuple(sql: &str, start: usize) -> Option<(String, usize)> {
        if !sql[start..].starts_with('(') {
            return None;
        }

        let close_idx = Self::find_matching_paren_at(sql, start)?;
        let inner = &sql[start + 1..close_idx];
        let trimmed = inner.trim();

        if !trimmed.ends_with(',') {
            return None;
        }

        let value = trimmed[..trimmed.len() - 1].trim();
        if value.is_empty() {
            return None;
        }

        if value.contains(',') {
            return None;
        }

        let result = format!("tuple({})", value);
        Some((result, close_idx + 1))
    }

    fn rewrite_tuple_element_access(sql: &str) -> String {
        let mut result = sql.to_string();
        let mut changed = true;

        while changed {
            changed = false;
            let chars: Vec<char> = result.chars().collect();
            let mut new_result = String::with_capacity(result.len());
            let mut idx = 0;
            let mut in_string = false;
            let mut string_char = ' ';

            while idx < chars.len() {
                let ch = chars[idx];

                if in_string {
                    new_result.push(ch);
                    if ch == string_char {
                        in_string = false;
                    }
                    idx += 1;
                    continue;
                }

                if ch == '\'' || ch == '"' {
                    in_string = true;
                    string_char = ch;
                    new_result.push(ch);
                    idx += 1;
                    continue;
                }

                if ch == ')'
                    && idx + 1 < chars.len()
                    && chars[idx + 1] == '.'
                    && idx + 2 < chars.len()
                    && chars[idx + 2].is_ascii_digit()
                {
                    let digit_start = idx + 2;
                    let mut digit_end = digit_start;
                    while digit_end < chars.len() && chars[digit_end].is_ascii_digit() {
                        digit_end += 1;
                    }
                    let index_str: String = chars[digit_start..digit_end].iter().collect();

                    if let Some(open_paren_idx) = Self::find_matching_open_paren_chars(&new_result)
                    {
                        let before_expr = &new_result[..open_paren_idx];
                        let expr = &new_result[open_paren_idx..];
                        new_result = format!("{}tupleElement({}", before_expr, expr);
                        new_result.push(')');
                        new_result.push_str(", ");
                        new_result.push_str(&index_str);
                        new_result.push(')');
                        idx = digit_end;
                        changed = true;
                        continue;
                    }
                }

                if (ch.is_ascii_alphanumeric() || ch == '_')
                    && idx + 1 < chars.len()
                    && chars[idx + 1] == '.'
                    && idx + 2 < chars.len()
                    && chars[idx + 2].is_ascii_digit()
                {
                    let digit_start = idx + 2;
                    let mut digit_end = digit_start;
                    while digit_end < chars.len() && chars[digit_end].is_ascii_digit() {
                        digit_end += 1;
                    }

                    if digit_end < chars.len()
                        && (chars[digit_end].is_ascii_alphanumeric() || chars[digit_end] == '_')
                    {
                        new_result.push(ch);
                        idx += 1;
                        continue;
                    }

                    let ident_start = Self::find_identifier_start(&new_result);
                    let ident = &new_result[ident_start..];

                    if ident.is_empty() || ident.chars().next().unwrap().is_ascii_digit() {
                        new_result.push(ch);
                        idx += 1;
                        continue;
                    }

                    let index_str: String = chars[digit_start..digit_end].iter().collect();

                    let before_ident = &new_result[..ident_start];
                    new_result = format!("{}tupleElement({}", before_ident, ident);
                    new_result.push(ch);
                    new_result.push_str(", ");
                    new_result.push_str(&index_str);
                    new_result.push(')');
                    idx = digit_end;
                    changed = true;
                    continue;
                }

                new_result.push(ch);
                idx += 1;
            }

            result = new_result;
        }

        result
    }

    fn find_matching_open_paren_chars(s: &str) -> Option<usize> {
        let mut depth = 1;
        let mut in_string = false;
        let mut string_char = ' ';

        for (i, ch) in s.char_indices().rev() {
            if in_string {
                if ch == string_char {
                    in_string = false;
                }
                continue;
            }

            if ch == '\'' || ch == '"' {
                in_string = true;
                string_char = ch;
                continue;
            }

            match ch {
                ')' => depth += 1,
                '(' => {
                    depth -= 1;
                    if depth == 0 {
                        let mut byte_idx = i;
                        while byte_idx > 0 {
                            let prev_char = s[..byte_idx].chars().last()?;
                            if prev_char.is_ascii_alphanumeric() || prev_char == '_' {
                                byte_idx -= prev_char.len_utf8();
                            } else {
                                break;
                            }
                        }
                        return Some(byte_idx);
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn find_identifier_start(s: &str) -> usize {
        let bytes = s.as_bytes();
        let mut idx = bytes.len();
        while idx > 0 && (bytes[idx - 1].is_ascii_alphanumeric() || bytes[idx - 1] == b'_') {
            idx -= 1;
        }
        idx
    }

    fn rewrite_pg_lock_clauses(sql: &str) -> String {
        let result = RE_NO_KEY_UPDATE.replace_all(sql, "FOR UPDATE");
        let result = RE_KEY_SHARE.replace_all(&result, "FOR SHARE");
        RE_SKIP_LOCKED.replace_all(&result, "").to_string()
    }

    fn strip_fk_match(sql: &str) -> String {
        RE_FK_MATCH.replace_all(sql, "").to_string()
    }

    fn strip_no_inherit(sql: &str) -> String {
        RE_NO_INHERIT.replace_all(sql, "").to_string()
    }

    fn strip_exclude_constraint(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let upper = sql.to_uppercase();
        let mut idx = 0;

        while idx < sql.len() {
            let is_exclude_keyword = upper[idx..].starts_with("EXCLUDE")
                && (idx + 7 >= sql.len()
                    || !sql[idx + 7..].starts_with(|c: char| c.is_alphanumeric() || c == '_'));
            if is_exclude_keyword {
                idx += 7;

                while idx < sql.len() && sql[idx..].starts_with(char::is_whitespace) {
                    idx += sql[idx..].chars().next().unwrap().len_utf8();
                }

                if upper[idx..].starts_with("USING") {
                    idx += 5;

                    while idx < sql.len() && sql[idx..].starts_with(char::is_whitespace) {
                        idx += sql[idx..].chars().next().unwrap().len_utf8();
                    }

                    while idx < sql.len() && sql[idx..].chars().next().unwrap().is_alphanumeric() {
                        idx += sql[idx..].chars().next().unwrap().len_utf8();
                    }

                    while idx < sql.len() && sql[idx..].starts_with(char::is_whitespace) {
                        idx += sql[idx..].chars().next().unwrap().len_utf8();
                    }

                    if idx < sql.len() && sql[idx..].starts_with('(') {
                        let mut depth = 1;
                        idx += 1;
                        while idx < sql.len() && depth > 0 {
                            let ch = sql[idx..].chars().next().unwrap();
                            match ch {
                                '(' => depth += 1,
                                ')' => depth -= 1,
                                _ => {}
                            }
                            idx += ch.len_utf8();
                        }

                        let trimmed = result.trim_end();
                        if let Some(stripped) = trimmed.strip_suffix(',') {
                            result = stripped.to_string();
                        }
                        continue;
                    }
                }
            }

            let ch = sql[idx..].chars().next().unwrap();
            result.push(ch);
            idx += ch.len_utf8();
        }

        result
    }

    fn rewrite_asof_join(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let upper = sql.to_uppercase();
        let mut idx = 0;

        while idx < sql.len() {
            if let Some((asof_match, is_left)) = Self::match_asof_join_at(&upper, idx) {
                idx += asof_match;

                while idx < sql.len() && sql[idx..].starts_with(char::is_whitespace) {
                    idx += sql[idx..].chars().next().unwrap().len_utf8();
                }

                let table_start = idx;
                while idx < sql.len() {
                    let ch = sql[idx..].chars().next().unwrap();
                    if ch.is_whitespace() || ch == '(' {
                        break;
                    }
                    idx += ch.len_utf8();
                }
                let table_name = &sql[table_start..idx];

                if is_left {
                    result.push_str(&format!("LEFT OUTER JOIN __ASOF_LEFT__{} ", table_name));
                } else {
                    result.push_str(&format!("INNER JOIN __ASOF__{} ", table_name));
                }
            } else {
                let ch = sql[idx..].chars().next().unwrap();
                result.push(ch);
                idx += ch.len_utf8();
            }
        }

        result
    }

    fn match_asof_join_at(upper_sql: &str, start: usize) -> Option<(usize, bool)> {
        let rest = &upper_sql[start..];

        if rest.starts_with("ASOF JOIN ") {
            return Some(("ASOF JOIN ".len(), false));
        }
        if rest.starts_with("LEFT ASOF JOIN ") {
            return Some(("LEFT ASOF JOIN ".len(), true));
        }
        None
    }

    fn rewrite_asof_left_to_left_asof(sql: &str) -> String {
        let upper = sql.to_uppercase();
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;

        while idx < sql.len() {
            if upper[idx..].starts_with("ASOF LEFT JOIN ") {
                result.push_str("LEFT ASOF JOIN ");
                idx += "ASOF LEFT JOIN ".len();
            } else {
                let ch = sql[idx..].chars().next().unwrap();
                result.push(ch);
                idx += ch.len_utf8();
            }
        }

        result
    }

    fn rewrite_paste_join(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let mut idx = 0;
        let upper = sql.to_uppercase();
        let mut paste_count = 0;

        while idx < sql.len() {
            if let Some(paste_match) = Self::match_paste_join_at(&upper, idx) {
                idx += paste_match;

                while idx < sql.len() && sql[idx..].starts_with(char::is_whitespace) {
                    idx += sql[idx..].chars().next().unwrap().len_utf8();
                }

                if idx < sql.len() && sql[idx..].starts_with('(') {
                    if let Some(close_paren) = Self::find_matching_paren_at(sql, idx) {
                        let subquery = &sql[idx..=close_paren];
                        result.push_str(&format!(
                            "CROSS JOIN {} AS __PASTE_SUBQUERY_{}__",
                            subquery, paste_count
                        ));
                        idx = close_paren + 1;
                        paste_count += 1;
                    } else {
                        result.push_str("CROSS JOIN __PASTE__ ");
                    }
                } else {
                    result.push_str("CROSS JOIN __PASTE__ ");
                }
            } else {
                let ch = sql[idx..].chars().next().unwrap();
                result.push(ch);
                idx += ch.len_utf8();
            }
        }

        result
    }

    fn find_matching_paren_at(sql: &str, start: usize) -> Option<usize> {
        if !sql[start..].starts_with('(') {
            return None;
        }
        let mut depth = 0;
        let mut in_string = false;
        let mut string_char = ' ';

        for (i, ch) in sql[start..].char_indices() {
            if in_string {
                if ch == string_char {
                    in_string = false;
                }
            } else {
                match ch {
                    '\'' | '"' => {
                        in_string = true;
                        string_char = ch;
                    }
                    '(' => depth += 1,
                    ')' => {
                        depth -= 1;
                        if depth == 0 {
                            return Some(start + i);
                        }
                    }
                    _ => {}
                }
            }
        }
        None
    }

    fn match_paste_join_at(upper_sql: &str, start: usize) -> Option<usize> {
        let rest = &upper_sql[start..];
        if !rest.starts_with("PASTE") {
            return None;
        }

        if start > 0 {
            let prev_char = upper_sql[..start].chars().next_back()?;
            if prev_char.is_ascii_alphanumeric() || prev_char == '_' {
                return None;
            }
        }

        let mut pos = 5;
        while pos < rest.len() && rest[pos..].starts_with(char::is_whitespace) {
            pos += rest[pos..].chars().next()?.len_utf8();
        }

        if !rest[pos..].starts_with("JOIN") {
            return None;
        }
        pos += 4;

        if pos < rest.len() {
            let next_char = rest[pos..].chars().next()?;
            if next_char.is_ascii_alphanumeric() || next_char == '_' {
                return None;
            }
        }

        Some(pos)
    }

    fn rewrite_array_join(sql: &str) -> String {
        let mut result = String::with_capacity(sql.len());
        let upper = sql.to_uppercase();
        let mut idx = 0;

        while idx < sql.len() {
            if let Some((len, is_left)) = Self::match_array_join_at(&upper, idx) {
                idx += len;

                while idx < sql.len() && sql[idx..].starts_with(char::is_whitespace) {
                    idx += sql[idx..].chars().next().unwrap().len_utf8();
                }

                let mut paren_depth = 0;
                let start_of_columns = idx;

                while idx < sql.len() {
                    let ch = sql[idx..].chars().next().unwrap();
                    match ch {
                        '(' => {
                            paren_depth += 1;
                            idx += 1;
                        }
                        ')' => {
                            if paren_depth == 0 {
                                break;
                            }
                            paren_depth -= 1;
                            idx += 1;
                        }
                        _ => {
                            if paren_depth == 0 && Self::is_keyword_boundary(&upper, idx) {
                                break;
                            }
                            idx += ch.len_utf8();
                        }
                    }
                }

                let columns = sql[start_of_columns..idx].trim().to_string();
                let encoded_columns = Self::encode_array_join_columns(&columns);

                if is_left {
                    result.push_str(&format!(
                        "LEFT OUTER JOIN __LEFT_ARRAY_JOIN__{}__ ",
                        encoded_columns
                    ));
                } else {
                    result.push_str(&format!("INNER JOIN __ARRAY_JOIN__{}__ ", encoded_columns));
                }
            } else {
                let ch = sql[idx..].chars().next().unwrap();
                result.push(ch);
                idx += ch.len_utf8();
            }
        }

        result
    }

    fn encode_array_join_columns(columns: &str) -> String {
        columns
            .replace(' ', "_SP_")
            .replace(',', "_CM_")
            .replace('(', "_LP_")
            .replace(')', "_RP_")
    }

    #[allow(dead_code)]
    fn decode_array_join_columns(encoded: &str) -> String {
        encoded
            .replace("_SP_", " ")
            .replace("_CM_", ",")
            .replace("_LP_", "(")
            .replace("_RP_", ")")
    }

    fn match_array_join_at(upper_sql: &str, start: usize) -> Option<(usize, bool)> {
        let rest = &upper_sql[start..];

        if start > 0 {
            let prev_char = upper_sql[..start].chars().next_back()?;
            if prev_char.is_ascii_alphanumeric() || prev_char == '_' {
                return None;
            }
        }

        if rest.starts_with("LEFT ARRAY JOIN") {
            let len = "LEFT ARRAY JOIN".len();
            if len >= rest.len() || !rest[len..].starts_with(|c: char| c.is_whitespace()) {
                return None;
            }
            return Some((len, true));
        }

        if rest.starts_with("ARRAY JOIN") {
            let len = "ARRAY JOIN".len();
            if len >= rest.len() || !rest[len..].starts_with(|c: char| c.is_whitespace()) {
                return None;
            }
            return Some((len, false));
        }

        None
    }

    fn is_keyword_boundary(upper_sql: &str, start: usize) -> bool {
        let keywords = [
            "WHERE",
            "ORDER",
            "GROUP",
            "HAVING",
            "LIMIT",
            "UNION",
            "INTERSECT",
            "EXCEPT",
            "INNER",
            "LEFT",
            "RIGHT",
            "FULL",
            "CROSS",
            "JOIN",
            "ON",
            "ARRAY",
            "PREWHERE",
            "SAMPLE",
            "FINAL",
            "FORMAT",
            "SETTINGS",
            "INTO",
            "WITH",
        ];

        for kw in keywords {
            if upper_sql[start..].starts_with(kw) {
                let after_kw = start + kw.len();
                if after_kw >= upper_sql.len()
                    || !upper_sql[after_kw..]
                        .chars()
                        .next()
                        .map(|c| c.is_ascii_alphanumeric() || c == '_')
                        .unwrap_or(false)
                {
                    if start == 0 {
                        return true;
                    }
                    if let Some(prev_char) = upper_sql[..start].chars().next_back() {
                        return !prev_char.is_ascii_alphanumeric() && prev_char != '_';
                    }
                }
            }
        }
        false
    }

    fn is_ttl_at(sql: &str, start: usize) -> bool {
        let keyword = "TTL";
        let keyword_len = keyword.len();

        let Some(substr) = sql.get(start..start + keyword_len) else {
            return false;
        };

        if !substr.eq_ignore_ascii_case(keyword) {
            return false;
        }

        if start > 0
            && let Some(prev) = sql[..start].chars().next_back()
            && (prev.is_ascii_alphanumeric() || prev == '_')
        {
            return false;
        }

        if start + keyword_len < sql.len()
            && let Some(next) = sql[start + keyword_len..].chars().next()
            && (next.is_ascii_alphanumeric() || next == '_')
        {
            return false;
        }

        true
    }

    fn find_ttl_clause_end(sql: &str, start: usize) -> usize {
        let stop_keywords = ["SETTINGS", "PRIMARY", "SAMPLE", "AS", "COMMENT"];

        let mut idx = start + 3;
        let mut paren_depth = 0;
        let mut context = SqlContext::new();

        while idx < sql.len() {
            let ch = match Self::next_char(sql, idx) {
                Some(c) => c,
                None => break,
            };
            let peek = Self::next_char(sql, idx + ch.len_utf8());

            if context.process_char(ch, peek) {
                idx += ch.len_utf8();
                continue;
            }

            if context.is_in_code() {
                match ch {
                    '(' => {
                        paren_depth += 1;
                        idx += ch.len_utf8();
                        continue;
                    }
                    ')' => {
                        if paren_depth > 0 {
                            paren_depth -= 1;
                        }
                        idx += ch.len_utf8();
                        continue;
                    }
                    ',' if paren_depth == 0 => {
                        let rest = sql[idx + 1..].trim_start();
                        let is_next_ttl_rule = rest
                            .split_whitespace()
                            .next()
                            .map(|w| {
                                let after_word = rest[w.len()..].trim_start();
                                !w.eq_ignore_ascii_case("SETTINGS")
                                    && !w.eq_ignore_ascii_case("PARTITION")
                                    && !w.eq_ignore_ascii_case("PRIMARY")
                                    && (after_word.starts_with('+')
                                        || w.contains('+')
                                        || after_word.chars().next().is_some_and(|c| c == '+'))
                            })
                            .unwrap_or(false);

                        if is_next_ttl_rule {
                            idx += ch.len_utf8();
                            continue;
                        }
                        return idx;
                    }
                    _ => {}
                }

                if paren_depth == 0 {
                    for kw in &stop_keywords {
                        if Self::keyword_at(sql, idx, kw) {
                            return idx;
                        }
                    }
                }
            }

            idx += ch.len_utf8();
        }

        idx
    }

    fn is_codec_at(sql: &str, start: usize) -> bool {
        let keyword = "CODEC";
        let keyword_len = keyword.len();

        let Some(substr) = sql.get(start..start + keyword_len) else {
            return false;
        };

        if !substr.eq_ignore_ascii_case(keyword) {
            return false;
        }

        if start > 0
            && let Some(prev) = sql[..start].chars().next_back()
            && (prev.is_ascii_alphanumeric() || prev == '_')
        {
            return false;
        }

        if start + keyword_len < sql.len()
            && let Some(next) = sql[start + keyword_len..].chars().next()
            && (next.is_ascii_alphanumeric() || next == '_')
        {
            return false;
        }

        true
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

    fn is_alter_schema(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["ALTER", "SCHEMA"])
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

    fn is_lock_table(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["LOCK"])
    }

    fn is_begin(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["BEGIN"])
    }

    fn is_create_snapshot_table(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["CREATE", "SNAPSHOT", "TABLE"])
    }

    fn is_drop_snapshot_table(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["DROP", "SNAPSHOT", "TABLE"])
    }

    #[allow(dead_code)]
    fn is_create_rule(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["CREATE", "RULE"])
            || self.matches_keyword_sequence(tokens, &["CREATE", "OR", "REPLACE", "RULE"])
    }

    #[allow(dead_code)]
    fn is_drop_rule(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["DROP", "RULE"])
    }

    #[allow(dead_code)]
    fn is_alter_rule(&self, tokens: &[&Token]) -> bool {
        self.matches_keyword_sequence(tokens, &["ALTER", "RULE"])
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

    fn rewrite_in_table(sql: &str) -> String {
        if !sql.to_uppercase().contains(" IN ") {
            return sql.to_string();
        }

        let mut result = String::new();
        let mut last_end = 0;

        for caps in RE_IN_TABLE.captures_iter(sql) {
            let full_match = caps.get(0).unwrap();
            let match_start = full_match.start();
            let match_end = full_match.end();

            result.push_str(&sql[last_end..match_start]);

            let remaining = &sql[match_end..];
            let next_char = remaining.chars().next();

            if next_char == Some('(') {
                result.push_str(full_match.as_str());
            } else {
                let table_name = &caps[1];
                result.push_str(&format!("IN (SELECT * FROM {})", table_name));
            }

            last_end = match_end;
        }

        result.push_str(&sql[last_end..]);
        result
    }

    fn rewrite_view_table_function(sql: &str) -> String {
        let sql_with_view = RE_VIEW_TABLE_FUNC
            .replace_all(sql, |caps: &regex::Captures| {
                let subquery = &caps[1];
                format!("({}) AS __view_subquery", subquery)
            })
            .to_string();

        RE_INPUT_TABLE_FUNC
            .replace_all(&sql_with_view, |caps: &regex::Captures| {
                let schema = &caps[1];
                let values = &caps[2];
                format!("VALUES('{}', {})", schema, values.trim())
            })
            .to_string()
    }

    fn rewrite_columns_apply(sql: &str) -> String {
        RE_COLUMNS_APPLY
            .replace_all(sql, |caps: &regex::Captures| {
                let pattern = &caps[1];
                let apply_part = &caps[2];
                let fns: Vec<&str> = RE_APPLY_PART
                    .captures_iter(apply_part)
                    .filter_map(|c| c.get(1).map(|m| m.as_str()))
                    .collect();
                let fns_str = fns
                    .iter()
                    .map(|f| format!("'{}'", f))
                    .collect::<Vec<_>>()
                    .join(", ");
                format!("__COLUMNS_APPLY__('{}', {})", pattern, fns_str)
            })
            .to_string()
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
