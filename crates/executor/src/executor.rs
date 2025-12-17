//! Query executor - parses and executes SQL statements.

#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::ptr_arg)]

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema, TimeUnit,
};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use sqlparser::ast::{
    self, CreateFunctionBody, Expr, LimitClause, ObjectName, OrderBy, OrderByExpr, OrderByKind,
    Query, Select, SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use sqlparser::tokenizer::Span;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField, Value};
use yachtsql_storage::{Column, Field, Record, Schema, Table, TableSchemaOps};

use crate::catalog::{Catalog, ColumnDefault, FunctionBody, UserFunction, UserProcedure};
use crate::evaluator::{Evaluator, parse_byte_string_escapes};

#[derive(Debug, Clone)]
struct LoadDataInfo {
    table_name: String,
    overwrite: bool,
    format: String,
    uris: Vec<String>,
    is_temp_table: bool,
    column_defs: Vec<(String, String)>,
}

fn parse_load_data(sql: &str) -> Option<LoadDataInfo> {
    let upper = sql.to_uppercase();
    if !upper.trim_start().starts_with("LOAD DATA") {
        return None;
    }

    let overwrite = upper.contains("OVERWRITE");
    let is_temp_table = upper.contains("TEMP TABLE");

    let (table_name, column_defs) = if overwrite {
        let after_overwrite = &sql[upper.find("OVERWRITE").unwrap() + 9..];
        let trimmed = after_overwrite.trim_start();
        let end = trimmed
            .find(|c: char| c.is_whitespace())
            .unwrap_or(trimmed.len());
        (trimmed[..end].to_string(), Vec::new())
    } else {
        let into_pos = upper.find("INTO")?;
        let after_into = &sql[into_pos + 4..];
        let trimmed = after_into.trim_start();

        if is_temp_table {
            let after_temp = trimmed
                .strip_prefix("TEMP TABLE")
                .or_else(|| trimmed.strip_prefix("temp table"))
                .unwrap_or(trimmed)
                .trim_start();

            let paren_pos = after_temp.find('(');
            let space_pos = after_temp.find(|c: char| c.is_whitespace());
            let table_end = match (paren_pos, space_pos) {
                (Some(p), Some(s)) => p.min(s),
                (Some(p), None) => p,
                (None, Some(s)) => s,
                (None, None) => after_temp.len(),
            };

            let table_name = after_temp[..table_end].to_string();
            let cols = if let Some(start) = after_temp.find('(') {
                let col_section = &after_temp[start..];
                if let Some(end) = find_matching_paren(col_section) {
                    parse_column_defs(&col_section[1..end])
                } else {
                    Vec::new()
                }
            } else {
                Vec::new()
            };
            (table_name, cols)
        } else {
            let end = trimmed
                .find(|c: char| c.is_whitespace())
                .unwrap_or(trimmed.len());
            (trimmed[..end].to_string(), Vec::new())
        }
    };

    let files_pos = upper.find("FROM FILES")?;
    let options_start = sql[files_pos..].find('(')? + files_pos + 1;
    let options_end = find_matching_paren(&sql[options_start - 1..])? + options_start - 1;
    let options_str = &sql[options_start..options_end];

    let format = extract_option(options_str, "FORMAT")
        .or_else(|| extract_option(options_str, "format"))
        .unwrap_or_else(|| "PARQUET".to_string())
        .trim_matches(|c| c == '\'' || c == '"')
        .to_uppercase();

    let uris_str =
        extract_option(options_str, "URIS").or_else(|| extract_option(options_str, "uris"))?;
    let uris = parse_uri_array(&uris_str);

    Some(LoadDataInfo {
        table_name,
        overwrite,
        format,
        uris,
        is_temp_table,
        column_defs,
    })
}

fn parse_column_defs(s: &str) -> Vec<(String, String)> {
    let mut cols = Vec::new();
    for part in s.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let mut iter = part.split_whitespace();
        if let (Some(name), Some(dtype)) = (iter.next(), iter.next()) {
            cols.push((name.to_string(), dtype.to_string()));
        }
    }
    cols
}

fn parse_simple_data_type(s: &str) -> DataType {
    match s.to_uppercase().as_str() {
        "BOOL" | "BOOLEAN" => DataType::Bool,
        "INT64" | "INTEGER" | "INT" => DataType::Int64,
        "FLOAT64" | "FLOAT" | "DOUBLE" => DataType::Float64,
        "STRING" | "VARCHAR" | "TEXT" => DataType::String,
        "BYTES" => DataType::Bytes,
        "DATE" => DataType::Date,
        "DATETIME" => DataType::DateTime,
        "TIME" => DataType::Time,
        "TIMESTAMP" => DataType::Timestamp,
        "NUMERIC" => DataType::Numeric(None),
        "JSON" => DataType::Json,
        _ => DataType::String,
    }
}

fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    for (i, c) in s.chars().enumerate() {
        if in_string {
            if c == string_char {
                in_string = false;
            }
            continue;
        }

        match c {
            '\'' | '"' => {
                in_string = true;
                string_char = c;
            }
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn extract_option(options_str: &str, key: &str) -> Option<String> {
    let upper = options_str.to_uppercase();
    let key_upper = key.to_uppercase();
    let key_pos = upper.find(&key_upper)?;

    let after_key = &options_str[key_pos + key.len()..];
    let after_eq = after_key.trim_start().strip_prefix('=')?;
    let trimmed = after_eq.trim_start();

    if trimmed.starts_with('[') {
        let end = find_matching_bracket(trimmed)?;
        Some(trimmed[..=end].to_string())
    } else if trimmed.starts_with('\'') || trimmed.starts_with('"') {
        let quote = trimmed.chars().next()?;
        let end = trimmed[1..].find(quote)? + 1;
        Some(trimmed[1..end].to_string())
    } else {
        let end = trimmed.find([',', ')']).unwrap_or(trimmed.len());
        Some(trimmed[..end].trim().to_string())
    }
}

fn find_matching_bracket(s: &str) -> Option<usize> {
    let mut depth = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    for (i, c) in s.chars().enumerate() {
        if in_string {
            if c == string_char {
                in_string = false;
            }
            continue;
        }

        match c {
            '\'' | '"' => {
                in_string = true;
                string_char = c;
            }
            '[' => depth += 1,
            ']' => {
                depth -= 1;
                if depth == 0 {
                    return Some(i);
                }
            }
            _ => {}
        }
    }
    None
}

fn parse_uri_array(s: &str) -> Vec<String> {
    let trimmed = s.trim();
    let inner = if trimmed.starts_with('[') && trimmed.ends_with(']') {
        &trimmed[1..trimmed.len() - 1]
    } else {
        trimmed
    };

    let mut uris = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut string_char = ' ';

    for c in inner.chars() {
        if in_string {
            if c == string_char {
                in_string = false;
                uris.push(current.clone());
                current.clear();
            } else {
                current.push(c);
            }
        } else {
            match c {
                '\'' | '"' => {
                    in_string = true;
                    string_char = c;
                }
                ',' | ' ' | '\n' | '\t' => {}
                _ => current.push(c),
            }
        }
    }

    if !current.is_empty() {
        uris.push(current);
    }

    uris
}

#[derive(Debug, Clone)]
struct WindowFunctionInfo {
    func_name: String,
    args: Vec<Expr>,
    partition_by: Vec<Expr>,
    order_by: Vec<ast::OrderByExpr>,
}

#[derive(Debug, Clone)]
struct ScriptVariable {
    data_type: DataType,
    value: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LoopControl {
    Continue,
    Break,
    Return,
}

#[derive(Debug, Clone)]
struct LoopStatement {
    label: Option<String>,
    body: String,
}

#[derive(Debug, Clone)]
struct WhileDoStatement {
    label: Option<String>,
    condition: String,
    body: String,
}

#[derive(Debug, Clone)]
struct RepeatStatement {
    label: Option<String>,
    body: String,
    until_condition: String,
}

#[derive(Debug, Clone)]
struct ForStatement {
    label: Option<String>,
    loop_var: String,
    query: String,
    body: String,
}

fn parse_loop_statement(sql: &str) -> Option<LoopStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    let (label, rest) = if upper.starts_with("LOOP") {
        (None, trimmed)
    } else if let Some(colon_pos) = trimmed.find(':') {
        let potential_label = trimmed[..colon_pos].trim();
        if potential_label
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
        {
            let after_colon = trimmed[colon_pos + 1..].trim();
            if after_colon.to_uppercase().starts_with("LOOP") {
                (Some(potential_label.to_string()), after_colon)
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    };

    if !rest.to_uppercase().starts_with("LOOP") {
        return None;
    }

    let after_loop = rest[4..].trim();

    let end_loop_pattern = if label.is_some() {
        format!("END LOOP {}", label.as_ref().unwrap())
    } else {
        "END LOOP".to_string()
    };

    let end_pos = find_end_loop_position(after_loop, &label)?;
    let body = after_loop[..end_pos].trim().to_string();

    Some(LoopStatement { label, body })
}

fn find_end_loop_position(s: &str, label: &Option<String>) -> Option<usize> {
    let upper = s.to_uppercase();
    let mut search_pos = 0;
    let mut depth = 1;

    while search_pos < upper.len() {
        let remaining = &upper[search_pos..];

        if remaining.starts_with("LOOP")
            && !remaining[..4]
                .chars()
                .last()
                .map(|c| c.is_alphanumeric())
                .unwrap_or(false)
        {
            if search_pos == 0
                || !s[..search_pos]
                    .chars()
                    .last()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false)
            {
                depth += 1;
            }
        }

        if let Some(after_end_loop) = remaining.strip_prefix("END LOOP") {
            let is_end = after_end_loop.is_empty()
                || after_end_loop.starts_with(';')
                || after_end_loop.starts_with(char::is_whitespace);

            if is_end {
                depth -= 1;
                if depth == 0 {
                    return Some(search_pos);
                }
            }
        }

        search_pos += 1;
    }
    None
}

fn parse_while_do_statement(sql: &str) -> Option<WhileDoStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    let (label, rest) = if upper.starts_with("WHILE") {
        (None, trimmed)
    } else if let Some(colon_pos) = trimmed.find(':') {
        let potential_label = trimmed[..colon_pos].trim();
        if potential_label
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
        {
            let after_colon = trimmed[colon_pos + 1..].trim();
            if after_colon.to_uppercase().starts_with("WHILE") {
                (Some(potential_label.to_string()), after_colon)
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    };

    if !rest.to_uppercase().starts_with("WHILE") {
        return None;
    }

    let after_while = rest[5..].trim();
    let rest_upper = after_while.to_uppercase();
    let do_pos = find_do_position(&rest_upper)?;

    let condition = after_while[..do_pos].trim().to_string();
    let after_do = after_while[do_pos + 2..].trim();

    let end_pos = find_end_while_position(after_do, &label)?;
    let body = after_do[..end_pos].trim().to_string();

    Some(WhileDoStatement {
        label,
        condition,
        body,
    })
}

fn find_do_position(s: &str) -> Option<usize> {
    let mut i = 0;
    while i < s.len() {
        if s[i..].starts_with("DO") {
            let before_ok = i == 0
                || !s[..i]
                    .chars()
                    .last()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false);
            let after_ok = s.len() <= i + 2
                || !s[i + 2..]
                    .chars()
                    .next()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false);
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn find_end_while_position(s: &str, label: &Option<String>) -> Option<usize> {
    let upper = s.to_uppercase();
    let mut search_pos = 0;
    let mut depth = 1;

    while search_pos < upper.len() {
        let remaining = &upper[search_pos..];

        if let Some(after) = remaining.strip_prefix("WHILE") {
            if after.is_empty() || after.starts_with(char::is_whitespace) {
                if search_pos == 0
                    || !s[..search_pos]
                        .chars()
                        .last()
                        .map(|c| c.is_alphanumeric() || c == '_')
                        .unwrap_or(false)
                {
                    depth += 1;
                }
            }
        }

        if let Some(after_end) = remaining.strip_prefix("END WHILE") {
            let is_end = after_end.is_empty()
                || after_end.starts_with(';')
                || after_end.starts_with(char::is_whitespace);

            if is_end {
                depth -= 1;
                if depth == 0 {
                    return Some(search_pos);
                }
            }
        }

        search_pos += 1;
    }
    None
}

fn parse_repeat_statement(sql: &str) -> Option<RepeatStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    let (label, rest) = if upper.starts_with("REPEAT") {
        (None, trimmed)
    } else if let Some(colon_pos) = trimmed.find(':') {
        let potential_label = trimmed[..colon_pos].trim();
        if potential_label
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
        {
            let after_colon = trimmed[colon_pos + 1..].trim();
            if after_colon.to_uppercase().starts_with("REPEAT") {
                (Some(potential_label.to_string()), after_colon)
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    };

    if !rest.to_uppercase().starts_with("REPEAT") {
        return None;
    }

    let after_repeat = rest[6..].trim();

    let until_pos = find_until_position(after_repeat)?;
    let body = after_repeat[..until_pos].trim().to_string();
    let after_until = after_repeat[until_pos + 5..].trim();

    let end_repeat_pos = find_end_repeat_position(after_until)?;
    let until_condition = after_until[..end_repeat_pos].trim().to_string();

    Some(RepeatStatement {
        label,
        body,
        until_condition,
    })
}

fn find_until_position(s: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let mut i = 0;
    let mut depth = 1;

    while i < upper.len() {
        let remaining = &upper[i..];

        if let Some(after) = remaining.strip_prefix("REPEAT") {
            if after.is_empty() || after.starts_with(char::is_whitespace) {
                if i == 0
                    || !s[..i]
                        .chars()
                        .last()
                        .map(|c| c.is_alphanumeric() || c == '_')
                        .unwrap_or(false)
                {
                    depth += 1;
                }
            }
        }

        if remaining.starts_with("END REPEAT") {
            depth -= 1;
        }

        if remaining.starts_with("UNTIL") && depth == 1 {
            let after = &remaining[5..];
            if after.is_empty() || after.starts_with(char::is_whitespace) {
                if i == 0
                    || !s[..i]
                        .chars()
                        .last()
                        .map(|c| c.is_alphanumeric() || c == '_')
                        .unwrap_or(false)
                {
                    return Some(i);
                }
            }
        }

        i += 1;
    }
    None
}

fn find_end_repeat_position(s: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let mut i = 0;
    while i < upper.len() {
        if upper[i..].starts_with("END REPEAT") {
            return Some(i);
        }
        i += 1;
    }
    None
}

fn parse_for_statement(sql: &str) -> Option<ForStatement> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    let (label, rest) = if upper.starts_with("FOR") {
        (None, trimmed)
    } else if let Some(colon_pos) = trimmed.find(':') {
        let potential_label = trimmed[..colon_pos].trim();
        if potential_label
            .chars()
            .all(|c| c.is_alphanumeric() || c == '_')
        {
            let after_colon = trimmed[colon_pos + 1..].trim();
            if after_colon.to_uppercase().starts_with("FOR") {
                (Some(potential_label.to_string()), after_colon)
            } else {
                return None;
            }
        } else {
            return None;
        }
    } else {
        return None;
    };

    if !rest.to_uppercase().starts_with("FOR") {
        return None;
    }

    let after_for = rest[3..].trim();
    let rest_upper = after_for.to_uppercase();

    let in_pos = find_in_keyword_position(&rest_upper)?;
    let loop_var = after_for[..in_pos].trim().to_string();
    let after_in = after_for[in_pos + 2..].trim();

    let do_pos = find_do_position(&after_in.to_uppercase())?;
    let query = after_in[..do_pos].trim().to_string();
    let after_do = after_in[do_pos + 2..].trim();

    let end_pos = find_end_for_position(after_do)?;
    let body = after_do[..end_pos].trim().to_string();

    Some(ForStatement {
        label,
        loop_var,
        query,
        body,
    })
}

fn find_in_keyword_position(s: &str) -> Option<usize> {
    let mut i = 0;
    while i < s.len() {
        if s[i..].starts_with("IN") {
            let before_ok = i == 0
                || !s[..i]
                    .chars()
                    .last()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false);
            let after_ok = s.len() <= i + 2
                || !s[i + 2..]
                    .chars()
                    .next()
                    .map(|c| c.is_alphanumeric() || c == '_')
                    .unwrap_or(false);
            if before_ok && after_ok {
                return Some(i);
            }
        }
        i += 1;
    }
    None
}

fn find_end_for_position(s: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let mut search_pos = 0;
    let mut depth = 1;

    while search_pos < upper.len() {
        let remaining = &upper[search_pos..];

        if let Some(after) = remaining.strip_prefix("FOR") {
            if after.is_empty() || after.starts_with(char::is_whitespace) {
                if search_pos == 0
                    || !s[..search_pos]
                        .chars()
                        .last()
                        .map(|c| c.is_alphanumeric() || c == '_')
                        .unwrap_or(false)
                {
                    depth += 1;
                }
            }
        }

        if let Some(after_end) = remaining.strip_prefix("END FOR") {
            let is_end = after_end.is_empty()
                || after_end.starts_with(';')
                || after_end.starts_with(char::is_whitespace);

            if is_end {
                depth -= 1;
                if depth == 0 {
                    return Some(search_pos);
                }
            }
        }

        search_pos += 1;
    }
    None
}

fn is_leave_or_iterate_statement(sql: &str) -> Option<(bool, Option<String>)> {
    let trimmed = sql.trim().trim_end_matches(';').trim();
    let upper = trimmed.to_uppercase();

    if upper.starts_with("LEAVE") {
        let label = trimmed[5..].trim();
        if label.is_empty() {
            Some((true, None))
        } else {
            Some((true, Some(label.to_string())))
        }
    } else if upper.starts_with("ITERATE") {
        let label = trimmed[7..].trim();
        if label.is_empty() {
            Some((false, None))
        } else {
            Some((false, Some(label.to_string())))
        }
    } else if upper == "BREAK" {
        Some((true, None))
    } else if upper == "CONTINUE" {
        Some((false, None))
    } else {
        None
    }
}

fn preprocess_range_types(sql: &str) -> String {
    use regex::Regex;
    lazy_static::lazy_static! {
        static ref RANGE_TYPE_RE: Regex = Regex::new(r"(?i)\bRANGE\s*<\s*(DATE|DATETIME|TIMESTAMP)\s*>").unwrap();
    }
    RANGE_TYPE_RE.replace_all(sql, "RANGE_$1").to_string()
}

fn preprocess_loop_control_statements(sql: &str) -> String {
    let mut result = String::with_capacity(sql.len());
    let chars: Vec<char> = sql.chars().collect();
    let mut i = 0;
    let mut in_string = false;
    let mut string_char = ' ';

    while i < chars.len() {
        let c = chars[i];

        if in_string {
            result.push(c);
            if c == string_char {
                in_string = false;
            }
            i += 1;
            continue;
        }

        match c {
            '\'' | '"' => {
                in_string = true;
                string_char = c;
                result.push(c);
                i += 1;
            }
            _ => {
                let remaining = &sql[i..];
                let remaining_upper = remaining.to_uppercase();

                let is_word_boundary =
                    i == 0 || !chars[i - 1].is_alphanumeric() && chars[i - 1] != '_';

                if is_word_boundary {
                    if let Some(after) = remaining_upper.strip_prefix("LEAVE") {
                        if after.is_empty()
                            || after.starts_with(';')
                            || after.starts_with(char::is_whitespace)
                        {
                            result.push_str("RAISE USING MESSAGE = '__LOOP_LEAVE__'");
                            i += 5;
                            while i < chars.len()
                                && (chars[i].is_whitespace()
                                    || chars[i].is_alphanumeric()
                                    || chars[i] == '_')
                                && chars[i] != ';'
                            {
                                i += 1;
                            }
                            continue;
                        }
                    }
                    if let Some(after) = remaining_upper.strip_prefix("ITERATE") {
                        if after.is_empty()
                            || after.starts_with(';')
                            || after.starts_with(char::is_whitespace)
                        {
                            result.push_str("RAISE USING MESSAGE = '__LOOP_ITERATE__'");
                            i += 7;
                            while i < chars.len()
                                && (chars[i].is_whitespace()
                                    || chars[i].is_alphanumeric()
                                    || chars[i] == '_')
                                && chars[i] != ';'
                            {
                                i += 1;
                            }
                            continue;
                        }
                    }
                    if let Some(after) = remaining_upper.strip_prefix("BREAK") {
                        if after.is_empty()
                            || after.starts_with(';')
                            || after.starts_with(char::is_whitespace)
                        {
                            result.push_str("RAISE USING MESSAGE = '__LOOP_LEAVE__'");
                            i += 5;
                            continue;
                        }
                    }
                    if let Some(after) = remaining_upper.strip_prefix("CONTINUE") {
                        if after.is_empty()
                            || after.starts_with(';')
                            || after.starts_with(char::is_whitespace)
                        {
                            result.push_str("RAISE USING MESSAGE = '__LOOP_ITERATE__'");
                            i += 8;
                            continue;
                        }
                    }
                }
                result.push(c);
                i += 1;
            }
        }
    }

    result
}

fn split_script_statements(body: &str) -> Vec<String> {
    let mut statements = Vec::new();
    let mut current = String::new();
    let mut in_string = false;
    let mut string_char = ' ';
    let mut depth = 0;
    let chars: Vec<char> = body.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if in_string {
            current.push(c);
            if c == string_char {
                in_string = false;
            }
            i += 1;
            continue;
        }

        match c {
            '\'' | '"' => {
                in_string = true;
                string_char = c;
                current.push(c);
            }
            ';' if depth == 0 => {
                let trimmed = current.trim();
                if !trimmed.is_empty() {
                    statements.push(current.clone());
                }
                current.clear();
            }
            _ => {
                let remaining = body[i..].to_uppercase();
                if remaining.starts_with("IF ")
                    || remaining.starts_with("IF\n")
                    || remaining.starts_with("IF\t")
                    || remaining.starts_with("BEGIN")
                    || remaining.starts_with("LOOP")
                    || remaining.starts_with("WHILE")
                    || remaining.starts_with("REPEAT")
                    || remaining.starts_with("FOR ")
                    || remaining.starts_with("CASE ")
                    || remaining.starts_with("CASE\n")
                {
                    let prev_word_boundary =
                        i == 0 || !chars[i - 1].is_alphanumeric() && chars[i - 1] != '_';
                    if prev_word_boundary {
                        depth += 1;
                    }
                }

                if remaining.starts_with("END IF")
                    || remaining.starts_with("END;")
                    || remaining.starts_with("END ")
                    || remaining.starts_with("END\n")
                    || remaining.starts_with("END LOOP")
                    || remaining.starts_with("END WHILE")
                    || remaining.starts_with("END REPEAT")
                    || remaining.starts_with("END FOR")
                    || remaining.starts_with("END CASE")
                {
                    let prev_word_boundary =
                        i == 0 || !chars[i - 1].is_alphanumeric() && chars[i - 1] != '_';
                    if prev_word_boundary && depth > 0 {
                        depth -= 1;
                    }
                }

                current.push(c);
            }
        }
        i += 1;
    }

    let trimmed = current.trim();
    if !trimmed.is_empty() {
        statements.push(current);
    }

    statements
}

#[derive(Debug, Clone)]
struct ParsedProcedure {
    name: String,
    or_replace: bool,
    parameters: Vec<ast::ProcedureParam>,
    body: String,
}

fn parse_create_procedure_begin_end(sql: &str) -> Option<ParsedProcedure> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if !upper.starts_with("CREATE") {
        return None;
    }

    let after_create = trimmed[6..].trim();
    let after_create_upper = after_create.to_uppercase();

    let (or_replace, rest) = if after_create_upper.starts_with("OR REPLACE") {
        (true, after_create[10..].trim())
    } else {
        (false, after_create)
    };

    let rest_upper = rest.to_uppercase();
    if !rest_upper.starts_with("PROCEDURE") {
        return None;
    }

    let after_procedure = rest[9..].trim();

    let paren_start = after_procedure.find('(')?;
    let name = after_procedure[..paren_start].trim().to_string();

    let paren_end = find_matching_paren(&after_procedure[paren_start..])?;
    let params_str = &after_procedure[paren_start + 1..paren_start + paren_end];
    let parameters = parse_procedure_params(params_str);

    let after_params = after_procedure[paren_start + paren_end + 1..].trim();
    let after_params_upper = after_params.to_uppercase();

    if !after_params_upper.starts_with("BEGIN") {
        return None;
    }

    let begin_pos = 5;
    let after_begin = &after_params[begin_pos..];

    let end_pos = find_procedure_end_position(after_begin)?;
    let body = after_begin[..end_pos].trim().to_string();

    Some(ParsedProcedure {
        name,
        or_replace,
        parameters,
        body,
    })
}

fn parse_procedure_params(params_str: &str) -> Vec<ast::ProcedureParam> {
    let mut params = Vec::new();
    let trimmed = params_str.trim();
    if trimmed.is_empty() {
        return params;
    }

    for part in split_params(trimmed) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        let upper = part.to_uppercase();
        let (mode, rest) = if upper.starts_with("IN ") && !upper.starts_with("INOUT") {
            (Some(ast::ArgMode::In), part[3..].trim())
        } else if upper.starts_with("OUT ") {
            (Some(ast::ArgMode::Out), part[4..].trim())
        } else if upper.starts_with("INOUT ") {
            (Some(ast::ArgMode::InOut), part[6..].trim())
        } else {
            (None, part)
        };

        let tokens: Vec<&str> = rest.split_whitespace().collect();
        if tokens.len() >= 2 {
            let param_name = tokens[0];
            let type_str = tokens[1..].join(" ");
            if let Some(data_type) = parse_sql_type(&type_str) {
                params.push(ast::ProcedureParam {
                    name: ast::Ident::new(param_name),
                    data_type,
                    mode,
                });
            }
        }
    }

    params
}

fn split_params(s: &str) -> Vec<String> {
    let mut result = Vec::new();
    let mut current = String::new();
    let mut depth = 0;

    for c in s.chars() {
        match c {
            '<' | '(' => {
                depth += 1;
                current.push(c);
            }
            '>' | ')' => {
                depth -= 1;
                current.push(c);
            }
            ',' if depth == 0 => {
                result.push(current.clone());
                current.clear();
            }
            _ => current.push(c),
        }
    }
    if !current.is_empty() {
        result.push(current);
    }
    result
}

fn parse_sql_type(type_str: &str) -> Option<ast::DataType> {
    let upper = type_str.to_uppercase();
    let upper = upper.trim();

    match upper {
        "INT64" | "INTEGER" | "INT" => Some(ast::DataType::Int64),
        "FLOAT64" | "FLOAT" | "DOUBLE" => Some(ast::DataType::Float64),
        "BOOL" | "BOOLEAN" => Some(ast::DataType::Bool),
        "STRING" | "TEXT" => Some(ast::DataType::String(None)),
        "BYTES" => Some(ast::DataType::Bytes(None)),
        "DATE" => Some(ast::DataType::Date),
        "DATETIME" => Some(ast::DataType::Datetime(None)),
        "TIME" => Some(ast::DataType::Time(None, ast::TimezoneInfo::None)),
        "TIMESTAMP" => Some(ast::DataType::Timestamp(None, ast::TimezoneInfo::None)),
        "NUMERIC" => Some(ast::DataType::Numeric(ast::ExactNumberInfo::None)),
        "JSON" => Some(ast::DataType::JSON),
        _ => {
            if upper.starts_with("ARRAY<") {
                Some(ast::DataType::Array(ast::ArrayElemTypeDef::None))
            } else if upper.starts_with("STRUCT<") {
                Some(ast::DataType::Struct(
                    vec![],
                    ast::StructBracketKind::AngleBrackets,
                ))
            } else {
                Some(ast::DataType::Custom(
                    ast::ObjectName(vec![ast::ObjectNamePart::Identifier(ast::Ident::new(
                        type_str,
                    ))]),
                    vec![],
                ))
            }
        }
    }
}

fn find_procedure_end_position(s: &str) -> Option<usize> {
    let upper = s.to_uppercase();
    let mut i = 0;
    let mut depth = 1;

    while i < upper.len() {
        let remaining = &upper[i..];

        let is_word_boundary = i == 0
            || !s
                .chars()
                .nth(i - 1)
                .map(|c| c.is_alphanumeric() || c == '_')
                .unwrap_or(false);

        if is_word_boundary {
            if remaining.starts_with("BEGIN") {
                let after = remaining.get(5..);
                if after
                    .map(|a| {
                        a.is_empty() || a.starts_with(char::is_whitespace) || a.starts_with(';')
                    })
                    .unwrap_or(true)
                {
                    depth += 1;
                }
            }

            if remaining.starts_with("END") {
                let after = remaining.get(3..);
                if after
                    .map(|a| {
                        a.is_empty() || a.starts_with(char::is_whitespace) || a.starts_with(';')
                    })
                    .unwrap_or(true)
                {
                    depth -= 1;
                    if depth == 0 {
                        return Some(i);
                    }
                }
            }
        }

        i += 1;
    }
    None
}

pub struct QueryExecutor {
    catalog: Catalog,
    variables: Vec<HashMap<String, ScriptVariable>>,
    system_variables: HashMap<String, Value>,
}

impl QueryExecutor {
    pub fn new() -> Self {
        let mut system_variables = HashMap::new();
        system_variables.insert("time_zone".to_string(), Value::string("UTC"));
        Self {
            catalog: Catalog::new(),
            variables: vec![HashMap::new()],
            system_variables,
        }
    }

    fn push_scope(&mut self) {
        self.variables.push(HashMap::new());
    }

    fn pop_scope(&mut self) {
        if self.variables.len() > 1 {
            self.variables.pop();
        }
    }

    fn get_variable(&self, name: &str) -> Option<&ScriptVariable> {
        let name_upper = name.to_uppercase();
        for scope in self.variables.iter().rev() {
            if let Some(var) = scope.get(&name_upper) {
                return Some(var);
            }
        }
        None
    }

    fn set_variable(&mut self, name: &str, value: Value) -> Result<()> {
        let name_upper = name.to_uppercase();
        for scope in self.variables.iter_mut().rev() {
            if scope.contains_key(&name_upper) {
                if let Some(var) = scope.get_mut(&name_upper) {
                    var.value = value;
                }
                return Ok(());
            }
        }
        if name.starts_with('@') {
            let data_type = value.data_type();
            self.declare_variable(name, data_type, Some(value));
            return Ok(());
        }
        Err(Error::ColumnNotFound(format!(
            "Variable '{}' not declared",
            name
        )))
    }

    fn declare_variable(&mut self, name: &str, data_type: DataType, default_value: Option<Value>) {
        let name_upper = name.to_uppercase();
        let value = default_value.unwrap_or(Value::null());
        if let Some(scope) = self.variables.last_mut() {
            scope.insert(name_upper, ScriptVariable { data_type, value });
        }
    }

    fn get_system_variable(&self, name: &str) -> Option<&Value> {
        self.system_variables.get(&name.to_lowercase())
    }

    fn set_system_variable(&mut self, name: &str, value: Value) {
        self.system_variables.insert(name.to_lowercase(), value);
    }

    fn value_to_sql_value(value: &Value) -> SqlValue {
        match value {
            Value::Null => SqlValue::Null,
            Value::Int64(n) => SqlValue::Number(n.to_string(), false),
            Value::Float64(f) => SqlValue::Number(f.to_string(), false),
            Value::String(s) => SqlValue::SingleQuotedString(s.clone()),
            Value::Bool(b) => SqlValue::Boolean(*b),
            Value::Numeric(d) => SqlValue::Number(d.to_string(), false),
            Value::Date(d) => SqlValue::SingleQuotedString(d.to_string()),
            Value::Timestamp(ts) => SqlValue::SingleQuotedString(ts.to_string()),
            Value::DateTime(dt) => SqlValue::SingleQuotedString(dt.to_string()),
            Value::Time(t) => SqlValue::SingleQuotedString(t.to_string()),
            Value::Bytes(b) => SqlValue::SingleQuotedByteStringLiteral(
                b.iter().map(|byte| format!("{:02x}", byte)).collect(),
            ),
            Value::Interval(_) => SqlValue::Null,
            Value::Array(_) => SqlValue::Null,
            Value::Struct(_) => SqlValue::Null,
            Value::Json(j) => SqlValue::SingleQuotedString(j.to_string()),
            Value::Geography(g) => SqlValue::SingleQuotedString(g.to_string()),
            Value::Range(_) => SqlValue::Null,
        }
    }

    fn substitute_variables(&self, expr: &Expr) -> Expr {
        match expr {
            Expr::Identifier(ident) => {
                let name = &ident.value;
                if let Some(var) = self.get_variable(name) {
                    Expr::Value(ast::ValueWithSpan {
                        value: Self::value_to_sql_value(&var.value),
                        span: Span::empty(),
                    })
                } else {
                    expr.clone()
                }
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 1 {
                    let name = &parts[0].value;
                    if let Some(var) = self.get_variable(name) {
                        return Expr::Value(ast::ValueWithSpan {
                            value: Self::value_to_sql_value(&var.value),
                            span: Span::empty(),
                        });
                    }
                }
                expr.clone()
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.substitute_variables(left)),
                op: op.clone(),
                right: Box::new(self.substitute_variables(right)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.substitute_variables(inner)),
            },
            Expr::Nested(inner) => Expr::Nested(Box::new(self.substitute_variables(inner))),
            Expr::IsNull(inner) => Expr::IsNull(Box::new(self.substitute_variables(inner))),
            Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(self.substitute_variables(inner))),
            Expr::Between {
                expr: inner,
                negated,
                low,
                high,
            } => Expr::Between {
                expr: Box::new(self.substitute_variables(inner)),
                negated: *negated,
                low: Box::new(self.substitute_variables(low)),
                high: Box::new(self.substitute_variables(high)),
            },
            Expr::InList {
                expr: inner,
                list,
                negated,
            } => Expr::InList {
                expr: Box::new(self.substitute_variables(inner)),
                list: list.iter().map(|e| self.substitute_variables(e)).collect(),
                negated: *negated,
            },
            Expr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } => Expr::Case {
                case_token: case_token.clone(),
                end_token: end_token.clone(),
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(self.substitute_variables(o))),
                conditions: conditions
                    .iter()
                    .map(|cw| ast::CaseWhen {
                        condition: self.substitute_variables(&cw.condition),
                        result: self.substitute_variables(&cw.result),
                    })
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(self.substitute_variables(e))),
            },
            Expr::Function(func) => {
                let mut new_func = func.clone();
                new_func.args = ast::FunctionArguments::List(ast::FunctionArgumentList {
                    duplicate_treatment: None,
                    args: match &func.args {
                        ast::FunctionArguments::None => vec![],
                        ast::FunctionArguments::Subquery(_) => return expr.clone(),
                        ast::FunctionArguments::List(list) => list
                            .args
                            .iter()
                            .map(|func_arg| match func_arg {
                                ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                    ast::FunctionArgExpr::Expr(e) => ast::FunctionArg::Unnamed(
                                        ast::FunctionArgExpr::Expr(self.substitute_variables(e)),
                                    ),
                                    _ => func_arg.clone(),
                                },
                                ast::FunctionArg::Named {
                                    name,
                                    arg: arg_expr,
                                    operator,
                                } => match arg_expr {
                                    ast::FunctionArgExpr::Expr(e) => ast::FunctionArg::Named {
                                        name: name.clone(),
                                        arg: ast::FunctionArgExpr::Expr(
                                            self.substitute_variables(e),
                                        ),
                                        operator: operator.clone(),
                                    },
                                    _ => func_arg.clone(),
                                },
                                _ => func_arg.clone(),
                            })
                            .collect(),
                    },
                    clauses: match &func.args {
                        ast::FunctionArguments::List(list) => list.clauses.clone(),
                        _ => vec![],
                    },
                });
                Expr::Function(new_func)
            }
            Expr::Cast {
                expr: inner,
                data_type,
                format,
                kind,
            } => Expr::Cast {
                expr: Box::new(self.substitute_variables(inner)),
                data_type: data_type.clone(),
                format: format.clone(),
                kind: kind.clone(),
            },
            _ => expr.clone(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Table> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        if upper.starts_with("CREATE SNAPSHOT TABLE") {
            return self.execute_create_snapshot(trimmed);
        }
        if upper.starts_with("DROP SNAPSHOT TABLE") {
            return self.execute_drop_snapshot(trimmed);
        }
        if let Some(load_info) = parse_load_data(sql) {
            return self.execute_load_data(&load_info);
        }

        if let Some(loop_stmt) = parse_loop_statement(trimmed) {
            return self.execute_loop(&loop_stmt).map(|(t, _)| t);
        }
        if let Some(while_stmt) = parse_while_do_statement(trimmed) {
            return self.execute_while_do(&while_stmt).map(|(t, _)| t);
        }
        if let Some(repeat_stmt) = parse_repeat_statement(trimmed) {
            return self.execute_repeat(&repeat_stmt).map(|(t, _)| t);
        }
        if let Some(for_stmt) = parse_for_statement(trimmed) {
            return self.execute_for(&for_stmt).map(|(t, _)| t);
        }
        if let Some((is_leave, label)) = is_leave_or_iterate_statement(trimmed) {
            return self
                .execute_leave_iterate(is_leave, label.as_deref())
                .map(|(t, _)| t);
        }

        if let Some(proc) = parse_create_procedure_begin_end(trimmed) {
            return self.execute_create_procedure_parsed(&proc);
        }

        let dialect = BigQueryDialect {};
        let preprocessed_sql = preprocess_range_types(sql);
        let statements = Parser::parse_sql(&dialect, &preprocessed_sql)
            .map_err(|e| Error::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(Error::ParseError("Empty SQL statement".to_string()));
        }

        self.execute_statement(&statements[0])
    }

    fn execute_create_snapshot(&mut self, sql: &str) -> Result<Table> {
        let upper = sql.to_uppercase();
        let if_not_exists = upper.contains("IF NOT EXISTS");

        let rest = if if_not_exists {
            let idx = upper.find("IF NOT EXISTS").unwrap() + 13;
            sql[idx..].trim()
        } else {
            let idx = upper.find("CREATE SNAPSHOT TABLE").unwrap() + 21;
            sql[idx..].trim()
        };

        let clone_idx = rest.to_uppercase().find("CLONE").ok_or_else(|| {
            Error::ParseError("CREATE SNAPSHOT TABLE requires CLONE clause".to_string())
        })?;

        let snapshot_name = rest[..clone_idx].trim().to_string();
        let after_clone = rest[clone_idx + 5..].trim();

        let (source_name, _options) =
            if let Some(for_idx) = after_clone.to_uppercase().find("FOR SYSTEM_TIME") {
                (after_clone[..for_idx].trim().to_string(), String::new())
            } else if let Some(opt_idx) = after_clone.to_uppercase().find("OPTIONS") {
                (
                    after_clone[..opt_idx].trim().to_string(),
                    after_clone[opt_idx..].to_string(),
                )
            } else {
                (after_clone.to_string(), String::new())
            };

        if if_not_exists && self.catalog.get_table(&snapshot_name).is_some() {
            return Ok(Table::new(Schema::new()));
        }

        let source_table = self
            .catalog
            .get_table(&source_name)
            .ok_or_else(|| Error::TableNotFound(source_name.clone()))?;

        let snapshot = source_table.clone();
        self.catalog.insert_table(&snapshot_name, snapshot)?;

        Ok(Table::new(Schema::new()))
    }

    fn execute_drop_snapshot(&mut self, sql: &str) -> Result<Table> {
        let upper = sql.to_uppercase();
        let if_exists = upper.contains("IF EXISTS");

        let rest = if if_exists {
            let idx = upper.find("IF EXISTS").unwrap() + 9;
            sql[idx..].trim()
        } else {
            let idx = upper.find("DROP SNAPSHOT TABLE").unwrap() + 19;
            sql[idx..].trim()
        };

        let snapshot_name = rest.trim().to_string();

        if if_exists && self.catalog.get_table(&snapshot_name).is_none() {
            return Ok(Table::new(Schema::new()));
        }

        self.catalog
            .drop_table(&snapshot_name)
            .map_err(|_| Error::TableNotFound(snapshot_name))?;

        Ok(Table::new(Schema::new()))
    }

    fn execute_statement(&mut self, stmt: &Statement) -> Result<Table> {
        match self.execute_statement_internal(stmt) {
            Ok((table, _control)) => Ok(table),
            Err(e) => Err(e),
        }
    }

    fn execute_statement_internal(
        &mut self,
        stmt: &Statement,
    ) -> Result<(Table, Option<LoopControl>)> {
        match stmt {
            Statement::Query(query) => match query.body.as_ref() {
                SetExpr::Insert(_) | SetExpr::Update(_) | SetExpr::Delete(_) => {
                    self.execute_query_dml(query).map(|t| (t, None))
                }
                _ => self.execute_query(query).map(|t| (t, None)),
            },
            Statement::CreateTable(create) => self.execute_create_table(create).map(|t| (t, None)),
            Statement::CreateView {
                name,
                columns,
                query,
                or_replace,
                if_not_exists,
                materialized,
                ..
            } => self
                .execute_create_view(
                    name,
                    columns,
                    query,
                    *or_replace,
                    *if_not_exists,
                    *materialized,
                )
                .map(|t| (t, None)),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                cascade,
                ..
            } => self
                .execute_drop(object_type, names, *if_exists, *cascade)
                .map(|t| (t, None)),
            Statement::Insert(insert) => self.execute_insert(insert).map(|t| (t, None)),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self
                .execute_update(table, assignments, selection.as_ref())
                .map(|t| (t, None)),
            Statement::Delete(delete) => self.execute_delete(delete).map(|t| (t, None)),
            Statement::Truncate { table_names, .. } => {
                self.execute_truncate(table_names).map(|t| (t, None))
            }
            Statement::AlterTable {
                name, operations, ..
            } => self
                .execute_alter_table(name, operations)
                .map(|t| (t, None)),
            Statement::CreateFunction(create_func) => {
                self.execute_create_function(create_func).map(|t| (t, None))
            }
            Statement::DropFunction {
                if_exists,
                func_desc,
                ..
            } => self
                .execute_drop_function(func_desc, *if_exists)
                .map(|t| (t, None)),
            Statement::CreateProcedure {
                or_alter,
                name,
                params,
                body,
                ..
            } => self
                .execute_create_procedure(name, params, body, *or_alter)
                .map(|t| (t, None)),
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                ..
            } => self
                .execute_drop_procedure(proc_desc, *if_exists)
                .map(|t| (t, None)),
            Statement::Call(func) => self.execute_call(func).map(|t| (t, None)),
            Statement::ExportData(export_data) => {
                self.execute_export_data(export_data).map(|t| (t, None))
            }
            Statement::CreateSchema {
                schema_name,
                if_not_exists,
                options,
                ..
            } => self
                .execute_create_schema(schema_name, *if_not_exists, options)
                .map(|t| (t, None)),
            Statement::AlterSchema(alter_schema) => {
                self.execute_alter_schema(alter_schema).map(|t| (t, None))
            }
            Statement::Merge { .. } => self.execute_merge(stmt).map(|t| (t, None)),
            Statement::Declare { stmts } => self.execute_declare(stmts).map(|t| (t, None)),
            Statement::Set(set_stmt) => self.execute_set_statement(set_stmt).map(|t| (t, None)),
            Statement::If(if_stmt) => self.execute_if(if_stmt),
            Statement::While(while_stmt) => self.execute_while(while_stmt),
            Statement::StartTransaction {
                statements,
                exception,
                ..
            } => self.execute_begin_block(statements, exception.as_ref()),
            Statement::Return(ret) => self.execute_return(ret),
            Statement::Raise(raise) => self.execute_raise(raise),
            Statement::Execute {
                parameters,
                immediate,
                ..
            } if *immediate => self.execute_immediate(parameters),
            Statement::Case(case_stmt) => self.execute_case_statement(case_stmt),
            Statement::Assert { condition, message } => {
                self.execute_assert(condition, message.as_ref())
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    fn execute_assert(
        &self,
        condition: &Expr,
        message: Option<&Expr>,
    ) -> Result<(Table, Option<LoopControl>)> {
        let cond_val = self.evaluate_assert_expr(condition)?;
        let is_true = cond_val.as_bool().unwrap_or(false);

        if is_true {
            Ok((Table::empty(Schema::new()), None))
        } else {
            let msg = message
                .map(|m| self.evaluate_assert_expr(m))
                .transpose()?
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "Assertion failed".to_string());
            Err(Error::InvalidQuery(format!("ASSERT failed: {}", msg)))
        }
    }

    fn evaluate_assert_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Subquery(query) => {
                let result = self.execute_query(query)?;
                let records = result.to_records()?;
                if records.is_empty() {
                    return Ok(Value::null());
                }
                let first_record = &records[0];
                let values = first_record.values();
                if values.is_empty() {
                    return Ok(Value::null());
                }
                Ok(values[0].clone())
            }
            Expr::Nested(inner) => self.evaluate_assert_expr(inner),
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_assert_expr(left)?;
                let right_val = self.evaluate_assert_expr(right)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }
            _ => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = Evaluator::new(&empty_schema);
                evaluator.evaluate(expr, &empty_record)
            }
        }
    }

    fn execute_query(&self, query: &Query) -> Result<Table> {
        let cte_tables = self.materialize_ctes(&query.with)?;
        self.execute_query_with_ctes(query, &cte_tables)
    }

    fn execute_query_dml(&mut self, query: &Query) -> Result<Table> {
        let cte_tables = self.materialize_ctes(&query.with)?;
        match query.body.as_ref() {
            SetExpr::Insert(Statement::Insert(insert)) => {
                self.execute_insert_with_ctes(insert, &cte_tables)
            }
            SetExpr::Update(Statement::Update {
                table,
                assignments,
                selection,
                ..
            }) => {
                self.execute_update_with_ctes(table, assignments, selection.as_ref(), &cte_tables)
            }
            SetExpr::Delete(Statement::Delete(delete)) => {
                self.execute_delete_with_ctes(delete, &cte_tables)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "DML type not supported in query: {:?}",
                query.body
            ))),
        }
    }

    fn execute_view_query(&self, query_sql: &str) -> Result<Table> {
        let dialect = BigQueryDialect {};
        let statements =
            Parser::parse_sql(&dialect, query_sql).map_err(|e| Error::ParseError(e.to_string()))?;
        match statements.first() {
            Some(Statement::Query(query)) => self.execute_query(query),
            _ => Err(Error::ParseError("View query must be a SELECT".to_string())),
        }
    }

    fn materialize_ctes(&self, with_clause: &Option<ast::With>) -> Result<HashMap<String, Table>> {
        let mut cte_tables = HashMap::new();
        if let Some(with) = with_clause {
            for cte in &with.cte_tables {
                let name = cte.alias.name.value.to_uppercase();
                let column_aliases: Vec<String> = cte
                    .alias
                    .columns
                    .iter()
                    .map(|c| c.name.value.clone())
                    .collect();

                let cte_result = if with.recursive && self.is_recursive_cte(&cte.query, &name) {
                    self.execute_recursive_cte(&cte.query, &name, &cte_tables)?
                } else {
                    self.execute_query_with_ctes(&cte.query, &cte_tables)?
                };

                let cte_result = if !column_aliases.is_empty() {
                    self.rename_columns(cte_result, &column_aliases)?
                } else {
                    cte_result
                };

                cte_tables.insert(name, cte_result);
            }
        }
        Ok(cte_tables)
    }

    fn rename_columns(&self, table: Table, new_names: &[String]) -> Result<Table> {
        let old_schema = table.schema();
        if new_names.len() != old_schema.field_count() {
            return Err(Error::InvalidQuery(format!(
                "CTE column list has {} names but query returns {} columns",
                new_names.len(),
                old_schema.field_count()
            )));
        }

        let new_fields: Vec<Field> = old_schema
            .fields()
            .iter()
            .zip(new_names.iter())
            .map(|(f, new_name)| Field::nullable(new_name.clone(), f.data_type.clone()))
            .collect();
        let new_schema = Schema::from_fields(new_fields);

        let rows = table.to_records()?;
        let values: Vec<Vec<Value>> = rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(new_schema, values)
    }

    fn is_recursive_cte(&self, query: &Query, cte_name: &str) -> bool {
        self.set_expr_references_cte(query.body.as_ref(), cte_name)
    }

    fn set_expr_references_cte(&self, set_expr: &SetExpr, cte_name: &str) -> bool {
        match set_expr {
            SetExpr::Select(select) => self.select_references_cte(select, cte_name),
            SetExpr::SetOperation { left, right, .. } => {
                self.set_expr_references_cte(left, cte_name)
                    || self.set_expr_references_cte(right, cte_name)
            }
            SetExpr::Query(q) => self.set_expr_references_cte(q.body.as_ref(), cte_name),
            _ => false,
        }
    }

    fn select_references_cte(&self, select: &Select, cte_name: &str) -> bool {
        for table_with_joins in &select.from {
            if self.table_factor_references_cte(&table_with_joins.relation, cte_name) {
                return true;
            }
            for join in &table_with_joins.joins {
                if self.table_factor_references_cte(&join.relation, cte_name) {
                    return true;
                }
            }
        }
        false
    }

    fn table_factor_references_cte(&self, table_factor: &TableFactor, cte_name: &str) -> bool {
        match table_factor {
            TableFactor::Table { name, .. } => name.to_string().to_uppercase() == cte_name,
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                self.table_factor_references_cte(&table_with_joins.relation, cte_name)
                    || table_with_joins
                        .joins
                        .iter()
                        .any(|j| self.table_factor_references_cte(&j.relation, cte_name))
            }
            TableFactor::Derived { subquery, .. } => {
                self.set_expr_references_cte(subquery.body.as_ref(), cte_name)
            }
            _ => false,
        }
    }

    fn execute_recursive_cte(
        &self,
        query: &Query,
        cte_name: &str,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        const MAX_ITERATIONS: usize = 500;

        let (anchor_exprs, recursive_exprs) =
            self.split_recursive_cte(query.body.as_ref(), cte_name)?;

        if anchor_exprs.is_empty() {
            return Err(Error::InvalidQuery(
                "Recursive CTE must have an anchor member".to_string(),
            ));
        }

        let mut combined_result: Option<Table> = None;
        for anchor_expr in &anchor_exprs {
            let anchor_result = self.execute_set_expr(anchor_expr, cte_tables)?;
            combined_result = Some(match combined_result {
                None => anchor_result,
                Some(existing) => self.union_tables(existing, anchor_result)?,
            });
        }

        let anchor_schema = combined_result
            .as_ref()
            .ok_or_else(|| Error::InvalidQuery("Anchor produced no result".to_string()))?
            .schema()
            .clone();

        let mut working_table = combined_result.clone().unwrap();

        for iteration in 0..MAX_ITERATIONS {
            let mut new_rows: Option<Table> = None;
            let mut cte_tables_with_self = cte_tables.clone();
            cte_tables_with_self.insert(cte_name.to_string(), working_table.clone());

            for recursive_expr in &recursive_exprs {
                let iter_result = self.execute_set_expr(recursive_expr, &cte_tables_with_self)?;
                let iter_result = self.coerce_table_schema(iter_result, &anchor_schema)?;
                new_rows = Some(match new_rows {
                    None => iter_result,
                    Some(existing) => self.union_tables(existing, iter_result)?,
                });
            }

            let new_rows = match new_rows {
                Some(t) if t.row_count() > 0 => t,
                _ => break,
            };

            combined_result = Some(match combined_result {
                None => new_rows.clone(),
                Some(existing) => self.union_tables(existing, new_rows.clone())?,
            });

            working_table = new_rows;

            if iteration == MAX_ITERATIONS - 1 {
                return Err(Error::InvalidQuery(format!(
                    "Recursive CTE exceeded maximum iterations ({})",
                    MAX_ITERATIONS
                )));
            }
        }

        combined_result
            .ok_or_else(|| Error::InvalidQuery("Recursive CTE produced no result".to_string()))
    }

    fn coerce_table_schema(&self, table: Table, target_schema: &Schema) -> Result<Table> {
        let source_schema = table.schema();
        if source_schema.field_count() != target_schema.field_count() {
            return Err(Error::InvalidQuery(format!(
                "Recursive CTE member has {} columns but anchor has {}",
                source_schema.field_count(),
                target_schema.field_count()
            )));
        }

        let merged_fields: Vec<Field> = target_schema
            .fields()
            .iter()
            .zip(source_schema.fields().iter())
            .map(|(tgt_field, src_field)| {
                let data_type = match &tgt_field.data_type {
                    DataType::Unknown => src_field.data_type.clone(),
                    dt => dt.clone(),
                };
                Field::nullable(tgt_field.name.clone(), data_type)
            })
            .collect();
        let merged_schema = Schema::from_fields(merged_fields);

        let rows = table.to_records()?;
        let values: Vec<Vec<Value>> = rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(merged_schema, values)
    }

    fn types_compatible(&self, src: &DataType, tgt: &DataType) -> bool {
        match (src, tgt) {
            (DataType::Unknown, _) | (_, DataType::Unknown) => true,
            (a, b) => a == b,
        }
    }

    fn split_recursive_cte<'a>(
        &self,
        set_expr: &'a SetExpr,
        cte_name: &str,
    ) -> Result<(Vec<&'a SetExpr>, Vec<&'a SetExpr>)> {
        let mut anchor_parts = Vec::new();
        let mut recursive_parts = Vec::new();

        self.collect_union_parts(set_expr, cte_name, &mut anchor_parts, &mut recursive_parts);

        Ok((anchor_parts, recursive_parts))
    }

    fn collect_union_parts<'a>(
        &self,
        set_expr: &'a SetExpr,
        cte_name: &str,
        anchor_parts: &mut Vec<&'a SetExpr>,
        recursive_parts: &mut Vec<&'a SetExpr>,
    ) {
        match set_expr {
            SetExpr::SetOperation {
                op: ast::SetOperator::Union,
                left,
                right,
                ..
            } => {
                self.collect_union_parts(left, cte_name, anchor_parts, recursive_parts);
                self.collect_union_parts(right, cte_name, anchor_parts, recursive_parts);
            }
            _ => {
                if self.set_expr_references_cte(set_expr, cte_name) {
                    recursive_parts.push(set_expr);
                } else {
                    anchor_parts.push(set_expr);
                }
            }
        }
    }

    fn union_tables(&self, left: Table, right: Table) -> Result<Table> {
        let left_schema = left.schema();
        let right_schema = right.schema();

        let merged_fields: Vec<Field> = left_schema
            .fields()
            .iter()
            .zip(right_schema.fields().iter())
            .map(|(left_field, right_field)| {
                let data_type = match &left_field.data_type {
                    DataType::Unknown => right_field.data_type.clone(),
                    dt => dt.clone(),
                };
                Field::nullable(left_field.name.clone(), data_type)
            })
            .collect();
        let merged_schema = Schema::from_fields(merged_fields);

        let mut left_rows = left.to_records()?;
        let right_rows = right.to_records()?;
        left_rows.extend(right_rows);

        let values: Vec<Vec<Value>> = left_rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(merged_schema, values)
    }

    fn execute_query_with_ctes(
        &self,
        query: &Query,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        match query.body.as_ref() {
            SetExpr::Select(select) => self.execute_select_with_ctes(
                select,
                &query.order_by,
                &query.limit_clause,
                cte_tables,
            ),
            SetExpr::Values(values) => self.execute_values(values),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => {
                let result =
                    self.execute_set_operation(op, set_quantifier, left, right, cte_tables)?;
                self.apply_order_and_limit(result, &query.order_by, &query.limit_clause)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Query type not yet supported: {:?}",
                query.body
            ))),
        }
    }

    fn apply_order_and_limit(
        &self,
        table: Table,
        order_by: &Option<OrderBy>,
        limit_clause: &Option<LimitClause>,
    ) -> Result<Table> {
        let schema = table.schema().clone();
        let mut rows = table.to_records()?;

        if let Some(order_by) = order_by {
            self.sort_rows(&schema, &mut rows, order_by)?;
        }

        if let Some(limit_clause) = limit_clause {
            match limit_clause {
                LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(offset_expr) = offset {
                        let offset_val = self.evaluate_literal_expr(&offset_expr.value)?;
                        let offset_num = offset_val.as_i64().ok_or_else(|| {
                            Error::InvalidQuery("OFFSET must be an integer".to_string())
                        })? as usize;
                        if offset_num < rows.len() {
                            rows = rows.into_iter().skip(offset_num).collect();
                        } else {
                            rows.clear();
                        }
                    }
                    if let Some(limit_expr) = limit {
                        let limit_val = self.evaluate_literal_expr(limit_expr)?;
                        let limit_num = limit_val.as_i64().ok_or_else(|| {
                            Error::InvalidQuery("LIMIT must be an integer".to_string())
                        })? as usize;
                        rows.truncate(limit_num);
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    let offset_val = self.evaluate_literal_expr(offset)?;
                    let offset_num = offset_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery("OFFSET must be an integer".to_string())
                    })? as usize;
                    if offset_num < rows.len() {
                        rows = rows.into_iter().skip(offset_num).collect();
                    } else {
                        rows.clear();
                    }
                    let limit_val = self.evaluate_literal_expr(limit)?;
                    let limit_num = limit_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery("LIMIT must be an integer".to_string())
                    })? as usize;
                    rows.truncate(limit_num);
                }
            }
        }

        let values: Vec<Vec<Value>> = rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(schema, values)
    }

    fn execute_set_operation(
        &self,
        op: &ast::SetOperator,
        set_quantifier: &ast::SetQuantifier,
        left: &SetExpr,
        right: &SetExpr,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        let left_result = self.execute_set_expr(left, cte_tables)?;
        let right_result = self.execute_set_expr(right, cte_tables)?;

        let left_schema = left_result.schema().clone();
        let right_schema = right_result.schema().clone();

        if left_schema.field_count() != right_schema.field_count() {
            return Err(Error::InvalidQuery(format!(
                "UNION operands have different column counts: {} vs {}",
                left_schema.field_count(),
                right_schema.field_count()
            )));
        }

        let mut left_rows = left_result.to_records()?;
        let right_rows = right_result.to_records()?;

        match op {
            ast::SetOperator::Union => {
                left_rows.extend(right_rows);
                match set_quantifier {
                    ast::SetQuantifier::All | ast::SetQuantifier::ByName => {}
                    ast::SetQuantifier::Distinct
                    | ast::SetQuantifier::None
                    | ast::SetQuantifier::DistinctByName
                    | ast::SetQuantifier::AllByName => {
                        let mut seen = std::collections::HashSet::new();
                        left_rows.retain(|row| {
                            let key = format!("{:?}", row.values());
                            seen.insert(key)
                        });
                    }
                }
            }
            ast::SetOperator::Intersect => {
                let is_all = matches!(
                    set_quantifier,
                    ast::SetQuantifier::All | ast::SetQuantifier::ByName
                );
                if is_all {
                    let mut right_counts: HashMap<String, usize> = HashMap::new();
                    for row in &right_rows {
                        let key = format!("{:?}", row.values());
                        *right_counts.entry(key).or_insert(0) += 1;
                    }
                    let mut result_rows = Vec::new();
                    for row in left_rows {
                        let key = format!("{:?}", row.values());
                        if let Some(count) = right_counts.get_mut(&key) {
                            if *count > 0 {
                                result_rows.push(row);
                                *count -= 1;
                            }
                        }
                    }
                    left_rows = result_rows;
                } else {
                    let right_set: std::collections::HashSet<String> = right_rows
                        .iter()
                        .map(|row| format!("{:?}", row.values()))
                        .collect();
                    let mut seen = std::collections::HashSet::new();
                    left_rows.retain(|row| {
                        let key = format!("{:?}", row.values());
                        right_set.contains(&key) && seen.insert(key)
                    });
                }
            }
            ast::SetOperator::Except | ast::SetOperator::Minus => {
                let is_all = matches!(
                    set_quantifier,
                    ast::SetQuantifier::All | ast::SetQuantifier::ByName
                );
                if is_all {
                    let mut right_counts: HashMap<String, usize> = HashMap::new();
                    for row in &right_rows {
                        let key = format!("{:?}", row.values());
                        *right_counts.entry(key).or_insert(0) += 1;
                    }
                    let mut result_rows = Vec::new();
                    for row in left_rows {
                        let key = format!("{:?}", row.values());
                        if let Some(count) = right_counts.get_mut(&key) {
                            if *count > 0 {
                                *count -= 1;
                                continue;
                            }
                        }
                        result_rows.push(row);
                    }
                    left_rows = result_rows;
                } else {
                    let right_set: std::collections::HashSet<String> = right_rows
                        .iter()
                        .map(|row| format!("{:?}", row.values()))
                        .collect();
                    let mut seen = std::collections::HashSet::new();
                    left_rows.retain(|row| {
                        let key = format!("{:?}", row.values());
                        !right_set.contains(&key) && seen.insert(key)
                    });
                }
            }
        }

        let values: Vec<Vec<Value>> = left_rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(left_schema, values)
    }

    fn execute_set_expr(
        &self,
        set_expr: &SetExpr,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        match set_expr {
            SetExpr::Select(select) => {
                self.execute_select_with_ctes(select, &None, &None, cte_tables)
            }
            SetExpr::Values(values) => self.execute_values(values),
            SetExpr::SetOperation {
                op,
                set_quantifier,
                left,
                right,
            } => self.execute_set_operation(op, set_quantifier, left, right, cte_tables),
            SetExpr::Query(query) => self.execute_query_with_ctes(query, cte_tables),
            _ => Err(Error::UnsupportedFeature(format!(
                "SetExpr type not yet supported: {:?}",
                set_expr
            ))),
        }
    }

    fn execute_select(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
        limit_clause: &Option<LimitClause>,
    ) -> Result<Table> {
        self.execute_select_with_ctes(select, order_by, limit_clause, &HashMap::new())
    }

    fn execute_select_with_ctes(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
        limit_clause: &Option<LimitClause>,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        let (pre_projection_schema, mut rows, do_projection) = if select.from.is_empty() {
            let (schema, rows) = self.evaluate_select_without_from(select, cte_tables)?;
            (schema, rows, false)
        } else {
            self.evaluate_select_with_from_for_ordering_ctes(select, order_by, cte_tables)?
        };

        if let Some(order_by) = order_by {
            self.sort_rows(&pre_projection_schema, &mut rows, order_by)?;
        }

        let (schema, mut rows) = if do_projection {
            let (schema, mut projected_rows) =
                self.project_rows(&pre_projection_schema, &rows, &select.projection)?;
            if select.distinct.is_some() {
                let mut seen = std::collections::HashSet::new();
                projected_rows.retain(|row| {
                    let key = format!("{:?}", row.values());
                    seen.insert(key)
                });
            }
            (schema, projected_rows)
        } else {
            (pre_projection_schema, rows)
        };

        if let Some(limit_clause) = limit_clause {
            match limit_clause {
                LimitClause::LimitOffset { limit, offset, .. } => {
                    if let Some(offset_expr) = offset {
                        let offset_val = self.evaluate_literal_expr(&offset_expr.value)?;
                        let offset_num = offset_val.as_i64().ok_or_else(|| {
                            Error::InvalidQuery("OFFSET must be an integer".to_string())
                        })? as usize;
                        if offset_num < rows.len() {
                            rows = rows.into_iter().skip(offset_num).collect();
                        } else {
                            rows.clear();
                        }
                    }
                    if let Some(limit_expr) = limit {
                        let limit_val = self.evaluate_literal_expr(limit_expr)?;
                        let limit_num = limit_val.as_i64().ok_or_else(|| {
                            Error::InvalidQuery("LIMIT must be an integer".to_string())
                        })? as usize;
                        rows.truncate(limit_num);
                    }
                }
                LimitClause::OffsetCommaLimit { offset, limit } => {
                    let offset_val = self.evaluate_literal_expr(offset)?;
                    let offset_num = offset_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery("OFFSET must be an integer".to_string())
                    })? as usize;
                    if offset_num < rows.len() {
                        rows = rows.into_iter().skip(offset_num).collect();
                    } else {
                        rows.clear();
                    }
                    let limit_val = self.evaluate_literal_expr(limit)?;
                    let limit_num = limit_val.as_i64().ok_or_else(|| {
                        Error::InvalidQuery("LIMIT must be an integer".to_string())
                    })? as usize;
                    rows.truncate(limit_num);
                }
            }
        }

        let values: Vec<Vec<Value>> = rows.into_iter().map(|r| r.into_values()).collect();
        Table::from_values(schema, values)
    }

    fn evaluate_select_without_from(
        &self,
        select: &Select,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>)> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = Evaluator::with_user_functions(&empty_schema, self.catalog.get_functions());

        let mut result_values = Vec::new();
        let mut field_names = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let resolved_expr =
                        self.resolve_scalar_subqueries_with_ctes(expr, cte_tables)?;
                    let val =
                        self.evaluate_with_variables(&evaluator, &resolved_expr, &empty_record)?;
                    result_values.push(val.clone());
                    field_names.push(self.expr_to_alias(expr, idx));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let resolved_expr =
                        self.resolve_scalar_subqueries_with_ctes(expr, cte_tables)?;
                    let val =
                        self.evaluate_with_variables(&evaluator, &resolved_expr, &empty_record)?;
                    result_values.push(val.clone());
                    field_names.push(alias.value.clone());
                }
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported projection item".to_string(),
                    ));
                }
            }
        }

        let fields: Vec<Field> = result_values
            .iter()
            .zip(field_names.iter())
            .map(|(val, name)| Field::nullable(name.clone(), val.data_type()))
            .collect();

        let schema = Schema::from_fields(fields);
        let row = Record::from_values(result_values);

        Ok((schema, vec![row]))
    }

    fn evaluate_select_with_from(&self, select: &Select) -> Result<(Schema, Vec<Record>)> {
        let (input_schema, input_rows) = self.get_from_data(&select.from)?;
        let evaluator = Evaluator::with_user_functions(&input_schema, self.catalog.get_functions());

        let mut filtered_rows: Vec<Record> = if let Some(selection) = &select.selection {
            let substituted_selection = self.substitute_variables(selection);
            let resolved_selection = self.resolve_scalar_subqueries(&substituted_selection)?;
            input_rows
                .iter()
                .filter(|row| {
                    evaluator
                        .evaluate_to_bool(&resolved_selection, row)
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            input_rows.clone()
        };

        let has_aggregates = self.has_aggregate_functions(&select.projection);
        let has_group_by = !matches!(select.group_by, ast::GroupByExpr::Expressions(ref v, _) if v.is_empty())
            && !matches!(select.group_by, ast::GroupByExpr::All(_));

        if has_aggregates || has_group_by {
            return self.execute_aggregate_query_with_ctes(
                &input_schema,
                &filtered_rows,
                select,
                &HashMap::new(),
            );
        }

        let has_window_funcs = self.has_window_functions(&select.projection);
        let has_qualify = select.qualify.is_some();
        let qualify_has_window = select
            .qualify
            .as_ref()
            .is_some_and(|q| self.expr_has_window_function(q));

        if has_window_funcs || qualify_has_window {
            let window_results = self.compute_window_functions_with_qualify(
                &input_schema,
                &filtered_rows,
                &select.projection,
                select.qualify.as_ref(),
            )?;

            let rows_after_qualify = if let Some(qualify_expr) = &select.qualify {
                let qualifying_indices = self.apply_qualify_filter(
                    &input_schema,
                    &filtered_rows,
                    qualify_expr,
                    &window_results,
                )?;

                let filtered: Vec<Record> = qualifying_indices
                    .iter()
                    .map(|&i| filtered_rows[i].clone())
                    .collect();

                let new_window_results: HashMap<String, Vec<Value>> = window_results
                    .iter()
                    .map(|(k, v)| {
                        let filtered_v: Vec<Value> =
                            qualifying_indices.iter().map(|&i| v[i].clone()).collect();
                        (k.clone(), filtered_v)
                    })
                    .collect();

                return self.project_rows_with_windows(
                    &input_schema,
                    &filtered,
                    &select.projection,
                    &new_window_results,
                );
            } else {
                filtered_rows.clone()
            };

            return self.project_rows_with_windows(
                &input_schema,
                &rows_after_qualify,
                &select.projection,
                &window_results,
            );
        }

        if has_qualify {
            let evaluator =
                Evaluator::with_user_functions(&input_schema, self.catalog.get_functions());
            let qualify_expr = select.qualify.as_ref().unwrap();
            filtered_rows.retain(|row| {
                evaluator
                    .evaluate_to_bool(qualify_expr, row)
                    .unwrap_or(false)
            });
        }

        let (output_schema, mut output_rows) =
            self.project_rows(&input_schema, &filtered_rows, &select.projection)?;

        if select.distinct.is_some() {
            let mut seen = std::collections::HashSet::new();
            output_rows.retain(|row| {
                let key = format!("{:?}", row.values());
                seen.insert(key)
            });
        }

        Ok((output_schema, output_rows))
    }

    fn evaluate_select_with_from_for_ordering(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
    ) -> Result<(Schema, Vec<Record>, bool)> {
        self.evaluate_select_with_from_for_ordering_ctes(select, order_by, &HashMap::new())
    }

    fn evaluate_select_with_from_for_ordering_ctes(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>, bool)> {
        let (input_schema, input_rows) = self.get_from_data_ctes(&select.from, cte_tables)?;
        let evaluator = Evaluator::with_user_functions(&input_schema, self.catalog.get_functions());

        let mut filtered_rows: Vec<Record> = if let Some(selection) = &select.selection {
            let substituted_selection = self.substitute_variables(selection);
            let resolved_selection = self.resolve_scalar_subqueries(&substituted_selection)?;
            input_rows
                .iter()
                .filter(|row| {
                    evaluator
                        .evaluate_to_bool(&resolved_selection, row)
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            input_rows.clone()
        };

        let has_aggregates = self.has_aggregate_functions(&select.projection);
        let has_group_by = !matches!(select.group_by, ast::GroupByExpr::Expressions(ref v, _) if v.is_empty())
            && !matches!(select.group_by, ast::GroupByExpr::All(_));

        if has_aggregates || has_group_by {
            let (schema, rows) = self.execute_aggregate_query_with_ctes(
                &input_schema,
                &filtered_rows,
                select,
                cte_tables,
            )?;
            return Ok((schema, rows, false));
        }

        let has_window_funcs = self.has_window_functions(&select.projection);
        let has_qualify = select.qualify.is_some();
        let qualify_has_window = select
            .qualify
            .as_ref()
            .is_some_and(|q| self.expr_has_window_function(q));

        if has_window_funcs || qualify_has_window {
            let window_results = self.compute_window_functions_with_qualify(
                &input_schema,
                &filtered_rows,
                &select.projection,
                select.qualify.as_ref(),
            )?;

            if let Some(qualify_expr) = &select.qualify {
                let qualifying_indices = self.apply_qualify_filter(
                    &input_schema,
                    &filtered_rows,
                    qualify_expr,
                    &window_results,
                )?;

                let filtered: Vec<Record> = qualifying_indices
                    .iter()
                    .map(|&i| filtered_rows[i].clone())
                    .collect();

                let new_window_results: HashMap<String, Vec<Value>> = window_results
                    .iter()
                    .map(|(k, v)| {
                        let filtered_v: Vec<Value> =
                            qualifying_indices.iter().map(|&i| v[i].clone()).collect();
                        (k.clone(), filtered_v)
                    })
                    .collect();

                let (schema, rows) = self.project_rows_with_windows(
                    &input_schema,
                    &filtered,
                    &select.projection,
                    &new_window_results,
                )?;
                return Ok((schema, rows, false));
            }

            let (schema, rows) = self.project_rows_with_windows(
                &input_schema,
                &filtered_rows,
                &select.projection,
                &window_results,
            )?;
            return Ok((schema, rows, false));
        }

        if has_qualify {
            let evaluator =
                Evaluator::with_user_functions(&input_schema, self.catalog.get_functions());
            let qualify_expr = select.qualify.as_ref().unwrap();
            filtered_rows.retain(|row| {
                evaluator
                    .evaluate_to_bool(qualify_expr, row)
                    .unwrap_or(false)
            });
        }

        let needs_deferred_projection = order_by.as_ref().is_some_and(|ob| {
            self.order_by_references_non_projected_columns(&input_schema, &select.projection, ob)
        });

        if needs_deferred_projection {
            Ok((input_schema, filtered_rows, true))
        } else {
            let (output_schema, mut output_rows) =
                self.project_rows(&input_schema, &filtered_rows, &select.projection)?;
            if select.distinct.is_some() {
                let mut seen = std::collections::HashSet::new();
                output_rows.retain(|row| {
                    let key = format!("{:?}", row.values());
                    seen.insert(key)
                });
            }
            Ok((output_schema, output_rows, false))
        }
    }

    fn order_by_references_non_projected_columns(
        &self,
        input_schema: &Schema,
        projection: &[SelectItem],
        order_by: &OrderBy,
    ) -> bool {
        let projected_columns: std::collections::HashSet<String> = projection
            .iter()
            .filter_map(|item| match item {
                SelectItem::UnnamedExpr(Expr::Identifier(ident)) => {
                    Some(ident.value.to_uppercase())
                }
                SelectItem::UnnamedExpr(Expr::CompoundIdentifier(parts)) => {
                    parts.last().map(|p| p.value.to_uppercase())
                }
                SelectItem::ExprWithAlias { alias, .. } => Some(alias.value.to_uppercase()),
                SelectItem::Wildcard(_) => None,
                _ => None,
            })
            .collect();

        let has_wildcard = projection
            .iter()
            .any(|item| matches!(item, SelectItem::Wildcard(_)));
        if has_wildcard {
            return false;
        }

        let exprs = match &order_by.kind {
            OrderByKind::Expressions(exprs) => exprs,
            OrderByKind::All(_) => return false,
        };

        for order_expr in exprs {
            match &order_expr.expr {
                Expr::Identifier(ident) => {
                    let col_name = ident.value.to_uppercase();
                    if !projected_columns.contains(&col_name) {
                        if input_schema
                            .fields()
                            .iter()
                            .any(|f| f.name.to_uppercase() == col_name)
                        {
                            return true;
                        }
                    }
                }
                Expr::CompoundIdentifier(parts) => {
                    if let Some(first) = parts.first() {
                        let base_col = first.value.to_uppercase();
                        if !projected_columns.contains(&base_col) {
                            if input_schema.fields().iter().any(|f| {
                                f.name.to_uppercase() == base_col
                                    || f.name.to_uppercase().ends_with(&format!(".{}", base_col))
                            }) {
                                return true;
                            }
                        }
                    }
                }
                _ => {
                    if self.expr_references_non_projected(
                        &order_expr.expr,
                        &projected_columns,
                        input_schema,
                    ) {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn expr_references_non_projected(
        &self,
        expr: &Expr,
        projected_columns: &std::collections::HashSet<String>,
        input_schema: &Schema,
    ) -> bool {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = ident.value.to_uppercase();
                !projected_columns.contains(&col_name)
                    && input_schema
                        .fields()
                        .iter()
                        .any(|f| f.name.to_uppercase() == col_name)
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_references_non_projected(left, projected_columns, input_schema)
                    || self.expr_references_non_projected(right, projected_columns, input_schema)
            }
            Expr::UnaryOp { expr, .. } => {
                self.expr_references_non_projected(expr, projected_columns, input_schema)
            }
            Expr::Function(func) => {
                if let ast::FunctionArguments::List(args) = &func.args {
                    for arg in &args.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(e)) = arg {
                            if self.expr_references_non_projected(
                                e,
                                projected_columns,
                                input_schema,
                            ) {
                                return true;
                            }
                        }
                    }
                }
                false
            }
            Expr::Nested(inner) => {
                self.expr_references_non_projected(inner, projected_columns, input_schema)
            }
            _ => false,
        }
    }

    fn get_from_data(&self, from: &[TableWithJoins]) -> Result<(Schema, Vec<Record>)> {
        self.get_from_data_ctes(from, &HashMap::new())
    }

    fn get_from_data_ctes(
        &self,
        from: &[TableWithJoins],
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>)> {
        if from.is_empty() {
            return Err(Error::InvalidQuery("FROM clause is empty".to_string()));
        }

        let first_table = &from[0];
        let (mut schema, mut rows) =
            self.get_table_factor_data_ctes(&first_table.relation, cte_tables)?;

        for join in &first_table.joins {
            let (right_schema, right_rows) =
                self.get_table_factor_data_ctes(&join.relation, cte_tables)?;
            (schema, rows) = self.execute_join(
                &schema,
                &rows,
                &right_schema,
                &right_rows,
                &join.join_operator,
            )?;
        }

        for additional_from in from.iter().skip(1) {
            if let TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                ..
            } = &additional_from.relation
            {
                (schema, rows) = self.execute_unnest_lateral(
                    &schema,
                    &rows,
                    array_exprs,
                    alias.as_ref(),
                    *with_offset,
                    with_offset_alias.as_ref(),
                )?;
            } else {
                let (right_schema, right_rows) =
                    self.get_table_factor_data_ctes(&additional_from.relation, cte_tables)?;
                (schema, rows) =
                    self.execute_cross_join(&schema, &rows, &right_schema, &right_rows)?;
            }

            for join in &additional_from.joins {
                let (join_right_schema, join_right_rows) =
                    self.get_table_factor_data_ctes(&join.relation, cte_tables)?;
                (schema, rows) = self.execute_join(
                    &schema,
                    &rows,
                    &join_right_schema,
                    &join_right_rows,
                    &join.join_operator,
                )?;
            }
        }

        Ok((schema, rows))
    }

    fn get_table_factor_data(&self, table_factor: &TableFactor) -> Result<(Schema, Vec<Record>)> {
        self.get_table_factor_data_ctes(table_factor, &HashMap::new())
    }

    fn get_table_factor_data_ctes(
        &self,
        table_factor: &TableFactor,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>)> {
        match table_factor {
            TableFactor::Table {
                name,
                alias,
                sample,
                args,
                ..
            } => {
                let table_name = name.to_string();
                let table_name_upper = table_name.to_uppercase();

                if table_name_upper == "NUMBERS" {
                    if let Some(table_args) = args {
                        let empty_schema = Schema::new();
                        let empty_record = Record::from_values(vec![]);
                        let evaluator = Evaluator::new(&empty_schema);

                        let n = if let Some(first_arg) = table_args.args.first() {
                            match first_arg {
                                ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                                    evaluator
                                        .evaluate(expr, &empty_record)?
                                        .as_i64()
                                        .unwrap_or(0)
                                }
                                _ => 0,
                            }
                        } else {
                            0
                        };

                        let schema = Schema::from_fields(vec![Field::nullable(
                            "number".to_string(),
                            DataType::Int64,
                        )]);
                        let rows: Vec<Record> = (0..n)
                            .map(|i| Record::from_values(vec![Value::int64(i)]))
                            .collect();
                        return Ok((schema, rows));
                    }
                }

                if let Some(cte_table) = cte_tables.get(&table_name_upper) {
                    let table_data = cte_table.clone();
                    let schema = if let Some(alias) = alias {
                        let prefix = &alias.name.value;
                        Schema::from_fields(
                            table_data
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| {
                                    Field::nullable(
                                        format!("{}.{}", prefix, f.name),
                                        f.data_type.clone(),
                                    )
                                })
                                .collect(),
                        )
                    } else {
                        table_data.schema().clone()
                    };
                    let mut rows = table_data.to_records()?;
                    rows = self.apply_table_sample(rows, sample)?;
                    return Ok((schema, rows));
                }

                if let Some(view_def) = self.catalog.get_view(&table_name) {
                    let view_query = view_def.query.clone();
                    let column_aliases = view_def.column_aliases.clone();
                    let view_result = self.execute_view_query(&view_query)?;

                    let mut rows = view_result.to_records()?;
                    rows = self.apply_table_sample(rows, sample)?;
                    let base_schema = view_result.schema().clone();

                    let schema = if !column_aliases.is_empty() {
                        let fields = base_schema
                            .fields()
                            .iter()
                            .zip(column_aliases.iter())
                            .map(|(f, alias)| Field::nullable(alias.clone(), f.data_type.clone()))
                            .collect();
                        Schema::from_fields(fields)
                    } else if let Some(table_alias) = alias {
                        let prefix = &table_alias.name.value;
                        Schema::from_fields(
                            base_schema
                                .fields()
                                .iter()
                                .map(|f| {
                                    Field::nullable(
                                        format!("{}.{}", prefix, f.name),
                                        f.data_type.clone(),
                                    )
                                })
                                .collect(),
                        )
                    } else {
                        base_schema
                    };

                    return Ok((schema, rows));
                }

                let table_data = self
                    .catalog
                    .get_table(&table_name)
                    .cloned()
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                let schema = if let Some(alias) = alias {
                    let prefix = &alias.name.value;
                    Schema::from_fields(
                        table_data
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| {
                                Field::nullable(
                                    format!("{}.{}", prefix, f.name),
                                    f.data_type.clone(),
                                )
                            })
                            .collect(),
                    )
                } else {
                    table_data.schema().clone()
                };

                let mut rows = table_data.to_records()?;
                rows = self.apply_table_sample(rows, sample)?;
                Ok((schema, rows))
            }
            TableFactor::NestedJoin {
                table_with_joins, ..
            } => {
                let (mut schema, mut rows) =
                    self.get_table_factor_data_ctes(&table_with_joins.relation, cte_tables)?;
                for join in &table_with_joins.joins {
                    let (right_schema, right_rows) =
                        self.get_table_factor_data_ctes(&join.relation, cte_tables)?;
                    (schema, rows) = self.execute_join(
                        &schema,
                        &rows,
                        &right_schema,
                        &right_rows,
                        &join.join_operator,
                    )?;
                }
                Ok((schema, rows))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let result = self.execute_query(subquery)?;
                let rows = result.rows()?;
                let schema = if let Some(alias) = alias {
                    Schema::from_fields(
                        result
                            .schema()
                            .fields()
                            .iter()
                            .map(|f| {
                                Field::nullable(
                                    format!("{}.{}", alias.name.value, f.name),
                                    f.data_type.clone(),
                                )
                            })
                            .collect(),
                    )
                } else {
                    result.schema().clone()
                };
                Ok((schema, rows))
            }
            TableFactor::UNNEST {
                alias,
                array_exprs,
                with_offset,
                with_offset_alias,
                ..
            } => {
                if array_exprs.is_empty() {
                    return Err(Error::InvalidQuery(
                        "UNNEST requires at least one array expression".to_string(),
                    ));
                }

                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = Evaluator::new(&empty_schema);
                let array_expr = &array_exprs[0];

                let element_alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| "element".to_string());
                let offset_alias = with_offset_alias
                    .as_ref()
                    .map(|a| a.value.clone())
                    .unwrap_or_else(|| "offset".to_string());

                let array_val = evaluator.evaluate(array_expr, &empty_record)?;
                let mut result_rows = Vec::new();
                let mut element_type: Option<DataType> = None;

                match array_val.as_array() {
                    Some(elements) => {
                        if !elements.is_empty() {
                            element_type = Some(elements[0].data_type());
                        }
                        for (idx, elem) in elements.iter().enumerate() {
                            let mut values = vec![elem.clone()];
                            if *with_offset {
                                values.push(Value::int64(idx as i64));
                            }
                            result_rows.push(Record::from_values(values));
                        }
                    }
                    None => {
                        if !array_val.is_null() {
                            return Err(Error::TypeMismatch {
                                expected: "Array".to_string(),
                                actual: format!("{:?}", array_val.data_type()),
                            });
                        }
                    }
                }

                let final_element_type = element_type.unwrap_or(DataType::String);
                let mut output_fields = vec![Field::nullable(element_alias, final_element_type)];
                if *with_offset {
                    output_fields.push(Field::nullable(offset_alias, DataType::Int64));
                }
                let output_schema = Schema::from_fields(output_fields);

                Ok((output_schema, result_rows))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Table factor not yet supported: {:?}",
                table_factor
            ))),
        }
    }

    fn apply_table_sample(
        &self,
        rows: Vec<Record>,
        sample: &Option<ast::TableSampleKind>,
    ) -> Result<Vec<Record>> {
        let sample_spec = match sample {
            Some(ast::TableSampleKind::BeforeTableAlias(s)) => s,
            Some(ast::TableSampleKind::AfterTableAlias(s)) => s,
            None => return Ok(rows),
        };

        let quantity = match &sample_spec.quantity {
            Some(q) => q,
            None => return Ok(rows),
        };

        let value = self.evaluate_literal_expr(&quantity.value)?;
        let num = value
            .as_i64()
            .or_else(|| value.as_f64().map(|f| f as i64))
            .unwrap_or(0);

        match &quantity.unit {
            Some(ast::TableSampleUnit::Rows) => {
                let limit = num.max(0) as usize;
                Ok(rows.into_iter().take(limit).collect())
            }
            Some(ast::TableSampleUnit::Percent) | None => {
                if num <= 0 {
                    return Ok(Vec::new());
                }
                if num >= 100 {
                    return Ok(rows);
                }
                let count = (rows.len() as f64 * (num as f64 / 100.0)).ceil() as usize;
                Ok(rows.into_iter().take(count).collect())
            }
        }
    }

    fn execute_join(
        &self,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
        join_op: &ast::JoinOperator,
    ) -> Result<(Schema, Vec<Record>)> {
        let combined_schema = Schema::from_fields(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .cloned()
                .collect(),
        );

        match join_op {
            ast::JoinOperator::Inner(constraint) | ast::JoinOperator::Join(constraint) => self
                .execute_inner_join(
                    &combined_schema,
                    left_schema,
                    left_rows,
                    right_schema,
                    right_rows,
                    constraint,
                ),
            ast::JoinOperator::Left(constraint) | ast::JoinOperator::LeftOuter(constraint) => self
                .execute_left_join(
                    &combined_schema,
                    left_schema,
                    left_rows,
                    right_schema,
                    right_rows,
                    constraint,
                ),
            ast::JoinOperator::Right(constraint) | ast::JoinOperator::RightOuter(constraint) => {
                self.execute_right_join(
                    &combined_schema,
                    left_schema,
                    left_rows,
                    right_schema,
                    right_rows,
                    constraint,
                )
            }
            ast::JoinOperator::FullOuter(constraint) => self.execute_full_join(
                &combined_schema,
                left_schema,
                left_rows,
                right_schema,
                right_rows,
                constraint,
            ),
            ast::JoinOperator::CrossJoin(_) => {
                self.execute_cross_join(left_schema, left_rows, right_schema, right_rows)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Join type not yet supported: {:?}",
                join_op
            ))),
        }
    }

    fn execute_inner_join(
        &self,
        combined_schema: &Schema,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
        constraint: &ast::JoinConstraint,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator =
            Evaluator::with_user_functions(combined_schema, self.catalog.get_functions());
        let mut result_rows = Vec::new();

        for left_record in left_rows {
            for right_record in right_rows {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                let combined_record = Record::from_values(combined_values);

                let matches = match constraint {
                    ast::JoinConstraint::On(expr) => {
                        evaluator.evaluate_to_bool(expr, &combined_record)?
                    }
                    ast::JoinConstraint::None => true,
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Join constraint not supported".to_string(),
                        ));
                    }
                };

                if matches {
                    result_rows.push(combined_record);
                }
            }
        }

        Ok((combined_schema.clone(), result_rows))
    }

    fn execute_left_join(
        &self,
        combined_schema: &Schema,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
        constraint: &ast::JoinConstraint,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator =
            Evaluator::with_user_functions(combined_schema, self.catalog.get_functions());
        let mut result_rows = Vec::new();
        let null_right: Vec<Value> = (0..right_schema.field_count())
            .map(|_| Value::null())
            .collect();

        for left_record in left_rows {
            let mut found_match = false;
            for right_record in right_rows {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                let combined_record = Record::from_values(combined_values);

                let matches = match constraint {
                    ast::JoinConstraint::On(expr) => {
                        evaluator.evaluate_to_bool(expr, &combined_record)?
                    }
                    ast::JoinConstraint::None => true,
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Join constraint not supported".to_string(),
                        ));
                    }
                };

                if matches {
                    result_rows.push(combined_record);
                    found_match = true;
                }
            }
            if !found_match {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(null_right.iter())
                    .cloned()
                    .collect();
                result_rows.push(Record::from_values(combined_values));
            }
        }

        Ok((combined_schema.clone(), result_rows))
    }

    fn execute_right_join(
        &self,
        combined_schema: &Schema,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
        constraint: &ast::JoinConstraint,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator =
            Evaluator::with_user_functions(combined_schema, self.catalog.get_functions());
        let mut result_rows = Vec::new();
        let null_left: Vec<Value> = (0..left_schema.field_count())
            .map(|_| Value::null())
            .collect();

        for right_record in right_rows {
            let mut found_match = false;
            for left_record in left_rows {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                let combined_record = Record::from_values(combined_values);

                let matches = match constraint {
                    ast::JoinConstraint::On(expr) => {
                        evaluator.evaluate_to_bool(expr, &combined_record)?
                    }
                    ast::JoinConstraint::None => true,
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Join constraint not supported".to_string(),
                        ));
                    }
                };

                if matches {
                    result_rows.push(combined_record);
                    found_match = true;
                }
            }
            if !found_match {
                let combined_values: Vec<Value> = null_left
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                result_rows.push(Record::from_values(combined_values));
            }
        }

        Ok((combined_schema.clone(), result_rows))
    }

    fn execute_full_join(
        &self,
        combined_schema: &Schema,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
        constraint: &ast::JoinConstraint,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator =
            Evaluator::with_user_functions(combined_schema, self.catalog.get_functions());
        let mut result_rows = Vec::new();
        let null_left: Vec<Value> = (0..left_schema.field_count())
            .map(|_| Value::null())
            .collect();
        let null_right: Vec<Value> = (0..right_schema.field_count())
            .map(|_| Value::null())
            .collect();
        let mut right_matched: Vec<bool> = vec![false; right_rows.len()];

        for left_record in left_rows {
            let mut found_match = false;
            for (right_idx, right_row) in right_rows.iter().enumerate() {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_row.values().iter())
                    .cloned()
                    .collect();
                let combined_record = Record::from_values(combined_values);

                let matches = match constraint {
                    ast::JoinConstraint::On(expr) => {
                        evaluator.evaluate_to_bool(expr, &combined_record)?
                    }
                    ast::JoinConstraint::None => true,
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Join constraint not supported".to_string(),
                        ));
                    }
                };

                if matches {
                    result_rows.push(combined_record);
                    found_match = true;
                    right_matched[right_idx] = true;
                }
            }
            if !found_match {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(null_right.iter())
                    .cloned()
                    .collect();
                result_rows.push(Record::from_values(combined_values));
            }
        }

        for (right_idx, right_row) in right_rows.iter().enumerate() {
            if !right_matched[right_idx] {
                let combined_values: Vec<Value> = null_left
                    .iter()
                    .chain(right_row.values().iter())
                    .cloned()
                    .collect();
                result_rows.push(Record::from_values(combined_values));
            }
        }

        Ok((combined_schema.clone(), result_rows))
    }

    fn execute_cross_join(
        &self,
        left_schema: &Schema,
        left_rows: &[Record],
        right_schema: &Schema,
        right_rows: &[Record],
    ) -> Result<(Schema, Vec<Record>)> {
        let combined_schema = Schema::from_fields(
            left_schema
                .fields()
                .iter()
                .chain(right_schema.fields().iter())
                .cloned()
                .collect(),
        );

        let mut result_rows = Vec::new();
        for left_record in left_rows {
            for right_record in right_rows {
                let combined_values: Vec<Value> = left_record
                    .values()
                    .iter()
                    .chain(right_record.values().iter())
                    .cloned()
                    .collect();
                result_rows.push(Record::from_values(combined_values));
            }
        }

        Ok((combined_schema, result_rows))
    }

    fn execute_unnest_lateral(
        &self,
        left_schema: &Schema,
        left_rows: &[Record],
        array_exprs: &[Expr],
        alias: Option<&ast::TableAlias>,
        with_offset: bool,
        with_offset_alias: Option<&ast::Ident>,
    ) -> Result<(Schema, Vec<Record>)> {
        if array_exprs.is_empty() {
            return Err(Error::InvalidQuery(
                "UNNEST requires at least one array expression".to_string(),
            ));
        }

        let evaluator = Evaluator::new(left_schema);
        let array_expr = &array_exprs[0];

        let element_alias = alias
            .map(|a| a.name.value.clone())
            .unwrap_or_else(|| "element".to_string());
        let offset_alias = with_offset_alias
            .map(|a| a.value.clone())
            .unwrap_or_else(|| "offset".to_string());

        let mut result_rows = Vec::new();
        let mut element_type: Option<DataType> = None;

        for left_record in left_rows {
            let array_val = evaluator.evaluate(array_expr, left_record)?;

            match array_val.as_array() {
                Some(elements) => {
                    if element_type.is_none() && !elements.is_empty() {
                        element_type = Some(elements[0].data_type());
                    }
                    for (idx, elem) in elements.iter().enumerate() {
                        let mut combined_values: Vec<Value> = left_record.values().to_vec();
                        combined_values.push(elem.clone());
                        if with_offset {
                            combined_values.push(Value::int64(idx as i64));
                        }
                        result_rows.push(Record::from_values(combined_values));
                    }
                }
                None => {
                    if array_val.is_null() {
                        continue;
                    }
                    return Err(Error::TypeMismatch {
                        expected: "Array".to_string(),
                        actual: format!("{:?}", array_val.data_type()),
                    });
                }
            }
        }

        let final_element_type = element_type.unwrap_or(DataType::String);
        let mut output_fields: Vec<Field> = left_schema.fields().to_vec();
        output_fields.push(Field::nullable(element_alias, final_element_type));
        if with_offset {
            output_fields.push(Field::nullable(offset_alias, DataType::Int64));
        }
        let output_schema = Schema::from_fields(output_fields);

        Ok((output_schema, result_rows))
    }

    fn has_aggregate_functions(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.expr_has_aggregate(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn expr_has_aggregate(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                if func.over.is_some() {
                    return false;
                }
                let name = func.name.to_string().to_uppercase();
                if self.is_aggregate_function(&name) {
                    return true;
                }
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr)) = arg
                        {
                            if self.expr_has_aggregate(arg_expr) {
                                return true;
                            }
                        }
                    }
                }
                false
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_aggregate(left) || self.expr_has_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => self.expr_has_aggregate(expr),
            Expr::Nested(inner) => self.expr_has_aggregate(inner),
            Expr::IsNull(inner) => self.expr_has_aggregate(inner),
            Expr::IsNotNull(inner) => self.expr_has_aggregate(inner),
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                conditions.iter().any(|c| {
                    self.expr_has_aggregate(&c.condition) || self.expr_has_aggregate(&c.result)
                }) || else_result
                    .as_ref()
                    .map(|e| self.expr_has_aggregate(e))
                    .unwrap_or(false)
            }
            Expr::Cast { expr, .. } => self.expr_has_aggregate(expr),
            Expr::Between {
                expr, low, high, ..
            } => {
                self.expr_has_aggregate(expr)
                    || self.expr_has_aggregate(low)
                    || self.expr_has_aggregate(high)
            }
            _ => false,
        }
    }

    fn has_array_join(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.expr_has_array_join(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn expr_has_array_join(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                name == "ARRAYJOIN"
            }
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_array_join(left) || self.expr_has_array_join(right)
            }
            Expr::UnaryOp { expr, .. } => self.expr_has_array_join(expr),
            Expr::Nested(inner) => self.expr_has_array_join(inner),
            _ => false,
        }
    }

    fn find_array_join_expr<'a>(&self, expr: &'a Expr) -> Option<&'a Expr> {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if name == "ARRAYJOIN" {
                    return Some(expr);
                }
                None
            }
            Expr::BinaryOp { left, right, .. } => self
                .find_array_join_expr(left)
                .or_else(|| self.find_array_join_expr(right)),
            Expr::UnaryOp { expr, .. } => self.find_array_join_expr(expr),
            Expr::Nested(inner) => self.find_array_join_expr(inner),
            _ => None,
        }
    }

    fn execute_aggregate_query_with_ctes(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        select: &Select,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());
        let group_exprs = self.extract_group_by_exprs(&select.group_by, &select.projection);
        let grouping_sets = self.extract_grouping_sets(&select.group_by);

        let group_by_is_empty = group_exprs.is_empty() && grouping_sets.is_none();

        let groups: Vec<(Vec<Value>, Vec<&Record>, Vec<usize>)> = if group_by_is_empty {
            vec![(vec![], rows.iter().collect(), vec![])]
        } else if let Some(sets) = grouping_sets {
            let mut all_groups = Vec::new();
            for grouping_set in &sets {
                let active_indices: Vec<usize> = grouping_set.clone();
                let mut group_map: HashMap<String, (Vec<Value>, Vec<&Record>)> = HashMap::new();

                for row in rows {
                    let mut group_key_values = Vec::new();
                    for (i, group_expr) in group_exprs.iter().enumerate() {
                        if active_indices.contains(&i) {
                            let val = evaluator.evaluate(group_expr, row)?;
                            group_key_values.push(val);
                        } else {
                            group_key_values.push(Value::null());
                        }
                    }
                    let key = format!("{:?}", group_key_values);
                    group_map
                        .entry(key)
                        .or_insert_with(|| (group_key_values.clone(), Vec::new()))
                        .1
                        .push(row);
                }
                for (group_key, group_rows) in group_map.into_values() {
                    all_groups.push((group_key, group_rows, active_indices.clone()));
                }
            }
            all_groups
        } else {
            let mut group_map: HashMap<String, (Vec<Value>, Vec<&Record>)> = HashMap::new();
            let all_indices: Vec<usize> = (0..group_exprs.len()).collect();

            for row in rows {
                let mut group_key_values = Vec::new();
                for group_expr in &group_exprs {
                    let val = evaluator.evaluate(group_expr, row)?;
                    group_key_values.push(val);
                }
                let key = format!("{:?}", group_key_values);
                group_map
                    .entry(key)
                    .or_insert_with(|| (group_key_values.clone(), Vec::new()))
                    .1
                    .push(row);
            }
            group_map
                .into_values()
                .map(|(k, v)| (k, v, all_indices.clone()))
                .collect()
        };

        let mut result_rows = Vec::new();
        let mut output_fields: Option<Vec<Field>> = None;

        for (group_key, group_rows, active_indices) in &groups {
            if let Some(having) = &select.having {
                let having_result = self.evaluate_aggregate_expr_with_grouping_ctes(
                    having,
                    input_schema,
                    group_rows,
                    group_key,
                    &group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                match having_result {
                    Value::Bool(true) => {}
                    Value::Bool(false) | Value::Null => continue,
                    _ => continue,
                }
            }

            let mut row_values = Vec::new();
            let mut field_names = Vec::new();

            for (idx, item) in select.projection.iter().enumerate() {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.evaluate_aggregate_expr_with_grouping_ctes(
                            expr,
                            input_schema,
                            group_rows,
                            group_key,
                            &group_exprs,
                            active_indices,
                            cte_tables,
                        )?;
                        field_names.push(self.expr_to_alias(expr, idx));
                        row_values.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.evaluate_aggregate_expr_with_grouping_ctes(
                            expr,
                            input_schema,
                            group_rows,
                            group_key,
                            &group_exprs,
                            active_indices,
                            cte_tables,
                        )?;
                        field_names.push(alias.value.clone());
                        row_values.push(val);
                    }
                    SelectItem::Wildcard(_) => {
                        for (i, val) in group_key.iter().enumerate() {
                            row_values.push(val.clone());
                            field_names.push(format!("_col{}", i));
                        }
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Unsupported projection in aggregate".to_string(),
                        ));
                    }
                }
            }

            if output_fields.is_none() {
                output_fields = Some(
                    row_values
                        .iter()
                        .zip(field_names.iter())
                        .map(|(val, name)| Field::nullable(name.clone(), val.data_type()))
                        .collect(),
                );
            }

            if let Some(having) = &select.having {
                let having_result = self.evaluate_having_with_aggregates(
                    having,
                    input_schema,
                    group_rows,
                    group_key,
                    &group_exprs,
                    active_indices,
                    cte_tables,
                    &row_values,
                    &field_names,
                )?;
                if !having_result {
                    continue;
                }
            }

            result_rows.push(Record::from_values(row_values));
        }

        let schema = Schema::from_fields(output_fields.unwrap_or_default());

        let has_window_funcs = self.has_window_functions(&select.projection);
        if has_window_funcs {
            let mut expr_to_col: HashMap<String, usize> = HashMap::new();
            for (idx, item) in select.projection.iter().enumerate() {
                let expr = match item {
                    SelectItem::UnnamedExpr(expr) => expr,
                    SelectItem::ExprWithAlias { expr, .. } => expr,
                    _ => continue,
                };
                let key = self.normalize_expr_key(expr);
                expr_to_col.insert(key.clone(), idx);
                if let Expr::Function(func) = expr {
                    let name = func.name.to_string().to_uppercase();
                    if self.is_aggregate_function(&name) && func.over.is_none() {
                        expr_to_col.entry(key).or_insert(idx);
                    }
                }
            }
            let window_results = self.compute_window_functions_with_aggregates(
                &schema,
                &result_rows,
                &select.projection,
                &expr_to_col,
            )?;
            return self.project_rows_with_windows_and_aggregates(
                &schema,
                &result_rows,
                &select.projection,
                &window_results,
                &expr_to_col,
            );
        }

        if let Some(qualify_expr) = &select.qualify {
            let substituted_qualify =
                self.substitute_aggregates_in_qualify(qualify_expr, &select.projection);
            let qualify_has_window = self.expr_has_window_function(&substituted_qualify);

            if qualify_has_window {
                let window_results = self.compute_window_functions_with_qualify(
                    &schema,
                    &result_rows,
                    &[],
                    Some(&substituted_qualify),
                )?;

                let qualifying_indices = self.apply_qualify_filter(
                    &schema,
                    &result_rows,
                    &substituted_qualify,
                    &window_results,
                )?;

                result_rows = qualifying_indices
                    .iter()
                    .map(|&i| result_rows[i].clone())
                    .collect();
            } else {
                let evaluator =
                    Evaluator::with_user_functions(&schema, self.catalog.get_functions());
                result_rows.retain(|row| {
                    evaluator
                        .evaluate_to_bool(&substituted_qualify, row)
                        .unwrap_or(false)
                });
            }
        }

        Ok((schema, result_rows))
    }

    fn normalize_expr_key(&self, expr: &Expr) -> String {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                let args = if let ast::FunctionArguments::List(list) = &func.args {
                    list.args
                        .iter()
                        .map(|a| self.normalize_func_arg_key(a))
                        .collect::<Vec<_>>()
                        .join(",")
                } else {
                    String::new()
                };
                format!("{}({})", name, args)
            }
            Expr::Identifier(ident) => ident.value.to_uppercase(),
            Expr::CompoundIdentifier(parts) => parts
                .iter()
                .map(|p| p.value.to_uppercase())
                .collect::<Vec<_>>()
                .join("."),
            Expr::BinaryOp { left, op, right } => {
                format!(
                    "({} {:?} {})",
                    self.normalize_expr_key(left),
                    op,
                    self.normalize_expr_key(right)
                )
            }
            Expr::Value(val) => format!("VAL:{:?}", val),
            _ => format!("{:?}", expr),
        }
    }

    fn normalize_func_arg_key(&self, arg: &ast::FunctionArg) -> String {
        match arg {
            ast::FunctionArg::Named { name, arg, .. } => {
                format!(
                    "{}={}",
                    name.value.to_uppercase(),
                    self.normalize_func_arg_expr_key(arg)
                )
            }
            ast::FunctionArg::ExprNamed { name, arg, .. } => {
                format!(
                    "{}={}",
                    self.normalize_expr_key(name),
                    self.normalize_func_arg_expr_key(arg)
                )
            }
            ast::FunctionArg::Unnamed(arg_expr) => self.normalize_func_arg_expr_key(arg_expr),
        }
    }

    fn normalize_func_arg_expr_key(&self, arg: &ast::FunctionArgExpr) -> String {
        match arg {
            ast::FunctionArgExpr::Expr(expr) => self.normalize_expr_key(expr),
            ast::FunctionArgExpr::Wildcard => "*".to_string(),
            ast::FunctionArgExpr::QualifiedWildcard(name) => format!("{}.*", name),
        }
    }

    fn collect_aggregate_subexprs(
        &self,
        expr: &Expr,
        expr_to_col: &mut HashMap<String, usize>,
        col_idx: usize,
    ) {
        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if self.is_aggregate_function(&name) && func.over.is_none() {
                    let key = self.normalize_expr_key(expr);
                    expr_to_col.entry(key).or_insert(col_idx);
                }
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_aggregate_subexprs(left, expr_to_col, col_idx);
                self.collect_aggregate_subexprs(right, expr_to_col, col_idx);
            }
            Expr::UnaryOp { expr: inner, .. } => {
                self.collect_aggregate_subexprs(inner, expr_to_col, col_idx);
            }
            Expr::Nested(inner) => {
                self.collect_aggregate_subexprs(inner, expr_to_col, col_idx);
            }
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                for cond in conditions {
                    self.collect_aggregate_subexprs(&cond.condition, expr_to_col, col_idx);
                    self.collect_aggregate_subexprs(&cond.result, expr_to_col, col_idx);
                }
                if let Some(e) = else_result {
                    self.collect_aggregate_subexprs(e, expr_to_col, col_idx);
                }
            }
            _ => {}
        }
    }

    fn resolve_group_by_alias(&self, expr: &Expr, projection: &[SelectItem]) -> Expr {
        let alias_name = match expr {
            Expr::Identifier(ident) => ident.value.to_lowercase(),
            _ => return expr.clone(),
        };

        for item in projection {
            match item {
                SelectItem::ExprWithAlias {
                    expr: proj_expr,
                    alias,
                } => {
                    if alias.value.to_lowercase() == alias_name {
                        return proj_expr.clone();
                    }
                }
                SelectItem::UnnamedExpr(proj_expr) => {
                    let inferred_name = self.expr_to_alias(proj_expr, 0).to_lowercase();
                    if inferred_name == alias_name {
                        return proj_expr.clone();
                    }
                }
                _ => {}
            }
        }

        expr.clone()
    }

    fn substitute_aggregates_in_qualify(
        &self,
        qualify_expr: &Expr,
        projection: &[SelectItem],
    ) -> Expr {
        let mut agg_to_alias: HashMap<String, String> = HashMap::new();
        for (idx, item) in projection.iter().enumerate() {
            match item {
                SelectItem::ExprWithAlias { expr, alias } => {
                    if self.expr_has_aggregate(expr) {
                        let expr_key = format!("{}", expr);
                        agg_to_alias.insert(expr_key, alias.value.clone());
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    if self.expr_has_aggregate(expr) {
                        let expr_key = format!("{}", expr);
                        let alias = self.expr_to_alias(expr, idx);
                        agg_to_alias.insert(expr_key, alias);
                    }
                }
                _ => {}
            }
        }

        self.substitute_aggregate_in_expr(qualify_expr, &agg_to_alias)
    }

    fn substitute_aggregate_in_expr(
        &self,
        expr: &Expr,
        agg_to_alias: &HashMap<String, String>,
    ) -> Expr {
        let expr_key = format!("{}", expr);
        if let Some(alias) = agg_to_alias.get(&expr_key) {
            return Expr::Identifier(ast::Ident::new(alias.clone()));
        }

        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let new_over = func.over.as_ref().map(|over| match over {
                    ast::WindowType::WindowSpec(spec) => {
                        let new_partition_by: Vec<Expr> = spec
                            .partition_by
                            .iter()
                            .map(|e| self.substitute_aggregate_in_expr(e, agg_to_alias))
                            .collect();
                        let new_order_by: Vec<ast::OrderByExpr> = spec
                            .order_by
                            .iter()
                            .map(|ob| ast::OrderByExpr {
                                expr: self.substitute_aggregate_in_expr(&ob.expr, agg_to_alias),
                                options: ob.options,
                                with_fill: ob.with_fill.clone(),
                            })
                            .collect();
                        ast::WindowType::WindowSpec(ast::WindowSpec {
                            partition_by: new_partition_by,
                            order_by: new_order_by,
                            window_frame: spec.window_frame.clone(),
                            window_name: spec.window_name.clone(),
                        })
                    }
                    ast::WindowType::NamedWindow(name) => {
                        ast::WindowType::NamedWindow(name.clone())
                    }
                });

                let new_args = match &func.args {
                    ast::FunctionArguments::List(list) => {
                        let new_list = ast::FunctionArgumentList {
                            duplicate_treatment: list.duplicate_treatment,
                            args: list
                                .args
                                .iter()
                                .map(|arg| match arg {
                                    ast::FunctionArg::Unnamed(unnamed_arg) => match unnamed_arg {
                                        ast::FunctionArgExpr::Expr(e) => {
                                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                                self.substitute_aggregate_in_expr(e, agg_to_alias),
                                            ))
                                        }
                                        _ => arg.clone(),
                                    },
                                    _ => arg.clone(),
                                })
                                .collect(),
                            clauses: list.clauses.clone(),
                        };
                        ast::FunctionArguments::List(new_list)
                    }
                    other => other.clone(),
                };

                Expr::Function(ast::Function {
                    name: func.name.clone(),
                    uses_odbc_syntax: func.uses_odbc_syntax,
                    parameters: func.parameters.clone(),
                    args: new_args,
                    filter: func.filter.clone(),
                    null_treatment: func.null_treatment,
                    over: new_over,
                    within_group: func.within_group.clone(),
                })
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.substitute_aggregate_in_expr(left, agg_to_alias)),
                op: op.clone(),
                right: Box::new(self.substitute_aggregate_in_expr(right, agg_to_alias)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.substitute_aggregate_in_expr(inner, agg_to_alias)),
            },
            Expr::Nested(inner) => Expr::Nested(Box::new(
                self.substitute_aggregate_in_expr(inner, agg_to_alias),
            )),
            _ => expr.clone(),
        }
    }

    fn extract_group_by_exprs(
        &self,
        group_by: &ast::GroupByExpr,
        projection: &[SelectItem],
    ) -> Vec<Expr> {
        match group_by {
            ast::GroupByExpr::Expressions(exprs, _) => {
                let mut result = Vec::new();
                for expr in exprs {
                    match expr {
                        Expr::Rollup(rollup_exprs) => {
                            for inner in rollup_exprs {
                                for e in inner {
                                    result.push(self.resolve_group_by_alias(e, projection));
                                }
                            }
                        }
                        Expr::Cube(cube_exprs) => {
                            for inner in cube_exprs {
                                for e in inner {
                                    result.push(self.resolve_group_by_alias(e, projection));
                                }
                            }
                        }
                        Expr::GroupingSets(sets_exprs) => {
                            for set in sets_exprs {
                                for e in set {
                                    result.push(self.resolve_group_by_alias(e, projection));
                                }
                            }
                        }
                        _ => result.push(self.resolve_group_by_alias(expr, projection)),
                    }
                }
                let mut seen = HashMap::new();
                result
                    .into_iter()
                    .filter(|e| {
                        let key = self.expr_key(e);
                        if let std::collections::hash_map::Entry::Vacant(entry) = seen.entry(key) {
                            entry.insert(true);
                            true
                        } else {
                            false
                        }
                    })
                    .collect()
            }
            ast::GroupByExpr::All(_) => vec![],
        }
    }

    fn extract_grouping_sets(&self, group_by: &ast::GroupByExpr) -> Option<Vec<Vec<usize>>> {
        match group_by {
            ast::GroupByExpr::Expressions(exprs, _) => {
                let mut all_exprs: Vec<Expr> = Vec::new();
                let mut expr_indices: HashMap<String, usize> = HashMap::new();
                let mut grouping_sets: Vec<Vec<usize>> = Vec::new();
                let mut has_grouping_modifier = false;
                let mut regular_indices: Vec<usize> = Vec::new();

                for expr in exprs {
                    match expr {
                        Expr::Rollup(rollup_exprs) => {
                            has_grouping_modifier = true;
                            let flat_exprs: Vec<Expr> =
                                rollup_exprs.iter().flatten().cloned().collect();
                            let indices = self.add_exprs_to_index_map(
                                &mut all_exprs,
                                &mut expr_indices,
                                &flat_exprs,
                            );
                            let sets = self.expand_rollup_indices(&indices);
                            grouping_sets.extend(sets);
                        }
                        Expr::Cube(cube_exprs) => {
                            has_grouping_modifier = true;
                            let flat_exprs: Vec<Expr> =
                                cube_exprs.iter().flatten().cloned().collect();
                            let indices = self.add_exprs_to_index_map(
                                &mut all_exprs,
                                &mut expr_indices,
                                &flat_exprs,
                            );
                            let sets = self.expand_cube_indices(&indices);
                            grouping_sets.extend(sets);
                        }
                        Expr::GroupingSets(sets_exprs) => {
                            has_grouping_modifier = true;
                            for set_vec in sets_exprs {
                                let indices = self.add_exprs_to_index_map(
                                    &mut all_exprs,
                                    &mut expr_indices,
                                    set_vec,
                                );
                                grouping_sets.push(indices);
                            }
                        }
                        _ => {
                            let idx =
                                self.add_expr_to_index_map(&mut all_exprs, &mut expr_indices, expr);
                            regular_indices.push(idx);
                        }
                    }
                }

                if has_grouping_modifier {
                    if !regular_indices.is_empty() {
                        let mut expanded_sets = Vec::new();
                        for set in grouping_sets {
                            let mut new_set = regular_indices.clone();
                            new_set.extend(set);
                            expanded_sets.push(new_set);
                        }
                        grouping_sets = expanded_sets;
                    }
                    Some(grouping_sets)
                } else {
                    None
                }
            }
            ast::GroupByExpr::All(_) => None,
        }
    }

    fn expr_key(&self, expr: &Expr) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.to_uppercase(),
            Expr::CompoundIdentifier(parts) => parts
                .iter()
                .map(|p| p.value.to_uppercase())
                .collect::<Vec<_>>()
                .join("."),
            _ => format!("{:?}", expr),
        }
    }

    fn add_expr_to_index_map(
        &self,
        all_exprs: &mut Vec<Expr>,
        expr_indices: &mut HashMap<String, usize>,
        expr: &Expr,
    ) -> usize {
        let key = self.expr_key(expr);
        if let Some(&idx) = expr_indices.get(&key) {
            return idx;
        }
        let idx = all_exprs.len();
        all_exprs.push(expr.clone());
        expr_indices.insert(key, idx);
        idx
    }

    fn add_exprs_to_index_map(
        &self,
        all_exprs: &mut Vec<Expr>,
        expr_indices: &mut HashMap<String, usize>,
        exprs: &[Expr],
    ) -> Vec<usize> {
        exprs
            .iter()
            .map(|e| self.add_expr_to_index_map(all_exprs, expr_indices, e))
            .collect()
    }

    fn expand_rollup_indices(&self, indices: &[usize]) -> Vec<Vec<usize>> {
        let mut sets = Vec::new();
        for i in (0..=indices.len()).rev() {
            sets.push(indices[..i].to_vec());
        }
        sets
    }

    fn expand_cube_indices(&self, indices: &[usize]) -> Vec<Vec<usize>> {
        let n = indices.len();
        let mut sets = Vec::new();
        for mask in (0..(1 << n)).rev() {
            let mut set = Vec::new();
            for (i, &idx) in indices.iter().enumerate() {
                if mask & (1 << i) != 0 {
                    set.push(idx);
                }
            }
            sets.push(set);
        }
        sets
    }

    fn evaluate_having_expr(
        &self,
        expr: &Expr,
        row: &Record,
        field_names: &[String],
    ) -> Result<bool> {
        match self.evaluate_having_value(expr, row, field_names)? {
            Value::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    fn evaluate_having_value(
        &self,
        expr: &Expr,
        row: &Record,
        field_names: &[String],
    ) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value(left, row, field_names)?;
                let right_val = self.evaluate_having_value(right, row, field_names)?;
                match op {
                    ast::BinaryOperator::Gt => Ok(Value::Bool(
                        self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Greater,
                    )),
                    ast::BinaryOperator::Lt => Ok(Value::Bool(
                        self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                    )),
                    ast::BinaryOperator::GtEq => Ok(Value::Bool(
                        self.compare_values(&left_val, &right_val) != std::cmp::Ordering::Less,
                    )),
                    ast::BinaryOperator::LtEq => Ok(Value::Bool(
                        self.compare_values(&left_val, &right_val) != std::cmp::Ordering::Greater,
                    )),
                    ast::BinaryOperator::Eq => Ok(Value::Bool(left_val == right_val)),
                    ast::BinaryOperator::NotEq => Ok(Value::Bool(left_val != right_val)),
                    ast::BinaryOperator::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l && r))
                    }
                    ast::BinaryOperator::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l || r))
                    }
                    _ => Ok(Value::null()),
                }
            }
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if let Some(idx) = field_names
                    .iter()
                    .position(|f| f.to_uppercase() == name || f.to_uppercase().contains(&name))
                {
                    return Ok(row.values()[idx].clone());
                }
                for (idx, field_name) in field_names.iter().enumerate() {
                    if let Some(val) = row.values().get(idx) {
                        if val.as_i64().is_some() || val.as_f64().is_some() {
                            return Ok(val.clone());
                        }
                    }
                }
                Ok(Value::null())
            }
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                if let Some(idx) = field_names.iter().position(|f| f.to_uppercase() == name) {
                    return Ok(row.values()[idx].clone());
                }
                Ok(Value::null())
            }
            Expr::Value(v) => self.sql_value_to_value(&v.value),
            _ => Ok(Value::null()),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_having_with_aggregates(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        group_rows: &[&Record],
        group_key: &[Value],
        group_exprs: &[Expr],
        active_indices: &[usize],
        cte_tables: &HashMap<String, Table>,
        row_values: &[Value],
        field_names: &[String],
    ) -> Result<bool> {
        let val = self.evaluate_having_value_with_aggregates(
            expr,
            input_schema,
            group_rows,
            group_key,
            group_exprs,
            active_indices,
            cte_tables,
            row_values,
            field_names,
        )?;
        match val {
            Value::Bool(b) => Ok(b),
            _ => Ok(false),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_having_value_with_aggregates(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        group_rows: &[&Record],
        group_key: &[Value],
        group_exprs: &[Expr],
        active_indices: &[usize],
        cte_tables: &HashMap<String, Table>,
        row_values: &[Value],
        field_names: &[String],
    ) -> Result<Value> {
        match expr {
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_having_value_with_aggregates(
                    left,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                    row_values,
                    field_names,
                )?;
                let right_val = self.evaluate_having_value_with_aggregates(
                    right,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                    row_values,
                    field_names,
                )?;
                match op {
                    ast::BinaryOperator::Gt => {
                        if left_val.is_null() || right_val.is_null() {
                            return Ok(Value::Bool(false));
                        }
                        Ok(Value::Bool(
                            self.compare_values(&left_val, &right_val)
                                == std::cmp::Ordering::Greater,
                        ))
                    }
                    ast::BinaryOperator::Lt => {
                        if left_val.is_null() || right_val.is_null() {
                            return Ok(Value::Bool(false));
                        }
                        Ok(Value::Bool(
                            self.compare_values(&left_val, &right_val) == std::cmp::Ordering::Less,
                        ))
                    }
                    ast::BinaryOperator::GtEq => {
                        if left_val.is_null() || right_val.is_null() {
                            return Ok(Value::Bool(false));
                        }
                        Ok(Value::Bool(
                            self.compare_values(&left_val, &right_val) != std::cmp::Ordering::Less,
                        ))
                    }
                    ast::BinaryOperator::LtEq => {
                        if left_val.is_null() || right_val.is_null() {
                            return Ok(Value::Bool(false));
                        }
                        Ok(Value::Bool(
                            self.compare_values(&left_val, &right_val)
                                != std::cmp::Ordering::Greater,
                        ))
                    }
                    ast::BinaryOperator::Eq => {
                        if left_val.is_null() && right_val.is_null() {
                            return Ok(Value::Bool(true));
                        }
                        if left_val.is_null() || right_val.is_null() {
                            return Ok(Value::Bool(false));
                        }
                        Ok(Value::Bool(left_val == right_val))
                    }
                    ast::BinaryOperator::NotEq => {
                        if left_val.is_null() && right_val.is_null() {
                            return Ok(Value::Bool(false));
                        }
                        if left_val.is_null() || right_val.is_null() {
                            return Ok(Value::Bool(true));
                        }
                        Ok(Value::Bool(left_val != right_val))
                    }
                    ast::BinaryOperator::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l && r))
                    }
                    ast::BinaryOperator::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l || r))
                    }
                    _ => Ok(Value::null()),
                }
            }
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if let Some(idx) = field_names
                    .iter()
                    .position(|f| f.to_uppercase() == name || f.to_uppercase().contains(&name))
                {
                    return Ok(row_values[idx].clone());
                }
                if self.is_aggregate_function(&name) {
                    return self.evaluate_aggregate_expr_with_grouping_ctes(
                        expr,
                        input_schema,
                        group_rows,
                        group_key,
                        group_exprs,
                        active_indices,
                        cte_tables,
                    );
                }
                for (idx, _field_name) in field_names.iter().enumerate() {
                    if let Some(val) = row_values.get(idx) {
                        if val.as_i64().is_some() || val.as_f64().is_some() {
                            return Ok(val.clone());
                        }
                    }
                }
                Ok(Value::null())
            }
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                if let Some(idx) = field_names.iter().position(|f| f.to_uppercase() == name) {
                    return Ok(row_values[idx].clone());
                }
                Ok(Value::null())
            }
            Expr::Value(v) => self.sql_value_to_value(&v.value),
            _ => Ok(Value::null()),
        }
    }

    fn evaluate_aggregate_expr(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        group_rows: &[&Record],
        group_key: &[Value],
        group_by: &ast::GroupByExpr,
    ) -> Result<Value> {
        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());

        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if self.is_aggregate_function(&name) {
                    return self.compute_aggregate(&name, func, input_schema, group_rows);
                }
                let mut has_agg_arg = false;
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr)) = arg
                        {
                            if self.expr_has_aggregate(arg_expr) {
                                has_agg_arg = true;
                                break;
                            }
                        }
                    }
                }
                if has_agg_arg {
                    let mut evaluated_args = Vec::new();
                    if let ast::FunctionArguments::List(arg_list) = &func.args {
                        for arg in &arg_list.args {
                            if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr)) =
                                arg
                            {
                                let val = self.evaluate_aggregate_expr(
                                    arg_expr,
                                    input_schema,
                                    group_rows,
                                    group_key,
                                    group_by,
                                )?;
                                evaluated_args.push(val);
                            }
                        }
                    }
                    return evaluator.evaluate_function_with_args(&name, &evaluated_args);
                }
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate_function_internal(func, row)
                } else {
                    Ok(Value::null())
                }
            }
            Expr::Identifier(ident) => {
                let group_exprs = self.extract_group_by_exprs(group_by, &[]);
                for (i, ge) in group_exprs.iter().enumerate() {
                    if self.exprs_equal(expr, ge) {
                        if i < group_key.len() {
                            return Ok(group_key[i].clone());
                        }
                    }
                }
                if let Some(row) = group_rows.first() {
                    match evaluator.evaluate(expr, row) {
                        Ok(val) => Ok(val),
                        Err(Error::ColumnNotFound(name)) => {
                            let upper = name.to_uppercase();
                            match upper.as_str() {
                                "MICROSECOND" | "MILLISECOND" | "SECOND" | "MINUTE" | "HOUR"
                                | "DAY" | "DAYOFWEEK" | "DAYOFYEAR" | "WEEK" | "MONTH"
                                | "QUARTER" | "YEAR" => Ok(Value::string(upper)),
                                _ => Err(Error::ColumnNotFound(ident.value.clone())),
                            }
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(Value::null())
                }
            }
            Expr::CompoundIdentifier(_) => {
                let group_exprs = self.extract_group_by_exprs(group_by, &[]);
                for (i, ge) in group_exprs.iter().enumerate() {
                    if self.exprs_equal(expr, ge) {
                        if i < group_key.len() {
                            return Ok(group_key[i].clone());
                        }
                    }
                }
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(expr, row)
                } else {
                    Ok(Value::null())
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_aggregate_expr(
                    left,
                    input_schema,
                    group_rows,
                    group_key,
                    group_by,
                )?;
                let right_val = self.evaluate_aggregate_expr(
                    right,
                    input_schema,
                    group_rows,
                    group_key,
                    group_by,
                )?;
                evaluator.evaluate_binary_op_values(&left_val, op, &right_val)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                match operand {
                    Some(op_expr) => {
                        let op_val = self.evaluate_aggregate_expr(
                            op_expr,
                            input_schema,
                            group_rows,
                            group_key,
                            group_by,
                        )?;
                        for cond in conditions {
                            let when_val = self.evaluate_aggregate_expr(
                                &cond.condition,
                                input_schema,
                                group_rows,
                                group_key,
                                group_by,
                            )?;
                            if op_val == when_val {
                                return self.evaluate_aggregate_expr(
                                    &cond.result,
                                    input_schema,
                                    group_rows,
                                    group_key,
                                    group_by,
                                );
                            }
                        }
                    }
                    None => {
                        for cond in conditions {
                            let cond_val = self.evaluate_aggregate_expr(
                                &cond.condition,
                                input_schema,
                                group_rows,
                                group_key,
                                group_by,
                            )?;
                            if let Some(true) = cond_val.as_bool() {
                                return self.evaluate_aggregate_expr(
                                    &cond.result,
                                    input_schema,
                                    group_rows,
                                    group_key,
                                    group_by,
                                );
                            }
                        }
                    }
                }
                match else_result {
                    Some(else_expr) => self.evaluate_aggregate_expr(
                        else_expr,
                        input_schema,
                        group_rows,
                        group_key,
                        group_by,
                    ),
                    None => Ok(Value::null()),
                }
            }
            Expr::Cast {
                expr: inner_expr,
                data_type,
                ..
            } => {
                let val = self.evaluate_aggregate_expr(
                    inner_expr,
                    input_schema,
                    group_rows,
                    group_key,
                    group_by,
                )?;
                evaluator.cast_value(&val, data_type, false)
            }
            _ => {
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(expr, row)
                } else {
                    Ok(Value::null())
                }
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn evaluate_aggregate_expr_with_grouping_ctes(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        group_rows: &[&Record],
        group_key: &[Value],
        group_exprs: &[Expr],
        active_indices: &[usize],
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Value> {
        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());

        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if func.over.is_some() {
                    return Ok(Value::null());
                }
                if name == "GROUPING" {
                    return self.evaluate_grouping_function(func, group_exprs, active_indices);
                }
                if name == "GROUPING_ID" {
                    return self.evaluate_grouping_id_function(func, group_exprs, active_indices);
                }
                if self.is_aggregate_function(&name) {
                    return self.compute_aggregate(&name, func, input_schema, group_rows);
                }
                let mut evaluated_args = Vec::new();
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr)) = arg
                        {
                            let val = self.evaluate_aggregate_expr_with_grouping_ctes(
                                arg_expr,
                                input_schema,
                                group_rows,
                                group_key,
                                group_exprs,
                                active_indices,
                                cte_tables,
                            )?;
                            evaluated_args.push(val);
                        }
                    }
                }
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate_function(&name, &evaluated_args, func, row)
                } else {
                    Ok(Value::null())
                }
            }
            Expr::Identifier(ident) => {
                for (i, ge) in group_exprs.iter().enumerate() {
                    if self.exprs_equal(expr, ge) && i < group_key.len() {
                        return Ok(group_key[i].clone());
                    }
                }
                if let Some(row) = group_rows.first() {
                    match evaluator.evaluate(expr, row) {
                        Ok(val) => Ok(val),
                        Err(Error::ColumnNotFound(name)) => {
                            let upper = name.to_uppercase();
                            match upper.as_str() {
                                "MICROSECOND" | "MILLISECOND" | "SECOND" | "MINUTE" | "HOUR"
                                | "DAY" | "DAYOFWEEK" | "DAYOFYEAR" | "WEEK" | "MONTH"
                                | "QUARTER" | "YEAR" => Ok(Value::string(upper)),
                                _ => Err(Error::ColumnNotFound(ident.value.clone())),
                            }
                        }
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(Value::null())
                }
            }
            Expr::CompoundIdentifier(_) => {
                for (i, ge) in group_exprs.iter().enumerate() {
                    if self.exprs_equal(expr, ge) && i < group_key.len() {
                        return Ok(group_key[i].clone());
                    }
                }
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(expr, row)
                } else {
                    Ok(Value::null())
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    left,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                let right_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    right,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                evaluator.evaluate_binary_op_values(&left_val, op, &right_val)
            }
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                match operand {
                    Some(op_expr) => {
                        let op_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                            op_expr,
                            input_schema,
                            group_rows,
                            group_key,
                            group_exprs,
                            active_indices,
                            cte_tables,
                        )?;
                        for cond in conditions {
                            let when_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                                &cond.condition,
                                input_schema,
                                group_rows,
                                group_key,
                                group_exprs,
                                active_indices,
                                cte_tables,
                            )?;
                            if op_val == when_val {
                                return self.evaluate_aggregate_expr_with_grouping_ctes(
                                    &cond.result,
                                    input_schema,
                                    group_rows,
                                    group_key,
                                    group_exprs,
                                    active_indices,
                                    cte_tables,
                                );
                            }
                        }
                    }
                    None => {
                        for cond in conditions {
                            let cond_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                                &cond.condition,
                                input_schema,
                                group_rows,
                                group_key,
                                group_exprs,
                                active_indices,
                                cte_tables,
                            )?;
                            if cond_val == Value::Bool(true) {
                                return self.evaluate_aggregate_expr_with_grouping_ctes(
                                    &cond.result,
                                    input_schema,
                                    group_rows,
                                    group_key,
                                    group_exprs,
                                    active_indices,
                                    cte_tables,
                                );
                            }
                        }
                    }
                }
                if let Some(else_expr) = else_result {
                    self.evaluate_aggregate_expr_with_grouping_ctes(
                        else_expr,
                        input_schema,
                        group_rows,
                        group_key,
                        group_exprs,
                        active_indices,
                        cte_tables,
                    )
                } else {
                    Ok(Value::null())
                }
            }
            Expr::Value(v) => self.sql_value_to_value(&v.value),
            Expr::Subquery(query) => {
                let result = self.execute_query_with_ctes(query, cte_tables)?;
                let rows = result.to_records()?;
                if rows.len() != 1 || result.schema().field_count() != 1 {
                    return Err(Error::InvalidQuery(
                        "Scalar subquery must return exactly one row and one column".to_string(),
                    ));
                }
                Ok(rows[0].values()[0].clone())
            }
            Expr::Cast {
                expr: inner,
                data_type,
                kind,
                ..
            } => {
                let val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    inner,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                let is_safe = matches!(kind, ast::CastKind::SafeCast);
                evaluator.cast_value(&val, data_type, is_safe)
            }
            Expr::Nested(inner) => self.evaluate_aggregate_expr_with_grouping_ctes(
                inner,
                input_schema,
                group_rows,
                group_key,
                group_exprs,
                active_indices,
                cte_tables,
            ),
            Expr::IsNull(inner) => {
                let val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    inner,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                Ok(Value::Bool(val.is_null()))
            }
            Expr::IsNotNull(inner) => {
                let val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    inner,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                Ok(Value::Bool(!val.is_null()))
            }
            Expr::Between {
                expr: inner_expr,
                low,
                high,
                negated,
            } => {
                let expr_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    inner_expr,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                let low_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    low,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                let high_val = self.evaluate_aggregate_expr_with_grouping_ctes(
                    high,
                    input_schema,
                    group_rows,
                    group_key,
                    group_exprs,
                    active_indices,
                    cte_tables,
                )?;
                evaluator.evaluate_between_values(&expr_val, &low_val, &high_val, *negated)
            }
            _ => {
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(expr, row)
                } else {
                    Ok(Value::null())
                }
            }
        }
    }

    fn evaluate_grouping_function(
        &self,
        func: &ast::Function,
        group_exprs: &[Expr],
        active_indices: &[usize],
    ) -> Result<Value> {
        let args = match &func.args {
            ast::FunctionArguments::List(list) => &list.args,
            _ => return Ok(Value::Int64(0)),
        };

        if args.is_empty() {
            return Ok(Value::Int64(0));
        }

        if let Some(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr))) = args.first()
        {
            for (i, ge) in group_exprs.iter().enumerate() {
                if self.exprs_equal(arg_expr, ge) {
                    let is_active = active_indices.contains(&i);
                    return Ok(Value::Int64(if is_active { 0 } else { 1 }));
                }
            }
        }

        Ok(Value::Int64(0))
    }

    fn evaluate_grouping_id_function(
        &self,
        func: &ast::Function,
        group_exprs: &[Expr],
        active_indices: &[usize],
    ) -> Result<Value> {
        let args = match &func.args {
            ast::FunctionArguments::List(list) => &list.args,
            _ => return Ok(Value::Int64(0)),
        };

        if args.is_empty() {
            return Ok(Value::Int64(0));
        }

        let mut result: i64 = 0;
        let n = args.len();
        for (arg_pos, arg) in args.iter().enumerate() {
            if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr)) = arg {
                for (i, ge) in group_exprs.iter().enumerate() {
                    if self.exprs_equal(arg_expr, ge) {
                        let is_active = active_indices.contains(&i);
                        if !is_active {
                            result |= 1 << (n - 1 - arg_pos);
                        }
                        break;
                    }
                }
            }
        }

        Ok(Value::Int64(result))
    }

    fn is_aggregate_function(&self, name: &str) -> bool {
        matches!(
            name,
            "COUNT"
                | "SUM"
                | "AVG"
                | "MIN"
                | "MAX"
                | "ARRAY_AGG"
                | "STRING_AGG"
                | "GROUP_CONCAT"
                | "LISTAGG"
                | "STDDEV"
                | "STDDEV_POP"
                | "STDDEV_SAMP"
                | "VARIANCE"
                | "VAR_POP"
                | "VAR_SAMP"
                | "COVAR_POP"
                | "COVAR_SAMP"
                | "CORR"
                | "BIT_AND"
                | "BIT_OR"
                | "BIT_XOR"
                | "BOOL_AND"
                | "BOOL_OR"
                | "EVERY"
                | "ANY_VALUE"
                | "APPROX_COUNT_DISTINCT"
                | "APPROX_QUANTILES"
                | "APPROX_TOP_COUNT"
                | "APPROX_TOP_SUM"
                | "HLL_COUNT_INIT"
                | "HLL_COUNT_MERGE"
                | "HLL_COUNT_MERGE_PARTIAL"
                | "COUNTIF"
                | "COUNT_IF"
                | "SUMIF"
                | "SUM_IF"
                | "XMLAGG"
        )
    }

    fn compute_aggregate(
        &self,
        name: &str,
        func: &ast::Function,
        input_schema: &Schema,
        group_rows: &[&Record],
    ) -> Result<Value> {
        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());

        let is_distinct = matches!(&func.args, ast::FunctionArguments::List(list) if list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct));

        let arg_expr = self.extract_first_function_arg(func)?;

        match name {
            "COUNT" => {
                if arg_expr.is_none() {
                    return Ok(Value::int64(group_rows.len() as i64));
                }
                let expr = arg_expr.unwrap();
                if matches!(expr, Expr::Wildcard(_)) {
                    return Ok(Value::int64(group_rows.len() as i64));
                }
                let mut count = 0i64;
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        if is_distinct {
                            let key = format!("{:?}", val);
                            if seen.insert(key) {
                                count += 1;
                            }
                        } else {
                            count += 1;
                        }
                    }
                }
                Ok(Value::int64(count))
            }
            "SUM" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery("SUM requires an argument".to_string()))?;
                let mut sum_int: Option<i64> = None;
                let mut sum_float: Option<f64> = None;
                let mut sum_decimal: Option<rust_decimal::Decimal> = None;
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    if is_distinct {
                        let key = format!("{:?}", val);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    if let Some(d) = val.as_numeric() {
                        sum_decimal = Some(sum_decimal.unwrap_or(rust_decimal::Decimal::ZERO) + d);
                    } else if let Some(i) = val.as_i64() {
                        sum_int = Some(sum_int.unwrap_or(0) + i);
                    } else if let Some(f) = val.as_f64() {
                        sum_float = Some(sum_float.unwrap_or(0.0) + f);
                    }
                }

                if let Some(s) = sum_decimal {
                    let mut total = s;
                    if let Some(i) = sum_int {
                        total += rust_decimal::Decimal::from(i);
                    }
                    if let Some(f) = sum_float {
                        if let Some(fd) = rust_decimal::Decimal::from_f64_retain(f) {
                            total += fd;
                        }
                    }
                    Ok(Value::numeric(total))
                } else if let Some(s) = sum_float {
                    Ok(Value::float64(s + sum_int.unwrap_or(0) as f64))
                } else if let Some(s) = sum_int {
                    Ok(Value::int64(s))
                } else {
                    Ok(Value::null())
                }
            }
            "AVG" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery("AVG requires an argument".to_string()))?;
                let mut sum: f64 = 0.0;
                let mut sum_decimal: Option<rust_decimal::Decimal> = None;
                let mut count: i64 = 0;
                let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    if is_distinct {
                        let key = format!("{:?}", val);
                        if !seen.insert(key) {
                            continue;
                        }
                    }
                    if let Some(d) = val.as_numeric() {
                        sum_decimal = Some(sum_decimal.unwrap_or(rust_decimal::Decimal::ZERO) + d);
                        count += 1;
                    } else if let Some(i) = val.as_i64() {
                        sum += i as f64;
                        count += 1;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                        count += 1;
                    }
                }

                if count > 0 {
                    if let Some(s) = sum_decimal {
                        let avg = s / rust_decimal::Decimal::from(count);
                        Ok(Value::numeric(avg))
                    } else {
                        Ok(Value::float64(sum / count as f64))
                    }
                } else {
                    Ok(Value::null())
                }
            }
            "MIN" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery("MIN requires an argument".to_string()))?;
                let mut min: Option<Value> = None;

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    match &min {
                        None => min = Some(val),
                        Some(m) => {
                            if self.compare_values_for_ordering(&val, m) == std::cmp::Ordering::Less
                            {
                                min = Some(val);
                            }
                        }
                    }
                }

                Ok(min.unwrap_or_else(Value::null))
            }
            "MAX" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery("MAX requires an argument".to_string()))?;
                let mut max: Option<Value> = None;

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    match &max {
                        None => max = Some(val),
                        Some(m) => {
                            if self.compare_values_for_ordering(&val, m)
                                == std::cmp::Ordering::Greater
                            {
                                max = Some(val);
                            }
                        }
                    }
                }

                Ok(max.unwrap_or_else(Value::null))
            }
            "ARRAY_AGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("ARRAY_AGG requires an argument".to_string())
                })?;
                let mut values = Vec::new();

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    values.push(val);
                }

                Ok(Value::array(values))
            }
            "STRING_AGG" | "GROUP_CONCAT" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("STRING_AGG requires an argument".to_string())
                })?;
                let separator = self
                    .extract_second_function_arg(func)
                    .and_then(|e| {
                        if let Some(row) = group_rows.first() {
                            evaluator
                                .evaluate(&e, row)
                                .ok()
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_else(|| ",".to_string());

                let mut parts = Vec::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        parts.push(val.to_string());
                    }
                }

                Ok(Value::string(parts.join(&separator)))
            }
            "ANY_VALUE" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("ANY_VALUE requires an argument".to_string())
                })?;
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(&expr, row)
                } else {
                    Ok(Value::null())
                }
            }
            "STDDEV" | "STDDEV_SAMP" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery(format!("{} requires an argument", name)))?;
                let mut n: u64 = 0;
                let mut mean: f64 = 0.0;
                let mut m2: f64 = 0.0;

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    let x = match val.as_f64() {
                        Some(f) => f,
                        None => match val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let delta = x - mean;
                    mean += delta / n as f64;
                    let delta2 = x - mean;
                    m2 += delta * delta2;
                }

                if n < 2 {
                    Ok(Value::null())
                } else {
                    let variance = m2 / (n - 1) as f64;
                    Ok(Value::float64(variance.sqrt()))
                }
            }
            "STDDEV_POP" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("STDDEV_POP requires an argument".to_string())
                })?;
                let mut n: u64 = 0;
                let mut mean: f64 = 0.0;
                let mut m2: f64 = 0.0;

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    let x = match val.as_f64() {
                        Some(f) => f,
                        None => match val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let delta = x - mean;
                    mean += delta / n as f64;
                    let delta2 = x - mean;
                    m2 += delta * delta2;
                }

                if n == 0 {
                    Ok(Value::null())
                } else {
                    let variance = m2 / n as f64;
                    Ok(Value::float64(variance.sqrt()))
                }
            }
            "VARIANCE" | "VAR_SAMP" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery(format!("{} requires an argument", name)))?;
                let mut n: u64 = 0;
                let mut mean: f64 = 0.0;
                let mut m2: f64 = 0.0;

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    let x = match val.as_f64() {
                        Some(f) => f,
                        None => match val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let delta = x - mean;
                    mean += delta / n as f64;
                    let delta2 = x - mean;
                    m2 += delta * delta2;
                }

                if n < 2 {
                    Ok(Value::null())
                } else {
                    let variance = m2 / (n - 1) as f64;
                    Ok(Value::float64(variance))
                }
            }
            "VAR_POP" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("VAR_POP requires an argument".to_string())
                })?;
                let mut n: u64 = 0;
                let mut mean: f64 = 0.0;
                let mut m2: f64 = 0.0;

                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    let x = match val.as_f64() {
                        Some(f) => f,
                        None => match val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let delta = x - mean;
                    mean += delta / n as f64;
                    let delta2 = x - mean;
                    m2 += delta * delta2;
                }

                if n == 0 {
                    Ok(Value::null())
                } else {
                    let variance = m2 / n as f64;
                    Ok(Value::float64(variance))
                }
            }
            "COVAR_POP" => {
                let x_expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("COVAR_POP requires two arguments".to_string())
                })?;
                let y_expr = self.extract_second_function_arg(func).ok_or_else(|| {
                    Error::InvalidQuery("COVAR_POP requires two arguments".to_string())
                })?;

                let mut n: u64 = 0;
                let mut mean_x: f64 = 0.0;
                let mut mean_y: f64 = 0.0;
                let mut co_moment: f64 = 0.0;

                for row in group_rows {
                    let x_val = evaluator.evaluate(&x_expr, row)?;
                    let y_val = evaluator.evaluate(&y_expr, row)?;
                    if x_val.is_null() || y_val.is_null() {
                        continue;
                    }
                    let x = match x_val.as_f64() {
                        Some(f) => f,
                        None => match x_val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    let y = match y_val.as_f64() {
                        Some(f) => f,
                        None => match y_val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let dx = x - mean_x;
                    mean_x += dx / n as f64;
                    mean_y += (y - mean_y) / n as f64;
                    co_moment += dx * (y - mean_y);
                }

                if n == 0 {
                    Ok(Value::null())
                } else {
                    Ok(Value::float64(co_moment / n as f64))
                }
            }
            "COVAR_SAMP" => {
                let x_expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("COVAR_SAMP requires two arguments".to_string())
                })?;
                let y_expr = self.extract_second_function_arg(func).ok_or_else(|| {
                    Error::InvalidQuery("COVAR_SAMP requires two arguments".to_string())
                })?;

                let mut n: u64 = 0;
                let mut mean_x: f64 = 0.0;
                let mut mean_y: f64 = 0.0;
                let mut co_moment: f64 = 0.0;

                for row in group_rows {
                    let x_val = evaluator.evaluate(&x_expr, row)?;
                    let y_val = evaluator.evaluate(&y_expr, row)?;
                    if x_val.is_null() || y_val.is_null() {
                        continue;
                    }
                    let x = match x_val.as_f64() {
                        Some(f) => f,
                        None => match x_val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    let y = match y_val.as_f64() {
                        Some(f) => f,
                        None => match y_val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let dx = x - mean_x;
                    mean_x += dx / n as f64;
                    mean_y += (y - mean_y) / n as f64;
                    co_moment += dx * (y - mean_y);
                }

                if n < 2 {
                    Ok(Value::null())
                } else {
                    Ok(Value::float64(co_moment / (n - 1) as f64))
                }
            }
            "CORR" => {
                let x_expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("CORR requires two arguments".to_string())
                })?;
                let y_expr = self.extract_second_function_arg(func).ok_or_else(|| {
                    Error::InvalidQuery("CORR requires two arguments".to_string())
                })?;

                let mut n: u64 = 0;
                let mut mean_x: f64 = 0.0;
                let mut mean_y: f64 = 0.0;
                let mut m2_x: f64 = 0.0;
                let mut m2_y: f64 = 0.0;
                let mut co_moment: f64 = 0.0;

                for row in group_rows {
                    let x_val = evaluator.evaluate(&x_expr, row)?;
                    let y_val = evaluator.evaluate(&y_expr, row)?;
                    if x_val.is_null() || y_val.is_null() {
                        continue;
                    }
                    let x = match x_val.as_f64() {
                        Some(f) => f,
                        None => match x_val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    let y = match y_val.as_f64() {
                        Some(f) => f,
                        None => match y_val.as_i64() {
                            Some(i) => i as f64,
                            None => continue,
                        },
                    };
                    n += 1;
                    let dx = x - mean_x;
                    let dy = y - mean_y;
                    mean_x += dx / n as f64;
                    mean_y += dy / n as f64;
                    let dx2 = x - mean_x;
                    let dy2 = y - mean_y;
                    m2_x += dx * dx2;
                    m2_y += dy * dy2;
                    co_moment += dx * dy2;
                }

                if n < 2 {
                    Ok(Value::null())
                } else {
                    let stddev_x = (m2_x / n as f64).sqrt();
                    let stddev_y = (m2_y / n as f64).sqrt();
                    if stddev_x == 0.0 || stddev_y == 0.0 {
                        Ok(Value::null())
                    } else {
                        let covar = co_moment / n as f64;
                        Ok(Value::float64(covar / (stddev_x * stddev_y)))
                    }
                }
            }
            "APPROX_COUNT_DISTINCT" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("APPROX_COUNT_DISTINCT requires an argument".to_string())
                })?;
                let mut seen: std::collections::HashSet<u64> = std::collections::HashSet::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        let hash = ahash::RandomState::with_seeds(1, 2, 3, 4)
                            .hash_one(format!("{:?}", val));
                        seen.insert(hash);
                    }
                }
                Ok(Value::int64(seen.len() as i64))
            }
            "APPROX_QUANTILES" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("APPROX_QUANTILES requires an argument".to_string())
                })?;
                let num_quantiles = self
                    .extract_second_function_arg(func)
                    .and_then(|e| {
                        if let Some(row) = group_rows.first() {
                            evaluator.evaluate(&e, row).ok().and_then(|v| v.as_i64())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(100) as usize;
                let mut values: Vec<f64> = Vec::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        if let Some(f) = val.as_f64() {
                            values.push(f);
                        } else if let Some(i) = val.as_i64() {
                            values.push(i as f64);
                        }
                    }
                }
                if values.is_empty() {
                    return Ok(Value::array(vec![]));
                }
                values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let n = values.len();
                let mut quantiles = Vec::with_capacity(num_quantiles + 1);
                for i in 0..=num_quantiles {
                    let idx = if num_quantiles == 0 {
                        0
                    } else {
                        (i * (n - 1)) / num_quantiles
                    };
                    quantiles.push(Value::float64(values[idx]));
                }
                Ok(Value::array(quantiles))
            }
            "APPROX_TOP_COUNT" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("APPROX_TOP_COUNT requires an argument".to_string())
                })?;
                let top_n = self
                    .extract_second_function_arg(func)
                    .and_then(|e| {
                        if let Some(row) = group_rows.first() {
                            evaluator.evaluate(&e, row).ok().and_then(|v| v.as_i64())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(10) as usize;
                let mut counts: std::collections::HashMap<String, i64> =
                    std::collections::HashMap::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        let key = format!("{:?}", val);
                        *counts.entry(key).or_insert(0) += 1;
                    }
                }
                let mut count_vec: Vec<_> = counts.into_iter().collect();
                count_vec.sort_by(|a, b| b.1.cmp(&a.1));
                let result: Vec<Value> = count_vec
                    .into_iter()
                    .take(top_n)
                    .map(|(val, count)| {
                        let parsed_val = if val.starts_with("String(\"") && val.ends_with("\")") {
                            Value::string(val[8..val.len() - 2].to_string())
                        } else if val.starts_with("Int64(") && val.ends_with(")") {
                            val[6..val.len() - 1]
                                .parse::<i64>()
                                .map(Value::int64)
                                .unwrap_or_else(|_| Value::string(val))
                        } else {
                            Value::string(val)
                        };
                        Value::Struct(vec![
                            ("value".to_string(), parsed_val),
                            ("count".to_string(), Value::int64(count)),
                        ])
                    })
                    .collect();
                Ok(Value::array(result))
            }
            "APPROX_TOP_SUM" => {
                let value_expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("APPROX_TOP_SUM requires a value argument".to_string())
                })?;
                let weight_expr = self.extract_second_function_arg(func).ok_or_else(|| {
                    Error::InvalidQuery("APPROX_TOP_SUM requires a weight argument".to_string())
                })?;
                let third_arg = self.extract_third_function_arg(func);
                let top_n = third_arg
                    .and_then(|e| {
                        if let Some(row) = group_rows.first() {
                            evaluator.evaluate(&e, row).ok().and_then(|v| v.as_i64())
                        } else {
                            None
                        }
                    })
                    .unwrap_or(10) as usize;
                let mut sums: std::collections::HashMap<String, f64> =
                    std::collections::HashMap::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&value_expr, row)?;
                    let weight_val = evaluator.evaluate(&weight_expr, row)?;
                    if !val.is_null() && !weight_val.is_null() {
                        let key = format!("{:?}", val);
                        let weight = weight_val
                            .as_f64()
                            .or_else(|| weight_val.as_i64().map(|i| i as f64))
                            .unwrap_or(0.0);
                        *sums.entry(key).or_insert(0.0) += weight;
                    }
                }
                let mut sum_vec: Vec<_> = sums.into_iter().collect();
                sum_vec.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
                let result: Vec<Value> = sum_vec
                    .into_iter()
                    .take(top_n)
                    .map(|(val, sum)| {
                        let parsed_val = if val.starts_with("String(\"") && val.ends_with("\")") {
                            Value::string(val[8..val.len() - 2].to_string())
                        } else if val.starts_with("Int64(") && val.ends_with(")") {
                            val[6..val.len() - 1]
                                .parse::<i64>()
                                .map(Value::int64)
                                .unwrap_or_else(|_| Value::string(val))
                        } else {
                            Value::string(val)
                        };
                        Value::Struct(vec![
                            ("value".to_string(), parsed_val),
                            ("sum".to_string(), Value::float64(sum)),
                        ])
                    })
                    .collect();
                Ok(Value::array(result))
            }
            "HLL_COUNT_INIT" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("HLL_COUNT_INIT requires an argument".to_string())
                })?;
                let mut hll_registers: Vec<u8> = vec![0; 16384];
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        let hash = ahash::RandomState::with_seeds(1, 2, 3, 4)
                            .hash_one(format!("{:?}", val));
                        let idx = (hash & 0x3FFF) as usize;
                        let w = ((hash >> 14) | (1 << 50)).trailing_zeros() as u8 + 1;
                        if w > hll_registers[idx] {
                            hll_registers[idx] = w;
                        }
                    }
                }
                let encoded = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &hll_registers,
                );
                Ok(Value::string(format!("HLL_SKETCH:p14:{}", encoded)))
            }
            "HLL_COUNT_MERGE" | "HLL_COUNT_MERGE_PARTIAL" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("HLL_COUNT_MERGE requires an argument".to_string())
                })?;
                let mut merged_registers: Vec<u8> = vec![0; 16384];
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if let Some(sketch_str) = val.as_str() {
                        if let Some(encoded) = sketch_str.strip_prefix("HLL_SKETCH:p14:") {
                            if let Ok(registers) = base64::Engine::decode(
                                &base64::engine::general_purpose::STANDARD,
                                encoded,
                            ) {
                                for (i, &r) in registers.iter().enumerate().take(16384) {
                                    if r > merged_registers[i] {
                                        merged_registers[i] = r;
                                    }
                                }
                            }
                        } else if let Some(rest) = sketch_str.strip_prefix("HLL_SKETCH:p15:n") {
                            if let Ok(count) = rest.parse::<i64>() {
                                for i in 0..count {
                                    let h = ahash::RandomState::with_seeds(1, 2, 3, 4).hash_one(i);
                                    let idx = (h & 0x3FFF) as usize;
                                    let w = ((h >> 14) | (1 << 50)).trailing_zeros() as u8 + 1;
                                    if w > merged_registers[idx] {
                                        merged_registers[idx] = w;
                                    }
                                }
                            }
                        }
                    }
                }
                let encoded = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    &merged_registers,
                );
                Ok(Value::string(format!("HLL_SKETCH:p14:{}", encoded)))
            }
            "LISTAGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("LISTAGG requires an argument".to_string())
                })?;
                let separator = self
                    .extract_second_function_arg(func)
                    .and_then(|e| {
                        if let Some(row) = group_rows.first() {
                            evaluator
                                .evaluate(&e, row)
                                .ok()
                                .and_then(|v| v.as_str().map(|s| s.to_string()))
                        } else {
                            None
                        }
                    })
                    .unwrap_or_default();

                let mut parts = Vec::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        parts.push(val.to_string());
                    }
                }

                Ok(Value::string(parts.join(&separator)))
            }
            "COUNTIF" | "COUNT_IF" => {
                let condition_expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("COUNT_IF requires a condition argument".to_string())
                })?;
                let mut count = 0i64;
                for row in group_rows {
                    let cond = evaluator.evaluate(&condition_expr, row)?;
                    if let Some(b) = cond.as_bool() {
                        if b {
                            count += 1;
                        }
                    }
                }
                Ok(Value::int64(count))
            }
            "SUMIF" | "SUM_IF" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("SUM_IF requires an expression argument".to_string())
                })?;
                let condition_expr = self.extract_second_function_arg(func).ok_or_else(|| {
                    Error::InvalidQuery("SUM_IF requires a condition argument".to_string())
                })?;

                let mut sum_int: Option<i64> = None;
                let mut sum_float: Option<f64> = None;

                for row in group_rows {
                    let cond = evaluator.evaluate(&condition_expr, row)?;
                    let cond_true = cond.as_bool().unwrap_or(false);
                    if !cond_true {
                        continue;
                    }
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        sum_int = Some(sum_int.unwrap_or(0) + i);
                    } else if let Some(f) = val.as_f64() {
                        sum_float = Some(sum_float.unwrap_or(0.0) + f);
                    }
                }

                if let Some(s) = sum_float {
                    Ok(Value::float64(s + sum_int.unwrap_or(0) as f64))
                } else if let Some(s) = sum_int {
                    Ok(Value::int64(s))
                } else {
                    Ok(Value::null())
                }
            }
            "XMLAGG" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("XMLAGG requires an argument".to_string())
                })?;

                let mut parts = Vec::new();
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if !val.is_null() {
                        parts.push(val.to_string());
                    }
                }

                Ok(Value::string(parts.join("")))
            }
            "BIT_AND" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("BIT_AND requires an argument".to_string())
                })?;
                let mut result: Option<i64> = None;
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        result = Some(match result {
                            None => i,
                            Some(r) => r & i,
                        });
                    }
                }
                Ok(result.map(Value::int64).unwrap_or_else(Value::null))
            }
            "BIT_OR" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("BIT_OR requires an argument".to_string())
                })?;
                let mut result: Option<i64> = None;
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        result = Some(match result {
                            None => i,
                            Some(r) => r | i,
                        });
                    }
                }
                Ok(result.map(Value::int64).unwrap_or_else(Value::null))
            }
            "BIT_XOR" => {
                let expr = arg_expr.ok_or_else(|| {
                    Error::InvalidQuery("BIT_XOR requires an argument".to_string())
                })?;
                let mut result: Option<i64> = None;
                for row in group_rows {
                    let val = evaluator.evaluate(&expr, row)?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        result = Some(match result {
                            None => i,
                            Some(r) => r ^ i,
                        });
                    }
                }
                Ok(result.map(Value::int64).unwrap_or_else(Value::null))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Aggregate function {} not yet supported",
                name
            ))),
        }
    }

    fn extract_first_function_arg(&self, func: &ast::Function) -> Result<Option<Expr>> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.first() {
                match arg {
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) => {
                        return Ok(Some(expr.clone()));
                    }
                    ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard) => {
                        return Ok(None);
                    }
                    _ => {}
                }
            }
        }
        Ok(None)
    }

    fn extract_second_function_arg(&self, func: &ast::Function) -> Option<Expr> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.get(1) {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    return Some(expr.clone());
                }
            }
        }
        None
    }

    fn extract_third_function_arg(&self, func: &ast::Function) -> Option<Expr> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.get(2) {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    return Some(expr.clone());
                }
            }
        }
        None
    }

    fn exprs_equal(&self, a: &Expr, b: &Expr) -> bool {
        match (a, b) {
            (Expr::Identifier(a_id), Expr::Identifier(b_id)) => {
                a_id.value.to_uppercase() == b_id.value.to_uppercase()
            }
            (Expr::CompoundIdentifier(a_parts), Expr::CompoundIdentifier(b_parts)) => {
                a_parts.len() == b_parts.len()
                    && a_parts
                        .iter()
                        .zip(b_parts.iter())
                        .all(|(a, b)| a.value.to_uppercase() == b.value.to_uppercase())
            }
            (Expr::Identifier(a_id), Expr::CompoundIdentifier(b_parts)) => {
                b_parts.last().map(|p| p.value.to_uppercase()) == Some(a_id.value.to_uppercase())
            }
            (Expr::CompoundIdentifier(a_parts), Expr::Identifier(b_id)) => {
                a_parts.last().map(|p| p.value.to_uppercase()) == Some(b_id.value.to_uppercase())
            }
            _ => false,
        }
    }

    fn compare_values_for_ordering(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        if a.is_null() && b.is_null() {
            return std::cmp::Ordering::Equal;
        }
        if a.is_null() {
            return std::cmp::Ordering::Greater;
        }
        if b.is_null() {
            return std::cmp::Ordering::Less;
        }

        if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
            return ai.cmp(&bi);
        }
        if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
            return af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
            return as_.cmp(bs);
        }
        if let (Some(ad), Some(bd)) = (a.as_date(), b.as_date()) {
            return ad.cmp(&bd);
        }
        if let (Some(at), Some(bt)) = (a.as_timestamp(), b.as_timestamp()) {
            return at.cmp(&bt);
        }
        if let (Some(at), Some(bt)) = (a.as_time(), b.as_time()) {
            return at.cmp(&bt);
        }
        if let (Some(an), Some(bn)) = (a.as_numeric(), b.as_numeric()) {
            return an.cmp(&bn);
        }
        if let (Some(ab), Some(bb)) = (a.as_bytes(), b.as_bytes()) {
            return ab.cmp(bb);
        }
        std::cmp::Ordering::Equal
    }

    fn project_rows(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
    ) -> Result<(Schema, Vec<Record>)> {
        let resolved_projection = self.resolve_projection_subqueries(projection)?;

        if self.has_array_join(&resolved_projection) {
            return self.project_rows_with_array_join(input_schema, rows, &resolved_projection);
        }

        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());
        let sample_record = rows.first().cloned().unwrap_or_else(|| {
            Record::from_values(vec![Value::null(); input_schema.field_count()])
        });

        let mut all_cols: Vec<(String, DataType)> = Vec::new();

        for (idx, item) in resolved_projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(opts) => {
                    let except_cols = Self::get_except_columns(opts);
                    let replace_map = Self::get_replace_map(opts);
                    for field in input_schema.fields() {
                        if !except_cols.contains(&field.name.to_lowercase()) {
                            if let Some(replace_expr) = replace_map.get(&field.name.to_lowercase())
                            {
                                let val = evaluator
                                    .evaluate(replace_expr, &sample_record)
                                    .unwrap_or(Value::null());
                                all_cols.push((field.name.clone(), val.data_type()));
                            } else {
                                all_cols.push((field.name.clone(), field.data_type.clone()));
                            }
                        }
                    }
                }
                SelectItem::QualifiedWildcard(name, opts) => {
                    let table_name = name.to_string();
                    let except_cols = Self::get_except_columns(opts);
                    let replace_map = Self::get_replace_map(opts);
                    for field in input_schema.fields() {
                        let matches_table = field
                            .source_table
                            .as_ref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                        if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                            if let Some(replace_expr) = replace_map.get(&field.name.to_lowercase())
                            {
                                let val = evaluator
                                    .evaluate(replace_expr, &sample_record)
                                    .unwrap_or(Value::null());
                                all_cols.push((field.name.clone(), val.data_type()));
                            } else {
                                all_cols.push((field.name.clone(), field.data_type.clone()));
                            }
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let resolved_expr = self.resolve_scalar_subqueries(expr)?;
                    let val = evaluator
                        .evaluate(&resolved_expr, &sample_record)
                        .unwrap_or(Value::null());
                    let name = self.expr_to_alias(expr, idx);
                    let data_type = if val.data_type() == DataType::Unknown {
                        self.infer_expr_type(expr, input_schema)
                            .unwrap_or(DataType::Unknown)
                    } else {
                        val.data_type()
                    };
                    all_cols.push((name, data_type));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let resolved_expr = self.resolve_scalar_subqueries(expr)?;
                    let val = evaluator
                        .evaluate(&resolved_expr, &sample_record)
                        .unwrap_or(Value::null());
                    let data_type = if val.data_type() == DataType::Unknown {
                        self.infer_expr_type(expr, input_schema)
                            .unwrap_or(DataType::Unknown)
                    } else {
                        val.data_type()
                    };
                    all_cols.push((alias.value.clone(), data_type));
                }
            }
        }

        let fields: Vec<Field> = all_cols
            .iter()
            .map(|(name, dt)| Field::nullable(name.clone(), dt.clone()))
            .collect();
        let output_schema = Schema::from_fields(fields);

        let mut output_rows = Vec::with_capacity(rows.len());
        for row in rows {
            let mut values = Vec::new();
            for item in &resolved_projection {
                match item {
                    SelectItem::Wildcard(opts) => {
                        let except_cols = Self::get_except_columns(opts);
                        let replace_map = Self::get_replace_map(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            if !except_cols.contains(&field.name.to_lowercase()) {
                                if let Some(replace_expr) =
                                    replace_map.get(&field.name.to_lowercase())
                                {
                                    let val = evaluator.evaluate(replace_expr, row)?;
                                    values.push(val);
                                } else {
                                    values.push(row.values()[i].clone());
                                }
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(name, opts) => {
                        let table_name = name.to_string();
                        let except_cols = Self::get_except_columns(opts);
                        let replace_map = Self::get_replace_map(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            let matches_table = field
                                .source_table
                                .as_ref()
                                .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                            if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                                if let Some(replace_expr) =
                                    replace_map.get(&field.name.to_lowercase())
                                {
                                    let val = evaluator.evaluate(replace_expr, row)?;
                                    values.push(val);
                                } else {
                                    values.push(row.values()[i].clone());
                                }
                            }
                        }
                    }
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let resolved_expr = self.resolve_scalar_subqueries(expr)?;
                        let val = evaluator.evaluate(&resolved_expr, row)?;
                        values.push(val);
                    }
                }
            }
            output_rows.push(Record::from_values(values));
        }

        Ok((output_schema, output_rows))
    }

    fn resolve_projection_subqueries(&self, projection: &[SelectItem]) -> Result<Vec<SelectItem>> {
        projection
            .iter()
            .map(|item| match item {
                SelectItem::UnnamedExpr(expr) => {
                    let resolved = self.resolve_scalar_subqueries(expr)?;
                    Ok(SelectItem::UnnamedExpr(resolved))
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let resolved = self.resolve_scalar_subqueries(expr)?;
                    Ok(SelectItem::ExprWithAlias {
                        expr: resolved,
                        alias: alias.clone(),
                    })
                }
                other => Ok(other.clone()),
            })
            .collect()
    }

    fn project_rows_with_array_join(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator = Evaluator::new(input_schema);

        let mut array_join_col_idx: Option<usize> = None;
        let mut all_cols: Vec<(String, DataType)> = Vec::new();

        for (idx, item) in projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(opts) => {
                    let except_cols = Self::get_except_columns(opts);
                    for field in input_schema.fields() {
                        if !except_cols.contains(&field.name.to_lowercase()) {
                            all_cols.push((field.name.clone(), field.data_type.clone()));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(name, opts) => {
                    let table_name = name.to_string();
                    let except_cols = Self::get_except_columns(opts);
                    for field in input_schema.fields() {
                        let matches_table = field
                            .source_table
                            .as_ref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                        if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                            all_cols.push((field.name.clone(), field.data_type.clone()));
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let sample_record = rows.first().cloned().unwrap_or_else(|| {
                        Record::from_values(vec![Value::null(); input_schema.field_count()])
                    });
                    let val = evaluator
                        .evaluate(expr, &sample_record)
                        .unwrap_or(Value::null());
                    let name = self.expr_to_alias(expr, idx);
                    if self.expr_has_array_join(expr) {
                        array_join_col_idx = Some(all_cols.len());
                        let inner_type = match val.data_type() {
                            DataType::Array(inner) => (*inner).clone(),
                            _ => val.data_type(),
                        };
                        all_cols.push((name, inner_type));
                    } else {
                        all_cols.push((name, val.data_type()));
                    }
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let sample_record = rows.first().cloned().unwrap_or_else(|| {
                        Record::from_values(vec![Value::null(); input_schema.field_count()])
                    });
                    let val = evaluator
                        .evaluate(expr, &sample_record)
                        .unwrap_or(Value::null());
                    if self.expr_has_array_join(expr) {
                        array_join_col_idx = Some(all_cols.len());
                        let inner_type = match val.data_type() {
                            DataType::Array(inner) => (*inner).clone(),
                            _ => val.data_type(),
                        };
                        all_cols.push((alias.value.clone(), inner_type));
                    } else {
                        all_cols.push((alias.value.clone(), val.data_type()));
                    }
                }
            }
        }

        let fields: Vec<Field> = all_cols
            .iter()
            .map(|(name, dt)| Field::nullable(name.clone(), dt.clone()))
            .collect();
        let output_schema = Schema::from_fields(fields);

        let mut output_rows = Vec::new();
        for row in rows {
            let mut base_values: Vec<Value> = Vec::new();
            let mut array_values: Vec<Value> = Vec::new();

            for item in projection {
                match item {
                    SelectItem::Wildcard(opts) => {
                        let except_cols = Self::get_except_columns(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            if !except_cols.contains(&field.name.to_lowercase()) {
                                base_values.push(row.values()[i].clone());
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(name, opts) => {
                        let table_name = name.to_string();
                        let except_cols = Self::get_except_columns(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            let matches_table = field
                                .source_table
                                .as_ref()
                                .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                            if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                                base_values.push(row.values()[i].clone());
                            }
                        }
                    }
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let val = evaluator.evaluate(expr, row)?;
                        if self.expr_has_array_join(expr) {
                            if let Some(arr) = val.as_array() {
                                array_values = arr.to_vec();
                            } else {
                                array_values = vec![val];
                            }
                            base_values.push(Value::null());
                        } else {
                            base_values.push(val);
                        }
                    }
                }
            }

            if let Some(arr_idx) = array_join_col_idx {
                if array_values.is_empty() {
                    continue;
                }
                for arr_val in array_values {
                    let mut new_values = base_values.clone();
                    new_values[arr_idx] = arr_val;
                    output_rows.push(Record::from_values(new_values));
                }
            } else {
                output_rows.push(Record::from_values(base_values));
            }
        }

        Ok((output_schema, output_rows))
    }

    fn sort_rows(&self, schema: &Schema, rows: &mut Vec<Record>, order_by: &OrderBy) -> Result<()> {
        let evaluator = Evaluator::with_user_functions(schema, self.catalog.get_functions());

        let exprs: &[OrderByExpr] = match &order_by.kind {
            OrderByKind::Expressions(exprs) => exprs,
            OrderByKind::All(_) => return Ok(()),
        };

        rows.sort_by(|a, b| {
            for order_expr in exprs {
                let a_val = evaluator
                    .evaluate(&order_expr.expr, a)
                    .unwrap_or(Value::null());
                let b_val = evaluator
                    .evaluate(&order_expr.expr, b)
                    .unwrap_or(Value::null());

                let asc = order_expr.options.asc.unwrap_or(true);
                let nulls_first = order_expr.options.nulls_first.unwrap_or(!asc);

                let a_is_null = a_val.is_null();
                let b_is_null = b_val.is_null();

                if a_is_null && b_is_null {
                    continue;
                }
                if a_is_null {
                    return if nulls_first {
                        std::cmp::Ordering::Less
                    } else {
                        std::cmp::Ordering::Greater
                    };
                }
                if b_is_null {
                    return if nulls_first {
                        std::cmp::Ordering::Greater
                    } else {
                        std::cmp::Ordering::Less
                    };
                }

                let ordering = self.compare_values(&a_val, &b_val);
                let ordering = if asc { ordering } else { ordering.reverse() };

                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(())
    }

    fn compare_values_with_nulls(
        &self,
        a: &Value,
        b: &Value,
        nulls_first: bool,
    ) -> std::cmp::Ordering {
        if a.is_null() && b.is_null() {
            return std::cmp::Ordering::Equal;
        }
        if a.is_null() {
            return if nulls_first {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Greater
            };
        }
        if b.is_null() {
            return if nulls_first {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Less
            };
        }
        self.compare_values(a, b)
    }

    fn compare_values(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        if a.is_null() && b.is_null() {
            return std::cmp::Ordering::Equal;
        }
        if a.is_null() {
            return std::cmp::Ordering::Greater;
        }
        if b.is_null() {
            return std::cmp::Ordering::Less;
        }

        if let (Some(a_i), Some(b_i)) = (a.as_i64(), b.as_i64()) {
            return a_i.cmp(&b_i);
        }
        if let (Some(a_f), Some(b_f)) = (a.as_f64(), b.as_f64()) {
            return a_f.partial_cmp(&b_f).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let (Some(a_s), Some(b_s)) = (a.as_str(), b.as_str()) {
            return a_s.cmp(b_s);
        }
        if let (Some(a_b), Some(b_b)) = (a.as_bool(), b.as_bool()) {
            return a_b.cmp(&b_b);
        }
        if let (Some(a_d), Some(b_d)) = (a.as_date(), b.as_date()) {
            return a_d.cmp(&b_d);
        }
        if let (Some(a_t), Some(b_t)) = (a.as_timestamp(), b.as_timestamp()) {
            return a_t.cmp(&b_t);
        }
        if let (Some(a_t), Some(b_t)) = (a.as_time(), b.as_time()) {
            return a_t.cmp(&b_t);
        }
        if let (Some(a_n), Some(b_n)) = (a.as_numeric(), b.as_numeric()) {
            return a_n.cmp(&b_n);
        }
        if let (Some(a_b), Some(b_b)) = (a.as_bytes(), b.as_bytes()) {
            return a_b.cmp(b_b);
        }

        std::cmp::Ordering::Equal
    }

    fn execute_values(&self, values: &ast::Values) -> Result<Table> {
        if values.rows.is_empty() {
            return Ok(Table::empty(Schema::new()));
        }

        let first_row = &values.rows[0];
        let num_cols = first_row.len();

        let mut all_rows: Vec<Vec<Value>> = Vec::new();
        for row_exprs in &values.rows {
            if row_exprs.len() != num_cols {
                return Err(Error::InvalidQuery(
                    "All rows must have the same number of columns".to_string(),
                ));
            }
            let mut row_values = Vec::new();
            for expr in row_exprs {
                let val = self.evaluate_literal_expr(expr)?;
                row_values.push(val);
            }
            all_rows.push(row_values);
        }

        let fields: Vec<Field> = (0..num_cols)
            .map(|i| {
                let dt = all_rows
                    .iter()
                    .find_map(|row| {
                        let dt = row[i].data_type();
                        if dt != DataType::Unknown {
                            Some(dt)
                        } else {
                            None
                        }
                    })
                    .unwrap_or(DataType::String);
                Field::nullable(format!("column{}", i + 1), dt)
            })
            .collect();

        let schema = Schema::from_fields(fields);
        let rows: Vec<Record> = all_rows.into_iter().map(Record::from_values).collect();

        Table::from_records(schema, rows)
    }

    fn execute_create_table(&mut self, create: &ast::CreateTable) -> Result<Table> {
        let table_name = create.name.to_string();

        if create.or_replace {
            let _ = self.catalog.drop_table(&table_name);
        } else if self.catalog.table_exists(&table_name) && !create.if_not_exists {
            return Err(Error::invalid_query(format!(
                "Table already exists: {}",
                table_name
            )));
        }

        if self.catalog.table_exists(&table_name) {
            return Ok(Table::empty(Schema::new()));
        }

        let mut column_defaults = Vec::new();

        let fields: Vec<Field> = create
            .columns
            .iter()
            .map(|col| {
                let data_type = self.sql_type_to_data_type(&col.data_type)?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ast::ColumnOption::NotNull));

                for opt in &col.options {
                    if let ast::ColumnOption::Default(expr) = &opt.option {
                        column_defaults.push(ColumnDefault {
                            column_name: col.name.value.clone(),
                            default_expr: expr.clone(),
                        });
                    }
                }

                let default_value = col.options.iter().find_map(|opt| {
                    if let ast::ColumnOption::Default(expr) = &opt.option {
                        self.evaluate_literal_expr(expr).ok()
                    } else {
                        None
                    }
                });
                let mut field = if nullable {
                    Field::nullable(col.name.value.clone(), data_type)
                } else {
                    Field::required(col.name.value.clone(), data_type)
                };
                if let Some(default) = default_value {
                    field = field.with_default(default);
                }
                Ok(field)
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = Schema::from_fields(fields);
        self.catalog.create_table(&table_name, schema)?;

        if !column_defaults.is_empty() {
            self.catalog
                .set_table_defaults(&table_name, column_defaults);
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_create_view(
        &mut self,
        name: &ObjectName,
        columns: &[ast::ViewColumnDef],
        query: &Query,
        or_replace: bool,
        if_not_exists: bool,
        materialized: bool,
    ) -> Result<Table> {
        let view_name = name.to_string();

        if materialized {
            let result_table = self.execute_query(query)?;
            let schema = result_table.schema().clone();

            if or_replace {
                let _ = self.catalog.drop_table(&view_name);
            } else if self.catalog.table_exists(&view_name) && !if_not_exists {
                return Err(Error::invalid_query(format!(
                    "Materialized view already exists: {}",
                    view_name
                )));
            }

            if self.catalog.table_exists(&view_name) {
                return Ok(Table::empty(Schema::new()));
            }

            self.catalog.create_table(&view_name, schema)?;

            let table_data = self.catalog.get_table_mut(&view_name).unwrap();
            for i in 0..result_table.row_count() {
                let row_values: Vec<Value> = result_table
                    .schema()
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(col_idx, field)| {
                        result_table
                            .columns()
                            .get(&field.name)
                            .map(|col| col.get_value(i))
                            .unwrap_or(Value::null())
                    })
                    .collect();
                table_data.push_row(row_values)?;
            }

            return Ok(Table::empty(Schema::new()));
        }

        let column_aliases: Vec<String> = columns.iter().map(|c| c.name.value.clone()).collect();
        let query_string = query.to_string();

        self.catalog.create_view(
            &view_name,
            query_string,
            column_aliases,
            or_replace,
            if_not_exists,
        )?;

        Ok(Table::empty(Schema::new()))
    }

    fn execute_drop(
        &mut self,
        object_type: &ast::ObjectType,
        names: &[ObjectName],
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        match object_type {
            ast::ObjectType::Table => {
                for name in names {
                    let table_name = name.to_string();
                    if if_exists && !self.catalog.table_exists(&table_name) {
                        continue;
                    }
                    self.catalog.drop_table(&table_name)?;
                }
                Ok(Table::empty(Schema::new()))
            }
            ast::ObjectType::Schema => {
                for name in names {
                    let schema_name = name.to_string();
                    self.catalog.drop_schema(&schema_name, if_exists, cascade)?;
                }
                Ok(Table::empty(Schema::new()))
            }
            ast::ObjectType::View => {
                for name in names {
                    let view_name = name.to_string();
                    self.catalog.drop_view(&view_name, if_exists)?;
                }
                Ok(Table::empty(Schema::new()))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "DROP {:?} not yet supported",
                object_type
            ))),
        }
    }

    fn execute_create_function(&mut self, create: &ast::CreateFunction) -> Result<Table> {
        let name = create.name.to_string();

        let language = create
            .language
            .as_ref()
            .map(|l| l.value.to_uppercase())
            .unwrap_or_else(|| "SQL".to_string());

        let body = match language.as_str() {
            "JAVASCRIPT" | "JS" => {
                let js_code = match &create.function_body {
                    Some(CreateFunctionBody::AsBeforeOptions(expr)) => {
                        self.extract_string_from_expr(expr)?
                    }
                    Some(CreateFunctionBody::AsAfterOptions(expr)) => {
                        self.extract_string_from_expr(expr)?
                    }
                    _ => {
                        return Err(Error::InvalidQuery(
                            "JavaScript UDF requires AS 'code' body".to_string(),
                        ));
                    }
                };
                FunctionBody::JavaScript(js_code)
            }
            "SQL" | "" => {
                let expr = match &create.function_body {
                    Some(CreateFunctionBody::AsBeforeOptions(expr)) => expr.clone(),
                    Some(CreateFunctionBody::AsAfterOptions(expr)) => expr.clone(),
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "SQL UDF requires AS (expr) body".to_string(),
                        ));
                    }
                };
                FunctionBody::Sql(Box::new(expr))
            }
            _ => {
                return Err(Error::UnsupportedFeature(format!(
                    "Unsupported function language: {}. Supported: SQL, JAVASCRIPT",
                    language
                )));
            }
        };

        let return_type = match &create.return_type {
            Some(dt) => self.sql_type_to_data_type(dt)?,
            None => {
                return Err(Error::InvalidQuery(
                    "RETURNS clause is required for CREATE FUNCTION".to_string(),
                ));
            }
        };

        let parameters = create.args.clone().unwrap_or_default();

        if create.if_not_exists && self.catalog.function_exists(&name) {
            return Ok(Table::empty(Schema::new()));
        }

        let func = UserFunction {
            name: name.clone(),
            parameters,
            return_type,
            body,
            is_temporary: create.temporary,
        };

        self.catalog.create_function(func, create.or_replace)?;
        Ok(Table::empty(Schema::new()))
    }

    fn extract_string_from_expr(&self, expr: &Expr) -> Result<String> {
        match expr {
            Expr::Value(val_with_span) => match &val_with_span.value {
                SqlValue::SingleQuotedString(s)
                | SqlValue::DoubleQuotedString(s)
                | SqlValue::TripleSingleQuotedString(s)
                | SqlValue::TripleDoubleQuotedString(s)
                | SqlValue::TripleSingleQuotedRawStringLiteral(s)
                | SqlValue::TripleDoubleQuotedRawStringLiteral(s)
                | SqlValue::SingleQuotedRawStringLiteral(s)
                | SqlValue::DoubleQuotedRawStringLiteral(s) => Ok(s.clone()),
                _ => Err(Error::InvalidQuery(format!(
                    "Expected string literal for function body, got: {:?}",
                    expr
                ))),
            },
            _ => Err(Error::InvalidQuery(format!(
                "Expected string literal for function body, got: {:?}",
                expr
            ))),
        }
    }

    fn execute_drop_function(
        &mut self,
        func_desc: &[ast::FunctionDesc],
        if_exists: bool,
    ) -> Result<Table> {
        for desc in func_desc {
            let name = desc.name.to_string();
            if if_exists && !self.catalog.function_exists(&name) {
                continue;
            }
            self.catalog.drop_function(&name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    fn execute_create_procedure(
        &mut self,
        name: &ObjectName,
        params: &Option<Vec<ast::ProcedureParam>>,
        body: &ast::ConditionalStatements,
        or_replace: bool,
    ) -> Result<Table> {
        let proc_name = name.to_string();
        let parameters = params.clone().unwrap_or_default();

        let proc = UserProcedure {
            name: proc_name,
            parameters,
            body: body.clone(),
        };

        self.catalog.create_procedure(proc, or_replace)?;
        Ok(Table::empty(Schema::new()))
    }

    fn execute_create_procedure_parsed(&mut self, parsed: &ParsedProcedure) -> Result<Table> {
        let statements = split_script_statements(&parsed.body);
        let dialect = BigQueryDialect {};
        let mut parsed_stmts = Vec::new();

        for stmt_sql in statements {
            let stmt_sql = stmt_sql.trim();
            if stmt_sql.is_empty() {
                continue;
            }
            match Parser::parse_sql(&dialect, stmt_sql) {
                Ok(stmts) => {
                    for stmt in stmts {
                        parsed_stmts.push(stmt);
                    }
                }
                Err(e) => {
                    return Err(Error::ParseError(format!(
                        "Error parsing procedure body: {}",
                        e
                    )));
                }
            }
        }

        let body = ast::ConditionalStatements::BeginEnd(ast::BeginEndStatements {
            begin_token: ast::helpers::attached_token::AttachedToken::empty(),
            statements: parsed_stmts,
            end_token: ast::helpers::attached_token::AttachedToken::empty(),
        });

        let proc = UserProcedure {
            name: parsed.name.clone(),
            parameters: parsed.parameters.clone(),
            body,
        };

        self.catalog.create_procedure(proc, parsed.or_replace)?;
        Ok(Table::empty(Schema::new()))
    }

    fn execute_drop_procedure(
        &mut self,
        proc_desc: &[ast::FunctionDesc],
        if_exists: bool,
    ) -> Result<Table> {
        for desc in proc_desc {
            let name = desc.name.to_string();
            if if_exists && !self.catalog.procedure_exists(&name) {
                continue;
            }
            self.catalog.drop_procedure(&name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    fn execute_call(&mut self, func: &ast::Function) -> Result<Table> {
        let name = func.name.to_string();
        let proc = self
            .catalog
            .get_procedure(&name)
            .ok_or_else(|| Error::invalid_query(format!("Procedure not found: {}", name)))?
            .clone();

        let (args, arg_var_refs) = self.extract_call_args_with_refs(func)?;

        self.push_scope();

        for (i, param) in proc.parameters.iter().enumerate() {
            let param_name = param.name.value.to_uppercase();
            let value = args.get(i).cloned().unwrap_or(Value::null());
            let data_type = value.data_type();
            self.declare_variable(&param_name, data_type, Some(value));
        }

        let result = match &proc.body {
            ast::ConditionalStatements::Sequence { statements } => {
                let mut result = Table::empty(Schema::new());
                for stmt in statements {
                    result = self.execute_statement(stmt)?;
                }
                Ok(result)
            }
            ast::ConditionalStatements::BeginEnd(begin_end) => {
                let mut result = Table::empty(Schema::new());
                for stmt in &begin_end.statements {
                    result = self.execute_statement(stmt)?;
                }
                Ok(result)
            }
        };

        let mut out_values: Vec<(String, Value)> = Vec::new();
        for (i, param) in proc.parameters.iter().enumerate() {
            let is_out_or_inout = matches!(
                param.mode,
                Some(ast::ArgMode::Out) | Some(ast::ArgMode::InOut)
            );
            if is_out_or_inout {
                if let Some(var_ref) = arg_var_refs.get(i).and_then(|r| r.as_ref()) {
                    let param_name = param.name.value.to_uppercase();
                    if let Some(var) = self.get_variable(&param_name) {
                        out_values.push((var_ref.clone(), var.value.clone()));
                    }
                }
            }
        }

        self.pop_scope();

        for (var_name, value) in out_values {
            self.set_variable(&var_name, value)?;
        }

        result
    }

    fn extract_call_args_with_refs(
        &self,
        func: &ast::Function,
    ) -> Result<(Vec<Value>, Vec<Option<String>>)> {
        let empty_record = Record::from_values(vec![]);
        let empty_schema = Schema::new();
        let evaluator = Evaluator::with_user_functions(&empty_schema, self.catalog.get_functions());

        match &func.args {
            ast::FunctionArguments::None => Ok((vec![], vec![])),
            ast::FunctionArguments::Subquery(_) => Err(Error::UnsupportedFeature(
                "Subquery arguments not supported in CALL".to_string(),
            )),
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                let mut var_refs = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(expr) => {
                                let (value, var_ref) = self.evaluate_call_arg_with_ref(
                                    expr,
                                    &evaluator,
                                    &empty_record,
                                )?;
                                args.push(value);
                                var_refs.push(var_ref);
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                return Err(Error::InvalidQuery(
                                    "Wildcard not allowed in CALL".to_string(),
                                ));
                            }
                            ast::FunctionArgExpr::QualifiedWildcard(_) => {
                                return Err(Error::InvalidQuery(
                                    "Qualified wildcard not allowed in CALL".to_string(),
                                ));
                            }
                        },
                        ast::FunctionArg::Named { arg, .. }
                        | ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                            ast::FunctionArgExpr::Expr(expr) => {
                                let (value, var_ref) = self.evaluate_call_arg_with_ref(
                                    expr,
                                    &evaluator,
                                    &empty_record,
                                )?;
                                args.push(value);
                                var_refs.push(var_ref);
                            }
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "Invalid argument in CALL".to_string(),
                                ));
                            }
                        },
                    }
                }
                Ok((args, var_refs))
            }
        }
    }

    fn evaluate_call_arg_with_ref(
        &self,
        expr: &Expr,
        evaluator: &Evaluator,
        record: &Record,
    ) -> Result<(Value, Option<String>)> {
        match expr {
            Expr::Identifier(ident) if ident.value.starts_with('@') => {
                let var_name = ident.value.clone();
                let value = if let Some(var) = self.get_variable(&var_name) {
                    var.value.clone()
                } else {
                    Value::null()
                };
                Ok((value, Some(var_name)))
            }
            _ => {
                let value = evaluator.evaluate(expr, record)?;
                Ok((value, None))
            }
        }
    }

    fn execute_insert(&mut self, insert: &ast::Insert) -> Result<Table> {
        let table_name = insert.table.to_string();
        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();

        let column_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..schema.field_count()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|col| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == col.value.to_uppercase())
                        .ok_or_else(|| Error::ColumnNotFound(col.value.clone()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        let default_row_values = self.compute_default_row_values(&table_name, &schema)?;

        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| Error::InvalidQuery("INSERT requires VALUES or SELECT".to_string()))?;

        let default_row_values: Vec<Value> = schema
            .fields()
            .iter()
            .map(|f| f.default_value.clone().unwrap_or(Value::null()))
            .collect();

        match source.body.as_ref() {
            SetExpr::Values(values) => {
                for row_exprs in &values.rows {
                    if row_exprs.len() != column_indices.len() {
                        return Err(Error::InvalidQuery(format!(
                            "Expected {} values, got {}",
                            column_indices.len(),
                            row_exprs.len()
                        )));
                    }

                    let mut row_values = default_row_values.clone();
                    for (expr_idx, &col_idx) in column_indices.iter().enumerate() {
                        let col_name = &schema.fields()[col_idx].name;
                        let val =
                            self.evaluate_insert_expr(&row_exprs[expr_idx], &table_name, col_name)?;
                        let target_type = &schema.fields()[col_idx].data_type;
                        let coerced = self.coerce_value_to_type(val, target_type)?;
                        row_values[col_idx] = coerced;
                    }

                    let table_data = self.catalog.get_table_mut(&table_name).unwrap();
                    table_data.push_row(row_values)?;
                }
            }
            SetExpr::Select(select) => {
                let (_, rows) = self.evaluate_select_with_from(select)?;
                let table_data = self.catalog.get_table_mut(&table_name).unwrap();
                for row in rows {
                    let values = row.values();
                    if values.len() != column_indices.len() {
                        return Err(Error::InvalidQuery(format!(
                            "Expected {} values, got {}",
                            column_indices.len(),
                            values.len()
                        )));
                    }

                    let mut row_values = default_row_values.clone();
                    for (val_idx, &col_idx) in column_indices.iter().enumerate() {
                        row_values[col_idx] = values[val_idx].clone();
                    }
                    table_data.push_row(row_values)?;
                }
            }
            _ => {
                return Err(Error::UnsupportedFeature(
                    "INSERT source type not supported".to_string(),
                ));
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn compute_default_row_values(&self, table_name: &str, schema: &Schema) -> Result<Vec<Value>> {
        let mut row_values = vec![Value::null(); schema.field_count()];
        for (i, field) in schema.fields().iter().enumerate() {
            if let Some(default_expr) = self.catalog.get_column_default(table_name, &field.name) {
                let val = self.evaluate_literal_expr(default_expr)?;
                let coerced = self.coerce_value_to_type(val, &field.data_type)?;
                row_values[i] = coerced;
            }
        }
        Ok(row_values)
    }

    fn evaluate_insert_expr(
        &self,
        expr: &Expr,
        table_name: &str,
        column_name: &str,
    ) -> Result<Value> {
        if let Expr::Identifier(ident) = expr {
            if ident.value.to_uppercase() == "DEFAULT" {
                if let Some(default_expr) = self.catalog.get_column_default(table_name, column_name)
                {
                    return self.evaluate_literal_expr(default_expr);
                } else {
                    return Ok(Value::null());
                }
            }
        }
        self.evaluate_literal_expr(expr)
    }

    fn execute_insert_with_ctes(
        &mut self,
        insert: &ast::Insert,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        let table_name = insert.table.to_string();
        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();

        let column_indices: Vec<usize> = if insert.columns.is_empty() {
            (0..schema.field_count()).collect()
        } else {
            insert
                .columns
                .iter()
                .map(|col| {
                    schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == col.value.to_uppercase())
                        .ok_or_else(|| Error::ColumnNotFound(col.value.clone()))
                })
                .collect::<Result<Vec<_>>>()?
        };

        let source = insert
            .source
            .as_ref()
            .ok_or_else(|| Error::InvalidQuery("INSERT requires VALUES or SELECT".to_string()))?;

        match source.body.as_ref() {
            SetExpr::Values(values) => {
                for row_exprs in &values.rows {
                    if row_exprs.len() != column_indices.len() {
                        return Err(Error::InvalidQuery(format!(
                            "Expected {} values, got {}",
                            column_indices.len(),
                            row_exprs.len()
                        )));
                    }

                    let mut row_values = vec![Value::null(); schema.field_count()];
                    for (expr_idx, &col_idx) in column_indices.iter().enumerate() {
                        let val = self.evaluate_literal_expr(&row_exprs[expr_idx])?;
                        let target_type = &schema.fields()[col_idx].data_type;
                        let coerced = self.coerce_value_to_type(val, target_type)?;
                        row_values[col_idx] = coerced;
                    }

                    let table_data = self.catalog.get_table_mut(&table_name).unwrap();
                    table_data.push_row(row_values)?;
                }
            }
            SetExpr::Select(select) => {
                let result = self.execute_select_with_ctes(select, &None, &None, cte_tables)?;
                let rows = result.to_records()?;
                let table_data = self.catalog.get_table_mut(&table_name).unwrap();
                for row in rows {
                    let values = row.values();
                    if values.len() != column_indices.len() {
                        return Err(Error::InvalidQuery(format!(
                            "Expected {} values, got {}",
                            column_indices.len(),
                            values.len()
                        )));
                    }

                    let mut row_values = vec![Value::null(); schema.field_count()];
                    for (val_idx, &col_idx) in column_indices.iter().enumerate() {
                        row_values[col_idx] = values[val_idx].clone();
                    }
                    table_data.push_row(row_values)?;
                }
            }
            _ => {
                return Err(Error::UnsupportedFeature(
                    "INSERT source type not supported".to_string(),
                ));
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_update(
        &mut self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        selection: Option<&Expr>,
    ) -> Result<Table> {
        let table_name = self.extract_single_table_name(table)?;
        let user_functions = self.catalog.get_functions().clone();
        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::with_user_functions(&schema, &user_functions);

        let assignment_indices: Vec<(usize, &Expr)> = assignments
            .iter()
            .map(|a| {
                let col_name = match &a.target {
                    ast::AssignmentTarget::ColumnName(obj_name) => obj_name.to_string(),
                    ast::AssignmentTarget::Tuple(_) => {
                        return Err(Error::UnsupportedFeature(
                            "Tuple assignment not supported".to_string(),
                        ));
                    }
                };
                let idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == col_name.to_uppercase())
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                Ok((idx, &a.value))
            })
            .collect::<Result<Vec<_>>>()?;

        let num_rows = table_data.row_count();
        for row_idx in 0..num_rows {
            let row = table_data.get_row(row_idx)?;
            let should_update = match selection {
                Some(sel) => evaluator.evaluate_to_bool(sel, &row)?,
                None => true,
            };

            if should_update {
                let mut values = row.values().to_vec();
                for (col_idx, expr) in &assignment_indices {
                    let new_val = evaluator.evaluate(expr, &row)?;
                    values[*col_idx] = new_val;
                }
                table_data.update_row(row_idx, values)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_delete(&mut self, delete: &ast::Delete) -> Result<Table> {
        let table_name = self.extract_delete_table_name(delete)?;
        let user_functions = self.catalog.get_functions().clone();

        let resolved_selection = match &delete.selection {
            Some(selection) => Some(self.resolve_subqueries_in_expr(selection)?),
            None => None,
        };

        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::with_user_functions(&schema, &user_functions);

        match resolved_selection {
            Some(selection) => {
                let num_rows = table_data.row_count();
                let mut indices_to_delete = Vec::new();
                for row_idx in 0..num_rows {
                    let row = table_data.get_row(row_idx)?;
                    if evaluator
                        .evaluate_to_bool(&selection, &row)
                        .unwrap_or(false)
                    {
                        indices_to_delete.push(row_idx);
                    }
                }
                for idx in indices_to_delete.into_iter().rev() {
                    table_data.remove_row(idx);
                }
            }
            None => {
                table_data.clear();
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn resolve_subqueries_in_expr(&self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let subquery_result = self.execute_query(subquery)?;
                let values = self.table_column_to_expr_list(&subquery_result)?;
                let resolved_expr = self.resolve_subqueries_in_expr(expr)?;
                Ok(Expr::InList {
                    expr: Box::new(resolved_expr),
                    list: values,
                    negated: *negated,
                })
            }
            Expr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_subqueries_in_expr(left)?;
                let resolved_right = self.resolve_subqueries_in_expr(right)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: op.clone(),
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr } => {
                let resolved_expr = self.resolve_subqueries_in_expr(expr)?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved_expr),
                })
            }
            Expr::Nested(inner) => {
                let resolved = self.resolve_subqueries_in_expr(inner)?;
                Ok(Expr::Nested(Box::new(resolved)))
            }
            _ => Ok(expr.clone()),
        }
    }

    fn table_column_to_expr_list(&self, table: &Table) -> Result<Vec<Expr>> {
        if table.schema().fields().is_empty() {
            return Ok(vec![]);
        }
        let column_name = &table.schema().fields()[0].name;
        let column = table
            .column_by_name(column_name)
            .ok_or_else(|| Error::ColumnNotFound(column_name.clone()))?;

        let mut exprs = Vec::with_capacity(table.row_count());
        for i in 0..table.row_count() {
            let value = column.get_value(i);
            let sql_value = Self::value_to_sql_value(&value);
            exprs.push(Expr::Value(ast::ValueWithSpan {
                value: sql_value,
                span: Span::empty(),
            }));
        }
        Ok(exprs)
    }

    fn execute_update_with_ctes(
        &mut self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        selection: Option<&Expr>,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        let table_name = self.extract_single_table_name(table)?;
        let user_functions = self.catalog.get_functions().clone();

        let resolved_selection = match selection {
            Some(sel) => Some(self.resolve_scalar_subqueries_with_ctes(sel, cte_tables)?),
            None => None,
        };

        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::with_user_functions(&schema, &user_functions);

        let assignment_indices: Vec<(usize, &Expr)> = assignments
            .iter()
            .map(|a| {
                let col_name = match &a.target {
                    ast::AssignmentTarget::ColumnName(obj_name) => obj_name.to_string(),
                    ast::AssignmentTarget::Tuple(_) => {
                        return Err(Error::UnsupportedFeature(
                            "Tuple assignment not supported".to_string(),
                        ));
                    }
                };
                let idx = schema
                    .fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == col_name.to_uppercase())
                    .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                Ok((idx, &a.value))
            })
            .collect::<Result<Vec<_>>>()?;

        let num_rows = table_data.row_count();
        for row_idx in 0..num_rows {
            let row = table_data.get_row(row_idx)?;
            let should_update = match &resolved_selection {
                Some(sel) => evaluator.evaluate_to_bool(sel, &row)?,
                None => true,
            };

            if should_update {
                let mut values = row.values().to_vec();
                for (col_idx, expr) in &assignment_indices {
                    let new_val = evaluator.evaluate(expr, &row)?;
                    values[*col_idx] = new_val;
                }
                table_data.update_row(row_idx, values)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_delete_with_ctes(
        &mut self,
        delete: &ast::Delete,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Table> {
        let table_name = self.extract_delete_table_name(delete)?;
        let user_functions = self.catalog.get_functions().clone();

        let resolved_selection = match &delete.selection {
            Some(sel) => Some(self.resolve_scalar_subqueries_with_ctes(sel, cte_tables)?),
            None => None,
        };

        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::with_user_functions(&schema, &user_functions);

        match &resolved_selection {
            Some(selection) => {
                let num_rows = table_data.row_count();
                let mut indices_to_delete = Vec::new();
                for row_idx in 0..num_rows {
                    let row = table_data.get_row(row_idx)?;
                    if evaluator.evaluate_to_bool(selection, &row).unwrap_or(false) {
                        indices_to_delete.push(row_idx);
                    }
                }
                for idx in indices_to_delete.into_iter().rev() {
                    table_data.remove_row(idx);
                }
            }
            None => {
                table_data.clear();
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_truncate(&mut self, table_names: &[ast::TruncateTableTarget]) -> Result<Table> {
        for target in table_names {
            let table_name = target.name.to_string();
            if let Some(table_data) = self.catalog.get_table_mut(&table_name) {
                table_data.clear();
            } else {
                return Err(Error::TableNotFound(table_name));
            }
        }
        Ok(Table::empty(Schema::new()))
    }

    fn execute_alter_table(
        &mut self,
        name: &ast::ObjectName,
        operations: &[ast::AlterTableOperation],
    ) -> Result<Table> {
        let table_name = name.to_string();
        for op in operations {
            match op {
                ast::AlterTableOperation::AddColumn { column_def, .. } => {
                    let col_name = column_def.name.value.clone();
                    let data_type = self.sql_type_to_data_type(&column_def.data_type)?;

                    let default_value = column_def.options.iter().find_map(|opt| {
                        if let ast::ColumnOption::Default(expr) = &opt.option {
                            self.evaluate_literal_expr(expr).ok()
                        } else {
                            None
                        }
                    });

                    let table_data = self
                        .catalog
                        .get_table_mut(&table_name)
                        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                    let field = Field::nullable(col_name, data_type);
                    TableSchemaOps::add_column(table_data, field, default_value)?;
                }
                ast::AlterTableOperation::DropColumn { column_names, .. } => {
                    let column_name = column_names.first().ok_or_else(|| {
                        Error::InvalidQuery("DROP COLUMN requires a column name".to_string())
                    })?;
                    let table_data = self
                        .catalog
                        .get_table_mut(&table_name)
                        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                    table_data.drop_column(&column_name.value)?;
                }
                ast::AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let table_data = self
                        .catalog
                        .get_table_mut(&table_name)
                        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                    table_data.rename_column(&old_column_name.value, &new_column_name.value)?;
                }
                ast::AlterTableOperation::RenameTable {
                    table_name: new_name,
                } => {
                    let new_table_name = match new_name {
                        ast::RenameTableNameKind::To(name) => name.to_string(),
                        ast::RenameTableNameKind::As(name) => name.to_string(),
                    };
                    self.catalog.rename_table(&table_name, &new_table_name)?;
                }
                ast::AlterTableOperation::AddConstraint { .. } => {}
                ast::AlterTableOperation::AlterColumn { column_name, op } => match op {
                    ast::AlterColumnOperation::SetNotNull => {
                        let table_data = self
                            .catalog
                            .get_table_mut(&table_name)
                            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
                        table_data.set_column_not_null(&column_name.value)?;
                    }
                    ast::AlterColumnOperation::DropNotNull => {
                        let table_data = self
                            .catalog
                            .get_table_mut(&table_name)
                            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
                        table_data.set_column_nullable(&column_name.value)?;
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(format!(
                            "ALTER COLUMN operation not supported: {:?}",
                            op
                        )));
                    }
                },
                _ => {
                    return Err(Error::UnsupportedFeature(format!(
                        "ALTER TABLE operation not supported: {:?}",
                        op
                    )));
                }
            }
        }
        Ok(Table::empty(Schema::new()))
    }

    fn extract_table_name(&self, from: &[TableWithJoins]) -> Result<String> {
        if from.is_empty() {
            return Err(Error::InvalidQuery("FROM clause is empty".to_string()));
        }

        match &from[0].relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(Error::UnsupportedFeature(
                "Only simple table references supported".to_string(),
            )),
        }
    }

    fn extract_single_table_name(&self, table: &ast::TableWithJoins) -> Result<String> {
        match &table.relation {
            TableFactor::Table { name, .. } => Ok(name.to_string()),
            _ => Err(Error::UnsupportedFeature(
                "Only simple table references supported".to_string(),
            )),
        }
    }

    fn extract_delete_table_name(&self, delete: &ast::Delete) -> Result<String> {
        let tables = match &delete.from {
            ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => {
                tables
            }
        };
        if let Some(from) = tables.first() {
            match &from.relation {
                TableFactor::Table { name, .. } => Ok(name.to_string()),
                _ => Err(Error::UnsupportedFeature(
                    "Only simple table references supported".to_string(),
                )),
            }
        } else {
            Err(Error::InvalidQuery(
                "DELETE requires FROM clause".to_string(),
            ))
        }
    }

    fn resolve_scalar_subqueries(&self, expr: &Expr) -> Result<Expr> {
        self.resolve_scalar_subqueries_with_ctes(expr, &HashMap::new())
    }

    fn resolve_scalar_subqueries_with_ctes(
        &self,
        expr: &Expr,
        cte_tables: &HashMap<String, Table>,
    ) -> Result<Expr> {
        match expr {
            Expr::Subquery(query) => {
                let mut merged_ctes = cte_tables.clone();
                let inner_ctes = self.materialize_ctes(&query.with)?;
                merged_ctes.extend(inner_ctes);
                let result = self.execute_query_with_ctes(query, &merged_ctes)?;
                let rows = result.to_records()?;
                if rows.len() != 1 || result.schema().field_count() != 1 {
                    return Err(Error::InvalidQuery(
                        "Scalar subquery must return exactly one row and one column".to_string(),
                    ));
                }
                let value = rows[0].values()[0].clone();
                Ok(self.value_to_expr(&value))
            }
            Expr::BinaryOp { left, op, right } => {
                let resolved_left = self.resolve_scalar_subqueries_with_ctes(left, cte_tables)?;
                let resolved_right = self.resolve_scalar_subqueries_with_ctes(right, cte_tables)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: op.clone(),
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let resolved = self.resolve_scalar_subqueries_with_ctes(inner, cte_tables)?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved),
                })
            }
            Expr::Nested(inner) => {
                let resolved = self.resolve_scalar_subqueries_with_ctes(inner, cte_tables)?;
                Ok(Expr::Nested(Box::new(resolved)))
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let resolved_expr = self.resolve_scalar_subqueries_with_ctes(expr, cte_tables)?;
                let resolved_low = self.resolve_scalar_subqueries_with_ctes(low, cte_tables)?;
                let resolved_high = self.resolve_scalar_subqueries_with_ctes(high, cte_tables)?;
                Ok(Expr::Between {
                    expr: Box::new(resolved_expr),
                    low: Box::new(resolved_low),
                    high: Box::new(resolved_high),
                    negated: *negated,
                })
            }
            Expr::IsNull(inner) => {
                let resolved = self.resolve_scalar_subqueries_with_ctes(inner, cte_tables)?;
                Ok(Expr::IsNull(Box::new(resolved)))
            }
            Expr::IsNotNull(inner) => {
                let resolved = self.resolve_scalar_subqueries_with_ctes(inner, cte_tables)?;
                Ok(Expr::IsNotNull(Box::new(resolved)))
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                if let ast::FunctionArguments::Subquery(q) = &func.args {
                    let mut merged_ctes = cte_tables.clone();
                    let inner_ctes = self.materialize_ctes(&q.with)?;
                    merged_ctes.extend(inner_ctes);
                    let result = self.execute_query_with_ctes(q, &merged_ctes)?;
                    let rows = result.to_records()?;
                    if func_name == "ARRAY" {
                        if result.schema().field_count() != 1 {
                            return Err(Error::InvalidQuery(
                                "ARRAY() subquery must return exactly one column".to_string(),
                            ));
                        }
                        let values: Vec<Value> =
                            rows.iter().map(|r| r.values()[0].clone()).collect();
                        let array_val = Value::array(values);
                        return Ok(self.value_to_expr(&array_val));
                    }
                    if rows.len() != 1 || result.schema().field_count() != 1 {
                        return Err(Error::InvalidQuery(
                            "Scalar subquery must return exactly one row and one column"
                                .to_string(),
                        ));
                    }
                    let value = rows[0].values()[0].clone();
                    return Ok(self.value_to_expr(&value));
                }
                let new_args = match &func.args {
                    ast::FunctionArguments::None => ast::FunctionArguments::None,
                    ast::FunctionArguments::Subquery(_) => unreachable!(),
                    ast::FunctionArguments::List(list) => {
                        let new_list_args: Vec<ast::FunctionArg> = list
                            .args
                            .iter()
                            .map(|arg| match arg {
                                ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                    ast::FunctionArgExpr::Expr(e) => {
                                        let resolved = self.resolve_scalar_subqueries(e)?;
                                        Ok(ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                            resolved,
                                        )))
                                    }
                                    other => Ok(ast::FunctionArg::Unnamed(other.clone())),
                                },
                                ast::FunctionArg::Named {
                                    name,
                                    arg,
                                    operator,
                                } => match arg {
                                    ast::FunctionArgExpr::Expr(e) => {
                                        let resolved = self.resolve_scalar_subqueries(e)?;
                                        Ok(ast::FunctionArg::Named {
                                            name: name.clone(),
                                            arg: ast::FunctionArgExpr::Expr(resolved),
                                            operator: operator.clone(),
                                        })
                                    }
                                    other => Ok(ast::FunctionArg::Named {
                                        name: name.clone(),
                                        arg: other.clone(),
                                        operator: operator.clone(),
                                    }),
                                },
                                ast::FunctionArg::ExprNamed {
                                    name,
                                    arg,
                                    operator,
                                } => match arg {
                                    ast::FunctionArgExpr::Expr(e) => {
                                        let resolved = self.resolve_scalar_subqueries(e)?;
                                        Ok(ast::FunctionArg::ExprNamed {
                                            name: name.clone(),
                                            arg: ast::FunctionArgExpr::Expr(resolved),
                                            operator: operator.clone(),
                                        })
                                    }
                                    other => Ok(ast::FunctionArg::ExprNamed {
                                        name: name.clone(),
                                        arg: other.clone(),
                                        operator: operator.clone(),
                                    }),
                                },
                            })
                            .collect::<Result<Vec<_>>>()?;
                        ast::FunctionArguments::List(ast::FunctionArgumentList {
                            duplicate_treatment: list.duplicate_treatment,
                            args: new_list_args,
                            clauses: list.clauses.clone(),
                        })
                    }
                };
                Ok(Expr::Function(ast::Function {
                    name: func.name.clone(),
                    uses_odbc_syntax: func.uses_odbc_syntax,
                    args: new_args,
                    filter: func.filter.clone(),
                    null_treatment: func.null_treatment,
                    over: func.over.clone(),
                    within_group: func.within_group.clone(),
                    parameters: func.parameters.clone(),
                }))
            }
            Expr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } => {
                let resolved_operand = operand
                    .as_ref()
                    .map(|o| self.resolve_scalar_subqueries(o))
                    .transpose()?
                    .map(Box::new);
                let resolved_conditions: Vec<ast::CaseWhen> = conditions
                    .iter()
                    .map(|cw| {
                        let resolved_cond = self.resolve_scalar_subqueries(&cw.condition)?;
                        let resolved_result = self.resolve_scalar_subqueries(&cw.result)?;
                        Ok(ast::CaseWhen {
                            condition: resolved_cond,
                            result: resolved_result,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                let resolved_else = else_result
                    .as_ref()
                    .map(|e| self.resolve_scalar_subqueries(e))
                    .transpose()?
                    .map(Box::new);
                Ok(Expr::Case {
                    case_token: case_token.clone(),
                    end_token: end_token.clone(),
                    operand: resolved_operand,
                    conditions: resolved_conditions,
                    else_result: resolved_else,
                })
            }
            Expr::Cast {
                expr: inner,
                data_type,
                format,
                kind,
            } => {
                let resolved = self.resolve_scalar_subqueries(inner)?;
                Ok(Expr::Cast {
                    expr: Box::new(resolved),
                    data_type: data_type.clone(),
                    format: format.clone(),
                    kind: kind.clone(),
                })
            }
            Expr::InList {
                expr,
                list,
                negated,
            } => {
                let resolved_expr = self.resolve_scalar_subqueries(expr)?;
                let resolved_list: Vec<Expr> = list
                    .iter()
                    .map(|e| self.resolve_scalar_subqueries(e))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::InList {
                    expr: Box::new(resolved_expr),
                    list: resolved_list,
                    negated: *negated,
                })
            }
            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => {
                let resolved_expr = self.resolve_scalar_subqueries_with_ctes(expr, cte_tables)?;
                let mut merged_ctes = cte_tables.clone();
                let inner_ctes = self.materialize_ctes(&subquery.with)?;
                merged_ctes.extend(inner_ctes);
                let result = self.execute_query_with_ctes(subquery, &merged_ctes)?;
                let rows = result.to_records()?;
                if result.schema().field_count() != 1 {
                    return Err(Error::InvalidQuery(
                        "IN subquery must return exactly one column".to_string(),
                    ));
                }
                let list: Vec<Expr> = rows
                    .iter()
                    .map(|row| self.value_to_expr(&row.values()[0]))
                    .collect();
                Ok(Expr::InList {
                    expr: Box::new(resolved_expr),
                    list,
                    negated: *negated,
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn value_to_expr(&self, value: &Value) -> Expr {
        match value {
            Value::Array(arr) => {
                let elements: Vec<Expr> = arr.iter().map(|v| self.value_to_expr(v)).collect();
                Expr::Array(ast::Array {
                    elem: elements,
                    named: false,
                })
            }
            _ => {
                let sql_value = match value {
                    Value::Null => SqlValue::Null,
                    Value::Bool(b) => SqlValue::Boolean(*b),
                    Value::Int64(i) => SqlValue::Number(i.to_string(), false),
                    Value::Float64(f) => {
                        let s = f.to_string();
                        let s = if s.contains('.') || s.contains('e') || s.contains('E') {
                            s
                        } else {
                            format!("{}.0", s)
                        };
                        SqlValue::Number(s, false)
                    }
                    Value::String(s) => SqlValue::SingleQuotedString(s.clone()),
                    Value::Numeric(n) => SqlValue::Number(n.to_string(), false),
                    _ => SqlValue::Null,
                };
                Expr::Value(sql_value.into())
            }
        }
    }

    fn expr_to_alias(&self, expr: &Expr, idx: usize) -> String {
        match expr {
            Expr::Identifier(ident) => ident.value.clone(),
            Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|i| i.value.clone())
                .unwrap_or_else(|| format!("_col{}", idx)),
            _ => format!("_col{}", idx),
        }
    }

    fn infer_expr_type(&self, expr: &Expr, schema: &Schema) -> Option<DataType> {
        match expr {
            Expr::Identifier(ident) => {
                let col_name = ident.value.to_uppercase();
                schema
                    .fields()
                    .iter()
                    .find(|f| {
                        f.name.to_uppercase() == col_name
                            || f.name.to_uppercase().ends_with(&format!(".{}", col_name))
                    })
                    .map(|f| f.data_type.clone())
            }
            Expr::CompoundIdentifier(parts) => {
                let full_name = parts
                    .iter()
                    .map(|p| p.value.clone())
                    .collect::<Vec<_>>()
                    .join(".")
                    .to_uppercase();
                let col_name = parts.last().map(|p| p.value.to_uppercase())?;
                schema
                    .fields()
                    .iter()
                    .find(|f| {
                        f.name.to_uppercase() == full_name
                            || f.name.to_uppercase() == col_name
                            || f.name.to_uppercase().ends_with(&format!(".{}", col_name))
                    })
                    .map(|f| f.data_type.clone())
            }
            _ => None,
        }
    }

    fn evaluate_literal_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Value(val) => self.sql_value_to_value(&val.value),
            Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => {
                let val = self.evaluate_literal_expr(expr)?;
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(-i));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(-f));
                }
                Err(Error::InvalidQuery(
                    "Cannot negate non-numeric value".to_string(),
                ))
            }
            Expr::Identifier(ident) if ident.value.to_uppercase() == "NULL" => Ok(Value::null()),
            Expr::Identifier(ident) => {
                if let Some(var) = self.get_variable(&ident.value) {
                    return Ok(var.value.clone());
                }
                Err(Error::UnsupportedFeature(format!(
                    "Expression not supported in this context: {:?}",
                    expr
                )))
            }
            Expr::Array(arr) => {
                let mut values = Vec::with_capacity(arr.elem.len());
                for elem in &arr.elem {
                    values.push(self.evaluate_literal_expr(elem)?);
                }
                Ok(Value::array(values))
            }
            Expr::Nested(inner) => self.evaluate_literal_expr(inner),
            Expr::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());
                for e in exprs {
                    values.push(self.evaluate_literal_expr(e)?);
                }
                Ok(Value::array(values))
            }
            Expr::TypedString(ts) => self.evaluate_typed_string_literal(&ts.data_type, &ts.value),
            Expr::Struct { values, fields: _ } => {
                let mut struct_fields = Vec::with_capacity(values.len());
                for (i, e) in values.iter().enumerate() {
                    match e {
                        Expr::Named { expr, name } => {
                            let val = self.evaluate_literal_expr(expr)?;
                            struct_fields.push((name.value.clone(), val));
                        }
                        _ => {
                            let val = self.evaluate_literal_expr(e)?;
                            struct_fields.push((format!("_field{}", i), val));
                        }
                    }
                }
                Ok(Value::struct_val(struct_fields))
            }
            Expr::Interval(interval) => {
                let val = self.evaluate_literal_expr(&interval.value)?;
                let amount = val.as_i64().unwrap_or(0);
                let unit = interval
                    .leading_field
                    .as_ref()
                    .map(|f| format!("{:?}", f).to_uppercase())
                    .unwrap_or_else(|| "SECOND".to_string());
                use yachtsql_common::types::IntervalValue;
                let interval_val = match unit.as_str() {
                    "YEAR" => IntervalValue::from_months(amount as i32 * 12),
                    "MONTH" => IntervalValue::from_months(amount as i32),
                    "DAY" => IntervalValue::from_days(amount as i32),
                    "HOUR" => IntervalValue::from_hours(amount),
                    "MINUTE" => IntervalValue::new(0, 0, amount * IntervalValue::MICROS_PER_MINUTE),
                    "SECOND" => IntervalValue::new(0, 0, amount * IntervalValue::MICROS_PER_SECOND),
                    _ => IntervalValue::new(0, amount as i32, 0),
                };
                Ok(Value::interval(interval_val))
            }
            Expr::Function(_) => {
                let schema = Schema::default();
                let record = Record::from_values(vec![]);
                let evaluator = Evaluator::new(&schema);
                evaluator.evaluate(expr, &record)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_literal_expr(left)?;
                let right_val = self.evaluate_literal_expr(right)?;
                self.evaluate_binary_op_values(&left_val, op, &right_val)
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Expression not supported in this context: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_binary_op_values(
        &self,
        left: &Value,
        op: &ast::BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        match op {
            ast::BinaryOperator::Plus => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    return Ok(Value::int64(l + r));
                }
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    return Ok(Value::float64(l + r));
                }
                Err(Error::InvalidQuery(format!(
                    "Cannot add {:?} and {:?}",
                    left, right
                )))
            }
            ast::BinaryOperator::Minus => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    return Ok(Value::int64(l - r));
                }
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    return Ok(Value::float64(l - r));
                }
                Err(Error::InvalidQuery(format!(
                    "Cannot subtract {:?} and {:?}",
                    left, right
                )))
            }
            ast::BinaryOperator::Multiply => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    return Ok(Value::int64(l * r));
                }
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    return Ok(Value::float64(l * r));
                }
                Err(Error::InvalidQuery(format!(
                    "Cannot multiply {:?} and {:?}",
                    left, right
                )))
            }
            ast::BinaryOperator::Divide => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    if r == 0 {
                        return Err(Error::InvalidQuery("Division by zero".to_string()));
                    }
                    return Ok(Value::int64(l / r));
                }
                if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
                    return Ok(Value::float64(l / r));
                }
                Err(Error::InvalidQuery(format!(
                    "Cannot divide {:?} and {:?}",
                    left, right
                )))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Binary operator {:?} not supported in literal expressions",
                op
            ))),
        }
    }

    fn evaluate_typed_string_literal(
        &self,
        data_type: &ast::DataType,
        value: &ast::ValueWithSpan,
    ) -> Result<Value> {
        let s = match &value.value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.as_str(),
            _ => {
                return Err(Error::InvalidQuery(
                    "TypedString value must be a string".to_string(),
                ));
            }
        };
        match data_type {
            ast::DataType::Date => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Ok(Value::date(date))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            ast::DataType::Time(_, _) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    Ok(Value::time(time))
                } else if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                    Ok(Value::time(time))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            ast::DataType::Timestamp(_, _) | ast::DataType::Datetime(_) => {
                if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(Value::timestamp(ts.with_timezone(&chrono::Utc)))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                {
                    Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    ))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                {
                    Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    ))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            ast::DataType::JSON => {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(s) {
                    Ok(Value::json(json_val))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            ast::DataType::Bytes(_) => Ok(Value::bytes(s.as_bytes().to_vec())),
            ast::DataType::Numeric(_)
            | ast::DataType::Decimal(_)
            | ast::DataType::BigNumeric(_) => {
                if let Ok(d) = s.parse::<rust_decimal::Decimal>() {
                    Ok(Value::numeric(d))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Invalid NUMERIC literal: {}",
                        s
                    )))
                }
            }
            _ => Ok(Value::string(s.to_string())),
        }
    }

    fn sql_value_to_value(&self, val: &SqlValue) -> Result<Value> {
        match val {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::float64(f))
                } else {
                    Err(Error::ParseError(format!("Invalid number: {}", n)))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::string(s.clone()))
            }
            SqlValue::SingleQuotedByteStringLiteral(s)
            | SqlValue::DoubleQuotedByteStringLiteral(s) => {
                Ok(Value::bytes(parse_byte_string_escapes(s)))
            }
            SqlValue::HexStringLiteral(s) => {
                let bytes = hex::decode(s).unwrap_or_default();
                Ok(Value::bytes(bytes))
            }
            SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
            SqlValue::Null => Ok(Value::null()),
            _ => Err(Error::UnsupportedFeature(format!(
                "SQL value type not yet supported: {:?}",
                val
            ))),
        }
    }

    fn coerce_value_to_type(&self, value: Value, target_type: &DataType) -> Result<Value> {
        if value.is_null() {
            return Ok(value);
        }

        if value.data_type() == *target_type {
            return Ok(value);
        }

        match target_type {
            DataType::Date => {
                if let Some(s) = value.as_str() {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(Value::date(date));
                    }
                }
                Ok(value)
            }
            DataType::Timestamp => {
                if let Some(s) = value.as_str() {
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
                        return Ok(Value::timestamp(ts.with_timezone(&chrono::Utc)));
                    }
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        return Ok(Value::timestamp(dt.and_utc()));
                    }
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                        return Ok(Value::timestamp(dt.and_utc()));
                    }
                }
                Ok(value)
            }
            DataType::Time => {
                if let Some(s) = value.as_str() {
                    if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                        return Ok(Value::time(time));
                    }
                }
                Ok(value)
            }
            DataType::Int64 => {
                if let Some(s) = value.as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(Value::int64(i));
                    }
                }
                if let Some(f) = value.as_f64() {
                    return Ok(Value::int64(f as i64));
                }
                Ok(value)
            }
            DataType::Float64 => {
                if let Some(s) = value.as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                if let Some(i) = value.as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                Ok(value)
            }
            DataType::Bool => {
                if let Some(s) = value.as_str() {
                    let lower = s.to_lowercase();
                    if lower == "true" {
                        return Ok(Value::bool_val(true));
                    }
                    if lower == "false" {
                        return Ok(Value::bool_val(false));
                    }
                }
                Ok(value)
            }
            DataType::Numeric(_) => {
                if let Some(s) = value.as_str() {
                    if let Ok(d) = s.parse::<rust_decimal::Decimal>() {
                        return Ok(Value::numeric(d));
                    }
                }
                if let Some(i) = value.as_i64() {
                    return Ok(Value::numeric(rust_decimal::Decimal::from(i)));
                }
                if let Some(f) = value.as_f64() {
                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                        return Ok(Value::numeric(d));
                    }
                }
                Ok(value)
            }
            DataType::Bytes => {
                if let Some(s) = value.as_str() {
                    return Ok(Value::bytes(s.as_bytes().to_vec()));
                }
                Ok(value)
            }
            DataType::Struct(target_fields) => {
                if let Some(struct_vals) = value.as_struct() {
                    if struct_vals.len() == target_fields.len() {
                        let coerced_fields: Result<Vec<(String, Value)>> = struct_vals
                            .iter()
                            .zip(target_fields.iter())
                            .map(|((_, val), target_field)| {
                                let coerced_val = self
                                    .coerce_value_to_type(val.clone(), &target_field.data_type)?;
                                Ok((target_field.name.clone(), coerced_val))
                            })
                            .collect();
                        return Ok(Value::struct_val(coerced_fields?));
                    }
                }
                Ok(value)
            }
            DataType::Array(element_type) => {
                if let Some(arr) = value.as_array() {
                    let coerced_elements: Vec<Value> = arr
                        .iter()
                        .map(|elem| self.coerce_value_to_type(elem.clone(), element_type))
                        .collect::<Result<Vec<_>>>()?;
                    return Ok(Value::array(coerced_elements));
                }
                Ok(value)
            }
            _ => Ok(value),
        }
    }

    fn sql_type_to_data_type(&self, sql_type: &ast::DataType) -> Result<DataType> {
        match sql_type {
            ast::DataType::Int64 | ast::DataType::BigInt(_) | ast::DataType::Integer(_) => {
                Ok(DataType::Int64)
            }
            ast::DataType::Float64 | ast::DataType::Double(_) | ast::DataType::DoublePrecision => {
                Ok(DataType::Float64)
            }
            ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Bool),
            ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                Ok(DataType::String)
            }
            ast::DataType::Bytes(_) | ast::DataType::Binary(_) | ast::DataType::Bytea => {
                Ok(DataType::Bytes)
            }
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Time(_, _) => Ok(DataType::Time),
            ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            ast::DataType::Datetime(_) => Ok(DataType::Timestamp),
            ast::DataType::Numeric(_) | ast::DataType::Decimal(_) => Ok(DataType::Numeric(None)),
            ast::DataType::BigNumeric(_) => Ok(DataType::BigNumeric),
            ast::DataType::JSON => Ok(DataType::Json),
            ast::DataType::Array(inner) => {
                let element_type = match inner {
                    ast::ArrayElemTypeDef::None => DataType::Unknown,
                    ast::ArrayElemTypeDef::AngleBracket(dt)
                    | ast::ArrayElemTypeDef::SquareBracket(dt, _)
                    | ast::ArrayElemTypeDef::Parenthesis(dt) => self.sql_type_to_data_type(dt)?,
                };
                Ok(DataType::Array(Box::new(element_type)))
            }
            ast::DataType::Struct(fields, _) => {
                let struct_fields: Vec<StructField> = fields
                    .iter()
                    .map(|f| {
                        let dt = self.sql_type_to_data_type(&f.field_type)?;
                        let name = f
                            .field_name
                            .as_ref()
                            .map(|n| n.value.clone())
                            .unwrap_or_default();
                        Ok(StructField {
                            name,
                            data_type: dt,
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(DataType::Struct(struct_fields))
            }
            ast::DataType::Interval { .. } => Ok(DataType::Interval),
            ast::DataType::Custom(name, _) => {
                let type_name = name.to_string().to_uppercase();
                match type_name.as_str() {
                    "GEOGRAPHY" => Ok(DataType::Geography),
                    "RANGE_DATE" => Ok(DataType::Range(Box::new(DataType::Date))),
                    "RANGE_DATETIME" => Ok(DataType::Range(Box::new(DataType::DateTime))),
                    "RANGE_TIMESTAMP" => Ok(DataType::Range(Box::new(DataType::Timestamp))),
                    _ => Err(Error::UnsupportedFeature(format!(
                        "Data type not yet supported: {:?}",
                        sql_type
                    ))),
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Data type not yet supported: {:?}",
                sql_type
            ))),
        }
    }

    fn has_window_functions(&self, projection: &[SelectItem]) -> bool {
        for item in projection {
            match item {
                SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                    if self.expr_has_window_function(expr) {
                        return true;
                    }
                }
                _ => {}
            }
        }
        false
    }

    fn expr_has_window_function(&self, expr: &Expr) -> bool {
        match expr {
            Expr::Function(func) => func.over.is_some(),
            Expr::BinaryOp { left, right, .. } => {
                self.expr_has_window_function(left) || self.expr_has_window_function(right)
            }
            Expr::UnaryOp { expr, .. } => self.expr_has_window_function(expr),
            Expr::Nested(inner) => self.expr_has_window_function(inner),
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                conditions.iter().any(|c| {
                    self.expr_has_window_function(&c.condition)
                        || self.expr_has_window_function(&c.result)
                }) || else_result
                    .as_ref()
                    .is_some_and(|e| self.expr_has_window_function(e))
            }
            _ => false,
        }
    }

    fn compute_window_functions(
        &self,
        schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
    ) -> Result<HashMap<String, Vec<Value>>> {
        self.compute_window_functions_with_qualify(schema, rows, projection, None)
    }

    fn compute_window_functions_with_qualify(
        &self,
        schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
        qualify: Option<&Expr>,
    ) -> Result<HashMap<String, Vec<Value>>> {
        let mut window_results: HashMap<String, Vec<Value>> = HashMap::new();
        let evaluator = Evaluator::with_user_functions(schema, self.catalog.get_functions());

        for item in projection {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) => expr,
                SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };

            self.collect_window_function_results(
                expr,
                schema,
                rows,
                &evaluator,
                &mut window_results,
            )?;
        }

        if let Some(qualify_expr) = qualify {
            self.collect_window_function_results(
                qualify_expr,
                schema,
                rows,
                &evaluator,
                &mut window_results,
            )?;
        }

        Ok(window_results)
    }

    fn compute_window_functions_with_aggregates(
        &self,
        schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<HashMap<String, Vec<Value>>> {
        let mut window_results: HashMap<String, Vec<Value>> = HashMap::new();
        let evaluator = Evaluator::with_user_functions(schema, self.catalog.get_functions());

        for item in projection {
            let expr = match item {
                SelectItem::UnnamedExpr(expr) => expr,
                SelectItem::ExprWithAlias { expr, .. } => expr,
                _ => continue,
            };

            self.collect_window_function_results_with_aggregates(
                expr,
                schema,
                rows,
                &evaluator,
                &mut window_results,
                expr_to_col,
            )?;
        }

        Ok(window_results)
    }

    fn collect_window_function_results_with_aggregates(
        &self,
        expr: &Expr,
        schema: &Schema,
        rows: &[Record],
        evaluator: &Evaluator,
        window_results: &mut HashMap<String, Vec<Value>>,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<()> {
        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let key = format!("{:?}", func);
                if window_results.contains_key(&key) {
                    return Ok(());
                }

                let results = self.compute_single_window_function_with_aggregates(
                    func,
                    schema,
                    rows,
                    evaluator,
                    expr_to_col,
                )?;
                window_results.insert(key, results);
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_window_function_results_with_aggregates(
                    left,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                    expr_to_col,
                )?;
                self.collect_window_function_results_with_aggregates(
                    right,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                    expr_to_col,
                )?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_window_function_results_with_aggregates(
                    expr,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                    expr_to_col,
                )?;
            }
            Expr::Nested(inner) => {
                self.collect_window_function_results_with_aggregates(
                    inner,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                    expr_to_col,
                )?;
            }
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                for cond in conditions {
                    self.collect_window_function_results_with_aggregates(
                        &cond.condition,
                        schema,
                        rows,
                        evaluator,
                        window_results,
                        expr_to_col,
                    )?;
                    self.collect_window_function_results_with_aggregates(
                        &cond.result,
                        schema,
                        rows,
                        evaluator,
                        window_results,
                        expr_to_col,
                    )?;
                }
                if let Some(e) = else_result {
                    self.collect_window_function_results_with_aggregates(
                        e,
                        schema,
                        rows,
                        evaluator,
                        window_results,
                        expr_to_col,
                    )?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn compute_single_window_function_with_aggregates(
        &self,
        func: &ast::Function,
        schema: &Schema,
        rows: &[Record],
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<Vec<Value>> {
        let name = func.name.to_string().to_uppercase();
        let over = func.over.as_ref().unwrap();

        let (partition_by, order_by_exprs) = match over {
            ast::WindowType::WindowSpec(spec) => {
                let partition = spec.partition_by.clone();
                let order = spec.order_by.clone();
                (partition, order)
            }
            ast::WindowType::NamedWindow(_) => (vec![], vec![]),
        };

        let partitions = self.partition_rows_with_aggregates(
            schema,
            rows,
            &partition_by,
            evaluator,
            expr_to_col,
        )?;

        let mut results = vec![Value::null(); rows.len()];

        for partition_indices in partitions.values() {
            let sorted_indices = self.sort_partition_indices_with_aggregates(
                schema,
                rows,
                partition_indices,
                &order_by_exprs,
                evaluator,
                expr_to_col,
            )?;

            let partition_results = self.compute_window_for_partition_with_aggregates(
                &name,
                func,
                schema,
                rows,
                &sorted_indices,
                evaluator,
                expr_to_col,
            )?;

            for (local_idx, &global_idx) in sorted_indices.iter().enumerate() {
                results[global_idx] = partition_results[local_idx].clone();
            }
        }

        Ok(results)
    }

    fn partition_rows_with_aggregates(
        &self,
        schema: &Schema,
        rows: &[Record],
        partition_by: &[Expr],
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<HashMap<String, Vec<usize>>> {
        let mut partitions: HashMap<String, Vec<usize>> = HashMap::new();

        if partition_by.is_empty() {
            partitions.insert("__all__".to_string(), (0..rows.len()).collect());
        } else {
            for (idx, row) in rows.iter().enumerate() {
                let mut key_parts = Vec::new();
                for expr in partition_by {
                    let val =
                        self.evaluate_expr_with_aggregate_lookup(expr, row, evaluator, expr_to_col);
                    key_parts.push(format!("{:?}", val));
                }
                let key = key_parts.join("|");
                partitions.entry(key).or_default().push(idx);
            }
        }

        Ok(partitions)
    }

    fn sort_partition_indices_with_aggregates(
        &self,
        schema: &Schema,
        rows: &[Record],
        indices: &[usize],
        order_by: &[ast::OrderByExpr],
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<Vec<usize>> {
        if order_by.is_empty() {
            return Ok(indices.to_vec());
        }

        let mut sorted_indices = indices.to_vec();
        sorted_indices.sort_by(|&a, &b| {
            for ob in order_by {
                let a_val = self.evaluate_expr_with_aggregate_lookup(
                    &ob.expr,
                    &rows[a],
                    evaluator,
                    expr_to_col,
                );
                let b_val = self.evaluate_expr_with_aggregate_lookup(
                    &ob.expr,
                    &rows[b],
                    evaluator,
                    expr_to_col,
                );
                let ordering = self.compare_values_for_ordering(&a_val, &b_val);
                let ordering = if ob.options.asc.unwrap_or(true) {
                    ordering
                } else {
                    ordering.reverse()
                };
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(sorted_indices)
    }

    fn evaluate_expr_with_aggregate_lookup(
        &self,
        expr: &Expr,
        row: &Record,
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Value {
        let key = self.normalize_expr_key(expr);
        if let Some(&col_idx) = expr_to_col.get(&key) {
            if let Some(val) = row.values().get(col_idx) {
                return val.clone();
            }
        }
        match evaluator.evaluate(expr, row) {
            Ok(val) => val,
            Err(_) => Value::null(),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn compute_window_for_partition_with_aggregates(
        &self,
        name: &str,
        func: &ast::Function,
        schema: &Schema,
        rows: &[Record],
        sorted_indices: &[usize],
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<Vec<Value>> {
        let partition_size = sorted_indices.len();
        let mut results = Vec::with_capacity(partition_size);

        match name {
            "ROW_NUMBER" => {
                for i in 0..partition_size {
                    results.push(Value::int64((i + 1) as i64));
                }
            }
            "RANK" => {
                let over = func.over.as_ref().unwrap();
                let order_by = match over {
                    ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                    _ => vec![],
                };

                if order_by.is_empty() {
                    for _ in 0..partition_size {
                        results.push(Value::int64(1));
                    }
                } else {
                    let mut rank = 1i64;
                    let mut prev_values: Option<Vec<Value>> = None;
                    for (i, &idx) in sorted_indices.iter().enumerate() {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                self.evaluate_expr_with_aggregate_lookup(
                                    &ob.expr,
                                    &rows[idx],
                                    evaluator,
                                    expr_to_col,
                                )
                            })
                            .collect();

                        if let Some(prev) = &prev_values {
                            if curr_values != *prev {
                                rank = (i + 1) as i64;
                            }
                        }
                        results.push(Value::int64(rank));
                        prev_values = Some(curr_values);
                    }
                }
            }
            "DENSE_RANK" => {
                let over = func.over.as_ref().unwrap();
                let order_by = match over {
                    ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                    _ => vec![],
                };

                if order_by.is_empty() {
                    for _ in 0..partition_size {
                        results.push(Value::int64(1));
                    }
                } else {
                    let mut rank = 1i64;
                    let mut prev_values: Option<Vec<Value>> = None;
                    for &idx in sorted_indices {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                self.evaluate_expr_with_aggregate_lookup(
                                    &ob.expr,
                                    &rows[idx],
                                    evaluator,
                                    expr_to_col,
                                )
                            })
                            .collect();

                        if let Some(prev) = &prev_values {
                            if curr_values != *prev {
                                rank += 1;
                            }
                        }
                        results.push(Value::int64(rank));
                        prev_values = Some(curr_values);
                    }
                }
            }
            "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" => {
                let over = func.over.as_ref().unwrap();
                let (has_order_by, frame) = match over {
                    ast::WindowType::WindowSpec(spec) => {
                        (!spec.order_by.is_empty(), spec.window_frame.as_ref())
                    }
                    _ => (false, None),
                };

                if let Some(frame) = frame {
                    for curr_pos in 0..partition_size {
                        let start_idx =
                            self.compute_frame_start(&frame.start_bound, curr_pos, partition_size);
                        let end_idx = self.compute_frame_end(
                            frame.end_bound.as_ref(),
                            curr_pos,
                            partition_size,
                        );
                        let frame_indices: Vec<usize> = if start_idx <= end_idx {
                            sorted_indices[start_idx..=end_idx].to_vec()
                        } else {
                            vec![]
                        };
                        let agg_result = self.compute_window_aggregate_with_lookup(
                            name,
                            func,
                            rows,
                            &frame_indices,
                            evaluator,
                            expr_to_col,
                        )?;
                        results.push(agg_result);
                    }
                } else if has_order_by {
                    for end_pos in 0..partition_size {
                        let running_indices: Vec<usize> = sorted_indices[..=end_pos].to_vec();
                        let agg_result = self.compute_window_aggregate_with_lookup(
                            name,
                            func,
                            rows,
                            &running_indices,
                            evaluator,
                            expr_to_col,
                        )?;
                        results.push(agg_result);
                    }
                } else {
                    let agg_result = self.compute_window_aggregate_with_lookup(
                        name,
                        func,
                        rows,
                        sorted_indices,
                        evaluator,
                        expr_to_col,
                    )?;
                    for _ in 0..partition_size {
                        results.push(agg_result.clone());
                    }
                }
            }
            _ => {
                for _ in 0..partition_size {
                    results.push(Value::null());
                }
            }
        }

        Ok(results)
    }

    fn compute_window_aggregate_with_lookup(
        &self,
        name: &str,
        func: &ast::Function,
        rows: &[Record],
        sorted_indices: &[usize],
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<Value> {
        match name {
            "COUNT" => {
                let mut count = 0i64;
                for &idx in sorted_indices {
                    let val = self.extract_window_arg_with_lookup(
                        func,
                        &rows[idx],
                        evaluator,
                        expr_to_col,
                    )?;
                    if !val.is_null() {
                        count += 1;
                    }
                }
                Ok(Value::int64(count))
            }
            "SUM" => {
                let mut sum_int: Option<i64> = None;
                let mut sum_float: Option<f64> = None;
                for &idx in sorted_indices {
                    let val = self.extract_window_arg_with_lookup(
                        func,
                        &rows[idx],
                        evaluator,
                        expr_to_col,
                    )?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        sum_int = Some(sum_int.unwrap_or(0) + i);
                    } else if let Some(f) = val.as_f64() {
                        sum_float = Some(sum_float.unwrap_or(0.0) + f);
                    }
                }
                if let Some(f) = sum_float {
                    Ok(Value::float64(f + sum_int.unwrap_or(0) as f64))
                } else if let Some(i) = sum_int {
                    Ok(Value::int64(i))
                } else {
                    Ok(Value::null())
                }
            }
            "AVG" => {
                let mut sum = 0.0f64;
                let mut count = 0i64;
                for &idx in sorted_indices {
                    let val = self.extract_window_arg_with_lookup(
                        func,
                        &rows[idx],
                        evaluator,
                        expr_to_col,
                    )?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        sum += i as f64;
                        count += 1;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                        count += 1;
                    }
                }
                if count > 0 {
                    Ok(Value::float64(sum / count as f64))
                } else {
                    Ok(Value::null())
                }
            }
            "MIN" => {
                let mut min: Option<Value> = None;
                for &idx in sorted_indices {
                    let val = self.extract_window_arg_with_lookup(
                        func,
                        &rows[idx],
                        evaluator,
                        expr_to_col,
                    )?;
                    if val.is_null() {
                        continue;
                    }
                    match &min {
                        None => min = Some(val),
                        Some(m) => {
                            if self.compare_values_for_ordering(&val, m) == std::cmp::Ordering::Less
                            {
                                min = Some(val);
                            }
                        }
                    }
                }
                Ok(min.unwrap_or(Value::null()))
            }
            "MAX" => {
                let mut max: Option<Value> = None;
                for &idx in sorted_indices {
                    let val = self.extract_window_arg_with_lookup(
                        func,
                        &rows[idx],
                        evaluator,
                        expr_to_col,
                    )?;
                    if val.is_null() {
                        continue;
                    }
                    match &max {
                        None => max = Some(val),
                        Some(m) => {
                            if self.compare_values_for_ordering(&val, m)
                                == std::cmp::Ordering::Greater
                            {
                                max = Some(val);
                            }
                        }
                    }
                }
                Ok(max.unwrap_or(Value::null()))
            }
            _ => Ok(Value::null()),
        }
    }

    fn extract_window_arg_with_lookup(
        &self,
        func: &ast::Function,
        row: &Record,
        evaluator: &Evaluator,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<Value> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.first() {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    let key = self.normalize_expr_key(expr);
                    if let Some(&col_idx) = expr_to_col.get(&key) {
                        if let Some(val) = row.values().get(col_idx) {
                            return Ok(val.clone());
                        }
                    }
                    return evaluator.evaluate(expr, row);
                }
            }
        }
        Ok(Value::null())
    }

    fn collect_window_function_results(
        &self,
        expr: &Expr,
        schema: &Schema,
        rows: &[Record],
        evaluator: &Evaluator,
        window_results: &mut HashMap<String, Vec<Value>>,
    ) -> Result<()> {
        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let key = format!("{:?}", func);
                if window_results.contains_key(&key) {
                    return Ok(());
                }

                let results = self.compute_single_window_function(func, schema, rows, evaluator)?;
                window_results.insert(key, results);
            }
            Expr::BinaryOp { left, right, .. } => {
                self.collect_window_function_results(
                    left,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                )?;
                self.collect_window_function_results(
                    right,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                )?;
            }
            Expr::UnaryOp { expr, .. } => {
                self.collect_window_function_results(
                    expr,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                )?;
            }
            Expr::Nested(inner) => {
                self.collect_window_function_results(
                    inner,
                    schema,
                    rows,
                    evaluator,
                    window_results,
                )?;
            }
            Expr::Case {
                conditions,
                else_result,
                ..
            } => {
                for cond in conditions {
                    self.collect_window_function_results(
                        &cond.condition,
                        schema,
                        rows,
                        evaluator,
                        window_results,
                    )?;
                    self.collect_window_function_results(
                        &cond.result,
                        schema,
                        rows,
                        evaluator,
                        window_results,
                    )?;
                }
                if let Some(e) = else_result {
                    self.collect_window_function_results(
                        e,
                        schema,
                        rows,
                        evaluator,
                        window_results,
                    )?;
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn compute_single_window_function(
        &self,
        func: &ast::Function,
        schema: &Schema,
        rows: &[Record],
        evaluator: &Evaluator,
    ) -> Result<Vec<Value>> {
        let name = func.name.to_string().to_uppercase();
        let over = func.over.as_ref().unwrap();

        let (partition_by, order_by_exprs) = match over {
            ast::WindowType::WindowSpec(spec) => {
                let partition = spec.partition_by.clone();
                let order = spec.order_by.clone();
                (partition, order)
            }
            ast::WindowType::NamedWindow(_) => (vec![], vec![]),
        };

        let partitions = self.partition_rows(schema, rows, &partition_by, evaluator)?;

        let mut results = vec![Value::null(); rows.len()];

        for (partition_key, partition_indices) in &partitions {
            let sorted_indices = self.sort_partition_indices(
                schema,
                rows,
                partition_indices,
                &order_by_exprs,
                evaluator,
            )?;

            let partition_results = self.compute_window_for_partition(
                &name,
                func,
                schema,
                rows,
                &sorted_indices,
                evaluator,
            )?;

            for (local_idx, &global_idx) in sorted_indices.iter().enumerate() {
                results[global_idx] = partition_results[local_idx].clone();
            }
        }

        Ok(results)
    }

    fn partition_rows(
        &self,
        schema: &Schema,
        rows: &[Record],
        partition_by: &[Expr],
        evaluator: &Evaluator,
    ) -> Result<HashMap<String, Vec<usize>>> {
        let mut partitions: HashMap<String, Vec<usize>> = HashMap::new();

        if partition_by.is_empty() {
            partitions.insert("__all__".to_string(), (0..rows.len()).collect());
        } else {
            for (idx, row) in rows.iter().enumerate() {
                let mut key_parts = Vec::new();
                for expr in partition_by {
                    let val = evaluator.evaluate(expr, row)?;
                    key_parts.push(format!("{:?}", val));
                }
                let key = key_parts.join("|");
                partitions.entry(key).or_default().push(idx);
            }
        }

        Ok(partitions)
    }

    fn sort_partition_indices(
        &self,
        schema: &Schema,
        rows: &[Record],
        indices: &[usize],
        order_by: &[ast::OrderByExpr],
        evaluator: &Evaluator,
    ) -> Result<Vec<usize>> {
        if order_by.is_empty() {
            return Ok(indices.to_vec());
        }

        let mut sorted_indices = indices.to_vec();
        sorted_indices.sort_by(|&a, &b| {
            for ob in order_by {
                let a_val = self.evaluate_window_order_expr(&ob.expr, schema, &rows[a], evaluator);
                let b_val = self.evaluate_window_order_expr(&ob.expr, schema, &rows[b], evaluator);
                let ordering = self.compare_values_for_ordering(&a_val, &b_val);
                let ordering = if ob.options.asc.unwrap_or(true) {
                    ordering
                } else {
                    ordering.reverse()
                };
                if ordering != std::cmp::Ordering::Equal {
                    return ordering;
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(sorted_indices)
    }

    fn evaluate_window_order_expr(
        &self,
        expr: &Expr,
        schema: &Schema,
        row: &Record,
        evaluator: &Evaluator,
    ) -> Value {
        match evaluator.evaluate(expr, row) {
            Ok(val) => val,
            Err(_) => {
                for (idx, field) in schema.fields().iter().enumerate() {
                    if field.data_type == DataType::Date
                        || field.name.to_uppercase().contains("MONTH")
                        || field.name.to_uppercase().contains("DATE")
                    {
                        if let Some(val) = row.values().get(idx) {
                            return val.clone();
                        }
                    }
                }
                for (idx, val) in row.values().iter().enumerate() {
                    if val.as_date().is_some() {
                        return val.clone();
                    }
                }
                Value::null()
            }
        }
    }

    fn compute_window_for_partition(
        &self,
        name: &str,
        func: &ast::Function,
        schema: &Schema,
        rows: &[Record],
        sorted_indices: &[usize],
        evaluator: &Evaluator,
    ) -> Result<Vec<Value>> {
        let partition_size = sorted_indices.len();
        let mut results = Vec::with_capacity(partition_size);

        match name {
            "ROW_NUMBER" => {
                for i in 0..partition_size {
                    results.push(Value::int64((i + 1) as i64));
                }
            }
            "RANK" => {
                let over = func.over.as_ref().unwrap();
                let order_by = match over {
                    ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                    _ => vec![],
                };

                if order_by.is_empty() {
                    for _ in 0..partition_size {
                        results.push(Value::int64(1));
                    }
                } else {
                    let mut rank = 1i64;
                    let mut prev_values: Option<Vec<Value>> = None;
                    for (i, &idx) in sorted_indices.iter().enumerate() {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                evaluator
                                    .evaluate(&ob.expr, &rows[idx])
                                    .unwrap_or(Value::null())
                            })
                            .collect();

                        if let Some(prev) = &prev_values {
                            if curr_values != *prev {
                                rank = (i + 1) as i64;
                            }
                        }
                        results.push(Value::int64(rank));
                        prev_values = Some(curr_values);
                    }
                }
            }
            "DENSE_RANK" => {
                let over = func.over.as_ref().unwrap();
                let order_by = match over {
                    ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                    _ => vec![],
                };

                if order_by.is_empty() {
                    for _ in 0..partition_size {
                        results.push(Value::int64(1));
                    }
                } else {
                    let mut rank = 1i64;
                    let mut prev_values: Option<Vec<Value>> = None;
                    for &idx in sorted_indices {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                evaluator
                                    .evaluate(&ob.expr, &rows[idx])
                                    .unwrap_or(Value::null())
                            })
                            .collect();

                        if let Some(prev) = &prev_values {
                            if curr_values != *prev {
                                rank += 1;
                            }
                        }
                        results.push(Value::int64(rank));
                        prev_values = Some(curr_values);
                    }
                }
            }
            "NTILE" => {
                let n = self
                    .extract_first_window_arg(func, evaluator, &rows[sorted_indices[0]])?
                    .as_i64()
                    .unwrap_or(1) as usize;
                let n = n.max(1);
                for i in 0..partition_size {
                    let bucket = (i * n / partition_size) + 1;
                    results.push(Value::int64(bucket as i64));
                }
            }
            "LAG" => {
                let offset = if let Some(arg) =
                    self.extract_nth_window_arg(func, 1, evaluator, &rows[sorted_indices[0]])?
                {
                    arg.as_i64().unwrap_or(1) as usize
                } else {
                    1
                };
                let default = self
                    .extract_nth_window_arg(func, 2, evaluator, &rows[sorted_indices[0]])?
                    .unwrap_or(Value::null());

                for i in 0..partition_size {
                    let idx = sorted_indices[i];
                    if i >= offset {
                        let lag_idx = sorted_indices[i - offset];
                        let val = self.extract_first_window_arg(func, evaluator, &rows[lag_idx])?;
                        results.push(val);
                    } else {
                        results.push(default.clone());
                    }
                }
            }
            "LEAD" => {
                let offset = if let Some(arg) =
                    self.extract_nth_window_arg(func, 1, evaluator, &rows[sorted_indices[0]])?
                {
                    arg.as_i64().unwrap_or(1) as usize
                } else {
                    1
                };
                let default = self
                    .extract_nth_window_arg(func, 2, evaluator, &rows[sorted_indices[0]])?
                    .unwrap_or(Value::null());

                for i in 0..partition_size {
                    if i + offset < partition_size {
                        let lead_idx = sorted_indices[i + offset];
                        let val =
                            self.extract_first_window_arg(func, evaluator, &rows[lead_idx])?;
                        results.push(val);
                    } else {
                        results.push(default.clone());
                    }
                }
            }
            "FIRST_VALUE" => {
                let first_idx = sorted_indices[0];
                let first_val = self.extract_first_window_arg(func, evaluator, &rows[first_idx])?;
                for _ in 0..partition_size {
                    results.push(first_val.clone());
                }
            }
            "LAST_VALUE" => {
                let last_idx = sorted_indices[partition_size - 1];
                let last_val = self.extract_first_window_arg(func, evaluator, &rows[last_idx])?;
                for _ in 0..partition_size {
                    results.push(last_val.clone());
                }
            }
            "NTH_VALUE" => {
                let n = if let Some(arg) =
                    self.extract_nth_window_arg(func, 1, evaluator, &rows[sorted_indices[0]])?
                {
                    arg.as_i64().unwrap_or(1) as usize
                } else {
                    1
                };
                let nth_val = if n > 0 && n <= partition_size {
                    let nth_idx = sorted_indices[n - 1];
                    self.extract_first_window_arg(func, evaluator, &rows[nth_idx])?
                } else {
                    Value::null()
                };
                for _ in 0..partition_size {
                    results.push(nth_val.clone());
                }
            }
            "SUM" | "AVG" | "COUNT" | "MIN" | "MAX" => {
                let over = func.over.as_ref().unwrap();
                let (has_order_by, frame) = match over {
                    ast::WindowType::WindowSpec(spec) => {
                        (!spec.order_by.is_empty(), spec.window_frame.as_ref())
                    }
                    _ => (false, None),
                };

                if let Some(frame) = frame {
                    for curr_pos in 0..partition_size {
                        let start_idx =
                            self.compute_frame_start(&frame.start_bound, curr_pos, partition_size);
                        let end_idx = self.compute_frame_end(
                            frame.end_bound.as_ref(),
                            curr_pos,
                            partition_size,
                        );
                        let frame_indices: Vec<usize> = if start_idx <= end_idx {
                            sorted_indices[start_idx..=end_idx].to_vec()
                        } else {
                            vec![]
                        };
                        let agg_result = self.compute_window_aggregate(
                            name,
                            func,
                            rows,
                            &frame_indices,
                            evaluator,
                        )?;
                        results.push(agg_result);
                    }
                } else if has_order_by {
                    let order_by = match over {
                        ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                        _ => vec![],
                    };

                    let mut peer_groups: Vec<(usize, usize)> = Vec::new();
                    let mut group_start = 0;
                    let mut prev_values: Option<Vec<Value>> = None;

                    for (i, &idx) in sorted_indices.iter().enumerate() {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                evaluator
                                    .evaluate(&ob.expr, &rows[idx])
                                    .unwrap_or(Value::null())
                            })
                            .collect();

                        if let Some(prev) = &prev_values {
                            if curr_values != *prev {
                                peer_groups.push((group_start, i - 1));
                                group_start = i;
                            }
                        }
                        prev_values = Some(curr_values);
                    }
                    peer_groups.push((group_start, partition_size - 1));

                    for (group_start, group_end) in &peer_groups {
                        let running_indices: Vec<usize> = sorted_indices[..=*group_end].to_vec();
                        let agg_result = self.compute_window_aggregate(
                            name,
                            func,
                            rows,
                            &running_indices,
                            evaluator,
                        )?;
                        for _ in *group_start..=*group_end {
                            results.push(agg_result.clone());
                        }
                    }
                } else {
                    let agg_result =
                        self.compute_window_aggregate(name, func, rows, sorted_indices, evaluator)?;
                    for _ in 0..partition_size {
                        results.push(agg_result.clone());
                    }
                }
            }
            "PERCENT_RANK" => {
                if partition_size <= 1 {
                    for _ in 0..partition_size {
                        results.push(Value::float64(0.0));
                    }
                } else {
                    let over = func.over.as_ref().unwrap();
                    let order_by = match over {
                        ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                        _ => vec![],
                    };

                    let mut prev_values: Option<Vec<Value>> = None;
                    let mut rank = 0i64;
                    for (i, &idx) in sorted_indices.iter().enumerate() {
                        let curr_values: Vec<Value> = order_by
                            .iter()
                            .map(|ob| {
                                evaluator
                                    .evaluate(&ob.expr, &rows[idx])
                                    .unwrap_or(Value::null())
                            })
                            .collect();

                        if prev_values.as_ref() != Some(&curr_values) {
                            rank = i as i64;
                        }
                        let pct = rank as f64 / (partition_size - 1) as f64;
                        results.push(Value::float64(pct));
                        prev_values = Some(curr_values);
                    }
                }
            }
            "CUME_DIST" => {
                let over = func.over.as_ref().unwrap();
                let order_by = match over {
                    ast::WindowType::WindowSpec(spec) => spec.order_by.clone(),
                    _ => vec![],
                };

                let mut ranks = vec![0usize; partition_size];
                let mut prev_values: Option<Vec<Value>> = None;
                let mut group_end = 0usize;

                for (i, &idx) in sorted_indices.iter().enumerate() {
                    let curr_values: Vec<Value> = order_by
                        .iter()
                        .map(|ob| {
                            evaluator
                                .evaluate(&ob.expr, &rows[idx])
                                .unwrap_or(Value::null())
                        })
                        .collect();

                    if prev_values.as_ref() != Some(&curr_values) {
                        for rank in ranks.iter_mut().take(i).skip(group_end) {
                            *rank = i;
                        }
                        group_end = i;
                    }
                    prev_values = Some(curr_values);
                }
                for rank in ranks.iter_mut().take(partition_size).skip(group_end) {
                    *rank = partition_size;
                }

                for rank in ranks.iter().take(partition_size) {
                    let cume = *rank as f64 / partition_size as f64;
                    results.push(Value::float64(cume));
                }
            }
            "RUNNINGACCUMULATE" => {
                let mut running_sum: i64 = 0;
                for &idx in sorted_indices {
                    let val = self.extract_running_accumulate_value(func, evaluator, &rows[idx])?;
                    if let Some(n) = val.as_i64() {
                        running_sum += n;
                    } else if let Some(f) = val.as_f64() {
                        running_sum += f as i64;
                    }
                    results.push(Value::int64(running_sum));
                }
            }
            _ => {
                for _ in 0..partition_size {
                    results.push(Value::null());
                }
            }
        }

        Ok(results)
    }

    fn extract_first_window_arg(
        &self,
        func: &ast::Function,
        evaluator: &Evaluator,
        row: &Record,
    ) -> Result<Value> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.first() {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    match evaluator.evaluate(expr, row) {
                        Ok(val) => return Ok(val),
                        Err(Error::UnsupportedFeature(msg))
                            if msg.starts_with("Function not yet supported:") =>
                        {
                            if let Expr::Function(inner_func) = expr {
                                let inner_name = inner_func.name.to_string().to_uppercase();
                                if self.is_aggregate_function(&inner_name) {
                                    for (idx, field) in evaluator.schema.fields().iter().enumerate()
                                    {
                                        let field_upper = field.name.to_uppercase();
                                        if field_upper == inner_name
                                            || field_upper.contains(&inner_name)
                                            || (inner_name == "COUNT"
                                                && (field_upper.contains("COUNT")
                                                    || field_upper.contains("CUSTOMERS")
                                                    || field_upper.contains("ORDERS")
                                                    || field_upper.contains("NUM_")
                                                    || field_upper.starts_with("NEW_")))
                                            || (inner_name == "SUM"
                                                && (field_upper.contains("SUM")
                                                    || field_upper.contains("TOTAL")
                                                    || field_upper.contains("REVENUE")
                                                    || field_upper.contains("AMOUNT")))
                                        {
                                            if let Some(val) = row.values().get(idx) {
                                                return Ok(val.clone());
                                            }
                                        }
                                    }
                                    for (idx, val) in row.values().iter().enumerate() {
                                        if val.as_i64().is_some() || val.as_f64().is_some() {
                                            let field = &evaluator.schema.fields()[idx];
                                            let field_upper = field.name.to_uppercase();
                                            if !field_upper.contains("DATE")
                                                && !field_upper.contains("MONTH")
                                                && !field_upper.contains("YEAR")
                                                && !field_upper.contains("ID")
                                            {
                                                return Ok(val.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            return Err(Error::UnsupportedFeature(msg));
                        }
                        Err(e) => return Err(e),
                    }
                }
            }
        }
        Ok(Value::null())
    }

    fn extract_nth_window_arg(
        &self,
        func: &ast::Function,
        n: usize,
        evaluator: &Evaluator,
        row: &Record,
    ) -> Result<Option<Value>> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.get(n) {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    return Ok(Some(evaluator.evaluate(expr, row)?));
                }
            }
        }
        Ok(None)
    }

    fn extract_running_accumulate_value(
        &self,
        func: &ast::Function,
        evaluator: &Evaluator,
        row: &Record,
    ) -> Result<Value> {
        if let ast::FunctionArguments::List(arg_list) = &func.args {
            if let Some(arg) = arg_list.args.first() {
                if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(expr)) = arg {
                    if let Expr::Function(inner_func) = expr {
                        let inner_name = inner_func.name.to_string().to_uppercase();
                        if inner_name == "SUMSTATE" {
                            if let ast::FunctionArguments::List(inner_args) = &inner_func.args {
                                if let Some(inner_arg) = inner_args.args.first() {
                                    if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(
                                        inner_expr,
                                    )) = inner_arg
                                    {
                                        return evaluator.evaluate(inner_expr, row);
                                    }
                                }
                            }
                        }
                    }
                    return evaluator.evaluate(expr, row);
                }
            }
        }
        Ok(Value::null())
    }

    fn extract_numeric_offset(&self, expr: &Expr) -> Option<usize> {
        match expr {
            Expr::Value(vws) => match &vws.value {
                ast::Value::Number(n, _) => n.parse().ok(),
                _ => None,
            },
            _ => None,
        }
    }

    fn compute_frame_start(
        &self,
        bound: &ast::WindowFrameBound,
        curr_pos: usize,
        _partition_size: usize,
    ) -> usize {
        match bound {
            ast::WindowFrameBound::Preceding(None) => 0,
            ast::WindowFrameBound::Preceding(Some(expr)) => {
                let offset = self.extract_numeric_offset(expr).unwrap_or(0);
                curr_pos.saturating_sub(offset)
            }
            ast::WindowFrameBound::CurrentRow => curr_pos,
            ast::WindowFrameBound::Following(None) => curr_pos,
            ast::WindowFrameBound::Following(Some(expr)) => {
                let offset = self.extract_numeric_offset(expr).unwrap_or(0);
                curr_pos + offset
            }
        }
    }

    fn compute_frame_end(
        &self,
        bound: Option<&ast::WindowFrameBound>,
        curr_pos: usize,
        partition_size: usize,
    ) -> usize {
        match bound {
            None => curr_pos,
            Some(ast::WindowFrameBound::Preceding(None)) => 0,
            Some(ast::WindowFrameBound::Preceding(Some(expr))) => {
                let offset = self.extract_numeric_offset(expr).unwrap_or(0);
                curr_pos.saturating_sub(offset)
            }
            Some(ast::WindowFrameBound::CurrentRow) => curr_pos,
            Some(ast::WindowFrameBound::Following(None)) => partition_size.saturating_sub(1),
            Some(ast::WindowFrameBound::Following(Some(expr))) => {
                let offset = self.extract_numeric_offset(expr).unwrap_or(0);
                (curr_pos + offset).min(partition_size.saturating_sub(1))
            }
        }
    }

    fn compute_window_aggregate(
        &self,
        name: &str,
        func: &ast::Function,
        rows: &[Record],
        sorted_indices: &[usize],
        evaluator: &Evaluator,
    ) -> Result<Value> {
        match name {
            "COUNT" => {
                let is_count_star = match &func.args {
                    ast::FunctionArguments::List(arg_list) => arg_list.args.iter().any(|arg| {
                        matches!(
                            arg,
                            ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Wildcard)
                        )
                    }),
                    ast::FunctionArguments::Subquery(_) => false,
                    ast::FunctionArguments::None => false,
                };

                let count = if is_count_star {
                    sorted_indices.len() as i64
                } else {
                    let mut cnt = 0i64;
                    for &idx in sorted_indices {
                        let val = self.extract_first_window_arg(func, evaluator, &rows[idx])?;
                        if !val.is_null() {
                            cnt += 1;
                        }
                    }
                    cnt
                };
                Ok(Value::int64(count))
            }
            "SUM" => {
                let mut sum_int: Option<i64> = None;
                let mut sum_float: Option<f64> = None;
                for &idx in sorted_indices {
                    let val = self.extract_first_window_arg(func, evaluator, &rows[idx])?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        sum_int = Some(sum_int.unwrap_or(0) + i);
                    } else if let Some(f) = val.as_f64() {
                        sum_float = Some(sum_float.unwrap_or(0.0) + f);
                    }
                }
                if let Some(f) = sum_float {
                    Ok(Value::float64(f + sum_int.unwrap_or(0) as f64))
                } else if let Some(i) = sum_int {
                    Ok(Value::int64(i))
                } else {
                    Ok(Value::null())
                }
            }
            "AVG" => {
                let mut sum = 0.0f64;
                let mut count = 0i64;
                for &idx in sorted_indices {
                    let val = self.extract_first_window_arg(func, evaluator, &rows[idx])?;
                    if val.is_null() {
                        continue;
                    }
                    if let Some(i) = val.as_i64() {
                        sum += i as f64;
                        count += 1;
                    } else if let Some(f) = val.as_f64() {
                        sum += f;
                        count += 1;
                    }
                }
                if count > 0 {
                    Ok(Value::float64(sum / count as f64))
                } else {
                    Ok(Value::null())
                }
            }
            "MIN" => {
                let mut min: Option<Value> = None;
                for &idx in sorted_indices {
                    let val = self.extract_first_window_arg(func, evaluator, &rows[idx])?;
                    if val.is_null() {
                        continue;
                    }
                    match &min {
                        None => min = Some(val),
                        Some(m) => {
                            if self.compare_values_for_ordering(&val, m) == std::cmp::Ordering::Less
                            {
                                min = Some(val);
                            }
                        }
                    }
                }
                Ok(min.unwrap_or(Value::null()))
            }
            "MAX" => {
                let mut max: Option<Value> = None;
                for &idx in sorted_indices {
                    let val = self.extract_first_window_arg(func, evaluator, &rows[idx])?;
                    if val.is_null() {
                        continue;
                    }
                    match &max {
                        None => max = Some(val),
                        Some(m) => {
                            if self.compare_values_for_ordering(&val, m)
                                == std::cmp::Ordering::Greater
                            {
                                max = Some(val);
                            }
                        }
                    }
                }
                Ok(max.unwrap_or(Value::null()))
            }
            _ => Ok(Value::null()),
        }
    }

    fn infer_expr_type_with_windows(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        rows: &[Record],
        window_results: &HashMap<String, Vec<Value>>,
    ) -> Result<DataType> {
        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let key = format!("{:?}", func);
                if let Some(results) = window_results.get(&key) {
                    for val in results {
                        let dt = val.data_type();
                        if dt != DataType::Unknown {
                            return Ok(dt);
                        }
                    }
                }
                Ok(DataType::Int64)
            }
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                for field in input_schema.fields() {
                    if field.name.to_uppercase() == name {
                        return Ok(field.data_type.clone());
                    }
                }
                Ok(DataType::String)
            }
            Expr::CompoundIdentifier(parts) => {
                let last = parts
                    .last()
                    .map(|i| i.value.to_uppercase())
                    .unwrap_or_default();
                for field in input_schema.fields() {
                    if field.name.to_uppercase() == last
                        || field.name.to_uppercase().ends_with(&format!(".{}", last))
                    {
                        return Ok(field.data_type.clone());
                    }
                }
                Ok(DataType::String)
            }
            _ => {
                let sample_record = rows.first().cloned().unwrap_or_else(|| {
                    Record::from_values(vec![Value::null(); input_schema.field_count()])
                });
                let val = self
                    .evaluate_expr_with_window_results(
                        expr,
                        input_schema,
                        &sample_record,
                        0,
                        window_results,
                    )
                    .unwrap_or(Value::null());
                let dt = val.data_type();
                if dt == DataType::Unknown {
                    for (row_idx, row) in rows.iter().enumerate() {
                        let v = self
                            .evaluate_expr_with_window_results(
                                expr,
                                input_schema,
                                row,
                                row_idx,
                                window_results,
                            )
                            .ok();
                        if let Some(v) = v {
                            let t = v.data_type();
                            if t != DataType::Unknown {
                                return Ok(t);
                            }
                        }
                    }
                }
                Ok(dt)
            }
        }
    }

    fn project_rows_with_windows(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
        window_results: &HashMap<String, Vec<Value>>,
    ) -> Result<(Schema, Vec<Record>)> {
        let mut all_cols: Vec<(String, DataType)> = Vec::new();

        for (idx, item) in projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(opts) => {
                    let except_cols = Self::get_except_columns(opts);
                    for field in input_schema.fields() {
                        if !except_cols.contains(&field.name.to_lowercase()) {
                            all_cols.push((field.name.clone(), field.data_type.clone()));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(name, opts) => {
                    let table_name = name.to_string();
                    let except_cols = Self::get_except_columns(opts);
                    for field in input_schema.fields() {
                        let matches_table = field
                            .source_table
                            .as_ref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                        if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                            all_cols.push((field.name.clone(), field.data_type.clone()));
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let name = self.expr_to_alias(expr, idx);
                    let data_type = self.infer_expr_type_with_windows(
                        expr,
                        input_schema,
                        rows,
                        window_results,
                    )?;
                    all_cols.push((name, data_type));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let data_type = self.infer_expr_type_with_windows(
                        expr,
                        input_schema,
                        rows,
                        window_results,
                    )?;
                    all_cols.push((alias.value.clone(), data_type));
                }
            }
        }

        let fields: Vec<Field> = all_cols
            .iter()
            .map(|(name, dt)| Field::nullable(name.clone(), dt.clone()))
            .collect();
        let output_schema = Schema::from_fields(fields);

        let mut output_rows = Vec::with_capacity(rows.len());
        for (row_idx, row) in rows.iter().enumerate() {
            let mut values = Vec::new();
            for item in projection {
                match item {
                    SelectItem::Wildcard(opts) => {
                        let except_cols = Self::get_except_columns(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            if !except_cols.contains(&field.name.to_lowercase()) {
                                values.push(row.values()[i].clone());
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(name, opts) => {
                        let table_name = name.to_string();
                        let except_cols = Self::get_except_columns(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            let matches_table = field
                                .source_table
                                .as_ref()
                                .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                            if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                                values.push(row.values()[i].clone());
                            }
                        }
                    }
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let val = self.evaluate_expr_with_window_results(
                            expr,
                            input_schema,
                            row,
                            row_idx,
                            window_results,
                        )?;
                        values.push(val);
                    }
                }
            }
            output_rows.push(Record::from_values(values));
        }

        Ok((output_schema, output_rows))
    }

    fn project_rows_with_windows_and_aggregates(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        projection: &[SelectItem],
        window_results: &HashMap<String, Vec<Value>>,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<(Schema, Vec<Record>)> {
        let mut all_cols: Vec<(String, DataType)> = Vec::new();

        for (idx, item) in projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(opts) => {
                    let except_cols = Self::get_except_columns(opts);
                    for field in input_schema.fields() {
                        if !except_cols.contains(&field.name.to_lowercase()) {
                            all_cols.push((field.name.clone(), field.data_type.clone()));
                        }
                    }
                }
                SelectItem::QualifiedWildcard(name, opts) => {
                    let table_name = name.to_string();
                    let except_cols = Self::get_except_columns(opts);
                    for field in input_schema.fields() {
                        let matches_table = field
                            .source_table
                            .as_ref()
                            .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                        if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                            all_cols.push((field.name.clone(), field.data_type.clone()));
                        }
                    }
                }
                SelectItem::UnnamedExpr(expr) => {
                    let name = self.expr_to_alias(expr, idx);
                    let data_type = self.infer_expr_type_with_windows_and_aggregates(
                        expr,
                        input_schema,
                        rows,
                        window_results,
                        expr_to_col,
                    )?;
                    all_cols.push((name, data_type));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let data_type = self.infer_expr_type_with_windows_and_aggregates(
                        expr,
                        input_schema,
                        rows,
                        window_results,
                        expr_to_col,
                    )?;
                    all_cols.push((alias.value.clone(), data_type));
                }
            }
        }

        let fields: Vec<Field> = all_cols
            .iter()
            .map(|(name, dt)| Field::nullable(name.clone(), dt.clone()))
            .collect();
        let output_schema = Schema::from_fields(fields);

        let mut output_rows = Vec::with_capacity(rows.len());
        for (row_idx, row) in rows.iter().enumerate() {
            let mut values = Vec::new();
            for item in projection {
                match item {
                    SelectItem::Wildcard(opts) => {
                        let except_cols = Self::get_except_columns(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            if !except_cols.contains(&field.name.to_lowercase()) {
                                values.push(row.values()[i].clone());
                            }
                        }
                    }
                    SelectItem::QualifiedWildcard(name, opts) => {
                        let table_name = name.to_string();
                        let except_cols = Self::get_except_columns(opts);
                        for (i, field) in input_schema.fields().iter().enumerate() {
                            let matches_table = field
                                .source_table
                                .as_ref()
                                .is_some_and(|t| t.eq_ignore_ascii_case(&table_name));
                            if matches_table && !except_cols.contains(&field.name.to_lowercase()) {
                                values.push(row.values()[i].clone());
                            }
                        }
                    }
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let val = self.evaluate_expr_with_window_and_aggregates(
                            expr,
                            input_schema,
                            row,
                            row_idx,
                            window_results,
                            expr_to_col,
                        )?;
                        values.push(val);
                    }
                }
            }
            output_rows.push(Record::from_values(values));
        }

        Ok((output_schema, output_rows))
    }

    fn infer_expr_type_with_windows_and_aggregates(
        &self,
        expr: &Expr,
        input_schema: &Schema,
        rows: &[Record],
        window_results: &HashMap<String, Vec<Value>>,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<DataType> {
        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let key = format!("{:?}", func);
                if let Some(results) = window_results.get(&key) {
                    for val in results {
                        let dt = val.data_type();
                        if dt != DataType::Unknown {
                            return Ok(dt);
                        }
                    }
                }
                Ok(DataType::Int64)
            }
            Expr::Function(func) => {
                let key = self.normalize_expr_key(expr);
                if let Some(&col_idx) = expr_to_col.get(&key) {
                    if let Some(field) = input_schema.fields().get(col_idx) {
                        return Ok(field.data_type.clone());
                    }
                }
                let name = func.name.to_string().to_uppercase();
                if self.is_aggregate_function(&name) {
                    if let Some(row) = rows.first() {
                        if let Some(val) = row.values().first() {
                            return Ok(val.data_type());
                        }
                    }
                }
                if let Some(row) = rows.first() {
                    let evaluator =
                        Evaluator::with_user_functions(input_schema, self.catalog.get_functions());
                    if let Ok(val) = evaluator.evaluate(expr, row) {
                        return Ok(val.data_type());
                    }
                }
                Ok(DataType::Int64)
            }
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                for field in input_schema.fields() {
                    if field.name.to_uppercase() == name {
                        return Ok(field.data_type.clone());
                    }
                }
                Ok(DataType::String)
            }
            Expr::CompoundIdentifier(parts) => {
                let last = parts
                    .last()
                    .map(|i| i.value.to_uppercase())
                    .unwrap_or_default();
                for field in input_schema.fields() {
                    if field.name.to_uppercase() == last
                        || field.name.to_uppercase().ends_with(&format!(".{}", last))
                    {
                        return Ok(field.data_type.clone());
                    }
                }
                Ok(DataType::String)
            }
            Expr::Value(val) => {
                let evaluator =
                    Evaluator::with_user_functions(input_schema, self.catalog.get_functions());
                let sample_record = rows.first().cloned().unwrap_or_else(|| {
                    Record::from_values(vec![Value::null(); input_schema.field_count()])
                });
                match evaluator.evaluate(expr, &sample_record) {
                    Ok(v) => Ok(v.data_type()),
                    Err(_) => match &val.value {
                        ast::Value::Number(_, _) => Ok(DataType::Float64),
                        ast::Value::SingleQuotedString(_) | ast::Value::DoubleQuotedString(_) => {
                            Ok(DataType::String)
                        }
                        ast::Value::Boolean(_) => Ok(DataType::Bool),
                        ast::Value::Null => Ok(DataType::Unknown),
                        _ => Ok(DataType::Unknown),
                    },
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_type = self.infer_expr_type_with_windows_and_aggregates(
                    left,
                    input_schema,
                    rows,
                    window_results,
                    expr_to_col,
                )?;
                let right_type = self.infer_expr_type_with_windows_and_aggregates(
                    right,
                    input_schema,
                    rows,
                    window_results,
                    expr_to_col,
                )?;
                let result_type = match op {
                    ast::BinaryOperator::Plus
                    | ast::BinaryOperator::Minus
                    | ast::BinaryOperator::Multiply
                    | ast::BinaryOperator::Divide
                    | ast::BinaryOperator::Modulo => {
                        if left_type == DataType::Float64
                            || right_type == DataType::Float64
                            || matches!(left_type, DataType::Numeric(_))
                            || matches!(right_type, DataType::Numeric(_))
                        {
                            DataType::Float64
                        } else if left_type == DataType::Unknown || right_type == DataType::Unknown
                        {
                            let sample_record = rows.first().cloned().unwrap_or_else(|| {
                                Record::from_values(vec![Value::null(); input_schema.field_count()])
                            });
                            let val = self
                                .evaluate_expr_with_window_and_aggregates(
                                    expr,
                                    input_schema,
                                    &sample_record,
                                    0,
                                    window_results,
                                    expr_to_col,
                                )
                                .unwrap_or(Value::null());
                            val.data_type()
                        } else {
                            DataType::Int64
                        }
                    }
                    ast::BinaryOperator::Gt
                    | ast::BinaryOperator::Lt
                    | ast::BinaryOperator::GtEq
                    | ast::BinaryOperator::LtEq
                    | ast::BinaryOperator::Eq
                    | ast::BinaryOperator::NotEq
                    | ast::BinaryOperator::And
                    | ast::BinaryOperator::Or => DataType::Bool,
                    ast::BinaryOperator::BitwiseAnd
                    | ast::BinaryOperator::BitwiseOr
                    | ast::BinaryOperator::BitwiseXor => DataType::Int64,
                    ast::BinaryOperator::StringConcat => DataType::String,
                    _ => DataType::Unknown,
                };
                Ok(result_type)
            }
            Expr::Nested(inner) => self.infer_expr_type_with_windows_and_aggregates(
                inner,
                input_schema,
                rows,
                window_results,
                expr_to_col,
            ),
            _ => {
                let sample_record = rows.first().cloned().unwrap_or_else(|| {
                    Record::from_values(vec![Value::null(); input_schema.field_count()])
                });
                let val = self
                    .evaluate_expr_with_window_and_aggregates(
                        expr,
                        input_schema,
                        &sample_record,
                        0,
                        window_results,
                        expr_to_col,
                    )
                    .unwrap_or(Value::null());
                Ok(val.data_type())
            }
        }
    }

    fn evaluate_expr_with_window_and_aggregates(
        &self,
        expr: &Expr,
        schema: &Schema,
        record: &Record,
        row_idx: usize,
        window_results: &HashMap<String, Vec<Value>>,
        expr_to_col: &HashMap<String, usize>,
    ) -> Result<Value> {
        let evaluator = Evaluator::with_user_functions(schema, self.catalog.get_functions());

        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let key = format!("{:?}", func);
                if let Some(results) = window_results.get(&key) {
                    Ok(results[row_idx].clone())
                } else {
                    Ok(Value::null())
                }
            }
            Expr::Function(func) => {
                let key = self.normalize_expr_key(expr);
                if let Some(&col_idx) = expr_to_col.get(&key) {
                    if let Some(val) = record.values().get(col_idx) {
                        return Ok(val.clone());
                    }
                }
                let name = func.name.to_string().to_uppercase();
                if self.is_aggregate_function(&name) {
                    return Ok(Value::null());
                }
                evaluator.evaluate(expr, record)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr_with_window_and_aggregates(
                    left,
                    schema,
                    record,
                    row_idx,
                    window_results,
                    expr_to_col,
                )?;
                let right_val = self.evaluate_expr_with_window_and_aggregates(
                    right,
                    schema,
                    record,
                    row_idx,
                    window_results,
                    expr_to_col,
                )?;
                evaluator.evaluate_binary_op_values(&left_val, op, &right_val)
            }
            Expr::UnaryOp { op, expr: inner } => {
                let val = self.evaluate_expr_with_window_and_aggregates(
                    inner,
                    schema,
                    record,
                    row_idx,
                    window_results,
                    expr_to_col,
                )?;
                match op {
                    ast::UnaryOperator::Not => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else {
                            let b = val
                                .as_bool()
                                .ok_or_else(|| Error::type_mismatch("NOT requires BOOL"))?;
                            Ok(Value::bool_val(!b))
                        }
                    }
                    ast::UnaryOperator::Minus => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else if let Some(i) = val.as_i64() {
                            Ok(Value::int64(-i))
                        } else if let Some(f) = val.as_f64() {
                            Ok(Value::float64(-f))
                        } else {
                            Err(Error::type_mismatch("Minus requires numeric value"))
                        }
                    }
                    _ => evaluator.evaluate(expr, record),
                }
            }
            Expr::Nested(inner) => self.evaluate_expr_with_window_and_aggregates(
                inner,
                schema,
                record,
                row_idx,
                window_results,
                expr_to_col,
            ),
            _ => match evaluator.evaluate(expr, record) {
                Ok(val) => Ok(val),
                Err(Error::ColumnNotFound(name)) => {
                    if let Some(var) = self.get_variable(&name) {
                        return Ok(var.value.clone());
                    }
                    Err(Error::ColumnNotFound(name))
                }
                Err(e) => Err(e),
            },
        }
    }

    fn apply_qualify_filter(
        &self,
        schema: &Schema,
        rows: &[Record],
        qualify_expr: &Expr,
        window_results: &HashMap<String, Vec<Value>>,
    ) -> Result<Vec<usize>> {
        let mut qualifying_indices = Vec::new();

        for (row_idx, row) in rows.iter().enumerate() {
            let val = self.evaluate_expr_with_window_results(
                qualify_expr,
                schema,
                row,
                row_idx,
                window_results,
            )?;

            if val.as_bool().unwrap_or_default() {
                qualifying_indices.push(row_idx);
            }
        }

        Ok(qualifying_indices)
    }

    fn evaluate_expr_with_window_results(
        &self,
        expr: &Expr,
        schema: &Schema,
        record: &Record,
        row_idx: usize,
        window_results: &HashMap<String, Vec<Value>>,
    ) -> Result<Value> {
        let evaluator = Evaluator::with_user_functions(schema, self.catalog.get_functions());

        match expr {
            Expr::Function(func) if func.over.is_some() => {
                let key = format!("{:?}", func);
                if let Some(results) = window_results.get(&key) {
                    Ok(results[row_idx].clone())
                } else {
                    Ok(Value::null())
                }
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr_with_window_results(
                    left,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                let right_val = self.evaluate_expr_with_window_results(
                    right,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                evaluator.evaluate_binary_op_values(&left_val, op, &right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_expr_with_window_results(
                    expr,
                    schema,
                    record,
                    row_idx,
                    window_results,
                )?;
                match op {
                    ast::UnaryOperator::Not => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else {
                            let b = val
                                .as_bool()
                                .ok_or_else(|| Error::type_mismatch("NOT requires BOOL"))?;
                            Ok(Value::bool_val(!b))
                        }
                    }
                    ast::UnaryOperator::Minus => {
                        if val.is_null() {
                            Ok(Value::null())
                        } else if let Some(i) = val.as_i64() {
                            Ok(Value::int64(-i))
                        } else if let Some(f) = val.as_f64() {
                            Ok(Value::float64(-f))
                        } else {
                            Err(Error::type_mismatch("Minus requires numeric value"))
                        }
                    }
                    _ => evaluator.evaluate(expr, record),
                }
            }
            Expr::Nested(inner) => self.evaluate_expr_with_window_results(
                inner,
                schema,
                record,
                row_idx,
                window_results,
            ),
            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => {
                match operand {
                    Some(op_expr) => {
                        let op_val = self.evaluate_expr_with_window_results(
                            op_expr,
                            schema,
                            record,
                            row_idx,
                            window_results,
                        )?;
                        for cond in conditions {
                            let when_val = self.evaluate_expr_with_window_results(
                                &cond.condition,
                                schema,
                                record,
                                row_idx,
                                window_results,
                            )?;
                            if op_val == when_val {
                                return self.evaluate_expr_with_window_results(
                                    &cond.result,
                                    schema,
                                    record,
                                    row_idx,
                                    window_results,
                                );
                            }
                        }
                    }
                    None => {
                        for cond in conditions {
                            let cond_val = self.evaluate_expr_with_window_results(
                                &cond.condition,
                                schema,
                                record,
                                row_idx,
                                window_results,
                            )?;
                            if let Some(true) = cond_val.as_bool() {
                                return self.evaluate_expr_with_window_results(
                                    &cond.result,
                                    schema,
                                    record,
                                    row_idx,
                                    window_results,
                                );
                            }
                        }
                    }
                }
                match else_result {
                    Some(else_expr) => self.evaluate_expr_with_window_results(
                        else_expr,
                        schema,
                        record,
                        row_idx,
                        window_results,
                    ),
                    None => Ok(Value::null()),
                }
            }
            _ => match evaluator.evaluate(expr, record) {
                Ok(val) => Ok(val),
                Err(Error::ColumnNotFound(name)) => {
                    if let Some(var) = self.get_variable(&name) {
                        return Ok(var.value.clone());
                    }
                    if let Some(sys_name) = name.strip_prefix("@@") {
                        if let Some(val) = self.get_system_variable(sys_name) {
                            return Ok(val.clone());
                        }
                    }
                    Err(Error::ColumnNotFound(name))
                }
                Err(e) => Err(e),
            },
        }
    }

    fn execute_export_data(&self, export_data: &ast::ExportData) -> Result<Table> {
        let query_result = self.execute_query(&export_data.query)?;

        let mut uri = String::new();
        let mut format = "PARQUET".to_string();

        for option in &export_data.options {
            if let ast::SqlOption::KeyValue { key, value } = option {
                let key_str = key.value.to_uppercase();
                let value_str = self.expr_to_string(value);

                match key_str.as_str() {
                    "URI" => uri = value_str,
                    "FORMAT" => format = value_str.to_uppercase(),
                    _ => {}
                }
            }
        }

        if uri.is_empty() {
            return Err(Error::InvalidQuery(
                "EXPORT DATA requires uri option".to_string(),
            ));
        }

        let path = if uri.starts_with("file://") {
            uri.strip_prefix("file://").unwrap().to_string()
        } else if uri.starts_with("gs://") {
            let cloud_path = uri.strip_prefix("gs://").unwrap();
            cloud_path.replace('*', "data")
        } else if uri.starts_with("s3://") {
            let cloud_path = uri.strip_prefix("s3://").unwrap();
            cloud_path.replace('*', "data")
        } else {
            uri.replace('*', "data")
        };

        match format.as_str() {
            "PARQUET" => self.export_to_parquet(&query_result, &path),
            "JSON" => self.export_to_json(&query_result, &path),
            "CSV" => self.export_to_csv(&query_result, &path, &export_data.options),
            "AVRO" => self.export_to_avro(&query_result, &path),
            _ => Err(Error::UnsupportedFeature(format!(
                "Export format '{}' not supported",
                format
            ))),
        }
    }

    fn evaluate_with_variables(
        &self,
        _evaluator: &Evaluator,
        expr: &Expr,
        record: &Record,
    ) -> Result<Value> {
        let (extended_schema, extended_record) = self.extend_with_variables(record);
        let new_evaluator =
            Evaluator::with_user_functions(&extended_schema, self.catalog.get_functions());
        new_evaluator.evaluate(expr, &extended_record)
    }

    fn extend_with_variables(&self, base_record: &Record) -> (Schema, Record) {
        let mut fields = Vec::new();
        let mut values = base_record.values().to_vec();

        for scope in &self.variables {
            for (name, var) in scope {
                fields.push(Field::nullable(name.clone(), var.value.data_type()));
                values.push(var.value.clone());
            }
        }

        for (name, value) in &self.system_variables {
            let sys_name = format!("@@{}", name);
            fields.push(Field::nullable(sys_name, value.data_type()));
            values.push(value.clone());
        }

        let schema = Schema::from_fields(fields);
        let record = Record::from_values(values);
        (schema, record)
    }

    fn execute_declare(&mut self, stmts: &[ast::Declare]) -> Result<Table> {
        for decl in stmts {
            let data_type = match &decl.data_type {
                Some(dt) => self.sql_type_to_data_type(dt)?,
                None => DataType::Unknown,
            };

            let default_value = match &decl.assignment {
                Some(ast::DeclareAssignment::Default(expr)) => {
                    Some(self.evaluate_script_expr(expr)?)
                }
                Some(ast::DeclareAssignment::Expr(expr)) => Some(self.evaluate_script_expr(expr)?),
                Some(ast::DeclareAssignment::For(expr)) => Some(self.evaluate_script_expr(expr)?),
                Some(ast::DeclareAssignment::DuckAssignment(expr)) => {
                    Some(self.evaluate_script_expr(expr)?)
                }
                Some(ast::DeclareAssignment::MsSqlAssignment(expr)) => {
                    Some(self.evaluate_script_expr(expr)?)
                }
                None => None,
            };

            for name in &decl.names {
                self.declare_variable(&name.value, data_type.clone(), default_value.clone());
            }
        }
        Ok(Table::empty(Schema::new()))
    }

    fn execute_set_statement(&mut self, set_stmt: &ast::Set) -> Result<Table> {
        match set_stmt {
            ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let var_name = self.object_name_to_string(variable);
                let var_upper = var_name.to_uppercase();

                if var_upper == "SEARCH_PATH" {
                    return self.execute_set(set_stmt);
                }

                if let Some(sys_var_name) = var_name.strip_prefix("@@") {
                    if let Some(first_val) = values.first() {
                        let value = self.evaluate_script_expr(first_val)?;
                        self.set_system_variable(sys_var_name, value);
                    }
                    return Ok(Table::empty(Schema::new()));
                }

                if let Some(first_val) = values.first() {
                    let value = self.evaluate_script_expr(first_val)?;
                    self.set_variable(&var_name, value)?;
                }
                Ok(Table::empty(Schema::new()))
            }
            ast::Set::ParenthesizedAssignments { variables, values } => {
                for (var, val) in variables.iter().zip(values.iter()) {
                    let var_name = self.object_name_to_string(var);
                    let value = self.evaluate_script_expr(val)?;
                    self.set_variable(&var_name, value)?;
                }
                Ok(Table::empty(Schema::new()))
            }
            _ => self.execute_set(set_stmt),
        }
    }

    fn execute_if(&mut self, if_stmt: &ast::IfStatement) -> Result<(Table, Option<LoopControl>)> {
        let condition = if_stmt
            .if_block
            .condition
            .as_ref()
            .ok_or_else(|| Error::InvalidQuery("IF statement missing condition".to_string()))?;

        let cond_val = self.evaluate_script_expr(condition)?;
        let cond_bool = cond_val.as_bool().unwrap_or(false);

        if cond_bool {
            return self.execute_statement_block(&if_stmt.if_block.conditional_statements);
        }

        for elseif_block in &if_stmt.elseif_blocks {
            if let Some(elseif_cond) = &elseif_block.condition {
                let elseif_val = self.evaluate_script_expr(elseif_cond)?;
                if elseif_val.as_bool().unwrap_or(false) {
                    return self.execute_statement_block(&elseif_block.conditional_statements);
                }
            }
        }

        if let Some(else_block) = &if_stmt.else_block {
            return self.execute_statement_block(&else_block.conditional_statements);
        }

        Ok((Table::empty(Schema::new()), None))
    }

    fn execute_while(
        &mut self,
        while_stmt: &ast::WhileStatement,
    ) -> Result<(Table, Option<LoopControl>)> {
        let max_iterations = 10000;
        let mut iterations = 0;
        let mut last_result = Table::empty(Schema::new());

        let condition =
            while_stmt.while_block.condition.as_ref().ok_or_else(|| {
                Error::InvalidQuery("WHILE statement missing condition".to_string())
            })?;

        while iterations < max_iterations {
            let cond_val = self.evaluate_script_expr(condition)?;
            if !cond_val.as_bool().unwrap_or(false) {
                break;
            }

            let (result, control) =
                self.execute_statement_block(&while_stmt.while_block.conditional_statements)?;
            last_result = result;

            match control {
                Some(LoopControl::Break) => break,
                Some(LoopControl::Return) => return Ok((last_result, Some(LoopControl::Return))),
                Some(LoopControl::Continue) | None => {}
            }

            iterations += 1;
        }

        Ok((last_result, None))
    }

    fn execute_loop(&mut self, loop_stmt: &LoopStatement) -> Result<(Table, Option<LoopControl>)> {
        let max_iterations = 10000;
        let mut iterations = 0;
        let mut last_result = Table::empty(Schema::new());

        while iterations < max_iterations {
            let (result, control) = self.execute_script_body(&loop_stmt.body)?;
            last_result = result;

            match control {
                Some(LoopControl::Break) => break,
                Some(LoopControl::Return) => return Ok((last_result, Some(LoopControl::Return))),
                Some(LoopControl::Continue) | None => {}
            }

            iterations += 1;
        }

        Ok((last_result, None))
    }

    fn execute_while_do(
        &mut self,
        while_stmt: &WhileDoStatement,
    ) -> Result<(Table, Option<LoopControl>)> {
        let max_iterations = 10000;
        let mut iterations = 0;
        let mut last_result = Table::empty(Schema::new());

        while iterations < max_iterations {
            let cond_result = self.execute_sql(&format!("SELECT {}", while_stmt.condition))?;
            let records = cond_result.to_records()?;
            let cond_val = records
                .first()
                .and_then(|r| r.values().first())
                .cloned()
                .unwrap_or(Value::null());

            if !cond_val.as_bool().unwrap_or(false) {
                break;
            }

            let (result, control) = self.execute_script_body(&while_stmt.body)?;
            last_result = result;

            match control {
                Some(LoopControl::Break) => break,
                Some(LoopControl::Return) => return Ok((last_result, Some(LoopControl::Return))),
                Some(LoopControl::Continue) | None => {}
            }

            iterations += 1;
        }

        Ok((last_result, None))
    }

    fn execute_repeat(
        &mut self,
        repeat_stmt: &RepeatStatement,
    ) -> Result<(Table, Option<LoopControl>)> {
        let max_iterations = 10000;
        let mut iterations = 0;
        let mut last_result = Table::empty(Schema::new());

        loop {
            if iterations >= max_iterations {
                break;
            }

            let (result, control) = self.execute_script_body(&repeat_stmt.body)?;
            last_result = result;

            match control {
                Some(LoopControl::Break) => break,
                Some(LoopControl::Return) => return Ok((last_result, Some(LoopControl::Return))),
                Some(LoopControl::Continue) | None => {}
            }

            let cond_result =
                self.execute_sql(&format!("SELECT {}", repeat_stmt.until_condition))?;
            let records = cond_result.to_records()?;
            let cond_val = records
                .first()
                .and_then(|r| r.values().first())
                .cloned()
                .unwrap_or(Value::null());

            if cond_val.as_bool().unwrap_or(false) {
                break;
            }

            iterations += 1;
        }

        Ok((last_result, None))
    }

    fn execute_for(&mut self, for_stmt: &ForStatement) -> Result<(Table, Option<LoopControl>)> {
        let query_str = for_stmt.query.trim();
        let query_to_execute = if query_str.starts_with('(') && query_str.ends_with(')') {
            &query_str[1..query_str.len() - 1]
        } else {
            query_str
        };
        let query_result = self.execute_sql(query_to_execute)?;
        let records = query_result.to_records()?;
        let schema = query_result.schema();
        let mut last_result = Table::empty(Schema::new());

        self.push_scope();

        let loop_var_name = for_stmt.loop_var.to_uppercase();

        for record in records {
            let struct_fields: Vec<(String, Value)> = schema
                .fields()
                .iter()
                .zip(record.values().iter())
                .map(|(f, v)| (f.name.clone(), v.clone()))
                .collect();

            let struct_value = Value::Struct(struct_fields);

            self.declare_variable(&loop_var_name, DataType::Unknown, Some(struct_value));

            let (result, control) = self.execute_script_body(&for_stmt.body)?;
            last_result = result;

            match control {
                Some(LoopControl::Break) => break,
                Some(LoopControl::Return) => {
                    self.pop_scope();
                    return Ok((last_result, Some(LoopControl::Return)));
                }
                Some(LoopControl::Continue) | None => {}
            }
        }

        self.pop_scope();
        Ok((last_result, None))
    }

    fn execute_leave_iterate(
        &mut self,
        is_leave: bool,
        _label: Option<&str>,
    ) -> Result<(Table, Option<LoopControl>)> {
        if is_leave {
            Ok((Table::empty(Schema::new()), Some(LoopControl::Break)))
        } else {
            Ok((Table::empty(Schema::new()), Some(LoopControl::Continue)))
        }
    }

    fn execute_script_body(&mut self, body: &str) -> Result<(Table, Option<LoopControl>)> {
        let statements = split_script_statements(body);
        let mut last_result = Table::empty(Schema::new());
        let mut control = None;

        for stmt_sql in statements {
            let stmt_sql = stmt_sql.trim();
            if stmt_sql.is_empty() {
                continue;
            }

            if let Some(loop_stmt) = parse_loop_statement(stmt_sql) {
                let (result, ctrl) = self.execute_loop(&loop_stmt)?;
                last_result = result;
                control = ctrl;
            } else if let Some(while_stmt) = parse_while_do_statement(stmt_sql) {
                let (result, ctrl) = self.execute_while_do(&while_stmt)?;
                last_result = result;
                control = ctrl;
            } else if let Some(repeat_stmt) = parse_repeat_statement(stmt_sql) {
                let (result, ctrl) = self.execute_repeat(&repeat_stmt)?;
                last_result = result;
                control = ctrl;
            } else if let Some(for_stmt) = parse_for_statement(stmt_sql) {
                let (result, ctrl) = self.execute_for(&for_stmt)?;
                last_result = result;
                control = ctrl;
            } else if let Some((is_leave, label)) = is_leave_or_iterate_statement(stmt_sql) {
                let (result, ctrl) = self.execute_leave_iterate(is_leave, label.as_deref())?;
                last_result = result;
                control = ctrl;
            } else {
                let preprocessed = preprocess_loop_control_statements(stmt_sql);
                let dialect = BigQueryDialect {};
                match Parser::parse_sql(&dialect, &preprocessed) {
                    Ok(parsed_stmts) => {
                        if let Some(stmt) = parsed_stmts.first() {
                            let exec_result = self.execute_statement_internal(stmt);
                            match exec_result {
                                Ok((result, ctrl)) => {
                                    last_result = result;
                                    control = ctrl;
                                }
                                Err(Error::InvalidQuery(msg)) if msg.contains("__LOOP_LEAVE__") => {
                                    control = Some(LoopControl::Break);
                                }
                                Err(Error::InvalidQuery(msg))
                                    if msg.contains("__LOOP_ITERATE__") =>
                                {
                                    control = Some(LoopControl::Continue);
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }
                    Err(e) => {
                        return Err(Error::ParseError(e.to_string()));
                    }
                }
            }

            if control.is_some() {
                break;
            }
        }

        Ok((last_result, control))
    }

    fn execute_begin_block(
        &mut self,
        statements: &[Statement],
        exception: Option<&Vec<ast::ExceptionWhen>>,
    ) -> Result<(Table, Option<LoopControl>)> {
        self.push_scope();
        let result = self.execute_statements_with_exception(statements, exception);
        self.pop_scope();
        result
    }

    fn execute_statements_with_exception(
        &mut self,
        statements: &[Statement],
        exception: Option<&Vec<ast::ExceptionWhen>>,
    ) -> Result<(Table, Option<LoopControl>)> {
        let mut last_result = Table::empty(Schema::new());
        let mut control = None;

        for stmt in statements {
            let exec_result = self.execute_statement_internal(stmt);
            match exec_result {
                Ok((result, ctrl)) => {
                    last_result = result;
                    control = ctrl;
                    if control.is_some() {
                        break;
                    }
                }
                Err(e) => {
                    if let Some(exc_list) = exception {
                        if let Some(handler) = exc_list.first() {
                            return self.execute_exception_handler(handler);
                        }
                    }
                    return Err(e);
                }
            }
        }

        Ok((last_result, control))
    }

    fn execute_exception_handler(
        &mut self,
        exc: &ast::ExceptionWhen,
    ) -> Result<(Table, Option<LoopControl>)> {
        let mut last_result = Table::empty(Schema::new());
        let mut control = None;
        for stmt in &exc.statements {
            let (result, ctrl) = self.execute_statement_internal(stmt)?;
            last_result = result;
            control = ctrl;
            if control.is_some() {
                break;
            }
        }
        Ok((last_result, control))
    }

    fn execute_return(&self, _ret: &ast::ReturnStatement) -> Result<(Table, Option<LoopControl>)> {
        Ok((Table::empty(Schema::new()), Some(LoopControl::Return)))
    }

    fn execute_raise(&self, raise: &ast::RaiseStatement) -> Result<(Table, Option<LoopControl>)> {
        let message = match &raise.value {
            Some(ast::RaiseStatementValue::UsingMessage(e))
            | Some(ast::RaiseStatementValue::Expr(e)) => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = Evaluator::new(&empty_schema);
                evaluator
                    .evaluate(e, &empty_record)
                    .ok()
                    .and_then(|v| v.as_str().map(|s| s.to_string()))
                    .unwrap_or_else(|| "Error raised".to_string())
            }
            None => "Error raised".to_string(),
        };

        Err(Error::InvalidQuery(message))
    }

    fn execute_immediate(&mut self, parameters: &[Expr]) -> Result<(Table, Option<LoopControl>)> {
        let sql_value = parameters
            .first()
            .map(|p| self.evaluate_script_expr(p))
            .transpose()?
            .ok_or_else(|| {
                Error::InvalidQuery("EXECUTE IMMEDIATE requires a string argument".to_string())
            })?;

        let sql = sql_value.as_str().map(|s| s.to_string()).ok_or_else(|| {
            Error::InvalidQuery("EXECUTE IMMEDIATE requires a string argument".to_string())
        })?;

        let result = self.execute_sql(&sql)?;
        Ok((result, None))
    }

    fn execute_case_statement(
        &mut self,
        case_stmt: &ast::CaseStatement,
    ) -> Result<(Table, Option<LoopControl>)> {
        let match_operand = case_stmt
            .match_expr
            .as_ref()
            .map(|e| self.evaluate_script_expr(e))
            .transpose()?;

        for branch in &case_stmt.when_blocks {
            if let Some(condition) = &branch.condition {
                let cond_val = self.evaluate_script_expr(condition)?;

                let matches = match &match_operand {
                    Some(operand) => operand == &cond_val,
                    None => cond_val.as_bool().unwrap_or(false),
                };

                if matches {
                    return self.execute_statement_block(&branch.conditional_statements);
                }
            }
        }

        if let Some(else_block) = &case_stmt.else_block {
            return self.execute_statement_block(&else_block.conditional_statements);
        }

        Ok((Table::empty(Schema::new()), None))
    }

    fn execute_statement_block(
        &mut self,
        block: &ast::ConditionalStatements,
    ) -> Result<(Table, Option<LoopControl>)> {
        match block {
            ast::ConditionalStatements::Sequence { statements } => {
                let mut last_result = Table::empty(Schema::new());
                let mut control = None;
                for stmt in statements {
                    let (result, ctrl) = self.execute_statement_internal(stmt)?;
                    last_result = result;
                    control = ctrl;
                    if control.is_some() {
                        break;
                    }
                }
                Ok((last_result, control))
            }
            ast::ConditionalStatements::BeginEnd(begin_end) => {
                self.push_scope();
                let mut last_result = Table::empty(Schema::new());
                let mut control = None;
                for stmt in &begin_end.statements {
                    let (result, ctrl) = self.execute_statement_internal(stmt)?;
                    last_result = result;
                    control = ctrl;
                    if control.is_some() {
                        break;
                    }
                }
                self.pop_scope();
                Ok((last_result, control))
            }
        }
    }

    fn evaluate_script_expr(&self, expr: &Expr) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let name = &ident.value;
                if let Some(var) = self.get_variable(name) {
                    return Ok(var.value.clone());
                }
                if let Some(sys_var_name) = name.strip_prefix("@@") {
                    if let Some(val) = self.get_system_variable(sys_var_name) {
                        return Ok(val.clone());
                    }
                }
                self.evaluate_literal_expr(expr)
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_script_expr(left)?;
                let right_val = self.evaluate_script_expr(right)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }
            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate_script_expr(expr)?;
                match op {
                    ast::UnaryOperator::Minus => {
                        if let Some(i) = val.as_i64() {
                            return Ok(Value::int64(-i));
                        }
                        if let Some(f) = val.as_f64() {
                            return Ok(Value::float64(-f));
                        }
                        Err(Error::InvalidQuery(
                            "Cannot negate non-numeric value".to_string(),
                        ))
                    }
                    ast::UnaryOperator::Not => {
                        let b = val.as_bool().unwrap_or(false);
                        Ok(Value::bool_val(!b))
                    }
                    ast::UnaryOperator::Plus => Ok(val),
                    _ => self.evaluate_literal_expr(expr),
                }
            }
            Expr::Nested(inner) => self.evaluate_script_expr(inner),
            Expr::Subquery(query) => {
                let result = self.execute_query(query)?;
                let records = result.to_records()?;
                if records.is_empty() {
                    return Ok(Value::null());
                }
                let first_record = &records[0];
                let values = first_record.values();
                if values.is_empty() {
                    return Ok(Value::null());
                }
                Ok(values[0].clone())
            }
            Expr::CompoundIdentifier(parts) => {
                if parts.len() == 2 {
                    let var_name = &parts[0].value;
                    let field_name = &parts[1].value;
                    if let Some(var) = self.get_variable(var_name) {
                        if let Some(fields) = var.value.as_struct() {
                            for (name, value) in fields {
                                if name.to_uppercase() == field_name.to_uppercase() {
                                    return Ok(value.clone());
                                }
                            }
                        }
                    }
                }
                self.evaluate_literal_expr(expr)
            }
            Expr::Function(_) => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = Evaluator::new(&empty_schema);
                evaluator.evaluate(expr, &empty_record)
            }
            _ => self.evaluate_literal_expr(expr),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &ast::BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        match op {
            ast::BinaryOperator::Plus => self.add_values(left, right),
            ast::BinaryOperator::Minus => self.subtract_values(left, right),
            ast::BinaryOperator::Multiply => self.multiply_values(left, right),
            ast::BinaryOperator::Divide => self.divide_values(left, right),
            ast::BinaryOperator::Gt => Ok(Value::bool_val(
                self.compare_script_values(left, right)? > 0,
            )),
            ast::BinaryOperator::GtEq => Ok(Value::bool_val(
                self.compare_script_values(left, right)? >= 0,
            )),
            ast::BinaryOperator::Lt => Ok(Value::bool_val(
                self.compare_script_values(left, right)? < 0,
            )),
            ast::BinaryOperator::LtEq => Ok(Value::bool_val(
                self.compare_script_values(left, right)? <= 0,
            )),
            ast::BinaryOperator::Eq => Ok(Value::bool_val(
                self.compare_script_values(left, right)? == 0,
            )),
            ast::BinaryOperator::NotEq => Ok(Value::bool_val(
                self.compare_script_values(left, right)? != 0,
            )),
            ast::BinaryOperator::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::bool_val(l && r))
            }
            ast::BinaryOperator::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::bool_val(l || r))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }

    fn expr_to_string(&self, expr: &Expr) -> String {
        match expr {
            Expr::Value(v) => match &v.value {
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.clone(),
                other => other.to_string(),
            },
            Expr::Identifier(ident) => ident.value.clone(),
            other => other.to_string(),
        }
    }

    fn export_to_parquet(&self, table: &Table, path: &str) -> Result<Table> {
        use std::path::Path;
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let arrow_schema = self.table_schema_to_arrow_schema(table.schema());
        let record_batch = self.table_to_record_batch(table, &arrow_schema)?;

        let file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file: {}", e)))?;
        let mut writer = ArrowWriter::try_new(file, Arc::new(arrow_schema), None)
            .map_err(|e| Error::internal(format!("Failed to create Parquet writer: {}", e)))?;

        writer
            .write(&record_batch)
            .map_err(|e| Error::internal(format!("Failed to write Parquet: {}", e)))?;
        writer
            .close()
            .map_err(|e| Error::internal(format!("Failed to close Parquet writer: {}", e)))?;

        let result_schema = Schema::from_fields(vec![]);
        Ok(Table::empty(result_schema))
    }

    fn export_to_json(&self, table: &Table, path: &str) -> Result<Table> {
        use std::path::Path;
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file: {}", e)))?;
        let mut writer = BufWriter::new(file);

        let schema = table.schema();
        let num_rows = table.num_rows();

        for row_idx in 0..num_rows {
            let mut json_obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                let value = col.get_value(row_idx);
                let json_val = self.value_to_json(&value);
                json_obj.insert(field.name.clone(), json_val);
            }
            let line = serde_json::to_string(&serde_json::Value::Object(json_obj))
                .map_err(|e| Error::internal(format!("JSON serialization error: {}", e)))?;
            writeln!(writer, "{}", line)
                .map_err(|e| Error::internal(format!("Write error: {}", e)))?;
        }

        writer
            .flush()
            .map_err(|e| Error::internal(format!("Flush error: {}", e)))?;

        let result_schema = Schema::from_fields(vec![]);
        Ok(Table::empty(result_schema))
    }

    fn export_to_csv(
        &self,
        table: &Table,
        path: &str,
        options: &[ast::SqlOption],
    ) -> Result<Table> {
        use std::path::Path;
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file: {}", e)))?;
        let mut writer = BufWriter::new(file);

        let mut field_delimiter = ",";
        let mut include_header = false;

        for option in options {
            if let ast::SqlOption::KeyValue { key, value } = option {
                let key_str = key.value.to_uppercase();
                let value_str = self.expr_to_string(value);
                match key_str.as_str() {
                    "FIELD_DELIMITER" => field_delimiter = if value_str == "|" { "|" } else { "," },
                    "HEADER" => include_header = value_str.to_uppercase() == "TRUE",
                    _ => {}
                }
            }
        }

        let schema = table.schema();
        let num_rows = table.num_rows();

        if include_header {
            let header: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
            writeln!(writer, "{}", header.join(field_delimiter))
                .map_err(|e| Error::internal(format!("Write error: {}", e)))?;
        }

        for row_idx in 0..num_rows {
            let mut values = Vec::new();
            for col_idx in 0..schema.fields().len() {
                let col = table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                let value = col.get_value(row_idx);
                let csv_val = self.value_to_csv(&value);
                values.push(csv_val);
            }
            writeln!(writer, "{}", values.join(field_delimiter))
                .map_err(|e| Error::internal(format!("Write error: {}", e)))?;
        }

        writer
            .flush()
            .map_err(|e| Error::internal(format!("Flush error: {}", e)))?;

        let result_schema = Schema::from_fields(vec![]);
        Ok(Table::empty(result_schema))
    }

    fn value_to_csv(&self, value: &Value) -> String {
        if value.is_null() {
            return String::new();
        }

        match value {
            Value::Bool(b) => b.to_string(),
            Value::Int64(i) => i.to_string(),
            Value::Float64(f) => f.0.to_string(),
            Value::String(s) => {
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            }
            Value::Date(d) => d.format("%Y-%m-%d").to_string(),
            Value::DateTime(dt) => dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string(),
            _ => format!("{}", value),
        }
    }

    fn export_to_avro(&self, table: &Table, path: &str) -> Result<Table> {
        use std::path::Path;
        if let Some(parent) = Path::new(path).parent() {
            std::fs::create_dir_all(parent).ok();
        }

        let file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file: {}", e)))?;
        let mut writer = BufWriter::new(file);

        let schema = table.schema();
        let num_rows = table.num_rows();

        for row_idx in 0..num_rows {
            let mut json_obj = serde_json::Map::new();
            for (col_idx, field) in schema.fields().iter().enumerate() {
                let col = table
                    .column(col_idx)
                    .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                let value = col.get_value(row_idx);
                let json_val = self.value_to_json(&value);
                json_obj.insert(field.name.clone(), json_val);
            }
            let line = serde_json::to_string(&serde_json::Value::Object(json_obj))
                .map_err(|e| Error::internal(format!("JSON serialization error: {}", e)))?;
            writeln!(writer, "{}", line)
                .map_err(|e| Error::internal(format!("Write error: {}", e)))?;
        }

        writer
            .flush()
            .map_err(|e| Error::internal(format!("Flush error: {}", e)))?;

        let result_schema = Schema::from_fields(vec![]);
        Ok(Table::empty(result_schema))
    }

    fn value_to_json(&self, value: &Value) -> serde_json::Value {
        if value.is_null() {
            return serde_json::Value::Null;
        }

        match value {
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(i) => serde_json::Value::Number((*i).into()),
            Value::Float64(f) => serde_json::Number::from_f64(f.0)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Date(d) => serde_json::Value::String(d.format("%Y-%m-%d").to_string()),
            Value::DateTime(dt) => {
                serde_json::Value::String(dt.format("%Y-%m-%dT%H:%M:%S%.6f").to_string())
            }
            _ => serde_json::Value::String(format!("{}", value)),
        }
    }

    fn table_schema_to_arrow_schema(&self, schema: &Schema) -> ArrowSchema {
        let fields: Vec<ArrowField> = schema
            .fields()
            .iter()
            .map(|f| {
                let arrow_type = match &f.data_type {
                    DataType::Bool => ArrowDataType::Boolean,
                    DataType::Int64 => ArrowDataType::Int64,
                    DataType::Float64 => ArrowDataType::Float64,
                    DataType::String => ArrowDataType::Utf8,
                    DataType::Date => ArrowDataType::Date32,
                    DataType::DateTime => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                    DataType::Timestamp => ArrowDataType::Timestamp(TimeUnit::Microsecond, None),
                    _ => ArrowDataType::Utf8,
                };
                ArrowField::new(&f.name, arrow_type, f.is_nullable())
            })
            .collect();
        ArrowSchema::new(fields)
    }

    fn table_to_record_batch(
        &self,
        table: &Table,
        arrow_schema: &ArrowSchema,
    ) -> Result<RecordBatch> {
        let num_rows = table.num_rows();
        let schema = table.schema();

        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array: ArrayRef = match &field.data_type {
                DataType::Bool => {
                    let mut builder = BooleanBuilder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else if let Some(b) = val.as_bool() {
                            builder.append_value(b);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else if let Some(i) = val.as_i64() {
                            builder.append_value(i);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else if let Some(f) = val.as_f64() {
                            builder.append_value(f);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::String => {
                    let mut builder = StringBuilder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else if let Some(s) = val.as_str() {
                            builder.append_value(s);
                        } else {
                            builder.append_value(val.to_string());
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Date => {
                    use chrono::Datelike;
                    let mut builder = Date32Builder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else if let Value::Date(d) = val {
                            let days = d.num_days_from_ce() - 719163;
                            builder.append_value(days);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::DateTime | DataType::Timestamp => {
                    let mut builder = TimestampMicrosecondBuilder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else if let Value::DateTime(dt) = val {
                            let micros = dt.and_utc().timestamp_micros();
                            builder.append_value(micros);
                        } else if let Value::Timestamp(ts) = val {
                            let micros = ts.timestamp_micros();
                            builder.append_value(micros);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                _ => {
                    let mut builder = StringBuilder::new();
                    let col = table
                        .column(col_idx)
                        .ok_or_else(|| Error::internal(format!("Column {} not found", col_idx)))?;
                    for row_idx in 0..num_rows {
                        let val = col.get_value(row_idx);
                        if val.is_null() {
                            builder.append_null();
                        } else {
                            builder.append_value(val.to_string());
                        }
                    }
                    Arc::new(builder.finish())
                }
            };
            arrays.push(array);
        }

        RecordBatch::try_new(Arc::new(arrow_schema.clone()), arrays)
            .map_err(|e| Error::internal(format!("Failed to create RecordBatch: {}", e)))
    }

    fn execute_load_data(&mut self, load_info: &LoadDataInfo) -> Result<Table> {
        if load_info.is_temp_table && !load_info.column_defs.is_empty() {
            let fields: Vec<Field> = load_info
                .column_defs
                .iter()
                .map(|(name, dtype)| {
                    let data_type = parse_simple_data_type(dtype);
                    Field::nullable(name.clone(), data_type)
                })
                .collect();
            let schema = Schema::from_fields(fields);
            self.catalog
                .create_table(&load_info.table_name, schema)
                .ok();
        }

        let table = self
            .catalog
            .get_table_mut(&load_info.table_name)
            .ok_or_else(|| Error::table_not_found(&load_info.table_name))?;

        if load_info.overwrite {
            table.clear();
        }

        let schema = table.schema().clone();

        for uri in &load_info.uris {
            let (path, is_cloud_uri) = if uri.starts_with("file://") {
                (uri.strip_prefix("file://").unwrap().to_string(), false)
            } else if uri.starts_with("gs://") {
                (
                    uri.strip_prefix("gs://").unwrap().replace('*', "data"),
                    true,
                )
            } else if uri.starts_with("s3://") {
                (
                    uri.strip_prefix("s3://").unwrap().replace('*', "data"),
                    true,
                )
            } else {
                (uri.clone(), false)
            };

            if is_cloud_uri && !std::path::Path::new(&path).exists() {
                continue;
            }

            let rows = match load_info.format.as_str() {
                "PARQUET" => self.load_parquet(&path, &schema)?,
                "JSON" => self.load_json(&path, &schema)?,
                "CSV" => self.load_csv(&path, &schema)?,
                _ => {
                    return Err(Error::UnsupportedFeature(format!(
                        "Load format '{}' not supported",
                        load_info.format
                    )));
                }
            };

            let table = self
                .catalog
                .get_table_mut(&load_info.table_name)
                .ok_or_else(|| Error::table_not_found(&load_info.table_name))?;

            for row in rows {
                table.push_row(row)?;
            }
        }

        let result_schema = Schema::from_fields(vec![]);
        Ok(Table::empty(result_schema))
    }

    fn load_parquet(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::internal(format!("Failed to read Parquet: {}", e)))?
            .build()
            .map_err(|e| Error::internal(format!("Failed to build Parquet reader: {}", e)))?;

        let mut rows = Vec::new();
        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::internal(format!("Failed to read batch: {}", e)))?;

            let parquet_schema = batch.schema();
            let parquet_columns: Vec<String> = parquet_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();

            let column_mapping: Vec<Option<usize>> = target_columns
                .iter()
                .map(|target_col| {
                    parquet_columns
                        .iter()
                        .position(|pc| pc.eq_ignore_ascii_case(target_col))
                })
                .collect();

            for row_idx in 0..batch.num_rows() {
                let mut row_values = Vec::with_capacity(target_columns.len());

                for (col_idx, parquet_col_idx) in column_mapping.iter().enumerate() {
                    let value = match parquet_col_idx {
                        Some(pci) => {
                            let array = batch.column(*pci);
                            self.arrow_array_to_value(array, row_idx, &target_types[col_idx])?
                        }
                        None => Value::null(),
                    };
                    row_values.push(value);
                }
                rows.push(row_values);
            }
        }

        Ok(rows)
    }

    fn arrow_array_to_value(
        &self,
        array: &ArrayRef,
        row_idx: usize,
        target_type: &DataType,
    ) -> Result<Value> {
        use arrow::array::{Array, AsArray};

        if array.is_null(row_idx) {
            return Ok(Value::null());
        }

        let value = match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_boolean();
                Value::bool_val(arr.value(row_idx))
            }
            ArrowDataType::Int64 => {
                let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
                Value::int64(arr.value(row_idx))
            }
            ArrowDataType::Int32 => {
                let arr = array.as_primitive::<arrow::datatypes::Int32Type>();
                Value::int64(arr.value(row_idx) as i64)
            }
            ArrowDataType::Float64 => {
                let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
                Value::float64(arr.value(row_idx))
            }
            ArrowDataType::Float32 => {
                let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
                Value::float64(arr.value(row_idx) as f64)
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_string::<i32>();
                Value::string(arr.value(row_idx).to_string())
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_string::<i64>();
                Value::string(arr.value(row_idx).to_string())
            }
            ArrowDataType::Date32 => {
                let arr = array.as_primitive::<arrow::datatypes::Date32Type>();
                let days = arr.value(row_idx);
                let date =
                    chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();
                Value::date(date)
            }
            ArrowDataType::Timestamp(TimeUnit::Microsecond, _) => {
                let arr = array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                let ts = arr.value(row_idx);
                let datetime = chrono::DateTime::from_timestamp_micros(ts)
                    .map(|dt| dt.naive_utc())
                    .unwrap_or_default();
                match target_type {
                    DataType::DateTime => Value::datetime(datetime),
                    DataType::Timestamp => {
                        let utc_dt =
                            chrono::DateTime::from_timestamp_micros(ts).unwrap_or_default();
                        Value::timestamp(utc_dt)
                    }
                    _ => Value::datetime(datetime),
                }
            }
            ArrowDataType::Timestamp(TimeUnit::Second, _) => {
                let arr = array.as_primitive::<arrow::datatypes::TimestampSecondType>();
                let ts = arr.value(row_idx);
                let datetime = chrono::DateTime::from_timestamp(ts, 0)
                    .map(|dt| dt.naive_utc())
                    .unwrap_or_default();
                Value::datetime(datetime)
            }
            ArrowDataType::Timestamp(TimeUnit::Millisecond, _) => {
                let arr = array.as_primitive::<arrow::datatypes::TimestampMillisecondType>();
                let ts = arr.value(row_idx);
                let datetime = chrono::DateTime::from_timestamp_millis(ts)
                    .map(|dt| dt.naive_utc())
                    .unwrap_or_default();
                Value::datetime(datetime)
            }
            ArrowDataType::Timestamp(TimeUnit::Nanosecond, _) => {
                let arr = array.as_primitive::<arrow::datatypes::TimestampNanosecondType>();
                let ts = arr.value(row_idx);
                let datetime = chrono::DateTime::from_timestamp_nanos(ts).naive_utc();
                Value::datetime(datetime)
            }
            _ => Value::null(),
        };

        Ok(value)
    }

    fn load_json(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
        let reader = BufReader::new(file);

        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        let mut rows = Vec::new();

        for line_result in reader.lines() {
            let line =
                line_result.map_err(|e| Error::internal(format!("Failed to read line: {}", e)))?;

            if line.trim().is_empty() {
                continue;
            }

            let json_obj: serde_json::Value = serde_json::from_str(&line)
                .map_err(|e| Error::internal(format!("Invalid JSON: {}", e)))?;

            let json_map = match json_obj {
                serde_json::Value::Object(m) => m,
                _ => continue,
            };

            let mut row_values = Vec::with_capacity(target_columns.len());

            for (col_idx, target_col) in target_columns.iter().enumerate() {
                let json_val = json_map
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(target_col))
                    .map(|(_, v)| v);

                let value = match json_val {
                    Some(v) => self.json_to_value(v, &target_types[col_idx])?,
                    None => Value::null(),
                };
                row_values.push(value);
            }
            rows.push(row_values);
        }

        Ok(rows)
    }

    fn load_csv(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
        let reader = BufReader::new(file);

        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        let mut rows = Vec::new();

        for line_result in reader.lines() {
            let line =
                line_result.map_err(|e| Error::internal(format!("Failed to read line: {}", e)))?;

            if line.trim().is_empty() {
                continue;
            }

            let values = self.parse_csv_line(&line);
            let mut row_values = Vec::with_capacity(target_columns.len());

            for (col_idx, _) in target_columns.iter().enumerate() {
                let csv_val = values.get(col_idx).map(|s| s.as_str()).unwrap_or("");
                let value = self.csv_to_value(csv_val, &target_types[col_idx])?;
                row_values.push(value);
            }
            rows.push(row_values);
        }

        Ok(rows)
    }

    fn parse_csv_line(&self, line: &str) -> Vec<String> {
        let mut values = Vec::new();
        let mut current = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(c) = chars.next() {
            if in_quotes {
                if c == '"' {
                    if chars.peek() == Some(&'"') {
                        chars.next();
                        current.push('"');
                    } else {
                        in_quotes = false;
                    }
                } else {
                    current.push(c);
                }
            } else {
                match c {
                    '"' => in_quotes = true,
                    ',' => {
                        values.push(current.clone());
                        current.clear();
                    }
                    _ => current.push(c),
                }
            }
        }
        values.push(current);
        values
    }

    fn csv_to_value(&self, csv_val: &str, target_type: &DataType) -> Result<Value> {
        let trimmed = csv_val.trim();
        if trimmed.is_empty() {
            return Ok(Value::null());
        }

        let value = match target_type {
            DataType::Bool => {
                Value::bool_val(trimmed.eq_ignore_ascii_case("true") || trimmed == "1")
            }
            DataType::Int64 => trimmed
                .parse::<i64>()
                .map(Value::int64)
                .unwrap_or(Value::null()),
            DataType::Float64 => trimmed
                .parse::<f64>()
                .map(Value::float64)
                .unwrap_or(Value::null()),
            DataType::String => Value::string(trimmed.to_string()),
            DataType::Date => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(trimmed, "%Y-%m-%d") {
                    Value::date(date)
                } else {
                    Value::null()
                }
            }
            DataType::DateTime => {
                if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%dT%H:%M:%S%.f")
                {
                    Value::datetime(dt)
                } else if let Ok(dt) =
                    chrono::NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S")
                {
                    Value::datetime(dt)
                } else {
                    Value::null()
                }
            }
            _ => Value::null(),
        };

        Ok(value)
    }

    fn json_to_value(&self, json_val: &serde_json::Value, target_type: &DataType) -> Result<Value> {
        if json_val.is_null() {
            return Ok(Value::null());
        }

        let value = match target_type {
            DataType::Bool => match json_val {
                serde_json::Value::Bool(b) => Value::bool_val(*b),
                serde_json::Value::Number(n) => Value::bool_val(n.as_i64().unwrap_or(0) != 0),
                serde_json::Value::String(s) => {
                    Value::bool_val(s.eq_ignore_ascii_case("true") || s == "1")
                }
                _ => Value::null(),
            },
            DataType::Int64 => match json_val {
                serde_json::Value::Number(n) => {
                    if let Some(i) = n.as_i64() {
                        Value::int64(i)
                    } else if let Some(f) = n.as_f64() {
                        Value::int64(f as i64)
                    } else {
                        Value::null()
                    }
                }
                serde_json::Value::String(s) => {
                    s.parse::<i64>().map(Value::int64).unwrap_or(Value::null())
                }
                _ => Value::null(),
            },
            DataType::Float64 => match json_val {
                serde_json::Value::Number(n) => {
                    n.as_f64().map(Value::float64).unwrap_or(Value::null())
                }
                serde_json::Value::String(s) => s
                    .parse::<f64>()
                    .map(Value::float64)
                    .unwrap_or(Value::null()),
                _ => Value::null(),
            },
            DataType::String => match json_val {
                serde_json::Value::String(s) => Value::string(s.clone()),
                serde_json::Value::Number(n) => Value::string(n.to_string()),
                serde_json::Value::Bool(b) => Value::string(b.to_string()),
                _ => Value::null(),
            },
            DataType::Date => match json_val {
                serde_json::Value::String(s) => {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        Value::date(date)
                    } else {
                        Value::null()
                    }
                }
                serde_json::Value::Number(n) => {
                    if let Some(days) = n.as_i64() {
                        let date =
                            chrono::NaiveDate::from_num_days_from_ce_opt((days as i32) + 719163)
                                .unwrap_or_default();
                        Value::date(date)
                    } else {
                        Value::null()
                    }
                }
                _ => Value::null(),
            },
            DataType::DateTime => match json_val {
                serde_json::Value::String(s) => {
                    if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                    {
                        Value::datetime(dt)
                    } else if let Ok(dt) =
                        chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                    {
                        Value::datetime(dt)
                    } else {
                        Value::null()
                    }
                }
                serde_json::Value::Number(n) => {
                    if let Some(ts) = n.as_i64() {
                        let datetime = chrono::DateTime::from_timestamp_micros(ts)
                            .map(|dt| dt.naive_utc())
                            .unwrap_or_default();
                        Value::datetime(datetime)
                    } else {
                        Value::null()
                    }
                }
                _ => Value::null(),
            },
            _ => Value::null(),
        };

        Ok(value)
    }

    fn execute_create_schema(
        &mut self,
        schema_name: &ast::SchemaName,
        if_not_exists: bool,
        options: &Option<Vec<ast::SqlOption>>,
    ) -> Result<Table> {
        let name = match schema_name {
            ast::SchemaName::Simple(name) => name.to_string(),
            ast::SchemaName::UnnamedAuthorization(ident) => ident.value.clone(),
            ast::SchemaName::NamedAuthorization(name, _) => name.to_string(),
        };

        match options {
            Some(opts) if !opts.is_empty() => {
                let option_map = self.extract_schema_options(opts);
                self.catalog
                    .create_schema_with_options(&name, if_not_exists, option_map)?;
            }
            _ => {
                self.catalog.create_schema(&name, if_not_exists)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn extract_schema_options(
        &self,
        opts: &[ast::SqlOption],
    ) -> std::collections::HashMap<String, String> {
        let mut option_map = std::collections::HashMap::new();
        for opt in opts {
            if let ast::SqlOption::KeyValue { key, value } = opt {
                let key_str = key.value.clone();
                let value_str = match value {
                    Expr::Value(v) => match &v.value {
                        SqlValue::SingleQuotedString(s) => s.clone(),
                        SqlValue::DoubleQuotedString(s) => s.clone(),
                        SqlValue::Number(n, _) => n.clone(),
                        _ => format!("{}", value),
                    },
                    _ => format!("{}", value),
                };
                option_map.insert(key_str, value_str);
            }
        }
        option_map
    }

    fn execute_alter_schema(&mut self, alter_schema: &ast::AlterSchema) -> Result<Table> {
        let schema_name = alter_schema.name.to_string();

        for operation in &alter_schema.operations {
            match operation {
                ast::AlterSchemaOperation::SetOptionsParens { options } => {
                    let option_map = self.extract_schema_options(options);
                    self.catalog
                        .alter_schema_options(&schema_name, option_map)?;
                }
                _ => {
                    return Err(Error::UnsupportedFeature(format!(
                        "ALTER SCHEMA operation not supported: {:?}",
                        operation
                    )));
                }
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn execute_set(&mut self, set: &ast::Set) -> Result<Table> {
        match set {
            ast::Set::SingleAssignment {
                variable, values, ..
            } => {
                let var_name = variable.to_string().to_uppercase();
                if var_name == "SEARCH_PATH" {
                    let schemas: Vec<String> = values
                        .iter()
                        .filter_map(|v| match v {
                            Expr::Identifier(ident) => Some(ident.value.clone()),
                            Expr::Value(val) => match &val.value {
                                SqlValue::SingleQuotedString(s) => Some(s.clone()),
                                SqlValue::DoubleQuotedString(s) => Some(s.clone()),
                                _ => None,
                            },
                            _ => None,
                        })
                        .collect();
                    self.catalog.set_search_path(schemas);
                    Ok(Table::empty(Schema::new()))
                } else {
                    Err(Error::UnsupportedFeature(format!(
                        "SET {} not supported",
                        var_name
                    )))
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "SET variant not supported: {:?}",
                set
            ))),
        }
    }

    fn execute_merge(&mut self, stmt: &Statement) -> Result<Table> {
        let Statement::Merge {
            into: _,
            table,
            source,
            on,
            clauses,
            output: _,
        } = stmt
        else {
            panic!("execute_merge called with non-MERGE statement");
        };

        let target_name = match table {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => {
                return Err(Error::UnsupportedFeature(
                    "MERGE target must be a table".to_string(),
                ));
            }
        };
        let target_alias = match table {
            TableFactor::Table { alias, .. } => alias.as_ref().map(|a| a.name.value.clone()),
            _ => None,
        };

        let (source_schema, source_rows, source_alias) = self.get_merge_source_data(source)?;

        let target_table = self
            .catalog
            .get_table(&target_name)
            .ok_or_else(|| Error::TableNotFound(target_name.clone()))?;
        let target_schema = target_table.schema().clone();
        let target_row_count = target_table.row_count();
        let target_rows: Vec<Record> = (0..target_row_count)
            .map(|i| target_table.get_row(i))
            .collect::<Result<Vec<_>>>()?;

        let combined_schema = self.create_merge_combined_schema(
            &target_schema,
            &source_schema,
            target_alias.as_deref(),
            source_alias.as_deref(),
        );

        let mut matched_target_indices: std::collections::HashSet<usize> =
            std::collections::HashSet::new();
        let mut matched_source_indices: std::collections::HashSet<usize> =
            std::collections::HashSet::new();
        let mut match_pairs: Vec<(usize, usize)> = Vec::new();

        let evaluator = Evaluator::new(&combined_schema);

        for (t_idx, target_row) in target_rows.iter().enumerate() {
            for (s_idx, source_row) in source_rows.iter().enumerate() {
                let combined_record = self.create_combined_record(target_row, source_row);
                if evaluator
                    .evaluate_to_bool(on, &combined_record)
                    .unwrap_or(false)
                {
                    match_pairs.push((t_idx, s_idx));
                    matched_target_indices.insert(t_idx);
                    matched_source_indices.insert(s_idx);
                }
            }
        }

        let mut updates: Vec<(usize, Vec<Value>)> = Vec::new();
        let mut deletes: Vec<usize> = Vec::new();
        let mut inserts: Vec<Vec<Value>> = Vec::new();

        for clause in clauses {
            let ast::MergeClause {
                clause_kind,
                predicate,
                action,
            } = clause;

            match clause_kind {
                ast::MergeClauseKind::Matched => match action {
                    ast::MergeAction::Update { assignments } => {
                        for &(t_idx, s_idx) in &match_pairs {
                            let target_row = &target_rows[t_idx];
                            let source_row = &source_rows[s_idx];
                            let combined_record =
                                self.create_combined_record(target_row, source_row);

                            let should_apply = match predicate {
                                Some(pred) => evaluator.evaluate_to_bool(pred, &combined_record)?,
                                None => true,
                            };

                            if should_apply && !deletes.contains(&t_idx) {
                                let mut new_values = target_row.values().to_vec();
                                for assignment in assignments {
                                    let col_name = match &assignment.target {
                                        ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                                        _ => continue,
                                    };
                                    let col_idx = target_schema
                                        .fields()
                                        .iter()
                                        .position(|f| f.name.eq_ignore_ascii_case(&col_name))
                                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                                    let new_val =
                                        evaluator.evaluate(&assignment.value, &combined_record)?;
                                    new_values[col_idx] = new_val;
                                }
                                if let Some(pos) = updates.iter().position(|(idx, _)| *idx == t_idx)
                                {
                                    updates[pos] = (t_idx, new_values);
                                } else {
                                    updates.push((t_idx, new_values));
                                }
                            }
                        }
                    }
                    ast::MergeAction::Delete => {
                        for &(t_idx, s_idx) in &match_pairs {
                            let target_row = &target_rows[t_idx];
                            let source_row = &source_rows[s_idx];
                            let combined_record =
                                self.create_combined_record(target_row, source_row);

                            let should_apply = match predicate {
                                Some(pred) => evaluator.evaluate_to_bool(pred, &combined_record)?,
                                None => true,
                            };

                            if should_apply && !deletes.contains(&t_idx) {
                                deletes.push(t_idx);
                                updates.retain(|(idx, _)| *idx != t_idx);
                            }
                        }
                    }
                    ast::MergeAction::Insert(_) => {
                        return Err(Error::InvalidQuery(
                            "INSERT action not valid for WHEN MATCHED".to_string(),
                        ));
                    }
                },
                ast::MergeClauseKind::NotMatched | ast::MergeClauseKind::NotMatchedByTarget => {
                    match action {
                        ast::MergeAction::Insert(insert_expr) => {
                            for (s_idx, source_row) in source_rows.iter().enumerate() {
                                if matched_source_indices.contains(&s_idx) {
                                    continue;
                                }

                                let source_evaluator = Evaluator::new(&source_schema);

                                let should_apply = match predicate {
                                    Some(pred) => {
                                        source_evaluator.evaluate_to_bool(pred, source_row)?
                                    }
                                    None => true,
                                };

                                if should_apply {
                                    let insert_values = match &insert_expr.kind {
                                        ast::MergeInsertKind::Values(val) => {
                                            let val_exprs = &val.rows;
                                            let mut row_values =
                                                vec![Value::null(); target_schema.field_count()];
                                            let col_indices: Vec<usize> =
                                                if insert_expr.columns.is_empty() {
                                                    (0..target_schema.field_count()).collect()
                                                } else {
                                                    insert_expr
                                                        .columns
                                                        .iter()
                                                        .map(|c| {
                                                            target_schema
                                                                .fields()
                                                                .iter()
                                                                .position(|f| {
                                                                    f.name.eq_ignore_ascii_case(
                                                                        &c.value,
                                                                    )
                                                                })
                                                                .ok_or_else(|| {
                                                                    Error::ColumnNotFound(
                                                                        c.value.clone(),
                                                                    )
                                                                })
                                                        })
                                                        .collect::<Result<Vec<_>>>()?
                                                };

                                            if let Some(first_row) = val_exprs.first() {
                                                for (expr_idx, col_idx) in
                                                    col_indices.iter().enumerate()
                                                {
                                                    if expr_idx < first_row.len() {
                                                        let val = source_evaluator.evaluate(
                                                            &first_row[expr_idx],
                                                            source_row,
                                                        )?;
                                                        row_values[*col_idx] = val;
                                                    }
                                                }
                                            }
                                            row_values
                                        }
                                        ast::MergeInsertKind::Row => {
                                            let source_values = source_row.values();
                                            let mut row_values =
                                                vec![Value::null(); target_schema.field_count()];
                                            for (s_idx, s_field) in
                                                source_schema.fields().iter().enumerate()
                                            {
                                                if let Some(t_idx) =
                                                    target_schema.fields().iter().position(|f| {
                                                        f.name.eq_ignore_ascii_case(&s_field.name)
                                                    })
                                                {
                                                    row_values[t_idx] =
                                                        source_values[s_idx].clone();
                                                }
                                            }
                                            row_values
                                        }
                                    };
                                    inserts.push(insert_values);
                                }
                            }
                        }
                        _ => {
                            return Err(Error::InvalidQuery(
                                "Only INSERT action valid for WHEN NOT MATCHED".to_string(),
                            ));
                        }
                    }
                }
                ast::MergeClauseKind::NotMatchedBySource => match action {
                    ast::MergeAction::Delete => {
                        for (t_idx, target_row) in target_rows.iter().enumerate() {
                            if matched_target_indices.contains(&t_idx) {
                                continue;
                            }

                            let target_evaluator = Evaluator::new(&target_schema);

                            let should_apply = match predicate {
                                Some(pred) => {
                                    target_evaluator.evaluate_to_bool(pred, target_row)?
                                }
                                None => true,
                            };

                            if should_apply && !deletes.contains(&t_idx) {
                                deletes.push(t_idx);
                                updates.retain(|(idx, _)| *idx != t_idx);
                            }
                        }
                    }
                    ast::MergeAction::Update { assignments } => {
                        for (t_idx, target_row) in target_rows.iter().enumerate() {
                            if matched_target_indices.contains(&t_idx) {
                                continue;
                            }

                            let target_evaluator = Evaluator::new(&target_schema);

                            let should_apply = match predicate {
                                Some(pred) => {
                                    target_evaluator.evaluate_to_bool(pred, target_row)?
                                }
                                None => true,
                            };

                            if should_apply {
                                let mut new_values = target_row.values().to_vec();
                                for assignment in assignments {
                                    let col_name = match &assignment.target {
                                        ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                                        _ => continue,
                                    };
                                    let col_idx = target_schema
                                        .fields()
                                        .iter()
                                        .position(|f| f.name.eq_ignore_ascii_case(&col_name))
                                        .ok_or_else(|| Error::ColumnNotFound(col_name.clone()))?;
                                    let new_val =
                                        target_evaluator.evaluate(&assignment.value, target_row)?;
                                    new_values[col_idx] = new_val;
                                }
                                if let Some(pos) = updates.iter().position(|(idx, _)| *idx == t_idx)
                                {
                                    updates[pos] = (t_idx, new_values);
                                } else {
                                    updates.push((t_idx, new_values));
                                }
                            }
                        }
                    }
                    ast::MergeAction::Insert(_) => {
                        return Err(Error::InvalidQuery(
                            "INSERT action not valid for WHEN NOT MATCHED BY SOURCE".to_string(),
                        ));
                    }
                },
            }
        }

        let target_table = self.catalog.get_table_mut(&target_name).unwrap();
        for (idx, values) in updates {
            target_table.update_row(idx, values)?;
        }

        deletes.sort_unstable();
        deletes.reverse();
        for idx in deletes {
            target_table.remove_row(idx);
        }

        for values in inserts {
            target_table.push_row(values)?;
        }

        Ok(Table::empty(Schema::new()))
    }

    fn get_merge_source_data(
        &self,
        source: &ast::TableFactor,
    ) -> Result<(Schema, Vec<Record>, Option<String>)> {
        match source {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table = self
                    .catalog
                    .get_table(&table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;
                let schema = table.schema().clone();
                let rows: Vec<Record> = (0..table.row_count())
                    .map(|i| table.get_row(i))
                    .collect::<Result<Vec<_>>>()?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                Ok((schema, rows, alias_name))
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => {
                let result = self.execute_query(subquery)?;
                let schema = result.schema().clone();
                let rows: Vec<Record> = (0..result.row_count())
                    .map(|i| result.get_row(i))
                    .collect::<Result<Vec<_>>>()?;
                let alias_name = alias.as_ref().map(|a| a.name.value.clone());
                Ok((schema, rows, alias_name))
            }
            _ => Err(Error::UnsupportedFeature(
                "Unsupported MERGE source type".to_string(),
            )),
        }
    }

    fn create_merge_combined_schema(
        &self,
        target_schema: &Schema,
        source_schema: &Schema,
        target_alias: Option<&str>,
        source_alias: Option<&str>,
    ) -> Schema {
        let mut fields = Vec::new();

        for field in target_schema.fields() {
            let name = match target_alias {
                Some(alias) => format!("{}.{}", alias, field.name),
                None => field.name.clone(),
            };
            fields.push(Field::nullable(name, field.data_type.clone()));
        }

        for field in source_schema.fields() {
            let name = match source_alias {
                Some(alias) => format!("{}.{}", alias, field.name),
                None => field.name.clone(),
            };
            fields.push(Field::nullable(name, field.data_type.clone()));
        }

        Schema::from_fields(fields)
    }

    fn create_combined_record(&self, target_row: &Record, source_row: &Record) -> Record {
        let mut values = target_row.values().to_vec();
        values.extend(source_row.values().iter().cloned());
        Record::from_values(values)
    }

    fn add_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::int64(l + r));
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(Value::float64(l + r));
        }
        if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            return Ok(Value::string(format!("{}{}", l, r)));
        }
        Err(Error::InvalidQuery(
            "Cannot add incompatible types".to_string(),
        ))
    }

    fn subtract_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::int64(l - r));
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(Value::float64(l - r));
        }
        Err(Error::InvalidQuery(
            "Cannot subtract incompatible types".to_string(),
        ))
    }

    fn multiply_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::int64(l * r));
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(Value::float64(l * r));
        }
        Err(Error::InvalidQuery(
            "Cannot multiply incompatible types".to_string(),
        ))
    }

    fn divide_values(&self, left: &Value, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            if r == 0 {
                return Err(Error::InvalidQuery("Division by zero".to_string()));
            }
            return Ok(Value::int64(l / r));
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            if r == 0.0 {
                return Err(Error::InvalidQuery("Division by zero".to_string()));
            }
            return Ok(Value::float64(l / r));
        }
        Err(Error::InvalidQuery(
            "Cannot divide incompatible types".to_string(),
        ))
    }

    fn compare_script_values(&self, left: &Value, right: &Value) -> Result<i8> {
        if left.is_null() || right.is_null() {
            return Ok(0);
        }
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(match l.cmp(&r) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            });
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(if l < r {
                -1
            } else if l > r {
                1
            } else {
                0
            });
        }
        if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            return Ok(match l.cmp(r) {
                std::cmp::Ordering::Less => -1,
                std::cmp::Ordering::Equal => 0,
                std::cmp::Ordering::Greater => 1,
            });
        }
        Err(Error::InvalidQuery(
            "Cannot compare incompatible types".to_string(),
        ))
    }

    fn object_name_to_string(&self, name: &ast::ObjectName) -> String {
        name.0
            .iter()
            .map(|p| match p {
                ast::ObjectNamePart::Identifier(ident) => ident.value.clone(),
                ast::ObjectNamePart::Function(f) => f.name.value.clone(),
            })
            .collect::<Vec<_>>()
            .join(".")
    }

    fn get_except_columns(
        opts: &ast::WildcardAdditionalOptions,
    ) -> std::collections::HashSet<String> {
        opts.opt_except
            .as_ref()
            .map(|except| {
                let mut cols = std::collections::HashSet::new();
                cols.insert(except.first_element.value.to_lowercase());
                for ident in &except.additional_elements {
                    cols.insert(ident.value.to_lowercase());
                }
                cols
            })
            .unwrap_or_default()
    }

    fn get_replace_map(opts: &ast::WildcardAdditionalOptions) -> HashMap<String, Expr> {
        opts.opt_replace
            .as_ref()
            .map(|replace| {
                replace
                    .items
                    .iter()
                    .map(|item| (item.column_name.value.to_lowercase(), item.expr.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
