mod error;
mod expr_planner;
mod planner;

pub use error::PlannerError;
pub use planner::Planner;
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{ColumnDef, LoadFormat, LoadOptions, LogicalPlan};
use yachtsql_storage::Schema;

pub struct ViewDefinition {
    pub query: String,
    pub column_aliases: Vec<String>,
}

pub trait CatalogProvider {
    fn get_table_schema(&self, name: &str) -> Option<Schema>;
    fn get_view(&self, name: &str) -> Option<ViewDefinition>;
}

pub fn parse_sql(sql: &str) -> Result<Vec<sqlparser::ast::Statement>> {
    let dialect = BigQueryDialect {};
    Parser::parse_sql(&dialect, sql)
        .map_err(|e| yachtsql_common::error::Error::parse_error(e.to_string()))
}

pub fn plan_statement<C: CatalogProvider>(
    stmt: &sqlparser::ast::Statement,
    catalog: &C,
) -> Result<LogicalPlan> {
    let planner = Planner::new(catalog);
    planner.plan_statement(stmt)
}

pub fn parse_and_plan<C: CatalogProvider>(sql: &str, catalog: &C) -> Result<LogicalPlan> {
    let trimmed = sql.trim();
    let upper = trimmed.to_uppercase();

    if let Some(load_plan) = try_parse_load_data(sql)? {
        return Ok(load_plan);
    }

    if upper.starts_with("CREATE SNAPSHOT TABLE") {
        return parse_create_snapshot(trimmed);
    }
    if upper.starts_with("DROP SNAPSHOT TABLE") {
        return parse_drop_snapshot(trimmed);
    }

    let preprocessed = preprocess_range_types(sql);
    let statements = parse_sql(&preprocessed)?;

    if statements.is_empty() {
        return Err(yachtsql_common::error::Error::parse_error(
            "Empty SQL statement",
        ));
    }

    if statements.len() > 1 {
        return Err(yachtsql_common::error::Error::parse_error(
            "Multiple statements not supported",
        ));
    }

    plan_statement(&statements[0], catalog)
}

fn try_parse_load_data(sql: &str) -> Result<Option<LogicalPlan>> {
    let upper = sql.to_uppercase();
    if !upper.trim_start().starts_with("LOAD DATA") {
        return Ok(None);
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
        let into_pos = upper
            .find("INTO")
            .ok_or_else(|| Error::parse_error("LOAD DATA requires INTO clause"))?;
        let after_into = &sql[into_pos + 4..];
        let trimmed = after_into.trim_start();

        let trimmed = if is_temp_table {
            let temp_pos = trimmed.to_uppercase().find("TEMP TABLE").unwrap();
            let after_temp = &trimmed[temp_pos + 10..];
            after_temp.trim_start()
        } else {
            trimmed
        };

        let name_end = trimmed
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(trimmed.len());
        let table_name = trimmed[..name_end].to_string();
        let after_name = trimmed[name_end..].trim_start();

        if after_name.starts_with('(') && !after_name.to_uppercase().starts_with("(FORMAT") {
            let paren_end = find_matching_paren(after_name).unwrap_or(after_name.len());
            let col_str = &after_name[1..paren_end];
            let cols = parse_column_defs(col_str);
            (table_name, cols)
        } else {
            (table_name, Vec::new())
        }
    };

    let files_pos = upper
        .find("FROM FILES")
        .ok_or_else(|| Error::parse_error("LOAD DATA requires FROM FILES clause"))?;
    let options_start = sql[files_pos..].find('(').ok_or_else(|| {
        Error::parse_error("LOAD DATA FROM FILES requires options in parentheses")
    })? + files_pos
        + 1;
    let options_end = find_matching_paren(&sql[options_start - 1..])
        .map(|p| p + options_start - 1)
        .ok_or_else(|| Error::parse_error("Unmatched parenthesis in FROM FILES"))?;
    let options_str = &sql[options_start..options_end];

    let format_str = extract_option(options_str, "FORMAT")
        .or_else(|| extract_option(options_str, "format"))
        .unwrap_or_else(|| "PARQUET".to_string())
        .trim_matches(|c| c == '\'' || c == '"')
        .to_uppercase();

    let format = match format_str.as_str() {
        "CSV" => LoadFormat::Csv,
        "JSON" => LoadFormat::Json,
        "PARQUET" => LoadFormat::Parquet,
        "AVRO" => LoadFormat::Avro,
        _ => LoadFormat::Parquet,
    };

    let uris_str = extract_option(options_str, "URIS")
        .or_else(|| extract_option(options_str, "uris"))
        .ok_or_else(|| Error::parse_error("LOAD DATA requires URIS option"))?;
    let uris = parse_uri_array(&uris_str);

    let allow_schema_update = extract_option(options_str, "ALLOW_SCHEMA_UPDATE")
        .or_else(|| extract_option(options_str, "allow_schema_update"))
        .map(|s| s.to_uppercase() == "TRUE")
        .unwrap_or(false);

    let options = LoadOptions {
        uris,
        format,
        overwrite,
        allow_schema_update,
    };

    let temp_schema = if is_temp_table && !column_defs.is_empty() {
        Some(
            column_defs
                .into_iter()
                .map(|(name, dtype)| ColumnDef {
                    name,
                    data_type: parse_simple_data_type(&dtype),
                    nullable: true,
                    default_value: None,
                })
                .collect(),
        )
    } else {
        None
    };

    Ok(Some(LogicalPlan::LoadData {
        table_name,
        options,
        temp_table: is_temp_table,
        temp_schema,
    }))
}

fn find_matching_paren(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.chars().enumerate() {
        match c {
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
    let trimmed = after_key.trim_start();

    if !trimmed.starts_with('=') {
        return None;
    }

    let after_eq = trimmed[1..].trim_start();

    if after_eq.starts_with('[') {
        let end = find_matching_bracket(after_eq)?;
        Some(after_eq[..=end].to_string())
    } else if let Some(stripped) = after_eq.strip_prefix('\'') {
        let end = stripped.find('\'')?;
        Some(stripped[..end].to_string())
    } else if let Some(stripped) = after_eq.strip_prefix('"') {
        let end = stripped.find('"')?;
        Some(stripped[..end].to_string())
    } else {
        let end = after_eq
            .find(|c: char| c == ',' || c == ')' || c.is_whitespace())
            .unwrap_or(after_eq.len());
        Some(after_eq[..end].to_string())
    }
}

fn find_matching_bracket(s: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.chars().enumerate() {
        match c {
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

    inner
        .split(',')
        .map(|uri| {
            uri.trim()
                .trim_matches(|c| c == '\'' || c == '"')
                .to_string()
        })
        .filter(|s| !s.is_empty())
        .collect()
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
        "INT64" | "INTEGER" | "BIGINT" => DataType::Int64,
        "FLOAT64" | "DOUBLE" | "FLOAT" => DataType::Float64,
        "STRING" | "VARCHAR" | "TEXT" => DataType::String,
        "BOOL" | "BOOLEAN" => DataType::Bool,
        "DATE" => DataType::Date,
        "DATETIME" => DataType::DateTime,
        "TIMESTAMP" => DataType::Timestamp,
        "TIME" => DataType::Time,
        "BYTES" => DataType::Bytes,
        "JSON" => DataType::Json,
        _ => DataType::String,
    }
}

fn parse_create_snapshot(sql: &str) -> Result<LogicalPlan> {
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
        yachtsql_common::error::Error::parse_error("CREATE SNAPSHOT TABLE requires CLONE clause")
    })?;

    let snapshot_name = rest[..clone_idx].trim().to_string();
    let after_clone = rest[clone_idx + 5..].trim();

    let source_name = if let Some(for_idx) = after_clone.to_uppercase().find("FOR SYSTEM_TIME") {
        after_clone[..for_idx].trim().to_string()
    } else if let Some(opt_idx) = after_clone.to_uppercase().find("OPTIONS") {
        after_clone[..opt_idx].trim().to_string()
    } else {
        after_clone.to_string()
    };

    Ok(LogicalPlan::CreateSnapshot {
        snapshot_name,
        source_name,
        if_not_exists,
    })
}

fn parse_drop_snapshot(sql: &str) -> Result<LogicalPlan> {
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

    Ok(LogicalPlan::DropSnapshot {
        snapshot_name,
        if_exists,
    })
}

fn preprocess_range_types(sql: &str) -> String {
    use regex::Regex;
    let mut result = sql.to_string();

    let range_re = Regex::new(r"(?i)\bRANGE\s*<\s*(DATE|DATETIME|TIMESTAMP)\s*>").unwrap();
    result = range_re
        .replace_all(&result, |caps: &regex::Captures| {
            format!("RANGE_{}", caps[1].to_uppercase())
        })
        .to_string();

    let replace_proc_re = Regex::new(r"(?i)CREATE\s+OR\s+REPLACE\s+PROCEDURE\s+(\w+)").unwrap();
    result = replace_proc_re
        .replace_all(&result, "CREATE PROCEDURE __orp__$1")
        .to_string();

    let proc_re =
        Regex::new(r"(?i)(CREATE\s+PROCEDURE\s+\w+\s*\([^)]*\))\s*\n?\s*(BEGIN)").unwrap();
    result = proc_re.replace_all(&result, "$1 AS $2").to_string();

    result
}
