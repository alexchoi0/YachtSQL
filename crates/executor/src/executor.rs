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
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField, Value};
use yachtsql_storage::{Column, Field, Record, Schema, Table, TableSchemaOps};

use crate::catalog::{Catalog, UserFunction, UserProcedure};
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

pub struct QueryExecutor {
    catalog: Catalog,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> Result<Table> {
        if let Some(load_info) = parse_load_data(sql) {
            return self.execute_load_data(&load_info);
        }

        let dialect = BigQueryDialect {};
        let statements =
            Parser::parse_sql(&dialect, sql).map_err(|e| Error::ParseError(e.to_string()))?;

        if statements.is_empty() {
            return Err(Error::ParseError("Empty SQL statement".to_string()));
        }

        self.execute_statement(&statements[0])
    }

    fn execute_statement(&mut self, stmt: &Statement) -> Result<Table> {
        match stmt {
            Statement::Query(query) => self.execute_query(query),
            Statement::CreateTable(create) => self.execute_create_table(create),
            Statement::CreateView {
                name,
                columns,
                query,
                or_replace,
                if_not_exists,
                materialized,
                ..
            } => self.execute_create_view(
                name,
                columns,
                query,
                *or_replace,
                *if_not_exists,
                *materialized,
            ),
            Statement::Drop {
                object_type,
                names,
                if_exists,
                ..
            } => self.execute_drop(object_type, names, *if_exists),
            Statement::Insert(insert) => self.execute_insert(insert),
            Statement::Update {
                table,
                assignments,
                selection,
                ..
            } => self.execute_update(table, assignments, selection.as_ref()),
            Statement::Delete(delete) => self.execute_delete(delete),
            Statement::Truncate { table_names, .. } => self.execute_truncate(table_names),
            Statement::AlterTable {
                name, operations, ..
            } => self.execute_alter_table(name, operations),
            Statement::CreateFunction(create_func) => self.execute_create_function(create_func),
            Statement::DropFunction {
                if_exists,
                func_desc,
                ..
            } => self.execute_drop_function(func_desc, *if_exists),
            Statement::CreateProcedure {
                or_alter,
                name,
                params,
                body,
                ..
            } => self.execute_create_procedure(name, params, body, *or_alter),
            Statement::DropProcedure {
                if_exists,
                proc_desc,
                ..
            } => self.execute_drop_procedure(proc_desc, *if_exists),
            Statement::Call(func) => self.execute_call(func),
            Statement::ExportData(export_data) => self.execute_export_data(export_data),
            _ => Err(Error::UnsupportedFeature(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    fn execute_query(&self, query: &Query) -> Result<Table> {
        let cte_tables = self.materialize_ctes(&query.with)?;
        self.execute_query_with_ctes(query, &cte_tables)
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
                let right_set: std::collections::HashSet<String> = right_rows
                    .iter()
                    .map(|row| format!("{:?}", row.values()))
                    .collect();
                left_rows.retain(|row| {
                    let key = format!("{:?}", row.values());
                    right_set.contains(&key)
                });
                match set_quantifier {
                    ast::SetQuantifier::All | ast::SetQuantifier::ByName => {}
                    _ => {
                        let mut seen = std::collections::HashSet::new();
                        left_rows.retain(|row| {
                            let key = format!("{:?}", row.values());
                            seen.insert(key)
                        });
                    }
                }
            }
            ast::SetOperator::Except | ast::SetOperator::Minus => {
                let right_set: std::collections::HashSet<String> = right_rows
                    .iter()
                    .map(|row| format!("{:?}", row.values()))
                    .collect();
                left_rows.retain(|row| {
                    let key = format!("{:?}", row.values());
                    !right_set.contains(&key)
                });
                match set_quantifier {
                    ast::SetQuantifier::All | ast::SetQuantifier::ByName => {}
                    _ => {
                        let mut seen = std::collections::HashSet::new();
                        left_rows.retain(|row| {
                            let key = format!("{:?}", row.values());
                            seen.insert(key)
                        });
                    }
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
            let (schema, rows) = self.evaluate_select_without_from(select)?;
            (schema, rows, false)
        } else {
            self.evaluate_select_with_from_for_ordering_ctes(select, order_by, cte_tables)?
        };

        if let Some(order_by) = order_by {
            self.sort_rows(&pre_projection_schema, &mut rows, order_by)?;
        }

        let (schema, rows) = if do_projection {
            self.project_rows(&pre_projection_schema, &rows, &select.projection)?
        } else {
            (pre_projection_schema, rows)
        };
        let mut rows = rows;

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

    fn evaluate_select_without_from(&self, select: &Select) -> Result<(Schema, Vec<Record>)> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = Evaluator::with_user_functions(&empty_schema, self.catalog.get_functions());

        let mut result_values = Vec::new();
        let mut field_names = Vec::new();

        for (idx, item) in select.projection.iter().enumerate() {
            match item {
                SelectItem::UnnamedExpr(expr) => {
                    let val = evaluator.evaluate(expr, &empty_record)?;
                    result_values.push(val.clone());
                    field_names.push(self.expr_to_alias(expr, idx));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let val = evaluator.evaluate(expr, &empty_record)?;
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
            let resolved_selection = self.resolve_scalar_subqueries(selection)?;
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
            return self.execute_aggregate_query(&input_schema, &filtered_rows, select);
        }

        if select.distinct.is_some() {
            let mut seen = std::collections::HashSet::new();
            filtered_rows.retain(|row| {
                let key = format!("{:?}", row.values());
                seen.insert(key)
            });
        }

        let has_window_funcs = self.has_window_functions(&select.projection);
        if has_window_funcs {
            let window_results =
                self.compute_window_functions(&input_schema, &filtered_rows, &select.projection)?;
            return self.project_rows_with_windows(
                &input_schema,
                &filtered_rows,
                &select.projection,
                &window_results,
            );
        }

        let (output_schema, output_rows) =
            self.project_rows(&input_schema, &filtered_rows, &select.projection)?;

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
            let resolved_selection = self.resolve_scalar_subqueries(selection)?;
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
            let (schema, rows) =
                self.execute_aggregate_query(&input_schema, &filtered_rows, select)?;
            return Ok((schema, rows, false));
        }

        if select.distinct.is_some() {
            let mut seen = std::collections::HashSet::new();
            filtered_rows.retain(|row| {
                let key = format!("{:?}", row.values());
                seen.insert(key)
            });
        }

        let has_window_funcs = self.has_window_functions(&select.projection);
        if has_window_funcs {
            let window_results =
                self.compute_window_functions(&input_schema, &filtered_rows, &select.projection)?;
            let (schema, rows) = self.project_rows_with_windows(
                &input_schema,
                &filtered_rows,
                &select.projection,
                &window_results,
            )?;
            return Ok((schema, rows, false));
        }

        let needs_deferred_projection = order_by.as_ref().is_some_and(|ob| {
            self.order_by_references_non_projected_columns(&input_schema, &select.projection, ob)
        });

        if needs_deferred_projection {
            Ok((input_schema, filtered_rows, true))
        } else {
            let (output_schema, output_rows) =
                self.project_rows(&input_schema, &filtered_rows, &select.projection)?;
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
                    if let Some(last) = parts.last() {
                        let col_name = last.value.to_uppercase();
                        if !projected_columns.contains(&col_name) {
                            if input_schema.fields().iter().any(|f| {
                                f.name.to_uppercase() == col_name
                                    || f.name.to_uppercase().ends_with(&format!(".{}", col_name))
                            }) {
                                return true;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        false
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
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table_name_upper = table_name.to_uppercase();

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
                    return Ok((schema, table_data.to_records()?));
                }

                if let Some(view_def) = self.catalog.get_view(&table_name) {
                    let view_query = view_def.query.clone();
                    let column_aliases = view_def.column_aliases.clone();
                    let view_result = self.execute_view_query(&view_query)?;

                    let rows = view_result.to_records()?;
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

                Ok((schema, table_data.to_records()?))
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
                if matches!(
                    name.as_str(),
                    "COUNT"
                        | "SUM"
                        | "AVG"
                        | "MIN"
                        | "MAX"
                        | "ARRAY_AGG"
                        | "STRING_AGG"
                        | "GROUP_CONCAT"
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
                ) {
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

    fn execute_aggregate_query(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        select: &Select,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());

        let group_by_is_empty =
            matches!(&select.group_by, ast::GroupByExpr::Expressions(v, _) if v.is_empty());
        let groups: Vec<(Vec<Value>, Vec<&Record>)> = if group_by_is_empty {
            vec![(vec![], rows.iter().collect())]
        } else {
            let mut group_map: HashMap<String, (Vec<Value>, Vec<&Record>)> = HashMap::new();

            for row in rows {
                let mut group_key_values = Vec::new();
                for group_expr in self.extract_group_by_exprs(&select.group_by) {
                    let val = evaluator.evaluate(&group_expr, row)?;
                    group_key_values.push(val);
                }
                let key = format!("{:?}", group_key_values);
                group_map
                    .entry(key)
                    .or_insert_with(|| (group_key_values.clone(), Vec::new()))
                    .1
                    .push(row);
            }
            group_map.into_values().collect()
        };

        if let Some(having) = &select.having {
            let _ = having;
        }

        let mut result_rows = Vec::new();
        let mut output_fields: Option<Vec<Field>> = None;

        for (group_key, group_rows) in &groups {
            let mut row_values = Vec::new();
            let mut field_names = Vec::new();

            for (idx, item) in select.projection.iter().enumerate() {
                match item {
                    SelectItem::UnnamedExpr(expr) => {
                        let val = self.evaluate_aggregate_expr(
                            expr,
                            input_schema,
                            group_rows,
                            group_key,
                            &select.group_by,
                        )?;
                        field_names.push(self.expr_to_alias(expr, idx));
                        row_values.push(val);
                    }
                    SelectItem::ExprWithAlias { expr, alias } => {
                        let val = self.evaluate_aggregate_expr(
                            expr,
                            input_schema,
                            group_rows,
                            group_key,
                            &select.group_by,
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

            result_rows.push(Record::from_values(row_values));
        }

        let schema = Schema::from_fields(output_fields.unwrap_or_default());

        if let Some(having) = &select.having {
            let having_evaluator =
                Evaluator::with_user_functions(&schema, self.catalog.get_functions());
            result_rows.retain(|row| {
                having_evaluator
                    .evaluate_to_bool(having, row)
                    .unwrap_or(false)
            });
        }

        Ok((schema, result_rows))
    }

    fn extract_group_by_exprs(&self, group_by: &ast::GroupByExpr) -> Vec<Expr> {
        match group_by {
            ast::GroupByExpr::Expressions(exprs, _) => exprs.clone(),
            ast::GroupByExpr::All(_) => vec![],
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
                let mut evaluated_args = Vec::new();
                if let ast::FunctionArguments::List(arg_list) = &func.args {
                    for arg in &arg_list.args {
                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(arg_expr)) = arg
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
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate_function(&name, &evaluated_args, func, row)
                } else {
                    Ok(Value::null())
                }
            }
            Expr::Identifier(_) | Expr::CompoundIdentifier(_) => {
                let group_exprs = self.extract_group_by_exprs(group_by);
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
            _ => {
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(expr, row)
                } else {
                    Ok(Value::null())
                }
            }
        }
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
            "AVG" => {
                let expr = arg_expr
                    .ok_or_else(|| Error::InvalidQuery("AVG requires an argument".to_string()))?;
                let mut sum: f64 = 0.0;
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
        if self.has_array_join(projection) {
            return self.project_rows_with_array_join(input_schema, rows, projection);
        }

        let evaluator = Evaluator::with_user_functions(input_schema, self.catalog.get_functions());

        let mut all_cols: Vec<(String, DataType)> = Vec::new();

        for (idx, item) in projection.iter().enumerate() {
            match item {
                SelectItem::Wildcard(_) => {
                    for field in input_schema.fields() {
                        all_cols.push((field.name.clone(), field.data_type.clone()));
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
                    all_cols.push((name, val.data_type()));
                }
                SelectItem::ExprWithAlias { expr, alias } => {
                    let sample_record = rows.first().cloned().unwrap_or_else(|| {
                        Record::from_values(vec![Value::null(); input_schema.field_count()])
                    });
                    let val = evaluator
                        .evaluate(expr, &sample_record)
                        .unwrap_or(Value::null());
                    all_cols.push((alias.value.clone(), val.data_type()));
                }
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported projection item".to_string(),
                    ));
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
            for item in projection {
                match item {
                    SelectItem::Wildcard(_) => {
                        values.extend(row.values().iter().cloned());
                    }
                    SelectItem::UnnamedExpr(expr) | SelectItem::ExprWithAlias { expr, .. } => {
                        let val = evaluator.evaluate(expr, row)?;
                        values.push(val);
                    }
                    _ => {}
                }
            }
            output_rows.push(Record::from_values(values));
        }

        Ok((output_schema, output_rows))
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
                SelectItem::Wildcard(_) => {
                    for field in input_schema.fields() {
                        all_cols.push((field.name.clone(), field.data_type.clone()));
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
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported projection item".to_string(),
                    ));
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
                    SelectItem::Wildcard(_) => {
                        base_values.extend(row.values().iter().cloned());
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
                    _ => {}
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

                let ordering = self.compare_values(&a_val, &b_val);
                let ordering = if order_expr.options.asc.unwrap_or(true) {
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

        Ok(())
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

        let fields: Vec<Field> = create
            .columns
            .iter()
            .map(|col| {
                let data_type = self.sql_type_to_data_type(&col.data_type)?;
                let nullable = !col
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ast::ColumnOption::NotNull));
                if nullable {
                    Ok(Field::nullable(col.name.value.clone(), data_type))
                } else {
                    Ok(Field::required(col.name.value.clone(), data_type))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        let schema = Schema::from_fields(fields);
        self.catalog.create_table(&table_name, schema)?;

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
        if materialized {
            return Err(Error::UnsupportedFeature(
                "MATERIALIZED VIEW not yet supported".to_string(),
            ));
        }

        let view_name = name.to_string();
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

        let body = match &create.function_body {
            Some(CreateFunctionBody::AsBeforeOptions(expr)) => expr.clone(),
            Some(CreateFunctionBody::AsAfterOptions(expr)) => expr.clone(),
            _ => {
                return Err(Error::UnsupportedFeature(
                    "Only SQL UDFs with AS (expr) are supported".to_string(),
                ));
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

        let args = self.extract_call_args(func)?;

        let mut param_values: HashMap<String, Value> = HashMap::new();
        for (i, param) in proc.parameters.iter().enumerate() {
            let param_name = param.name.value.to_uppercase();
            let value = args.get(i).cloned().unwrap_or(Value::null());
            param_values.insert(param_name, value);
        }

        match &proc.body {
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
        }
    }

    fn extract_call_args(&self, func: &ast::Function) -> Result<Vec<Value>> {
        let empty_record = Record::from_values(vec![]);
        let empty_schema = Schema::new();
        let evaluator = Evaluator::with_user_functions(&empty_schema, self.catalog.get_functions());

        match &func.args {
            ast::FunctionArguments::None => Ok(vec![]),
            ast::FunctionArguments::Subquery(_) => Err(Error::UnsupportedFeature(
                "Subquery arguments not supported in CALL".to_string(),
            )),
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(expr) => {
                                args.push(evaluator.evaluate(expr, &empty_record)?);
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
                                args.push(evaluator.evaluate(expr, &empty_record)?);
                            }
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "Invalid argument in CALL".to_string(),
                                ));
                            }
                        },
                    }
                }
                Ok(args)
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
        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::with_user_functions(&schema, &user_functions);

        match &delete.selection {
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
                    let table_data = self
                        .catalog
                        .get_table_mut(&table_name)
                        .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

                    let field = Field::nullable(col_name, data_type);
                    TableSchemaOps::add_column(table_data, field, None)?;
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
                    let new_table_name = new_name.to_string();
                    self.catalog.rename_table(&table_name, &new_table_name)?;
                }
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
        match expr {
            Expr::Subquery(query) => {
                let result = self.execute_query(query)?;
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
                let resolved_left = self.resolve_scalar_subqueries(left)?;
                let resolved_right = self.resolve_scalar_subqueries(right)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(resolved_left),
                    op: op.clone(),
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let resolved = self.resolve_scalar_subqueries(inner)?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved),
                })
            }
            Expr::Nested(inner) => {
                let resolved = self.resolve_scalar_subqueries(inner)?;
                Ok(Expr::Nested(Box::new(resolved)))
            }
            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let resolved_expr = self.resolve_scalar_subqueries(expr)?;
                let resolved_low = self.resolve_scalar_subqueries(low)?;
                let resolved_high = self.resolve_scalar_subqueries(high)?;
                Ok(Expr::Between {
                    expr: Box::new(resolved_expr),
                    low: Box::new(resolved_low),
                    high: Box::new(resolved_high),
                    negated: *negated,
                })
            }
            Expr::IsNull(inner) => {
                let resolved = self.resolve_scalar_subqueries(inner)?;
                Ok(Expr::IsNull(Box::new(resolved)))
            }
            Expr::IsNotNull(inner) => {
                let resolved = self.resolve_scalar_subqueries(inner)?;
                Ok(Expr::IsNotNull(Box::new(resolved)))
            }
            _ => Ok(expr.clone()),
        }
    }

    fn value_to_expr(&self, value: &Value) -> Expr {
        let sql_value = match value {
            Value::Null => SqlValue::Null,
            Value::Bool(b) => SqlValue::Boolean(*b),
            Value::Int64(i) => SqlValue::Number(i.to_string(), false),
            Value::Float64(f) => SqlValue::Number(f.to_string(), false),
            Value::String(s) => SqlValue::SingleQuotedString(s.clone()),
            Value::Numeric(n) => SqlValue::Number(n.to_string(), false),
            _ => SqlValue::Null,
        };
        Expr::Value(sql_value.into())
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
            _ => Err(Error::UnsupportedFeature(format!(
                "Expression not supported in this context: {:?}",
                expr
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
                        let coerced_fields: Vec<(String, Value)> = struct_vals
                            .iter()
                            .zip(target_fields.iter())
                            .map(|((_, val), target_field)| {
                                (target_field.name.clone(), val.clone())
                            })
                            .collect();
                        return Ok(Value::struct_val(coerced_fields));
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

        Ok(window_results)
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
                let a_val = evaluator
                    .evaluate(&ob.expr, &rows[a])
                    .unwrap_or(Value::null());
                let b_val = evaluator
                    .evaluate(&ob.expr, &rows[b])
                    .unwrap_or(Value::null());
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
                    for end_pos in 0..partition_size {
                        let running_indices: Vec<usize> = sorted_indices[..=end_pos].to_vec();
                        let agg_result = self.compute_window_aggregate(
                            name,
                            func,
                            rows,
                            &running_indices,
                            evaluator,
                        )?;
                        results.push(agg_result);
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
                    return evaluator.evaluate(expr, row);
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
                SelectItem::Wildcard(_) => {
                    for field in input_schema.fields() {
                        all_cols.push((field.name.clone(), field.data_type.clone()));
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
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Unsupported projection item".to_string(),
                    ));
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
                    SelectItem::Wildcard(_) => {
                        values.extend(row.values().iter().cloned());
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
                    _ => {}
                }
            }
            output_rows.push(Record::from_values(values));
        }

        Ok((output_schema, output_rows))
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
            _ => evaluator.evaluate(expr, record),
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
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
