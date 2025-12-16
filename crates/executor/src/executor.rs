//! Query executor - parses and executes SQL statements.

#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::ptr_arg)]

use std::collections::HashMap;

use sqlparser::ast::{
    self, Expr, LimitClause, ObjectName, OrderBy, OrderByExpr, OrderByKind, Query, Select,
    SelectItem, SetExpr, Statement, TableFactor, TableWithJoins, Value as SqlValue,
};
use sqlparser::dialect::BigQueryDialect;
use sqlparser::parser::Parser;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, StructField, Value};
use yachtsql_storage::{Column, Field, Record, Schema, Table, TableSchemaOps};

use crate::catalog::Catalog;
use crate::evaluator::{Evaluator, parse_byte_string_escapes};

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
            _ => Err(Error::UnsupportedFeature(format!(
                "Statement type not yet supported: {:?}",
                stmt
            ))),
        }
    }

    fn execute_query(&self, query: &Query) -> Result<Table> {
        match query.body.as_ref() {
            SetExpr::Select(select) => {
                self.execute_select(select, &query.order_by, &query.limit_clause)
            }
            SetExpr::Values(values) => self.execute_values(values),
            _ => Err(Error::UnsupportedFeature(format!(
                "Query type not yet supported: {:?}",
                query.body
            ))),
        }
    }

    fn execute_select(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
        limit_clause: &Option<LimitClause>,
    ) -> Result<Table> {
        let (pre_projection_schema, mut rows, do_projection) = if select.from.is_empty() {
            let (schema, rows) = self.evaluate_select_without_from(select)?;
            (schema, rows, false)
        } else {
            self.evaluate_select_with_from_for_ordering(select, order_by)?
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
        let evaluator = Evaluator::new(&empty_schema);

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
        let evaluator = Evaluator::new(&input_schema);

        let mut filtered_rows: Vec<Record> = if let Some(selection) = &select.selection {
            input_rows
                .iter()
                .filter(|row| evaluator.evaluate_to_bool(selection, row).unwrap_or(false))
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

        let (output_schema, output_rows) =
            self.project_rows(&input_schema, &filtered_rows, &select.projection)?;

        Ok((output_schema, output_rows))
    }

    fn evaluate_select_with_from_for_ordering(
        &self,
        select: &Select,
        order_by: &Option<OrderBy>,
    ) -> Result<(Schema, Vec<Record>, bool)> {
        let (input_schema, input_rows) = self.get_from_data(&select.from)?;
        let evaluator = Evaluator::new(&input_schema);

        let mut filtered_rows: Vec<Record> = if let Some(selection) = &select.selection {
            input_rows
                .iter()
                .filter(|row| evaluator.evaluate_to_bool(selection, row).unwrap_or(false))
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
        if from.is_empty() {
            return Err(Error::InvalidQuery("FROM clause is empty".to_string()));
        }

        let first_table = &from[0];
        let (mut schema, mut rows) = self.get_table_factor_data(&first_table.relation)?;

        for join in &first_table.joins {
            let (right_schema, right_rows) = self.get_table_factor_data(&join.relation)?;
            (schema, rows) = self.execute_join(
                &schema,
                &rows,
                &right_schema,
                &right_rows,
                &join.join_operator,
            )?;
        }

        for additional_from in from.iter().skip(1) {
            let (right_schema, right_rows) =
                self.get_table_factor_data(&additional_from.relation)?;
            (schema, rows) = self.execute_cross_join(&schema, &rows, &right_schema, &right_rows)?;

            for join in &additional_from.joins {
                let (join_right_schema, join_right_rows) =
                    self.get_table_factor_data(&join.relation)?;
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
        match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let table_data = self
                    .catalog
                    .get_table(&table_name)
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
                    self.get_table_factor_data(&table_with_joins.relation)?;
                for join in &table_with_joins.joins {
                    let (right_schema, right_rows) = self.get_table_factor_data(&join.relation)?;
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
            ast::JoinOperator::Inner(constraint) => self.execute_inner_join(
                &combined_schema,
                left_schema,
                left_rows,
                right_schema,
                right_rows,
                constraint,
            ),
            ast::JoinOperator::LeftOuter(constraint) => self.execute_left_join(
                &combined_schema,
                left_schema,
                left_rows,
                right_schema,
                right_rows,
                constraint,
            ),
            ast::JoinOperator::RightOuter(constraint) => self.execute_right_join(
                &combined_schema,
                left_schema,
                left_rows,
                right_schema,
                right_rows,
                constraint,
            ),
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
        let evaluator = Evaluator::new(combined_schema);
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
        let evaluator = Evaluator::new(combined_schema);
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
        let evaluator = Evaluator::new(combined_schema);
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
        let evaluator = Evaluator::new(combined_schema);
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
                let name = func.name.to_string().to_uppercase();
                matches!(
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
                        | "BIT_AND"
                        | "BIT_OR"
                        | "BIT_XOR"
                        | "BOOL_AND"
                        | "BOOL_OR"
                        | "EVERY"
                        | "ANY_VALUE"
                )
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

    fn execute_aggregate_query(
        &self,
        input_schema: &Schema,
        rows: &[Record],
        select: &Select,
    ) -> Result<(Schema, Vec<Record>)> {
        let evaluator = Evaluator::new(input_schema);

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
            let having_evaluator = Evaluator::new(&schema);
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
        let evaluator = Evaluator::new(input_schema);

        match expr {
            Expr::Function(func) => {
                let name = func.name.to_string().to_uppercase();
                if self.is_aggregate_function(&name) {
                    return self.compute_aggregate(&name, func, input_schema, group_rows);
                }
                if let Some(row) = group_rows.first() {
                    evaluator.evaluate(expr, row)
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
        let evaluator = Evaluator::new(input_schema);

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
        let evaluator = Evaluator::new(input_schema);

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

    fn sort_rows(&self, schema: &Schema, rows: &mut Vec<Record>, order_by: &OrderBy) -> Result<()> {
        let evaluator = Evaluator::new(schema);

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
            _ => Err(Error::UnsupportedFeature(format!(
                "DROP {:?} not yet supported",
                object_type
            ))),
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
        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::new(&schema);

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
        let table_data = self
            .catalog
            .get_table_mut(&table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.clone()))?;

        let schema = table_data.schema().clone();
        let evaluator = Evaluator::new(&schema);

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
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}
