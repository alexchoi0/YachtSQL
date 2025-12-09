use std::cell::RefCell;
use std::rc::Rc;

use indexmap::IndexMap;
use sqlparser::ast::{Assignment, Expr as SqlExpr, Ident, MergeClause, Statement};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_storage::{Field, Row, Schema};

use super::super::super::QueryExecutor;
use super::super::super::expression_evaluator::ExpressionEvaluator;
use super::super::DdlExecutor;
use crate::Table;

pub trait DmlMergeExecutor {
    fn execute_merge(
        &mut self,
        stmt: &Statement,
        _original_sql: &str,
        merge_returning: Option<String>,
    ) -> Result<Table>;
}

impl DmlMergeExecutor for QueryExecutor {
    fn execute_merge(
        &mut self,
        stmt: &Statement,
        _original_sql: &str,
        merge_returning: Option<String>,
    ) -> Result<Table> {
        let session_udfs = self.session.udfs_for_parser();
        let plan_builder = yachtsql_parser::LogicalPlanBuilder::new()
            .with_storage(Rc::clone(&self.storage))
            .with_dialect(self.dialect())
            .with_merge_returning(merge_returning)
            .with_udfs(session_udfs);
        let logical_plan = plan_builder.merge_to_plan(stmt)?;

        let optimized = self.optimizer.optimize(logical_plan)?;

        let planner = crate::query_executor::logical_to_physical::LogicalToPhysicalPlanner::new(
            Rc::clone(&self.storage),
        )
        .with_dialect(self.dialect());
        let logical_plan_for_conversion =
            yachtsql_ir::plan::LogicalPlan::new(optimized.root().clone());
        let physical_plan = planner.create_physical_plan(&logical_plan_for_conversion)?;

        let batches = physical_plan.execute()?;

        if batches.is_empty() {
            return Ok(Table::empty(yachtsql_storage::Schema::from_fields(vec![])));
        }

        if batches.len() == 1 {
            return Ok(batches.into_iter().next().unwrap());
        }

        Table::concat(&batches)
    }
}

#[allow(dead_code)]
impl QueryExecutor {
    fn execute_merge_old(&mut self, stmt: &Statement, _original_sql: &str) -> Result<Table> {
        let Statement::Merge {
            table,
            source,
            on,
            clauses,
            ..
        } = stmt
        else {
            return Err(Error::InternalError("Not a MERGE statement".to_string()));
        };

        let (target_table_name, target_alias) = match table {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or(table_name.clone());
                (table_name, alias_name)
            }
            _ => {
                return Err(Error::UnsupportedFeature(
                    "MERGE only supports simple table references as target".to_string(),
                ));
            }
        };

        let (dataset_id, table_id) = self.parse_ddl_table_name(&target_table_name)?;

        let (source_sql, source_alias) = match source {
            sqlparser::ast::TableFactor::Table { name, alias, .. } => {
                let table_name = name.to_string();
                let alias_name = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or(table_name.clone());
                (format!("SELECT * FROM {}", name), alias_name)
            }
            sqlparser::ast::TableFactor::Derived { subquery, .. } => {
                (format!("({})", subquery), "subquery".to_string())
            }
            _ => {
                return Err(Error::UnsupportedFeature(
                    "MERGE source must be a table or subquery".to_string(),
                ));
            }
        };

        let source_result = self.execute_sql(&source_sql)?;
        let source_rows = source_result.rows().unwrap_or_default();
        let source_schema = source_result.schema().clone();

        let target_result = self.execute_sql(&format!("SELECT * FROM {}", target_table_name))?;
        let target_rows_vec = target_result.rows().unwrap_or_default();
        let target_schema = target_result.schema().clone();

        let mut rows_to_insert = Vec::new();
        let mut rows_to_update: Vec<(usize, IndexMap<String, Value>)> = Vec::new();
        let mut rows_to_delete: Vec<usize> = Vec::new();
        let mut matched_target_indices = std::collections::HashSet::new();

        for (src_idx, source_row) in source_rows.iter().enumerate() {
            let mut matched_target_idx = None;

            for (target_idx, target_row) in target_rows_vec.iter().enumerate() {
                let matches = self.evaluate_merge_on_condition(
                    on,
                    &target_alias,
                    &target_schema,
                    target_row.values(),
                    &source_alias,
                    &source_schema,
                    source_row.values(),
                )?;

                if matches {
                    if matched_target_idx.is_some() {
                        return Err(Error::InvalidQuery(
                            "MERGE matched multiple target rows for a single source row"
                                .to_string(),
                        ));
                    }
                    matched_target_idx = Some(target_idx);
                    matched_target_indices.insert(target_idx);
                }
            }

            if let Some(target_idx) = matched_target_idx {
                let target_row = &target_rows_vec[target_idx];
                let action = self.find_merge_matched_action(
                    clauses,
                    &target_alias,
                    &target_schema,
                    target_row.values(),
                    &source_alias,
                    &source_schema,
                    source_row.values(),
                )?;

                match action {
                    MergeAction::Update(assignments) => {
                        let updates = self.evaluate_merge_assignments(
                            &assignments,
                            &target_alias,
                            &target_schema,
                            target_row.values(),
                            &source_alias,
                            &source_schema,
                            source_row.values(),
                        )?;
                        rows_to_update.push((target_idx, updates));
                    }
                    MergeAction::Delete => {
                        rows_to_delete.push(target_idx);
                    }
                    _ => {}
                }
            } else {
                let action = self.find_merge_not_matched_action(
                    clauses,
                    &source_schema,
                    source_row.values(),
                )?;

                if let MergeAction::Insert(columns, values) = action {
                    let row_data = self.evaluate_merge_insert(
                        &target_schema,
                        &columns,
                        &values,
                        &source_schema,
                        source_row.values(),
                    )?;
                    rows_to_insert.push(row_data);
                }
            }
        }

        for (target_idx, target_row) in target_rows_vec.iter().enumerate() {
            if !matched_target_indices.contains(&target_idx) {
                let action = self.find_merge_not_matched_by_source_action(
                    clauses,
                    &target_schema,
                    target_row.values(),
                )?;

                match action {
                    MergeAction::Update(assignments) => {
                        let updates = self.evaluate_merge_assignments(
                            &assignments,
                            &target_alias,
                            &target_schema,
                            target_row.values(),
                            &target_alias,
                            &target_schema,
                            target_row.values(),
                        )?;
                        rows_to_update.push((target_idx, updates));
                    }
                    MergeAction::Delete => {
                        rows_to_delete.push(target_idx);
                    }
                    _ => {}
                }
            }
        }

        let mut total_affected = 0;

        for row_data in &rows_to_insert {
            let values_str = target_schema
                .fields()
                .iter()
                .map(|field| {
                    let value = row_data.get(&field.name).cloned().unwrap_or(Value::null());
                    self.value_to_sql_string(&value)
                })
                .collect::<Vec<_>>()
                .join(", ");

            let insert_sql = format!("INSERT INTO {} VALUES ({})", target_table_name, values_str);
            self.execute_sql(&insert_sql)?;
            total_affected += 1;
        }

        for (target_idx, updates) in &rows_to_update {
            let target_row = &target_rows_vec[*target_idx];

            let where_clause = self.build_row_where_clause(&target_schema, target_row.values())?;

            let set_parts: Vec<String> = updates
                .iter()
                .map(|(col, val)| format!("{} = {}", col, self.value_to_sql_string(val)))
                .collect();
            let set_clause = set_parts.join(", ");

            let update_sql = format!(
                "UPDATE {} SET {} WHERE {}",
                target_table_name, set_clause, where_clause
            );
            self.execute_sql(&update_sql)?;
            total_affected += 1;
        }

        for target_idx in &rows_to_delete {
            let target_row = &target_rows_vec[*target_idx];
            let where_clause = self.build_row_where_clause(&target_schema, target_row.values())?;

            let delete_sql = format!("DELETE FROM {} WHERE {}", target_table_name, where_clause);
            self.execute_sql(&delete_sql)?;
            total_affected += 1;
        }

        use yachtsql_core::types::DataType;
        use yachtsql_storage::{Column, Field};

        let schema = Schema::from_fields(vec![Field::nullable("rows_affected", DataType::Int64)]);

        let mut column = Column::new(&DataType::Int64, 1);
        column.push(Value::int64(total_affected as i64))?;

        Table::new(schema, vec![column])
    }
}

#[derive(Debug)]
enum MergeAction {
    Update(Vec<Assignment>),

    Delete,

    Insert(Vec<String>, Vec<SqlExpr>),

    None,
}

impl QueryExecutor {
    fn rewrite_qualified_columns(expr: &SqlExpr) -> SqlExpr {
        use sqlparser::ast::{BinaryOperator, Ident};

        match expr {
            SqlExpr::CompoundIdentifier(parts) if parts.len() == 2 => {
                let table = parts[0].value.clone();
                let column = parts[1].value.clone();
                SqlExpr::Identifier(Ident::new(format!("{}__{}", table, column)))
            }
            SqlExpr::BinaryOp { left, op, right } => SqlExpr::BinaryOp {
                left: Box::new(Self::rewrite_qualified_columns(left)),
                op: op.clone(),
                right: Box::new(Self::rewrite_qualified_columns(right)),
            },
            SqlExpr::Nested(inner) => {
                SqlExpr::Nested(Box::new(Self::rewrite_qualified_columns(inner)))
            }
            SqlExpr::UnaryOp { op, expr } => SqlExpr::UnaryOp {
                op: op.clone(),
                expr: Box::new(Self::rewrite_qualified_columns(expr)),
            },
            SqlExpr::IsNull(inner) => {
                SqlExpr::IsNull(Box::new(Self::rewrite_qualified_columns(inner)))
            }
            SqlExpr::IsNotNull(inner) => {
                SqlExpr::IsNotNull(Box::new(Self::rewrite_qualified_columns(inner)))
            }

            _ => expr.clone(),
        }
    }

    fn evaluate_merge_on_condition(
        &mut self,
        on_expr: &SqlExpr,
        target_table_name: &str,
        target_schema: &Schema,
        target_row: &[Value],
        source_table_name: &str,
        source_schema: &Schema,
        source_row: &[Value],
    ) -> Result<bool> {
        let mut combined_fields = Vec::new();
        let mut combined_values = Vec::new();

        for (idx, field) in target_schema.fields().iter().enumerate() {
            let mut qualified_field = field.clone();
            qualified_field.name = format!("{}__{}", target_table_name, field.name);
            combined_fields.push(qualified_field);
            combined_values.push(target_row[idx].clone());
        }

        for (idx, field) in source_schema.fields().iter().enumerate() {
            let mut qualified_field = field.clone();
            qualified_field.name = format!("{}__{}", source_table_name, field.name);
            combined_fields.push(qualified_field);
            combined_values.push(source_row[idx].clone());

            combined_fields.push(field.clone());
            combined_values.push(source_row[idx].clone());
        }

        let combined_schema = Schema::from_fields(combined_fields);
        let combined_row = Row::from_values(combined_values);

        let rewritten_expr = Self::rewrite_qualified_columns(on_expr);

        let evaluator = ExpressionEvaluator::new(&combined_schema);
        let result = evaluator.evaluate_condition_expr(&rewritten_expr, &combined_row)?;

        Ok(result.as_bool().unwrap_or(false))
    }

    fn find_merge_matched_action(
        &mut self,
        clauses: &[MergeClause],
        target_table_name: &str,
        target_schema: &Schema,
        target_row: &[Value],
        source_table_name: &str,
        source_schema: &Schema,
        source_row: &[Value],
    ) -> Result<MergeAction> {
        use sqlparser::ast::{MergeAction as SqlMergeAction, MergeClauseKind};

        for clause in clauses {
            if !matches!(clause.clause_kind, MergeClauseKind::Matched) {
                continue;
            }

            if let Some(ref predicate) = clause.predicate {
                let matches = self.evaluate_merge_on_condition(
                    predicate,
                    target_table_name,
                    target_schema,
                    target_row,
                    source_table_name,
                    source_schema,
                    source_row,
                )?;
                if !matches {
                    continue;
                }
            }

            match &clause.action {
                SqlMergeAction::Update { assignments } => {
                    return Ok(MergeAction::Update(assignments.clone()));
                }
                SqlMergeAction::Delete => {
                    return Ok(MergeAction::Delete);
                }
                _ => {
                    return Err(Error::InvalidQuery(
                        "Invalid action in WHEN MATCHED clause".to_string(),
                    ));
                }
            }
        }

        Ok(MergeAction::None)
    }

    fn find_merge_not_matched_action(
        &mut self,
        clauses: &[MergeClause],
        source_schema: &Schema,
        source_row: &[Value],
    ) -> Result<MergeAction> {
        use sqlparser::ast::{MergeAction as SqlMergeAction, MergeClauseKind};

        for clause in clauses {
            if !matches!(clause.clause_kind, MergeClauseKind::NotMatched) {
                continue;
            }

            if let Some(ref predicate) = clause.predicate {
                let evaluator = ExpressionEvaluator::new(source_schema);
                let row = Row::from_values(source_row.to_vec());
                let result = evaluator.evaluate_condition_expr(predicate, &row)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }

            match &clause.action {
                SqlMergeAction::Insert(insert) => {
                    let columns = insert.columns.iter().map(|c| c.value.clone()).collect();

                    let values = match &insert.kind {
                        sqlparser::ast::MergeInsertKind::Values(vals) => {
                            if vals.rows.is_empty() {
                                return Err(Error::InvalidQuery(
                                    "MERGE INSERT with empty VALUES".to_string(),
                                ));
                            }
                            vals.rows[0].clone()
                        }
                        _ => {
                            return Err(Error::UnsupportedFeature(
                                "MERGE INSERT ROW syntax not yet supported".to_string(),
                            ));
                        }
                    };

                    return Ok(MergeAction::Insert(columns, values));
                }
                _ => {
                    return Err(Error::InvalidQuery(
                        "Invalid action in WHEN NOT MATCHED clause".to_string(),
                    ));
                }
            }
        }

        Ok(MergeAction::None)
    }

    fn find_merge_not_matched_by_source_action(
        &mut self,
        clauses: &[MergeClause],
        target_schema: &Schema,
        target_row: &[Value],
    ) -> Result<MergeAction> {
        use sqlparser::ast::{MergeAction as SqlMergeAction, MergeClauseKind};

        for clause in clauses {
            if !matches!(clause.clause_kind, MergeClauseKind::NotMatchedBySource) {
                continue;
            }

            if let Some(ref predicate) = clause.predicate {
                let evaluator = ExpressionEvaluator::new(target_schema);
                let row = Row::from_values(target_row.to_vec());
                let result = evaluator.evaluate_condition_expr(predicate, &row)?;
                if !result.as_bool().unwrap_or(false) {
                    continue;
                }
            }

            match &clause.action {
                SqlMergeAction::Update { assignments } => {
                    return Ok(MergeAction::Update(assignments.clone()));
                }
                SqlMergeAction::Delete => {
                    return Ok(MergeAction::Delete);
                }
                _ => {
                    return Err(Error::InvalidQuery(
                        "Invalid action in WHEN NOT MATCHED BY SOURCE clause".to_string(),
                    ));
                }
            }
        }

        Ok(MergeAction::None)
    }

    fn evaluate_merge_assignments(
        &mut self,
        assignments: &[Assignment],
        target_table_name: &str,
        target_schema: &Schema,
        target_row: &[Value],
        source_table_name: &str,
        source_schema: &Schema,
        source_row: &[Value],
    ) -> Result<IndexMap<String, Value>> {
        let mut combined_fields = Vec::new();
        let mut combined_values = Vec::new();

        for (idx, field) in target_schema.fields().iter().enumerate() {
            let mut qualified_field = field.clone();
            qualified_field.name = format!("{}__{}", target_table_name, field.name);
            combined_fields.push(qualified_field);
            combined_values.push(target_row[idx].clone());
        }

        for (idx, field) in source_schema.fields().iter().enumerate() {
            let mut qualified_field = field.clone();
            qualified_field.name = format!("{}__{}", source_table_name, field.name);
            combined_fields.push(qualified_field);
            combined_values.push(source_row[idx].clone());

            combined_fields.push(field.clone());
            combined_values.push(source_row[idx].clone());
        }

        let combined_schema = Schema::from_fields(combined_fields);
        let combined_row = Row::from_values(combined_values);
        let evaluator = ExpressionEvaluator::new(&combined_schema);

        let mut result = IndexMap::new();
        for assignment in assignments {
            let col_name = match &assignment.target {
                sqlparser::ast::AssignmentTarget::ColumnName(name) => name.to_string(),
                _ => {
                    return Err(Error::UnsupportedFeature(
                        "Complex assignment targets not supported".to_string(),
                    ));
                }
            };

            let rewritten_value = Self::rewrite_qualified_columns(&assignment.value);
            let value = evaluator.evaluate_expr(&rewritten_value, &combined_row)?;
            result.insert(col_name, value);
        }

        Ok(result)
    }

    fn evaluate_merge_insert(
        &mut self,
        target_schema: &Schema,
        columns: &[String],
        values: &[SqlExpr],
        source_schema: &Schema,
        source_row: &[Value],
    ) -> Result<IndexMap<String, Value>> {
        let evaluator = ExpressionEvaluator::new(source_schema);
        let row = Row::from_values(source_row.to_vec());

        let mut result = IndexMap::new();

        let column_list: Vec<String> = if columns.is_empty() {
            target_schema
                .fields()
                .iter()
                .map(|f| f.name.clone())
                .collect()
        } else {
            columns.to_vec()
        };

        for (idx, value_expr) in values.iter().enumerate() {
            if idx >= column_list.len() {
                return Err(Error::InvalidQuery(
                    "MERGE INSERT: more values than columns".to_string(),
                ));
            }

            let value = evaluator.evaluate_expr(value_expr, &row)?;
            result.insert(column_list[idx].clone(), value);
        }

        for field in target_schema.fields() {
            if !result.contains_key(&field.name) {
                result.insert(field.name.clone(), Value::null());
            }
        }

        Ok(result)
    }

    fn build_row_where_clause(&mut self, schema: &Schema, row: &[Value]) -> Result<String> {
        let conditions: Vec<String> = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, field)| {
                let value = &row[idx];
                if value.is_null() {
                    format!("{} IS NULL", field.name)
                } else {
                    format!("{} = {}", field.name, self.value_to_sql_string(value))
                }
            })
            .collect();

        Ok(conditions.join(" AND "))
    }

    fn value_to_sql_string(&self, value: &Value) -> String {
        if value.is_null() {
            "NULL".to_string()
        } else if let Some(b) = value.as_bool() {
            b.to_string().to_uppercase()
        } else if let Some(i) = value.as_i64() {
            i.to_string()
        } else if let Some(f) = value.as_f64() {
            f.to_string()
        } else if let Some(s) = value.as_str() {
            format!("'{}'", s.replace('\'', "''"))
        } else {
            format!("'{}'", value)
        }
    }
}
