use debug_print::debug_eprintln;
use sqlparser::ast::{
    Expr as SqlExpr, JoinConstraint, JoinOperator, Statement, TableFactor, Value as SqlValue,
};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_storage::{Row, Schema, TableIndexOps};

use super::super::super::QueryExecutor;
use super::super::super::expression_evaluator::ExpressionEvaluator;
use super::super::DdlExecutor;
use super::super::query::QueryExecutorTrait;
use crate::Table;
use crate::query_executor::returning::DmlRowContext;

pub trait DmlDeleteExecutor {
    fn execute_delete(&mut self, stmt: &Statement, _original_sql: &str) -> Result<Table>;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinType {
    Inner,
    Left,
}

impl DmlDeleteExecutor for QueryExecutor {
    fn execute_delete(&mut self, stmt: &Statement, _original_sql: &str) -> Result<Table> {
        let Statement::Delete(delete) = stmt else {
            return Err(Error::InternalError("Not a DELETE statement".to_string()));
        };

        if let Some(ref _using_clause) = delete.using {
            return self.execute_delete_using(delete);
        }

        let has_joins = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                tables.first().is_some_and(|t| !t.joins.is_empty())
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                tables.first().is_some_and(|t| !t.joins.is_empty())
            }
        };

        if has_joins {
            if delete.tables.len() > 1 {
                return self.execute_multi_table_delete(delete);
            } else {
                return self.execute_delete_with_join(delete);
            }
        }

        self.execute_simple_delete(delete)
    }
}

impl QueryExecutor {
    fn execute_simple_delete(&mut self, delete: &sqlparser::ast::Delete) -> Result<Table> {
        let table_name_str = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                if let Some(table_with_joins) = tables.first() {
                    if let sqlparser::ast::TableFactor::Table { name, .. } =
                        &table_with_joins.relation
                    {
                        name.to_string()
                    } else {
                        return Err(Error::UnsupportedFeature(
                            "DELETE only supports simple table references".to_string(),
                        ));
                    }
                } else {
                    return Err(Error::InvalidQuery("DELETE FROM has no tables".to_string()));
                }
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                if let Some(table_with_joins) = tables.first() {
                    if let sqlparser::ast::TableFactor::Table { name, .. } =
                        &table_with_joins.relation
                    {
                        name.to_string()
                    } else {
                        return Err(Error::UnsupportedFeature(
                            "DELETE only supports simple table references".to_string(),
                        ));
                    }
                } else {
                    return Err(Error::InvalidQuery("DELETE has no tables".to_string()));
                }
            }
        };

        let returning_spec = if let Some(ref ret) = delete.returning {
            let temp_schema = {
                let storage = self.storage.borrow_mut();
                let (ds_id, tbl_id) = self.parse_ddl_table_name(&table_name_str)?;
                let dataset = storage.get_dataset(&ds_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", ds_id))
                })?;
                if let Some(table) = dataset.get_table(&tbl_id) {
                    table.schema().clone()
                } else {
                    return Err(Error::TableNotFound(format!(
                        "Table '{}.{}' not found",
                        ds_id, tbl_id
                    )));
                }
            };
            self.parse_returning_clause(ret, &temp_schema)?
        } else {
            crate::query_executor::returning::ReturningSpec::None
        };

        let capture_returning = !matches!(
            returning_spec,
            crate::query_executor::returning::ReturningSpec::None
        );

        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name_str)?;

        let is_view = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            dataset.views().exists(&table_id)
        };

        let (schema, all_rows) = if is_view {
            let view_sql = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(&dataset_id).unwrap();
                dataset.views().get_view(&table_id).unwrap().sql.clone()
            };
            let view_result = self.execute_sql(&view_sql)?;
            let view_schema = view_result.schema().clone();
            let view_rows = view_result.rows().map(|r| r.to_vec()).unwrap_or_default();
            (view_schema, view_rows)
        } else {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;
            (table.schema().clone(), table.get_all_rows())
        };

        let (mut rows_to_keep, mut deleted_rows) = if let Some(ref where_expr) = delete.selection {
            let processed_where = self.preprocess_subqueries_in_expr(where_expr)?;

            let evaluator = ExpressionEvaluator::new(&schema);
            let mut keep = Vec::new();
            let mut deleted = Vec::new();

            for row in all_rows {
                if evaluator
                    .evaluate_where(&processed_where, &row)
                    .unwrap_or(false)
                {
                    deleted.push(row);
                } else {
                    keep.push(row);
                }
            }
            (keep, deleted)
        } else {
            (Vec::new(), all_rows)
        };

        if !delete.order_by.is_empty() {
            self.sort_rows(&mut deleted_rows, &schema, &delete.order_by)?;
        }

        if let Some(ref limit_expr) = delete.limit {
            let limit_value = self.extract_limit_offset_value(limit_expr)?;
            if deleted_rows.len() > limit_value {
                let keep_deleted = deleted_rows.split_off(limit_value);
                rows_to_keep.extend(keep_deleted);
            }
        }

        let mut instead_of_count = 0;
        let mut instead_of_deletes: Vec<Row> = Vec::new();
        let mut normal_deletes: Vec<Row> = Vec::new();

        for deleted_row in deleted_rows {
            let instead_of_result = self.fire_instead_of_delete_triggers(
                &dataset_id,
                &table_id,
                &deleted_row,
                &schema,
            )?;

            if instead_of_result.triggers_fired > 0 {
                instead_of_count += 1;
                instead_of_deletes.push(deleted_row);
            } else {
                normal_deletes.push(deleted_row);
            }
        }

        if instead_of_count > 0 && normal_deletes.is_empty() {
            debug_eprintln!(
                "[executor::dml::delete] Deleted {} rows via INSTEAD OF triggers",
                instead_of_count
            );
            return Self::empty_result();
        }

        let mut final_deletes: Vec<Row> = Vec::new();
        let mut returning_contexts: Vec<DmlRowContext> = Vec::new();
        for deleted_row in normal_deletes {
            let before_result =
                self.fire_before_delete_triggers(&dataset_id, &table_id, &deleted_row, &schema)?;
            if !before_result.skip_operation {
                if capture_returning {
                    returning_contexts
                        .push(DmlRowContext::for_delete(deleted_row.values().to_vec()));
                }
                final_deletes.push(deleted_row);
            }
        }

        let deleted_count = final_deletes.len();

        {
            let mut storage = self.storage.borrow_mut();
            let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
            let table_full_name = format!("{}.{}", dataset_id, table_id);

            for deleted_row in &final_deletes {
                enforcer.cascade_delete(&table_full_name, deleted_row, &mut storage)?;
            }
        }

        self.replace_table_with_rows(&dataset_id, &table_id, &schema, rows_to_keep)?;

        for deleted_row in final_deletes {
            self.fire_after_delete_triggers(&dataset_id, &table_id, &deleted_row, &schema)?;
        }

        debug_eprintln!(
            "[executor::dml::delete] Deleted {} rows (INSTEAD OF: {}, normal: {})",
            instead_of_count + deleted_count,
            instead_of_count,
            deleted_count
        );

        if capture_returning {
            crate::query_executor::returning::build_returning_batch_with_old_new(
                &returning_spec,
                &returning_contexts,
                &schema,
            )
        } else {
            Self::empty_result()
        }
    }

    fn build_returning_result(
        &self,
        returning_spec: &crate::query_executor::returning::ReturningSpec,
        rows: &[Row],
        schema: &Schema,
    ) -> Result<Table> {
        let contexts: Vec<DmlRowContext> = rows
            .iter()
            .map(|row| DmlRowContext::for_delete(row.values().to_vec()))
            .collect();

        crate::query_executor::returning::build_returning_batch_with_old_new(
            returning_spec,
            &contexts,
            schema,
        )
    }

    fn execute_delete_with_join(&mut self, delete: &sqlparser::ast::Delete) -> Result<Table> {
        let table_with_joins = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                tables.first().ok_or_else(|| {
                    Error::InvalidQuery("DELETE with JOIN requires FROM clause".to_string())
                })?
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                tables.first().ok_or_else(|| {
                    Error::InvalidQuery("DELETE with JOIN requires FROM clause".to_string())
                })?
            }
        };

        let delete_target_alias = if !delete.tables.is_empty() {
            delete.tables[0].to_string()
        } else {
            self.extract_table_alias(&table_with_joins.relation)?
        };

        if table_with_joins.joins.len() != 1 {
            return Err(Error::UnsupportedFeature(
                "Only single JOIN supported in DELETE (no multi-join yet)".to_string(),
            ));
        }

        let left_table_name = self.extract_table_name(&table_with_joins.relation)?;
        let (left_dataset, left_table_orig) = self.parse_ddl_table_name(&left_table_name)?;
        let left_table = self.resolve_table_name(&left_dataset, &left_table_orig);
        let left_table_alias = self.extract_table_alias(&table_with_joins.relation)?;

        let left_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&left_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", left_dataset))
            })?;
            let table = dataset.get_table(&left_table).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", left_dataset, left_table))
            })?;
            table.schema().clone()
        };

        let left_rows = self.scan_table(&left_dataset, &left_table)?;

        let join = &table_with_joins.joins[0];
        let right_table_name = self.extract_table_name(&join.relation)?;
        let (right_dataset, right_table_orig) = self.parse_ddl_table_name(&right_table_name)?;
        let right_table = self.resolve_table_name(&right_dataset, &right_table_orig);
        let right_table_alias = self.extract_table_alias(&join.relation)?;

        let right_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&right_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", right_dataset))
            })?;
            let table = dataset.get_table(&right_table).ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    right_dataset, right_table
                ))
            })?;
            table.schema().clone()
        };

        let right_rows = self.scan_table(&right_dataset, &right_table)?;

        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
            other => {
                return Err(Error::UnsupportedFeature(format!(
                    "Only INNER JOIN and LEFT JOIN supported in DELETE, got: {:?}",
                    other
                )));
            }
        };

        let join_condition = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::Join(JoinConstraint::On(expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(expr))
            | JoinOperator::Left(JoinConstraint::On(expr)) => expr,
            _ => {
                return Err(Error::UnsupportedFeature(
                    "Only ON clause supported for JOIN in DELETE (no USING/NATURAL)".to_string(),
                ));
            }
        };

        let (joined_schema, joined_rows) = self.nested_loop_join_for_delete(
            &left_schema,
            left_rows.clone(),
            &right_schema,
            right_rows,
            join_type,
            join_condition,
            &left_table_alias,
            &right_table_alias,
        )?;

        let filtered_joined_rows = if let Some(ref where_expr) = delete.selection {
            let evaluator = ExpressionEvaluator::new_for_join(
                &joined_schema,
                &left_schema,
                &right_schema,
                &left_table_alias,
                &right_table_alias,
            );
            let total_joined = joined_rows.len();
            let filtered: Vec<Row> = joined_rows
                .into_iter()
                .filter(|row| {
                    let matches = evaluator.evaluate_where(where_expr, row).unwrap_or(false);
                    matches
                })
                .collect();
            filtered
        } else {
            joined_rows
        };

        let (delete_dataset, delete_table, delete_schema) =
            if delete_target_alias == left_table_alias {
                (
                    left_dataset.clone(),
                    left_table.clone(),
                    left_schema.clone(),
                )
            } else if delete_target_alias == right_table_alias {
                (
                    right_dataset.clone(),
                    right_table.clone(),
                    right_schema.clone(),
                )
            } else {
                return Err(Error::InvalidQuery(format!(
                    "DELETE target '{}' not found in FROM clause",
                    delete_target_alias
                )));
            };

        let left_field_count = left_schema.fields().len();
        let rows_to_delete: Vec<Row> = if delete_target_alias == left_table_alias {
            filtered_joined_rows
                .iter()
                .map(|joined_row| {
                    let values = joined_row
                        .values()
                        .iter()
                        .take(left_field_count)
                        .cloned()
                        .collect();
                    Row::from_values(values)
                })
                .collect()
        } else {
            filtered_joined_rows
                .iter()
                .map(|joined_row| {
                    let values = joined_row
                        .values()
                        .iter()
                        .skip(left_field_count)
                        .cloned()
                        .collect();
                    Row::from_values(values)
                })
                .collect()
        };

        let all_target_rows = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&delete_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", delete_dataset))
            })?;
            let table = dataset.get_table(&delete_table).ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    delete_dataset, delete_table
                ))
            })?;
            table.get_all_rows()
        };

        let rows_to_keep: Vec<Row> = all_target_rows
            .into_iter()
            .filter(|row| {
                !rows_to_delete.iter().any(|del_row| {
                    if row.values().len() != del_row.values().len() {
                        return false;
                    }
                    row.values()
                        .iter()
                        .zip(del_row.values().iter())
                        .all(|(v1, v2)| v1 == v2)
                })
            })
            .collect();

        let deleted_count = rows_to_delete.len();

        {
            let mut storage = self.storage.borrow_mut();
            let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
            let table_full_name = format!("{}.{}", delete_dataset, delete_table);

            for deleted_row in &rows_to_delete {
                enforcer.cascade_delete(&table_full_name, deleted_row, &mut storage)?;
            }
        }

        self.replace_table_with_rows(&delete_dataset, &delete_table, &delete_schema, rows_to_keep)?;

        Self::empty_result()
    }

    fn execute_delete_using(&mut self, delete: &sqlparser::ast::Delete) -> Result<Table> {
        let using_tables = delete.using.as_ref().ok_or_else(|| {
            Error::InvalidQuery("USING clause is required for DELETE USING".to_string())
        })?;

        let target_table_name = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                let table_with_joins = tables.first().ok_or_else(|| {
                    Error::InvalidQuery("DELETE USING requires FROM clause".to_string())
                })?;
                self.extract_table_name(&table_with_joins.relation)?
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                let table_with_joins = tables.first().ok_or_else(|| {
                    Error::InvalidQuery("DELETE USING requires FROM clause".to_string())
                })?;
                self.extract_table_name(&table_with_joins.relation)?
            }
        };

        let (target_dataset, target_table_orig) = self.parse_ddl_table_name(&target_table_name)?;
        let target_table = self.resolve_table_name(&target_dataset, &target_table_orig);

        let target_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&target_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", target_dataset))
            })?;
            let table = dataset.get_table(&target_table).ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    target_dataset, target_table
                ))
            })?;
            table.schema().clone()
        };

        let returning_spec = if let Some(ref ret) = delete.returning {
            self.parse_returning_clause(ret, &target_schema)?
        } else {
            crate::query_executor::returning::ReturningSpec::None
        };

        let capture_returning = !matches!(
            returning_spec,
            crate::query_executor::returning::ReturningSpec::None
        );

        let target_rows = self.scan_table(&target_dataset, &target_table)?;

        let mut combined_schema = target_schema.clone();
        let mut combined_rows = target_rows.clone();
        let mut table_names: Vec<String> = vec![target_table_name.clone()];
        let mut table_schemas: Vec<Schema> = vec![target_schema.clone()];

        for using_table_ref in using_tables {
            let (using_schema, using_rows) = self.get_table_data(&using_table_ref.relation)?;

            let using_table_name = self.extract_table_alias(&using_table_ref.relation)?;

            let mut new_combined_fields = combined_schema.fields().to_vec();
            for field in using_schema.fields() {
                new_combined_fields.push(field.clone());
            }
            let new_combined_schema = Schema::from_fields(new_combined_fields);

            let mut new_combined_rows = Vec::new();
            for combined_row in &combined_rows {
                for using_row in &using_rows {
                    let mut values = combined_row.values().to_vec();
                    values.extend_from_slice(using_row.values());
                    new_combined_rows.push(Row::from_values(values));
                }
            }

            combined_schema = new_combined_schema;
            combined_rows = new_combined_rows;
            table_names.push(using_table_name);
            table_schemas.push(using_schema);
        }

        let where_expr = delete
            .selection
            .as_ref()
            .ok_or_else(|| Error::InvalidQuery("DELETE USING requires WHERE clause".to_string()))?;

        let evaluator = ExpressionEvaluator::new_for_multi_table_delete(
            &combined_schema,
            &table_schemas,
            &table_names,
        );
        let matched_combined_rows: Vec<Row> = combined_rows
            .into_iter()
            .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
            .collect();

        let target_field_count = target_schema.fields().len();
        let rows_to_delete: Vec<Row> = matched_combined_rows
            .iter()
            .map(|combined_row| {
                let values = combined_row
                    .values()
                    .iter()
                    .take(target_field_count)
                    .cloned()
                    .collect();
                Row::from_values(values)
            })
            .collect();

        let rows_to_keep: Vec<Row> = target_rows
            .into_iter()
            .filter(|row| {
                !rows_to_delete.iter().any(|del_row| {
                    if row.values().len() != del_row.values().len() {
                        return false;
                    }
                    row.values()
                        .iter()
                        .zip(del_row.values().iter())
                        .all(|(v1, v2)| v1 == v2)
                })
            })
            .collect();

        let deleted_count = rows_to_delete.len();

        {
            let mut storage = self.storage.borrow_mut();
            let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
            let table_full_name = format!("{}.{}", target_dataset, target_table);

            for deleted_row in &rows_to_delete {
                enforcer.cascade_delete(&table_full_name, deleted_row, &mut storage)?;
            }
        }

        self.replace_table_with_rows(&target_dataset, &target_table, &target_schema, rows_to_keep)?;

        debug_eprintln!(
            "[executor::dml::delete] Deleted {} rows via USING",
            deleted_count
        );

        if capture_returning {
            self.build_returning_result(&returning_spec, &rows_to_delete, &target_schema)
        } else {
            Self::empty_result()
        }
    }

    fn execute_multi_table_delete(&mut self, delete: &sqlparser::ast::Delete) -> Result<Table> {
        let table_with_joins = match &delete.from {
            sqlparser::ast::FromTable::WithFromKeyword(tables) => {
                tables.first().ok_or_else(|| {
                    Error::InvalidQuery("Multi-table DELETE requires FROM clause".to_string())
                })?
            }
            sqlparser::ast::FromTable::WithoutKeyword(tables) => {
                tables.first().ok_or_else(|| {
                    Error::InvalidQuery("Multi-table DELETE requires FROM clause".to_string())
                })?
            }
        };

        let delete_targets: Vec<String> = delete.tables.iter().map(|t| t.to_string()).collect();
        if delete_targets.is_empty() {
            return Err(Error::InvalidQuery(
                "Multi-table DELETE requires at least one target table".to_string(),
            ));
        }

        if table_with_joins.joins.len() != 1 {
            return Err(Error::UnsupportedFeature(
                "Only single JOIN supported in multi-table DELETE".to_string(),
            ));
        }

        let left_table_name = self.extract_table_name(&table_with_joins.relation)?;
        let (left_dataset, left_table_orig) = self.parse_ddl_table_name(&left_table_name)?;
        let left_table = self.resolve_table_name(&left_dataset, &left_table_orig);
        let left_table_alias = self.extract_table_alias(&table_with_joins.relation)?;

        let left_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&left_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", left_dataset))
            })?;
            let table = dataset.get_table(&left_table).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", left_dataset, left_table))
            })?;
            table.schema().clone()
        };

        let left_rows = self.scan_table(&left_dataset, &left_table)?;

        let join = &table_with_joins.joins[0];
        let right_table_name = self.extract_table_name(&join.relation)?;
        let (right_dataset, right_table_orig) = self.parse_ddl_table_name(&right_table_name)?;
        let right_table = self.resolve_table_name(&right_dataset, &right_table_orig);
        let right_table_alias = self.extract_table_alias(&join.relation)?;

        let right_schema = {
            let storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset(&right_dataset).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", right_dataset))
            })?;
            let table = dataset.get_table(&right_table).ok_or_else(|| {
                Error::TableNotFound(format!(
                    "Table '{}.{}' not found",
                    right_dataset, right_table
                ))
            })?;
            table.schema().clone()
        };

        let right_rows = self.scan_table(&right_dataset, &right_table)?;

        let join_type = match &join.join_operator {
            JoinOperator::Inner(_) | JoinOperator::Join(_) => JoinType::Inner,
            JoinOperator::LeftOuter(_) | JoinOperator::Left(_) => JoinType::Left,
            other => {
                return Err(Error::UnsupportedFeature(format!(
                    "Only INNER JOIN and LEFT JOIN supported in multi-table DELETE, got: {:?}",
                    other
                )));
            }
        };

        let join_condition = match &join.join_operator {
            JoinOperator::Inner(JoinConstraint::On(expr))
            | JoinOperator::Join(JoinConstraint::On(expr))
            | JoinOperator::LeftOuter(JoinConstraint::On(expr))
            | JoinOperator::Left(JoinConstraint::On(expr)) => expr,
            _ => {
                return Err(Error::UnsupportedFeature(
                    "Only ON clause supported for JOIN in multi-table DELETE".to_string(),
                ));
            }
        };

        let (joined_schema, joined_rows) = self.nested_loop_join_for_delete(
            &left_schema,
            left_rows.clone(),
            &right_schema,
            right_rows.clone(),
            join_type,
            join_condition,
            &left_table_alias,
            &right_table_alias,
        )?;

        let filtered_joined_rows = if let Some(ref where_expr) = delete.selection {
            let evaluator = ExpressionEvaluator::new_for_join(
                &joined_schema,
                &left_schema,
                &right_schema,
                &left_table_alias,
                &right_table_alias,
            );
            joined_rows
                .into_iter()
                .filter(|row| evaluator.evaluate_where(where_expr, row).unwrap_or(false))
                .collect()
        } else {
            joined_rows
        };

        let left_field_count = left_schema.fields().len();

        let delete_from_left =
            delete_targets.contains(&left_table_alias) || delete_targets.contains(&left_table_name);
        let delete_from_right = delete_targets.contains(&right_table_alias)
            || delete_targets.contains(&right_table_name);

        let left_rows_to_delete: Vec<Row> = if delete_from_left {
            filtered_joined_rows
                .iter()
                .map(|joined_row| {
                    let values = joined_row
                        .values()
                        .iter()
                        .take(left_field_count)
                        .cloned()
                        .collect();
                    Row::from_values(values)
                })
                .collect()
        } else {
            Vec::new()
        };

        let right_rows_to_delete: Vec<Row> = if delete_from_right {
            filtered_joined_rows
                .iter()
                .map(|joined_row| {
                    let values = joined_row
                        .values()
                        .iter()
                        .skip(left_field_count)
                        .cloned()
                        .collect();
                    Row::from_values(values)
                })
                .collect()
        } else {
            Vec::new()
        };

        if delete_from_left && !left_rows_to_delete.is_empty() {
            let rows_to_keep: Vec<Row> = left_rows
                .into_iter()
                .filter(|row| {
                    !left_rows_to_delete.iter().any(|del_row| {
                        if row.values().len() != del_row.values().len() {
                            return false;
                        }
                        row.values()
                            .iter()
                            .zip(del_row.values().iter())
                            .all(|(v1, v2)| v1 == v2)
                    })
                })
                .collect();

            {
                let mut storage = self.storage.borrow_mut();
                let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
                let table_full_name = format!("{}.{}", left_dataset, left_table);

                for deleted_row in &left_rows_to_delete {
                    enforcer.cascade_delete(&table_full_name, deleted_row, &mut storage)?;
                }
            }

            self.replace_table_with_rows(&left_dataset, &left_table, &left_schema, rows_to_keep)?;
        }

        if delete_from_right && !right_rows_to_delete.is_empty() {
            let rows_to_keep: Vec<Row> = right_rows
                .into_iter()
                .filter(|row| {
                    !right_rows_to_delete.iter().any(|del_row| {
                        if row.values().len() != del_row.values().len() {
                            return false;
                        }
                        row.values()
                            .iter()
                            .zip(del_row.values().iter())
                            .all(|(v1, v2)| v1 == v2)
                    })
                })
                .collect();

            {
                let mut storage = self.storage.borrow_mut();
                let enforcer = crate::query_executor::enforcement::ForeignKeyEnforcer::new();
                let table_full_name = format!("{}.{}", right_dataset, right_table);

                for deleted_row in &right_rows_to_delete {
                    enforcer.cascade_delete(&table_full_name, deleted_row, &mut storage)?;
                }
            }

            self.replace_table_with_rows(
                &right_dataset,
                &right_table,
                &right_schema,
                rows_to_keep,
            )?;
        }

        let total_deleted = left_rows_to_delete.len() + right_rows_to_delete.len();
        debug_eprintln!(
            "[executor::dml::delete] Multi-table DELETE removed {} rows (left: {}, right: {})",
            total_deleted,
            left_rows_to_delete.len(),
            right_rows_to_delete.len()
        );

        Self::empty_result()
    }

    fn nested_loop_join_for_delete(
        &self,
        left_schema: &Schema,
        left_rows: Vec<Row>,
        right_schema: &Schema,
        right_rows: Vec<Row>,
        join_type: JoinType,
        join_condition: &sqlparser::ast::Expr,
        left_table_name: &str,
        right_table_name: &str,
    ) -> Result<(Schema, Vec<Row>)> {
        let mut combined_fields = Vec::new();

        for field in left_schema.fields() {
            combined_fields.push(field.clone());
        }

        for field in right_schema.fields() {
            combined_fields.push(field.clone());
        }

        let combined_schema = Schema::from_fields(combined_fields);

        let evaluator = ExpressionEvaluator::new_for_join(
            &combined_schema,
            left_schema,
            right_schema,
            left_table_name,
            right_table_name,
        );

        let mut result_rows = Vec::new();

        for left_row in &left_rows {
            let mut matched = false;

            for right_row in &right_rows {
                let mut combined_values = left_row.values().to_vec();
                combined_values.extend_from_slice(right_row.values());
                let combined_row = Row::from_values(combined_values);

                if evaluator
                    .evaluate_where(join_condition, &combined_row)
                    .unwrap_or(false)
                {
                    result_rows.push(combined_row);
                    matched = true;
                }
            }

            if join_type == JoinType::Left && !matched {
                let mut combined_values = left_row.values().to_vec();
                for _ in right_schema.fields() {
                    combined_values.push(yachtsql_core::types::Value::null());
                }
                result_rows.push(Row::from_values(combined_values));
            }
        }

        Ok((combined_schema, result_rows))
    }

    fn cartesian_product_two_tables(
        &self,
        left_schema: &Schema,
        left_rows: Vec<Row>,
        right_schema: &Schema,
        right_rows: Vec<Row>,
        _left_table_name: &str,
        _right_table_name: &str,
    ) -> Result<(Schema, Vec<Row>)> {
        let mut combined_fields = Vec::new();
        for field in left_schema.fields() {
            combined_fields.push(field.clone());
        }
        for field in right_schema.fields() {
            combined_fields.push(field.clone());
        }
        let combined_schema = Schema::from_fields(combined_fields);

        let mut result_rows = Vec::new();
        for left_row in &left_rows {
            for right_row in &right_rows {
                let mut combined_values = left_row.values().to_vec();
                combined_values.extend_from_slice(right_row.values());
                result_rows.push(Row::from_values(combined_values));
            }
        }

        Ok((combined_schema, result_rows))
    }

    fn replace_table_with_rows(
        &mut self,
        dataset_id: &str,
        table_id: &str,
        _schema: &yachtsql_storage::Schema,
        remaining_rows: Vec<Row>,
    ) -> Result<()> {
        let table_full_name = format!("{}.{}", dataset_id, table_id);

        let mut tm = self.transaction_manager.borrow_mut();
        if let Some(_txn) = tm.get_active_transaction_mut() {
            drop(tm);

            let original_rows = {
                let storage = self.storage.borrow_mut();
                let dataset = storage.get_dataset(dataset_id).ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
                })?;
                let table = dataset.get_table(table_id).ok_or_else(|| {
                    Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
                })?;
                table.get_all_rows()
            };

            let mut tm = self.transaction_manager.borrow_mut();
            if let Some(txn) = tm.get_active_transaction_mut() {
                for (row_index, original_row) in original_rows.iter().enumerate() {
                    if !remaining_rows.contains(original_row) {
                        txn.track_delete(&table_full_name, row_index);
                    }
                }
            }
        } else {
            drop(tm);

            let mut storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset_mut(dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let table = dataset.get_table_mut(table_id).ok_or_else(|| {
                Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
            })?;

            let original_rows = table.get_all_rows();

            for (row_index, original_row) in original_rows.iter().enumerate() {
                if !remaining_rows.contains(original_row) {
                    table.update_indexes_on_delete(original_row.values(), row_index)?;
                }
            }

            table.clear_rows()?;

            for row in remaining_rows {
                table.insert_row(row)?;
            }
        }

        Ok(())
    }

    fn preprocess_subqueries_in_expr(&mut self, expr: &SqlExpr) -> Result<SqlExpr> {
        match expr {
            SqlExpr::Subquery(query) => {
                let sql = query.to_string();
                let result = self.execute_sql(&sql)?;

                if result.num_rows() == 0 {
                    return Ok(SqlExpr::Value(SqlValue::Null.into()));
                }

                if result.num_rows() > 1 {
                    return Err(Error::InvalidQuery(
                        "Scalar subquery returned more than one row".to_string(),
                    ));
                }

                if result.num_columns() != 1 {
                    return Err(Error::InvalidQuery(format!(
                        "Scalar subquery must return exactly one column, got {}",
                        result.num_columns()
                    )));
                }

                let column = result.column(0).ok_or_else(|| {
                    Error::InternalError("Subquery result has no column".to_string())
                })?;
                let value = column.get(0)?;
                let sql_value = Self::value_to_sql_value(&value)?;
                Ok(SqlExpr::Value(sql_value.into()))
            }

            SqlExpr::InSubquery {
                expr: left_expr,
                subquery,
                negated,
            } => {
                let sql = subquery.to_string();
                let result = self.execute_sql(&sql)?;

                if result.num_columns() != 1 {
                    return Err(Error::InvalidQuery(
                        "IN subquery must return exactly one column".to_string(),
                    ));
                }

                let column = result.column(0).ok_or_else(|| {
                    Error::InternalError("IN subquery result has no column".to_string())
                })?;

                let mut list_values = Vec::new();
                for i in 0..result.num_rows() {
                    let value = column.get(i)?;
                    let sql_value = Self::value_to_sql_value(&value)?;
                    list_values.push(SqlExpr::Value(sql_value.into()));
                }

                let processed_left = self.preprocess_subqueries_in_expr(left_expr)?;

                Ok(SqlExpr::InList {
                    expr: Box::new(processed_left),
                    list: list_values,
                    negated: *negated,
                })
            }

            SqlExpr::Exists { subquery, negated } => {
                let sql = subquery.to_string();
                let result = self.execute_sql(&sql)?;

                let exists = result.num_rows() > 0;
                let bool_result = if *negated { !exists } else { exists };

                Ok(SqlExpr::Value(SqlValue::Boolean(bool_result).into()))
            }

            SqlExpr::BinaryOp { left, op, right } => {
                let processed_left = self.preprocess_subqueries_in_expr(left)?;
                let processed_right = self.preprocess_subqueries_in_expr(right)?;
                Ok(SqlExpr::BinaryOp {
                    left: Box::new(processed_left),
                    op: op.clone(),
                    right: Box::new(processed_right),
                })
            }

            SqlExpr::UnaryOp { op, expr: inner } => {
                let processed = self.preprocess_subqueries_in_expr(inner)?;
                Ok(SqlExpr::UnaryOp {
                    op: op.clone(),
                    expr: Box::new(processed),
                })
            }

            SqlExpr::Nested(inner) => {
                let processed = self.preprocess_subqueries_in_expr(inner)?;
                Ok(SqlExpr::Nested(Box::new(processed)))
            }

            SqlExpr::IsNull(inner) => {
                let processed = self.preprocess_subqueries_in_expr(inner)?;
                Ok(SqlExpr::IsNull(Box::new(processed)))
            }
            SqlExpr::IsNotNull(inner) => {
                let processed = self.preprocess_subqueries_in_expr(inner)?;
                Ok(SqlExpr::IsNotNull(Box::new(processed)))
            }

            SqlExpr::Between {
                expr: inner,
                negated,
                low,
                high,
            } => {
                let processed_expr = self.preprocess_subqueries_in_expr(inner)?;
                let processed_low = self.preprocess_subqueries_in_expr(low)?;
                let processed_high = self.preprocess_subqueries_in_expr(high)?;
                Ok(SqlExpr::Between {
                    expr: Box::new(processed_expr),
                    negated: *negated,
                    low: Box::new(processed_low),
                    high: Box::new(processed_high),
                })
            }

            SqlExpr::InList {
                expr: inner,
                list,
                negated,
            } => {
                let processed_expr = self.preprocess_subqueries_in_expr(inner)?;
                let processed_list: Result<Vec<SqlExpr>> = list
                    .iter()
                    .map(|e| self.preprocess_subqueries_in_expr(e))
                    .collect();
                Ok(SqlExpr::InList {
                    expr: Box::new(processed_expr),
                    list: processed_list?,
                    negated: *negated,
                })
            }

            SqlExpr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } => {
                let processed_operand = operand
                    .as_ref()
                    .map(|e| self.preprocess_subqueries_in_expr(e))
                    .transpose()?
                    .map(Box::new);
                let processed_conditions: Result<Vec<sqlparser::ast::CaseWhen>> = conditions
                    .iter()
                    .map(|cw| {
                        let condition = self.preprocess_subqueries_in_expr(&cw.condition)?;
                        let result = self.preprocess_subqueries_in_expr(&cw.result)?;
                        Ok(sqlparser::ast::CaseWhen { condition, result })
                    })
                    .collect();
                let processed_else = else_result
                    .as_ref()
                    .map(|e| self.preprocess_subqueries_in_expr(e))
                    .transpose()?
                    .map(Box::new);
                Ok(SqlExpr::Case {
                    case_token: case_token.clone(),
                    end_token: end_token.clone(),
                    operand: processed_operand,
                    conditions: processed_conditions?,
                    else_result: processed_else,
                })
            }

            _ => Ok(expr.clone()),
        }
    }

    fn value_to_sql_value(value: &Value) -> Result<SqlValue> {
        if value.is_null() {
            return Ok(SqlValue::Null);
        }

        if let Some(b) = value.as_bool() {
            return Ok(SqlValue::Boolean(b));
        }

        if let Some(i) = value.as_i64() {
            return Ok(SqlValue::Number(i.to_string(), false));
        }

        if let Some(f) = value.as_f64() {
            return Ok(SqlValue::Number(f.to_string(), false));
        }

        if let Some(s) = value.as_str() {
            return Ok(SqlValue::SingleQuotedString(s.to_string()));
        }

        Ok(SqlValue::SingleQuotedString(format!("{:?}", value)))
    }
}
