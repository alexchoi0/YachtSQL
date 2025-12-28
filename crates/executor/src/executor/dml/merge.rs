use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, MergeClause};
use yachtsql_storage::{Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_merge(
        &mut self,
        target_table: &str,
        source: &PhysicalPlan,
        on: &Expr,
        clauses: &[MergeClause],
    ) -> Result<Table> {
        let source_data = self.execute_plan(source)?;

        let target_data = self
            .catalog
            .get_table(target_table)
            .ok_or_else(|| Error::TableNotFound(target_table.to_string()))?
            .clone();

        let target_schema = target_data.schema().clone();
        let source_schema = source_data.schema().clone();

        let combined_schema = {
            let mut schema = Schema::new();
            for field in target_schema.fields() {
                schema.add_field(field.clone());
            }
            for field in source_schema.fields() {
                schema.add_field(field.clone());
            }
            schema
        };

        let evaluator = IrEvaluator::new(&combined_schema);
        let target_evaluator = IrEvaluator::new(&target_schema);
        let source_evaluator = IrEvaluator::new(&source_schema);

        let target_rows: Vec<Vec<Value>> = target_data
            .rows()?
            .iter()
            .map(|r| r.values().to_vec())
            .collect();
        let source_rows: Vec<Vec<Value>> = source_data
            .rows()?
            .iter()
            .map(|r| r.values().to_vec())
            .collect();

        let mut target_matched: Vec<bool> = vec![false; target_rows.len()];
        let mut source_matched: Vec<bool> = vec![false; source_rows.len()];

        for (target_idx, target_row) in target_rows.iter().enumerate() {
            for (source_idx, source_row) in source_rows.iter().enumerate() {
                let mut combined_values = target_row.clone();
                combined_values.extend(source_row.clone());
                let combined_record = yachtsql_storage::Record::from_values(combined_values);

                let matches = evaluator
                    .evaluate(on, &combined_record)?
                    .as_bool()
                    .unwrap_or(false);

                if matches {
                    target_matched[target_idx] = true;
                    source_matched[source_idx] = true;
                }
            }
        }

        let mut new_rows: Vec<Vec<Value>> = Vec::new();
        let mut rows_to_delete: Vec<usize> = Vec::new();
        let mut rows_to_update: Vec<(usize, Vec<Value>)> = Vec::new();

        self.process_matched_target_rows(
            &target_rows,
            &source_rows,
            &target_matched,
            &target_schema,
            &combined_schema,
            &evaluator,
            &target_evaluator,
            on,
            clauses,
            &mut rows_to_delete,
            &mut rows_to_update,
        )?;

        self.process_unmatched_source_rows(
            &source_rows,
            &source_matched,
            &target_schema,
            &source_evaluator,
            clauses,
            &mut new_rows,
        )?;

        let deleted_indices: std::collections::HashSet<usize> =
            rows_to_delete.into_iter().collect();

        let mut final_table = Table::empty(target_schema.clone());

        for (idx, row) in target_rows.iter().enumerate() {
            if deleted_indices.contains(&idx) {
                continue;
            }
            if let Some((_, updated_row)) = rows_to_update.iter().find(|(i, _)| *i == idx) {
                final_table.push_row(updated_row.clone())?;
            } else {
                final_table.push_row(row.clone())?;
            }
        }

        for row in new_rows {
            final_table.push_row(row)?;
        }

        self.catalog.replace_table(target_table, final_table)?;

        Ok(Table::empty(Schema::new()))
    }

    #[allow(clippy::too_many_arguments)]
    fn process_matched_target_rows(
        &mut self,
        target_rows: &[Vec<Value>],
        source_rows: &[Vec<Value>],
        target_matched: &[bool],
        target_schema: &Schema,
        combined_schema: &Schema,
        evaluator: &IrEvaluator,
        target_evaluator: &IrEvaluator,
        on: &Expr,
        clauses: &[MergeClause],
        rows_to_delete: &mut Vec<usize>,
        rows_to_update: &mut Vec<(usize, Vec<Value>)>,
    ) -> Result<()> {
        for (target_idx, target_row) in target_rows.iter().enumerate() {
            if target_matched[target_idx] {
                self.process_matched_row(
                    target_idx,
                    target_row,
                    source_rows,
                    target_schema,
                    combined_schema,
                    evaluator,
                    on,
                    clauses,
                    rows_to_delete,
                    rows_to_update,
                )?;
            } else {
                self.process_unmatched_by_source_row(
                    target_idx,
                    target_row,
                    target_schema,
                    target_evaluator,
                    clauses,
                    rows_to_delete,
                    rows_to_update,
                )?;
            }
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn process_matched_row(
        &mut self,
        target_idx: usize,
        target_row: &[Value],
        source_rows: &[Vec<Value>],
        target_schema: &Schema,
        combined_schema: &Schema,
        evaluator: &IrEvaluator,
        on: &Expr,
        clauses: &[MergeClause],
        rows_to_delete: &mut Vec<usize>,
        rows_to_update: &mut Vec<(usize, Vec<Value>)>,
    ) -> Result<()> {
        let matching_sources: Vec<&Vec<Value>> = source_rows
            .iter()
            .filter(|source_row| {
                let mut combined_values = target_row.to_vec();
                combined_values.extend((*source_row).clone());
                let combined_record = yachtsql_storage::Record::from_values(combined_values);
                evaluator
                    .evaluate(on, &combined_record)
                    .ok()
                    .and_then(|v| v.as_bool())
                    .unwrap_or(false)
            })
            .collect();

        let mut clause_applied = false;
        for source_row in &matching_sources {
            if clause_applied {
                break;
            }

            let mut combined_values = target_row.to_vec();
            combined_values.extend((*source_row).clone());
            let combined_record = yachtsql_storage::Record::from_values(combined_values);

            for clause in clauses {
                match clause {
                    MergeClause::MatchedUpdate {
                        condition,
                        assignments,
                    } => {
                        let condition_matches = match condition {
                            Some(cond) => {
                                if Self::expr_contains_subquery(cond) {
                                    self.eval_expr_with_subquery(
                                        cond,
                                        combined_schema,
                                        &combined_record,
                                    )?
                                    .as_bool()
                                    .unwrap_or(false)
                                } else {
                                    evaluator
                                        .evaluate(cond, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                            }
                            None => true,
                        };

                        if condition_matches {
                            let mut new_row = target_row.to_vec();
                            for assignment in assignments {
                                if let Some(col_idx) = target_schema.field_index(&assignment.column)
                                {
                                    let new_val =
                                        if Self::expr_contains_subquery(&assignment.value) {
                                            self.eval_expr_with_subquery(
                                                &assignment.value,
                                                combined_schema,
                                                &combined_record,
                                            )?
                                        } else {
                                            evaluator.evaluate(&assignment.value, &combined_record)?
                                        };
                                    new_row[col_idx] = new_val;
                                }
                            }
                            rows_to_update.push((target_idx, new_row));
                            clause_applied = true;
                            break;
                        }
                    }
                    MergeClause::MatchedDelete { condition } => {
                        let condition_matches = match condition {
                            Some(cond) => {
                                if Self::expr_contains_subquery(cond) {
                                    self.eval_expr_with_subquery(
                                        cond,
                                        combined_schema,
                                        &combined_record,
                                    )?
                                    .as_bool()
                                    .unwrap_or(false)
                                } else {
                                    evaluator
                                        .evaluate(cond, &combined_record)?
                                        .as_bool()
                                        .unwrap_or(false)
                                }
                            }
                            None => true,
                        };

                        if condition_matches {
                            rows_to_delete.push(target_idx);
                            clause_applied = true;
                            break;
                        }
                    }
                    MergeClause::NotMatched { .. }
                    | MergeClause::NotMatchedBySource { .. }
                    | MergeClause::NotMatchedBySourceDelete { .. } => {}
                }
            }
        }
        Ok(())
    }

    fn process_unmatched_by_source_row(
        &mut self,
        target_idx: usize,
        target_row: &[Value],
        target_schema: &Schema,
        target_evaluator: &IrEvaluator,
        clauses: &[MergeClause],
        rows_to_delete: &mut Vec<usize>,
        rows_to_update: &mut Vec<(usize, Vec<Value>)>,
    ) -> Result<()> {
        let target_record = yachtsql_storage::Record::from_values(target_row.to_vec());

        for clause in clauses {
            match clause {
                MergeClause::NotMatchedBySource {
                    condition,
                    assignments,
                } => {
                    let condition_matches = match condition {
                        Some(cond) => target_evaluator
                            .evaluate(cond, &target_record)?
                            .as_bool()
                            .unwrap_or(false),
                        None => true,
                    };

                    if condition_matches {
                        let mut new_row = target_row.to_vec();
                        for assignment in assignments {
                            if let Some(col_idx) = target_schema.field_index(&assignment.column) {
                                let new_val =
                                    target_evaluator.evaluate(&assignment.value, &target_record)?;
                                new_row[col_idx] = new_val;
                            }
                        }
                        rows_to_update.push((target_idx, new_row));
                        break;
                    }
                }
                MergeClause::NotMatchedBySourceDelete { condition } => {
                    let condition_matches = match condition {
                        Some(cond) => target_evaluator
                            .evaluate(cond, &target_record)?
                            .as_bool()
                            .unwrap_or(false),
                        None => true,
                    };

                    if condition_matches {
                        rows_to_delete.push(target_idx);
                        break;
                    }
                }
                MergeClause::MatchedUpdate { .. }
                | MergeClause::MatchedDelete { .. }
                | MergeClause::NotMatched { .. } => {}
            }
        }
        Ok(())
    }

    fn process_unmatched_source_rows(
        &mut self,
        source_rows: &[Vec<Value>],
        source_matched: &[bool],
        target_schema: &Schema,
        source_evaluator: &IrEvaluator,
        clauses: &[MergeClause],
        new_rows: &mut Vec<Vec<Value>>,
    ) -> Result<()> {
        for (source_idx, source_row) in source_rows.iter().enumerate() {
            if source_matched[source_idx] {
                continue;
            }

            let source_record = yachtsql_storage::Record::from_values(source_row.clone());

            for clause in clauses {
                match clause {
                    MergeClause::NotMatched {
                        condition,
                        columns,
                        values,
                    } => {
                        let condition_matches = match condition {
                            Some(cond) => source_evaluator
                                .evaluate(cond, &source_record)?
                                .as_bool()
                                .unwrap_or(false),
                            None => true,
                        };

                        if condition_matches {
                            if columns.is_empty() && values.is_empty() {
                                new_rows.push(source_row.clone());
                            } else {
                                let mut new_row: Vec<Value> =
                                    vec![Value::Null; target_schema.field_count()];
                                for (i, col_name) in columns.iter().enumerate() {
                                    if let Some(col_idx) = target_schema.field_index(col_name)
                                        && i < values.len()
                                    {
                                        let val =
                                            source_evaluator.evaluate(&values[i], &source_record)?;
                                        new_row[col_idx] = val;
                                    }
                                }
                                new_rows.push(new_row);
                            }
                            break;
                        }
                    }
                    MergeClause::MatchedUpdate { .. }
                    | MergeClause::MatchedDelete { .. }
                    | MergeClause::NotMatchedBySource { .. }
                    | MergeClause::NotMatchedBySourceDelete { .. } => {}
                }
            }
        }
        Ok(())
    }
}
