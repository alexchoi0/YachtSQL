mod expressions;
mod foreign_keys;
mod hash_index;
mod join_predicates;
mod mutations;
mod row_building;
mod types;
mod when_clauses;

use std::cell::RefCell;
use std::collections::HashSet;
use std::rc::Rc;

use debug_print::debug_eprintln;
use expressions::{evaluate_condition_row, evaluate_expr_row};
use foreign_keys::validate_merge_foreign_keys;
use join_predicates::extract_equijoin_predicates;
use mutations::MergeMutations;
use row_building::{build_join_row, extract_source_row};
use types::{MatchResult, MergeContext, MergeReturningRow};
use when_clauses::{
    apply_when_matched_row, apply_when_not_matched_by_source_row, apply_when_not_matched_row,
};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::Schema;
use yachtsql_storage::schema::Field;

use super::ExecutionPlan;
use crate::Table;
use crate::query_executor::returning::{
    ReturningColumnOrigin, ReturningExpressionItem, ReturningSpec, build_returning_batch_from_rows,
    returning_spec_output_schema,
};

#[derive(Debug)]
pub struct MergeExec {
    schema: Schema,
    returning_spec: ReturningSpec,
    target_table: String,
    target_alias: Option<String>,
    source: Rc<dyn ExecutionPlan>,
    source_alias: Option<String>,
    on_condition: Expr,
    when_matched: Vec<crate::optimizer::plan::MergeWhenMatched>,
    when_not_matched: Vec<crate::optimizer::plan::MergeWhenNotMatched>,
    when_not_matched_by_source: Vec<crate::optimizer::plan::MergeWhenNotMatchedBySource>,
    storage: Rc<RefCell<crate::storage::Storage>>,
    fk_enforcer: Rc<crate::query_executor::enforcement::ForeignKeyEnforcer>,
}

impl MergeExec {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        target_table: String,
        target_alias: Option<String>,
        source: Rc<dyn ExecutionPlan>,
        source_alias: Option<String>,
        on_condition: Expr,
        when_matched: Vec<crate::optimizer::plan::MergeWhenMatched>,
        when_not_matched: Vec<crate::optimizer::plan::MergeWhenNotMatched>,
        when_not_matched_by_source: Vec<crate::optimizer::plan::MergeWhenNotMatchedBySource>,
        returning_spec: ReturningSpec,
        storage: Rc<RefCell<crate::storage::Storage>>,
        fk_enforcer: Rc<crate::query_executor::enforcement::ForeignKeyEnforcer>,
    ) -> Result<Self> {
        let schema = if matches!(returning_spec, ReturningSpec::None) {
            Schema::from_fields(vec![Field::required("rows_affected", DataType::Int64)])
        } else {
            let storage_guard = storage.borrow();
            let table = storage_guard
                .get_table(&target_table)
                .ok_or_else(|| Error::table_not_found(target_table.clone()))?;
            returning_spec_output_schema(&returning_spec, table.schema(), Some(source.schema()))?
        };

        Ok(Self {
            schema,
            returning_spec,
            target_table,
            target_alias,
            source,
            source_alias,
            on_condition,
            when_matched,
            when_not_matched,
            when_not_matched_by_source,
            storage,
            fk_enforcer,
        })
    }

    fn compute_merge_mutations(
        &self,
        source_batches: &[Table],
        context: &MergeContext,
        capture_returning: bool,
    ) -> Result<MergeMutations> {
        let mut mutations = MergeMutations::new(capture_returning);
        let mut matched_target_rows = HashSet::new();

        self.process_source_rows(
            source_batches,
            context,
            &mut mutations,
            &mut matched_target_rows,
        )?;
        self.process_unmatched_target_rows(context, &matched_target_rows, &mut mutations)?;

        Ok(mutations)
    }

    fn process_source_rows(
        &self,
        source_batches: &[Table],
        context: &MergeContext,
        mutations: &mut MergeMutations,
        matched_target_rows: &mut HashSet<usize>,
    ) -> Result<()> {
        for batch in source_batches {
            for source_row_idx in 0..batch.num_rows() {
                let source_row = extract_source_row(batch, source_row_idx, context.source_schema)?;

                let match_result = self.find_matching_target_row(&source_row, context)?;

                match match_result {
                    MatchResult::Matched {
                        target_idx,
                        join_row,
                        source_row: matched_source_row,
                    } => {
                        self.check_cardinality_violation(target_idx, matched_target_rows)?;
                        matched_target_rows.insert(target_idx);

                        apply_when_matched_row(
                            &join_row,
                            &matched_source_row,
                            context,
                            target_idx,
                            mutations,
                            &self.when_matched,
                            self.source_alias.as_deref(),
                            self.target_alias.as_deref(),
                            &self.target_table,
                        )?;
                    }
                    MatchResult::NotMatched => {
                        apply_when_not_matched_row(
                            &source_row,
                            context,
                            mutations,
                            &self.when_not_matched,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn process_unmatched_target_rows(
        &self,
        context: &MergeContext,
        matched_target_rows: &HashSet<usize>,
        mutations: &mut MergeMutations,
    ) -> Result<()> {
        for (target_row_idx, target_row) in context.target_snapshot.iter().enumerate() {
            if !matched_target_rows.contains(&target_row_idx) {
                apply_when_not_matched_by_source_row(
                    target_row,
                    context,
                    target_row_idx,
                    mutations,
                    &self.when_not_matched_by_source,
                )?;
            }
        }
        Ok(())
    }

    fn find_matching_target_row(
        &self,
        source_row: &crate::storage::table::Row,
        context: &MergeContext,
    ) -> Result<MatchResult> {
        let mut match_count = 0;
        let mut matched_result = None;

        let candidate_indices: Vec<usize> = if let Some(ref hash_index) = context.hash_index {
            hash_index.lookup(source_row)
        } else {
            (0..context.target_snapshot.len()).collect()
        };

        for target_row_idx in candidate_indices {
            let target_row = &context.target_snapshot[target_row_idx];
            let join_row = build_join_row(
                source_row,
                target_row,
                context.source_schema,
                &context.target_schema,
                self.source_alias.as_deref(),
                self.target_alias.as_deref(),
                &self.target_table,
            )?;

            if evaluate_condition_row(
                &self.on_condition,
                &join_row,
                context.source_schema,
                &context.target_schema,
            )? {
                match_count += 1;
                if match_count == 1 {
                    matched_result = Some(MatchResult::Matched {
                        target_idx: target_row_idx,
                        join_row,
                        source_row: source_row.clone(),
                    });
                } else {
                    return Err(self.create_merge_cardinality_error(match_count));
                }
            }
        }

        Ok(matched_result.unwrap_or(MatchResult::NotMatched))
    }

    fn check_cardinality_violation(
        &self,
        target_idx: usize,
        matched_target_rows: &HashSet<usize>,
    ) -> Result<()> {
        if matched_target_rows.contains(&target_idx) {
            Err(self.create_merge_cardinality_error(2))
        } else {
            Ok(())
        }
    }

    fn create_merge_cardinality_error(&self, actual: usize) -> Error {
        Error::MergeCardinalityViolation { actual }
    }

    fn create_rows_affected_result(count: usize) -> Result<Vec<Table>> {
        use yachtsql_storage::Column;

        let schema = Schema::from_fields(vec![Field::required("rows_affected", DataType::Int64)]);

        let mut column = Column::new(&DataType::Int64, 1);
        column.push(Value::int64(count as i64))?;

        Ok(vec![Table::new(schema, vec![column])?])
    }

    fn is_merge_action_expr(expr: &Expr) -> bool {
        matches!(expr, Expr::Function { name, args, .. } if name.as_str().eq_ignore_ascii_case("merge_action") && args.is_empty())
    }

    fn build_returning_batch_from_expressions(
        &self,
        items: &[ReturningExpressionItem],
        target_schema: &Schema,
        contexts: Vec<MergeReturningRow>,
    ) -> Result<Table> {
        if contexts.is_empty() {
            let fields = items
                .iter()
                .map(|item| {
                    Field::nullable(
                        item.output_name
                            .clone()
                            .unwrap_or_else(|| "?column?".to_string()),
                        item.data_type.clone(),
                    )
                })
                .collect();
            let output_schema = Schema::from_fields(fields);
            return Ok(Table::empty(output_schema));
        }

        let source_schema = self.source.schema();
        let mut rows = Vec::with_capacity(contexts.len());

        for context in contexts {
            let MergeReturningRow {
                target_row,
                source_row,
                action,
            } = context;

            let owned_source_row;
            let source_ref = if let Some(row) = source_row {
                owned_source_row = row;
                &owned_source_row
            } else {
                owned_source_row = crate::storage::table::Row::empty();
                &owned_source_row
            };

            let join_row = build_join_row(
                source_ref,
                &target_row,
                source_schema,
                target_schema,
                self.source_alias.as_deref(),
                self.target_alias.as_deref(),
                &self.target_table,
            )?;

            let mut values = Vec::with_capacity(items.len());
            for item in items {
                let value = if Self::is_merge_action_expr(&item.expr) {
                    Value::string(action.as_str().to_string())
                } else {
                    evaluate_expr_row(&item.expr, &join_row, target_schema)?
                };
                values.push(value);
            }
            rows.push(values);
        }

        let first_row = &rows[0];
        let fields: Vec<Field> = items
            .iter()
            .enumerate()
            .map(|(idx, item)| {
                let name = item
                    .output_name
                    .clone()
                    .unwrap_or_else(|| "?column?".to_string());
                let data_type = first_row
                    .get(idx)
                    .map(|v| v.data_type())
                    .unwrap_or(item.data_type.clone());
                Field::nullable(name, data_type)
            })
            .collect();
        let output_schema = Schema::from_fields(fields);

        Table::from_values(output_schema, rows)
    }

    fn build_returning_batch_from_columns(
        &self,
        target_schema: &Schema,
        source_schema: &Schema,
        contexts: Vec<MergeReturningRow>,
    ) -> Result<Table> {
        let output_schema =
            returning_spec_output_schema(&self.returning_spec, target_schema, Some(source_schema))?;

        let ReturningSpec::Columns(columns) = &self.returning_spec else {
            return Err(Error::internal(
                "Expected column-based RETURNING specification for merge batch".to_string(),
            ));
        };

        if contexts.is_empty() {
            return Ok(Table::empty(output_schema));
        }

        let mut rows = Vec::with_capacity(contexts.len());

        for context in contexts {
            let MergeReturningRow {
                target_row,
                source_row,
                action,
            } = context;

            debug_eprintln!(
                "[executor::merge] build_returning: target_row values: {:?}",
                target_row.values()
            );
            debug_eprintln!(
                "[executor::merge] build_returning: target_schema fields: {:?}",
                target_schema
                    .fields()
                    .iter()
                    .map(|f| &f.name)
                    .collect::<Vec<_>>()
            );

            let mut values = Vec::with_capacity(columns.len());
            for column in columns {
                debug_eprintln!(
                    "[executor::merge] Looking for column '{}' with origin {:?}",
                    column.source_name,
                    column.origin
                );
                let value = match column.origin {
                    ReturningColumnOrigin::Target => {
                        let val = target_row
                            .get_by_name(target_schema, &column.source_name)
                            .cloned()
                            .or_else(|| {
                                target_schema
                                    .field_index(&column.source_name)
                                    .and_then(|idx| target_row.get(idx).cloned())
                            })
                            .unwrap_or(Value::null());
                        debug_eprintln!("[executor::merge] Got value {:?}", val);
                        val
                    }
                    ReturningColumnOrigin::Source => source_row
                        .as_ref()
                        .and_then(|row| {
                            source_schema
                                .field_index(&column.source_name)
                                .and_then(|idx| row.get(idx).cloned())
                        })
                        .unwrap_or(Value::null()),
                    ReturningColumnOrigin::Table(_) | ReturningColumnOrigin::Expression => {
                        Value::null()
                    }

                    ReturningColumnOrigin::Old => target_row
                        .get_by_name(target_schema, &column.source_name)
                        .cloned()
                        .or_else(|| {
                            target_schema
                                .field_index(&column.source_name)
                                .and_then(|idx| target_row.get(idx).cloned())
                        })
                        .unwrap_or(Value::null()),

                    ReturningColumnOrigin::New => target_row
                        .get_by_name(target_schema, &column.source_name)
                        .cloned()
                        .or_else(|| {
                            target_schema
                                .field_index(&column.source_name)
                                .and_then(|idx| target_row.get(idx).cloned())
                        })
                        .unwrap_or(Value::null()),
                };
                values.push(value);
            }

            rows.push(values);
        }

        Table::from_values(output_schema, rows)
    }
}

impl ExecutionPlan for MergeExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let source_batches = self.source.execute()?;

        let mut storage_lock = self.storage.borrow_mut();

        let capture_returning = !matches!(self.returning_spec, ReturningSpec::None);

        let equijoin_predicates = extract_equijoin_predicates(
            &self.on_condition,
            self.target_alias.as_deref(),
            self.source_alias.as_deref(),
        );

        let mutations = {
            let target_table = storage_lock
                .get_table(&self.target_table)
                .ok_or_else(|| Error::table_not_found(self.target_table.clone()))?;

            let context =
                MergeContext::new(target_table, self.source.schema(), equijoin_predicates)?;
            self.compute_merge_mutations(&source_batches, &context, capture_returning)?
        };

        {
            let target_table = storage_lock
                .get_table(&self.target_table)
                .ok_or_else(|| Error::table_not_found(self.target_table.clone()))?;
            validate_merge_foreign_keys(
                &mutations,
                target_table,
                &storage_lock,
                &self.target_table,
            )?;
        }

        let (rows_affected, returning_rows, target_schema) = {
            let (count, rows) = mutations.apply_to_table(
                &mut storage_lock,
                &self.target_table,
                &self.fk_enforcer,
            )?;
            let target_table = storage_lock
                .get_table(&self.target_table)
                .ok_or_else(|| Error::table_not_found(self.target_table.clone()))?;
            (count, rows, target_table.schema().clone())
        };

        if capture_returning {
            let batch = match &self.returning_spec {
                ReturningSpec::None => Err(Error::internal(
                    "RETURNING specification missing while capture is enabled",
                )),
                ReturningSpec::AllColumns => {
                    let rows: Vec<_> = returning_rows
                        .iter()
                        .map(|ctx| ctx.target_row.values().to_vec())
                        .collect();
                    build_returning_batch_from_rows(&self.returning_spec, &rows, &target_schema)
                }
                ReturningSpec::Columns(_) => self.build_returning_batch_from_columns(
                    &target_schema,
                    self.source.schema(),
                    returning_rows.clone(),
                ),
                ReturningSpec::Expressions(items) => self.build_returning_batch_from_expressions(
                    items,
                    &target_schema,
                    returning_rows.clone(),
                ),
                ReturningSpec::AstItems(_) => {
                    let rows: Vec<_> = returning_rows
                        .iter()
                        .map(|ctx| ctx.target_row.values().to_vec())
                        .collect();
                    let evaluator = crate::query_executor::returning::ReturningExprEvaluator::new(
                        &target_schema,
                    );
                    crate::query_executor::returning::build_returning_batch_with_ast_eval(
                        &self.returning_spec,
                        &rows,
                        &target_schema,
                        &evaluator,
                    )
                }
            }?;
            Ok(vec![batch])
        } else {
            Self::create_rows_affected_result(rows_affected)
        }
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.source.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "Merge: target={} matched_clauses={} not_matched_clauses={}",
            self.target_table,
            self.when_matched.len(),
            self.when_not_matched.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;

    use yachtsql_core::types::DataType;
    use yachtsql_storage::Field;

    use super::*;

    #[test]
    fn test_table_scan() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
        ]);

        let mut storage = yachtsql_storage::Storage::new();
        storage.create_dataset("default".to_string()).unwrap();
        storage
            .create_table("users".to_string(), schema.clone())
            .unwrap();

        let scan: Rc<dyn ExecutionPlan> = Rc::new(super::super::TableScanExec::new(
            schema.clone(),
            "users".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(storage)),
        ));
        let plan = super::super::PhysicalPlan::new(scan);

        assert_eq!(plan.schema().field_count(), 2);

        let batches = plan.execute().unwrap();
        assert_eq!(batches.len(), 1);
        assert!(batches[0].is_empty());
    }

    #[test]
    fn test_projection() {
        let schema = Schema::from_fields(vec![
            Field::nullable("id", DataType::Int64),
            Field::nullable("name", DataType::String),
            Field::nullable("age", DataType::Int64),
        ]);

        let scan: Rc<dyn ExecutionPlan> = Rc::new(super::super::TableScanExec::new(
            schema.clone(),
            "users".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let projected_schema = schema.project(&["name".to_string()]).unwrap();
        let projection = Rc::new(super::super::ProjectionExec::new(
            scan,
            projected_schema,
            vec![1],
        ));

        let plan = super::super::PhysicalPlan::new(projection);

        assert_eq!(plan.schema().field_count(), 1);
        assert_eq!(plan.schema().field("name").unwrap().name, "name");
    }

    #[test]
    fn test_display_tree() {
        let schema = Schema::from_fields(vec![Field::nullable("id", DataType::Int64)]);

        let scan: Rc<dyn ExecutionPlan> = Rc::new(super::super::TableScanExec::new(
            schema.clone(),
            "users".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let limit = Rc::new(super::super::LimitExec::new(scan, 10, 0));
        let plan = super::super::PhysicalPlan::new(limit);

        let tree = plan.display_tree();
        assert!(tree.contains("Limit: 10"));
        assert!(tree.contains("TableScan: users"));
    }
}
