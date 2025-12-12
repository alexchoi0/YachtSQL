use std::collections::HashMap;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::Schema;

use super::expressions::{check_when_condition, create_combined_schema, evaluate_expr_row};
use super::mutations::MergeMutations;
use super::row_building::build_join_schema;
use super::types::MergeContext;
use crate::storage::table::Row;

pub(super) fn apply_when_matched_row(
    join_row: &Row,
    source_row: &Row,
    context: &MergeContext,
    target_row_idx: usize,
    mutations: &mut MergeMutations,
    when_matched: &[crate::optimizer::plan::MergeWhenMatched],
    source_alias: Option<&str>,
    target_alias: Option<&str>,
    target_table: &str,
) -> Result<()> {
    use crate::optimizer::plan::MergeWhenMatched;

    let source_schema = context.source_schema;
    let target_schema = &context.target_schema;

    for clause in when_matched {
        match clause {
            MergeWhenMatched::Update {
                condition,
                assignments,
            } => {
                if !check_when_condition(condition, join_row, source_schema, target_schema)? {
                    continue;
                }

                let source_prefix = source_alias.unwrap_or(target_table);
                let target_prefix = target_alias.unwrap_or(target_table);
                let join_schema =
                    build_join_schema(source_schema, target_schema, source_prefix, target_prefix);

                let mut updates: HashMap<String, Value> = HashMap::new();

                for (col_name, expr) in assignments {
                    let new_value = evaluate_expr_row(expr, join_row, &join_schema)?;
                    updates.insert(col_name.clone(), new_value);
                }

                let mut row_snapshot = context.target_snapshot[target_row_idx].clone();
                for (col_name, value) in &updates {
                    row_snapshot.set_by_name(target_schema, col_name, value.clone())?;
                }

                let mut updated_row = Row::for_schema(target_schema);
                for (col_name, value) in updates {
                    updated_row.set_by_name(target_schema, &col_name, value)?;
                }
                mutations.record_update(target_row_idx, updated_row);

                mutations.push_returning_row(
                    row_snapshot,
                    Some(source_row.clone()),
                    super::types::MergeAction::Update,
                );
                return Ok(());
            }
            MergeWhenMatched::Delete { condition } => {
                if !check_when_condition(condition, join_row, source_schema, target_schema)? {
                    continue;
                }

                mutations.record_delete(target_row_idx);
                mutations.push_returning_row(
                    context.target_snapshot[target_row_idx].clone(),
                    Some(source_row.clone()),
                    super::types::MergeAction::Delete,
                );
                return Ok(());
            }
        }
    }

    Ok(())
}

pub(super) fn apply_when_not_matched_row(
    source_row: &Row,
    context: &MergeContext,
    mutations: &mut MergeMutations,
    when_not_matched: &[crate::optimizer::plan::MergeWhenNotMatched],
) -> Result<()> {
    use crate::optimizer::plan::MergeWhenNotMatched;

    let source_schema = context.source_schema;
    let target_schema = &context.target_schema;

    for clause in when_not_matched {
        match clause {
            MergeWhenNotMatched::Insert {
                condition,
                columns,
                values: value_exprs,
            } => {
                if let Some(cond) = condition {
                    let result = evaluate_expr_row(cond, source_row, source_schema)?;
                    if result != Value::bool_val(true) {
                        continue;
                    }
                }

                let new_row = build_insert_row(
                    columns,
                    value_exprs,
                    source_row,
                    source_schema,
                    target_schema,
                )?;

                mutations.push_returning_row(
                    new_row.clone(),
                    Some(source_row.clone()),
                    super::types::MergeAction::Insert,
                );
                mutations.record_insert(new_row);
                return Ok(());
            }
        }
    }

    Ok(())
}

pub(super) fn apply_when_not_matched_by_source_row(
    target_row: &Row,
    context: &MergeContext,
    target_row_idx: usize,
    mutations: &mut MergeMutations,
    when_not_matched_by_source: &[crate::optimizer::plan::MergeWhenNotMatchedBySource],
) -> Result<()> {
    use crate::optimizer::plan::MergeWhenNotMatchedBySource;

    let target_schema = &context.target_schema;

    for clause in when_not_matched_by_source {
        match clause {
            MergeWhenNotMatchedBySource::Update {
                condition,
                assignments,
            } => {
                if let Some(cond) = condition {
                    let cond_result = evaluate_expr_row(cond, target_row, target_schema)?;
                    if cond_result != Value::bool_val(true) {
                        continue;
                    }
                }

                let mut updates: HashMap<String, Value> = HashMap::new();
                for (col_name, expr) in assignments {
                    let value = evaluate_expr_row(expr, target_row, target_schema)?;
                    updates.insert(col_name.clone(), value);
                }

                let mut row_snapshot = target_row.clone();
                for (col_name, value) in &updates {
                    row_snapshot.set_by_name(target_schema, col_name, value.clone())?;
                }
                mutations.push_returning_row(row_snapshot, None, super::types::MergeAction::Update);

                let mut updated_values = Row::for_schema(target_schema);
                for (col_name, value) in updates {
                    updated_values.set_by_name(target_schema, &col_name, value)?;
                }
                mutations.record_update(target_row_idx, updated_values);
                return Ok(());
            }
            MergeWhenNotMatchedBySource::Delete { condition } => {
                if let Some(cond) = condition {
                    let cond_result = evaluate_expr_row(cond, target_row, target_schema)?;
                    if cond_result != Value::bool_val(true) {
                        continue;
                    }
                }

                mutations.record_delete(target_row_idx);
                mutations.push_returning_row(
                    target_row.clone(),
                    None,
                    super::types::MergeAction::Delete,
                );
                return Ok(());
            }
        }
    }

    Ok(())
}

fn build_insert_row(
    columns: &[String],
    value_exprs: &[Expr],
    source_row: &Row,
    source_schema: &Schema,
    target_schema: &Schema,
) -> Result<Row> {
    let mut new_row = Row::for_schema(target_schema);

    if columns.is_empty() {
        for (idx, expr) in value_exprs.iter().enumerate() {
            if is_default_expression(expr) {
                continue;
            }
            let value = evaluate_expr_row(expr, source_row, source_schema)?;
            if let Some(field) = target_schema.fields().get(idx) {
                new_row.set_by_name(target_schema, &field.name, value)?;
            }
        }
    } else {
        for (col_name, expr) in columns.iter().zip(value_exprs.iter()) {
            if is_default_expression(expr) {
                continue;
            }
            let value = evaluate_expr_row(expr, source_row, source_schema)?;
            new_row.set_by_name(target_schema, col_name, value)?;
        }
    }

    Ok(new_row)
}

fn is_default_expression(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Column {
            name,
            table: None,
        } if name.eq_ignore_ascii_case("default")
    )
}
