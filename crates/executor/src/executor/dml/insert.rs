use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::Expr;
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Schema, Table};

use super::{PlanExecutor, coerce_value};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    fn evaluate_insert_expr(
        &mut self,
        expr: &Expr,
        evaluator: &IrEvaluator,
        record: &yachtsql_storage::Record,
    ) -> Result<Value> {
        match expr {
            Expr::Subquery(logical_plan) | Expr::ScalarSubquery(logical_plan) => {
                let physical_plan = optimize(logical_plan)?;
                let subquery_result = self.execute(&physical_plan)?;
                let rows = subquery_result.to_records()?;
                if rows.len() == 1 && rows[0].values().len() == 1 {
                    Ok(rows[0].values()[0].clone())
                } else if rows.is_empty() {
                    Ok(Value::null())
                } else {
                    Err(Error::InvalidQuery(
                        "Scalar subquery returned more than one row".to_string(),
                    ))
                }
            }
            _ => evaluator.evaluate(expr, record),
        }
    }

    pub fn execute_insert(
        &mut self,
        table_name: &str,
        columns: &[String],
        source: &PhysicalPlan,
    ) -> Result<Table> {
        let target_schema = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .schema()
            .clone();

        let evaluator = IrEvaluator::new(&target_schema);
        let empty_record = yachtsql_storage::Record::new();

        let mut default_values: Vec<Option<Value>> = vec![None; target_schema.field_count()];
        if let Some(defaults) = self.catalog.get_table_defaults(table_name) {
            for default in defaults {
                if let Some(idx) = target_schema.field_index(&default.column_name)
                    && let Ok(val) = evaluator.evaluate(&default.default_expr, &empty_record)
                {
                    default_values[idx] = Some(val);
                }
            }
        }

        let fields = target_schema.fields().to_vec();

        if let PhysicalPlan::Values { values, .. } = source {
            let empty_schema = yachtsql_storage::Schema::new();
            let values_evaluator = IrEvaluator::new(&empty_schema);
            let empty_rec = yachtsql_storage::Record::from_values(vec![]);

            let mut all_rows: Vec<Vec<Value>> = Vec::new();

            for row_exprs in values {
                if columns.is_empty() {
                    let mut coerced_row = Vec::with_capacity(target_schema.field_count());
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if i < fields.len() {
                            let final_val = match expr {
                                Expr::Default => default_values[i].clone().unwrap_or(Value::Null),
                                _ => {
                                    self.evaluate_insert_expr(expr, &values_evaluator, &empty_rec)?
                                }
                            };
                            coerced_row.push(coerce_value(final_val, &fields[i].data_type)?);
                        } else {
                            coerced_row.push(self.evaluate_insert_expr(
                                expr,
                                &values_evaluator,
                                &empty_rec,
                            )?);
                        }
                    }
                    all_rows.push(coerced_row);
                } else {
                    let mut row: Vec<Value> = default_values
                        .iter()
                        .map(|opt| opt.clone().unwrap_or(Value::Null))
                        .collect();
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(col_idx) = target_schema.field_index(col_name)
                            && i < row_exprs.len()
                            && col_idx < fields.len()
                        {
                            let expr = &row_exprs[i];
                            let final_val = match expr {
                                Expr::Default => {
                                    default_values[col_idx].clone().unwrap_or(Value::Null)
                                }
                                _ => {
                                    self.evaluate_insert_expr(expr, &values_evaluator, &empty_rec)?
                                }
                            };
                            row[col_idx] = coerce_value(final_val, &fields[col_idx].data_type)?;
                        }
                    }
                    all_rows.push(row);
                }
            }

            let target = self
                .catalog
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
            for row in all_rows {
                target.push_row(row)?;
            }

            return Ok(Table::empty(Schema::new()));
        }

        let source_table = self.execute_plan(source)?;

        let target = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        for record in source_table.rows()? {
            if columns.is_empty() {
                let mut coerced_row = Vec::with_capacity(target_schema.field_count());
                for (i, val) in record.values().iter().enumerate() {
                    if i < fields.len() {
                        let final_val = match val {
                            Value::Default => default_values[i].clone().unwrap_or(Value::Null),
                            _ => val.clone(),
                        };
                        coerced_row.push(coerce_value(final_val, &fields[i].data_type)?);
                    } else {
                        coerced_row.push(val.clone());
                    }
                }
                target.push_row(coerced_row)?;
            } else {
                let mut row: Vec<Value> = default_values
                    .iter()
                    .map(|opt| opt.clone().unwrap_or(Value::Null))
                    .collect();
                for (i, col_name) in columns.iter().enumerate() {
                    if let Some(col_idx) = target_schema.field_index(col_name)
                        && i < record.values().len()
                        && col_idx < fields.len()
                    {
                        let val = &record.values()[i];
                        let final_val = match val {
                            Value::Default => {
                                default_values[col_idx].clone().unwrap_or(Value::Null)
                            }
                            _ => val.clone(),
                        };
                        row[col_idx] = coerce_value(final_val, &fields[col_idx].data_type)?;
                    }
                }
                target.push_row(row)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }
}
