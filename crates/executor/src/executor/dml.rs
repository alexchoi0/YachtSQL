use chrono::{DateTime, NaiveDate, NaiveTime, Timelike, Utc};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{Assignment, Expr, Literal, LogicalPlan, MergeClause};
use yachtsql_storage::{Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

fn coerce_value(value: Value, target_type: &DataType) -> Result<Value> {
    match (&value, target_type) {
        (Value::String(s), DataType::Date) => {
            let date = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| Error::InvalidQuery(format!("Invalid date string: {}", e)))?;
            Ok(Value::Date(date))
        }
        (Value::String(s), DataType::Time) => {
            let time = NaiveTime::parse_from_str(s, "%H:%M:%S")
                .or_else(|_| NaiveTime::parse_from_str(s, "%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid time string: {}", e)))?;
            Ok(Value::Time(time))
        }
        (Value::String(s), DataType::Timestamp) => {
            let dt = DateTime::parse_from_rfc3339(s)
                .map(|d| d.with_timezone(&Utc))
                .or_else(|_| {
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                        })
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .or_else(|_| {
                            chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
                        })
                        .map(|ndt| ndt.and_utc())
                })
                .map_err(|e| Error::InvalidQuery(format!("Invalid timestamp string: {}", e)))?;
            Ok(Value::Timestamp(dt))
        }
        (Value::String(s), DataType::DateTime) => {
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f"))
                .map_err(|e| Error::InvalidQuery(format!("Invalid datetime string: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        (Value::Int64(n), DataType::Float64) => {
            Ok(Value::Float64(ordered_float::OrderedFloat(*n as f64)))
        }
        (Value::Float64(f), DataType::Int64) => Ok(Value::Int64(f.0 as i64)),
        _ => Ok(value),
    }
}

impl<'a> PlanExecutor<'a> {
    pub fn execute_insert(
        &mut self,
        table_name: &str,
        columns: &[String],
        source: &ExecutorPlan,
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
                if let Some(idx) = target_schema.field_index(&default.column_name) {
                    if let Ok(val) = evaluator.evaluate(&default.default_expr, &empty_record) {
                        default_values[idx] = Some(val);
                    }
                }
            }
        }

        let fields = target_schema.fields().to_vec();

        if let ExecutorPlan::Values { values, .. } = source {
            let target = self
                .catalog
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            let empty_schema = yachtsql_storage::Schema::new();
            let values_evaluator = IrEvaluator::new(&empty_schema);
            let empty_rec = yachtsql_storage::Record::from_values(vec![]);

            for row_exprs in values {
                if columns.is_empty() {
                    let mut coerced_row = Vec::with_capacity(target_schema.field_count());
                    for (i, expr) in row_exprs.iter().enumerate() {
                        if i < fields.len() {
                            let final_val = match expr {
                                Expr::Default => default_values[i].clone().unwrap_or(Value::Null),
                                _ => values_evaluator.evaluate(expr, &empty_rec)?,
                            };
                            coerced_row.push(coerce_value(final_val, &fields[i].data_type)?);
                        } else {
                            coerced_row.push(values_evaluator.evaluate(expr, &empty_rec)?);
                        }
                    }
                    target.push_row(coerced_row)?;
                } else {
                    let mut row: Vec<Value> = default_values
                        .iter()
                        .map(|opt| opt.clone().unwrap_or(Value::Null))
                        .collect();
                    for (i, col_name) in columns.iter().enumerate() {
                        if let Some(col_idx) = target_schema.field_index(col_name) {
                            if i < row_exprs.len() && col_idx < fields.len() {
                                let expr = &row_exprs[i];
                                let final_val = match expr {
                                    Expr::Default => {
                                        default_values[col_idx].clone().unwrap_or(Value::Null)
                                    }
                                    _ => values_evaluator.evaluate(expr, &empty_rec)?,
                                };
                                row[col_idx] = coerce_value(final_val, &fields[col_idx].data_type)?;
                            }
                        }
                    }
                    target.push_row(row)?;
                }
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
                    if let Some(col_idx) = target_schema.field_index(col_name) {
                        if i < record.values().len() && col_idx < fields.len() {
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
                }
                target.push_row(row)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_update(
        &mut self,
        table_name: &str,
        assignments: &[Assignment],
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();

        let schema = table.schema().clone();
        let evaluator = IrEvaluator::new(&schema);

        let mut new_table = Table::empty(schema.clone());

        for record in table.rows()? {
            let should_update = match filter {
                Some(expr) => evaluator
                    .evaluate(expr, &record)?
                    .as_bool()
                    .unwrap_or(false),
                None => true,
            };

            if should_update {
                let mut new_row = record.values().to_vec();
                for assignment in assignments {
                    if let Some(col_idx) = schema.field_index(&assignment.column) {
                        let new_val = evaluator.evaluate(&assignment.value, &record)?;
                        new_row[col_idx] = new_val;
                    }
                }
                new_table.push_row(new_row)?;
            } else {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        self.catalog.replace_table(table_name, new_table)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_delete(&mut self, table_name: &str, filter: Option<&Expr>) -> Result<Table> {
        let resolved_filter = match filter {
            Some(expr) => Some(self.resolve_subqueries_in_expr(expr)?),
            None => None,
        };

        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?
            .clone();

        let schema = table.schema().clone();
        let evaluator = IrEvaluator::new(&schema);

        let mut new_table = Table::empty(schema.clone());

        for record in table.rows()? {
            let should_delete = match &resolved_filter {
                Some(expr) => evaluator
                    .evaluate(expr, &record)?
                    .as_bool()
                    .unwrap_or(false),
                None => true,
            };

            if !should_delete {
                new_table.push_row(record.values().to_vec())?;
            }
        }

        self.catalog.replace_table(table_name, new_table)?;

        Ok(Table::empty(Schema::new()))
    }

    fn resolve_subqueries_in_expr(&mut self, expr: &Expr) -> Result<Expr> {
        match expr {
            Expr::InSubquery {
                expr: inner_expr,
                subquery,
                negated,
            } => {
                let subquery_result = self.execute_logical_plan(subquery)?;
                let values = self.table_column_to_expr_list(&subquery_result)?;
                let resolved_expr = self.resolve_subqueries_in_expr(inner_expr)?;
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
                    op: *op,
                    right: Box::new(resolved_right),
                })
            }
            Expr::UnaryOp { op, expr: inner } => {
                let resolved_expr = self.resolve_subqueries_in_expr(inner)?;
                Ok(Expr::UnaryOp {
                    op: *op,
                    expr: Box::new(resolved_expr),
                })
            }
            _ => Ok(expr.clone()),
        }
    }

    fn execute_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Table> {
        let physical_plan = yachtsql_optimizer::optimize(plan)?;
        let executor_plan = ExecutorPlan::from_physical(&physical_plan);
        self.execute_plan(&executor_plan)
    }

    fn table_column_to_expr_list(&self, table: &Table) -> Result<Vec<Expr>> {
        if table.schema().fields().is_empty() {
            return Ok(vec![]);
        }
        let mut exprs = Vec::with_capacity(table.row_count());
        for record in table.rows()? {
            if let Some(val) = record.values().first() {
                exprs.push(Self::value_to_literal_expr(val));
            }
        }
        Ok(exprs)
    }

    fn value_to_literal_expr(value: &Value) -> Expr {
        let literal = match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(*b),
            Value::Int64(n) => Literal::Int64(*n),
            Value::Float64(f) => Literal::Float64(*f),
            Value::String(s) => Literal::String(s.clone()),
            Value::Bytes(b) => Literal::Bytes(b.clone()),
            Value::Numeric(n) => Literal::Numeric(*n),
            Value::Json(j) => Literal::Json(j.clone()),
            Value::Date(d) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Literal::Date(d.signed_duration_since(epoch).num_days() as i32)
            }
            Value::Time(t) => Literal::Time(t.num_seconds_from_midnight() as i64 * 1_000_000_000),
            Value::DateTime(dt) => Literal::Datetime(dt.and_utc().timestamp_micros()),
            Value::Timestamp(ts) => Literal::Timestamp(ts.timestamp_micros()),
            Value::Interval(i) => Literal::Interval {
                months: i.months,
                days: i.days,
                nanos: i.nanos,
            },
            Value::Array(arr) => {
                let items: Vec<Literal> = arr
                    .iter()
                    .filter_map(|v| match Self::value_to_literal_expr(v) {
                        Expr::Literal(lit) => Some(lit),
                        _ => None,
                    })
                    .collect();
                Literal::Array(items)
            }
            Value::Struct(fields) => {
                let items: Vec<(String, Literal)> = fields
                    .iter()
                    .filter_map(|(name, val)| match Self::value_to_literal_expr(val) {
                        Expr::Literal(lit) => Some((name.clone(), lit)),
                        _ => None,
                    })
                    .collect();
                Literal::Struct(items)
            }
            Value::Geography(_) | Value::Range(_) | Value::Default => Literal::Null,
        };
        Expr::Literal(literal)
    }

    pub fn execute_merge(
        &mut self,
        target_table: &str,
        source: &ExecutorPlan,
        on: &Expr,
        clauses: &[MergeClause],
    ) -> Result<Table> {
        let _source_table = self.execute_plan(source)?;

        Err(Error::UnsupportedFeature(
            "MERGE not yet fully implemented in new executor".into(),
        ))
    }
}
