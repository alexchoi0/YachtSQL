use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, Literal, LogicalPlan};
use yachtsql_storage::{Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_delete(
        &mut self,
        table_name: &str,
        alias: Option<&str>,
        filter: Option<&Expr>,
    ) -> Result<Table> {
        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| yachtsql_common::error::Error::TableNotFound(table_name.to_string()))?
            .clone();

        let base_schema = table.schema().clone();
        let has_subquery = filter.map(Self::expr_contains_subquery).unwrap_or(false);
        let source_name = alias.unwrap_or(table_name);

        if has_subquery {
            let mut schema_with_source = Schema::new();
            for field in base_schema.fields() {
                let mut new_field = field.clone();
                if new_field.source_table.is_none() {
                    new_field.source_table = Some(source_name.to_string());
                }
                schema_with_source.add_field(new_field);
            }

            let mut new_table = Table::empty(base_schema.clone());

            for record in table.rows()? {
                let should_delete = match filter {
                    Some(expr) => self
                        .eval_expr_with_subquery(expr, &schema_with_source, &record)?
                        .as_bool()
                        .unwrap_or(false),
                    None => true,
                };

                if !should_delete {
                    new_table.push_row(record.values().to_vec())?;
                }
            }

            self.catalog.replace_table(table_name, new_table)?;
        } else {
            let resolved_filter = match filter {
                Some(expr) => Some(self.resolve_subqueries_in_expr(expr)?),
                None => None,
            };

            let evaluator = IrEvaluator::new(&base_schema);
            let mut new_table = Table::empty(base_schema.clone());

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
        }

        Ok(Table::empty(Schema::new()))
    }

    pub(super) fn resolve_subqueries_in_expr(&mut self, expr: &Expr) -> Result<Expr> {
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

    pub(super) fn execute_logical_plan(&mut self, plan: &LogicalPlan) -> Result<Table> {
        let physical_plan = yachtsql_optimizer::optimize(plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical_plan);
        self.execute_plan(&executor_plan)
    }

    pub(super) fn table_column_to_expr_list(&self, table: &Table) -> Result<Vec<Expr>> {
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

    pub(super) fn value_to_literal_expr(value: &Value) -> Expr {
        let literal = match value {
            Value::Null => Literal::Null,
            Value::Bool(b) => Literal::Bool(*b),
            Value::Int64(n) => Literal::Int64(*n),
            Value::Float64(f) => Literal::Float64(*f),
            Value::String(s) => Literal::String(s.clone()),
            Value::Bytes(b) => Literal::Bytes(b.clone()),
            Value::Numeric(n) => Literal::Numeric(*n),
            Value::BigNumeric(n) => Literal::BigNumeric(*n),
            Value::Json(j) => Literal::Json(j.clone()),
            Value::Date(d) => {
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                Literal::Date(d.signed_duration_since(epoch).num_days() as i32)
            }
            Value::Time(t) => {
                Literal::Time(chrono::Timelike::num_seconds_from_midnight(t) as i64 * 1_000_000_000)
            }
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
}
