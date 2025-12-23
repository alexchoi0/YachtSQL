use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{Expr, PlanSchema};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;

impl<'a> PlanExecutor<'a> {
    pub fn execute_values(&mut self, values: &[Vec<Expr>], schema: &PlanSchema) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema.clone());

        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = IrEvaluator::new(&empty_schema);

        for row_exprs in values {
            let mut row = Vec::with_capacity(row_exprs.len());
            for expr in row_exprs {
                let val = self.evaluate_value_expr(expr, &evaluator, &empty_record)?;
                row.push(val);
            }
            result.push_row(row)?;
        }

        Ok(result)
    }

    fn evaluate_value_expr(
        &mut self,
        expr: &Expr,
        evaluator: &IrEvaluator,
        empty_record: &Record,
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
            _ => evaluator.evaluate(expr, empty_record),
        }
    }
}
