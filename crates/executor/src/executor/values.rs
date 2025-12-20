use yachtsql_common::error::Result;
use yachtsql_ir::{Expr, PlanSchema};
use yachtsql_storage::{Record, Schema, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;

impl<'a> PlanExecutor<'a> {
    pub fn execute_values(&self, values: &[Vec<Expr>], schema: &PlanSchema) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema.clone());

        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = IrEvaluator::new(&empty_schema);

        for row_exprs in values {
            let mut row = Vec::with_capacity(row_exprs.len());
            for expr in row_exprs {
                let val = evaluator.evaluate(expr, &empty_record)?;
                row.push(val);
            }
            result.push_row(row)?;
        }

        Ok(result)
    }
}
