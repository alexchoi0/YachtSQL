use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_ir::{PlanSchema, UnnestColumn};
use yachtsql_storage::{Record, Table};

use super::{PlanExecutor, plan_schema_to_schema};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_unnest(
        &mut self,
        input: &PhysicalPlan,
        columns: &[UnnestColumn],
        schema: &PlanSchema,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let input_schema = input_table.schema().clone();
        let evaluator = IrEvaluator::new(&input_schema);

        let result_schema = plan_schema_to_schema(schema);
        let mut result = Table::empty(result_schema);

        let input_rows = input_table.rows()?;

        if input_rows.is_empty() && !columns.is_empty() {
            let empty_record = Record::from_values(vec![]);
            let first_col = &columns[0];
            let array_val = evaluator.evaluate(&first_col.expr, &empty_record)?;

            Self::unnest_array(&array_val, first_col, &[], &mut result)?;
        } else {
            for record in input_rows {
                let base_values = record.values().to_vec();

                if columns.is_empty() {
                    result.push_row(base_values)?;
                    continue;
                }

                let first_col = &columns[0];
                let array_val = evaluator.evaluate(&first_col.expr, &record)?;

                Self::unnest_array(&array_val, first_col, &base_values, &mut result)?;
            }
        }

        Ok(result)
    }

    fn unnest_array(
        array_val: &Value,
        unnest_col: &UnnestColumn,
        base_values: &[Value],
        result: &mut Table,
    ) -> Result<()> {
        match array_val {
            Value::Array(elements) => {
                for (idx, elem) in elements.iter().enumerate() {
                    let mut row = base_values.to_vec();
                    row.push(elem.clone());
                    if unnest_col.with_offset {
                        row.push(Value::Int64(idx as i64));
                    }
                    result.push_row(row)?;
                }
            }
            Value::Null => {}
            _ => {
                return Err(Error::InvalidQuery("UNNEST requires array argument".into()));
            }
        }
        Ok(())
    }
}
