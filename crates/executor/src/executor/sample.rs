use rand::Rng;
use yachtsql_common::error::Result;
use yachtsql_optimizer::SampleType;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_sample(
        &mut self,
        input: &PhysicalPlan,
        sample_type: &SampleType,
        sample_value: i64,
    ) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let rows: Vec<_> = input_table.rows()?;

        let sampled_rows = match sample_type {
            SampleType::Rows => {
                let limit = sample_value.max(0) as usize;
                rows.into_iter().take(limit).collect::<Vec<_>>()
            }
            SampleType::Percent => {
                if sample_value <= 0 {
                    return Ok(Table::empty(schema));
                }
                if sample_value >= 100 {
                    rows
                } else {
                    let rate = sample_value as f64 / 100.0;
                    let mut rng = rand::thread_rng();
                    rows.into_iter()
                        .filter(|_| rng.gen_range(0.0..1.0) < rate)
                        .collect()
                }
            }
        };

        let mut result = Table::empty(schema);
        for row in sampled_rows {
            result.push_row(row.values().to_vec())?;
        }

        Ok(result)
    }
}
