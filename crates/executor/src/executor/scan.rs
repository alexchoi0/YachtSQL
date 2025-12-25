use yachtsql_common::error::{Error, Result};
use yachtsql_ir::PlanSchema;
use yachtsql_storage::{Field, FieldMode, Schema, Table};

use super::PlanExecutor;

impl<'a> PlanExecutor<'a> {
    pub fn execute_scan(&self, table_name: &str, planned_schema: &PlanSchema) -> Result<Table> {
        if let Some(cte_table) = self.cte_results.get(table_name) {
            return Ok(self.apply_planned_schema(cte_table, planned_schema));
        }

        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        Ok(self.apply_planned_schema(table, planned_schema))
    }

    fn apply_planned_schema(&self, source_table: &Table, planned_schema: &PlanSchema) -> Table {
        if planned_schema.fields.is_empty() {
            return source_table.clone();
        }

        let mut new_schema = Schema::new();
        for plan_field in &planned_schema.fields {
            let mode = if plan_field.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&plan_field.name, plan_field.data_type.clone(), mode);
            if let Some(ref table) = plan_field.table {
                field = field.with_source_table(table.clone());
            }
            let source_field = source_table
                .schema()
                .fields()
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(&plan_field.name));
            if let Some(src) = source_field
                && let Some(ref collation) = src.collation
            {
                field.collation = Some(collation.clone());
            }
            new_schema.add_field(field);
        }

        source_table.with_schema(new_schema)
    }
}
