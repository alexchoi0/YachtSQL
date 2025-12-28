use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{AlterColumnAction, AlterTableOp, ColumnDef};
use yachtsql_storage::{Field, FieldMode, Schema, Table, TableSchemaOps};

use super::super::PlanExecutor;
use crate::catalog::ColumnDefault;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
        query: Option<&PhysicalPlan>,
    ) -> Result<Table> {
        if self.catalog.get_table(table_name).is_some() {
            if or_replace {
                self.catalog.drop_table(table_name)?;
            } else if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            } else {
                return Err(Error::InvalidQuery(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
        }

        let schema_collation = table_name
            .find('.')
            .map(|dot_pos| &table_name[..dot_pos])
            .and_then(|schema_name| {
                self.catalog
                    .get_schema_option(schema_name, "default_collate")
            });

        if let Some(query_plan) = query {
            let result = self.execute_plan(query_plan)?;
            let schema = if let Some(plan_schema) = query_plan.schema()
                && !plan_schema.fields.is_empty()
            {
                super::super::plan_schema_to_schema(plan_schema)
            } else {
                result.schema().clone()
            };
            let values: Vec<Vec<Value>> = result
                .rows()?
                .into_iter()
                .map(|r| r.values().to_vec())
                .collect();
            let table = Table::from_values(schema.clone(), values)?;
            self.catalog.insert_table(table_name, table)?;
            return Ok(Table::empty(schema));
        }

        let mut schema = Schema::new();
        let mut defaults = Vec::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&col.name, col.data_type.clone(), mode);
            if let Some(ref collation) = col.collation {
                field = field.with_collation(collation);
            } else if col.data_type == DataType::String
                && let Some(ref collation) = schema_collation
            {
                field = field.with_collation(collation);
            }
            schema.add_field(field);
            if let Some(ref default_expr) = col.default_value {
                defaults.push(ColumnDefault {
                    column_name: col.name.clone(),
                    default_expr: default_expr.clone(),
                });
            }
        }

        let table = Table::empty(schema);
        self.catalog.insert_table(table_name, table)?;
        if !defaults.is_empty() {
            self.catalog.set_table_defaults(table_name, defaults);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_tables(
        &mut self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Table> {
        for table_name in table_names {
            if self.catalog.get_table(table_name).is_none() {
                if if_exists {
                    continue;
                }
                return Err(Error::TableNotFound(table_name.to_string()));
            }
            self.catalog.drop_table(table_name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_table(
        &mut self,
        table_name: &str,
        operation: &AlterTableOp,
        if_exists: bool,
    ) -> Result<Table> {
        let table_opt = self.catalog.get_table(table_name);
        if table_opt.is_none() {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        let _table = table_opt.unwrap();

        match operation {
            AlterTableOp::AddColumn {
                column,
                if_not_exists,
            } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

                if *if_not_exists && table.schema().field(&column.name).is_some() {
                    return Ok(Table::empty(Schema::new()));
                }

                let mode = if column.nullable {
                    FieldMode::Nullable
                } else {
                    FieldMode::Required
                };
                let field = Field::new(&column.name, column.data_type.clone(), mode);

                let default_value = match &column.default_value {
                    Some(default_expr) => {
                        let empty_schema = yachtsql_storage::Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema);
                        let empty_record = yachtsql_storage::Record::new();
                        evaluator.evaluate(default_expr, &empty_record).ok()
                    }
                    None => None,
                };

                table.add_column(field, default_value)?;

                if let Some(ref default_expr) = column.default_value {
                    let mut defaults = self
                        .catalog
                        .get_table_defaults(table_name)
                        .cloned()
                        .unwrap_or_default();
                    defaults.push(ColumnDefault {
                        column_name: column.name.clone(),
                        default_expr: default_expr.clone(),
                    });
                    self.catalog.set_table_defaults(table_name, defaults);
                }

                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::DropColumn { name, if_exists } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                if *if_exists && table.schema().field(name).is_none() {
                    return Ok(Table::empty(Schema::new()));
                }
                table.drop_column(name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::RenameColumn { old_name, new_name } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                table.rename_column(old_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::RenameTable { new_name } => {
                self.catalog.rename_table(table_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::SetOptions { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::AlterColumn { name, action } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                match action {
                    AlterColumnAction::SetNotNull => {
                        table.set_column_not_null(name)?;
                    }
                    AlterColumnAction::DropNotNull => {
                        table.set_column_nullable(name)?;
                    }
                    AlterColumnAction::SetDefault { default } => {
                        let empty_schema = yachtsql_storage::Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema);
                        let empty_record = yachtsql_storage::Record::new();
                        let default_value = evaluator.evaluate(default, &empty_record)?;
                        table.set_column_default(name, default_value)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .cloned()
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        defaults.push(ColumnDefault {
                            column_name: name.clone(),
                            default_expr: default.clone(),
                        });
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    AlterColumnAction::DropDefault => {
                        table.drop_column_default(name)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .cloned()
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    AlterColumnAction::SetDataType { data_type } => {
                        table.set_column_data_type(name, data_type.clone())?;
                    }
                    AlterColumnAction::SetOptions { collation } => {
                        if let Some(collate) = collation {
                            table.set_column_collation(name, collate.clone())?;
                        }
                    }
                }
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::AddConstraint { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::DropConstraint { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::DropPrimaryKey => Ok(Table::empty(Schema::new())),
        }
    }

    pub fn execute_truncate(&mut self, table_name: &str) -> Result<Table> {
        let table = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        table.clear();
        Ok(Table::empty(Schema::new()))
    }
}
