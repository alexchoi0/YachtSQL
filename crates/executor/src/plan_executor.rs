use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AggregateFunction, Expr, JoinType, LogicalPlan, PlanField, PlanSchema, SetOperationType,
    SortExpr,
};
use yachtsql_storage::{Field, FieldMode, Schema, Table};

use crate::catalog::Catalog;
use crate::ir_evaluator::IrEvaluator;

pub struct PlanExecutor<'a> {
    catalog: &'a mut Catalog,
    variables: HashMap<String, Value>,
    cte_results: HashMap<String, Table>,
}

impl<'a> PlanExecutor<'a> {
    pub fn new(catalog: &'a mut Catalog) -> Self {
        Self {
            catalog,
            variables: HashMap::new(),
            cte_results: HashMap::new(),
        }
    }

    pub fn with_variables(mut self, variables: HashMap<String, Value>) -> Self {
        self.variables = variables;
        self
    }

    pub fn execute(&mut self, plan: &LogicalPlan) -> Result<Table> {
        match plan {
            LogicalPlan::Scan { table_name, .. } => self.execute_scan(table_name),
            LogicalPlan::Filter { input, predicate } => Err(Error::UnsupportedFeature(
                "Filter not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Project {
                input,
                expressions,
                schema,
            } => Err(Error::UnsupportedFeature(
                "Project not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Aggregate { .. } => Err(Error::UnsupportedFeature(
                "Aggregate not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Join { .. } => Err(Error::UnsupportedFeature(
                "Join not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Sort { .. } => Err(Error::UnsupportedFeature(
                "Sort not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Limit {
                input,
                limit,
                offset,
            } => Err(Error::UnsupportedFeature(
                "Limit not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Distinct { .. } => Err(Error::UnsupportedFeature(
                "Distinct not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Values { values, schema } => self.execute_values(values, schema),
            LogicalPlan::Empty { schema } => Ok(Table::empty(plan_schema_to_schema(schema))),
            LogicalPlan::Insert { .. } => Err(Error::UnsupportedFeature(
                "Insert not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Update { .. } => Err(Error::UnsupportedFeature(
                "Update not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Delete { .. } => Err(Error::UnsupportedFeature(
                "Delete not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
            } => self.execute_create_table(table_name, columns, *if_not_exists, *or_replace),
            LogicalPlan::DropTable {
                table_names,
                if_exists,
            } => self.execute_drop_tables(table_names, *if_exists),
            LogicalPlan::AlterTable { .. } => Err(Error::UnsupportedFeature(
                "AlterTable not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Truncate { table_name } => self.execute_truncate(table_name),
            LogicalPlan::Window { .. } => Err(Error::UnsupportedFeature(
                "Window functions not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::WithCte { ctes, body } => Err(Error::UnsupportedFeature(
                "CTEs not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::SetOperation { .. } => Err(Error::UnsupportedFeature(
                "Set operations not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Unnest { .. } => Err(Error::UnsupportedFeature(
                "Unnest not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::CreateView { .. } => Err(Error::UnsupportedFeature(
                "CreateView not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::DropView { .. } => Err(Error::UnsupportedFeature(
                "DropView not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => Ok(Table::empty(Schema::new())),
            LogicalPlan::DropSchema { .. } => Ok(Table::empty(Schema::new())),
            LogicalPlan::CreateFunction { .. } => Err(Error::UnsupportedFeature(
                "CreateFunction not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::DropFunction { .. } => Err(Error::UnsupportedFeature(
                "DropFunction not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Call { .. } => Err(Error::UnsupportedFeature(
                "Call not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Merge { .. } => Err(Error::UnsupportedFeature(
                "Merge not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::ExportData { .. } => Err(Error::UnsupportedFeature(
                "ExportData not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Declare {
                name,
                data_type,
                default,
            } => Err(Error::UnsupportedFeature(
                "Declare not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::SetVariable { .. } => Err(Error::UnsupportedFeature(
                "SetVariable not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::If { .. } => Err(Error::UnsupportedFeature(
                "If not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::While { .. } => Err(Error::UnsupportedFeature(
                "While not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Loop { .. } => Err(Error::UnsupportedFeature(
                "Loop not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::For { .. } => Err(Error::UnsupportedFeature(
                "For not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Return { .. } => {
                Err(Error::InvalidQuery("RETURN outside of function".into()))
            }
            LogicalPlan::Raise { .. } => Err(Error::UnsupportedFeature(
                "Raise not yet implemented in IR plan executor".into(),
            )),
            LogicalPlan::Break => Err(Error::InvalidQuery("BREAK outside of loop".into())),
            LogicalPlan::Continue => Err(Error::InvalidQuery("CONTINUE outside of loop".into())),
        }
    }

    fn execute_scan(&self, table_name: &str) -> Result<Table> {
        if let Some(cte_table) = self.cte_results.get(table_name) {
            return Ok(cte_table.clone());
        }

        let table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        Ok(table.clone())
    }

    fn execute_values(&self, _values: &[Vec<Expr>], schema: &PlanSchema) -> Result<Table> {
        let result_schema = plan_schema_to_schema(schema);
        Ok(Table::empty(result_schema))
    }

    fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[yachtsql_ir::ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
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

        let mut schema = Schema::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            schema.add_field(Field::new(&col.name, col.data_type.clone(), mode));
        }

        let table = Table::empty(schema);
        self.catalog.insert_table(table_name, table)?;

        Ok(Table::empty(Schema::new()))
    }

    fn execute_drop_tables(&mut self, table_names: &[String], if_exists: bool) -> Result<Table> {
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

    fn execute_truncate(&mut self, table_name: &str) -> Result<Table> {
        let table = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        table.clear();
        Ok(Table::empty(Schema::new()))
    }
}

fn plan_schema_to_schema(plan_schema: &PlanSchema) -> Schema {
    let mut schema = Schema::new();
    for field in &plan_schema.fields {
        let mode = if field.nullable {
            FieldMode::Nullable
        } else {
            FieldMode::Required
        };
        schema.add_field(Field::new(&field.name, field.data_type.clone(), mode));
    }
    schema
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execute_create_table() {
        let mut catalog = Catalog::new();
        let mut executor = PlanExecutor::new(&mut catalog);

        let plan = LogicalPlan::CreateTable {
            table_name: "test_table".to_string(),
            columns: vec![
                yachtsql_ir::ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    default_value: None,
                },
                yachtsql_ir::ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::String,
                    nullable: true,
                    default_value: None,
                },
            ],
            if_not_exists: false,
            or_replace: false,
        };

        let result = executor.execute(&plan);
        assert!(result.is_ok());

        assert!(catalog.get_table("test_table").is_some());
        let table = catalog.get_table("test_table").unwrap();
        assert_eq!(table.schema().field_count(), 2);
    }

    #[test]
    fn test_execute_create_table_if_not_exists() {
        let mut catalog = Catalog::new();
        let mut executor = PlanExecutor::new(&mut catalog);

        let plan = LogicalPlan::CreateTable {
            table_name: "test_table".to_string(),
            columns: vec![yachtsql_ir::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default_value: None,
            }],
            if_not_exists: false,
            or_replace: false,
        };
        executor.execute(&plan).unwrap();

        let plan2 = LogicalPlan::CreateTable {
            table_name: "test_table".to_string(),
            columns: vec![yachtsql_ir::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default_value: None,
            }],
            if_not_exists: true,
            or_replace: false,
        };
        let result = executor.execute(&plan2);
        assert!(result.is_ok());
    }

    #[test]
    fn test_execute_drop_table() {
        let mut catalog = Catalog::new();
        let mut executor = PlanExecutor::new(&mut catalog);

        let create_plan = LogicalPlan::CreateTable {
            table_name: "test_table".to_string(),
            columns: vec![yachtsql_ir::ColumnDef {
                name: "id".to_string(),
                data_type: DataType::Int64,
                nullable: false,
                default_value: None,
            }],
            if_not_exists: false,
            or_replace: false,
        };
        executor.execute(&create_plan).unwrap();

        let drop_plan = LogicalPlan::DropTable {
            table_names: vec!["test_table".to_string()],
            if_exists: false,
        };
        let result = executor.execute(&drop_plan);
        assert!(result.is_ok());
        assert!(catalog.get_table("test_table").is_none());
    }

    #[test]
    fn test_execute_scan() {
        let mut catalog = Catalog::new();

        let mut schema = Schema::new();
        schema.add_field(Field::new("id", DataType::Int64, FieldMode::Required));
        let table = Table::empty(schema);
        catalog.insert_table("test_table", table).unwrap();

        let mut executor = PlanExecutor::new(&mut catalog);

        let plan = LogicalPlan::Scan {
            table_name: "test_table".to_string(),
            schema: PlanSchema {
                fields: vec![PlanField {
                    name: "id".to_string(),
                    data_type: DataType::Int64,
                    nullable: false,
                    table: None,
                }],
            },
            projection: None,
        };

        let result = executor.execute(&plan);
        assert!(result.is_ok());
        let table = result.unwrap();
        assert_eq!(table.schema().field_count(), 1);
    }

    #[test]
    fn test_execute_truncate() {
        let mut catalog = Catalog::new();

        let mut schema = Schema::new();
        schema.add_field(Field::new("id", DataType::Int64, FieldMode::Required));
        let mut table = Table::empty(schema);
        table.push_row(vec![Value::Int64(1)]).unwrap();
        table.push_row(vec![Value::Int64(2)]).unwrap();
        catalog.insert_table("test_table", table).unwrap();

        let mut executor = PlanExecutor::new(&mut catalog);

        let plan = LogicalPlan::Truncate {
            table_name: "test_table".to_string(),
        };

        let result = executor.execute(&plan);
        assert!(result.is_ok());

        let table = catalog.get_table("test_table").unwrap();
        assert_eq!(table.row_count(), 0);
    }
}
