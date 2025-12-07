use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::Result;
use yachtsql_storage::Schema;

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct CteExec {
    cte_plan: Rc<dyn ExecutionPlan>,
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    materialized: bool,
    cached_result: RefCell<Option<Vec<Table>>>,
}

impl CteExec {
    pub fn new(
        cte_plan: Rc<dyn ExecutionPlan>,
        input: Rc<dyn ExecutionPlan>,
        materialized: bool,
    ) -> Self {
        let schema = input.schema().clone();

        Self {
            cte_plan,
            input,
            schema,
            materialized,
            cached_result: RefCell::new(None),
        }
    }

    pub fn execute_cte(&self) -> Result<Vec<Table>> {
        if self.materialized {
            let mut cache = self.cached_result.borrow_mut();
            if let Some(ref cached) = *cache {
                return Ok(cached.clone());
            }

            let result = self.cte_plan.execute()?;
            *cache = Some(result.clone());
            Ok(result)
        } else {
            self.cte_plan.execute()
        }
    }
}

impl ExecutionPlan for CteExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let _ = self.execute_cte()?;
        self.input.execute()
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.cte_plan.clone(), self.input.clone()]
    }

    fn describe(&self) -> String {
        if self.materialized {
            "CTE (MATERIALIZED)".to_string()
        } else {
            "CTE".to_string()
        }
    }
}

#[derive(Debug)]
pub struct SubqueryScanExec {
    subquery: Rc<dyn ExecutionPlan>,
    schema: Schema,
}

impl SubqueryScanExec {
    pub fn new(subquery: Rc<dyn ExecutionPlan>) -> Self {
        let schema = subquery.schema().clone();

        Self { subquery, schema }
    }

    pub fn new_with_schema(subquery: Rc<dyn ExecutionPlan>, schema: Schema) -> Self {
        Self { subquery, schema }
    }
}

impl ExecutionPlan for SubqueryScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        self.subquery.execute()
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.subquery.clone()]
    }

    fn describe(&self) -> String {
        "SubqueryScan".to_string()
    }
}

#[derive(Debug)]
pub struct EmptyRelationExec {
    schema: Schema,
}

impl EmptyRelationExec {
    pub fn new(schema: Schema) -> Self {
        Self { schema }
    }
}

impl ExecutionPlan for EmptyRelationExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let one_empty_row = yachtsql_storage::Row::from_values(vec![]);
        Ok(vec![Table::from_rows(
            self.schema.clone(),
            vec![one_empty_row],
        )?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn describe(&self) -> String {
        "EmptyRelation".to_string()
    }
}

#[derive(Debug)]
pub struct MaterializedViewScanExec {
    schema: Schema,
    data: Table,
}

impl MaterializedViewScanExec {
    pub fn new(schema: Schema, data: Table) -> Self {
        Self { schema, data }
    }
}

impl ExecutionPlan for MaterializedViewScanExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        Ok(vec![self.data.to_column_format()?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![]
    }

    fn describe(&self) -> String {
        "MaterializedViewScan".to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

    #[test]
    fn test_cte_exec_materialized() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let cte_plan = Rc::new(TableScanExec::new(
            schema.clone(),
            "cte_table".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let input = Rc::new(TableScanExec::new(
            schema.clone(),
            "main".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let cte_exec = CteExec::new(cte_plan, input, true);
        assert_eq!(cte_exec.describe(), "CTE (MATERIALIZED)");
        assert_eq!(cte_exec.schema().fields().len(), 1);
    }

    #[test]
    fn test_cte_exec_not_materialized() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::String,
            )]);

        let cte_plan = Rc::new(TableScanExec::new(
            schema.clone(),
            "cte_table".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let input = Rc::new(TableScanExec::new(
            schema.clone(),
            "main".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let cte_exec = CteExec::new(cte_plan, input, false);
        assert_eq!(cte_exec.describe(), "CTE");
    }

    #[test]
    fn test_cte_caching() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let mut storage1 = yachtsql_storage::Storage::new();
        storage1.create_dataset("default".to_string()).unwrap();
        storage1
            .create_table("cte_table".to_string(), schema.clone())
            .unwrap();

        let mut storage2 = yachtsql_storage::Storage::new();
        storage2.create_dataset("default".to_string()).unwrap();
        storage2
            .create_table("main".to_string(), schema.clone())
            .unwrap();

        let cte_plan = Rc::new(TableScanExec::new(
            schema.clone(),
            "cte_table".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(storage1)),
        ));
        let input = Rc::new(TableScanExec::new(
            schema.clone(),
            "main".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(storage2)),
        ));

        let cte_exec = CteExec::new(cte_plan, input, true);

        let result1 = cte_exec.execute_cte();
        assert!(result1.is_ok());

        let result2 = cte_exec.execute_cte();
        assert!(result2.is_ok());
    }

    #[test]
    fn test_subquery_scan_exec() {
        let schema = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "name".to_string(),
                yachtsql_core::types::DataType::String,
            ),
        ]);

        let subquery = Rc::new(TableScanExec::new(
            schema.clone(),
            "subquery".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let subquery_scan = SubqueryScanExec::new(subquery);
        assert_eq!(subquery_scan.describe(), "SubqueryScan");
        assert_eq!(subquery_scan.schema().fields().len(), 2);
    }

    #[test]
    fn test_subquery_scan_execution() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let mut storage = yachtsql_storage::Storage::new();
        storage.create_dataset("default".to_string()).unwrap();
        storage
            .create_table("sq".to_string(), schema.clone())
            .unwrap();

        let subquery = Rc::new(TableScanExec::new(
            schema.clone(),
            "sq".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(storage)),
        ));
        let subquery_scan = SubqueryScanExec::new(subquery);

        let result = subquery_scan.execute();
        assert!(result.is_ok());
    }

    #[test]
    fn test_cte_children() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let cte_plan = Rc::new(TableScanExec::new(
            schema.clone(),
            "cte".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let input = Rc::new(TableScanExec::new(
            schema.clone(),
            "input".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let cte_exec = CteExec::new(cte_plan, input, true);
        let children = cte_exec.children();

        assert_eq!(children.len(), 2);
    }

    #[test]
    fn test_subquery_scan_children() {
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);

        let subquery = Rc::new(TableScanExec::new(
            schema,
            "sq".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));
        let subquery_scan = SubqueryScanExec::new(subquery);
        let children = subquery_scan.children();

        assert_eq!(children.len(), 1);
    }
}
