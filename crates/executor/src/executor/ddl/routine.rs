use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::DataType;
use yachtsql_ir::{FunctionArg, FunctionBody, ProcedureArg};
use yachtsql_storage::{Schema, Table};

use super::super::PlanExecutor;
use super::plan_conv::executor_plan_to_logical_plan;
use crate::catalog::{UserFunction, UserProcedure};
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_function(
        &mut self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
        is_aggregate: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.function_exists(name) {
            return Ok(Table::empty(Schema::new()));
        }
        let func = UserFunction {
            name: name.to_string(),
            parameters: args.to_vec(),
            return_type: return_type.clone(),
            body: body.clone(),
            is_temporary: is_temp,
            is_aggregate,
        };
        self.catalog.create_function(func, or_replace)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_function(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.function_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!("Function not found: {}", name)));
        }
        self.catalog.drop_function(name)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_procedure(
        &mut self,
        name: &str,
        args: &[ProcedureArg],
        body: &[PhysicalPlan],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        let body_plans = body.iter().map(executor_plan_to_logical_plan).collect();
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: body_plans,
        };
        self.catalog
            .create_procedure(proc, or_replace, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_procedure(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.procedure_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!(
                "Procedure not found: {}",
                name
            )));
        }
        self.catalog.drop_procedure(name)?;
        Ok(Table::empty(Schema::new()))
    }
}
