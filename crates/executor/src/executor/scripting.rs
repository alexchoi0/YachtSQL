use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{Expr, ProcedureArgMode, RaiseLevel};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_call(&mut self, procedure_name: &str, args: &[Expr]) -> Result<Table> {
        let proc = self
            .catalog
            .get_procedure(procedure_name)
            .ok_or_else(|| Error::InvalidQuery(format!("Procedure not found: {}", procedure_name)))?
            .clone();

        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = IrEvaluator::new(&empty_schema);

        let mut out_var_mappings: Vec<(String, String)> = Vec::new();

        for (i, param) in proc.parameters.iter().enumerate() {
            let param_name = param.name.to_uppercase();

            match param.mode {
                ProcedureArgMode::In => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        evaluator.evaluate(arg_expr, &empty_record)?
                    } else {
                        default_value_for_type(&param.data_type)
                    };
                    self.variables.insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);
                }
                ProcedureArgMode::Out => {
                    let value = default_value_for_type(&param.data_type);
                    self.variables.insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);

                    if let Some(Expr::Variable { name }) = args.get(i) {
                        out_var_mappings.push((param_name.clone(), name.clone()));
                    }
                }
                ProcedureArgMode::InOut => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        evaluator.evaluate(arg_expr, &empty_record)?
                    } else {
                        default_value_for_type(&param.data_type)
                    };
                    self.variables.insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);

                    if let Some(Expr::Variable { name }) = args.get(i) {
                        out_var_mappings.push((param_name.clone(), name.clone()));
                    }
                }
            }
        }

        let mut last_result = Table::empty(Schema::new());

        for body_plan in &proc.body {
            let physical_plan = optimize(body_plan)?;
            let executor_plan = ExecutorPlan::from_physical(&physical_plan);
            last_result = self.execute_plan(&executor_plan)?;
        }

        for (param_name, var_name) in out_var_mappings {
            if let Some(value) = self.variables.get(&param_name).cloned() {
                let var_name_upper = var_name.trim_start_matches('@').to_uppercase();
                self.variables.insert(var_name_upper.clone(), value.clone());
                self.session.set_variable(&var_name_upper, value);
            }
        }

        Ok(last_result)
    }

    pub fn execute_declare(
        &mut self,
        name: &str,
        data_type: &DataType,
        default: Option<&Expr>,
    ) -> Result<Table> {
        let value = match default {
            Some(expr) => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = IrEvaluator::new(&empty_schema).with_variables(&self.variables);
                evaluator.evaluate(expr, &empty_record)?
            }
            None => default_value_for_type(data_type),
        };

        self.variables.insert(name.to_uppercase(), value.clone());
        self.session.set_variable(name, value);

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_set_variable(&mut self, name: &str, value: &Expr) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = IrEvaluator::new(&empty_schema).with_variables(&self.variables);
        let val = evaluator.evaluate(value, &empty_record)?;

        self.variables.insert(name.to_uppercase(), val.clone());
        self.session.set_variable(name, val);

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_if(
        &mut self,
        condition: &Expr,
        then_branch: &[ExecutorPlan],
        else_branch: Option<&[ExecutorPlan]>,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        let evaluator = IrEvaluator::new(&empty_schema).with_variables(&self.variables);
        let cond_val = evaluator.evaluate(condition, &empty_record)?;

        if cond_val.as_bool().unwrap_or(false) {
            for plan in then_branch {
                self.execute_plan(plan)?;
            }
        } else if let Some(else_plans) = else_branch {
            for plan in else_plans {
                self.execute_plan(plan)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_while(&mut self, condition: &Expr, body: &[ExecutorPlan]) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);

        loop {
            let evaluator = IrEvaluator::new(&empty_schema).with_variables(&self.variables);
            let cond_val = evaluator.evaluate(condition, &empty_record)?;
            if !cond_val.as_bool().unwrap_or(false) {
                break;
            }

            for plan in body {
                match self.execute_plan(plan) {
                    Ok(_) => {}
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_loop(&mut self, body: &[ExecutorPlan], _label: Option<&str>) -> Result<Table> {
        loop {
            for plan in body {
                match self.execute_plan(plan) {
                    Ok(_) => {}
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }

    pub fn execute_repeat(
        &mut self,
        body: &[ExecutorPlan],
        until_condition: &Expr,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);

        loop {
            for plan in body {
                match self.execute_plan(plan) {
                    Ok(_) => {}
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }

            let evaluator = IrEvaluator::new(&empty_schema).with_variables(&self.variables);
            let cond_val = evaluator.evaluate(until_condition, &empty_record)?;
            if cond_val.as_bool().unwrap_or(false) {
                break;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_for(
        &mut self,
        variable: &str,
        query: &ExecutorPlan,
        body: &[ExecutorPlan],
    ) -> Result<Table> {
        let result = self.execute_plan(query)?;

        for record in result.rows()? {
            if let Some(val) = record.values().first() {
                self.variables.insert(variable.to_uppercase(), val.clone());
                self.session.set_variable(variable, val.clone());
            }

            for plan in body {
                match self.execute_plan(plan) {
                    Ok(_) => {}
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_raise(&mut self, message: Option<&Expr>, level: RaiseLevel) -> Result<Table> {
        let msg = match message {
            Some(expr) => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = IrEvaluator::new(&empty_schema).with_variables(&self.variables);
                evaluator
                    .evaluate(expr, &empty_record)?
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            }
            None => String::new(),
        };

        match level {
            RaiseLevel::Exception => Err(Error::InvalidQuery(msg)),
            RaiseLevel::Warning | RaiseLevel::Notice => Ok(Table::empty(Schema::new())),
        }
    }
}

fn default_value_for_type(data_type: &DataType) -> Value {
    match data_type {
        DataType::Int64 => Value::Int64(0),
        DataType::Float64 => Value::Float64(ordered_float::OrderedFloat(0.0)),
        DataType::Bool => Value::Bool(false),
        DataType::String => Value::String(String::new()),
        _ => Value::Null,
    }
}
