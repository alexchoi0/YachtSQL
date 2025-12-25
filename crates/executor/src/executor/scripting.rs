use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{Expr, ProcedureArgMode, RaiseLevel};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::PlanExecutor;
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_call(&mut self, procedure_name: &str, args: &[Expr]) -> Result<Table> {
        let proc = self
            .catalog
            .get_procedure(procedure_name)
            .ok_or_else(|| Error::InvalidQuery(format!("Procedure not found: {}", procedure_name)))?
            .clone();

        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);

        let mut out_var_mappings: Vec<(String, String)> = Vec::new();

        for (i, param) in proc.parameters.iter().enumerate() {
            let param_name = param.name.to_uppercase();

            match param.mode {
                ProcedureArgMode::In => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&self.variables)
                            .with_system_variables(self.session.system_variables());
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
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&self.variables)
                            .with_system_variables(self.session.system_variables());
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
            let executor_plan = PhysicalPlan::from_physical(&physical_plan);
            last_result = self.execute_plan(&executor_plan)?;
        }

        for (param_name, var_name) in out_var_mappings {
            if let Some(value) = self.variables.get(&param_name).cloned() {
                let var_name_upper = var_name.to_uppercase();
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
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables());
                let mut evaluated = evaluator.evaluate(expr, &empty_record)?;

                if let (DataType::Struct(type_fields), Value::Struct(value_fields)) =
                    (data_type, &evaluated)
                {
                    let named_fields: Vec<(String, Value)> = type_fields
                        .iter()
                        .zip(value_fields.iter())
                        .map(|(type_field, (_, v))| (type_field.name.clone(), v.clone()))
                        .collect();
                    evaluated = Value::Struct(named_fields);
                }

                evaluated
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

        let val = match value {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                self.eval_scalar_subquery_for_set(plan)?
            }
            _ => {
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables());
                evaluator.evaluate(value, &empty_record)?
            }
        };

        let upper_name = name.to_uppercase();
        if upper_name == "SEARCH_PATH"
            && let Some(schema_name) = val.as_str()
        {
            self.catalog.set_search_path(vec![schema_name.to_string()]);
        }

        if name.starts_with("@@") {
            self.session.set_system_variable(name, val);
        } else {
            self.variables.insert(upper_name, val.clone());
            self.session.set_variable(name, val);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_set_multiple_variables(
        &mut self,
        names: &[String],
        value: &Expr,
    ) -> Result<Table> {
        let field_values: Vec<(String, Value)> = match value {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                self.eval_row_as_struct_for_set(plan)?
            }
            _ => {
                let empty_schema = Schema::new();
                let empty_record = Record::from_values(vec![]);
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables());
                let val = evaluator.evaluate(value, &empty_record)?;
                match val {
                    Value::Struct(fields) => fields,
                    _ => {
                        return Err(Error::invalid_query(
                            "SET multiple variables requires a STRUCT value",
                        ));
                    }
                }
            }
        };

        if field_values.len() != names.len() {
            return Err(Error::invalid_query(format!(
                "SET: number of struct fields ({}) doesn't match number of variables ({})",
                field_values.len(),
                names.len()
            )));
        }

        for (i, name) in names.iter().enumerate() {
            let field_val = field_values[i].1.clone();
            let upper_name = name.to_uppercase();
            self.variables.insert(upper_name.clone(), field_val.clone());
            self.session.set_variable(name, field_val);
        }

        Ok(Table::empty(Schema::new()))
    }

    fn eval_row_as_struct_for_set(
        &mut self,
        plan: &yachtsql_ir::LogicalPlan,
    ) -> Result<Vec<(String, Value)>> {
        let physical = optimize(plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        if result_table.is_empty() {
            return Ok(vec![]);
        }

        let rows: Vec<_> = result_table.rows()?.into_iter().collect();
        if rows.is_empty() {
            return Ok(vec![]);
        }

        let first_row = &rows[0];
        let values = first_row.values();

        let schema = result_table.schema();
        let fields = schema.fields();

        let result: Vec<(String, Value)> = fields
            .iter()
            .zip(values.iter())
            .map(|(f, v)| (f.name.clone(), v.clone()))
            .collect();

        Ok(result)
    }

    fn eval_scalar_subquery_for_set(&mut self, plan: &yachtsql_ir::LogicalPlan) -> Result<Value> {
        let physical = optimize(plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        let result_table = self.execute_plan(&executor_plan)?;

        if result_table.is_empty() {
            return Ok(Value::Null);
        }

        let rows: Vec<_> = result_table.rows()?.into_iter().collect();
        if rows.is_empty() {
            return Ok(Value::Null);
        }

        let first_row = &rows[0];
        let values = first_row.values();
        if values.is_empty() {
            return Ok(Value::Null);
        }

        Ok(values[0].clone())
    }

    pub fn execute_if(
        &mut self,
        condition: &Expr,
        then_branch: &[PhysicalPlan],
        else_branch: Option<&[PhysicalPlan]>,
    ) -> Result<Table> {
        let cond_val = self.evaluate_scripting_expr(condition)?;

        let mut last_result = Table::empty(Schema::new());
        if cond_val.as_bool().unwrap_or(false) {
            for plan in then_branch {
                last_result = self.execute_plan(plan)?;
            }
        } else if let Some(else_plans) = else_branch {
            for plan in else_plans {
                last_result = self.execute_plan(plan)?;
            }
        }

        Ok(last_result)
    }

    fn evaluate_scripting_expr(&mut self, expr: &Expr) -> Result<Value> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);
        match expr {
            Expr::Exists { subquery, negated } => {
                let physical = optimize(subquery)?;
                let plan = PhysicalPlan::from_physical(&physical);
                let result = self.execute_plan(&plan)?;
                let has_rows = !result.is_empty();
                Ok(Value::Bool(if *negated { !has_rows } else { has_rows }))
            }
            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_scripting_expr(left)?;
                let right_val = self.evaluate_scripting_expr(right)?;
                use yachtsql_ir::BinaryOp;
                match op {
                    BinaryOp::And => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l && r))
                    }
                    BinaryOp::Or => {
                        let l = left_val.as_bool().unwrap_or(false);
                        let r = right_val.as_bool().unwrap_or(false);
                        Ok(Value::Bool(l || r))
                    }
                    _ => {
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&self.variables)
                            .with_system_variables(self.session.system_variables());
                        evaluator.evaluate(expr, &empty_record)
                    }
                }
            }
            Expr::UnaryOp {
                op: yachtsql_ir::UnaryOp::Not,
                expr: inner,
            } => {
                let val = self.evaluate_scripting_expr(inner)?;
                Ok(Value::Bool(!val.as_bool().unwrap_or(false)))
            }
            _ => {
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables());
                evaluator.evaluate(expr, &empty_record)
            }
        }
    }

    pub fn execute_while(
        &mut self,
        condition: &Expr,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        'outer: loop {
            let cond_val = self.evaluate_scripting_expr(condition)?;
            if !cond_val.as_bool().unwrap_or(false) {
                break;
            }

            for plan in body {
                match self.execute_plan(plan) {
                    Ok(_) => {}
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        if msg == "BREAK outside of loop" {
                            return Ok(Table::empty(Schema::new()));
                        }
                        if let Some(lbl) = label
                            && msg == format!("BREAK:{}", lbl)
                        {
                            return Ok(Table::empty(Schema::new()));
                        }
                        return Err(Error::InvalidQuery(msg));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        if msg == "CONTINUE outside of loop" {
                            continue 'outer;
                        }
                        if let Some(lbl) = label
                            && msg == format!("CONTINUE:{}", lbl)
                        {
                            continue 'outer;
                        }
                        return Err(Error::InvalidQuery(msg));
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_loop(&mut self, body: &[PhysicalPlan], label: Option<&str>) -> Result<Table> {
        'outer: loop {
            for plan in body {
                match self.execute_plan(plan) {
                    Ok(_) => {}
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        if msg == "BREAK outside of loop" {
                            return Ok(Table::empty(Schema::new()));
                        }
                        if let Some(lbl) = label
                            && msg == format!("BREAK:{}", lbl)
                        {
                            return Ok(Table::empty(Schema::new()));
                        }
                        return Err(Error::InvalidQuery(msg));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        if msg == "CONTINUE outside of loop" {
                            continue 'outer;
                        }
                        if let Some(lbl) = label
                            && msg == format!("CONTINUE:{}", lbl)
                        {
                            continue 'outer;
                        }
                        return Err(Error::InvalidQuery(msg));
                    }
                    Err(e) => return Err(e),
                }
            }
        }
    }

    pub fn execute_block(&mut self, body: &[PhysicalPlan], label: Option<&str>) -> Result<Table> {
        let mut last_result = Table::empty(Schema::new());
        for plan in body {
            match self.execute_plan(plan) {
                Ok(result) => {
                    last_result = result;
                }
                Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                    if let Some(lbl) = label
                        && msg == format!("BREAK:{}", lbl)
                    {
                        return Ok(last_result);
                    }
                    return Err(Error::InvalidQuery(msg));
                }
                Err(Error::InvalidQuery(msg)) if msg == "RETURN outside of function" => {
                    return Ok(last_result);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(last_result)
    }

    pub fn execute_repeat(
        &mut self,
        body: &[PhysicalPlan],
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

            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables());
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
        query: &PhysicalPlan,
        body: &[PhysicalPlan],
    ) -> Result<Table> {
        let result = self.execute_plan(query)?;
        let schema_fields = result.schema().fields();

        for record in result.rows()? {
            let values = record.values();
            let struct_fields: Vec<(String, Value)> = schema_fields
                .iter()
                .zip(values.iter())
                .map(|(f, v)| (f.name.clone(), v.clone()))
                .collect();
            let row_value = Value::Struct(struct_fields);
            self.variables
                .insert(variable.to_uppercase(), row_value.clone());
            self.session.set_variable(variable, row_value);

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
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&self.variables)
                    .with_system_variables(self.session.system_variables());
                evaluator
                    .evaluate(expr, &empty_record)?
                    .as_str()
                    .unwrap_or("")
                    .to_string()
            }
            None => String::new(),
        };

        match level {
            RaiseLevel::Exception => Err(Error::raised_exception(msg)),
            RaiseLevel::Warning | RaiseLevel::Notice => Ok(Table::empty(Schema::new())),
        }
    }

    pub fn execute_execute_immediate(
        &mut self,
        sql_expr: &Expr,
        into_variables: &[String],
        using_params: &[(Expr, Option<String>)],
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::from_values(vec![]);

        let sql_string = {
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables());
            let sql_value = evaluator.evaluate(sql_expr, &empty_record)?;
            sql_value
                .as_str()
                .ok_or_else(|| Error::InvalidQuery("EXECUTE IMMEDIATE requires a string".into()))?
                .to_string()
        };

        let mut named_params: Vec<(String, Value)> = Vec::new();
        let mut positional_values: Vec<Value> = Vec::new();
        for (param_expr, alias) in using_params {
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&self.variables)
                .with_system_variables(self.session.system_variables());
            let value = evaluator.evaluate(param_expr, &empty_record)?;
            positional_values.push(value.clone());
            if let Some(name) = alias {
                named_params.push((name.to_uppercase(), value));
            }
        }

        for (upper_name, value) in &named_params {
            self.variables.insert(upper_name.clone(), value.clone());
            self.session.set_variable(upper_name, value.clone());
        }

        let processed_sql = self.substitute_parameters(&sql_string, &positional_values);

        let result = self.execute_dynamic_sql(&processed_sql)?;

        if !into_variables.is_empty() && !result.is_empty() {
            let rows: Vec<_> = result.rows()?.into_iter().collect();
            if !rows.is_empty() {
                let first_row = &rows[0];
                let values = first_row.values();
                for (i, var_name) in into_variables.iter().enumerate() {
                    if let Some(val) = values.get(i) {
                        let upper_name = var_name.to_uppercase();
                        self.variables.insert(upper_name.clone(), val.clone());
                        self.session.set_variable(&upper_name, val.clone());
                    }
                }
            }
        }

        Ok(result)
    }

    fn substitute_parameters(&self, sql: &str, positional_values: &[Value]) -> String {
        let mut positional_idx = 0;

        let chars: Vec<char> = sql.chars().collect();
        let mut output = String::new();
        let mut i = 0;

        while i < chars.len() {
            if chars[i] == '?' {
                if let Some(val) = positional_values.get(positional_idx) {
                    output.push_str(&self.value_to_sql_literal(val));
                    positional_idx += 1;
                } else {
                    output.push(chars[i]);
                }
                i += 1;
            } else if chars[i] == '@' {
                let start = i;
                i += 1;
                let mut name = String::new();
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    name.push(chars[i]);
                    i += 1;
                }
                let upper_name = name.to_uppercase();
                if let Some(val) = self.variables.get(&upper_name) {
                    output.push_str(&self.value_to_sql_literal(val));
                } else {
                    for c in chars[start..i].iter() {
                        output.push(*c);
                    }
                }
            } else {
                output.push(chars[i]);
                i += 1;
            }
        }

        output
    }

    fn value_to_sql_literal(&self, value: &Value) -> String {
        match value {
            Value::Null => "NULL".to_string(),
            Value::Bool(b) => b.to_string().to_uppercase(),
            Value::Int64(i) => i.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Bytes(b) => format!("X'{}'", hex::encode(b)),
            _ => format!("{:?}", value),
        }
    }

    fn execute_dynamic_sql(&mut self, sql: &str) -> Result<Table> {
        let logical_plan = yachtsql_parser::parse_and_plan(sql, self.catalog)?;
        let physical = optimize(&logical_plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);
        self.execute_plan(&executor_plan)
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
