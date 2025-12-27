use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{Expr, RaiseLevel};
use yachtsql_optimizer::optimize;
use yachtsql_storage::{Record, Schema, Table};

use super::{ConcurrentPlanExecutor, default_value_for_type};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl ConcurrentPlanExecutor<'_> {
    pub(crate) async fn execute_call(&self, procedure_name: &str, args: &[Expr]) -> Result<Table> {
        use yachtsql_ir::ProcedureArgMode;

        let proc = self
            .catalog
            .get_procedure(procedure_name)
            .ok_or_else(|| Error::InvalidQuery(format!("Procedure not found: {}", procedure_name)))?
            .clone();

        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let mut out_var_mappings: Vec<(String, String)> = Vec::new();

        for (i, param) in proc.parameters.iter().enumerate() {
            let param_name = param.name.to_uppercase();

            match param.mode {
                ProcedureArgMode::In => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        let vars = self.get_variables();
                        let udf = self.get_user_functions();
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&vars)
                            .with_user_functions(&udf);
                        evaluator.evaluate(arg_expr, &empty_record)?
                    } else {
                        default_value_for_type(&param.data_type)
                    };
                    self.variables
                        .write()
                        .unwrap()
                        .insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);
                }
                ProcedureArgMode::Out => {
                    let value = default_value_for_type(&param.data_type);
                    self.variables
                        .write()
                        .unwrap()
                        .insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);

                    if let Some(Expr::Variable { name }) = args.get(i) {
                        out_var_mappings.push((param_name.clone(), name.clone()));
                    }
                }
                ProcedureArgMode::InOut => {
                    let value = if let Some(arg_expr) = args.get(i) {
                        let vars = self.get_variables();
                        let udf = self.get_user_functions();
                        let evaluator = IrEvaluator::new(&empty_schema)
                            .with_variables(&vars)
                            .with_user_functions(&udf);
                        evaluator.evaluate(arg_expr, &empty_record)?
                    } else {
                        default_value_for_type(&param.data_type)
                    };
                    self.variables
                        .write()
                        .unwrap()
                        .insert(param_name.clone(), value.clone());
                    self.session.set_variable(&param_name, value);

                    if let Some(Expr::Variable { name }) = args.get(i) {
                        out_var_mappings.push((param_name.clone(), name.clone()));
                    }
                }
            }
        }

        let mut last_result = Table::empty(Schema::new());

        let body_plans = self
            .catalog
            .get_procedure_body(procedure_name)
            .unwrap_or_default();

        for body_plan in &body_plans {
            let accesses = body_plan.extract_table_accesses();
            for (table_name, access_type) in accesses.accesses.iter() {
                let upper_name = table_name.to_uppercase();
                let already_locked = self.tables.get_table(&upper_name).is_some();
                if !already_locked && let Some(handle) = self.catalog.get_table_handle(table_name) {
                    match access_type {
                        crate::plan::AccessType::Read => {
                            if let Some(guard) = handle.try_read() {
                                self.tables
                                    .add_read_table(upper_name.clone(), guard.clone());
                            }
                        }
                        crate::plan::AccessType::Write | crate::plan::AccessType::WriteOptional => {
                            if let Some(guard) = handle.try_write() {
                                self.tables
                                    .add_write_table(upper_name.clone(), guard.clone());
                            }
                        }
                    }
                }
            }
        }

        for body_plan in &body_plans {
            last_result = self.execute_plan(body_plan).await?;
        }

        for (param_name, var_name) in out_var_mappings {
            let value = self.variables.read().unwrap().get(&param_name).cloned();
            if let Some(value) = value {
                let var_name_upper = var_name.to_uppercase();
                self.variables
                    .write()
                    .unwrap()
                    .insert(var_name_upper.clone(), value.clone());
                self.session.set_variable(&var_name_upper, value);
            }
        }

        Ok(last_result)
    }

    pub(crate) fn rollback_transaction(&self) {
        if let Some(mut snapshot) = self.catalog.take_transaction_snapshot() {
            let mut restored_tables = Vec::new();
            for (name, table_data) in &snapshot.tables {
                if let Some(table) = self.tables.get_table_mut(name) {
                    *table = table_data.clone();
                    restored_tables.push(name.clone());
                }
            }
            for name in restored_tables {
                snapshot.tables.remove(&name);
            }
            for (name, table_data) in snapshot.tables {
                if let Some(handle) = self.catalog.get_table_handle(&name)
                    && let Some(mut table) = handle.try_write()
                {
                    *table = table_data;
                }
            }
        }
    }

    pub(crate) fn execute_declare(
        &self,
        name: &str,
        data_type: &DataType,
        default: Option<&Expr>,
    ) -> Result<Table> {
        let value = if let Some(expr) = default {
            let empty_schema = Schema::new();
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            let mut evaluated = evaluator.evaluate(expr, &Record::new())?;

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
        } else {
            default_value_for_type(data_type)
        };
        self.variables
            .write()
            .unwrap()
            .insert(name.to_uppercase(), value.clone());
        self.session.set_variable(name, value);
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) async fn execute_set_variable(&self, name: &str, value: &Expr) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let val = match value {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                self.eval_scalar_subquery(plan, &empty_schema, &empty_record)
                    .await?
            }
            _ => {
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
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
            {
                self.variables
                    .write()
                    .unwrap()
                    .insert(upper_name, val.clone());
            }
            self.session.set_variable(name, val);
        }
        Ok(Table::empty(Schema::new()))
    }

    pub(crate) async fn execute_set_multiple_variables(
        &self,
        names: &[String],
        value: &Expr,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let field_values: Vec<(String, Value)> = match value {
            Expr::Subquery(plan) | Expr::ScalarSubquery(plan) => {
                let result = self.eval_scalar_subquery_as_row(plan).await?;
                match result {
                    Value::Struct(fields) => fields,
                    _ => {
                        return Err(Error::invalid_query(
                            "SET multiple variables: subquery must return a single row",
                        ));
                    }
                }
            }
            _ => {
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
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
            self.variables
                .write()
                .unwrap()
                .insert(upper_name.clone(), field_val.clone());
            self.session.set_variable(name, field_val);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub(crate) async fn execute_if(
        &self,
        condition: &Expr,
        then_branch: &[PhysicalPlan],
        else_branch: Option<&[PhysicalPlan]>,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let cond = if Self::expr_contains_subquery(condition) {
            self.eval_expr_with_subqueries(condition, &empty_schema, &empty_record)
                .await?
        } else {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            evaluator.evaluate(condition, &empty_record)?
        };

        let branch = if cond.as_bool().unwrap_or(false) {
            then_branch
        } else {
            else_branch.unwrap_or(&[])
        };

        let mut result = Table::empty(Schema::new());
        for stmt in branch {
            result = self.execute_plan(stmt).await?;
        }
        Ok(result)
    }

    pub(crate) async fn execute_while(
        &self,
        condition: &Expr,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        'outer: loop {
            let cond = {
                let vars = self.get_variables();
                let sys_vars = self.get_system_variables();
                let udf = self.get_user_functions();
                let evaluator = IrEvaluator::new(&empty_schema)
                    .with_variables(&vars)
                    .with_system_variables(&sys_vars)
                    .with_user_functions(&udf);
                evaluator.evaluate(condition, &Record::new())?
            };

            if !cond.as_bool().unwrap_or(false) {
                break;
            }

            for stmt in body {
                match self.execute_plan(stmt).await {
                    Ok(r) => result = r,
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
                    Err(Error::InvalidQuery(msg)) if msg == "RETURN outside of function" => {
                        return Ok(result);
                    }
                    Err(e) => return Err(e),
                }
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query(
                    "WHILE loop exceeded maximum iterations",
                ));
            }
        }

        Ok(result)
    }

    #[allow(unused_assignments)]
    pub(crate) async fn execute_loop(
        &self,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        'outer: loop {
            for stmt in body {
                match self.execute_plan(stmt).await {
                    Ok(r) => result = r,
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        if let Some(lbl) = label
                            && (msg.contains(&format!("BREAK:{}", lbl))
                                || msg == "BREAK outside of loop")
                        {
                            return Ok(Table::empty(Schema::new()));
                        }
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        continue 'outer;
                    }
                    Err(Error::InvalidQuery(msg)) if msg == "RETURN outside of function" => {
                        return Ok(result);
                    }
                    Err(e) => return Err(e),
                }
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query("LOOP exceeded maximum iterations"));
            }
        }
    }

    pub(crate) async fn execute_block(
        &self,
        body: &[PhysicalPlan],
        label: Option<&str>,
    ) -> Result<Table> {
        let mut last_result = Table::empty(Schema::new());
        for plan in body {
            match self.execute_plan(plan).await {
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

    pub(crate) async fn execute_repeat(
        &self,
        body: &[PhysicalPlan],
        until_condition: &Expr,
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let mut result = Table::empty(Schema::new());
        let mut iterations = 0;
        const MAX_ITERATIONS: usize = 10000;

        'outer: loop {
            for stmt in body {
                match self.execute_plan(stmt).await {
                    Ok(r) => result = r,
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        continue 'outer;
                    }
                    Err(Error::InvalidQuery(msg)) if msg == "RETURN outside of function" => {
                        return Ok(result);
                    }
                    Err(e) => return Err(e),
                }
            }

            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            let cond = evaluator.evaluate(until_condition, &Record::new())?;

            if cond.as_bool().unwrap_or(false) {
                break;
            }

            iterations += 1;
            if iterations >= MAX_ITERATIONS {
                return Err(Error::invalid_query(
                    "REPEAT loop exceeded maximum iterations",
                ));
            }
        }

        Ok(result)
    }

    pub(crate) async fn execute_for(
        &self,
        variable: &str,
        query: &PhysicalPlan,
        body: &[PhysicalPlan],
    ) -> Result<Table> {
        let query_result = self.execute_plan(query).await?;
        let schema_fields = query_result.schema().fields();
        let mut result = Table::empty(Schema::new());

        'outer: for record in query_result.rows()? {
            let values = record.values();
            let struct_fields: Vec<(String, Value)> = schema_fields
                .iter()
                .zip(values.iter())
                .map(|(f, v)| (f.name.clone(), v.clone()))
                .collect();
            let row_value = Value::Struct(struct_fields);
            self.variables
                .write()
                .unwrap()
                .insert(variable.to_uppercase(), row_value.clone());
            self.session.set_variable(variable, row_value);

            for stmt in body {
                match self.execute_plan(stmt).await {
                    Ok(r) => result = r,
                    Err(Error::InvalidQuery(msg)) if msg.contains("BREAK") => {
                        return Ok(Table::empty(Schema::new()));
                    }
                    Err(Error::InvalidQuery(msg)) if msg.contains("CONTINUE") => {
                        continue 'outer;
                    }
                    Err(Error::InvalidQuery(msg)) if msg == "RETURN outside of function" => {
                        return Ok(result);
                    }
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(result)
    }

    pub(crate) fn execute_raise(&self, message: Option<&Expr>, level: RaiseLevel) -> Result<Table> {
        let msg = if let Some(expr) = message {
            let empty_schema = Schema::new();
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            let val = evaluator.evaluate(expr, &Record::new())?;
            match val {
                Value::String(s) => s,
                _ => format!("{:?}", val),
            }
        } else {
            "Exception raised".to_string()
        };

        match level {
            RaiseLevel::Exception => Err(Error::raised_exception(msg)),
            RaiseLevel::Warning => Ok(Table::empty(Schema::new())),
            RaiseLevel::Notice => Ok(Table::empty(Schema::new())),
        }
    }

    pub(crate) async fn execute_try_catch(
        &self,
        try_block: &[(PhysicalPlan, Option<String>)],
        catch_block: &[PhysicalPlan],
    ) -> Result<Table> {
        let mut last_result = Table::empty(Schema::new());

        for (plan, source_sql) in try_block {
            match self.execute_plan(plan).await {
                Ok(result) => {
                    last_result = result;
                }
                Err(Error::InvalidQuery(msg)) if msg == "RETURN outside of function" => {
                    return Ok(last_result);
                }
                Err(e) => {
                    let error_message = e.to_string();
                    let stmt_text = source_sql.clone().unwrap_or_else(|| format!("{:?}", plan));
                    let error_struct = Value::Struct(vec![
                        ("message".to_string(), Value::String(error_message.clone())),
                        ("statement_text".to_string(), Value::String(stmt_text)),
                    ]);
                    self.variables
                        .write()
                        .unwrap()
                        .insert("@@ERROR".to_string(), error_struct.clone());
                    self.system_variables
                        .write()
                        .unwrap()
                        .insert("@@error".to_string(), error_struct.clone());
                    self.session.set_system_variable("@@error", error_struct);

                    for catch_plan in catch_block {
                        match self.execute_plan(catch_plan).await {
                            Ok(result) => last_result = result,
                            Err(Error::InvalidQuery(msg))
                                if msg == "RETURN outside of function" =>
                            {
                                return Ok(last_result);
                            }
                            Err(err) => return Err(err),
                        }
                    }
                    return Ok(last_result);
                }
            }
        }

        Ok(last_result)
    }

    pub(crate) async fn execute_execute_immediate(
        &self,
        sql_expr: &Expr,
        into_variables: &[String],
        using_params: &[(Expr, Option<String>)],
    ) -> Result<Table> {
        let empty_schema = Schema::new();
        let empty_record = Record::new();

        let sql_string = {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            let sql_value = evaluator.evaluate(sql_expr, &empty_record)?;
            sql_value
                .as_str()
                .ok_or_else(|| Error::InvalidQuery("EXECUTE IMMEDIATE requires a string".into()))?
                .to_string()
        };

        let mut named_params: Vec<(String, Value)> = Vec::new();
        let mut positional_values: Vec<Value> = Vec::new();
        for (param_expr, alias) in using_params {
            let vars = self.get_variables();
            let sys_vars = self.get_system_variables();
            let udf = self.get_user_functions();
            let evaluator = IrEvaluator::new(&empty_schema)
                .with_variables(&vars)
                .with_system_variables(&sys_vars)
                .with_user_functions(&udf);
            let value = evaluator.evaluate(param_expr, &empty_record)?;
            positional_values.push(value.clone());
            if let Some(name) = alias {
                named_params.push((name.to_uppercase(), value));
            }
        }

        for (upper_name, value) in &named_params {
            self.variables
                .write()
                .unwrap()
                .insert(upper_name.clone(), value.clone());
            self.session.set_variable(upper_name, value.clone());
        }

        let processed_sql = self.substitute_parameters(&sql_string, &positional_values);

        let result = self.execute_dynamic_sql(&processed_sql).await?;

        if !into_variables.is_empty() && !result.is_empty() {
            let rows: Vec<_> = result.rows()?.into_iter().collect();
            if !rows.is_empty() {
                let first_row = &rows[0];
                let values = first_row.values();
                for (i, var_name) in into_variables.iter().enumerate() {
                    if let Some(val) = values.get(i) {
                        let upper_name = var_name.to_uppercase();
                        self.variables
                            .write()
                            .unwrap()
                            .insert(upper_name.clone(), val.clone());
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
                while i < chars.len() && (chars[i].is_alphanumeric() || chars[i] == '_') {
                    i += 1;
                }
                let param_name: String = chars[start..i].iter().collect();
                let upper_name = param_name[1..].to_uppercase();
                if let Some(val) = self.variables.read().unwrap().get(&upper_name) {
                    output.push_str(&self.value_to_sql_literal(val));
                } else {
                    output.push_str(&param_name);
                }
            } else {
                output.push(chars[i]);
                i += 1;
            }
        }

        output
    }

    fn value_to_sql_literal(&self, val: &Value) -> String {
        match val {
            Value::Null => "NULL".to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.to_string(),
            Value::Bool(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
            Value::String(s) => format!("'{}'", s.replace('\'', "''")),
            Value::Date(d) => format!("DATE '{}'", d.format("%Y-%m-%d")),
            Value::Time(t) => format!("TIME '{}'", t.format("%H:%M:%S%.f")),
            Value::Timestamp(ts) => format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.f")),
            Value::DateTime(dt) => format!("DATETIME '{}'", dt.format("%Y-%m-%d %H:%M:%S%.f")),
            _ => format!("{:?}", val),
        }
    }

    async fn execute_dynamic_sql(&self, sql: &str) -> Result<Table> {
        let logical_plan = yachtsql_parser::parse_and_plan(sql, self.catalog)?;
        let physical = optimize(&logical_plan)?;
        let executor_plan = PhysicalPlan::from_physical(&physical);

        let accesses = executor_plan.extract_table_accesses();
        for (table_name, access_type) in accesses.accesses.iter() {
            let upper_name = table_name.to_uppercase();
            let already_locked = self.tables.get_table(&upper_name).is_some();
            if !already_locked && let Some(handle) = self.catalog.get_table_handle(table_name) {
                match access_type {
                    crate::plan::AccessType::Read => {
                        let table = handle.read().clone();
                        self.tables.add_read_table(upper_name.clone(), table);
                    }
                    crate::plan::AccessType::Write | crate::plan::AccessType::WriteOptional => {
                        let table = handle.write().clone();
                        self.tables.add_write_table(upper_name.clone(), table);
                    }
                }
            }
        }

        self.execute_plan(&executor_plan).await
    }
}
