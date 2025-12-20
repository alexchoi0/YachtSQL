#![allow(dead_code)]
#![allow(unused_imports)]
#![allow(unused_variables)]
#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::collapsible_if)]

mod catalog;
mod error;
mod executor;
mod ir_evaluator;
mod js_udf;
mod plan;
mod session;

pub use catalog::{Catalog, ColumnDefault, UserFunction, UserProcedure, ViewDef};
pub use error::{Error, Result};
pub use executor::{PlanExecutor, plan_schema_to_schema};
pub use plan::ExecutorPlan;
pub use session::Session;
use yachtsql_optimizer::PhysicalPlan;
pub use yachtsql_storage::{Record, Table};

pub struct QueryExecutor {
    catalog: Catalog,
    session: Session,
}

impl QueryExecutor {
    pub fn new() -> Self {
        Self {
            catalog: Catalog::new(),
            session: Session::new(),
        }
    }

    pub fn execute_sql(&mut self, sql: &str) -> yachtsql_common::error::Result<Table> {
        use yachtsql_parser::parse_sql;

        let statements = parse_sql(sql)?;
        let mut last_result = Table::empty(yachtsql_storage::Schema::new());

        for stmt in statements {
            last_result = self.execute_statement(&stmt)?;
        }

        Ok(last_result)
    }

    fn execute_statement(
        &mut self,
        stmt: &sqlparser::ast::Statement,
    ) -> yachtsql_common::error::Result<Table> {
        use sqlparser::ast::Statement;

        match stmt {
            Statement::StartTransaction {
                statements,
                exception,
                ..
            } => self.execute_begin_block(statements, exception.as_ref()),
            Statement::Commit { .. } => Ok(Table::empty(yachtsql_storage::Schema::new())),
            Statement::Rollback { .. } => Ok(Table::empty(yachtsql_storage::Schema::new())),
            Statement::Declare { stmts } => self.execute_declare_stmts(stmts),
            Statement::Set(set_stmt) => self.execute_set(set_stmt),
            Statement::If(if_stmt) => self.execute_if(if_stmt),
            Statement::While(while_stmt) => self.execute_while(while_stmt),
            Statement::Return(ret) => self.execute_return(ret),
            Statement::Raise(raise) => self.execute_raise(raise),
            Statement::Execute {
                parameters,
                immediate,
                ..
            } if *immediate => self.execute_immediate(parameters),
            Statement::Case(case_stmt) => self.execute_case_statement(case_stmt),
            Statement::Assert { condition, message } => {
                self.execute_assert(condition, message.as_ref())
            }
            _ => {
                let logical = yachtsql_parser::plan_statement(stmt, &self.catalog)?;
                let physical = yachtsql_optimizer::optimize(&logical)?;
                let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
                executor.execute(&physical)
            }
        }
    }

    fn execute_declare_stmts(
        &mut self,
        stmts: &[sqlparser::ast::Declare],
    ) -> yachtsql_common::error::Result<Table> {
        use sqlparser::ast::DeclareAssignment;
        use yachtsql_common::types::Value;

        for decl in stmts {
            let default_value = match &decl.assignment {
                Some(DeclareAssignment::Default(expr)) => self.evaluate_expr(expr)?,
                Some(DeclareAssignment::Expr(expr)) => self.evaluate_expr(expr)?,
                Some(DeclareAssignment::For(expr)) => self.evaluate_expr(expr)?,
                Some(DeclareAssignment::DuckAssignment(expr)) => self.evaluate_expr(expr)?,
                Some(DeclareAssignment::MsSqlAssignment(expr)) => self.evaluate_expr(expr)?,
                None => Value::Null,
            };

            for name in &decl.names {
                let var_name = name.value.to_uppercase();
                self.session.set_variable(&var_name, default_value.clone());
            }
        }
        Ok(Table::empty(yachtsql_storage::Schema::new()))
    }

    fn execute_set(
        &mut self,
        set_stmt: &sqlparser::ast::Set,
    ) -> yachtsql_common::error::Result<Table> {
        use sqlparser::ast::Set;
        use yachtsql_common::types::Value;

        match set_stmt {
            Set::SingleAssignment {
                variable, values, ..
            } => {
                let var_name = variable.to_string();
                let is_system_var = var_name.starts_with("@@");
                let clean_name = if is_system_var {
                    var_name[2..].to_uppercase()
                } else {
                    var_name.trim_start_matches('@').to_uppercase()
                };

                let value = if let Some(first_val) = values.first() {
                    self.evaluate_expr(first_val)?
                } else {
                    Value::Null
                };

                if is_system_var {
                    self.session
                        .set_system_variable(&format!("@@{}", clean_name), value);
                } else {
                    self.session.set_variable(&clean_name, value);
                }
                Ok(Table::empty(yachtsql_storage::Schema::new()))
            }
            Set::ParenthesizedAssignments { variables, values } => {
                for (var, val) in variables.iter().zip(values.iter()) {
                    let var_name = var.to_string().to_uppercase();
                    let value = self.evaluate_expr(val)?;
                    self.session.set_variable(&var_name, value);
                }
                Ok(Table::empty(yachtsql_storage::Schema::new()))
            }
            _ => {
                let logical = yachtsql_parser::plan_statement(
                    &sqlparser::ast::Statement::Set(set_stmt.clone()),
                    &self.catalog,
                )?;
                let physical = yachtsql_optimizer::optimize(&logical)?;
                let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
                executor.execute(&physical)
            }
        }
    }

    fn execute_if(
        &mut self,
        if_stmt: &sqlparser::ast::IfStatement,
    ) -> yachtsql_common::error::Result<Table> {
        if let Some(ref cond) = if_stmt.if_block.condition {
            let cond_val = self.evaluate_expr(cond)?;
            if cond_val.as_bool().unwrap_or(false) {
                return self
                    .execute_conditional_statements(&if_stmt.if_block.conditional_statements);
            }
        }

        for elseif_block in &if_stmt.elseif_blocks {
            if let Some(ref cond) = elseif_block.condition {
                let cond_val = self.evaluate_expr(cond)?;
                if cond_val.as_bool().unwrap_or(false) {
                    return self
                        .execute_conditional_statements(&elseif_block.conditional_statements);
                }
            }
        }

        if let Some(ref else_block) = if_stmt.else_block {
            return self.execute_conditional_statements(&else_block.conditional_statements);
        }

        Ok(Table::empty(yachtsql_storage::Schema::new()))
    }

    fn execute_while(
        &mut self,
        while_stmt: &sqlparser::ast::WhileStatement,
    ) -> yachtsql_common::error::Result<Table> {
        let max_iterations = 10000;
        let mut iterations = 0;

        let condition = while_stmt.while_block.condition.as_ref().ok_or_else(|| {
            yachtsql_common::error::Error::InvalidQuery(
                "WHILE statement missing condition".to_string(),
            )
        })?;

        while iterations < max_iterations {
            let cond_val = self.evaluate_expr(condition)?;
            if !cond_val.as_bool().unwrap_or(false) {
                break;
            }

            match self
                .execute_conditional_statements(&while_stmt.while_block.conditional_statements)
            {
                Ok(_) => {}
                Err(e) if e.to_string().contains("BREAK") || e.to_string().contains("LEAVE") => {
                    break;
                }
                Err(e)
                    if e.to_string().contains("CONTINUE") || e.to_string().contains("ITERATE") =>
                {
                    iterations += 1;
                    continue;
                }
                Err(e) => return Err(e),
            }

            iterations += 1;
        }
        Ok(Table::empty(yachtsql_storage::Schema::new()))
    }

    fn execute_begin_block(
        &mut self,
        statements: &[sqlparser::ast::Statement],
        exception: Option<&Vec<sqlparser::ast::ExceptionWhen>>,
    ) -> yachtsql_common::error::Result<Table> {
        let result = self.execute_statement_block(statements);

        match result {
            Ok(table) => Ok(table),
            Err(e) => {
                if let Some(handlers) = exception {
                    if let Some(handler) = handlers.first() {
                        return self.execute_statement_block(&handler.statements);
                    }
                }
                Err(e)
            }
        }
    }

    fn execute_return(
        &mut self,
        _ret: &sqlparser::ast::ReturnStatement,
    ) -> yachtsql_common::error::Result<Table> {
        Err(yachtsql_common::error::Error::InvalidQuery(
            "__RETURN__".to_string(),
        ))
    }

    fn execute_raise(
        &mut self,
        raise: &sqlparser::ast::RaiseStatement,
    ) -> yachtsql_common::error::Result<Table> {
        let message = match &raise.value {
            Some(sqlparser::ast::RaiseStatementValue::UsingMessage(e))
            | Some(sqlparser::ast::RaiseStatementValue::Expr(e)) => self
                .evaluate_expr(e)?
                .as_str()
                .map(|s| s.to_string())
                .unwrap_or_else(|| "Error raised".to_string()),
            None => "Error raised".to_string(),
        };
        Err(yachtsql_common::error::Error::InvalidQuery(message))
    }

    fn execute_immediate(
        &mut self,
        parameters: &[sqlparser::ast::Expr],
    ) -> yachtsql_common::error::Result<Table> {
        let sql_value = parameters
            .first()
            .map(|p| self.evaluate_expr(p))
            .transpose()?
            .ok_or_else(|| {
                yachtsql_common::error::Error::InvalidQuery(
                    "EXECUTE IMMEDIATE requires a string argument".to_string(),
                )
            })?;

        let sql = sql_value.as_str().map(|s| s.to_string()).ok_or_else(|| {
            yachtsql_common::error::Error::InvalidQuery(
                "EXECUTE IMMEDIATE requires a string argument".to_string(),
            )
        })?;

        self.execute_sql(&sql)
    }

    fn execute_case_statement(
        &mut self,
        case_stmt: &sqlparser::ast::CaseStatement,
    ) -> yachtsql_common::error::Result<Table> {
        let match_operand = case_stmt
            .match_expr
            .as_ref()
            .map(|e| self.evaluate_expr(e))
            .transpose()?;

        for branch in &case_stmt.when_blocks {
            if let Some(condition) = &branch.condition {
                let cond_val = self.evaluate_expr(condition)?;

                let matches = match &match_operand {
                    Some(operand) => operand == &cond_val,
                    None => cond_val.as_bool().unwrap_or(false),
                };

                if matches {
                    return self.execute_conditional_statements(&branch.conditional_statements);
                }
            }
        }

        if let Some(else_block) = &case_stmt.else_block {
            return self.execute_statement_block(else_block.statements());
        }

        Ok(Table::empty(yachtsql_storage::Schema::new()))
    }

    fn execute_assert(
        &mut self,
        condition: &sqlparser::ast::Expr,
        message: Option<&sqlparser::ast::Expr>,
    ) -> yachtsql_common::error::Result<Table> {
        let cond_val = self.evaluate_expr(condition)?;
        let is_true = cond_val.as_bool().unwrap_or(false);

        if is_true {
            Ok(Table::empty(yachtsql_storage::Schema::new()))
        } else {
            let msg = message
                .map(|m| self.evaluate_expr(m))
                .transpose()?
                .and_then(|v| v.as_str().map(|s| s.to_string()))
                .unwrap_or_else(|| "Assertion failed".to_string());
            Err(yachtsql_common::error::Error::InvalidQuery(format!(
                "ASSERT failed: {}",
                msg
            )))
        }
    }

    fn execute_statement_block(
        &mut self,
        statements: &[sqlparser::ast::Statement],
    ) -> yachtsql_common::error::Result<Table> {
        let mut last_result = Table::empty(yachtsql_storage::Schema::new());
        for stmt in statements {
            match self.execute_statement(stmt) {
                Ok(result) => last_result = result,
                Err(e) if e.to_string().contains("__RETURN__") => {
                    return Ok(last_result);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(last_result)
    }

    fn execute_conditional_statements(
        &mut self,
        statements: &sqlparser::ast::ConditionalStatements,
    ) -> yachtsql_common::error::Result<Table> {
        match statements {
            sqlparser::ast::ConditionalStatements::Sequence { statements } => {
                self.execute_statement_block(statements)
            }
            sqlparser::ast::ConditionalStatements::BeginEnd(begin_end) => {
                self.execute_statement_block(&begin_end.statements)
            }
        }
    }

    fn evaluate_expr(
        &mut self,
        expr: &sqlparser::ast::Expr,
    ) -> yachtsql_common::error::Result<yachtsql_common::types::Value> {
        use sqlparser::ast::Expr as SqlExpr;
        use yachtsql_common::types::Value;

        match expr {
            SqlExpr::Value(val) => self.evaluate_value(val),
            SqlExpr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                if name.starts_with("@@") {
                    self.session
                        .get_system_variable(&name)
                        .cloned()
                        .ok_or(yachtsql_common::error::Error::ColumnNotFound(name))
                } else {
                    self.session
                        .get_variable(&name)
                        .cloned()
                        .ok_or(yachtsql_common::error::Error::ColumnNotFound(name))
                }
            }
            SqlExpr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expr(left)?;
                let right_val = self.evaluate_expr(right)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }
            SqlExpr::Nested(inner) => self.evaluate_expr(inner),
            SqlExpr::Subquery(query) => {
                let result = self.execute_sql(&query.to_string())?;
                let rows = result.rows()?;
                if let Some(row) = rows.first() {
                    if let Some(val) = row.values().first() {
                        return Ok(val.clone());
                    }
                }
                Ok(Value::Null)
            }
            SqlExpr::Function(_) => {
                let result = self.execute_sql(&format!("SELECT {}", expr))?;
                let rows = result.rows()?;
                if let Some(row) = rows.first() {
                    if let Some(val) = row.values().first() {
                        return Ok(val.clone());
                    }
                }
                Ok(Value::Null)
            }
            SqlExpr::IsNotNull(inner) => {
                let val = self.evaluate_expr(inner)?;
                Ok(Value::Bool(!val.is_null()))
            }
            SqlExpr::IsNull(inner) => {
                let val = self.evaluate_expr(inner)?;
                Ok(Value::Bool(val.is_null()))
            }
            _ => {
                let result = self.execute_sql(&format!("SELECT {}", expr))?;
                let rows = result.rows()?;
                if let Some(row) = rows.first() {
                    if let Some(val) = row.values().first() {
                        return Ok(val.clone());
                    }
                }
                Ok(Value::Null)
            }
        }
    }

    fn evaluate_value(
        &self,
        val: &sqlparser::ast::ValueWithSpan,
    ) -> yachtsql_common::error::Result<yachtsql_common::types::Value> {
        use sqlparser::ast::Value as SqlValue;
        use yachtsql_common::types::Value;

        match &val.value {
            SqlValue::Number(n, _) => {
                if n.contains('.') {
                    Ok(Value::Float64(ordered_float::OrderedFloat(
                        n.parse().unwrap_or(0.0),
                    )))
                } else {
                    Ok(Value::Int64(n.parse().unwrap_or(0)))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::String(s.clone()))
            }
            SqlValue::Boolean(b) => Ok(Value::Bool(*b)),
            SqlValue::Null => Ok(Value::Null),
            _ => Ok(Value::Null),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &yachtsql_common::types::Value,
        op: &sqlparser::ast::BinaryOperator,
        right: &yachtsql_common::types::Value,
    ) -> yachtsql_common::error::Result<yachtsql_common::types::Value> {
        use sqlparser::ast::BinaryOperator;
        use yachtsql_common::types::Value;

        match op {
            BinaryOperator::Eq => Ok(Value::Bool(left == right)),
            BinaryOperator::NotEq => Ok(Value::Bool(left != right)),
            BinaryOperator::Lt => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l < r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(l < r)),
                _ => Ok(Value::Bool(false)),
            },
            BinaryOperator::Gt => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l > r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(l > r)),
                _ => Ok(Value::Bool(false)),
            },
            BinaryOperator::LtEq => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l <= r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(l <= r)),
                _ => Ok(Value::Bool(false)),
            },
            BinaryOperator::GtEq => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Bool(l >= r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Bool(l >= r)),
                _ => Ok(Value::Bool(false)),
            },
            BinaryOperator::And => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l && r))
            }
            BinaryOperator::Or => {
                let l = left.as_bool().unwrap_or(false);
                let r = right.as_bool().unwrap_or(false);
                Ok(Value::Bool(l || r))
            }
            BinaryOperator::Plus => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l + r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l + *r)),
                _ => Ok(Value::Null),
            },
            BinaryOperator::Minus => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l - r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l - *r)),
                _ => Ok(Value::Null),
            },
            BinaryOperator::Multiply => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) => Ok(Value::Int64(l * r)),
                (Value::Float64(l), Value::Float64(r)) => Ok(Value::Float64(*l * *r)),
                _ => Ok(Value::Null),
            },
            BinaryOperator::Divide => match (left, right) {
                (Value::Int64(l), Value::Int64(r)) if *r != 0 => Ok(Value::Int64(l / r)),
                (Value::Float64(l), Value::Float64(r)) if r.0 != 0.0 => Ok(Value::Float64(*l / *r)),
                _ => Err(yachtsql_common::error::Error::InvalidQuery(
                    "Division by zero".to_string(),
                )),
            },
            _ => Ok(Value::Null),
        }
    }

    pub fn execute(&mut self, plan: &PhysicalPlan) -> yachtsql_common::error::Result<Table> {
        let mut executor = PlanExecutor::new(&mut self.catalog, &mut self.session);
        executor.execute(plan)
    }

    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    pub fn catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }
}

impl Default for QueryExecutor {
    fn default() -> Self {
        Self::new()
    }
}

pub fn execute(
    catalog: &mut Catalog,
    session: &mut Session,
    plan: &PhysicalPlan,
) -> yachtsql_common::error::Result<Table> {
    let mut executor = PlanExecutor::new(catalog, session);
    executor.execute(plan)
}
