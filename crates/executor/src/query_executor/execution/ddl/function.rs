use sqlparser::ast::{CreateFunctionBody, Statement as SqlStatement};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::DataType;

use crate::QueryExecutor;
use crate::query_executor::execution::session::{
    ProcedureDefinition, ProcedureParameter, ProcedureParameterMode, UdfDefinition,
};

pub trait FunctionExecutor {
    fn execute_create_function(&mut self, stmt: &SqlStatement) -> Result<()>;
    fn execute_drop_function(&mut self, stmt: &SqlStatement) -> Result<()>;
}

impl FunctionExecutor for QueryExecutor {
    fn execute_create_function(&mut self, stmt: &SqlStatement) -> Result<()> {
        debug_print::debug_eprintln!("[function] execute_create_function called with {:?}", stmt);
        if let SqlStatement::CreateFunction(create_func) = stmt {
            let func_name = create_func.name.to_string();
            debug_print::debug_eprintln!("[function] Creating function: {}", func_name);

            let parameters: Vec<String> = create_func
                .args
                .as_ref()
                .map(|args| {
                    args.iter()
                        .map(|arg| {
                            arg.name
                                .as_ref()
                                .map(|n| n.value.clone())
                                .unwrap_or_else(|| {
                                    format!(
                                        "arg{}",
                                        args.iter().position(|a| a == arg).unwrap_or(0)
                                    )
                                })
                        })
                        .collect()
                })
                .unwrap_or_default();

            let body_expr = match &create_func.function_body {
                Some(CreateFunctionBody::AsBeforeOptions(expr)) => expr.clone(),
                Some(CreateFunctionBody::AsAfterOptions(expr)) => expr.clone(),
                Some(CreateFunctionBody::Return(expr)) => expr.clone(),
                Some(CreateFunctionBody::AsReturnExpr(expr)) => expr.clone(),
                Some(CreateFunctionBody::AsBeginEnd(_)) => {
                    return Err(Error::unsupported_feature(
                        "BEGIN...END function bodies are not supported",
                    ));
                }
                Some(CreateFunctionBody::AsReturnSelect(_)) => {
                    return Err(Error::unsupported_feature(
                        "SELECT function bodies are not supported",
                    ));
                }
                None => {
                    return Err(Error::invalid_query(
                        "CREATE FUNCTION requires a function body",
                    ));
                }
            };

            if create_func.or_replace {
                self.session.drop_udf(&func_name);
            } else if !create_func.if_not_exists && self.session.has_udf(&func_name) {
                return Err(Error::invalid_query(format!(
                    "Function '{}' already exists",
                    func_name
                )));
            }

            if !self.session.has_udf(&func_name) {
                let udf_def = UdfDefinition {
                    parameters,
                    body: body_expr,
                };
                self.session.register_udf(func_name, udf_def);
            }

            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected CREATE FUNCTION statement".to_string(),
            ))
        }
    }

    fn execute_drop_function(&mut self, stmt: &SqlStatement) -> Result<()> {
        if let SqlStatement::DropFunction {
            func_desc,
            if_exists,
            ..
        } = stmt
        {
            for desc in func_desc {
                let func_name = desc.name.to_string();

                if !self.session.drop_udf(&func_name) && !*if_exists {
                    return Err(Error::invalid_query(format!(
                        "Function '{}' does not exist",
                        func_name
                    )));
                }
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected DROP FUNCTION statement".to_string(),
            ))
        }
    }
}

pub trait ProcedureExecutor {
    fn execute_create_procedure(&mut self, stmt: &SqlStatement) -> Result<()>;
    fn execute_drop_procedure(&mut self, stmt: &SqlStatement) -> Result<()>;
}

impl ProcedureExecutor for QueryExecutor {
    fn execute_create_procedure(&mut self, stmt: &SqlStatement) -> Result<()> {
        debug_print::debug_eprintln!(
            "[procedure] execute_create_procedure called with {:?}",
            stmt
        );

        if let SqlStatement::CreateProcedure {
            name,
            or_alter,
            params,
            body,
            ..
        } = stmt
        {
            let proc_name = name.to_string();
            debug_print::debug_eprintln!("[procedure] Creating procedure: {}", proc_name);

            let parameters: Vec<ProcedureParameter> = params
                .as_ref()
                .map(|p| {
                    p.iter()
                        .map(|param| {
                            let mode = match &param.mode {
                                Some(sqlparser::ast::ArgMode::In) => ProcedureParameterMode::In,
                                Some(sqlparser::ast::ArgMode::Out) => ProcedureParameterMode::Out,
                                Some(sqlparser::ast::ArgMode::InOut) => {
                                    ProcedureParameterMode::InOut
                                }
                                None => ProcedureParameterMode::In,
                            };
                            let param_name = param.name.value.clone();
                            let data_type = convert_sql_datatype(&param.data_type);
                            ProcedureParameter {
                                name: param_name,
                                data_type,
                                mode,
                            }
                        })
                        .collect()
                })
                .unwrap_or_default();

            let body_stmts = body.statements().clone();

            let is_or_replace = *or_alter;

            if is_or_replace {
                self.session.drop_procedure(&proc_name);
            } else if self.session.has_procedure(&proc_name) {
                return Err(Error::invalid_query(format!(
                    "Procedure '{}' already exists",
                    proc_name
                )));
            }

            if !self.session.has_procedure(&proc_name) {
                let proc_def = ProcedureDefinition {
                    parameters,
                    body: body_stmts,
                };
                self.session.register_procedure(proc_name, proc_def);
            }

            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected CREATE PROCEDURE statement".to_string(),
            ))
        }
    }

    fn execute_drop_procedure(&mut self, stmt: &SqlStatement) -> Result<()> {
        debug_print::debug_eprintln!("[procedure] execute_drop_procedure called with {:?}", stmt);

        if let SqlStatement::DropProcedure {
            proc_desc,
            if_exists,
            ..
        } = stmt
        {
            for desc in proc_desc {
                let proc_name = desc.name.to_string();

                if !self.session.drop_procedure(&proc_name) && !*if_exists {
                    return Err(Error::invalid_query(format!(
                        "Procedure '{}' does not exist",
                        proc_name
                    )));
                }
            }
            Ok(())
        } else {
            Err(Error::InternalError(
                "Expected DROP PROCEDURE statement".to_string(),
            ))
        }
    }
}

fn convert_sql_datatype(sql_dt: &sqlparser::ast::DataType) -> DataType {
    match sql_dt {
        sqlparser::ast::DataType::Int64 | sqlparser::ast::DataType::BigInt(_) => DataType::Int64,
        sqlparser::ast::DataType::Int(_) | sqlparser::ast::DataType::Integer(_) => DataType::Int64,
        sqlparser::ast::DataType::Float64 | sqlparser::ast::DataType::Double(_) => {
            DataType::Float64
        }
        sqlparser::ast::DataType::Float(_) | sqlparser::ast::DataType::Real => DataType::Float32,
        sqlparser::ast::DataType::String(_)
        | sqlparser::ast::DataType::Text
        | sqlparser::ast::DataType::Varchar(_) => DataType::String,
        sqlparser::ast::DataType::Boolean => DataType::Bool,
        _ => DataType::String,
    }
}
