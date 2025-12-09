use sqlparser::ast::{CreateFunctionBody, Statement as SqlStatement};
use yachtsql_core::error::{Error, Result};

use crate::QueryExecutor;
use crate::query_executor::execution::session::UdfDefinition;

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
