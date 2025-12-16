use sqlparser::ast::{
    DataType as SqlDataType, Expr, ObjectName, Statement as SqlStatement, Value as SqlValue,
    ValueWithSpan as SqlValueWithSpan,
};
use yachtsql_core::error::{Error, Result};
use yachtsql_parser::{Parser, Statement};

#[derive(Debug, Clone)]
pub enum StatementJob {
    DDL {
        operation: DdlOperation,
        stmt: Box<SqlStatement>,
    },

    DML {
        operation: DmlOperation,
        stmt: Box<SqlStatement>,
    },

    CteDml {
        operation: DmlOperation,
        stmt: Box<SqlStatement>,
    },

    Query {
        stmt: Box<SqlStatement>,
    },

    Merge {
        operation: MergeOperation,
    },

    Utility {
        operation: UtilityOperation,
    },

    Procedure {
        name: String,
        args: Vec<sqlparser::ast::Expr>,
    },

    Copy {
        operation: CopyOperation,
    },

    Scripting {
        operation: ScriptingOperation,
    },
}

#[derive(Debug, Clone)]
pub enum DdlOperation {
    CreateTable,
    DropTable,
    AlterTable,
    CreateView,
    DropView,
    CreateMaterializedView,
    CreateSchema,
    DropSchema,
    CreateFunction,
    DropFunction,
    CreateProcedure,
    DropProcedure,
    CreateDatabase {
        name: ObjectName,
        if_not_exists: bool,
    },
    DropDatabase,
    CreateUser,
    DropUser,
    AlterUser,
    CreateRole,
    DropRole,
    AlterRole,
    Grant,
    Revoke,
    SetRole,
    SetDefaultRole,
    CreateSnapshotTable,
    DropSnapshotTable,
    CommentOn,
}

#[derive(Debug, Clone)]
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
    Truncate,
}

#[derive(Debug, Clone)]
pub struct CopyOperation {
    pub stmt: Box<SqlStatement>,
}

#[derive(Debug, Clone)]
pub struct MergeOperation {
    pub stmt: Box<SqlStatement>,
    pub merge_returning: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ScriptingOperation {
    Declare {
        names: Vec<String>,
        data_type: Option<SqlDataType>,
        default_expr: Option<Box<Expr>>,
    },
    SetVariable {
        name: String,
        value: Box<Expr>,
    },
    If {
        stmt: Box<SqlStatement>,
    },
    While {
        stmt: Box<SqlStatement>,
    },
    Loop {
        stmt: Box<SqlStatement>,
    },
    Repeat {
        stmt: Box<SqlStatement>,
    },
    BeginEnd {
        stmt: Box<SqlStatement>,
    },
    Case {
        stmt: Box<SqlStatement>,
    },
    Leave {
        label: Option<String>,
    },
    Continue {
        label: Option<String>,
    },
    Return {
        value: Option<Box<Expr>>,
    },
    ExecuteImmediate {
        stmt: Box<SqlStatement>,
    },
    Assert {
        condition: Box<Expr>,
        message: Option<String>,
    },
}

#[derive(Debug, Clone)]
pub enum UtilityOperation {
    Show {
        variable: Option<String>,
    },
    Explain {
        stmt: Box<SqlStatement>,
        analyze: bool,
        verbose: bool,
    },
    SetCapabilities {
        enable: bool,
        features: Vec<String>,
    },
    SetSearchPath {
        schemas: Vec<String>,
    },
    DescribeTable {
        table_name: ObjectName,
    },
    ShowCreateTable {
        table_name: ObjectName,
    },
    ShowTables {
        filter: Option<String>,
    },
    ShowColumns {
        table_name: ObjectName,
    },
    ExistsTable {
        table_name: ObjectName,
    },
    ExistsDatabase {
        db_name: ObjectName,
    },
    ShowUsers,
    ShowRoles,
    ShowGrants {
        user_name: Option<String>,
    },
    OptimizeTable {
        table_name: ObjectName,
    },
}

pub struct Dispatcher {
    parser: Parser,
}

impl Dispatcher {
    pub fn new() -> Self {
        Self::with_parser(Parser::new())
    }

    pub fn with_parser(parser: Parser) -> Self {
        Self { parser }
    }

    pub fn dispatch(&mut self, sql: &str) -> Result<StatementJob> {
        let statements = self
            .parser
            .parse_sql(sql)
            .map_err(|e| Error::parse_error(format!("Failed to parse SQL: {}", e)))?;

        if statements.is_empty() {
            return Err(Error::parse_error("No SQL statement provided".to_string()));
        }

        if statements.len() > 1 {
            return Err(Error::parse_error(format!(
                "Multiple statements not supported in single call (found {}). Execute statements separately.",
                statements.len()
            )));
        }

        let statement = &statements[0];
        self.classify_statement(statement)
    }

    pub fn classify_statement(&self, statement: &Statement) -> Result<StatementJob> {
        match statement {
            Statement::Standard(std_stmt) => {
                let ast = std_stmt.ast();
                let merge_returning = std_stmt.merge_returning().map(|s| s.to_string());

                match ast {
                    SqlStatement::StartTransaction {
                        statements,
                        has_end_keyword,
                        ..
                    } if !statements.is_empty() || *has_end_keyword => {
                        Ok(StatementJob::Scripting {
                            operation: ScriptingOperation::BeginEnd {
                                stmt: Box::new(ast.clone()),
                            },
                        })
                    }

                    SqlStatement::CreateTable { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateTable,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::CreateView {
                        materialized: true, ..
                    } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateMaterializedView,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::CreateView { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateView,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::CreateFunction(_) => {
                        debug_print::debug_eprintln!("[dispatcher] Matched CreateFunction");
                        Ok(StatementJob::DDL {
                            operation: DdlOperation::CreateFunction,
                            stmt: Box::new(ast.clone()),
                        })
                    }

                    SqlStatement::CreateProcedure { .. } => {
                        debug_print::debug_eprintln!("[dispatcher] Matched CreateProcedure");
                        Ok(StatementJob::DDL {
                            operation: DdlOperation::CreateProcedure,
                            stmt: Box::new(ast.clone()),
                        })
                    }

                    SqlStatement::CreateSchema { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateSchema,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Drop { object_type, .. } => {
                        use sqlparser::ast::ObjectType;
                        let operation = match object_type {
                            ObjectType::Table => DdlOperation::DropTable,
                            ObjectType::View => DdlOperation::DropView,
                            ObjectType::Schema => DdlOperation::DropSchema,
                            ObjectType::Role => DdlOperation::DropRole,
                            ObjectType::User => DdlOperation::DropUser,
                            ObjectType::Database => DdlOperation::DropDatabase,
                            _ => {
                                return Err(Error::unsupported_feature(format!(
                                    "DROP {} is not supported in BigQuery",
                                    object_type
                                )));
                            }
                        };
                        Ok(StatementJob::DDL {
                            operation,
                            stmt: Box::new(ast.clone()),
                        })
                    }

                    SqlStatement::DropProcedure { .. } => {
                        debug_print::debug_eprintln!("[dispatcher] Matched DropProcedure");
                        Ok(StatementJob::DDL {
                            operation: DdlOperation::DropProcedure,
                            stmt: Box::new(ast.clone()),
                        })
                    }

                    SqlStatement::DropFunction { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::DropFunction,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::AlterTable { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::AlterTable,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Insert { .. } => Ok(StatementJob::DML {
                        operation: DmlOperation::Insert,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Update { .. } => Ok(StatementJob::DML {
                        operation: DmlOperation::Update,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Delete { .. } => Ok(StatementJob::DML {
                        operation: DmlOperation::Delete,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Truncate { .. } => Ok(StatementJob::DML {
                        operation: DmlOperation::Truncate,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Merge { .. } => Ok(StatementJob::Merge {
                        operation: MergeOperation {
                            stmt: Box::new(ast.clone()),
                            merge_returning,
                        },
                    }),

                    SqlStatement::Query(query) => {
                        use sqlparser::ast::SetExpr;

                        match query.body.as_ref() {
                            SetExpr::Insert(_) => Ok(StatementJob::CteDml {
                                operation: DmlOperation::Insert,
                                stmt: Box::new(ast.clone()),
                            }),
                            SetExpr::Update(_) => Ok(StatementJob::CteDml {
                                operation: DmlOperation::Update,
                                stmt: Box::new(ast.clone()),
                            }),
                            SetExpr::Delete(_) => Ok(StatementJob::CteDml {
                                operation: DmlOperation::Delete,
                                stmt: Box::new(ast.clone()),
                            }),
                            _ => Ok(StatementJob::Query {
                                stmt: Box::new(ast.clone()),
                            }),
                        }
                    }

                    SqlStatement::Set(set_stmt) => self.handle_set_statement(set_stmt),

                    SqlStatement::ShowVariable { variable } => {
                        let var_name = variable
                            .iter()
                            .map(|ident| ident.value.clone())
                            .collect::<Vec<_>>()
                            .join(".");
                        Ok(StatementJob::Utility {
                            operation: UtilityOperation::Show {
                                variable: Some(var_name),
                            },
                        })
                    }

                    SqlStatement::Explain {
                        analyze,
                        verbose,
                        statement,
                        ..
                    } => Ok(StatementJob::Utility {
                        operation: UtilityOperation::Explain {
                            stmt: statement.clone(),
                            analyze: *analyze,
                            verbose: *verbose,
                        },
                    }),

                    SqlStatement::Call(function) => {
                        let name = function.name.to_string();

                        use sqlparser::ast::{FunctionArg, FunctionArgExpr, FunctionArguments};
                        let args = match &function.args {
                            FunctionArguments::List(arg_list) => arg_list
                                .args
                                .iter()
                                .filter_map(|arg| match arg {
                                    FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) => {
                                        Some(e.clone())
                                    }
                                    _ => None,
                                })
                                .collect(),
                            _ => Vec::new(),
                        };

                        Ok(StatementJob::Procedure { name, args })
                    }

                    SqlStatement::Copy { .. } => Ok(StatementJob::Copy {
                        operation: CopyOperation {
                            stmt: Box::new(ast.clone()),
                        },
                    }),

                    SqlStatement::ExplainTable { table_name, .. } => Ok(StatementJob::Utility {
                        operation: UtilityOperation::DescribeTable {
                            table_name: table_name.clone(),
                        },
                    }),

                    SqlStatement::ShowCreate {
                        obj_type: sqlparser::ast::ShowCreateObject::Table,
                        obj_name,
                    } => Ok(StatementJob::Utility {
                        operation: UtilityOperation::ShowCreateTable {
                            table_name: obj_name.clone(),
                        },
                    }),

                    SqlStatement::ShowTables { show_options, .. } => {
                        let filter_str =
                            show_options
                                .filter_position
                                .as_ref()
                                .and_then(|fp| match fp {
                                    sqlparser::ast::ShowStatementFilterPosition::Infix(f)
                                    | sqlparser::ast::ShowStatementFilterPosition::Suffix(f) => {
                                        match f {
                                            sqlparser::ast::ShowStatementFilter::Like(s)
                                            | sqlparser::ast::ShowStatementFilter::ILike(s) => {
                                                Some(s.clone())
                                            }
                                            _ => None,
                                        }
                                    }
                                });
                        Ok(StatementJob::Utility {
                            operation: UtilityOperation::ShowTables { filter: filter_str },
                        })
                    }

                    SqlStatement::ShowColumns { show_options, .. } => {
                        let table_name = show_options
                            .show_in
                            .as_ref()
                            .and_then(|si| si.parent_name.clone())
                            .unwrap_or_else(|| ObjectName(vec![]));
                        Ok(StatementJob::Utility {
                            operation: UtilityOperation::ShowColumns { table_name },
                        })
                    }

                    SqlStatement::CreateDatabase {
                        db_name,
                        if_not_exists,
                        ..
                    } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateDatabase {
                            name: db_name.clone(),
                            if_not_exists: *if_not_exists,
                        },
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::CreateUser(_) => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateUser,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::CreateRole { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CreateRole,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::AlterRole { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::AlterRole,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Grant { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::Grant,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Revoke { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::Revoke,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Declare { stmts } => {
                        if stmts.is_empty() {
                            return Err(Error::invalid_query(
                                "DECLARE statement requires at least one variable".to_string(),
                            ));
                        }
                        let first = &stmts[0];
                        let names: Vec<String> = first
                            .names
                            .iter()
                            .map(|ident| ident.value.clone())
                            .collect();
                        let data_type = first.data_type.clone();
                        let default_expr = first.assignment.as_ref().map(|a| {
                            use sqlparser::ast::DeclareAssignment;
                            match a {
                                DeclareAssignment::Expr(e)
                                | DeclareAssignment::Default(e)
                                | DeclareAssignment::DuckAssignment(e)
                                | DeclareAssignment::MsSqlAssignment(e)
                                | DeclareAssignment::For(e) => e.clone(),
                            }
                        });
                        Ok(StatementJob::Scripting {
                            operation: ScriptingOperation::Declare {
                                names,
                                data_type,
                                default_expr,
                            },
                        })
                    }

                    SqlStatement::OptimizeTable { name, .. } => Ok(StatementJob::Utility {
                        operation: UtilityOperation::OptimizeTable {
                            table_name: name.clone(),
                        },
                    }),

                    SqlStatement::If(_) => Ok(StatementJob::Scripting {
                        operation: ScriptingOperation::If {
                            stmt: Box::new(ast.clone()),
                        },
                    }),

                    SqlStatement::While(_) => Ok(StatementJob::Scripting {
                        operation: ScriptingOperation::While {
                            stmt: Box::new(ast.clone()),
                        },
                    }),

                    SqlStatement::Case(_) => Ok(StatementJob::Scripting {
                        operation: ScriptingOperation::Case {
                            stmt: Box::new(ast.clone()),
                        },
                    }),

                    SqlStatement::Return(_) => Ok(StatementJob::Scripting {
                        operation: ScriptingOperation::Return { value: None },
                    }),

                    SqlStatement::Execute {
                        immediate: true, ..
                    } => Ok(StatementJob::Scripting {
                        operation: ScriptingOperation::ExecuteImmediate {
                            stmt: Box::new(ast.clone()),
                        },
                    }),

                    SqlStatement::Comment { .. } => Ok(StatementJob::DDL {
                        operation: DdlOperation::CommentOn,
                        stmt: Box::new(ast.clone()),
                    }),

                    SqlStatement::Assert { condition, message } => {
                        let message_str = message.as_ref().map(|m| m.to_string());
                        Ok(StatementJob::Scripting {
                            operation: ScriptingOperation::Assert {
                                condition: Box::new(condition.clone()),
                                message: message_str,
                            },
                        })
                    }

                    _ => Err(Error::unsupported_feature(format!(
                        "Statement type {:?} is not yet supported",
                        ast
                    ))),
                }
            }

            Statement::Custom(_) => Err(Error::InternalError(
                "Custom statements should be handled before dispatcher".to_string(),
            )),
        }
    }
}

impl Dispatcher {
    fn handle_set_statement(&self, set_stmt: &sqlparser::ast::Set) -> Result<StatementJob> {
        use sqlparser::ast::Set;

        match set_stmt {
            Set::SingleAssignment {
                scope,
                hivevar,
                variable,
                values,
            } => {
                if scope.is_some() || *hivevar {
                    return Err(Error::unsupported_feature(
                        "LOCAL/HIVEVAR modifiers are not supported".to_string(),
                    ));
                }

                let variable_name = Self::resolve_set_variable_name(variable)?;
                self.dispatch_single_assignment(variable_name, values)
            }
            Set::SetRole { .. } => Ok(StatementJob::DDL {
                operation: DdlOperation::SetRole,
                stmt: Box::new(SqlStatement::Set(set_stmt.clone())),
            }),
            _ => Err(Error::unsupported_feature(
                "Only simple SET assignments are supported".to_string(),
            )),
        }
    }

    fn dispatch_single_assignment(
        &self,
        variable_name: String,
        value: &[Expr],
    ) -> Result<StatementJob> {
        let key = variable_name.to_ascii_lowercase();

        if key == "yachtsql.capability.enable" || key == "yachtsql.capability.disable" {
            let enable = key.ends_with("enable");
            let features = Self::parse_capability_feature_list(value)?;
            return Ok(StatementJob::Utility {
                operation: UtilityOperation::SetCapabilities { enable, features },
            });
        }

        if key == "search_path" {
            let schemas = Self::parse_search_path_value(value)?;
            return Ok(StatementJob::Utility {
                operation: UtilityOperation::SetSearchPath { schemas },
            });
        }

        if value.len() == 1 {
            return Ok(StatementJob::Scripting {
                operation: ScriptingOperation::SetVariable {
                    name: variable_name,
                    value: Box::new(value[0].clone()),
                },
            });
        }

        Err(Error::unsupported_feature(format!(
            "SET variable '{}' not supported",
            variable_name
        )))
    }

    fn parse_search_path_value(value: &[Expr]) -> Result<Vec<String>> {
        if value.is_empty() {
            return Err(Error::invalid_query(
                "SET search_path requires at least one schema".to_string(),
            ));
        }

        let mut schemas = Vec::new();
        for expr in value {
            match expr {
                Expr::Identifier(ident) => {
                    schemas.push(ident.value.clone());
                }
                Expr::Value(SqlValueWithSpan {
                    value: SqlValue::SingleQuotedString(s),
                    ..
                })
                | Expr::Value(SqlValueWithSpan {
                    value: SqlValue::DoubleQuotedString(s),
                    ..
                }) => {
                    schemas.push(s.clone());
                }
                other => {
                    return Err(Error::invalid_query(format!(
                        "SET search_path value must be a schema name, got: {:?}",
                        other
                    )));
                }
            }
        }

        Ok(schemas)
    }

    fn resolve_set_variable_name(variable: &ObjectName) -> Result<String> {
        let parts: Vec<String> = variable
            .0
            .iter()
            .map(|part| {
                part.as_ident()
                    .map(|ident| ident.value.clone())
                    .ok_or_else(|| {
                        Error::invalid_query(
                            "SET variable must be an identifier (optionally qualified)".to_string(),
                        )
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        if parts.is_empty() {
            return Err(Error::InvalidOperation(
                "SET statement requires a variable name".to_string(),
            ));
        }

        Ok(parts.join("."))
    }

    fn parse_capability_feature_list(value: &[Expr]) -> Result<Vec<String>> {
        let raw = Self::extract_set_value(value)?;
        let features: Vec<String> = raw
            .split(',')
            .map(|item| item.trim())
            .filter(|item| !item.is_empty())
            .map(|item| item.to_string())
            .collect();

        if features.is_empty() {
            return Err(Error::invalid_query(
                "SET yachtsql.capability requires at least one feature identifier".to_string(),
            ));
        }

        Ok(features)
    }

    fn extract_set_value(value: &[Expr]) -> Result<String> {
        if value.is_empty() {
            return Err(Error::invalid_query(
                "SET statement requires a value".to_string(),
            ));
        }

        if value.len() != 1 {
            return Err(Error::unsupported_feature(
                "SET statement with multiple values is not supported".to_string(),
            ));
        }

        match &value[0] {
            Expr::Value(SqlValueWithSpan {
                value: SqlValue::SingleQuotedString(s),
                ..
            })
            | Expr::Value(SqlValueWithSpan {
                value: SqlValue::DoubleQuotedString(s),
                ..
            }) => Ok(s.clone()),
            Expr::Identifier(ident) => Ok(ident.value.clone()),
            Expr::CompoundIdentifier(idents) => Ok(idents
                .iter()
                .map(|ident| ident.value.clone())
                .collect::<Vec<_>>()
                .join(".")),
            other => Err(Error::unsupported_feature(format!(
                "SET value expression not supported: {:?}",
                other
            ))),
        }
    }
}

impl Default for Dispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dispatch_set_capability_enable() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("SET yachtsql.capability.enable = 'F001,F051'");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::Utility {
                operation: UtilityOperation::SetCapabilities { enable, features },
            } => {
                assert!(enable);
                assert_eq!(features, vec!["F001".to_string(), "F051".to_string()]);
            }
            other => panic!("Expected capability utility, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_set_capability_disable() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("SET yachtsql.capability.disable = 'F001'");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::Utility {
                operation: UtilityOperation::SetCapabilities { enable, features },
            } => {
                assert!(!enable);
                assert_eq!(features, vec!["F001".to_string()]);
            }
            other => panic!("Expected capability disable, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_create_table() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("CREATE TABLE users (id INT64, name STRING)");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::DDL {
                operation: DdlOperation::CreateTable,
                ..
            } => {}
            other => panic!("Expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_create_table_if_not_exists() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("CREATE TABLE IF NOT EXISTS users (id INT64)");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::DDL {
                operation: DdlOperation::CreateTable,
                ..
            } => {}
            other => panic!("Expected CreateTable, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_drop_table() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("DROP TABLE users");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::DDL {
                operation: DdlOperation::DropTable,
                ..
            } => {}
            other => panic!("Expected DropTable, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_insert() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::DML {
                operation: DmlOperation::Insert,
                ..
            } => {}
            other => panic!("Expected Insert, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_update() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("UPDATE users SET name = 'Bob' WHERE id = 1");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::DML {
                operation: DmlOperation::Update,
                ..
            } => {}
            other => panic!("Expected Update, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_delete() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("DELETE FROM users WHERE id = 1");
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::DML {
                operation: DmlOperation::Delete,
                ..
            } => {}
            other => panic!("Expected Delete, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_merge() {
        let mut dispatcher = Dispatcher::new();
        let sql = "MERGE INTO target USING source ON target.id = source.id \
                   WHEN MATCHED THEN UPDATE SET value = source.value \
                   WHEN NOT MATCHED THEN INSERT (id, value) VALUES (source.id, source.value)";
        let result = dispatcher.dispatch(sql);
        assert!(result.is_ok());

        match result.unwrap() {
            StatementJob::Merge { .. } => {}
            other => panic!("Expected Merge, got {:?}", other),
        }
    }

    #[test]
    fn test_dispatch_empty_sql() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("");
        assert!(result.is_err());
    }

    #[test]
    fn test_dispatch_invalid_sql() {
        let mut dispatcher = Dispatcher::new();
        let result = dispatcher.dispatch("INVALID SQL SYNTAX");
        assert!(result.is_err());
    }
}
