use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{Expr, LiteralValue};

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn format_json_path_key(key: &str) -> String {
        if key.parse::<usize>().is_ok() {
            format!("$[{}]", key)
        } else {
            format!("$.{}", key)
        }
    }

    pub(super) fn make_json_arrow_function(
        &self,
        func_name: &str,
        left: &ast::Expr,
        right: &ast::Expr,
    ) -> Result<Expr> {
        let json_col = self.sql_expr_to_expr(left)?;
        let key = match right {
            ast::Expr::Value(ast::ValueWithSpan {
                value: ast::Value::SingleQuotedString(s),
                ..
            }) => s.clone(),
            ast::Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(n, _),
                ..
            }) => n.to_string(),
            _ => {
                return Err(Error::invalid_query(
                    "JSON arrow operator right side must be string or number".to_string(),
                ));
            }
        };
        let path = Self::format_json_path_key(&key);
        Ok(Expr::Function {
            name: yachtsql_ir::FunctionName::parse(func_name),
            args: vec![json_col, Expr::Literal(LiteralValue::String(path))],
        })
    }

    pub(super) fn make_json_path_array_function(
        &self,
        func_name: &str,
        left: &ast::Expr,
        right: &ast::Expr,
    ) -> Result<Expr> {
        let json_col = self.sql_expr_to_expr(left)?;
        let path_array = self.sql_expr_to_expr(right)?;
        Ok(Expr::Function {
            name: yachtsql_ir::FunctionName::parse(func_name),
            args: vec![json_col, path_array],
        })
    }

    pub(super) fn convert_trim(
        &self,
        expr: &ast::Expr,
        trim_where: &Option<ast::TrimWhereField>,
        trim_what: &Option<Box<ast::Expr>>,
        trim_characters: &Option<Vec<ast::Expr>>,
    ) -> Result<Expr> {
        let arg_expr = self.sql_expr_to_expr(expr)?;

        let func_name = match trim_where {
            Some(ast::TrimWhereField::Leading) => yachtsql_ir::FunctionName::Ltrim,
            Some(ast::TrimWhereField::Trailing) => yachtsql_ir::FunctionName::Rtrim,
            Some(ast::TrimWhereField::Both) | None => yachtsql_ir::FunctionName::Trim,
        };

        if let Some(chars) = trim_characters {
            if chars.len() == 1 {
                let char_expr = self.sql_expr_to_expr(&chars[0])?;

                Ok(Expr::Function {
                    name: yachtsql_ir::FunctionName::Custom(format!(
                        "{}_CHARS",
                        func_name.as_str()
                    )),
                    args: vec![arg_expr, char_expr],
                })
            } else if chars.is_empty() {
                Ok(Expr::Function {
                    name: func_name,
                    args: vec![arg_expr],
                })
            } else {
                Err(Error::unsupported_feature(
                    "TRIM with multiple character sets not supported".to_string(),
                ))
            }
        } else if let Some(what) = trim_what {
            let what_expr = self.sql_expr_to_expr(what)?;
            Ok(Expr::Function {
                name: yachtsql_ir::FunctionName::Custom(format!("{}_CHARS", func_name.as_str())),
                args: vec![arg_expr, what_expr],
            })
        } else {
            Ok(Expr::Function {
                name: func_name,
                args: vec![arg_expr],
            })
        }
    }

    pub(super) fn convert_interval(&self, interval: &ast::Interval) -> Result<Expr> {
        let value_str = match interval.value.as_ref() {
            ast::Expr::Value(ast::ValueWithSpan {
                value: ast::Value::Number(s, _),
                ..
            }) => s.clone(),
            ast::Expr::Value(ast::ValueWithSpan {
                value: ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s),
                ..
            }) => s.clone(),
            ast::Expr::UnaryOp {
                op: ast::UnaryOperator::Minus,
                expr,
            } => match expr.as_ref() {
                ast::Expr::Value(ast::ValueWithSpan {
                    value:
                        ast::Value::Number(s, _)
                        | ast::Value::SingleQuotedString(s)
                        | ast::Value::DoubleQuotedString(s),
                    ..
                }) => format!("-{}", s),
                _ => {
                    return Err(Error::unsupported_feature(
                        "Complex interval value expressions not supported".to_string(),
                    ));
                }
            },
            _ => {
                return Err(Error::unsupported_feature(
                    "INTERVAL value must be a number or string literal".to_string(),
                ));
            }
        };

        let unit = interval
            .leading_field
            .as_ref()
            .map(|f| f.to_string().to_uppercase());

        Ok(Expr::Function {
            name: yachtsql_ir::FunctionName::IntervalParse,
            args: vec![
                Expr::Literal(LiteralValue::String(value_str)),
                Expr::Literal(LiteralValue::String(unit.unwrap_or_default())),
            ],
        })
    }

    pub(super) fn convert_extract(
        &self,
        field: &ast::DateTimeField,
        expr: &ast::Expr,
    ) -> Result<Expr> {
        let field_name = format!("{:?}", field).to_uppercase();
        let date_arg = self.sql_expr_to_expr(expr)?;

        Ok(Expr::Function {
            name: yachtsql_ir::FunctionName::Extract,
            args: vec![Expr::Literal(LiteralValue::String(field_name)), date_arg],
        })
    }

    pub(super) fn convert_position(&self, expr: &ast::Expr, in_expr: &ast::Expr) -> Result<Expr> {
        let substring_expr = self.sql_expr_to_expr(expr)?;
        let string_expr = self.sql_expr_to_expr(in_expr)?;
        Ok(Expr::Function {
            name: yachtsql_ir::FunctionName::Position,
            args: vec![substring_expr, string_expr],
        })
    }

    pub(super) fn convert_substring(
        &self,
        expr: &ast::Expr,
        substring_from: &Option<Box<ast::Expr>>,
        substring_for: &Option<Box<ast::Expr>>,
    ) -> Result<Expr> {
        let string_expr = self.sql_expr_to_expr(expr)?;

        let mut args = vec![string_expr];

        if let Some(from_expr) = substring_from {
            args.push(self.sql_expr_to_expr(from_expr)?);
        }

        if let Some(for_expr) = substring_for {
            if substring_from.is_none() {
                args.push(Expr::Literal(LiteralValue::Int64(1)));
            }
            args.push(self.sql_expr_to_expr(for_expr)?);
        }

        Ok(Expr::Function {
            name: yachtsql_ir::FunctionName::Substring,
            args,
        })
    }
}
