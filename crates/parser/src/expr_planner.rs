use rust_decimal::Decimal;
use sqlparser::ast;
use yachtsql_common::error::{Error, Result};
use yachtsql_ir::{
    AggregateFunction, BinaryOp, Expr, Literal, PlanSchema, ScalarFunction, UnaryOp, WhenClause,
};

pub struct ExprPlanner;

impl ExprPlanner {
    pub fn plan_expr(sql_expr: &ast::Expr, schema: &PlanSchema) -> Result<Expr> {
        match sql_expr {
            ast::Expr::Identifier(ident) => Self::resolve_column(&ident.value, None, schema),

            ast::Expr::CompoundIdentifier(parts) => {
                let (table, name) = if parts.len() > 1 {
                    (
                        Some(
                            parts[..parts.len() - 1]
                                .iter()
                                .map(|p| p.value.clone())
                                .collect::<Vec<_>>()
                                .join("."),
                        ),
                        parts.last().map(|p| p.value.clone()).unwrap_or_default(),
                    )
                } else {
                    (
                        None,
                        parts.first().map(|p| p.value.clone()).unwrap_or_default(),
                    )
                };
                Self::resolve_column(&name, table.as_deref(), schema)
            }

            ast::Expr::Value(val) => Ok(Expr::Literal(Self::plan_literal(&val.value)?)),

            ast::Expr::BinaryOp { left, op, right } => {
                let left = Self::plan_expr(left, schema)?;
                let right = Self::plan_expr(right, schema)?;
                let op = Self::plan_binary_op(op)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(left),
                    op,
                    right: Box::new(right),
                })
            }

            ast::Expr::UnaryOp { op, expr } => {
                let expr = Self::plan_expr(expr, schema)?;
                let op = Self::plan_unary_op(op)?;
                Ok(Expr::UnaryOp {
                    op,
                    expr: Box::new(expr),
                })
            }

            ast::Expr::Function(func) => Self::plan_function(func, schema),

            ast::Expr::IsNull(inner) => {
                let expr = Self::plan_expr(inner, schema)?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: false,
                })
            }

            ast::Expr::IsNotNull(inner) => {
                let expr = Self::plan_expr(inner, schema)?;
                Ok(Expr::IsNull {
                    expr: Box::new(expr),
                    negated: true,
                })
            }

            ast::Expr::Nested(inner) => Self::plan_expr(inner, schema),

            ast::Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => Self::plan_case(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                schema,
            ),

            ast::Expr::Cast {
                expr, data_type, ..
            } => {
                let expr = Self::plan_expr(expr, schema)?;
                let data_type = Self::plan_data_type(data_type)?;
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    data_type,
                })
            }

            ast::Expr::InList {
                expr,
                list,
                negated,
            } => {
                let expr = Self::plan_expr(expr, schema)?;
                let list = list
                    .iter()
                    .map(|e| Self::plan_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Expr::InList {
                    expr: Box::new(expr),
                    list,
                    negated: *negated,
                })
            }

            ast::Expr::Between {
                expr,
                low,
                high,
                negated,
            } => {
                let expr = Self::plan_expr(expr, schema)?;
                let low = Self::plan_expr(low, schema)?;
                let high = Self::plan_expr(high, schema)?;
                Ok(Expr::Between {
                    expr: Box::new(expr),
                    low: Box::new(low),
                    high: Box::new(high),
                    negated: *negated,
                })
            }

            ast::Expr::Like {
                expr,
                pattern,
                negated,
                ..
            } => {
                let expr = Self::plan_expr(expr, schema)?;
                let pattern = Self::plan_expr(pattern, schema)?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: *negated,
                    case_insensitive: false,
                })
            }

            ast::Expr::ILike {
                expr,
                pattern,
                negated,
                ..
            } => {
                let expr = Self::plan_expr(expr, schema)?;
                let pattern = Self::plan_expr(pattern, schema)?;
                Ok(Expr::Like {
                    expr: Box::new(expr),
                    pattern: Box::new(pattern),
                    negated: *negated,
                    case_insensitive: true,
                })
            }

            ast::Expr::IsTrue(inner) => {
                let expr = Self::plan_expr(inner, schema)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(true))),
                })
            }

            ast::Expr::IsFalse(inner) => {
                let expr = Self::plan_expr(inner, schema)?;
                Ok(Expr::BinaryOp {
                    left: Box::new(expr),
                    op: BinaryOp::Eq,
                    right: Box::new(Expr::Literal(Literal::Bool(false))),
                })
            }

            ast::Expr::Array(arr) => {
                let elements = arr
                    .elem
                    .iter()
                    .map(|e| Self::plan_expr(e, schema))
                    .collect::<Result<Vec<_>>>()?;

                let literals: Result<Vec<Literal>> = elements
                    .into_iter()
                    .map(|e| match e {
                        Expr::Literal(lit) => Ok(lit),
                        _ => Err(Error::parse_error("Array elements must be literals")),
                    })
                    .collect();

                Ok(Expr::Literal(Literal::Array(literals?)))
            }

            _ => Err(Error::unsupported(format!(
                "Unsupported expression: {:?}",
                sql_expr
            ))),
        }
    }

    fn resolve_column(name: &str, table: Option<&str>, schema: &PlanSchema) -> Result<Expr> {
        let index = schema.field_index_qualified(name, table);

        Ok(Expr::Column {
            table: table.map(String::from),
            name: name.to_string(),
            index,
        })
    }

    fn plan_literal(val: &ast::Value) -> Result<Literal> {
        match val {
            ast::Value::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Literal::Int64(i))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Literal::Float64(ordered_float::OrderedFloat(f)))
                } else if let Ok(d) = n.parse::<Decimal>() {
                    Ok(Literal::Numeric(d))
                } else {
                    Err(Error::parse_error(format!("Invalid number: {}", n)))
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                Ok(Literal::String(s.clone()))
            }
            ast::Value::Boolean(b) => Ok(Literal::Bool(*b)),
            ast::Value::Null => Ok(Literal::Null),
            ast::Value::HexStringLiteral(s) => {
                let bytes = hex::decode(s)
                    .map_err(|e| Error::parse_error(format!("Invalid hex string: {}", e)))?;
                Ok(Literal::Bytes(bytes))
            }
            _ => Err(Error::unsupported(format!(
                "Unsupported literal: {:?}",
                val
            ))),
        }
    }

    fn plan_binary_op(op: &ast::BinaryOperator) -> Result<BinaryOp> {
        match op {
            ast::BinaryOperator::Plus => Ok(BinaryOp::Add),
            ast::BinaryOperator::Minus => Ok(BinaryOp::Sub),
            ast::BinaryOperator::Multiply => Ok(BinaryOp::Mul),
            ast::BinaryOperator::Divide => Ok(BinaryOp::Div),
            ast::BinaryOperator::Modulo => Ok(BinaryOp::Mod),
            ast::BinaryOperator::Eq => Ok(BinaryOp::Eq),
            ast::BinaryOperator::NotEq => Ok(BinaryOp::NotEq),
            ast::BinaryOperator::Lt => Ok(BinaryOp::Lt),
            ast::BinaryOperator::LtEq => Ok(BinaryOp::LtEq),
            ast::BinaryOperator::Gt => Ok(BinaryOp::Gt),
            ast::BinaryOperator::GtEq => Ok(BinaryOp::GtEq),
            ast::BinaryOperator::And => Ok(BinaryOp::And),
            ast::BinaryOperator::Or => Ok(BinaryOp::Or),
            ast::BinaryOperator::StringConcat => Ok(BinaryOp::Concat),
            ast::BinaryOperator::BitwiseAnd => Ok(BinaryOp::BitwiseAnd),
            ast::BinaryOperator::BitwiseOr => Ok(BinaryOp::BitwiseOr),
            ast::BinaryOperator::BitwiseXor => Ok(BinaryOp::BitwiseXor),
            ast::BinaryOperator::PGBitwiseShiftLeft => Ok(BinaryOp::ShiftLeft),
            ast::BinaryOperator::PGBitwiseShiftRight => Ok(BinaryOp::ShiftRight),
            _ => Err(Error::unsupported(format!(
                "Unsupported binary operator: {:?}",
                op
            ))),
        }
    }

    fn plan_unary_op(op: &ast::UnaryOperator) -> Result<UnaryOp> {
        match op {
            ast::UnaryOperator::Not => Ok(UnaryOp::Not),
            ast::UnaryOperator::Minus => Ok(UnaryOp::Minus),
            ast::UnaryOperator::Plus => Ok(UnaryOp::Plus),
            ast::UnaryOperator::PGBitwiseNot => Ok(UnaryOp::BitwiseNot),
            _ => Err(Error::unsupported(format!(
                "Unsupported unary operator: {:?}",
                op
            ))),
        }
    }

    fn plan_function(func: &ast::Function, schema: &PlanSchema) -> Result<Expr> {
        let name = func.name.to_string().to_uppercase();

        if let Some(agg_func) = Self::try_aggregate_function(&name) {
            let distinct = matches!(
                &func.args,
                ast::FunctionArguments::List(list) if list.duplicate_treatment == Some(ast::DuplicateTreatment::Distinct)
            );
            let args = Self::extract_function_args(func, schema)?;
            return Ok(Expr::Aggregate {
                func: agg_func,
                args,
                distinct,
            });
        }

        let args = Self::extract_function_args(func, schema)?;
        let scalar_func = Self::try_scalar_function(&name)?;
        Ok(Expr::ScalarFunction {
            name: scalar_func,
            args,
        })
    }

    fn try_aggregate_function(name: &str) -> Option<AggregateFunction> {
        match name {
            "COUNT" => Some(AggregateFunction::Count),
            "SUM" => Some(AggregateFunction::Sum),
            "AVG" => Some(AggregateFunction::Avg),
            "MIN" => Some(AggregateFunction::Min),
            "MAX" => Some(AggregateFunction::Max),
            "ARRAY_AGG" => Some(AggregateFunction::ArrayAgg),
            "STRING_AGG" => Some(AggregateFunction::StringAgg),
            "ANY_VALUE" => Some(AggregateFunction::AnyValue),
            _ => None,
        }
    }

    fn try_scalar_function(name: &str) -> Result<ScalarFunction> {
        match name {
            "UPPER" => Ok(ScalarFunction::Upper),
            "LOWER" => Ok(ScalarFunction::Lower),
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => Ok(ScalarFunction::Length),
            "TRIM" => Ok(ScalarFunction::Trim),
            "LTRIM" => Ok(ScalarFunction::LTrim),
            "RTRIM" => Ok(ScalarFunction::RTrim),
            "SUBSTR" | "SUBSTRING" => Ok(ScalarFunction::Substr),
            "CONCAT" => Ok(ScalarFunction::Concat),
            "REPLACE" => Ok(ScalarFunction::Replace),
            "REVERSE" => Ok(ScalarFunction::Reverse),
            "LEFT" => Ok(ScalarFunction::Left),
            "RIGHT" => Ok(ScalarFunction::Right),
            "REPEAT" => Ok(ScalarFunction::Repeat),
            "STARTS_WITH" => Ok(ScalarFunction::StartsWith),
            "ENDS_WITH" => Ok(ScalarFunction::EndsWith),
            "CONTAINS" => Ok(ScalarFunction::Contains),
            "ABS" => Ok(ScalarFunction::Abs),
            "ROUND" => Ok(ScalarFunction::Round),
            "FLOOR" => Ok(ScalarFunction::Floor),
            "CEIL" | "CEILING" => Ok(ScalarFunction::Ceil),
            "SQRT" => Ok(ScalarFunction::Sqrt),
            "POWER" | "POW" => Ok(ScalarFunction::Power),
            "MOD" => Ok(ScalarFunction::Mod),
            "SIGN" => Ok(ScalarFunction::Sign),
            "EXP" => Ok(ScalarFunction::Exp),
            "LN" => Ok(ScalarFunction::Ln),
            "LOG" => Ok(ScalarFunction::Log),
            "LOG10" => Ok(ScalarFunction::Log10),
            "COALESCE" => Ok(ScalarFunction::Coalesce),
            "IFNULL" => Ok(ScalarFunction::IfNull),
            "NULLIF" => Ok(ScalarFunction::NullIf),
            "IF" => Ok(ScalarFunction::If),
            "CURRENT_DATE" => Ok(ScalarFunction::CurrentDate),
            "CURRENT_TIMESTAMP" | "NOW" => Ok(ScalarFunction::CurrentTimestamp),
            "CURRENT_TIME" => Ok(ScalarFunction::CurrentTime),
            "EXTRACT" => Ok(ScalarFunction::Extract),
            "DATE_ADD" => Ok(ScalarFunction::DateAdd),
            "DATE_SUB" => Ok(ScalarFunction::DateSub),
            "DATE_DIFF" | "DATEDIFF" => Ok(ScalarFunction::DateDiff),
            "DATE_TRUNC" => Ok(ScalarFunction::DateTrunc),
            "FORMAT_DATE" => Ok(ScalarFunction::FormatDate),
            "FORMAT_TIMESTAMP" => Ok(ScalarFunction::FormatTimestamp),
            "PARSE_DATE" => Ok(ScalarFunction::ParseDate),
            "PARSE_TIMESTAMP" => Ok(ScalarFunction::ParseTimestamp),
            _ => Ok(ScalarFunction::Custom(name.to_string())),
        }
    }

    fn extract_function_args(func: &ast::Function, schema: &PlanSchema) -> Result<Vec<Expr>> {
        match &func.args {
            ast::FunctionArguments::None => Ok(vec![]),
            ast::FunctionArguments::Subquery(_) => {
                Err(Error::unsupported("Subquery function arguments"))
            }
            ast::FunctionArguments::List(list) => {
                let mut args = Vec::new();
                for arg in &list.args {
                    match arg {
                        ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                            ast::FunctionArgExpr::Expr(e) => {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                            ast::FunctionArgExpr::Wildcard => {
                                args.push(Expr::Wildcard { table: None });
                            }
                            ast::FunctionArgExpr::QualifiedWildcard(name) => {
                                args.push(Expr::Wildcard {
                                    table: Some(name.to_string()),
                                });
                            }
                        },
                        ast::FunctionArg::Named { arg, .. } => match arg {
                            ast::FunctionArgExpr::Expr(e) => {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                            _ => {}
                        },
                        ast::FunctionArg::ExprNamed { arg, .. } => match arg {
                            ast::FunctionArgExpr::Expr(e) => {
                                args.push(Self::plan_expr(e, schema)?);
                            }
                            _ => {}
                        },
                    }
                }
                Ok(args)
            }
        }
    }

    fn plan_case(
        operand: Option<&ast::Expr>,
        conditions: &[ast::CaseWhen],
        else_result: Option<&ast::Expr>,
        schema: &PlanSchema,
    ) -> Result<Expr> {
        let operand = operand
            .map(|e| Self::plan_expr(e, schema))
            .transpose()?
            .map(Box::new);

        let mut when_clauses = Vec::new();
        for cw in conditions {
            let condition = Self::plan_expr(&cw.condition, schema)?;
            let result = Self::plan_expr(&cw.result, schema)?;
            when_clauses.push(WhenClause { condition, result });
        }

        let else_result = else_result
            .map(|e| Self::plan_expr(e, schema))
            .transpose()?
            .map(Box::new);

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_result,
        })
    }

    fn plan_data_type(sql_type: &ast::DataType) -> Result<yachtsql_common::types::DataType> {
        use yachtsql_common::types::DataType;
        match sql_type {
            ast::DataType::Boolean | ast::DataType::Bool => Ok(DataType::Bool),
            ast::DataType::Int64 | ast::DataType::BigInt(_) => Ok(DataType::Int64),
            ast::DataType::Float64 | ast::DataType::Double(_) => Ok(DataType::Float64),
            ast::DataType::Numeric(info) | ast::DataType::Decimal(info) => {
                let ps = match info {
                    ast::ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                    ast::ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                    ast::ExactNumberInfo::None => None,
                };
                Ok(DataType::Numeric(ps))
            }
            ast::DataType::String(_) | ast::DataType::Varchar(_) | ast::DataType::Text => {
                Ok(DataType::String)
            }
            ast::DataType::Bytes(_) | ast::DataType::Bytea => Ok(DataType::Bytes),
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Time(..) => Ok(DataType::Time),
            ast::DataType::Datetime(_) => Ok(DataType::DateTime),
            ast::DataType::Timestamp(..) => Ok(DataType::Timestamp),
            ast::DataType::JSON | ast::DataType::JSONB => Ok(DataType::Json),
            ast::DataType::Interval { .. } => Ok(DataType::Interval),
            _ => Ok(DataType::Unknown),
        }
    }
}
