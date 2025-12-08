use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{BinaryOp, CastDataType, Expr, LiteralValue};

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn convert_collate(
        &self,
        expr: &ast::Expr,
        collation: &ast::ObjectName,
    ) -> Result<Expr> {
        let base_expr = self.sql_expr_to_expr(expr)?;
        let collation_name = Self::object_name_to_string(collation);

        if collation_name.eq_ignore_ascii_case("yachtsql.case_insensitive") {
            Ok(Expr::Function {
                name: yachtsql_ir::FunctionName::Lower,
                args: vec![base_expr],
            })
        } else {
            Err(Error::unsupported_feature(format!(
                "Collation T061 unknown: {}",
                collation_name
            )))
        }
    }

    pub(super) fn convert_case(
        &self,
        operand: &Option<Box<ast::Expr>>,
        conditions: &[ast::CaseWhen],
        else_result: &Option<Box<ast::Expr>>,
    ) -> Result<Expr> {
        let operand_expr = if let Some(op) = operand {
            Some(Box::new(self.sql_expr_to_expr(op)?))
        } else {
            None
        };

        let when_then: Vec<(Expr, Expr)> = conditions
            .iter()
            .map(|case_when| {
                Ok((
                    self.sql_expr_to_expr(&case_when.condition)?,
                    self.sql_expr_to_expr(&case_when.result)?,
                ))
            })
            .collect::<Result<_>>()?;

        let else_expr = if let Some(else_e) = else_result {
            Some(Box::new(self.sql_expr_to_expr(else_e)?))
        } else {
            None
        };

        Ok(Expr::Case {
            operand: operand_expr,
            when_then,
            else_expr,
        })
    }

    pub(super) fn convert_cast(
        &self,
        expr: &ast::Expr,
        data_type: &ast::DataType,
        kind: &ast::CastKind,
    ) -> Result<Expr> {
        let cast_type = Self::sql_datatype_to_cast_type(data_type)?;
        let inner_expr = Box::new(self.sql_expr_to_expr_for_cast(expr, &cast_type)?);

        let result_expr = match kind {
            ast::CastKind::TryCast | ast::CastKind::SafeCast => Expr::TryCast {
                expr: inner_expr,
                data_type: cast_type,
            },
            ast::CastKind::Cast | ast::CastKind::DoubleColon => Expr::Cast {
                expr: inner_expr,
                data_type: cast_type,
            },
        };

        Ok(result_expr)
    }

    pub(super) fn convert_composite_access(
        &self,
        expr: &ast::Expr,
        access_chain: &[ast::AccessExpr],
    ) -> Result<Expr> {
        if access_chain.is_empty() {
            return self.sql_expr_to_expr(expr);
        }

        let mut current_expr = self.sql_expr_to_expr(expr)?;
        for access in access_chain {
            current_expr = self.apply_access_expr(current_expr, access)?;
        }

        Ok(current_expr)
    }

    fn apply_access_expr(&self, base_expr: Expr, access: &ast::AccessExpr) -> Result<Expr> {
        match access {
            ast::AccessExpr::Dot(field_expr) => {
                if let ast::Expr::Identifier(ident) = field_expr {
                    Ok(Expr::StructFieldAccess {
                        expr: Box::new(base_expr),
                        field: ident.value.clone(),
                    })
                } else {
                    Err(Error::unsupported_feature(
                        "Only identifier-based dot access is supported".to_string(),
                    ))
                }
            }
            ast::AccessExpr::Subscript(subscript) => {
                self.convert_subscript_access(base_expr, subscript)
            }
        }
    }

    fn convert_subscript_access(
        &self,
        array_expr: Expr,
        subscript: &ast::Subscript,
    ) -> Result<Expr> {
        match subscript {
            ast::Subscript::Index { index } => {
                let (index_expr, safe) = self.parse_array_index_expr(index)?;
                Ok(Expr::ArrayIndex {
                    array: Box::new(array_expr),
                    index: Box::new(index_expr),
                    safe,
                })
            }
            ast::Subscript::Slice {
                lower_bound,
                upper_bound,
                stride,
            } => {
                if stride.is_some() {
                    return Err(Error::unsupported_feature(
                        "Array slice with stride is not supported".to_string(),
                    ));
                }

                let start = if let Some(lb) = lower_bound {
                    Some(Box::new(self.sql_expr_to_expr(lb)?))
                } else {
                    None
                };

                let end = if let Some(ub) = upper_bound {
                    Some(Box::new(self.sql_expr_to_expr(ub)?))
                } else {
                    None
                };

                Ok(Expr::ArraySlice {
                    array: Box::new(array_expr),
                    start,
                    end,
                })
            }
        }
    }

    fn parse_array_index_expr(&self, index_expr: &ast::Expr) -> Result<(Expr, bool)> {
        match index_expr {
            ast::Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                if matches!(
                    func_name.as_str(),
                    "OFFSET" | "ORDINAL" | "SAFE_OFFSET" | "SAFE_ORDINAL"
                ) {
                    let safe = func_name.starts_with("SAFE_");

                    if let ast::FunctionArguments::List(arg_list) = &func.args {
                        if arg_list.args.len() != 1 {
                            return Err(Error::invalid_query(format!(
                                "{} requires exactly 1 argument",
                                func_name
                            )));
                        }

                        if let ast::FunctionArg::Unnamed(ast::FunctionArgExpr::Expr(idx_expr)) =
                            &arg_list.args[0]
                        {
                            let index = self.sql_expr_to_expr(idx_expr)?;
                            let adjusted_index = if func_name.contains("ORDINAL") {
                                Expr::binary_op(
                                    index,
                                    BinaryOp::Subtract,
                                    Expr::literal(LiteralValue::Int64(1)),
                                )
                            } else {
                                index
                            };
                            Ok((adjusted_index, safe))
                        } else {
                            Err(Error::invalid_query(
                                "Invalid OFFSET/ORDINAL argument".to_string(),
                            ))
                        }
                    } else {
                        Err(Error::invalid_query(format!(
                            "{} requires argument list",
                            func_name
                        )))
                    }
                } else {
                    Ok((self.sql_expr_to_expr(index_expr)?, false))
                }
            }
            _ => Ok((self.sql_expr_to_expr(index_expr)?, false)),
        }
    }

    pub(super) fn array_elem_type_def_to_datatype(
        elem_def: &sqlparser::ast::ArrayElemTypeDef,
    ) -> sqlparser::ast::DataType {
        use sqlparser::ast::ArrayElemTypeDef;
        match elem_def {
            ArrayElemTypeDef::AngleBracket(inner)
            | ArrayElemTypeDef::SquareBracket(inner, _)
            | ArrayElemTypeDef::Parenthesis(inner) => inner.as_ref().clone(),
            ArrayElemTypeDef::None => sqlparser::ast::DataType::String(None),
        }
    }

    pub(super) fn sql_datatype_to_cast_type(data_type: &ast::DataType) -> Result<CastDataType> {
        use sqlparser::ast::DataType as SqlDataType;

        let data_type_str = format!("{:?}", data_type).to_uppercase();

        match data_type {
            SqlDataType::Int(_)
            | SqlDataType::Integer(_)
            | SqlDataType::BigInt(_)
            | SqlDataType::SmallInt(_) => {
                return Ok(CastDataType::Int64);
            }
            SqlDataType::Float(_) | SqlDataType::Double(_) | SqlDataType::Real => {
                return Ok(CastDataType::Float64);
            }
            SqlDataType::Decimal(precision_info)
            | SqlDataType::Dec(precision_info)
            | SqlDataType::Numeric(precision_info) => {
                let (precision, scale) = match precision_info {
                    sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                        (Some((*p) as u8), Some((*s) as u8))
                    }
                    sqlparser::ast::ExactNumberInfo::Precision(p) => (Some((*p) as u8), Some(0)),
                    sqlparser::ast::ExactNumberInfo::None => (None, None),
                };

                let precision_scale = match (precision, scale) {
                    (Some(p), Some(s)) => Some((p, s)),
                    _ => None,
                };

                return Ok(CastDataType::Numeric(precision_scale));
            }
            SqlDataType::String(_)
            | SqlDataType::Text
            | SqlDataType::Varchar(_)
            | SqlDataType::Char(_) => {
                return Ok(CastDataType::String);
            }
            SqlDataType::Varbinary(_) | SqlDataType::Binary(_) | SqlDataType::Blob(_) => {
                return Ok(CastDataType::Bytes);
            }
            SqlDataType::Boolean => {
                return Ok(CastDataType::Bool);
            }
            SqlDataType::Date => {
                return Ok(CastDataType::Date);
            }
            SqlDataType::Datetime(_) => {
                return Ok(CastDataType::DateTime);
            }
            SqlDataType::Time(_, _) => {
                return Ok(CastDataType::Time);
            }
            SqlDataType::Timestamp(_, _) => {
                return Ok(CastDataType::Timestamp);
            }
            SqlDataType::Array(inner) => {
                let element_datatype = Self::array_elem_type_def_to_datatype(inner);
                let element_cast = Self::sql_datatype_to_cast_type(&element_datatype)?;
                return Ok(CastDataType::Array(Box::new(element_cast)));
            }
            SqlDataType::Custom(type_name, _) => {
                let name = type_name.to_string();
                return Ok(CastDataType::Custom(name, Vec::new()));
            }
            _ => {}
        }

        if data_type_str.contains("INTERVAL") {
            Ok(CastDataType::Interval)
        } else if data_type_str.contains("INT64") || data_type_str.starts_with("INT") {
            Ok(CastDataType::Int64)
        } else if data_type_str.contains("FLOAT64")
            || data_type_str.starts_with("FLOAT")
            || data_type_str.starts_with("DOUBLE")
        {
            Ok(CastDataType::Float64)
        } else if data_type_str.contains("NUMERIC") || data_type_str.contains("DECIMAL") {
            Ok(CastDataType::Numeric(None))
        } else if data_type_str.contains("STRING")
            || data_type_str.starts_with("VARCHAR")
            || data_type_str.starts_with("CHAR")
            || data_type_str.starts_with("TEXT")
        {
            Ok(CastDataType::String)
        } else if data_type_str.contains("BYTES")
            || data_type_str.contains("BYTEA")
            || data_type_str.contains("VARBINARY")
            || data_type_str.contains("BINARY")
        {
            Ok(CastDataType::Bytes)
        } else if data_type_str.contains("BOOL") {
            Ok(CastDataType::Bool)
        } else if data_type_str.contains("DATE") && !data_type_str.contains("DATETIME") {
            Ok(CastDataType::Date)
        } else if data_type_str.contains("DATETIME") {
            Ok(CastDataType::DateTime)
        } else if data_type_str.contains("TIME") && !data_type_str.contains("DATETIME") {
            Ok(CastDataType::Time)
        } else if data_type_str.contains("TIMESTAMP") {
            Ok(CastDataType::Timestamp)
        } else if data_type_str.contains("JSON") || data_type_str.contains("JSONB") {
            Ok(CastDataType::Json)
        } else if data_type_str.contains("GEOGRAPHY") {
            Ok(CastDataType::Geography)
        } else if data_type_str.contains("HSTORE") {
            Ok(CastDataType::Hstore)
        } else if data_type_str.contains("MACADDR8") {
            Ok(CastDataType::MacAddr8)
        } else if data_type_str.contains("MACADDR") {
            Ok(CastDataType::MacAddr)
        } else if data_type_str.contains("INT4RANGE") {
            Ok(CastDataType::Int4Range)
        } else if data_type_str.contains("INT8RANGE") {
            Ok(CastDataType::Int8Range)
        } else if data_type_str.contains("NUMRANGE") {
            Ok(CastDataType::NumRange)
        } else if data_type_str.contains("TSTZRANGE") {
            Ok(CastDataType::TsTzRange)
        } else if data_type_str.contains("TSRANGE") {
            Ok(CastDataType::TsRange)
        } else if data_type_str.contains("DATERANGE") {
            Ok(CastDataType::DateRange)
        } else if data_type_str.contains("UUID") {
            Ok(CastDataType::Uuid)
        } else {
            Err(Error::unsupported_feature(format!(
                "Data type not supported in CAST: {:?}",
                data_type
            )))
        }
    }
}
