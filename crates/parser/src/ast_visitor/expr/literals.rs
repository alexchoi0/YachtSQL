use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{Expr, LiteralValue};

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(crate) fn select_item_to_expr(
        &self,
        item: &ast::SelectItem,
    ) -> Result<(Expr, Option<String>)> {
        match item {
            ast::SelectItem::UnnamedExpr(expr) => Ok((self.sql_expr_to_expr(expr)?, None)),
            ast::SelectItem::ExprWithAlias { expr, alias } => {
                Ok((self.sql_expr_to_expr(expr)?, Some(alias.value.clone())))
            }
            ast::SelectItem::Wildcard(_) => Ok((Expr::Wildcard, None)),
            ast::SelectItem::QualifiedWildcard(kind, _) => {
                let qualifier = match kind {
                    ast::SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                        Self::object_name_to_string(object_name)
                    }
                    ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                        return Err(Error::unsupported_feature(format!(
                            "Expression-qualified wildcard not supported: {}",
                            expr
                        )));
                    }
                };

                Ok((Expr::QualifiedWildcard { qualifier }, None))
            }
        }
    }

    pub(super) fn convert_typed_string(
        &self,
        data_type: &ast::DataType,
        value: &str,
    ) -> Result<Expr> {
        match data_type {
            ast::DataType::Date => {
                if chrono::NaiveDate::parse_from_str(value, "%Y-%m-%d").is_err() {
                    return Err(Error::invalid_query(format!(
                        "Invalid date format: '{}'. Expected format: YYYY-MM-DD",
                        value
                    )));
                }
                Ok(Expr::Literal(LiteralValue::Date(value.to_string())))
            }
            ast::DataType::Timestamp(_, _) => {
                if yachtsql_core::types::parse_timestamp_to_utc(value).is_none() {
                    return Err(Error::invalid_query(format!(
                        "Invalid timestamp format: '{}'",
                        value
                    )));
                }
                Ok(Expr::Literal(LiteralValue::Timestamp(value.to_string())))
            }
            ast::DataType::Uuid => Ok(Expr::Literal(LiteralValue::Uuid(value.to_string()))),
            ast::DataType::JSON | ast::DataType::JSONB => {
                Ok(Expr::Literal(LiteralValue::Json(value.to_string())))
            }
            ast::DataType::Custom(name, _)
                if Self::object_name_matches(name, "JSON")
                    || Self::object_name_matches(name, "JSONB") =>
            {
                Ok(Expr::Literal(LiteralValue::Json(value.to_string())))
            }
            ast::DataType::Custom(name, _args) if Self::object_name_matches(name, "VECTOR") => {
                let vector_elements = Self::parse_vector_literal_to_floats(value)?;
                Ok(Expr::Literal(LiteralValue::Vector(vector_elements)))
            }
            ast::DataType::Custom(name, _) if Self::object_name_matches(name, "INTERVAL") => {
                Ok(Expr::Literal(LiteralValue::Interval(value.to_string())))
            }
            ast::DataType::Custom(name, _) if Self::object_name_matches(name, "POINT") => {
                Ok(Expr::Literal(LiteralValue::Point(value.to_string())))
            }
            ast::DataType::Custom(name, _)
                if Self::object_name_matches(name, "BOX")
                    || Self::object_name_matches(name, "PGBOX") =>
            {
                Ok(Expr::Literal(LiteralValue::PgBox(value.to_string())))
            }
            ast::DataType::Custom(name, _) if Self::object_name_matches(name, "CIRCLE") => {
                Ok(Expr::Literal(LiteralValue::Circle(value.to_string())))
            }
            ast::DataType::Custom(name, _) if Self::object_name_matches(name, "MACADDR") => {
                Ok(Expr::Literal(LiteralValue::MacAddr(value.to_string())))
            }
            ast::DataType::Custom(name, _) if Self::object_name_matches(name, "MACADDR8") => {
                Ok(Expr::Literal(LiteralValue::MacAddr8(value.to_string())))
            }
            ast::DataType::GeometricType(kind) => {
                use sqlparser::ast::GeometricTypeKind;
                match kind {
                    GeometricTypeKind::Point => {
                        Ok(Expr::Literal(LiteralValue::Point(value.to_string())))
                    }
                    GeometricTypeKind::GeometricBox => {
                        Ok(Expr::Literal(LiteralValue::PgBox(value.to_string())))
                    }
                    GeometricTypeKind::Circle => {
                        Ok(Expr::Literal(LiteralValue::Circle(value.to_string())))
                    }
                    GeometricTypeKind::Line => Err(Error::unsupported_feature(
                        "LINE geometric type not yet supported",
                    )),
                    GeometricTypeKind::LineSegment => Err(Error::unsupported_feature(
                        "LSEG geometric type not yet supported",
                    )),
                    GeometricTypeKind::GeometricPath => Err(Error::unsupported_feature(
                        "PATH geometric type not yet supported",
                    )),
                    GeometricTypeKind::Polygon => Err(Error::unsupported_feature(
                        "POLYGON geometric type not yet supported",
                    )),
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Typed string with data type {:?} not supported",
                data_type
            ))),
        }
    }

    pub(super) fn sql_value_to_literal(&self, value: &ast::ValueWithSpan) -> Result<LiteralValue> {
        match &value.value {
            ast::Value::Number(s, _) => {
                if s.contains('.') || s.to_lowercase().contains('e') {
                    s.parse::<rust_decimal::Decimal>()
                        .map(LiteralValue::Numeric)
                        .or_else(|_| {
                            s.parse::<f64>()
                                .map(LiteralValue::Float64)
                                .map_err(|_| Error::invalid_query("Invalid numeric literal"))
                        })
                } else {
                    s.parse::<i64>().map(LiteralValue::Int64).or_else(|_| {
                        s.parse::<rust_decimal::Decimal>()
                            .map(LiteralValue::Numeric)
                            .map_err(|_| Error::invalid_query("Invalid integer literal"))
                    })
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                if let Some(hex_str) = s.strip_prefix("\\x") {
                    return Self::parse_hex_literal(hex_str);
                }

                if s.starts_with('[')
                    && s.ends_with(']')
                    && let Ok(vector_elements) = Self::parse_vector_literal_to_floats(s)
                {
                    return Ok(LiteralValue::Vector(vector_elements));
                }
                Ok(LiteralValue::String(s.clone()))
            }
            ast::Value::Boolean(b) => Ok(LiteralValue::Boolean(*b)),
            ast::Value::Null => Ok(LiteralValue::Null),

            ast::Value::SingleQuotedByteStringLiteral(s)
            | ast::Value::DoubleQuotedByteStringLiteral(s)
            | ast::Value::TripleSingleQuotedByteStringLiteral(s)
            | ast::Value::TripleDoubleQuotedByteStringLiteral(s) => {
                Ok(LiteralValue::Bytes(s.as_bytes().to_vec()))
            }

            ast::Value::HexStringLiteral(s) => Self::parse_hex_literal(s),
            _ => Err(Error::unsupported_feature(format!(
                "Value type not supported: {:?}",
                value.value
            ))),
        }
    }

    pub(super) fn parse_vector_literal_to_floats(s: &str) -> Result<Vec<f64>> {
        let trimmed = s.trim();

        let content = trimmed
            .strip_prefix('[')
            .and_then(|s| s.strip_suffix(']'))
            .ok_or_else(|| {
                Error::invalid_query(format!(
                    "Invalid vector literal format: '{}'. Expected '[x, y, z, ...]'",
                    s
                ))
            })?;

        let content = content.trim();
        if content.is_empty() {
            return Err(Error::invalid_query(
                "Vector cannot be empty. Must have at least one element.".to_string(),
            ));
        }

        content
            .split(',')
            .map(|part| {
                let trimmed = part.trim();
                trimmed.parse::<f64>().map_err(|_| {
                    Error::invalid_query(format!(
                        "Invalid vector element: '{}'. All elements must be numeric.",
                        trimmed
                    ))
                })
            })
            .collect()
    }

    pub(super) fn parse_hex_literal(hex_str: &str) -> Result<LiteralValue> {
        if !hex_str.len().is_multiple_of(2) {
            return Err(Error::invalid_query(format!(
                "Invalid hex literal: '{}' - hex string must have even length",
                hex_str
            )));
        }

        let mut bytes = Vec::with_capacity(hex_str.len() / 2);

        for i in (0..hex_str.len()).step_by(2) {
            let byte_str = &hex_str[i..i + 2];
            match u8::from_str_radix(byte_str, 16) {
                Ok(byte) => bytes.push(byte),
                Err(_) => {
                    return Err(Error::invalid_query(format!(
                        "Invalid hex literal: '{}' contains invalid hex digits",
                        hex_str
                    )));
                }
            }
        }

        Ok(LiteralValue::Bytes(bytes))
    }
}
