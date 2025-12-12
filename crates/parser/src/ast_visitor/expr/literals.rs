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
                        let inner_expr = self.sql_expr_to_expr(expr)?;
                        return Ok((
                            Expr::ExpressionWildcard {
                                expr: Box::new(inner_expr),
                            },
                            None,
                        ));
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
            ast::DataType::Time(_, _) => {
                if chrono::NaiveTime::parse_from_str(value, "%H:%M:%S").is_err()
                    && chrono::NaiveTime::parse_from_str(value, "%H:%M:%S%.f").is_err()
                    && chrono::NaiveTime::parse_from_str(value, "%H:%M").is_err()
                {
                    return Err(Error::invalid_query(format!(
                        "Invalid time format: '{}'. Expected format: HH:MM:SS",
                        value
                    )));
                }
                Ok(Expr::Literal(LiteralValue::Time(value.to_string())))
            }
            ast::DataType::Datetime(_) => {
                if yachtsql_core::types::parse_timestamp_to_utc(value).is_none() {
                    return Err(Error::invalid_query(format!(
                        "Invalid datetime format: '{}'",
                        value
                    )));
                }
                Ok(Expr::Literal(LiteralValue::DateTime(value.to_string())))
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
            ast::DataType::Numeric(_) | ast::DataType::BigNumeric(_) => {
                use std::str::FromStr;

                use rust_decimal::Decimal;
                Decimal::from_str(value)
                    .map(|d| Expr::Literal(LiteralValue::Numeric(d)))
                    .map_err(|e| {
                        Error::invalid_query(format!("Invalid NUMERIC literal '{}': {}", value, e))
                    })
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
                    GeometricTypeKind::Line => {
                        Ok(Expr::Literal(LiteralValue::Line(value.to_string())))
                    }
                    GeometricTypeKind::LineSegment => {
                        Ok(Expr::Literal(LiteralValue::Lseg(value.to_string())))
                    }
                    GeometricTypeKind::GeometricPath => {
                        Ok(Expr::Literal(LiteralValue::Path(value.to_string())))
                    }
                    GeometricTypeKind::Polygon => {
                        Ok(Expr::Literal(LiteralValue::Polygon(value.to_string())))
                    }
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
                use rust_decimal::Decimal;
                if s.contains('.') || s.to_lowercase().contains('e') {
                    Decimal::from_str_exact(s)
                        .map(LiteralValue::Numeric)
                        .or_else(|_| {
                            s.parse::<f64>()
                                .map(LiteralValue::Float64)
                                .map_err(|_| Error::invalid_query("Invalid numeric literal"))
                        })
                } else {
                    s.parse::<i64>().map(LiteralValue::Int64).or_else(|_| {
                        Decimal::from_str_exact(s)
                            .map(LiteralValue::Numeric)
                            .map_err(|_| Error::invalid_query("Invalid integer literal"))
                    })
                }
            }
            ast::Value::SingleQuotedString(s) | ast::Value::DoubleQuotedString(s) => {
                if let Some(hex_str) = s.strip_prefix("\\x") {
                    return Self::parse_hex_literal(hex_str);
                }
                Ok(LiteralValue::String(s.clone()))
            }
            ast::Value::SingleQuotedRawStringLiteral(s)
            | ast::Value::DoubleQuotedRawStringLiteral(s) => Ok(LiteralValue::String(s.clone())),
            ast::Value::Boolean(b) => Ok(LiteralValue::Boolean(*b)),
            ast::Value::Null => Ok(LiteralValue::Null),

            ast::Value::SingleQuotedByteStringLiteral(s)
            | ast::Value::DoubleQuotedByteStringLiteral(s)
            | ast::Value::TripleSingleQuotedByteStringLiteral(s)
            | ast::Value::TripleDoubleQuotedByteStringLiteral(s) => {
                if s.contains("\\x") {
                    Self::parse_byte_string_with_escapes(s)
                } else {
                    Self::parse_bit_string_literal(s)
                }
            }

            ast::Value::HexStringLiteral(s) => Self::parse_hex_literal(s),
            ast::Value::EscapedStringLiteral(s) => {
                let unescaped = Self::unescape_string(s);
                if let Some(hex_str) = unescaped.strip_prefix("\\x") {
                    Self::parse_hex_literal(hex_str)
                } else {
                    Ok(LiteralValue::String(unescaped))
                }
            }
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

    pub(super) fn parse_bit_string_literal(bit_str: &str) -> Result<LiteralValue> {
        if bit_str.chars().all(|c| c == '0' || c == '1') {
            let padded_len = bit_str.len().div_ceil(8) * 8;
            let padded = format!("{:0>width$}", bit_str, width = padded_len);

            let mut bytes = Vec::with_capacity(padded_len / 8);
            for chunk in padded.as_bytes().chunks(8) {
                let byte_str = std::str::from_utf8(chunk).unwrap();
                let byte = u8::from_str_radix(byte_str, 2).map_err(|_| {
                    Error::invalid_query(format!(
                        "Invalid bit string literal: '{}' contains invalid bits",
                        bit_str
                    ))
                })?;
                bytes.push(byte);
            }
            Ok(LiteralValue::Bytes(bytes))
        } else {
            Ok(LiteralValue::Bytes(bit_str.as_bytes().to_vec()))
        }
    }

    pub(super) fn parse_byte_string_with_escapes(s: &str) -> Result<LiteralValue> {
        if let Some(hex_str) = s.strip_prefix("\\x")
            && hex_str.chars().all(|c| c.is_ascii_hexdigit())
        {
            return Self::parse_hex_literal(hex_str);
        }

        let mut bytes = Vec::new();
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' && chars.peek() == Some(&'x') {
                chars.next();
                let mut hex = String::with_capacity(2);
                for _ in 0..2 {
                    match chars.next() {
                        Some(h) if h.is_ascii_hexdigit() => hex.push(h),
                        Some(h) => {
                            return Err(Error::invalid_query(format!(
                                "Invalid hex escape: \\x{}{} is not valid hex",
                                hex, h
                            )));
                        }
                        None => {
                            return Err(Error::invalid_query(
                                "Incomplete hex escape sequence".to_string(),
                            ));
                        }
                    }
                }
                let byte = u8::from_str_radix(&hex, 16).unwrap();
                bytes.push(byte);
            } else if c == '\\' && chars.peek() == Some(&'\\') {
                chars.next();
                bytes.push(b'\\');
            } else if c == '\\' && chars.peek() == Some(&'n') {
                chars.next();
                bytes.push(b'\n');
            } else if c == '\\' && chars.peek() == Some(&'r') {
                chars.next();
                bytes.push(b'\r');
            } else if c == '\\' && chars.peek() == Some(&'t') {
                chars.next();
                bytes.push(b'\t');
            } else {
                for b in c.to_string().as_bytes() {
                    bytes.push(*b);
                }
            }
        }

        Ok(LiteralValue::Bytes(bytes))
    }

    pub(super) fn parse_hex_literal(hex_str: &str) -> Result<LiteralValue> {
        let padded = if hex_str.len() % 2 == 1 {
            format!("0{}", hex_str)
        } else {
            hex_str.to_string()
        };

        let mut bytes = Vec::with_capacity(padded.len() / 2);

        for i in (0..padded.len()).step_by(2) {
            let byte_str = &padded[i..i + 2];
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

    fn unescape_string(s: &str) -> String {
        let mut result = String::with_capacity(s.len());
        let mut chars = s.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '\\' {
                match chars.peek() {
                    Some('\\') => {
                        chars.next();
                        result.push('\\');
                    }
                    Some('n') => {
                        chars.next();
                        result.push('\n');
                    }
                    Some('r') => {
                        chars.next();
                        result.push('\r');
                    }
                    Some('t') => {
                        chars.next();
                        result.push('\t');
                    }
                    Some('\'') => {
                        chars.next();
                        result.push('\'');
                    }
                    Some('"') => {
                        chars.next();
                        result.push('"');
                    }
                    Some('x') => {
                        result.push('\\');
                        result.push(chars.next().unwrap());
                    }
                    _ => {
                        result.push(c);
                    }
                }
            } else {
                result.push(c);
            }
        }

        result
    }

    pub(super) fn value_to_literal(
        &self,
        value: &yachtsql_core::types::Value,
    ) -> Result<LiteralValue> {
        use yachtsql_core::types::DataType;

        if value.is_null() {
            return Ok(LiteralValue::Null);
        }

        match value.data_type() {
            DataType::Int64 => Ok(LiteralValue::Int64(value.as_i64().unwrap_or(0))),
            DataType::Float64 => Ok(LiteralValue::Float64(value.as_f64().unwrap_or(0.0))),
            DataType::String => Ok(LiteralValue::String(
                value.as_str().unwrap_or("").to_string(),
            )),
            DataType::Bool => Ok(LiteralValue::Boolean(value.as_bool().unwrap_or(false))),
            DataType::Numeric(_) => {
                if let Some(d) = value.as_numeric() {
                    Ok(LiteralValue::Numeric(d))
                } else {
                    Ok(LiteralValue::String(value.to_string()))
                }
            }
            DataType::Date => Ok(LiteralValue::Date(value.to_string())),
            DataType::Timestamp => Ok(LiteralValue::Timestamp(value.to_string())),
            DataType::Bytes => {
                if let Some(bytes) = value.as_bytes() {
                    Ok(LiteralValue::Bytes(bytes.to_vec()))
                } else {
                    Ok(LiteralValue::Bytes(vec![]))
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Cannot convert value of type {:?} to literal",
                value.data_type()
            ))),
        }
    }
}
