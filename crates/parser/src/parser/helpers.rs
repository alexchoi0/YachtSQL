use sqlparser::ast::{
    ArrayElemTypeDef, DataType, ExactNumberInfo, Ident, ObjectName, ObjectNamePart,
};
use sqlparser::tokenizer::{Span, Token};
use yachtsql_core::error::{Error, Result};

pub struct ParserHelpers;

impl ParserHelpers {
    pub fn consume_keyword(tokens: &[&Token], idx: &mut usize, keyword: &str) -> bool {
        if let Some(Token::Word(w)) = tokens.get(*idx)
            && w.value.eq_ignore_ascii_case(keyword)
        {
            *idx += 1;
            return true;
        }
        false
    }

    #[inline]
    pub fn expect_keyword(tokens: &[&Token], idx: &mut usize, keyword: &str) -> bool {
        Self::consume_keyword(tokens, idx, keyword)
    }

    pub fn consume_keyword_pair(
        tokens: &[&Token],
        idx: &mut usize,
        first: &str,
        second: &str,
    ) -> bool {
        let saved_idx = *idx;
        if Self::consume_keyword(tokens, idx, first) && Self::consume_keyword(tokens, idx, second) {
            return true;
        }
        *idx = saved_idx;
        false
    }

    pub fn check_keyword(tokens: &[&Token], idx: usize, keyword: &str) -> bool {
        if let Some(Token::Word(w)) = tokens.get(idx) {
            return w.value.eq_ignore_ascii_case(keyword);
        }
        false
    }

    pub fn peek_is_number(tokens: &[&Token], idx: usize) -> bool {
        matches!(tokens.get(idx), Some(Token::Number(_, _)))
    }

    pub fn parse_number<T>(tokens: &[&Token], idx: &mut usize, type_name: &str) -> Result<T>
    where
        T: std::str::FromStr,
        T::Err: std::fmt::Display,
    {
        if let Some(Token::Number(s, _)) = tokens.get(*idx) {
            *idx += 1;
            s.parse::<T>()
                .map_err(|e| Error::parse_error(format!("Invalid {}: {} ({})", type_name, s, e)))
        } else {
            Err(Error::parse_error(format!("Expected {}", type_name)))
        }
    }

    pub fn parse_i64(tokens: &[&Token], idx: &mut usize) -> Result<i64> {
        let is_negative = if matches!(tokens.get(*idx), Some(Token::Minus)) {
            *idx += 1;
            true
        } else {
            false
        };

        let value = Self::parse_number::<i64>(tokens, idx, "i64")?;
        Ok(if is_negative { -value } else { value })
    }

    pub fn parse_u32(tokens: &[&Token], idx: &mut usize) -> Result<u32> {
        Self::parse_number::<u32>(tokens, idx, "u32")
    }

    pub fn parse_object_name_at(tokens: &[&Token], idx: &mut usize) -> Result<ObjectName> {
        let mut parts = Vec::new();

        loop {
            if let Some(Token::Word(w)) = tokens.get(*idx) {
                parts.push(ObjectNamePart::Identifier(Ident {
                    value: w.value.clone(),
                    quote_style: w.quote_style,
                    span: Span::empty(),
                }));
                *idx += 1;

                if matches!(tokens.get(*idx), Some(Token::Period)) {
                    *idx += 1;
                    continue;
                } else {
                    break;
                }
            } else {
                return Err(Error::parse_error("Expected object name".to_string()));
            }
        }

        if parts.is_empty() {
            return Err(Error::parse_error("Empty object name".to_string()));
        }

        Ok(ObjectName(parts))
    }

    pub fn parse_owned_by(tokens: &[&Token], idx: &mut usize) -> Result<(String, String)> {
        if let Some(Token::Word(table_word)) = tokens.get(*idx) {
            *idx += 1;
            let table_name = table_word.value.clone();

            if !matches!(tokens.get(*idx), Some(Token::Period)) {
                return Err(Error::parse_error(
                    "Expected '.' after table name in OWNED BY clause".to_string(),
                ));
            }
            *idx += 1;

            if let Some(Token::Word(column_word)) = tokens.get(*idx) {
                *idx += 1;
                let column_name = column_word.value.clone();
                return Ok((table_name, column_name));
            }
        }

        Err(Error::parse_error(
            "Invalid OWNED BY clause format".to_string(),
        ))
    }

    pub fn parse_data_type(tokens: &[&Token], idx: &mut usize) -> Result<DataType> {
        let type_name = match tokens.get(*idx) {
            Some(Token::Word(w)) => {
                *idx += 1;
                w.value.to_uppercase()
            }
            _ => return Err(Error::parse_error("Expected data type".to_string())),
        };

        let (type_name, _is_compound) = if type_name == "DOUBLE" {
            if Self::consume_keyword(tokens, idx, "PRECISION") {
                ("DOUBLE PRECISION".to_string(), true)
            } else {
                (type_name, false)
            }
        } else if type_name == "CHARACTER" {
            if Self::consume_keyword(tokens, idx, "VARYING") {
                ("VARCHAR".to_string(), true)
            } else {
                ("CHAR".to_string(), false)
            }
        } else if type_name == "TIMESTAMP" {
            if Self::consume_keyword(tokens, idx, "WITH") {
                if Self::consume_keyword(tokens, idx, "TIME")
                    && Self::consume_keyword(tokens, idx, "ZONE")
                {
                    ("TIMESTAMPTZ".to_string(), true)
                } else {
                    return Err(Error::parse_error(
                        "Expected TIME ZONE after WITH".to_string(),
                    ));
                }
            } else if Self::consume_keyword(tokens, idx, "WITHOUT") {
                if Self::consume_keyword(tokens, idx, "TIME")
                    && Self::consume_keyword(tokens, idx, "ZONE")
                {
                    ("TIMESTAMP".to_string(), true)
                } else {
                    return Err(Error::parse_error(
                        "Expected TIME ZONE after WITHOUT".to_string(),
                    ));
                }
            } else {
                (type_name, false)
            }
        } else {
            (type_name, false)
        };

        let (precision, scale) = if matches!(tokens.get(*idx), Some(Token::LParen)) {
            *idx += 1;
            let p = Self::parse_number::<u64>(tokens, idx, "precision")?;

            let s = if matches!(tokens.get(*idx), Some(Token::Comma)) {
                *idx += 1;
                Some(Self::parse_number::<u64>(tokens, idx, "scale")?)
            } else {
                None
            };

            if !matches!(tokens.get(*idx), Some(Token::RParen)) {
                return Err(Error::parse_error(
                    "Expected ')' after type precision".to_string(),
                ));
            }
            *idx += 1;

            (Some(p), s)
        } else {
            (None, None)
        };

        let base_type =
            match type_name.as_str() {
                "INT" | "INTEGER" | "INT4" => DataType::Int(None),
                "BIGINT" | "INT8" | "INT64" => DataType::BigInt(None),
                "SMALLINT" | "INT2" => DataType::SmallInt(None),
                "TINYINT" => DataType::TinyInt(None),

                "TEXT" => DataType::Text,
                "VARCHAR" | "STRING" => DataType::Varchar(precision.map(|p| {
                    sqlparser::ast::CharacterLength::IntegerLength {
                        length: p,
                        unit: None,
                    }
                })),
                "CHAR" => DataType::Char(precision.map(|p| {
                    sqlparser::ast::CharacterLength::IntegerLength {
                        length: p,
                        unit: None,
                    }
                })),

                "BOOLEAN" | "BOOL" => DataType::Boolean,

                "NUMERIC" | "DECIMAL" => {
                    let info = match (precision, scale) {
                        (Some(p), Some(s)) => ExactNumberInfo::PrecisionAndScale(p, s as i64),
                        (Some(p), None) => ExactNumberInfo::Precision(p),
                        _ => ExactNumberInfo::None,
                    };
                    DataType::Numeric(info)
                }

                "FLOAT" | "FLOAT4" => {
                    let info = match precision {
                        Some(p) => ExactNumberInfo::Precision(p),
                        None => ExactNumberInfo::None,
                    };
                    DataType::Float(info)
                }
                "REAL" => DataType::Real,
                "DOUBLE PRECISION" | "FLOAT8" => DataType::DoublePrecision,

                "DATE" => DataType::Date,
                "TIME" => DataType::Time(None, sqlparser::ast::TimezoneInfo::None),
                "TIMESTAMP" => DataType::Timestamp(None, sqlparser::ast::TimezoneInfo::None),
                "TIMESTAMPTZ" => DataType::Timestamp(None, sqlparser::ast::TimezoneInfo::Tz),

                "UUID" => DataType::Uuid,
                "JSON" | "JSONB" => DataType::JSON,
                "BYTEA" | "BYTES" => DataType::Bytea,

                "SERIAL" => DataType::Int4(None),
                "BIGSERIAL" => DataType::Int8(None),

                "INTERVAL" => DataType::Interval {
                    fields: None,
                    precision: None,
                },

                other => DataType::Custom(
                    ObjectName(vec![ObjectNamePart::Identifier(Ident {
                        value: other.to_string(),
                        quote_style: None,
                        span: Span::empty(),
                    })]),
                    vec![],
                ),
            };

        let final_type = if matches!(tokens.get(*idx), Some(Token::LBracket)) {
            *idx += 1;
            if !matches!(tokens.get(*idx), Some(Token::RBracket)) {
                return Err(Error::parse_error(
                    "Expected ']' after '[' for array type".to_string(),
                ));
            }
            *idx += 1;
            DataType::Array(ArrayElemTypeDef::SquareBracket(Box::new(base_type), None))
        } else if Self::consume_keyword(tokens, idx, "ARRAY") {
            DataType::Array(ArrayElemTypeDef::None)
        } else {
            base_type
        };

        Ok(final_type)
    }

    pub fn object_name_from_string(s: &str) -> ObjectName {
        let parts: Vec<ObjectNamePart> = s
            .split('.')
            .map(|part| {
                ObjectNamePart::Identifier(Ident {
                    value: part.to_string(),
                    quote_style: None,
                    span: Span::empty(),
                })
            })
            .collect();
        ObjectName(parts)
    }
}
