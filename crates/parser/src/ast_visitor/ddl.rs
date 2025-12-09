use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::DataType;
use yachtsql_ir::plan::{ReferentialAction, TableConstraint};

use super::LogicalPlanBuilder;
use crate::Sql2023Types;

impl LogicalPlanBuilder {
    pub(super) fn parse_table_constraint(
        constraint: &ast::TableConstraint,
    ) -> Result<TableConstraint> {
        match constraint {
            ast::TableConstraint::PrimaryKey { name, columns, .. } => {
                Ok(TableConstraint::PrimaryKey {
                    name: name.as_ref().map(|n| n.value.clone()),
                    columns: Self::index_columns_to_ident_strings(columns)?,
                })
            }
            ast::TableConstraint::Unique { name, columns, .. } => Ok(TableConstraint::Unique {
                name: name.as_ref().map(|n| n.value.clone()),
                columns: Self::index_columns_to_ident_strings(columns)?,
            }),
            ast::TableConstraint::ForeignKey {
                name,
                columns,
                foreign_table,
                referred_columns,
                on_delete,
                on_update,
                ..
            } => Ok(TableConstraint::ForeignKey {
                name: name.as_ref().map(|n| n.value.clone()),
                columns: columns.iter().map(|col| col.value.clone()).collect(),
                ref_table: Self::object_name_to_string(foreign_table),
                ref_columns: referred_columns
                    .iter()
                    .map(|ident| ident.value.clone())
                    .collect(),
                on_delete: on_delete.as_ref().and_then(Self::parse_referential_action),
                on_update: on_update.as_ref().and_then(Self::parse_referential_action),
            }),
            ast::TableConstraint::Check { expr, .. } => Err(Error::unsupported_feature(format!(
                "CHECK constraint not yet supported: {}",
                expr
            ))),
            _ => Err(Error::unsupported_feature(
                "Constraint type not supported".to_string(),
            )),
        }
    }

    pub(super) fn parse_referential_action(
        action: &ast::ReferentialAction,
    ) -> Option<ReferentialAction> {
        match action {
            ast::ReferentialAction::Cascade => Some(ReferentialAction::Cascade),
            ast::ReferentialAction::SetNull => Some(ReferentialAction::SetNull),
            ast::ReferentialAction::NoAction => Some(ReferentialAction::NoAction),
            ast::ReferentialAction::Restrict => Some(ReferentialAction::Restrict),
            ast::ReferentialAction::SetDefault => Some(ReferentialAction::SetDefault),
        }
    }

    pub(super) fn sql_datatype_to_datatype(data_type: &ast::DataType) -> Result<DataType> {
        match data_type {
            ast::DataType::Int(_) | ast::DataType::Integer(_) => Ok(DataType::Int64),
            ast::DataType::BigInt(_) => Ok(DataType::Int64),
            ast::DataType::SmallInt(_) => Ok(DataType::Int64),
            ast::DataType::TinyInt(_) => Ok(DataType::Int64),
            ast::DataType::Float(_) => Ok(DataType::Float64),
            ast::DataType::Double(_) | ast::DataType::Real => Ok(DataType::Float64),
            ast::DataType::Decimal(precision_info)
            | ast::DataType::Dec(precision_info)
            | ast::DataType::Numeric(precision_info) => {
                let precision_scale = match precision_info {
                    sqlparser::ast::ExactNumberInfo::PrecisionAndScale(p, s) => {
                        Some(((*p) as u8, (*s) as u8))
                    }
                    sqlparser::ast::ExactNumberInfo::Precision(p) => Some(((*p) as u8, 0)),
                    sqlparser::ast::ExactNumberInfo::None => None,
                };
                Ok(DataType::Numeric(precision_scale))
            }
            ast::DataType::String(_) | ast::DataType::Text => Ok(DataType::String),
            ast::DataType::Varchar(_) | ast::DataType::Char(_) => Ok(DataType::String),
            ast::DataType::Bit(_) | ast::DataType::BitVarying(_) => Ok(DataType::Bytes),
            ast::DataType::Boolean => Ok(DataType::Bool),
            ast::DataType::Date => Ok(DataType::Date),
            ast::DataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            ast::DataType::Time(_, _) => Ok(DataType::Time),
            ast::DataType::Datetime(_) => Ok(DataType::DateTime),
            ast::DataType::JSON | ast::DataType::JSONB => Ok(DataType::Json),
            ast::DataType::Uuid => Ok(DataType::Uuid),
            ast::DataType::Array(inner_def) => {
                use sqlparser::ast::ArrayElemTypeDef;
                let inner_type = match inner_def {
                    ArrayElemTypeDef::AngleBracket(dt) | ArrayElemTypeDef::SquareBracket(dt, _) => {
                        Self::sql_datatype_to_datatype(dt)?
                    }
                    ArrayElemTypeDef::Parenthesis(dt) => Self::sql_datatype_to_datatype(dt)?,
                    ArrayElemTypeDef::None => DataType::String,
                };
                Ok(DataType::Array(Box::new(inner_type)))
            }
            ast::DataType::Custom(name, modifiers) => {
                let type_name = name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .unwrap_or_default();
                let canonical = Sql2023Types::normalize_type_name(&type_name);

                let type_upper = type_name.to_uppercase();
                match type_upper.as_str() {
                    "MACADDR" => return Ok(DataType::MacAddr),
                    "MACADDR8" => return Ok(DataType::MacAddr8),
                    "VECTOR" => {
                        let dims = modifiers
                            .first()
                            .and_then(|s| s.parse::<usize>().ok())
                            .unwrap_or(0);
                        return Ok(DataType::Vector(dims));
                    }
                    "DECIMAL32" => {
                        let scale = modifiers
                            .first()
                            .and_then(|s| s.parse::<u8>().ok())
                            .unwrap_or(0);
                        return Ok(DataType::Numeric(Some((9, scale))));
                    }
                    "DECIMAL64" => {
                        let scale = modifiers
                            .first()
                            .and_then(|s| s.parse::<u8>().ok())
                            .unwrap_or(0);
                        return Ok(DataType::Numeric(Some((18, scale))));
                    }
                    "DECIMAL128" => {
                        let scale = modifiers
                            .first()
                            .and_then(|s| s.parse::<u8>().ok())
                            .unwrap_or(0);
                        return Ok(DataType::Numeric(Some((38, scale))));
                    }
                    "DATETIME64" => {
                        return Ok(DataType::Timestamp);
                    }
                    "FIXEDSTRING" => {
                        return Ok(DataType::String);
                    }
                    _ => {}
                }

                match canonical.as_str() {
                    "BIGINT" => Ok(DataType::Int64),
                    "DOUBLE PRECISION" => Ok(DataType::Float64),
                    "REAL" => Ok(DataType::Float64),
                    "VARCHAR" | "CHAR" => Ok(DataType::String),
                    "BOOLEAN" => Ok(DataType::Bool),
                    "VARBINARY" | "BINARY" => Ok(DataType::Bytes),
                    "NUMERIC" => Ok(DataType::Numeric(None)),
                    "JSON" => Ok(DataType::Json),
                    "GEOGRAPHY" => Ok(DataType::Geography),
                    "INET" => Ok(DataType::Inet),
                    "CIDR" => Ok(DataType::Cidr),
                    "POINT" => Ok(DataType::Point),
                    "BOX" => Ok(DataType::PgBox),
                    "CIRCLE" => Ok(DataType::Circle),
                    "HSTORE" => Ok(DataType::Hstore),
                    _ => Err(Error::unsupported_feature(format!(
                        "Custom data type not supported: {}",
                        type_name
                    ))),
                }
            }
            ast::DataType::GeometricType(kind) => {
                use sqlparser::ast::GeometricTypeKind;
                match kind {
                    GeometricTypeKind::Point => Ok(DataType::Point),
                    GeometricTypeKind::GeometricBox => Ok(DataType::PgBox),
                    GeometricTypeKind::Circle => Ok(DataType::Circle),
                    GeometricTypeKind::Line
                    | GeometricTypeKind::LineSegment
                    | GeometricTypeKind::GeometricPath
                    | GeometricTypeKind::Polygon => Err(Error::unsupported_feature(format!(
                        "Geometric type {:?} not yet supported",
                        kind
                    ))),
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Data type not supported: {:?}",
                data_type
            ))),
        }
    }

    fn index_columns_to_ident_strings(columns: &[ast::IndexColumn]) -> Result<Vec<String>> {
        columns
            .iter()
            .map(|col| Self::order_by_expr_to_identifier(&col.column))
            .collect()
    }

    fn order_by_expr_to_identifier(order_expr: &ast::OrderByExpr) -> Result<String> {
        match &order_expr.expr {
            ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
            ast::Expr::CompoundIdentifier(idents) => {
                if idents.is_empty() {
                    Err(Error::invalid_query(
                        "Index column identifier cannot be empty".to_string(),
                    ))
                } else {
                    Ok(idents
                        .iter()
                        .map(|ident| ident.value.clone())
                        .collect::<Vec<_>>()
                        .join("."))
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Complex index column expression not supported: {}",
                order_expr.expr
            ))),
        }
    }
}
