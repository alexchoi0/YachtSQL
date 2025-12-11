use std::collections::HashMap;
use std::rc::Rc;

use rust_decimal::prelude::ToPrimitive;
use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;
use yachtsql_storage::{Column, Field, Schema};

use super::ExecutionPlan;
use crate::Table;

#[derive(Debug)]
pub struct AggregateExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    group_by: Vec<Expr>,
    aggregates: Vec<(Expr, Option<String>)>,
    having: Option<Expr>,
}

impl AggregateExec {
    pub(crate) fn contains_aggregate(expr: &Expr) -> bool {
        match expr {
            Expr::Aggregate { .. } => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::contains_aggregate(left) || Self::contains_aggregate(right)
            }
            Expr::UnaryOp { expr, .. } => Self::contains_aggregate(expr),
            Expr::Function { args, .. } => args.iter().any(|a| Self::contains_aggregate(a)),
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand
                    .as_ref()
                    .is_some_and(|o| Self::contains_aggregate(o))
                    || when_then
                        .iter()
                        .any(|(w, t)| Self::contains_aggregate(w) || Self::contains_aggregate(t))
                    || else_expr
                        .as_ref()
                        .is_some_and(|e| Self::contains_aggregate(e))
            }
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => Self::contains_aggregate(expr),
            Expr::InList { expr, list, .. } => {
                Self::contains_aggregate(expr) || list.iter().any(|e| Self::contains_aggregate(e))
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::contains_aggregate(expr)
                    || Self::contains_aggregate(low)
                    || Self::contains_aggregate(high)
            }
            _ => false,
        }
    }

    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(Expr, Option<String>)>,
        having: Option<Expr>,
    ) -> Result<Self> {
        use yachtsql_core::error::Error;

        for (agg_expr, _) in &aggregates {
            if let Expr::Aggregate { filter, .. } = agg_expr {
                if let Some(filter_expr) = filter {
                    if Self::contains_aggregate(filter_expr) {
                        return Err(Error::InvalidQuery(
                            "FILTER clause cannot contain aggregate functions".to_string(),
                        ));
                    }
                }
            }
        }

        let mut fields = Vec::new();

        let input_schema = input.schema();
        for (idx, group_expr) in group_by.iter().enumerate() {
            let field_name = if let Expr::Column { name, .. } = group_expr {
                name.clone()
            } else {
                format!("group_{}", idx)
            };

            let data_type = Self::infer_expr_type(group_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::String);

            fields.push(Field::nullable(field_name, data_type));
        }

        for (idx, (agg_expr, alias)) in aggregates.iter().enumerate() {
            let field_name = alias.clone().unwrap_or_else(|| {
                Self::expr_to_field_name(agg_expr).unwrap_or_else(|| format!("agg_{}", idx))
            });

            let data_type = Self::infer_aggregate_type(agg_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::Float64);
            fields.push(Field::nullable(field_name, data_type));
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            group_by,
            aggregates,
            having,
        })
    }

    fn compute_group_key(&self, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        let mut key = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let value = self.evaluate_expr(expr, batch, row_idx)?;
            key.push(value);
        }
        Ok(key)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        use super::ProjectionWithExprExec;
        ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn evaluate_aggregate_arg(
        &self,
        agg_expr: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        use yachtsql_ir::FunctionName;
        match agg_expr {
            Expr::Aggregate {
                name,
                args,
                filter,
                order_by,
                ..
            } => {
                if let Some(filter_expr) = filter {
                    let filter_result = self.evaluate_expr(filter_expr, batch, row_idx)?;
                    if filter_result.as_bool() != Some(true) {
                        return Ok(Value::null());
                    }
                }
                match name {
                    FunctionName::PercentileCont
                    | FunctionName::PercentileDisc
                    | FunctionName::Median
                    | FunctionName::Mode => {
                        if let Some(order_exprs) = order_by {
                            if !order_exprs.is_empty() {
                                return self.evaluate_expr(&order_exprs[0].expr, batch, row_idx);
                            }
                        }
                        if !args.is_empty() {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        } else {
                            Ok(Value::null())
                        }
                    }
                    FunctionName::ArrayAgg | FunctionName::StringAgg => {
                        let value = if args.is_empty() {
                            Value::null()
                        } else {
                            self.evaluate_expr(&args[0], batch, row_idx)?
                        };
                        if let Some(order_exprs) = order_by {
                            if !order_exprs.is_empty() {
                                let mut sort_keys = Vec::with_capacity(order_exprs.len() * 2);
                                for order_expr in order_exprs {
                                    let sort_val =
                                        self.evaluate_expr(&order_expr.expr, batch, row_idx)?;
                                    let asc = order_expr.asc.unwrap_or(true);
                                    sort_keys.push(sort_val);
                                    sort_keys.push(Value::bool_val(asc));
                                }
                                return Ok(Value::array(vec![value, Value::array(sort_keys)]));
                            }
                        }
                        Ok(value)
                    }
                    FunctionName::UniqUpTo => {
                        if args.len() >= 2 {
                            self.evaluate_expr(&args[1], batch, row_idx)
                        } else if !args.is_empty() {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        } else {
                            Ok(Value::null())
                        }
                    }
                    FunctionName::UniqIf => {
                        if args.len() >= 2 {
                            let condition = self.evaluate_expr(&args[1], batch, row_idx)?;
                            if condition.as_bool() == Some(true) {
                                self.evaluate_expr(&args[0], batch, row_idx)
                            } else {
                                Ok(Value::null())
                            }
                        } else if !args.is_empty() {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        } else {
                            Ok(Value::null())
                        }
                    }
                    _ => {
                        if args.is_empty() {
                            Ok(Value::int64(1))
                        } else if args.len() >= 2 {
                            let needs_array = matches!(
                                name,
                                FunctionName::Corr
                                    | FunctionName::CovarPop
                                    | FunctionName::CovarSamp
                                    | FunctionName::AvgWeighted
                                    | FunctionName::ArgMin
                                    | FunctionName::ArgMax
                                    | FunctionName::TopK
                                    | FunctionName::WindowFunnel
                                    | FunctionName::RegrSlope
                                    | FunctionName::RegrIntercept
                                    | FunctionName::RegrCount
                                    | FunctionName::RegrR2
                                    | FunctionName::RegrAvgx
                                    | FunctionName::RegrAvgy
                                    | FunctionName::RegrSxx
                                    | FunctionName::RegrSyy
                                    | FunctionName::RegrSxy
                                    | FunctionName::JsonObjectAgg
                                    | FunctionName::JsonbObjectAgg
                                    | FunctionName::ApproxTopSum
                                    | FunctionName::CramersV
                                    | FunctionName::CramersVBiasCorrected
                                    | FunctionName::TheilU
                                    | FunctionName::ContingencyCoefficient
                                    | FunctionName::RankCorr
                                    | FunctionName::ExponentialMovingAverage
                                    | FunctionName::Uniq
                                    | FunctionName::UniqExact
                                    | FunctionName::UniqHll12
                                    | FunctionName::UniqCombined
                                    | FunctionName::UniqCombined64
                                    | FunctionName::UniqThetaSketch
                                    | FunctionName::ApproxCountDistinct
                                    | FunctionName::ApproxDistinct
                                    | FunctionName::Ndv
                            );
                            if needs_array {
                                let mut values = Vec::with_capacity(args.len());
                                for arg in args {
                                    values.push(self.evaluate_expr(arg, batch, row_idx)?);
                                }
                                Ok(Value::array(values))
                            } else {
                                self.evaluate_expr(&args[0], batch, row_idx)
                            }
                        } else {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        }
                    }
                }
            }

            _ => self.evaluate_expr(agg_expr, batch, row_idx),
        }
    }

    pub(crate) fn infer_expr_type(
        expr: &Expr,
        schema: &Schema,
    ) -> Option<yachtsql_core::types::DataType> {
        match expr {
            Expr::Column { name, .. } => schema
                .fields()
                .iter()
                .find(|f| f.name.eq_ignore_ascii_case(name))
                .map(|f| f.data_type.clone()),
            Expr::Literal(lit) => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::expr::LiteralValue;
                Some(match lit {
                    LiteralValue::Int64(_) => DataType::Int64,
                    LiteralValue::Float64(_) => DataType::Float64,
                    LiteralValue::Numeric(_) => DataType::Numeric(None),
                    LiteralValue::String(_) => DataType::String,
                    LiteralValue::Boolean(_) => DataType::Bool,
                    LiteralValue::Date(_) => DataType::Date,
                    LiteralValue::Time(_) => DataType::Time,
                    LiteralValue::DateTime(_) => DataType::DateTime,
                    LiteralValue::Timestamp(_) => DataType::Timestamp,
                    LiteralValue::Json(_) => DataType::Json,
                    LiteralValue::Bytes(_) => DataType::Bytes,
                    LiteralValue::Uuid(_) => DataType::Uuid,
                    LiteralValue::Interval(_) => DataType::Interval,
                    LiteralValue::Array(elements) => {
                        let elem_type = elements
                            .first()
                            .and_then(|e| Self::infer_expr_type(e, schema))
                            .unwrap_or(DataType::String);
                        DataType::Array(Box::new(elem_type))
                    }
                    LiteralValue::Vector(v) => DataType::Vector(v.len()),
                    LiteralValue::Null => return None,
                    LiteralValue::Range(_) => {
                        DataType::Range(yachtsql_core::types::RangeType::Int4Range)
                    }
                    LiteralValue::Point(_) => DataType::Point,
                    LiteralValue::PgBox(_) => DataType::PgBox,
                    LiteralValue::Circle(_) => DataType::Circle,
                    LiteralValue::Line(_) => DataType::Line,
                    LiteralValue::Lseg(_) => DataType::Lseg,
                    LiteralValue::Path(_) => DataType::Path,
                    LiteralValue::Polygon(_) => DataType::Polygon,
                    LiteralValue::MacAddr(_) => DataType::MacAddr,
                    LiteralValue::MacAddr8(_) => DataType::MacAddr8,
                })
            }
            Expr::Cast { data_type, .. } | Expr::TryCast { data_type, .. } => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::expr::CastDataType;
                Some(match data_type {
                    CastDataType::Int64 => DataType::Int64,
                    CastDataType::Float64 => DataType::Float64,
                    CastDataType::Numeric(p) => DataType::Numeric(*p),
                    CastDataType::String => DataType::String,
                    CastDataType::Bool => DataType::Bool,
                    CastDataType::Date => DataType::Date,
                    CastDataType::DateTime => DataType::DateTime,
                    CastDataType::Time => DataType::Time,
                    CastDataType::Timestamp => DataType::Timestamp,
                    CastDataType::TimestampTz => DataType::TimestampTz,
                    CastDataType::Bytes => DataType::Bytes,
                    CastDataType::Json => DataType::Json,
                    CastDataType::Uuid => DataType::Uuid,
                    CastDataType::Interval => DataType::Interval,
                    CastDataType::Geography => DataType::Geography,
                    CastDataType::Vector(dims) => DataType::Vector(*dims),
                    CastDataType::Hstore => DataType::Hstore,
                    CastDataType::MacAddr => DataType::MacAddr,
                    CastDataType::MacAddr8 => DataType::MacAddr8,
                    CastDataType::Inet => DataType::Inet,
                    CastDataType::Cidr => DataType::Cidr,
                    CastDataType::Int4Range => {
                        DataType::Range(yachtsql_core::types::RangeType::Int4Range)
                    }
                    CastDataType::Int8Range => {
                        DataType::Range(yachtsql_core::types::RangeType::Int8Range)
                    }
                    CastDataType::NumRange => {
                        DataType::Range(yachtsql_core::types::RangeType::NumRange)
                    }
                    CastDataType::TsRange => {
                        DataType::Range(yachtsql_core::types::RangeType::TsRange)
                    }
                    CastDataType::TsTzRange => {
                        DataType::Range(yachtsql_core::types::RangeType::TsTzRange)
                    }
                    CastDataType::DateRange => {
                        DataType::Range(yachtsql_core::types::RangeType::DateRange)
                    }
                    CastDataType::Point => DataType::Point,
                    CastDataType::PgBox => DataType::PgBox,
                    CastDataType::Circle => DataType::Circle,
                    CastDataType::Xid => DataType::Xid,
                    CastDataType::Xid8 => DataType::Xid8,
                    CastDataType::Tid => DataType::Tid,
                    CastDataType::Cid => DataType::Cid,
                    CastDataType::Oid => DataType::Oid,
                    CastDataType::Array(inner) => {
                        let inner_expr = Expr::Cast {
                            expr: Box::new(Expr::Literal(yachtsql_ir::expr::LiteralValue::Null)),
                            data_type: (**inner).clone(),
                        };
                        let inner_type =
                            Self::infer_expr_type(&inner_expr, schema).unwrap_or(DataType::String);
                        DataType::Array(Box::new(inner_type))
                    }
                    CastDataType::Custom(name, _) => DataType::Custom(name.clone()),
                })
            }
            Expr::BinaryOp { left, op, right } => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::expr::BinaryOp;
                match op {
                    BinaryOp::Equal
                    | BinaryOp::NotEqual
                    | BinaryOp::LessThan
                    | BinaryOp::LessThanOrEqual
                    | BinaryOp::GreaterThan
                    | BinaryOp::GreaterThanOrEqual
                    | BinaryOp::And
                    | BinaryOp::Or => Some(DataType::Bool),
                    BinaryOp::Add | BinaryOp::Subtract | BinaryOp::Multiply | BinaryOp::Divide => {
                        let left_type = Self::infer_expr_type(left, schema);
                        let right_type = Self::infer_expr_type(right, schema);
                        match (&left_type, &right_type) {
                            (Some(DataType::Float64), _) | (_, Some(DataType::Float64)) => {
                                Some(DataType::Float64)
                            }
                            (Some(DataType::Numeric(_)), _) | (_, Some(DataType::Numeric(_))) => {
                                Some(DataType::Numeric(None))
                            }
                            (Some(DataType::Int64), _) | (_, Some(DataType::Int64)) => {
                                Some(DataType::Int64)
                            }
                            _ => left_type.or(right_type),
                        }
                    }
                    _ => None,
                }
            }
            Expr::Function { name, args } => {
                use yachtsql_core::types::DataType;
                use yachtsql_ir::FunctionName;
                match name {
                    FunctionName::Count
                    | FunctionName::Length
                    | FunctionName::Len
                    | FunctionName::CharLength
                    | FunctionName::CharacterLength => Some(DataType::Int64),
                    FunctionName::Sum
                    | FunctionName::Min
                    | FunctionName::Minimum
                    | FunctionName::Max
                    | FunctionName::Maximum => {
                        args.first().and_then(|a| Self::infer_expr_type(a, schema))
                    }
                    FunctionName::Avg
                    | FunctionName::Average
                    | FunctionName::Stddev
                    | FunctionName::Stdev
                    | FunctionName::StandardDeviation
                    | FunctionName::Variance
                    | FunctionName::Var => Some(DataType::Float64),
                    FunctionName::Concat
                    | FunctionName::Concatenate
                    | FunctionName::Upper
                    | FunctionName::Ucase
                    | FunctionName::Lower
                    | FunctionName::Lcase
                    | FunctionName::Trim
                    | FunctionName::Btrim => Some(DataType::String),
                    FunctionName::Coalesce => {
                        args.iter().find_map(|a| Self::infer_expr_type(a, schema))
                    }
                    FunctionName::DateTrunc
                    | FunctionName::TruncDate
                    | FunctionName::Date
                    | FunctionName::DateAdd
                    | FunctionName::Dateadd
                    | FunctionName::Adddate
                    | FunctionName::DateSub
                    | FunctionName::Datesub
                    | FunctionName::Subdate
                    | FunctionName::CurrentDate
                    | FunctionName::Curdate
                    | FunctionName::Today
                    | FunctionName::ParseDate
                    | FunctionName::MakeDate
                    | FunctionName::LastDay => Some(DataType::Date),
                    FunctionName::CurrentTimestamp
                    | FunctionName::Now
                    | FunctionName::Getdate
                    | FunctionName::TimestampTrunc
                    | FunctionName::ParseTimestamp
                    | FunctionName::MakeTimestamp
                    | FunctionName::AtTimeZone => Some(DataType::Timestamp),
                    FunctionName::Extract
                    | FunctionName::DatePart
                    | FunctionName::Datepart
                    | FunctionName::DateDiff
                    | FunctionName::Datediff
                    | FunctionName::TimestampDiff
                    | FunctionName::Timestampdiff
                    | FunctionName::Year
                    | FunctionName::Month
                    | FunctionName::Day
                    | FunctionName::Hour
                    | FunctionName::Minute
                    | FunctionName::Second
                    | FunctionName::Quarter
                    | FunctionName::Week
                    | FunctionName::Dow
                    | FunctionName::Doy
                    | FunctionName::Dayofweek
                    | FunctionName::Dayofyear => Some(DataType::Int64),
                    FunctionName::Custom(s) => match s.as_str() {
                        "TUMBLE" | "TUMBLESTART" | "TUMBLEEND" | "HOP" | "HOPSTART" | "HOPEND"
                        | "TIMESLOT" | "DATE_BIN" => Some(DataType::DateTime),
                        "TIMESLOTS" => Some(DataType::Array(Box::new(DataType::DateTime))),
                        "WINDOWID" => Some(DataType::Int64),
                        _ => None,
                    },
                    _ => None,
                }
            }
            Expr::Case {
                when_then,
                else_expr,
                ..
            } => {
                for (_, then_expr) in when_then {
                    if let Some(t) = Self::infer_expr_type(then_expr, schema) {
                        return Some(t);
                    }
                }
                if let Some(else_e) = else_expr {
                    return Self::infer_expr_type(else_e, schema);
                }
                None
            }
            _ => None,
        }
    }

    pub(crate) fn infer_aggregate_type(
        expr: &Expr,
        schema: &Schema,
    ) -> Option<yachtsql_core::types::DataType> {
        use yachtsql_core::types::DataType;
        use yachtsql_ir::FunctionName;

        match expr {
            Expr::Aggregate { name, args, .. } => match name {
                FunctionName::Count => Some(DataType::Int64),
                FunctionName::Sum
                | FunctionName::SumWithOverflow
                | FunctionName::Min
                | FunctionName::Max => {
                    if let Some(arg) = args.first() {
                        Self::infer_expr_type(arg, schema)
                    } else {
                        Some(DataType::Float64)
                    }
                }
                FunctionName::Avg => {
                    if let Some(arg) = args.first() {
                        let input_type = Self::infer_expr_type(arg, schema);
                        match input_type {
                            Some(DataType::Numeric(precision)) => {
                                Some(DataType::Numeric(precision))
                            }
                            _ => Some(DataType::Float64),
                        }
                    } else {
                        Some(DataType::Float64)
                    }
                }

                FunctionName::Stddev
                | FunctionName::StddevPop
                | FunctionName::StddevSamp
                | FunctionName::Variance
                | FunctionName::VarPop
                | FunctionName::VarSamp => Some(DataType::Float64),

                FunctionName::ApproxCountDistinct
                | FunctionName::ApproxDistinct
                | FunctionName::Ndv => Some(DataType::Int64),

                FunctionName::ApproxQuantiles => Some(DataType::Array(Box::new(DataType::Float64))),

                FunctionName::ApproxTopCount | FunctionName::ApproxTopSum => {
                    use yachtsql_core::types::StructField;
                    Some(DataType::Array(Box::new(DataType::Struct(vec![
                        StructField {
                            name: "value".to_string(),
                            data_type: DataType::String,
                        },
                        StructField {
                            name: "count".to_string(),
                            data_type: DataType::Int64,
                        },
                    ]))))
                }

                FunctionName::ArrayAgg => {
                    if let Some(arg) = args.first() {
                        let elem_type =
                            Self::infer_expr_type(arg, schema).unwrap_or(DataType::String);
                        Some(DataType::Array(Box::new(elem_type)))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                }

                FunctionName::StringAgg => Some(DataType::String),

                FunctionName::ArgMin | FunctionName::ArgMax => {
                    if let Some(arg) = args.first() {
                        Self::infer_expr_type(arg, schema)
                    } else {
                        Some(DataType::String)
                    }
                }

                FunctionName::Any | FunctionName::AnyLast | FunctionName::AnyHeavy => {
                    if let Some(arg) = args.first() {
                        Self::infer_expr_type(arg, schema)
                    } else {
                        Some(DataType::String)
                    }
                }

                FunctionName::Uniq
                | FunctionName::UniqExact
                | FunctionName::UniqHll12
                | FunctionName::UniqCombined
                | FunctionName::UniqCombined64
                | FunctionName::UniqThetaSketch => Some(DataType::Int64),

                FunctionName::Quantile
                | FunctionName::QuantileExact
                | FunctionName::QuantileTDigest
                | FunctionName::QuantileTiming => Some(DataType::Float64),

                FunctionName::QuantilesTDigest | FunctionName::QuantilesTiming => {
                    Some(DataType::Array(Box::new(DataType::Float64)))
                }

                FunctionName::GroupArray | FunctionName::GroupUniqArray => {
                    if let Some(arg) = args.first() {
                        let elem_type =
                            Self::infer_expr_type(arg, schema).unwrap_or(DataType::String);
                        Some(DataType::Array(Box::new(elem_type)))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                }

                FunctionName::GroupArrayMovingAvg | FunctionName::GroupArrayMovingSum => {
                    Some(DataType::Array(Box::new(DataType::Float64)))
                }

                FunctionName::IntervalLengthSum => Some(DataType::Int64),

                FunctionName::TopK => {
                    if let Some(arg) = args.first() {
                        let elem_type =
                            Self::infer_expr_type(arg, schema).unwrap_or(DataType::String);
                        Some(DataType::Array(Box::new(elem_type)))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                }

                FunctionName::WindowFunnel => Some(DataType::Int64),

                FunctionName::Retention => Some(DataType::Array(Box::new(DataType::Bool))),

                FunctionName::BitAnd | FunctionName::BitOr | FunctionName::BitXor => {
                    Some(DataType::Int64)
                }

                FunctionName::BoolAnd | FunctionName::BoolOr | FunctionName::Every => {
                    Some(DataType::Bool)
                }

                FunctionName::JsonAgg
                | FunctionName::JsonbAgg
                | FunctionName::JsonObjectAgg
                | FunctionName::JsonbObjectAgg => Some(DataType::Json),

                FunctionName::GroupBitmapState => Some(DataType::Array(Box::new(DataType::Int64))),

                FunctionName::SumMap
                | FunctionName::SumMapWithOverflow
                | FunctionName::MinMap
                | FunctionName::MaxMap
                | FunctionName::AvgMap => Some(DataType::Array(Box::new(DataType::Array(
                    Box::new(DataType::Unknown),
                )))),

                FunctionName::GroupArrayIntersect | FunctionName::GroupArrayLast => {
                    if let Some(arg) = args.first() {
                        let elem_type =
                            Self::infer_expr_type(arg, schema).unwrap_or(DataType::String);
                        Some(DataType::Array(Box::new(elem_type)))
                    } else {
                        Some(DataType::Array(Box::new(DataType::String)))
                    }
                }

                _ => Some(DataType::Float64),
            },
            _ => None,
        }
    }

    fn evaluate_having(&self, _group_values: &[Value], _agg_values: &[Value]) -> Result<bool> {
        match &self.having {
            None => Ok(true),
            Some(_having_expr) => Ok(true),
        }
    }

    pub(crate) fn expr_to_field_name(expr: &Expr) -> Option<String> {
        match expr {
            Expr::Aggregate {
                name,
                args,
                distinct,
                ..
            } => {
                let first_is_wildcard = args.first().is_some_and(|e| matches!(e, Expr::Wildcard));
                let arg_str = if args.is_empty() || first_is_wildcard {
                    "*".to_string()
                } else {
                    let arg_strs: Vec<String> = args
                        .iter()
                        .map(|arg| match arg {
                            Expr::Column { name: col_name, .. } => col_name.clone(),
                            Expr::Wildcard => "*".to_string(),
                            _ => "...".to_string(),
                        })
                        .collect();
                    arg_strs.join(", ")
                };
                if *distinct {
                    Some(format!("{}(DISTINCT {})", name.as_str(), arg_str))
                } else {
                    Some(format!("{}({})", name.as_str(), arg_str))
                }
            }
            Expr::Column { name, .. } => Some(name.clone()),
            Expr::Literal(lit) => Some(format!("{:?}", lit)),
            _ => None,
        }
    }
}

impl ExecutionPlan for AggregateExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            if self.group_by.is_empty() {
                let empty_agg_values: Vec<Value> = self
                    .aggregates
                    .iter()
                    .map(|(agg_expr, _)| match agg_expr {
                        Expr::Aggregate { name, .. } => {
                            use yachtsql_ir::FunctionName;
                            match name {
                                FunctionName::Count => Value::int64(0),
                                _ => Value::null(),
                            }
                        }
                        _ => Value::null(),
                    })
                    .collect();

                let mut columns = Vec::new();

                for (idx, field) in self.schema.fields().iter().enumerate() {
                    let mut column = Column::new(&field.data_type, 1);
                    column.push(empty_agg_values.get(idx).cloned().unwrap_or(Value::null()))?;
                    columns.push(column);
                }

                return Ok(vec![Table::new(self.schema.clone(), columns)?]);
            }
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut groups: HashMap<Vec<u8>, (Vec<Value>, Vec<Vec<Value>>)> = HashMap::new();

        for input_batch in input_batches {
            let num_rows = input_batch.num_rows();

            for row_idx in 0..num_rows {
                let group_key = self.compute_group_key(&input_batch, row_idx)?;
                let key_bytes = serialize_key(&group_key);

                let mut agg_input_values = Vec::new();
                for (agg_expr, _) in &self.aggregates {
                    let value = self.evaluate_aggregate_arg(agg_expr, &input_batch, row_idx)?;
                    agg_input_values.push(value);
                }

                groups
                    .entry(key_bytes)
                    .or_insert_with(|| (group_key.clone(), Vec::new()))
                    .1
                    .push(agg_input_values);
            }
        }

        if groups.is_empty() && self.group_by.is_empty() {
            let empty_agg_values: Vec<Value> = self
                .aggregates
                .iter()
                .map(|(agg_expr, _)| match agg_expr {
                    Expr::Aggregate { name, .. } => {
                        use yachtsql_ir::FunctionName;
                        match name {
                            FunctionName::Count => Value::int64(0),
                            _ => Value::null(),
                        }
                    }
                    _ => Value::null(),
                })
                .collect();

            let mut columns = Vec::new();

            for (idx, field) in self.schema.fields().iter().enumerate() {
                let mut column = Column::new(&field.data_type, 1);
                column.push(empty_agg_values.get(idx).cloned().unwrap_or(Value::null()))?;
                columns.push(column);
            }

            return Ok(vec![Table::new(self.schema.clone(), columns)?]);
        }

        let mut result_rows = Vec::new();

        for (group_values, agg_input_rows) in groups.values() {
            let agg_values = self.compute_aggregates(agg_input_rows)?;

            if self.evaluate_having(group_values, &agg_values)? {
                let mut row = group_values.clone();
                row.extend(agg_values);
                result_rows.push(row);
            }
        }

        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in &result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "Aggregate [group_by: {}, aggregates: {}]",
            self.group_by.len(),
            self.aggregates.len()
        )
    }
}

impl AggregateExec {
    fn try_create_column(values: &[&Value]) -> Option<Column> {
        if values.is_empty() {
            return None;
        }

        let first_type = values.iter().find(|v| !v.is_null())?;

        if first_type.as_i64().is_some() {
            let mut column = Column::new(&yachtsql_core::types::DataType::Int64, values.len());
            for val in values {
                if column.push((*val).clone()).is_err() {
                    return None;
                }
            }
            return Some(column);
        }

        if first_type.as_f64().is_some() {
            let mut column = Column::new(&yachtsql_core::types::DataType::Float64, values.len());
            for val in values {
                if column.push((*val).clone()).is_err() {
                    return None;
                }
            }
            return Some(column);
        }

        None
    }

    fn compute_aggregates(&self, agg_input_rows: &[Vec<Value>]) -> Result<Vec<Value>> {
        use yachtsql_ir::FunctionName;
        let mut result = Vec::with_capacity(self.aggregates.len());

        for agg_idx in 0..self.aggregates.len() {
            let values: Vec<&Value> = agg_input_rows.iter().map(|row| &row[agg_idx]).collect();

            let agg_result = match &self.aggregates[agg_idx].0 {
                Expr::Aggregate {
                    name,
                    args,
                    distinct,
                    ..
                } => match name {
                    FunctionName::Count => {
                        if *distinct {
                            let mut unique_values = std::collections::HashSet::new();
                            for val in &values {
                                if !val.is_null() {
                                    unique_values.insert(format!("{:?}", *val));
                                }
                            }
                            Value::int64(unique_values.len() as i64)
                        } else {
                            let count = values.iter().filter(|v| !v.is_null()).count();
                            Value::int64(count as i64)
                        }
                    }
                    FunctionName::CountIf => {
                        let count = values.iter().filter(|v| v.as_bool() == Some(true)).count();
                        Value::int64(count as i64)
                    }
                    FunctionName::Sum => {
                        let values_to_sum: Vec<&Value> = if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            values
                                .iter()
                                .filter(|v| {
                                    if v.is_null() {
                                        return false;
                                    }
                                    let key = format!("{:?}", *v);
                                    unique_strs.insert(key)
                                })
                                .copied()
                                .collect()
                        } else {
                            values.to_vec()
                        };

                        let has_numeric = values_to_sum.iter().any(|v| v.as_numeric().is_some());

                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut has_values = false;
                            for val in &values_to_sum {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    has_values = true;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        has_values = true;
                                    }
                                }
                            }
                            if has_values {
                                Value::numeric(sum)
                            } else {
                                Value::null()
                            }
                        } else if !*distinct {
                            if let Some(column) = Self::try_create_column(&values) {
                                match column {
                                    Column::Int64 { .. } => {
                                        if let Ok(sum) = column.sum_i64() {
                                            Value::int64(sum)
                                        } else {
                                            Value::null()
                                        }
                                    }
                                    Column::Float64 { .. } => {
                                        if let Ok(sum) = column.sum_f64() {
                                            Value::float64(sum)
                                        } else {
                                            Value::null()
                                        }
                                    }
                                    _ => {
                                        let mut sum = 0.0;
                                        let mut has_values = false;
                                        for val in values {
                                            if let Some(i) = val.as_i64() {
                                                sum += i as f64;
                                                has_values = true;
                                            } else if let Some(f) = val.as_f64() {
                                                sum += f;
                                                has_values = true;
                                            }
                                        }
                                        if has_values {
                                            Value::float64(sum)
                                        } else {
                                            Value::null()
                                        }
                                    }
                                }
                            } else {
                                let mut sum = 0.0;
                                let mut has_values = false;
                                for val in values {
                                    if let Some(i) = val.as_i64() {
                                        sum += i as f64;
                                        has_values = true;
                                    } else if let Some(f) = val.as_f64() {
                                        sum += f;
                                        has_values = true;
                                    }
                                }
                                if has_values {
                                    Value::float64(sum)
                                } else {
                                    Value::null()
                                }
                            }
                        } else {
                            let mut sum = 0.0;
                            let mut has_values = false;
                            for val in values_to_sum {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    has_values = true;
                                }
                            }
                            if has_values {
                                Value::float64(sum)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    FunctionName::Avg => {
                        let values_to_avg: Vec<&Value> = if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            values
                                .iter()
                                .filter(|v| {
                                    if v.is_null() {
                                        return false;
                                    }
                                    let key = format!("{:?}", *v);
                                    unique_strs.insert(key)
                                })
                                .copied()
                                .collect()
                        } else {
                            values.to_vec()
                        };

                        let has_numeric = values_to_avg.iter().any(|v| v.as_numeric().is_some());

                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut count = 0u32;
                            for val in &values_to_avg {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    count += 1;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        count += 1;
                                    }
                                }
                            }
                            if count > 0 {
                                let avg = sum / rust_decimal::Decimal::from(count);
                                Value::numeric(avg)
                            } else {
                                Value::null()
                            }
                        } else if !*distinct {
                            if let Some(column) = Self::try_create_column(&values) {
                                match column {
                                    Column::Int64 { .. } => {
                                        if let Ok(Some(avg)) = column.avg_i64() {
                                            Value::float64(avg)
                                        } else {
                                            Value::null()
                                        }
                                    }
                                    Column::Float64 { .. } => {
                                        if let Ok(Some(avg)) = column.avg_f64() {
                                            Value::float64(avg)
                                        } else {
                                            Value::null()
                                        }
                                    }
                                    _ => {
                                        let mut sum = 0.0;
                                        let mut count = 0;
                                        for val in values {
                                            if let Some(i) = val.as_i64() {
                                                sum += i as f64;
                                                count += 1;
                                            } else if let Some(f) = val.as_f64() {
                                                sum += f;
                                                count += 1;
                                            }
                                        }
                                        if count > 0 {
                                            Value::float64(sum / count as f64)
                                        } else {
                                            Value::null()
                                        }
                                    }
                                }
                            } else {
                                let mut sum = 0.0;
                                let mut count = 0;
                                for val in values {
                                    if let Some(i) = val.as_i64() {
                                        sum += i as f64;
                                        count += 1;
                                    } else if let Some(f) = val.as_f64() {
                                        sum += f;
                                        count += 1;
                                    }
                                }
                                if count > 0 {
                                    Value::float64(sum / count as f64)
                                } else {
                                    Value::null()
                                }
                            }
                        } else {
                            let mut sum = 0.0;
                            let mut count = 0;
                            for val in values_to_avg {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 {
                                Value::float64(sum / count as f64)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    FunctionName::Min => {
                        if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Int64 { .. } => {
                                    if let Ok(Some(min)) = column.min_i64() {
                                        Value::int64(min)
                                    } else {
                                        Value::null()
                                    }
                                }
                                _ => {
                                    let mut min: Option<Value> = None;
                                    for val in values {
                                        if val.is_null() {
                                            continue;
                                        }
                                        min = Some(match min {
                                            None => (*val).clone(),
                                            Some(ref current_min) => {
                                                if compare_values(val, current_min)?
                                                    == std::cmp::Ordering::Less
                                                {
                                                    (*val).clone()
                                                } else {
                                                    current_min.clone()
                                                }
                                            }
                                        });
                                    }
                                    min.unwrap_or(Value::null())
                                }
                            }
                        } else {
                            let mut min: Option<Value> = None;
                            for val in values {
                                if val.is_null() {
                                    continue;
                                }
                                min = Some(match min {
                                    None => (*val).clone(),
                                    Some(ref current_min) => {
                                        if compare_values(val, current_min)?
                                            == std::cmp::Ordering::Less
                                        {
                                            (*val).clone()
                                        } else {
                                            current_min.clone()
                                        }
                                    }
                                });
                            }
                            min.unwrap_or(Value::null())
                        }
                    }
                    FunctionName::Max => {
                        if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Int64 { .. } => {
                                    if let Ok(Some(max)) = column.max_i64() {
                                        Value::int64(max)
                                    } else {
                                        Value::null()
                                    }
                                }
                                _ => {
                                    let mut max: Option<Value> = None;
                                    for val in values {
                                        if val.is_null() {
                                            continue;
                                        }
                                        max = Some(match max {
                                            None => (*val).clone(),
                                            Some(ref current_max) => {
                                                if compare_values(val, current_max)?
                                                    == std::cmp::Ordering::Greater
                                                {
                                                    (*val).clone()
                                                } else {
                                                    current_max.clone()
                                                }
                                            }
                                        });
                                    }
                                    max.unwrap_or(Value::null())
                                }
                            }
                        } else {
                            let mut max: Option<Value> = None;
                            for val in values {
                                if val.is_null() {
                                    continue;
                                }
                                max = Some(match max {
                                    None => (*val).clone(),
                                    Some(ref current_max) => {
                                        if compare_values(val, current_max)?
                                            == std::cmp::Ordering::Greater
                                        {
                                            (*val).clone()
                                        } else {
                                            current_max.clone()
                                        }
                                    }
                                });
                            }
                            max.unwrap_or(Value::null())
                        }
                    }
                    FunctionName::Variance
                    | FunctionName::VarSamp
                    | FunctionName::StddevSamp
                    | FunctionName::Stddev
                    | FunctionName::VarPop
                    | FunctionName::StddevPop => {
                        if let Some(column) = Self::try_create_column(&values) {
                            match column {
                                Column::Float64 { .. } => {
                                    let result = match name {
                                        FunctionName::VarPop => column.variance_pop_f64(),
                                        FunctionName::VarSamp | FunctionName::Variance => {
                                            column.variance_samp_f64()
                                        }
                                        FunctionName::StddevPop => column.stddev_pop_f64(),
                                        FunctionName::StddevSamp | FunctionName::Stddev => {
                                            column.stddev_samp_f64()
                                        }
                                        _ => Ok(None),
                                    };
                                    match result {
                                        Ok(Some(val)) => Value::float64(val),
                                        _ => Value::null(),
                                    }
                                }
                                Column::Int64 { data, nulls } => {
                                    let mut float_data =
                                        aligned_vec::AVec::with_capacity(64, data.len());
                                    for &val in data.as_slice() {
                                        float_data.push(val as f64);
                                    }
                                    let float_col = Column::Float64 {
                                        data: float_data,
                                        nulls: nulls.clone(),
                                    };
                                    let result = match name {
                                        FunctionName::VarPop => float_col.variance_pop_f64(),
                                        FunctionName::VarSamp | FunctionName::Variance => {
                                            float_col.variance_samp_f64()
                                        }
                                        FunctionName::StddevPop => float_col.stddev_pop_f64(),
                                        FunctionName::StddevSamp | FunctionName::Stddev => {
                                            float_col.stddev_samp_f64()
                                        }
                                        _ => Ok(None),
                                    };
                                    match result {
                                        Ok(Some(val)) => Value::float64(val),
                                        _ => Value::null(),
                                    }
                                }
                                _ => Value::null(),
                            }
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::ApproxQuantiles => {
                        let num_quantiles = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(n)) => {
                                    *n as usize
                                }
                                _ => 4,
                            }
                        } else {
                            4
                        };

                        let mut numeric_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| {
                                if v.is_null() {
                                    None
                                } else if let Some(i) = v.as_i64() {
                                    Some(i as f64)
                                } else {
                                    v.as_f64()
                                }
                            })
                            .collect();

                        if numeric_values.is_empty() {
                            Value::null()
                        } else {
                            numeric_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });

                            let mut quantile_values = Vec::with_capacity(num_quantiles + 1);
                            let len = numeric_values.len();

                            for i in 0..=num_quantiles {
                                let p = i as f64 / num_quantiles as f64;
                                let idx = (p * (len - 1) as f64).round() as usize;
                                quantile_values.push(Value::float64(numeric_values[idx]));
                            }

                            Value::array(quantile_values)
                        }
                    }
                    FunctionName::ArrayAgg => {
                        let has_sort_keys = values.first().is_some_and(|v| {
                            if let Some(arr) = v.as_array() {
                                arr.len() == 2 && arr[1].as_array().is_some()
                            } else {
                                false
                            }
                        });

                        if has_sort_keys {
                            let mut sorted_entries: Vec<(Value, Vec<Value>)> = values
                                .iter()
                                .filter_map(|v| {
                                    if let Some(arr) = v.as_array() {
                                        if arr.len() == 2 {
                                            let value = arr[0].clone();
                                            let sort_keys = arr[1]
                                                .as_array()
                                                .map(|a| a.to_vec())
                                                .unwrap_or_default();
                                            return Some((value, sort_keys));
                                        }
                                    }
                                    None
                                })
                                .collect();

                            sorted_entries.sort_by(|a, b| {
                                let keys_a = &a.1;
                                let keys_b = &b.1;
                                let mut i = 0;
                                while i < keys_a.len() && i + 1 < keys_a.len() {
                                    let val_a = &keys_a[i];
                                    let val_b = &keys_b[i];
                                    let asc = keys_a[i + 1].as_bool().unwrap_or(true);
                                    let cmp = compare_values(val_a, val_b)
                                        .unwrap_or(std::cmp::Ordering::Equal);
                                    if cmp != std::cmp::Ordering::Equal {
                                        return if asc { cmp } else { cmp.reverse() };
                                    }
                                    i += 2;
                                }
                                std::cmp::Ordering::Equal
                            });

                            let array_values: Vec<Value> =
                                sorted_entries.into_iter().map(|(v, _)| v).collect();

                            if *distinct {
                                let mut unique_strs = std::collections::HashSet::new();
                                let mut unique_values = Vec::new();
                                for val in array_values {
                                    let str_repr = format!("{:?}", val);
                                    if unique_strs.insert(str_repr) {
                                        unique_values.push(val);
                                    }
                                }
                                Value::array(unique_values)
                            } else {
                                Value::array(array_values)
                            }
                        } else {
                            let array_values: Vec<Value> =
                                values.iter().map(|v| (*v).clone()).collect();
                            if *distinct {
                                let mut unique_strs = std::collections::HashSet::new();
                                let mut unique_values = Vec::new();
                                for val in array_values {
                                    let str_repr = format!("{:?}", val);
                                    if unique_strs.insert(str_repr) {
                                        unique_values.push(val);
                                    }
                                }
                                Value::array(unique_values)
                            } else {
                                Value::array(array_values)
                            }
                        }
                    }
                    FunctionName::StringAgg => {
                        let delimiter = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::String(s)) => {
                                    s.clone()
                                }
                                _ => String::new(),
                            }
                        } else {
                            String::new()
                        };

                        let has_sort_keys = values.first().is_some_and(|v| {
                            if let Some(arr) = v.as_array() {
                                arr.len() == 2 && arr[1].as_array().is_some()
                            } else {
                                false
                            }
                        });

                        let string_values: Vec<String> = if has_sort_keys {
                            let mut sorted_entries: Vec<(Value, Vec<Value>)> = values
                                .iter()
                                .filter_map(|v| {
                                    if let Some(arr) = v.as_array() {
                                        if arr.len() == 2 {
                                            let value = arr[0].clone();
                                            let sort_keys = arr[1]
                                                .as_array()
                                                .map(|a| a.to_vec())
                                                .unwrap_or_default();
                                            return Some((value, sort_keys));
                                        }
                                    }
                                    None
                                })
                                .collect();

                            sorted_entries.sort_by(|a, b| {
                                let keys_a = &a.1;
                                let keys_b = &b.1;
                                let mut i = 0;
                                while i < keys_a.len() && i + 1 < keys_a.len() {
                                    let val_a = &keys_a[i];
                                    let val_b = &keys_b[i];
                                    let asc = keys_a[i + 1].as_bool().unwrap_or(true);
                                    let cmp = compare_values(val_a, val_b)
                                        .unwrap_or(std::cmp::Ordering::Equal);
                                    if cmp != std::cmp::Ordering::Equal {
                                        return if asc { cmp } else { cmp.reverse() };
                                    }
                                    i += 2;
                                }
                                std::cmp::Ordering::Equal
                            });

                            if *distinct {
                                let mut unique_strs = std::collections::HashSet::new();
                                sorted_entries
                                    .into_iter()
                                    .filter_map(|(v, _)| {
                                        if v.is_null() {
                                            None
                                        } else {
                                            v.as_str().and_then(|s| {
                                                if unique_strs.insert(s.to_string()) {
                                                    Some(s.to_string())
                                                } else {
                                                    None
                                                }
                                            })
                                        }
                                    })
                                    .collect()
                            } else {
                                sorted_entries
                                    .into_iter()
                                    .filter_map(|(v, _)| {
                                        if v.is_null() {
                                            None
                                        } else {
                                            v.as_str().map(|s| s.to_string())
                                        }
                                    })
                                    .collect()
                            }
                        } else if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            values
                                .iter()
                                .filter_map(|v| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        v.as_str().and_then(|s| {
                                            if unique_strs.insert(s.to_string()) {
                                                Some(s.to_string())
                                            } else {
                                                None
                                            }
                                        })
                                    }
                                })
                                .collect()
                        } else {
                            values
                                .iter()
                                .filter_map(|v| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        v.as_str().map(|s| s.to_string())
                                    }
                                })
                                .collect()
                        };

                        if string_values.is_empty() {
                            Value::null()
                        } else {
                            Value::string(string_values.join(&delimiter))
                        }
                    }
                    FunctionName::Corr => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut m2_x = 0.0f64;
                        let mut m2_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        let delta_y = y - mean_y;
                                        mean_x += delta_x / n;
                                        mean_y += delta_y / n;
                                        let delta_x2 = x - mean_x;
                                        let delta_y2 = y - mean_y;
                                        m2_x += delta_x * delta_x2;
                                        m2_y += delta_y * delta_y2;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            let var_x = m2_x / count as f64;
                            let var_y = m2_y / count as f64;
                            if var_x == 0.0 || var_y == 0.0 {
                                Value::null()
                            } else {
                                let corr = coproduct / (count as f64 * var_x.sqrt() * var_y.sqrt());
                                Value::float64(corr)
                            }
                        }
                    }
                    FunctionName::CovarPop => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count == 0 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / count as f64)
                        }
                    }
                    FunctionName::CovarSamp => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / (count - 1) as f64)
                        }
                    }
                    FunctionName::Uniq
                    | FunctionName::UniqExact
                    | FunctionName::UniqHll12
                    | FunctionName::UniqCombined
                    | FunctionName::UniqCombined64
                    | FunctionName::UniqThetaSketch
                    | FunctionName::UniqIf
                    | FunctionName::ApproxCountDistinct
                    | FunctionName::ApproxDistinct
                    | FunctionName::Ndv => {
                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                unique_values.insert(key);
                            }
                        }
                        Value::int64(unique_values.len() as i64)
                    }
                    FunctionName::UniqUpTo => {
                        let threshold = if !args.is_empty() {
                            match &args[0] {
                                Expr::Literal(lit) => {
                                    lit.as_f64().map(|f| f as usize).unwrap_or(100)
                                }
                                _ => 100,
                            }
                        } else {
                            100
                        };

                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                unique_values.insert(key);
                            }
                        }
                        let count = unique_values.len();
                        if count > threshold {
                            Value::int64((threshold + 1) as i64)
                        } else {
                            Value::int64(count as i64)
                        }
                    }
                    FunctionName::ApproxTopCount => {
                        let number = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(n)) => {
                                    *n as usize
                                }
                                _ => 10,
                            }
                        } else {
                            10
                        };

                        let mut freq_map: std::collections::HashMap<String, (usize, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                let entry = freq_map.entry(key).or_insert((0, (*val).clone()));
                                entry.0 += 1;
                            }
                        }
                        let mut freq_vec: Vec<_> = freq_map.into_iter().collect();
                        freq_vec.sort_by(|a, b| b.1.0.cmp(&a.1.0));
                        let top_values: Vec<Value> = freq_vec
                            .iter()
                            .take(number)
                            .map(|(_, (count, val))| {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("value".to_string(), val.clone());
                                map.insert("count".to_string(), Value::int64(*count as i64));
                                Value::struct_val(map)
                            })
                            .collect();
                        Value::array(top_values)
                    }
                    FunctionName::ApproxTopSum => {
                        let number = if args.len() >= 3 {
                            match &args[2] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(n)) => {
                                    *n as usize
                                }
                                _ => 10,
                            }
                        } else {
                            10
                        };

                        let mut sum_map: std::collections::HashMap<String, (f64, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let key_val = &arr[0];
                                    let weight = arr[1]
                                        .as_f64()
                                        .or_else(|| arr[1].as_i64().map(|i| i as f64))
                                        .unwrap_or(0.0);
                                    if !key_val.is_null() {
                                        let key = format!("{:?}", key_val);
                                        let entry =
                                            sum_map.entry(key).or_insert((0.0, key_val.clone()));
                                        entry.0 += weight;
                                    }
                                }
                            }
                        }
                        let mut sum_vec: Vec<_> = sum_map.into_iter().collect();
                        sum_vec.sort_by(|a, b| {
                            b.1.0
                                .partial_cmp(&a.1.0)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                        let top_values: Vec<Value> = sum_vec
                            .iter()
                            .take(number)
                            .map(|(_, (sum, val))| {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("value".to_string(), val.clone());
                                map.insert("sum".to_string(), Value::int64(*sum as i64));
                                Value::struct_val(map)
                            })
                            .collect();
                        Value::array(top_values)
                    }
                    FunctionName::TopK => {
                        let mut freq_map: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                *freq_map.entry(key).or_insert(0) += 1;
                            }
                        }
                        let mut freq_vec: Vec<_> = freq_map.into_iter().collect();
                        freq_vec.sort_by(|a, b| b.1.cmp(&a.1));
                        let top_values: Vec<Value> = freq_vec
                            .iter()
                            .take(10)
                            .map(|(k, _)| Value::string(k.clone()))
                            .collect();
                        Value::array(top_values)
                    }
                    FunctionName::Quantile
                    | FunctionName::QuantileExact
                    | FunctionName::QuantileTiming
                    | FunctionName::QuantileTDigest => {
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let mid = float_values.len() / 2;
                            let median = if float_values.len() % 2 == 0 {
                                (float_values[mid - 1] + float_values[mid]) / 2.0
                            } else {
                                float_values[mid]
                            };
                            Value::float64(median)
                        }
                    }
                    FunctionName::QuantilesTiming | FunctionName::QuantilesTDigest => {
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::array(vec![])
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let quantiles = [0.5, 0.9, 0.99];
                            let result: Vec<Value> = quantiles
                                .iter()
                                .map(|&q| {
                                    let idx =
                                        ((float_values.len() as f64 - 1.0) * q).round() as usize;
                                    Value::float64(float_values[idx.min(float_values.len() - 1)])
                                })
                                .collect();
                            Value::array(result)
                        }
                    }
                    FunctionName::ArgMin => {
                        let mut min_comparison: Option<Value> = None;
                        let mut arg_at_min: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let arg_to_return = &arr[0];
                                    let val_to_compare = &arr[1];
                                    if val_to_compare.is_null() {
                                        continue;
                                    }
                                    let is_smaller = match &min_comparison {
                                        None => true,
                                        Some(current_min) => {
                                            compare_values(val_to_compare, current_min)
                                                .map(|o| o == std::cmp::Ordering::Less)
                                                .unwrap_or(false)
                                        }
                                    };
                                    if is_smaller {
                                        min_comparison = Some(val_to_compare.clone());
                                        arg_at_min = Some(arg_to_return.clone());
                                    }
                                }
                            }
                        }
                        arg_at_min.unwrap_or(Value::null())
                    }
                    FunctionName::ArgMax => {
                        let mut max_comparison: Option<Value> = None;
                        let mut arg_at_max: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let arg_to_return = &arr[0];
                                    let val_to_compare = &arr[1];
                                    if val_to_compare.is_null() {
                                        continue;
                                    }
                                    let is_larger = match &max_comparison {
                                        None => true,
                                        Some(current_max) => {
                                            compare_values(val_to_compare, current_max)
                                                .map(|o| o == std::cmp::Ordering::Greater)
                                                .unwrap_or(false)
                                        }
                                    };
                                    if is_larger {
                                        max_comparison = Some(val_to_compare.clone());
                                        arg_at_max = Some(arg_to_return.clone());
                                    }
                                }
                            }
                        }
                        arg_at_max.unwrap_or(Value::null())
                    }
                    FunctionName::GroupArray => {
                        let arr: Vec<Value> = values.iter().map(|v| (*v).clone()).collect();
                        Value::array(arr)
                    }
                    FunctionName::GroupUniqArray => {
                        let mut seen = std::collections::HashSet::new();
                        let arr: Vec<Value> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter(|v| {
                                let key = format!("{:?}", v);
                                seen.insert(key)
                            })
                            .map(|v| (*v).clone())
                            .collect();
                        Value::array(arr)
                    }
                    FunctionName::Any => values
                        .iter()
                        .find(|v| !v.is_null())
                        .map(|v| (*v).clone())
                        .unwrap_or(Value::null()),
                    FunctionName::AnyLast => values
                        .iter()
                        .rev()
                        .find(|v| !v.is_null())
                        .map(|v| (*v).clone())
                        .unwrap_or(Value::null()),
                    FunctionName::AnyHeavy => {
                        let mut freq_map: std::collections::HashMap<String, (usize, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                freq_map.entry(key).or_insert((0, (*val).clone())).0 += 1;
                            }
                        }
                        freq_map
                            .into_iter()
                            .max_by_key(|(_, (count, _))| *count)
                            .map(|(_, (_, val))| val)
                            .unwrap_or(Value::null())
                    }
                    FunctionName::SumWithOverflow => {
                        let mut sum: i64 = 0;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                sum = sum.wrapping_add(i);
                            } else if let Some(f) = val.as_f64() {
                                sum = sum.wrapping_add(f as i64);
                            }
                        }
                        Value::int64(sum)
                    }
                    FunctionName::GroupArrayMovingAvg => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        let mut moving_avgs = Vec::new();
                        let mut sum = 0.0;
                        for (i, &val) in float_values.iter().enumerate() {
                            sum += val;
                            moving_avgs.push(Value::float64(sum / (i + 1) as f64));
                        }
                        Value::array(moving_avgs)
                    }
                    FunctionName::GroupArrayMovingSum => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        let mut moving_sums = Vec::new();
                        let mut sum = 0.0;
                        for &val in &float_values {
                            sum += val;
                            moving_sums.push(Value::float64(sum));
                        }
                        Value::array(moving_sums)
                    }
                    FunctionName::SumMap | FunctionName::MinMap | FunctionName::MaxMap => {
                        Value::array(vec![Value::array(vec![]), Value::array(vec![])])
                    }
                    FunctionName::GroupBitmap => {
                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                unique_values.insert(i);
                            }
                        }
                        Value::int64(unique_values.len() as i64)
                    }
                    FunctionName::GroupBitmapAnd
                    | FunctionName::GroupBitmapOr
                    | FunctionName::GroupBitmapXor => {
                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                unique_values.insert(i);
                            }
                        }
                        Value::int64(unique_values.len() as i64)
                    }
                    FunctionName::GroupBitmapState => {
                        let mut unique_values = std::collections::BTreeSet::new();
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                unique_values.insert(i);
                            }
                        }
                        let arr: Vec<Value> =
                            unique_values.iter().map(|&n| Value::int64(n)).collect();
                        Value::array(arr)
                    }
                    FunctionName::GroupBitAnd => {
                        let mut result: Option<i64> = None;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result = Some(match result {
                                    Some(r) => r & i,
                                    None => i,
                                });
                            }
                        }
                        result.map(Value::int64).unwrap_or(Value::null())
                    }
                    FunctionName::GroupBitOr => {
                        let mut result: i64 = 0;
                        let mut has_value = false;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result |= i;
                                has_value = true;
                            }
                        }
                        if has_value {
                            Value::int64(result)
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::GroupBitXor => {
                        let mut result: i64 = 0;
                        let mut has_value = false;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result ^= i;
                                has_value = true;
                            }
                        }
                        if has_value {
                            Value::int64(result)
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::RegrSlope => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            let numerator = n * sum_xy - sum_x * sum_y;
                            let denominator = n * sum_xx - sum_x * sum_x;
                            if denominator.abs() < f64::EPSILON {
                                Value::null()
                            } else {
                                Value::float64(numerator / denominator)
                            }
                        }
                    }
                    FunctionName::RegrIntercept => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            let denominator = n * sum_xx - sum_x * sum_x;
                            if denominator.abs() < f64::EPSILON {
                                Value::null()
                            } else {
                                let slope = (n * sum_xy - sum_x * sum_y) / denominator;
                                let intercept = (sum_y - slope * sum_x) / n;
                                Value::float64(intercept)
                            }
                        }
                    }
                    FunctionName::RegrCount => {
                        let count = values
                            .iter()
                            .filter(|v| {
                                v.as_array().is_some_and(|arr| {
                                    arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null()
                                })
                            })
                            .count();
                        Value::int64(count as i64)
                    }
                    FunctionName::RegrR2 => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            let sum_yy: f64 = pairs.iter().map(|(y, _)| y * y).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            let var_x = n * sum_xx - sum_x * sum_x;
                            let var_y = n * sum_yy - sum_y * sum_y;
                            if var_x.abs() < f64::EPSILON || var_y.abs() < f64::EPSILON {
                                Value::null()
                            } else {
                                let r =
                                    (n * sum_xy - sum_x * sum_y) / (var_x.sqrt() * var_y.sqrt());
                                Value::float64(r * r)
                            }
                        }
                    }
                    FunctionName::RegrAvgx => {
                        let xs: Vec<f64> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                        arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if xs.is_empty() {
                            Value::null()
                        } else {
                            let sum: f64 = xs.iter().sum();
                            Value::float64(sum / xs.len() as f64)
                        }
                    }
                    FunctionName::RegrAvgy => {
                        let ys: Vec<f64> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                        arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if ys.is_empty() {
                            Value::null()
                        } else {
                            let sum: f64 = ys.iter().sum();
                            Value::float64(sum / ys.len() as f64)
                        }
                    }
                    FunctionName::RegrSxx => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.is_empty() {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            Value::float64(sum_xx - sum_x * sum_x / n)
                        }
                    }
                    FunctionName::RegrSyy => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.is_empty() {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_yy: f64 = pairs.iter().map(|(y, _)| y * y).sum();
                            Value::float64(sum_yy - sum_y * sum_y / n)
                        }
                    }
                    FunctionName::RegrSxy => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.is_empty() {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            Value::float64(sum_xy - sum_x * sum_y / n)
                        }
                    }
                    FunctionName::RankCorr => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let x = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let y = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        x.zip(y)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            fn rank_values(values: &[f64]) -> Vec<f64> {
                                let mut indexed: Vec<(usize, f64)> =
                                    values.iter().copied().enumerate().collect();
                                indexed.sort_by(|a, b| {
                                    a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                let mut ranks = vec![0.0; values.len()];
                                let mut i = 0;
                                while i < indexed.len() {
                                    let mut j = i;
                                    while j < indexed.len() && indexed[j].1 == indexed[i].1 {
                                        j += 1;
                                    }
                                    let rank = (i + j - 1) as f64 / 2.0 + 1.0;
                                    for k in i..j {
                                        ranks[indexed[k].0] = rank;
                                    }
                                    i = j;
                                }
                                ranks
                            }
                            let x_values: Vec<f64> = pairs.iter().map(|(x, _)| *x).collect();
                            let y_values: Vec<f64> = pairs.iter().map(|(_, y)| *y).collect();
                            let x_ranks = rank_values(&x_values);
                            let y_ranks = rank_values(&y_values);
                            let n = x_ranks.len() as f64;
                            let mean_x: f64 = x_ranks.iter().sum::<f64>() / n;
                            let mean_y: f64 = y_ranks.iter().sum::<f64>() / n;
                            let mut sum_xy = 0.0f64;
                            let mut sum_x2 = 0.0f64;
                            let mut sum_y2 = 0.0f64;
                            for i in 0..x_ranks.len() {
                                let dx = x_ranks[i] - mean_x;
                                let dy = y_ranks[i] - mean_y;
                                sum_xy += dx * dy;
                                sum_x2 += dx * dx;
                                sum_y2 += dy * dy;
                            }
                            if sum_x2 == 0.0 || sum_y2 == 0.0 {
                                Value::null()
                            } else {
                                let corr = sum_xy / (sum_x2 * sum_y2).sqrt();
                                Value::float64(corr)
                            }
                        }
                    }
                    FunctionName::ExponentialMovingAverage => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            let alpha = 0.5;
                            let mut ema = float_values[0];
                            for &val in &float_values[1..] {
                                ema = alpha * val + (1.0 - alpha) * ema;
                            }
                            Value::float64(ema)
                        }
                    }
                    FunctionName::IntervalLengthSum => {
                        let mut total_length = 0i64;
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(start), Some(end)) =
                                        (arr[0].as_i64(), arr[1].as_i64())
                                    {
                                        if end > start {
                                            total_length += end - start;
                                        }
                                    }
                                }
                            }
                        }
                        Value::int64(total_length)
                    }
                    FunctionName::Retention => {
                        let conditions: Vec<bool> =
                            values.iter().filter_map(|v| v.as_bool()).collect();
                        let result: Vec<Value> =
                            conditions.iter().map(|&b| Value::bool_val(b)).collect();
                        Value::array(result)
                    }
                    FunctionName::WindowFunnel => Value::int64(0),
                    FunctionName::BitAnd => {
                        let mut result: Option<i64> = None;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result = Some(match result {
                                    None => i,
                                    Some(prev) => prev & i,
                                });
                            }
                        }
                        result.map(Value::int64).unwrap_or(Value::null())
                    }
                    FunctionName::BitOr => {
                        let mut result: Option<i64> = None;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result = Some(match result {
                                    None => i,
                                    Some(prev) => prev | i,
                                });
                            }
                        }
                        result.map(Value::int64).unwrap_or(Value::null())
                    }
                    FunctionName::BitXor => {
                        let mut result: Option<i64> = None;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result = Some(match result {
                                    None => i,
                                    Some(prev) => prev ^ i,
                                });
                            }
                        }
                        result.map(Value::int64).unwrap_or(Value::null())
                    }
                    FunctionName::BoolAnd | FunctionName::Every => {
                        let mut result: Option<bool> = None;
                        for val in &values {
                            if let Some(b) = val.as_bool() {
                                result = Some(match result {
                                    None => b,
                                    Some(prev) => prev && b,
                                });
                            }
                        }
                        result.map(Value::bool_val).unwrap_or(Value::null())
                    }
                    FunctionName::BoolOr => {
                        let mut result: Option<bool> = None;
                        for val in &values {
                            if let Some(b) = val.as_bool() {
                                result = Some(match result {
                                    None => b,
                                    Some(prev) => prev || b,
                                });
                            }
                        }
                        result.map(Value::bool_val).unwrap_or(Value::null())
                    }
                    FunctionName::JsonAgg | FunctionName::JsonbAgg => {
                        let non_null_values: Vec<serde_json::Value> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .map(|v| value_to_json(v))
                            .collect();
                        if non_null_values.is_empty() {
                            Value::null()
                        } else {
                            Value::json(serde_json::Value::Array(non_null_values))
                        }
                    }
                    FunctionName::JsonObjectAgg | FunctionName::JsonbObjectAgg => {
                        let mut obj = serde_json::Map::new();
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let Some(key) = arr[0].as_str() {
                                        let json_val = value_to_json(&arr[1]);
                                        obj.insert(key.to_string(), json_val);
                                    } else if let Some(key) = arr[0].as_i64() {
                                        let json_val = value_to_json(&arr[1]);
                                        obj.insert(key.to_string(), json_val);
                                    }
                                }
                            }
                        }
                        if obj.is_empty() {
                            Value::null()
                        } else {
                            Value::json(serde_json::Value::Object(obj))
                        }
                    }
                    FunctionName::Median => {
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let len = float_values.len();
                            if len % 2 == 1 {
                                Value::float64(float_values[len / 2])
                            } else {
                                let mid = len / 2;
                                Value::float64((float_values[mid - 1] + float_values[mid]) / 2.0)
                            }
                        }
                    }
                    FunctionName::PercentileCont => {
                        let percentile = args
                            .first()
                            .and_then(|arg| {
                                if let Expr::Literal(lit) = arg {
                                    lit.as_f64()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0.5);
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let n = float_values.len();
                            let index = percentile * (n - 1) as f64;
                            let lower = index.floor() as usize;
                            let upper = index.ceil() as usize;
                            if lower == upper {
                                Value::float64(float_values[lower])
                            } else {
                                let fraction = index - lower as f64;
                                let interpolated = float_values[lower] * (1.0 - fraction)
                                    + float_values[upper] * fraction;
                                Value::float64(interpolated)
                            }
                        }
                    }
                    FunctionName::PercentileDisc => {
                        let percentile = args
                            .first()
                            .and_then(|arg| {
                                if let Expr::Literal(lit) = arg {
                                    lit.as_f64()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0.5);
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let n = float_values.len();
                            let index = (percentile * n as f64).ceil() as usize;
                            let index = index.saturating_sub(1).min(n - 1);
                            Value::float64(float_values[index])
                        }
                    }
                    FunctionName::Mode => {
                        let mut counts: std::collections::HashMap<String, (usize, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            let key = format!("{:?}", val);
                            let entry = counts.entry(key).or_insert((0, (*val).clone()));
                            entry.0 += 1;
                        }
                        counts
                            .values()
                            .max_by_key(|(count, _)| *count)
                            .map(|(_, value)| value.clone())
                            .unwrap_or(Value::null())
                    }
                    FunctionName::CramersV | FunctionName::CramersVBiasCorrected => {
                        let mut contingency: std::collections::HashMap<(String, String), usize> =
                            std::collections::HashMap::new();
                        let mut row_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut col_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;

                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                    let x = format!("{:?}", arr[0]);
                                    let y = format!("{:?}", arr[1]);
                                    *contingency.entry((x.clone(), y.clone())).or_insert(0) += 1;
                                    *row_totals.entry(x).or_insert(0) += 1;
                                    *col_totals.entry(y).or_insert(0) += 1;
                                    total += 1;
                                }
                            }
                        }

                        if total == 0 {
                            Value::null()
                        } else {
                            let n = total as f64;
                            let mut chi_sq = 0.0f64;
                            for ((x, y), observed) in &contingency {
                                let row_total = *row_totals.get(x).unwrap_or(&0) as f64;
                                let col_total = *col_totals.get(y).unwrap_or(&0) as f64;
                                let expected = (row_total * col_total) / n;
                                if expected > 0.0 {
                                    let diff = *observed as f64 - expected;
                                    chi_sq += (diff * diff) / expected;
                                }
                            }

                            let r = row_totals.len() as f64;
                            let c = col_totals.len() as f64;
                            let k = r.min(c);

                            if k <= 1.0 || n == 0.0 {
                                Value::float64(0.0)
                            } else if matches!(name, FunctionName::CramersVBiasCorrected) {
                                let phi_sq = chi_sq / n;
                                let phi_sq_corrected =
                                    (phi_sq - ((r - 1.0) * (c - 1.0)) / (n - 1.0)).max(0.0);
                                let r_corrected = r - ((r - 1.0).powi(2)) / (n - 1.0);
                                let c_corrected = c - ((c - 1.0).powi(2)) / (n - 1.0);
                                let k_corrected = (r_corrected - 1.0).min(c_corrected - 1.0);
                                if k_corrected <= 0.0 {
                                    Value::float64(0.0)
                                } else {
                                    Value::float64((phi_sq_corrected / k_corrected).sqrt())
                                }
                            } else {
                                Value::float64((chi_sq / (n * (k - 1.0))).sqrt())
                            }
                        }
                    }
                    FunctionName::TheilU => {
                        let mut contingency: std::collections::HashMap<(String, String), usize> =
                            std::collections::HashMap::new();
                        let mut y_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;

                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                    let x = format!("{:?}", arr[0]);
                                    let y = format!("{:?}", arr[1]);
                                    *contingency.entry((x, y.clone())).or_insert(0) += 1;
                                    *y_totals.entry(y).or_insert(0) += 1;
                                    total += 1;
                                }
                            }
                        }

                        if total == 0 {
                            Value::null()
                        } else {
                            let n = total as f64;
                            let mut h_y = 0.0f64;
                            for &count in y_totals.values() {
                                if count > 0 {
                                    let p = count as f64 / n;
                                    h_y -= p * p.ln();
                                }
                            }

                            let mut h_y_given_x = 0.0f64;
                            let mut x_totals: std::collections::HashMap<String, usize> =
                                std::collections::HashMap::new();
                            for ((x, _), count) in &contingency {
                                *x_totals.entry(x.clone()).or_insert(0) += count;
                            }
                            for ((x, _), &count) in &contingency {
                                if count > 0 {
                                    let x_total = *x_totals.get(x).unwrap_or(&1) as f64;
                                    let p_xy = count as f64 / n;
                                    let p_y_given_x = count as f64 / x_total;
                                    h_y_given_x -= p_xy * p_y_given_x.ln();
                                }
                            }

                            if h_y.abs() < f64::EPSILON {
                                Value::float64(0.0)
                            } else {
                                Value::float64((h_y - h_y_given_x) / h_y)
                            }
                        }
                    }
                    FunctionName::ContingencyCoefficient => {
                        let mut contingency: std::collections::HashMap<(String, String), usize> =
                            std::collections::HashMap::new();
                        let mut row_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut col_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;

                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                    let x = format!("{:?}", arr[0]);
                                    let y = format!("{:?}", arr[1]);
                                    *contingency.entry((x.clone(), y.clone())).or_insert(0) += 1;
                                    *row_totals.entry(x).or_insert(0) += 1;
                                    *col_totals.entry(y).or_insert(0) += 1;
                                    total += 1;
                                }
                            }
                        }

                        if total == 0 {
                            Value::null()
                        } else {
                            let n = total as f64;
                            let mut chi_sq = 0.0f64;
                            for ((x, y), observed) in &contingency {
                                let row_total = *row_totals.get(x).unwrap_or(&0) as f64;
                                let col_total = *col_totals.get(y).unwrap_or(&0) as f64;
                                let expected = (row_total * col_total) / n;
                                if expected > 0.0 {
                                    let diff = *observed as f64 - expected;
                                    chi_sq += (diff * diff) / expected;
                                }
                            }

                            Value::float64((chi_sq / (chi_sq + n)).sqrt())
                        }
                    }
                    FunctionName::AvgWeighted => {
                        let mut sum_weighted = 0.0f64;
                        let mut sum_weights = 0.0f64;

                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let x = arr[0]
                                        .as_f64()
                                        .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                    let w = arr[1]
                                        .as_f64()
                                        .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                    if let (Some(x), Some(w)) = (x, w) {
                                        sum_weighted += x * w;
                                        sum_weights += w;
                                    }
                                }
                            }
                        }

                        if sum_weights == 0.0 {
                            Value::null()
                        } else {
                            Value::float64(sum_weighted / sum_weights)
                        }
                    }
                    FunctionName::SumDistinct => {
                        let mut seen = std::collections::HashSet::new();
                        let mut sum = 0.0f64;
                        let mut has_value = false;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            let key = format!("{:?}", val);
                            if seen.insert(key) {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    has_value = true;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    has_value = true;
                                } else if let Some(n) = val.as_numeric() {
                                    if let Some(f) = n.to_f64() {
                                        sum += f;
                                        has_value = true;
                                    }
                                }
                            }
                        }
                        if has_value {
                            Value::float64(sum)
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::AvgDistinct => {
                        let mut seen = std::collections::HashSet::new();
                        let mut sum = 0.0f64;
                        let mut count = 0usize;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            let key = format!("{:?}", val);
                            if seen.insert(key) {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    count += 1;
                                } else if let Some(n) = val.as_numeric() {
                                    if let Some(f) = n.to_f64() {
                                        sum += f;
                                        count += 1;
                                    }
                                }
                            }
                        }
                        if count > 0 {
                            Value::float64(sum / count as f64)
                        } else {
                            Value::null()
                        }
                    }
                    _ => Value::null(),
                },
                _ => Value::null(),
            };

            result.push(agg_result);
        }

        Ok(result)
    }
}

fn serialize_key(key: &[Value]) -> Vec<u8> {
    let serialized = serde_json::to_string(key).unwrap_or_default();
    serialized.into_bytes()
}

fn value_to_json(value: &Value) -> serde_json::Value {
    if value.is_null() {
        return serde_json::Value::Null;
    }
    if let Some(b) = value.as_bool() {
        return serde_json::Value::Bool(b);
    }
    if let Some(i) = value.as_i64() {
        return serde_json::Value::Number(serde_json::Number::from(i));
    }
    if let Some(f) = value.as_f64() {
        if let Some(n) = serde_json::Number::from_f64(f) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::Null;
    }
    if let Some(s) = value.as_str() {
        return serde_json::Value::String(s.to_string());
    }
    if let Some(arr) = value.as_array() {
        let json_arr: Vec<serde_json::Value> = arr.iter().map(|v| value_to_json(v)).collect();
        return serde_json::Value::Array(json_arr);
    }
    if let Some(json) = value.as_json() {
        return json.clone();
    }
    if let Some(numeric) = value.as_numeric() {
        if let Some(n) = serde_json::Number::from_f64(numeric.to_string().parse().unwrap_or(0.0)) {
            return serde_json::Value::Number(n);
        }
        return serde_json::Value::String(numeric.to_string());
    }
    if let Some(ipv4) = value.as_ipv4() {
        return serde_json::Value::String(format!("ipv4:{}", ipv4.0));
    }
    if let Some(ipv6) = value.as_ipv6() {
        return serde_json::Value::String(format!("ipv6:{}", ipv6.0));
    }
    if let Some(d32) = value.as_date32() {
        return serde_json::Value::Number(serde_json::Number::from(d32.0));
    }
    serde_json::Value::String(format!("{:?}", value))
}

#[derive(Debug)]
pub struct SortAggregateExec {
    input: Rc<dyn ExecutionPlan>,
    schema: Schema,
    group_by: Vec<Expr>,
    aggregates: Vec<(Expr, Option<String>)>,
    having: Option<Expr>,
}

impl SortAggregateExec {
    pub fn new(
        input: Rc<dyn ExecutionPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<(Expr, Option<String>)>,
        having: Option<Expr>,
    ) -> Result<Self> {
        use yachtsql_core::error::Error;

        for (agg_expr, _) in &aggregates {
            if let Expr::Aggregate { filter, .. } = agg_expr {
                if let Some(filter_expr) = filter {
                    if AggregateExec::contains_aggregate(filter_expr) {
                        return Err(Error::InvalidQuery(
                            "FILTER clause cannot contain aggregate functions".to_string(),
                        ));
                    }
                }
            }
        }

        let mut fields = Vec::new();

        let input_schema = input.schema();
        for (idx, group_expr) in group_by.iter().enumerate() {
            let field_name = if let Expr::Column { name, .. } = group_expr {
                name.clone()
            } else {
                format!("group_{}", idx)
            };

            let data_type = AggregateExec::infer_expr_type(group_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::String);

            fields.push(Field::nullable(field_name, data_type));
        }

        for (idx, (agg_expr, alias)) in aggregates.iter().enumerate() {
            let field_name = alias.clone().unwrap_or_else(|| {
                AggregateExec::expr_to_field_name(agg_expr)
                    .unwrap_or_else(|| format!("agg_{}", idx))
            });

            let data_type = AggregateExec::infer_aggregate_type(agg_expr, input_schema)
                .unwrap_or(yachtsql_core::types::DataType::Float64);
            fields.push(Field::nullable(field_name, data_type));
        }

        let schema = Schema::from_fields(fields);

        Ok(Self {
            input,
            schema,
            group_by,
            aggregates,
            having,
        })
    }

    fn compute_group_key(&self, batch: &Table, row_idx: usize) -> Result<Vec<Value>> {
        let mut key = Vec::with_capacity(self.group_by.len());
        for expr in &self.group_by {
            let value = self.evaluate_expr(expr, batch, row_idx)?;
            key.push(value);
        }
        Ok(key)
    }

    fn evaluate_expr(&self, expr: &Expr, batch: &Table, row_idx: usize) -> Result<Value> {
        use super::ProjectionWithExprExec;
        ProjectionWithExprExec::evaluate_expr(expr, batch, row_idx)
    }

    fn evaluate_aggregate_arg(
        &self,
        agg_expr: &Expr,
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        use yachtsql_ir::FunctionName;
        match agg_expr {
            Expr::Aggregate {
                name,
                args,
                filter,
                order_by,
                ..
            } => {
                if let Some(filter_expr) = filter {
                    let filter_result = self.evaluate_expr(filter_expr, batch, row_idx)?;
                    if filter_result.as_bool() != Some(true) {
                        return Ok(Value::null());
                    }
                }
                match name {
                    FunctionName::PercentileCont
                    | FunctionName::PercentileDisc
                    | FunctionName::Median
                    | FunctionName::Mode => {
                        if let Some(order_exprs) = order_by {
                            if !order_exprs.is_empty() {
                                return self.evaluate_expr(&order_exprs[0].expr, batch, row_idx);
                            }
                        }
                        if !args.is_empty() {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        } else {
                            Ok(Value::null())
                        }
                    }
                    FunctionName::ArrayAgg | FunctionName::StringAgg => {
                        let value = if args.is_empty() {
                            Value::null()
                        } else {
                            self.evaluate_expr(&args[0], batch, row_idx)?
                        };
                        if let Some(order_exprs) = order_by {
                            if !order_exprs.is_empty() {
                                let mut sort_keys = Vec::with_capacity(order_exprs.len() * 2);
                                for order_expr in order_exprs {
                                    let sort_val =
                                        self.evaluate_expr(&order_expr.expr, batch, row_idx)?;
                                    let asc = order_expr.asc.unwrap_or(true);
                                    sort_keys.push(sort_val);
                                    sort_keys.push(Value::bool_val(asc));
                                }
                                return Ok(Value::array(vec![value, Value::array(sort_keys)]));
                            }
                        }
                        Ok(value)
                    }
                    FunctionName::UniqUpTo => {
                        if args.len() >= 2 {
                            self.evaluate_expr(&args[1], batch, row_idx)
                        } else if !args.is_empty() {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        } else {
                            Ok(Value::null())
                        }
                    }
                    FunctionName::UniqIf => {
                        if args.len() >= 2 {
                            let condition = self.evaluate_expr(&args[1], batch, row_idx)?;
                            if condition.as_bool() == Some(true) {
                                self.evaluate_expr(&args[0], batch, row_idx)
                            } else {
                                Ok(Value::null())
                            }
                        } else if !args.is_empty() {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        } else {
                            Ok(Value::null())
                        }
                    }
                    _ => {
                        if args.is_empty() {
                            Ok(Value::int64(1))
                        } else if args.len() >= 2 {
                            let needs_array = matches!(
                                name,
                                FunctionName::Corr
                                    | FunctionName::CovarPop
                                    | FunctionName::CovarSamp
                                    | FunctionName::AvgWeighted
                                    | FunctionName::ArgMin
                                    | FunctionName::ArgMax
                                    | FunctionName::TopK
                                    | FunctionName::WindowFunnel
                                    | FunctionName::RegrSlope
                                    | FunctionName::RegrIntercept
                                    | FunctionName::RegrCount
                                    | FunctionName::RegrR2
                                    | FunctionName::RegrAvgx
                                    | FunctionName::RegrAvgy
                                    | FunctionName::RegrSxx
                                    | FunctionName::RegrSyy
                                    | FunctionName::RegrSxy
                                    | FunctionName::JsonObjectAgg
                                    | FunctionName::JsonbObjectAgg
                                    | FunctionName::ApproxTopSum
                                    | FunctionName::CramersV
                                    | FunctionName::CramersVBiasCorrected
                                    | FunctionName::TheilU
                                    | FunctionName::ContingencyCoefficient
                                    | FunctionName::RankCorr
                                    | FunctionName::ExponentialMovingAverage
                                    | FunctionName::Uniq
                                    | FunctionName::UniqExact
                                    | FunctionName::UniqHll12
                                    | FunctionName::UniqCombined
                                    | FunctionName::UniqCombined64
                                    | FunctionName::UniqThetaSketch
                                    | FunctionName::ApproxCountDistinct
                                    | FunctionName::ApproxDistinct
                                    | FunctionName::Ndv
                            );
                            if needs_array {
                                let mut values = Vec::with_capacity(args.len());
                                for arg in args {
                                    values.push(self.evaluate_expr(arg, batch, row_idx)?);
                                }
                                Ok(Value::array(values))
                            } else {
                                self.evaluate_expr(&args[0], batch, row_idx)
                            }
                        } else {
                            self.evaluate_expr(&args[0], batch, row_idx)
                        }
                    }
                }
            }
            _ => self.evaluate_expr(agg_expr, batch, row_idx),
        }
    }

    fn keys_equal(&self, a: &[Value], b: &[Value]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        for (va, vb) in a.iter().zip(b.iter()) {
            if !values_equal(va, vb) {
                return false;
            }
        }
        true
    }

    fn evaluate_having(&self, _group_values: &[Value], _agg_values: &[Value]) -> Result<bool> {
        match &self.having {
            None => Ok(true),
            Some(_having_expr) => Ok(true),
        }
    }

    fn compute_aggregates_streaming(&self, agg_input_rows: &[Vec<Value>]) -> Result<Vec<Value>> {
        use yachtsql_ir::FunctionName;
        let mut result = Vec::with_capacity(self.aggregates.len());

        for agg_idx in 0..self.aggregates.len() {
            let values: Vec<&Value> = agg_input_rows.iter().map(|row| &row[agg_idx]).collect();

            let agg_result = match &self.aggregates[agg_idx].0 {
                Expr::Aggregate {
                    name,
                    args,
                    distinct,
                    ..
                } => match name {
                    FunctionName::Count => {
                        if *distinct {
                            let mut unique_values = std::collections::HashSet::new();
                            for val in &values {
                                if !val.is_null() {
                                    unique_values.insert(format!("{:?}", *val));
                                }
                            }
                            Value::int64(unique_values.len() as i64)
                        } else {
                            let count = values.iter().filter(|v| !v.is_null()).count();
                            Value::int64(count as i64)
                        }
                    }
                    FunctionName::CountIf => {
                        let count = values.iter().filter(|v| v.as_bool() == Some(true)).count();
                        Value::int64(count as i64)
                    }
                    FunctionName::Sum => {
                        let values_to_sum: Vec<&Value> = if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            values
                                .iter()
                                .filter(|v| {
                                    if v.is_null() {
                                        return false;
                                    }
                                    let key = format!("{:?}", *v);
                                    unique_strs.insert(key)
                                })
                                .copied()
                                .collect()
                        } else {
                            values.to_vec()
                        };

                        let has_numeric = values_to_sum.iter().any(|v| v.as_numeric().is_some());
                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut has_values = false;
                            for val in &values_to_sum {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    has_values = true;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        has_values = true;
                                    }
                                }
                            }
                            if has_values {
                                Value::numeric(sum)
                            } else {
                                Value::null()
                            }
                        } else {
                            let mut sum = 0.0f64;
                            let mut has_values = false;
                            for val in &values_to_sum {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    has_values = true;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    has_values = true;
                                }
                            }
                            if has_values {
                                Value::float64(sum)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    FunctionName::Avg => {
                        let values_to_avg: Vec<&Value> = if *distinct {
                            let mut unique_strs = std::collections::HashSet::new();
                            values
                                .iter()
                                .filter(|v| {
                                    if v.is_null() {
                                        return false;
                                    }
                                    let key = format!("{:?}", *v);
                                    unique_strs.insert(key)
                                })
                                .copied()
                                .collect()
                        } else {
                            values.to_vec()
                        };

                        let has_numeric = values_to_avg.iter().any(|v| v.as_numeric().is_some());
                        if has_numeric {
                            let mut sum = rust_decimal::Decimal::ZERO;
                            let mut count = 0u32;
                            for val in &values_to_avg {
                                if let Some(n) = val.as_numeric() {
                                    sum += n;
                                    count += 1;
                                } else if let Some(i) = val.as_i64() {
                                    sum += rust_decimal::Decimal::from(i);
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    if let Some(d) = rust_decimal::Decimal::from_f64_retain(f) {
                                        sum += d;
                                        count += 1;
                                    }
                                }
                            }
                            if count > 0 {
                                Value::numeric(sum / rust_decimal::Decimal::from(count))
                            } else {
                                Value::null()
                            }
                        } else {
                            let mut sum = 0.0f64;
                            let mut count = 0usize;
                            for val in &values_to_avg {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    count += 1;
                                }
                            }
                            if count > 0 {
                                Value::float64(sum / count as f64)
                            } else {
                                Value::null()
                            }
                        }
                    }
                    FunctionName::Min => {
                        let mut min: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            min = Some(match min {
                                None => (*val).clone(),
                                Some(ref current_min) => {
                                    if compare_values(val, current_min)? == std::cmp::Ordering::Less
                                    {
                                        (*val).clone()
                                    } else {
                                        current_min.clone()
                                    }
                                }
                            });
                        }
                        min.unwrap_or(Value::null())
                    }
                    FunctionName::Max => {
                        let mut max: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            max = Some(match max {
                                None => (*val).clone(),
                                Some(ref current_max) => {
                                    if compare_values(val, current_max)?
                                        == std::cmp::Ordering::Greater
                                    {
                                        (*val).clone()
                                    } else {
                                        current_max.clone()
                                    }
                                }
                            });
                        }
                        max.unwrap_or(Value::null())
                    }
                    FunctionName::ArrayAgg => {
                        let has_sort_keys = values.first().is_some_and(|v| {
                            if let Some(arr) = v.as_array() {
                                arr.len() == 2 && arr[1].as_array().is_some()
                            } else {
                                false
                            }
                        });

                        if has_sort_keys {
                            let mut sorted_entries: Vec<(Value, Vec<Value>)> = values
                                .iter()
                                .filter_map(|v| {
                                    if let Some(arr) = v.as_array() {
                                        if arr.len() == 2 {
                                            let value = arr[0].clone();
                                            let sort_keys = arr[1]
                                                .as_array()
                                                .map(|a| a.to_vec())
                                                .unwrap_or_default();
                                            return Some((value, sort_keys));
                                        }
                                    }
                                    None
                                })
                                .collect();

                            sorted_entries.sort_by(|a, b| {
                                let keys_a = &a.1;
                                let keys_b = &b.1;
                                let mut i = 0;
                                while i < keys_a.len() && i + 1 < keys_a.len() {
                                    let val_a = &keys_a[i];
                                    let val_b = &keys_b[i];
                                    let asc = keys_a[i + 1].as_bool().unwrap_or(true);
                                    let cmp = compare_values(val_a, val_b)
                                        .unwrap_or(std::cmp::Ordering::Equal);
                                    if cmp != std::cmp::Ordering::Equal {
                                        return if asc { cmp } else { cmp.reverse() };
                                    }
                                    i += 2;
                                }
                                std::cmp::Ordering::Equal
                            });

                            let array_values: Vec<Value> =
                                sorted_entries.into_iter().map(|(v, _)| v).collect();

                            if *distinct {
                                let mut unique_strs = std::collections::HashSet::new();
                                let mut unique_values = Vec::new();
                                for val in array_values {
                                    let str_repr = format!("{:?}", val);
                                    if unique_strs.insert(str_repr) {
                                        unique_values.push(val);
                                    }
                                }
                                Value::array(unique_values)
                            } else {
                                Value::array(array_values)
                            }
                        } else {
                            let array_values: Vec<Value> =
                                values.iter().map(|v| (*v).clone()).collect();
                            if *distinct {
                                let mut unique_strs = std::collections::HashSet::new();
                                let mut unique_values = Vec::new();
                                for val in array_values {
                                    let str_repr = format!("{:?}", val);
                                    if unique_strs.insert(str_repr) {
                                        unique_values.push(val);
                                    }
                                }
                                Value::array(unique_values)
                            } else {
                                Value::array(array_values)
                            }
                        }
                    }
                    FunctionName::StringAgg => {
                        let delimiter = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::String(s)) => {
                                    s.clone()
                                }
                                _ => String::new(),
                            }
                        } else {
                            String::new()
                        };

                        let has_sort_keys = values.first().is_some_and(|v| {
                            if let Some(arr) = v.as_array() {
                                arr.len() == 2 && arr[1].as_array().is_some()
                            } else {
                                false
                            }
                        });

                        let string_values: Vec<String> = if has_sort_keys {
                            let mut sorted_entries: Vec<(Value, Vec<Value>)> = values
                                .iter()
                                .filter_map(|v| {
                                    if let Some(arr) = v.as_array() {
                                        if arr.len() == 2 {
                                            let value = arr[0].clone();
                                            let sort_keys = arr[1]
                                                .as_array()
                                                .map(|a| a.to_vec())
                                                .unwrap_or_default();
                                            return Some((value, sort_keys));
                                        }
                                    }
                                    None
                                })
                                .collect();

                            sorted_entries.sort_by(|a, b| {
                                let keys_a = &a.1;
                                let keys_b = &b.1;
                                let mut i = 0;
                                while i < keys_a.len() && i + 1 < keys_a.len() {
                                    let val_a = &keys_a[i];
                                    let val_b = &keys_b[i];
                                    let asc = keys_a[i + 1].as_bool().unwrap_or(true);
                                    let cmp = compare_values(val_a, val_b)
                                        .unwrap_or(std::cmp::Ordering::Equal);
                                    if cmp != std::cmp::Ordering::Equal {
                                        return if asc { cmp } else { cmp.reverse() };
                                    }
                                    i += 2;
                                }
                                std::cmp::Ordering::Equal
                            });

                            sorted_entries
                                .into_iter()
                                .filter_map(|(v, _)| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        v.as_str().map(|s| s.to_string())
                                    }
                                })
                                .collect()
                        } else {
                            values
                                .iter()
                                .filter_map(|v| {
                                    if v.is_null() {
                                        None
                                    } else {
                                        v.as_str().map(|s| s.to_string())
                                    }
                                })
                                .collect()
                        };

                        if string_values.is_empty() {
                            Value::null()
                        } else {
                            Value::string(string_values.join(&delimiter))
                        }
                    }
                    FunctionName::Corr => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut m2_x = 0.0f64;
                        let mut m2_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        let delta_y = y - mean_y;
                                        mean_x += delta_x / n;
                                        mean_y += delta_y / n;
                                        let delta_x2 = x - mean_x;
                                        let delta_y2 = y - mean_y;
                                        m2_x += delta_x * delta_x2;
                                        m2_y += delta_y * delta_y2;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            let var_x = m2_x / count as f64;
                            let var_y = m2_y / count as f64;
                            if var_x == 0.0 || var_y == 0.0 {
                                Value::null()
                            } else {
                                let corr = coproduct / (count as f64 * var_x.sqrt() * var_y.sqrt());
                                Value::float64(corr)
                            }
                        }
                    }
                    FunctionName::CovarPop => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count == 0 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / count as f64)
                        }
                    }
                    FunctionName::CovarSamp => {
                        let mut count = 0usize;
                        let mut mean_x = 0.0f64;
                        let mut mean_y = 0.0f64;
                        let mut coproduct = 0.0f64;

                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(x), Some(y)) = (arr[0].as_f64(), arr[1].as_f64()) {
                                        count += 1;
                                        let n = count as f64;
                                        let delta_x = x - mean_x;
                                        mean_x += delta_x / n;
                                        let delta_y = y - mean_y;
                                        mean_y += delta_y / n;
                                        let delta_y2 = y - mean_y;
                                        coproduct += delta_x * delta_y2;
                                    }
                                }
                            }
                        }

                        if count < 2 {
                            Value::null()
                        } else {
                            Value::float64(coproduct / (count - 1) as f64)
                        }
                    }
                    FunctionName::Uniq
                    | FunctionName::UniqExact
                    | FunctionName::UniqHll12
                    | FunctionName::UniqCombined
                    | FunctionName::UniqCombined64
                    | FunctionName::UniqThetaSketch
                    | FunctionName::UniqIf
                    | FunctionName::ApproxCountDistinct
                    | FunctionName::ApproxDistinct
                    | FunctionName::Ndv => {
                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                unique_values.insert(key);
                            }
                        }
                        Value::int64(unique_values.len() as i64)
                    }
                    FunctionName::UniqUpTo => {
                        let threshold = if !args.is_empty() {
                            match &args[0] {
                                Expr::Literal(lit) => {
                                    lit.as_f64().map(|f| f as usize).unwrap_or(100)
                                }
                                _ => 100,
                            }
                        } else {
                            100
                        };

                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                unique_values.insert(key);
                            }
                        }
                        let count = unique_values.len();
                        if count > threshold {
                            Value::int64((threshold + 1) as i64)
                        } else {
                            Value::int64(count as i64)
                        }
                    }
                    FunctionName::ApproxTopCount => {
                        let number = if args.len() >= 2 {
                            match &args[1] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(n)) => {
                                    *n as usize
                                }
                                _ => 10,
                            }
                        } else {
                            10
                        };

                        let mut freq_map: std::collections::HashMap<String, (usize, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                let entry = freq_map.entry(key).or_insert((0, (*val).clone()));
                                entry.0 += 1;
                            }
                        }
                        let mut freq_vec: Vec<_> = freq_map.into_iter().collect();
                        freq_vec.sort_by(|a, b| b.1.0.cmp(&a.1.0));
                        let top_values: Vec<Value> = freq_vec
                            .iter()
                            .take(number)
                            .map(|(_, (count, val))| {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("value".to_string(), val.clone());
                                map.insert("count".to_string(), Value::int64(*count as i64));
                                Value::struct_val(map)
                            })
                            .collect();
                        Value::array(top_values)
                    }
                    FunctionName::ApproxTopSum => {
                        let number = if args.len() >= 3 {
                            match &args[2] {
                                Expr::Literal(yachtsql_ir::expr::LiteralValue::Int64(n)) => {
                                    *n as usize
                                }
                                _ => 10,
                            }
                        } else {
                            10
                        };

                        let mut sum_map: std::collections::HashMap<String, (f64, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let key_val = &arr[0];
                                    let weight = arr[1]
                                        .as_f64()
                                        .or_else(|| arr[1].as_i64().map(|i| i as f64))
                                        .unwrap_or(0.0);
                                    if !key_val.is_null() {
                                        let key = format!("{:?}", key_val);
                                        let entry =
                                            sum_map.entry(key).or_insert((0.0, key_val.clone()));
                                        entry.0 += weight;
                                    }
                                }
                            }
                        }
                        let mut sum_vec: Vec<_> = sum_map.into_iter().collect();
                        sum_vec.sort_by(|a, b| {
                            b.1.0
                                .partial_cmp(&a.1.0)
                                .unwrap_or(std::cmp::Ordering::Equal)
                        });
                        let top_values: Vec<Value> = sum_vec
                            .iter()
                            .take(number)
                            .map(|(_, (sum, val))| {
                                let mut map = indexmap::IndexMap::new();
                                map.insert("value".to_string(), val.clone());
                                map.insert("sum".to_string(), Value::int64(*sum as i64));
                                Value::struct_val(map)
                            })
                            .collect();
                        Value::array(top_values)
                    }
                    FunctionName::TopK => {
                        let mut freq_map: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                *freq_map.entry(key).or_insert(0) += 1;
                            }
                        }
                        let mut freq_vec: Vec<_> = freq_map.into_iter().collect();
                        freq_vec.sort_by(|a, b| b.1.cmp(&a.1));
                        let top_values: Vec<Value> = freq_vec
                            .iter()
                            .take(10)
                            .map(|(k, _)| Value::string(k.clone()))
                            .collect();
                        Value::array(top_values)
                    }
                    FunctionName::Quantile
                    | FunctionName::QuantileExact
                    | FunctionName::QuantileTiming
                    | FunctionName::QuantileTDigest => {
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let mid = float_values.len() / 2;
                            let median = if float_values.len() % 2 == 0 {
                                (float_values[mid - 1] + float_values[mid]) / 2.0
                            } else {
                                float_values[mid]
                            };
                            Value::float64(median)
                        }
                    }
                    FunctionName::QuantilesTiming | FunctionName::QuantilesTDigest => {
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::array(vec![])
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let quantiles = [0.5, 0.9, 0.99];
                            let result: Vec<Value> = quantiles
                                .iter()
                                .map(|&q| {
                                    let idx =
                                        ((float_values.len() as f64 - 1.0) * q).round() as usize;
                                    Value::float64(float_values[idx.min(float_values.len() - 1)])
                                })
                                .collect();
                            Value::array(result)
                        }
                    }
                    FunctionName::ArgMin => {
                        let mut min_comparison: Option<Value> = None;
                        let mut arg_at_min: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let arg_to_return = &arr[0];
                                    let val_to_compare = &arr[1];
                                    if val_to_compare.is_null() {
                                        continue;
                                    }
                                    let is_smaller = match &min_comparison {
                                        None => true,
                                        Some(current_min) => {
                                            compare_values(val_to_compare, current_min)
                                                .map(|o| o == std::cmp::Ordering::Less)
                                                .unwrap_or(false)
                                        }
                                    };
                                    if is_smaller {
                                        min_comparison = Some(val_to_compare.clone());
                                        arg_at_min = Some(arg_to_return.clone());
                                    }
                                }
                            }
                        }
                        arg_at_min.unwrap_or(Value::null())
                    }
                    FunctionName::ArgMax => {
                        let mut max_comparison: Option<Value> = None;
                        let mut arg_at_max: Option<Value> = None;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    let arg_to_return = &arr[0];
                                    let val_to_compare = &arr[1];
                                    if val_to_compare.is_null() {
                                        continue;
                                    }
                                    let is_larger = match &max_comparison {
                                        None => true,
                                        Some(current_max) => {
                                            compare_values(val_to_compare, current_max)
                                                .map(|o| o == std::cmp::Ordering::Greater)
                                                .unwrap_or(false)
                                        }
                                    };
                                    if is_larger {
                                        max_comparison = Some(val_to_compare.clone());
                                        arg_at_max = Some(arg_to_return.clone());
                                    }
                                }
                            }
                        }
                        arg_at_max.unwrap_or(Value::null())
                    }
                    FunctionName::GroupArray => {
                        let arr: Vec<Value> = values.iter().map(|v| (*v).clone()).collect();
                        Value::array(arr)
                    }
                    FunctionName::GroupUniqArray => {
                        let mut seen = std::collections::HashSet::new();
                        let arr: Vec<Value> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter(|v| {
                                let key = format!("{:?}", v);
                                seen.insert(key)
                            })
                            .map(|v| (*v).clone())
                            .collect();
                        Value::array(arr)
                    }
                    FunctionName::Any => values
                        .iter()
                        .find(|v| !v.is_null())
                        .map(|v| (*v).clone())
                        .unwrap_or(Value::null()),
                    FunctionName::AnyLast => values
                        .iter()
                        .rev()
                        .find(|v| !v.is_null())
                        .map(|v| (*v).clone())
                        .unwrap_or(Value::null()),
                    FunctionName::AnyHeavy => {
                        let mut freq_map: std::collections::HashMap<String, (usize, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                freq_map.entry(key).or_insert((0, (*val).clone())).0 += 1;
                            }
                        }
                        freq_map
                            .into_iter()
                            .max_by_key(|(_, (count, _))| *count)
                            .map(|(_, (_, val))| val)
                            .unwrap_or(Value::null())
                    }
                    FunctionName::SumWithOverflow => {
                        let mut sum: i64 = 0;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                sum = sum.wrapping_add(i);
                            } else if let Some(f) = val.as_f64() {
                                sum = sum.wrapping_add(f as i64);
                            }
                        }
                        Value::int64(sum)
                    }
                    FunctionName::GroupArrayMovingAvg => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        let mut moving_avgs = Vec::new();
                        let mut sum = 0.0;
                        for (i, &val) in float_values.iter().enumerate() {
                            sum += val;
                            moving_avgs.push(Value::float64(sum / (i + 1) as f64));
                        }
                        Value::array(moving_avgs)
                    }
                    FunctionName::GroupArrayMovingSum => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        let mut moving_sums = Vec::new();
                        let mut sum = 0.0;
                        for &val in &float_values {
                            sum += val;
                            moving_sums.push(Value::float64(sum));
                        }
                        Value::array(moving_sums)
                    }
                    FunctionName::SumMap | FunctionName::MinMap | FunctionName::MaxMap => {
                        Value::array(vec![Value::array(vec![]), Value::array(vec![])])
                    }
                    FunctionName::GroupBitmap => {
                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                unique_values.insert(i);
                            }
                        }
                        Value::int64(unique_values.len() as i64)
                    }
                    FunctionName::GroupBitmapAnd
                    | FunctionName::GroupBitmapOr
                    | FunctionName::GroupBitmapXor => {
                        let mut unique_values = std::collections::HashSet::new();
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                unique_values.insert(i);
                            }
                        }
                        Value::int64(unique_values.len() as i64)
                    }
                    FunctionName::GroupBitmapState => {
                        let mut unique_values = std::collections::BTreeSet::new();
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                unique_values.insert(i);
                            }
                        }
                        let arr: Vec<Value> =
                            unique_values.iter().map(|&n| Value::int64(n)).collect();
                        Value::array(arr)
                    }
                    FunctionName::GroupBitAnd => {
                        let mut result: Option<i64> = None;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result = Some(match result {
                                    Some(r) => r & i,
                                    None => i,
                                });
                            }
                        }
                        result.map(Value::int64).unwrap_or(Value::null())
                    }
                    FunctionName::GroupBitOr => {
                        let mut result: i64 = 0;
                        let mut has_value = false;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result |= i;
                                has_value = true;
                            }
                        }
                        if has_value {
                            Value::int64(result)
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::GroupBitXor => {
                        let mut result: i64 = 0;
                        let mut has_value = false;
                        for val in &values {
                            if let Some(i) = val.as_i64() {
                                result ^= i;
                                has_value = true;
                            }
                        }
                        if has_value {
                            Value::int64(result)
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::RegrSlope => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            let numerator = n * sum_xy - sum_x * sum_y;
                            let denominator = n * sum_xx - sum_x * sum_x;
                            if denominator.abs() < f64::EPSILON {
                                Value::null()
                            } else {
                                Value::float64(numerator / denominator)
                            }
                        }
                    }
                    FunctionName::RegrIntercept => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            let denominator = n * sum_xx - sum_x * sum_x;
                            if denominator.abs() < f64::EPSILON {
                                Value::null()
                            } else {
                                let slope = (n * sum_xy - sum_x * sum_y) / denominator;
                                let intercept = (sum_y - slope * sum_x) / n;
                                Value::float64(intercept)
                            }
                        }
                    }
                    FunctionName::RegrCount => {
                        let count = values
                            .iter()
                            .filter(|v| {
                                v.as_array().is_some_and(|arr| {
                                    arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null()
                                })
                            })
                            .count();
                        Value::int64(count as i64)
                    }
                    FunctionName::RegrR2 => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            let sum_yy: f64 = pairs.iter().map(|(y, _)| y * y).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            let var_x = n * sum_xx - sum_x * sum_x;
                            let var_y = n * sum_yy - sum_y * sum_y;
                            if var_x.abs() < f64::EPSILON || var_y.abs() < f64::EPSILON {
                                Value::null()
                            } else {
                                let r =
                                    (n * sum_xy - sum_x * sum_y) / (var_x.sqrt() * var_y.sqrt());
                                Value::float64(r * r)
                            }
                        }
                    }
                    FunctionName::RegrAvgx => {
                        let xs: Vec<f64> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                        arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if xs.is_empty() {
                            Value::null()
                        } else {
                            let sum: f64 = xs.iter().sum();
                            Value::float64(sum / xs.len() as f64)
                        }
                    }
                    FunctionName::RegrAvgy => {
                        let ys: Vec<f64> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                        arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64))
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if ys.is_empty() {
                            Value::null()
                        } else {
                            let sum: f64 = ys.iter().sum();
                            Value::float64(sum / ys.len() as f64)
                        }
                    }
                    FunctionName::RegrSxx => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.is_empty() {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_xx: f64 = pairs.iter().map(|(_, x)| x * x).sum();
                            Value::float64(sum_xx - sum_x * sum_x / n)
                        }
                    }
                    FunctionName::RegrSyy => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.is_empty() {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_yy: f64 = pairs.iter().map(|(y, _)| y * y).sum();
                            Value::float64(sum_yy - sum_y * sum_y / n)
                        }
                    }
                    FunctionName::RegrSxy => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let y = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let x = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        y.zip(x)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.is_empty() {
                            Value::null()
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(_, x)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(y, _)| y).sum();
                            let sum_xy: f64 = pairs.iter().map(|(y, x)| x * y).sum();
                            Value::float64(sum_xy - sum_x * sum_y / n)
                        }
                    }
                    FunctionName::RankCorr => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let x = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let y = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        x.zip(y)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::null()
                        } else {
                            fn rank_values(values: &[f64]) -> Vec<f64> {
                                let mut indexed: Vec<(usize, f64)> =
                                    values.iter().copied().enumerate().collect();
                                indexed.sort_by(|a, b| {
                                    a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
                                });
                                let mut ranks = vec![0.0; values.len()];
                                let mut i = 0;
                                while i < indexed.len() {
                                    let mut j = i;
                                    while j < indexed.len() && indexed[j].1 == indexed[i].1 {
                                        j += 1;
                                    }
                                    let rank = (i + j - 1) as f64 / 2.0 + 1.0;
                                    for k in i..j {
                                        ranks[indexed[k].0] = rank;
                                    }
                                    i = j;
                                }
                                ranks
                            }
                            let x_values: Vec<f64> = pairs.iter().map(|(x, _)| *x).collect();
                            let y_values: Vec<f64> = pairs.iter().map(|(_, y)| *y).collect();
                            let x_ranks = rank_values(&x_values);
                            let y_ranks = rank_values(&y_values);
                            let n = x_ranks.len() as f64;
                            let mean_x: f64 = x_ranks.iter().sum::<f64>() / n;
                            let mean_y: f64 = y_ranks.iter().sum::<f64>() / n;
                            let mut sum_xy = 0.0f64;
                            let mut sum_x2 = 0.0f64;
                            let mut sum_y2 = 0.0f64;
                            for i in 0..x_ranks.len() {
                                let dx = x_ranks[i] - mean_x;
                                let dy = y_ranks[i] - mean_y;
                                sum_xy += dx * dy;
                                sum_x2 += dx * dx;
                                sum_y2 += dy * dy;
                            }
                            if sum_x2 == 0.0 || sum_y2 == 0.0 {
                                Value::null()
                            } else {
                                let corr = sum_xy / (sum_x2 * sum_y2).sqrt();
                                Value::float64(corr)
                            }
                        }
                    }
                    FunctionName::ExponentialMovingAverage => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            let alpha = 0.5;
                            let mut ema = float_values[0];
                            for &val in &float_values[1..] {
                                ema = alpha * val + (1.0 - alpha) * ema;
                            }
                            Value::float64(ema)
                        }
                    }
                    FunctionName::IntervalLengthSum => {
                        let mut total_length = 0i64;
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 {
                                    if let (Some(start), Some(end)) =
                                        (arr[0].as_i64(), arr[1].as_i64())
                                    {
                                        if end > start {
                                            total_length += end - start;
                                        }
                                    }
                                }
                            }
                        }
                        Value::int64(total_length)
                    }
                    FunctionName::Retention => {
                        let conditions: Vec<bool> =
                            values.iter().filter_map(|v| v.as_bool()).collect();
                        let result: Vec<Value> =
                            conditions.iter().map(|&b| Value::bool_val(b)).collect();
                        Value::array(result)
                    }
                    FunctionName::SimpleLinearRegression => {
                        let pairs: Vec<(f64, f64)> = values
                            .iter()
                            .filter_map(|v| {
                                v.as_array().and_then(|arr| {
                                    if arr.len() >= 2 {
                                        let x = arr[0]
                                            .as_f64()
                                            .or_else(|| arr[0].as_i64().map(|i| i as f64));
                                        let y = arr[1]
                                            .as_f64()
                                            .or_else(|| arr[1].as_i64().map(|i| i as f64));
                                        x.zip(y)
                                    } else {
                                        None
                                    }
                                })
                            })
                            .collect();
                        if pairs.len() < 2 {
                            Value::array(vec![Value::null(), Value::null()])
                        } else {
                            let n = pairs.len() as f64;
                            let sum_x: f64 = pairs.iter().map(|(x, _)| x).sum();
                            let sum_y: f64 = pairs.iter().map(|(_, y)| y).sum();
                            let sum_xy: f64 = pairs.iter().map(|(x, y)| x * y).sum();
                            let sum_x2: f64 = pairs.iter().map(|(x, _)| x * x).sum();
                            let slope = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x * sum_x);
                            let intercept = (sum_y - slope * sum_x) / n;
                            Value::array(vec![Value::float64(slope), Value::float64(intercept)])
                        }
                    }
                    FunctionName::StochasticLinearRegression => {
                        Value::array(vec![Value::float64(0.0), Value::float64(0.0)])
                    }
                    FunctionName::StochasticLogisticRegression => {
                        Value::array(vec![Value::float64(0.0)])
                    }
                    FunctionName::CategoricalInformationValue => Value::float64(0.0),
                    FunctionName::Entropy => {
                        let mut freq_map: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;
                        for val in &values {
                            if !val.is_null() {
                                let key = format!("{:?}", val);
                                *freq_map.entry(key).or_insert(0) += 1;
                                total += 1;
                            }
                        }
                        if total == 0 {
                            Value::float64(0.0)
                        } else {
                            let entropy: f64 = freq_map
                                .values()
                                .map(|&count| {
                                    let p = count as f64 / total as f64;
                                    if p > 0.0 { -p * p.ln() } else { 0.0 }
                                })
                                .sum();
                            Value::float64(entropy)
                        }
                    }
                    FunctionName::MeanZscore => {
                        let float_values: Vec<f64> = values
                            .iter()
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.len() < 2 {
                            Value::array(vec![])
                        } else {
                            let mean: f64 =
                                float_values.iter().sum::<f64>() / float_values.len() as f64;
                            let variance: f64 =
                                float_values.iter().map(|x| (x - mean).powi(2)).sum::<f64>()
                                    / float_values.len() as f64;
                            let std_dev = variance.sqrt();
                            if std_dev == 0.0 {
                                Value::array(
                                    float_values.iter().map(|_| Value::float64(0.0)).collect(),
                                )
                            } else {
                                Value::array(
                                    float_values
                                        .iter()
                                        .map(|x| Value::float64((x - mean) / std_dev))
                                        .collect(),
                                )
                            }
                        }
                    }
                    FunctionName::UniqUpdown => {
                        let mut count = 0i64;
                        let mut prev: Option<f64> = None;
                        for val in &values {
                            if let Some(cur) =
                                val.as_f64().or_else(|| val.as_i64().map(|i| i as f64))
                            {
                                if let Some(p) = prev {
                                    if cur > p {
                                        count += 1;
                                    } else if cur < p {
                                        count -= 1;
                                    }
                                }
                                prev = Some(cur);
                            }
                        }
                        Value::int64(count)
                    }
                    FunctionName::CramersV | FunctionName::CramersVBiasCorrected => {
                        let mut counts: std::collections::HashMap<(String, String), usize> =
                            std::collections::HashMap::new();
                        let mut row_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut col_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                    let x = format!("{:?}", arr[0]);
                                    let y = format!("{:?}", arr[1]);
                                    *counts.entry((x.clone(), y.clone())).or_insert(0) += 1;
                                    *row_totals.entry(x).or_insert(0) += 1;
                                    *col_totals.entry(y).or_insert(0) += 1;
                                    total += 1;
                                }
                            }
                        }
                        if total == 0 {
                            Value::null()
                        } else {
                            let mut chi_square = 0.0f64;
                            for ((row, col), &observed) in &counts {
                                let row_total = *row_totals.get(row).unwrap_or(&0) as f64;
                                let col_total = *col_totals.get(col).unwrap_or(&0) as f64;
                                let expected = (row_total * col_total) / total as f64;
                                if expected > 0.0 {
                                    let diff = observed as f64 - expected;
                                    chi_square += (diff * diff) / expected;
                                }
                            }
                            let rows = row_totals.len();
                            let cols = col_totals.len();
                            let min_dim = rows.min(cols) as f64;
                            if min_dim <= 1.0 {
                                Value::null()
                            } else {
                                let cramers_v =
                                    (chi_square / (total as f64 * (min_dim - 1.0))).sqrt();
                                Value::float64(cramers_v)
                            }
                        }
                    }
                    FunctionName::TheilU => {
                        let mut counts: std::collections::HashMap<(String, String), usize> =
                            std::collections::HashMap::new();
                        let mut x_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut y_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                    let x = format!("{:?}", arr[0]);
                                    let y = format!("{:?}", arr[1]);
                                    *counts.entry((x.clone(), y.clone())).or_insert(0) += 1;
                                    *x_totals.entry(x).or_insert(0) += 1;
                                    *y_totals.entry(y).or_insert(0) += 1;
                                    total += 1;
                                }
                            }
                        }
                        if total == 0 {
                            Value::null()
                        } else {
                            let n = total as f64;
                            let h_y: f64 = y_totals
                                .values()
                                .map(|&c| {
                                    let p = c as f64 / n;
                                    if p > 0.0 { -p * p.ln() } else { 0.0 }
                                })
                                .sum();
                            let mut h_y_given_x = 0.0f64;
                            for (x_val, &x_count) in &x_totals {
                                let p_x = x_count as f64 / n;
                                let mut h_y_x = 0.0f64;
                                for (y_val, &y_count) in &y_totals {
                                    let joint_count =
                                        *counts.get(&(x_val.clone(), y_val.clone())).unwrap_or(&0);
                                    if joint_count > 0 {
                                        let p_y_given_x = joint_count as f64 / x_count as f64;
                                        h_y_x -= p_y_given_x * p_y_given_x.ln();
                                    }
                                }
                                h_y_given_x += p_x * h_y_x;
                            }
                            if h_y.abs() < f64::EPSILON {
                                Value::float64(0.0)
                            } else {
                                let theil_u = (h_y - h_y_given_x) / h_y;
                                Value::float64(theil_u)
                            }
                        }
                    }
                    FunctionName::ContingencyCoefficient => {
                        let mut counts: std::collections::HashMap<(String, String), usize> =
                            std::collections::HashMap::new();
                        let mut row_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut col_totals: std::collections::HashMap<String, usize> =
                            std::collections::HashMap::new();
                        let mut total = 0usize;
                        for val in &values {
                            if let Some(arr) = val.as_array() {
                                if arr.len() >= 2 && !arr[0].is_null() && !arr[1].is_null() {
                                    let x = format!("{:?}", arr[0]);
                                    let y = format!("{:?}", arr[1]);
                                    *counts.entry((x.clone(), y.clone())).or_insert(0) += 1;
                                    *row_totals.entry(x).or_insert(0) += 1;
                                    *col_totals.entry(y).or_insert(0) += 1;
                                    total += 1;
                                }
                            }
                        }
                        if total == 0 {
                            Value::null()
                        } else {
                            let mut chi_square = 0.0f64;
                            for ((row, col), &observed) in &counts {
                                let row_total = *row_totals.get(row).unwrap_or(&0) as f64;
                                let col_total = *col_totals.get(col).unwrap_or(&0) as f64;
                                let expected = (row_total * col_total) / total as f64;
                                if expected > 0.0 {
                                    let diff = observed as f64 - expected;
                                    chi_square += (diff * diff) / expected;
                                }
                            }
                            let n = total as f64;
                            let contingency = (chi_square / (chi_square + n)).sqrt();
                            Value::float64(contingency)
                        }
                    }
                    FunctionName::MannWhitneyUTest
                    | FunctionName::StudentTTest
                    | FunctionName::WelchTTest
                    | FunctionName::KolmogorovSmirnovTest => {
                        Value::array(vec![Value::float64(0.0), Value::float64(1.0)])
                    }
                    FunctionName::Median => {
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let len = float_values.len();
                            if len % 2 == 1 {
                                Value::float64(float_values[len / 2])
                            } else {
                                let mid = len / 2;
                                Value::float64((float_values[mid - 1] + float_values[mid]) / 2.0)
                            }
                        }
                    }
                    FunctionName::PercentileCont => {
                        let percentile = args
                            .first()
                            .and_then(|arg| {
                                if let Expr::Literal(lit) = arg {
                                    lit.as_f64()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0.5);
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let n = float_values.len();
                            let index = percentile * (n - 1) as f64;
                            let lower = index.floor() as usize;
                            let upper = index.ceil() as usize;
                            if lower == upper {
                                Value::float64(float_values[lower])
                            } else {
                                let fraction = index - lower as f64;
                                let interpolated = float_values[lower] * (1.0 - fraction)
                                    + float_values[upper] * fraction;
                                Value::float64(interpolated)
                            }
                        }
                    }
                    FunctionName::PercentileDisc => {
                        let percentile = args
                            .first()
                            .and_then(|arg| {
                                if let Expr::Literal(lit) = arg {
                                    lit.as_f64()
                                } else {
                                    None
                                }
                            })
                            .unwrap_or(0.5);
                        let mut float_values: Vec<f64> = values
                            .iter()
                            .filter(|v| !v.is_null())
                            .filter_map(|v| v.as_f64().or_else(|| v.as_i64().map(|i| i as f64)))
                            .collect();
                        if float_values.is_empty() {
                            Value::null()
                        } else {
                            float_values.sort_by(|a, b| {
                                a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal)
                            });
                            let n = float_values.len();
                            let index = (percentile * n as f64).ceil() as usize;
                            let index = index.saturating_sub(1).min(n - 1);
                            Value::float64(float_values[index])
                        }
                    }
                    FunctionName::Mode => {
                        let mut counts: std::collections::HashMap<String, (usize, Value)> =
                            std::collections::HashMap::new();
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            let key = format!("{:?}", val);
                            let entry = counts.entry(key).or_insert((0, (*val).clone()));
                            entry.0 += 1;
                        }
                        counts
                            .values()
                            .max_by_key(|(count, _)| *count)
                            .map(|(_, value)| value.clone())
                            .unwrap_or(Value::null())
                    }
                    FunctionName::SumDistinct => {
                        let mut seen = std::collections::HashSet::new();
                        let mut sum = 0.0f64;
                        let mut has_value = false;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            let key = format!("{:?}", val);
                            if seen.insert(key) {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    has_value = true;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    has_value = true;
                                } else if let Some(n) = val.as_numeric() {
                                    if let Some(f) = n.to_f64() {
                                        sum += f;
                                        has_value = true;
                                    }
                                }
                            }
                        }
                        if has_value {
                            Value::float64(sum)
                        } else {
                            Value::null()
                        }
                    }
                    FunctionName::AvgDistinct => {
                        let mut seen = std::collections::HashSet::new();
                        let mut sum = 0.0f64;
                        let mut count = 0usize;
                        for val in &values {
                            if val.is_null() {
                                continue;
                            }
                            let key = format!("{:?}", val);
                            if seen.insert(key) {
                                if let Some(i) = val.as_i64() {
                                    sum += i as f64;
                                    count += 1;
                                } else if let Some(f) = val.as_f64() {
                                    sum += f;
                                    count += 1;
                                } else if let Some(n) = val.as_numeric() {
                                    if let Some(f) = n.to_f64() {
                                        sum += f;
                                        count += 1;
                                    }
                                }
                            }
                        }
                        if count > 0 {
                            Value::float64(sum / count as f64)
                        } else {
                            Value::null()
                        }
                    }
                    _ => Value::null(),
                },
                _ => Value::null(),
            };

            result.push(agg_result);
        }

        Ok(result)
    }
}

impl ExecutionPlan for SortAggregateExec {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn execute(&self) -> Result<Vec<Table>> {
        let input_batches = self.input.execute()?;

        if input_batches.is_empty() {
            if self.group_by.is_empty() {
                let empty_agg_values: Vec<Value> = self
                    .aggregates
                    .iter()
                    .map(|(agg_expr, _)| match agg_expr {
                        Expr::Aggregate { name, .. } => {
                            use yachtsql_ir::FunctionName;
                            match name {
                                FunctionName::Count => Value::int64(0),
                                _ => Value::null(),
                            }
                        }
                        _ => Value::null(),
                    })
                    .collect();

                let mut columns = Vec::new();

                for (idx, field) in self.schema.fields().iter().enumerate() {
                    let mut column = Column::new(&field.data_type, 1);
                    column.push(empty_agg_values.get(idx).cloned().unwrap_or(Value::null()))?;
                    columns.push(column);
                }

                return Ok(vec![Table::new(self.schema.clone(), columns)?]);
            }
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let mut result_rows: Vec<Vec<Value>> = Vec::new();
        let mut current_group_key: Option<Vec<Value>> = None;
        let mut current_group_agg_inputs: Vec<Vec<Value>> = Vec::new();

        for input_batch in &input_batches {
            let num_rows = input_batch.num_rows();

            for row_idx in 0..num_rows {
                let group_key = self.compute_group_key(input_batch, row_idx)?;

                let same_group = current_group_key
                    .as_ref()
                    .map(|k| self.keys_equal(k, &group_key))
                    .unwrap_or(false);

                if !same_group {
                    if let Some(ref prev_key) = current_group_key {
                        let agg_values =
                            self.compute_aggregates_streaming(&current_group_agg_inputs)?;
                        if self.evaluate_having(prev_key, &agg_values)? {
                            let mut row = prev_key.clone();
                            row.extend(agg_values);
                            result_rows.push(row);
                        }
                    }
                    current_group_key = Some(group_key.clone());
                    current_group_agg_inputs.clear();
                }

                let mut agg_input_values = Vec::new();
                for (agg_expr, _) in &self.aggregates {
                    let value = self.evaluate_aggregate_arg(agg_expr, input_batch, row_idx)?;
                    agg_input_values.push(value);
                }
                current_group_agg_inputs.push(agg_input_values);
            }
        }

        if let Some(ref prev_key) = current_group_key {
            let agg_values = self.compute_aggregates_streaming(&current_group_agg_inputs)?;
            if self.evaluate_having(prev_key, &agg_values)? {
                let mut row = prev_key.clone();
                row.extend(agg_values);
                result_rows.push(row);
            }
        }

        if result_rows.is_empty() && self.group_by.is_empty() {
            let empty_agg_values: Vec<Value> = self
                .aggregates
                .iter()
                .map(|(agg_expr, _)| match agg_expr {
                    Expr::Aggregate { name, .. } => {
                        use yachtsql_ir::FunctionName;
                        match name {
                            FunctionName::Count => Value::int64(0),
                            _ => Value::null(),
                        }
                    }
                    _ => Value::null(),
                })
                .collect();

            let mut columns = Vec::new();
            for (idx, field) in self.schema.fields().iter().enumerate() {
                let mut column = Column::new(&field.data_type, 1);
                column.push(empty_agg_values.get(idx).cloned().unwrap_or(Value::null()))?;
                columns.push(column);
            }

            return Ok(vec![Table::new(self.schema.clone(), columns)?]);
        }

        if result_rows.is_empty() {
            return Ok(vec![Table::empty(self.schema.clone())]);
        }

        let num_output_rows = result_rows.len();
        let num_cols = self.schema.fields().len();
        let mut columns = Vec::new();

        for col_idx in 0..num_cols {
            let field = &self.schema.fields()[col_idx];
            let mut column = Column::new(&field.data_type, num_output_rows);

            for row in &result_rows {
                column.push(row[col_idx].clone())?;
            }

            columns.push(column);
        }

        Ok(vec![Table::new(self.schema.clone(), columns)?])
    }

    fn children(&self) -> Vec<Rc<dyn ExecutionPlan>> {
        vec![self.input.clone()]
    }

    fn describe(&self) -> String {
        format!(
            "SortAggregate [group_by: {}, aggregates: {}]",
            self.group_by.len(),
            self.aggregates.len()
        )
    }
}

fn values_equal(a: &Value, b: &Value) -> bool {
    if a.is_null() && b.is_null() {
        return true;
    }
    if a.is_null() || b.is_null() {
        return false;
    }
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return (x - y).abs() < f64::EPSILON;
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return x == y;
    }
    if let (Some(x), Some(y)) = (a.as_bool(), b.as_bool()) {
        return x == y;
    }
    false
}

fn compare_values(a: &Value, b: &Value) -> Result<std::cmp::Ordering> {
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_i64()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_f64()) {
        return Ok(x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Some(x), Some(y)) = (a.as_i64(), b.as_f64()) {
        return Ok((x as f64)
            .partial_cmp(&y)
            .unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Some(x), Some(y)) = (a.as_f64(), b.as_i64()) {
        return Ok(x
            .partial_cmp(&(y as f64))
            .unwrap_or(std::cmp::Ordering::Equal));
    }
    if let (Some(x), Some(y)) = (a.as_str(), b.as_str()) {
        return Ok(x.cmp(y));
    }
    if let (Some(fs_a), Some(fs_b)) = (a.as_fixed_string(), b.as_fixed_string()) {
        return Ok(fs_a.data.cmp(&fs_b.data));
    }
    if let (Some(fs), Some(s)) = (a.as_fixed_string(), b.as_str()) {
        return Ok(fs.to_string_lossy().cmp(&s.to_string()));
    }
    if let (Some(s), Some(fs)) = (a.as_str(), b.as_fixed_string()) {
        return Ok(s.to_string().cmp(&fs.to_string_lossy()));
    }
    if let (Some(x), Some(y)) = (a.as_ipv4(), b.as_ipv4()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_ipv6(), b.as_ipv6()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_date32(), b.as_date32()) {
        return Ok(x.0.cmp(&y.0));
    }
    if let (Some(x), Some(y)) = (a.as_date(), b.as_date()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_datetime(), b.as_datetime()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_timestamp(), b.as_timestamp()) {
        return Ok(x.cmp(&y));
    }
    if let (Some(x), Some(y)) = (a.as_time(), b.as_time()) {
        return Ok(x.cmp(&y));
    }
    Ok(std::cmp::Ordering::Equal)
}

#[cfg(test)]
mod tests {
    use yachtsql_ir::FunctionName;

    use super::*;
    use crate::query_executor::evaluator::physical_plan::TableScanExec;

    #[test]
    fn test_serialize_key() {
        let key = vec![Value::int64(1), Value::string("test".to_string())];
        let bytes = serialize_key(&key);
        assert!(!bytes.is_empty());
    }

    #[test]
    fn test_serialize_key_consistency() {
        let key1 = vec![Value::int64(42), Value::string("hello".to_string())];
        let key2 = vec![Value::int64(42), Value::string("hello".to_string())];
        assert_eq!(serialize_key(&key1), serialize_key(&key2));
    }

    #[test]
    fn test_aggregate_count() {
        let schema = yachtsql_storage::Schema::from_fields(vec![
            yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
            yachtsql_storage::Field::required(
                "value".to_string(),
                yachtsql_core::types::DataType::Int64,
            ),
        ]);

        let mut col1 = yachtsql_storage::Column::new(&yachtsql_core::types::DataType::Int64, 3);
        col1.push(Value::int64(1)).unwrap();
        col1.push(Value::int64(1)).unwrap();
        col1.push(Value::int64(2)).unwrap();

        let mut col2 = yachtsql_storage::Column::new(&yachtsql_core::types::DataType::Int64, 3);
        col2.push(Value::int64(10)).unwrap();
        col2.push(Value::int64(20)).unwrap();
        col2.push(Value::int64(30)).unwrap();

        let batch = crate::Table::new(schema.clone(), vec![col1, col2]).unwrap();
        let input_exec = Rc::new(TableScanExec::new(
            schema.clone(),
            "test".to_string(),
            std::rc::Rc::new(std::cell::RefCell::new(yachtsql_storage::Storage::new())),
        ));

        let group_by = vec![yachtsql_optimizer::expr::Expr::Column {
            name: "id".to_string(),
            table: None,
        }];

        let aggregates = vec![(
            yachtsql_optimizer::expr::Expr::Aggregate {
                name: FunctionName::Count,
                args: vec![yachtsql_optimizer::expr::Expr::Column {
                    name: "value".to_string(),
                    table: None,
                }],
                distinct: false,
                order_by: None,
                filter: None,
            },
            Some("count".to_string()),
        )];

        let agg_exec = AggregateExec::new(input_exec, group_by, aggregates, None);
        assert!(agg_exec.is_ok());
    }

    #[test]
    fn test_compare_values() {
        assert_eq!(
            compare_values(&Value::int64(5), &Value::int64(10)).unwrap(),
            std::cmp::Ordering::Less
        );

        assert_eq!(
            compare_values(
                &Value::string("a".to_string()),
                &Value::string("b".to_string())
            )
            .unwrap(),
            std::cmp::Ordering::Less
        );

        assert_eq!(
            compare_values(&Value::int64(5), &Value::float64(5.0)).unwrap(),
            std::cmp::Ordering::Equal
        );
    }
}
