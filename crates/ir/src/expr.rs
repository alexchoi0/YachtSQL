use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Expr {
    Literal(Literal),

    Column {
        table: Option<String>,
        name: String,
        index: Option<usize>,
    },

    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },

    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },

    ScalarFunction {
        name: ScalarFunction,
        args: Vec<Expr>,
    },

    Aggregate {
        func: AggregateFunction,
        args: Vec<Expr>,
        distinct: bool,
    },

    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Expr>>,
    },

    Cast {
        expr: Box<Expr>,
        data_type: DataType,
    },

    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },

    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    Between {
        expr: Box<Expr>,
        low: Box<Expr>,
        high: Box<Expr>,
        negated: bool,
    },

    Like {
        expr: Box<Expr>,
        pattern: Box<Expr>,
        negated: bool,
        case_insensitive: bool,
    },

    Alias {
        expr: Box<Expr>,
        name: String,
    },

    Wildcard {
        table: Option<String>,
    },

    Subquery(Box<crate::plan::LogicalPlan>),

    InSubquery {
        expr: Box<Expr>,
        subquery: Box<crate::plan::LogicalPlan>,
        negated: bool,
    },

    Exists {
        subquery: Box<crate::plan::LogicalPlan>,
        negated: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WhenClause {
    pub condition: Expr,
    pub result: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Literal {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(ordered_float::OrderedFloat<f64>),
    Numeric(Decimal),
    String(String),
    Bytes(Vec<u8>),
    Date(i32),
    Time(i64),
    Timestamp(i64),
    Interval { months: i32, days: i32, nanos: i64 },
    Array(Vec<Literal>),
    Struct(Vec<(String, Literal)>),
    Json(serde_json::Value),
}

impl Literal {
    pub fn data_type(&self) -> DataType {
        match self {
            Literal::Null => DataType::Unknown,
            Literal::Bool(_) => DataType::Bool,
            Literal::Int64(_) => DataType::Int64,
            Literal::Float64(_) => DataType::Float64,
            Literal::Numeric(_) => DataType::Numeric(None),
            Literal::String(_) => DataType::String,
            Literal::Bytes(_) => DataType::Bytes,
            Literal::Date(_) => DataType::Date,
            Literal::Time(_) => DataType::Time,
            Literal::Timestamp(_) => DataType::Timestamp,
            Literal::Interval { .. } => DataType::Interval,
            Literal::Array(elements) => {
                let elem_type = elements
                    .first()
                    .map(|e| e.data_type())
                    .unwrap_or(DataType::Unknown);
                DataType::Array(Box::new(elem_type))
            }
            Literal::Struct(fields) => {
                let struct_fields = fields
                    .iter()
                    .map(|(name, lit)| yachtsql_common::types::StructField {
                        name: name.clone(),
                        data_type: lit.data_type(),
                    })
                    .collect();
                DataType::Struct(struct_fields)
            }
            Literal::Json(_) => DataType::Json,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum BinaryOp {
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
    And,
    Or,
    Concat,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum UnaryOp {
    Not,
    Minus,
    Plus,
    BitwiseNot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum AggregateFunction {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    ArrayAgg,
    StringAgg,
    AnyValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ScalarFunction {
    Upper,
    Lower,
    Length,
    Trim,
    LTrim,
    RTrim,
    Substr,
    Concat,
    Replace,
    Reverse,
    Left,
    Right,
    Repeat,
    StartsWith,
    EndsWith,
    Contains,
    Abs,
    Round,
    Floor,
    Ceil,
    Sqrt,
    Power,
    Mod,
    Sign,
    Exp,
    Ln,
    Log,
    Log10,
    Coalesce,
    IfNull,
    NullIf,
    If,
    CurrentDate,
    CurrentTimestamp,
    CurrentTime,
    Extract,
    DateAdd,
    DateSub,
    DateDiff,
    DateTrunc,
    FormatDate,
    FormatTimestamp,
    ParseDate,
    ParseTimestamp,
    Custom(String),
}

impl Expr {
    pub fn literal_null() -> Self {
        Expr::Literal(Literal::Null)
    }

    pub fn literal_bool(v: bool) -> Self {
        Expr::Literal(Literal::Bool(v))
    }

    pub fn literal_i64(v: i64) -> Self {
        Expr::Literal(Literal::Int64(v))
    }

    pub fn literal_f64(v: f64) -> Self {
        Expr::Literal(Literal::Float64(ordered_float::OrderedFloat(v)))
    }

    pub fn literal_string(v: impl Into<String>) -> Self {
        Expr::Literal(Literal::String(v.into()))
    }

    pub fn column(name: impl Into<String>) -> Self {
        Expr::Column {
            table: None,
            name: name.into(),
            index: None,
        }
    }

    pub fn qualified_column(table: impl Into<String>, name: impl Into<String>) -> Self {
        Expr::Column {
            table: Some(table.into()),
            name: name.into(),
            index: None,
        }
    }

    pub fn alias(self, name: impl Into<String>) -> Self {
        Expr::Alias {
            expr: Box::new(self),
            name: name.into(),
        }
    }

    pub fn eq(self, other: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Eq,
            right: Box::new(other),
        }
    }

    pub fn and(self, other: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::And,
            right: Box::new(other),
        }
    }

    pub fn or(self, other: Expr) -> Self {
        Expr::BinaryOp {
            left: Box::new(self),
            op: BinaryOp::Or,
            right: Box::new(other),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SortExpr {
    pub expr: Expr,
    pub asc: bool,
    pub nulls_first: bool,
}
