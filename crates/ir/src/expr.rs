use yachtsql_core::types::DataType;

#[derive(Debug, Clone, PartialEq)]
pub enum Expr {
    Column {
        name: String,
        table: Option<String>,
    },
    Literal(LiteralValue),
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOp,
        right: Box<Expr>,
    },
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    Function {
        name: crate::function::FunctionName,
        args: Vec<Expr>,
    },
    Aggregate {
        name: crate::function::FunctionName,
        args: Vec<Expr>,
        distinct: bool,
        order_by: Option<Vec<OrderByExpr>>,
        filter: Option<Box<Expr>>,
    },
    Case {
        operand: Option<Box<Expr>>,
        when_then: Vec<(Expr, Expr)>,
        else_expr: Option<Box<Expr>>,
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
    Wildcard,
    QualifiedWildcard {
        qualifier: String,
    },
    Tuple(Vec<Expr>),
    Cast {
        expr: Box<Expr>,
        data_type: CastDataType,
    },
    TryCast {
        expr: Box<Expr>,
        data_type: CastDataType,
    },
    Subquery {
        plan: Box<crate::plan::PlanNode>,
    },
    Exists {
        plan: Box<crate::plan::PlanNode>,
        negated: bool,
    },
    InSubquery {
        expr: Box<Expr>,
        plan: Box<crate::plan::PlanNode>,
        negated: bool,
    },
    TupleInList {
        tuple: Vec<Expr>,
        list: Vec<Vec<Expr>>,
        negated: bool,
    },
    TupleInSubquery {
        tuple: Vec<Expr>,
        plan: Box<crate::plan::PlanNode>,
        negated: bool,
    },
    WindowFunction {
        name: crate::function::FunctionName,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<OrderByExpr>,
        frame_units: Option<WindowFrameUnits>,
        frame_start_offset: Option<i64>,
        frame_end_offset: Option<i64>,
        exclude: Option<ExcludeMode>,
        null_treatment: Option<NullTreatment>,
    },
    ArrayIndex {
        array: Box<Expr>,
        index: Box<Expr>,
        safe: bool,
    },
    ArraySlice {
        array: Box<Expr>,
        start: Option<Box<Expr>>,
        end: Option<Box<Expr>>,
    },
    StructLiteral {
        fields: Vec<StructLiteralField>,
    },
    StructFieldAccess {
        expr: Box<Expr>,
        field: String,
    },
    AnyOp {
        left: Box<Expr>,
        compare_op: BinaryOp,
        right: Box<Expr>,
    },
    AllOp {
        left: Box<Expr>,
        compare_op: BinaryOp,
        right: Box<Expr>,
    },
    ScalarSubquery {
        subquery: Box<crate::plan::PlanNode>,
    },
    Grouping {
        column: String,
    },
    Excluded {
        column: String,
    },
    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },
    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderByExpr {
    pub expr: Expr,
    pub asc: Option<bool>,
    pub nulls_first: Option<bool>,
    pub collation: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ExcludeMode {
    #[default]
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WindowFrameUnits {
    Rows,
    Range,
    Groups,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NullTreatment {
    #[default]
    RespectNulls,
    IgnoreNulls,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CastDataType {
    Int64,
    Float64,
    Numeric(Option<(u8, u8)>),
    String,
    Bytes,
    Bool,
    Date,
    DateTime,
    Time,
    Timestamp,
    TimestampTz,
    Geography,
    Json,
    Array(Box<CastDataType>),
    Vector(usize),
    Interval,
    Uuid,
    Hstore,
    MacAddr,
    MacAddr8,
    Int4Range,
    Int8Range,
    NumRange,
    TsRange,
    TsTzRange,
    DateRange,
    Custom(String, Vec<yachtsql_core::types::StructField>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinaryOp {
    Add,
    Subtract,
    Multiply,
    Divide,
    Modulo,
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    And,
    Or,
    BitwiseAnd,
    BitwiseOr,
    BitwiseXor,
    ShiftLeft,
    ShiftRight,
    Like,
    NotLike,
    ILike,
    NotILike,
    SimilarTo,
    NotSimilarTo,
    Concat,
    RegexMatch,
    RegexNotMatch,
    RegexMatchI,
    RegexNotMatchI,
    In,
    NotIn,
    VectorL2Distance,
    VectorInnerProduct,
    VectorCosineDistance,
    ArrayContains,
    ArrayContainedBy,
    ArrayOverlap,
    GeometricDistance,
    GeometricContains,
    GeometricContainedBy,
    GeometricOverlap,
    HashMinus,
    RangeAdjacent,
    RangeStrictlyLeft,
    RangeStrictlyRight,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Not,
    Negate,
    Plus,
    IsNull,
    IsNotNull,
    BitwiseNot,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LiteralValue {
    Null,
    Boolean(bool),
    Int64(i64),
    Float64(f64),
    Numeric(rust_decimal::Decimal),
    String(String),
    Bytes(Vec<u8>),
    Date(String),
    Timestamp(String),
    Uuid(String),
    Json(String),
    Array(Vec<Expr>),
    Vector(Vec<f64>),
    Interval(String),
    Range(String),
    Point(String),
    PgBox(String),
    Circle(String),
    MacAddr(String),
    MacAddr8(String),
}

impl LiteralValue {
    pub fn to_value(&self) -> yachtsql_core::types::Value {
        use yachtsql_core::types::Value;

        match self {
            LiteralValue::Null => Value::null(),
            LiteralValue::Boolean(b) => Value::bool_val(*b),
            LiteralValue::Int64(i) => Value::int64(*i),
            LiteralValue::Float64(f) => Value::float64(*f),
            LiteralValue::Numeric(d) => Value::numeric(*d),
            LiteralValue::String(s) => Value::string(s.clone()),
            LiteralValue::Bytes(b) => Value::bytes(b.clone()),
            LiteralValue::Date(s) => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Value::date(date)
                } else {
                    Value::null()
                }
            }
            LiteralValue::Timestamp(s) => yachtsql_core::types::parse_timestamp_to_utc(s)
                .map(Value::timestamp)
                .unwrap_or(Value::null()),
            LiteralValue::Json(s) => match serde_json::from_str(s) {
                Ok(json_val) => Value::json(json_val),
                Err(_) => Value::null(),
            },
            LiteralValue::Array(elements) => {
                let mut values = Vec::with_capacity(elements.len());
                for element in elements {
                    match element {
                        Expr::Literal(inner) => {
                            values.push(inner.to_value());
                        }
                        _ => {
                            return Value::null();
                        }
                    }
                }
                Value::array(values)
            }
            LiteralValue::Uuid(s) => yachtsql_core::types::parse_uuid_literal(s),
            LiteralValue::Vector(values) => Value::vector(values.clone()),
            LiteralValue::Interval(_s) => Value::null(),
            LiteralValue::Range(_s) => Value::null(),
            LiteralValue::Point(s) => yachtsql_core::types::parse_point_literal(s),
            LiteralValue::PgBox(s) => yachtsql_core::types::parse_pgbox_literal(s),
            LiteralValue::Circle(s) => yachtsql_core::types::parse_circle_literal(s),
            LiteralValue::MacAddr(s) => match yachtsql_core::types::MacAddress::parse(s, false) {
                Some(mac) => Value::macaddr(mac),
                None => Value::null(),
            },
            LiteralValue::MacAddr8(s) => match yachtsql_core::types::MacAddress::parse(s, true) {
                Some(mac) => Value::macaddr8(mac),
                None => match yachtsql_core::types::MacAddress::parse(s, false) {
                    Some(mac) => Value::macaddr8(mac.to_eui64()),
                    None => Value::null(),
                },
            },
        }
    }

    pub fn from_value(val: &yachtsql_core::types::Value) -> Self {
        if val.is_null() {
            LiteralValue::Null
        } else if let Some(b) = val.as_bool() {
            LiteralValue::Boolean(b)
        } else if let Some(i) = val.as_i64() {
            LiteralValue::Int64(i)
        } else if let Some(f) = val.as_f64() {
            LiteralValue::Float64(f)
        } else if let Some(s) = val.as_str() {
            LiteralValue::String(s.to_string())
        } else if let Some(arr) = val.as_array() {
            let elements: Vec<Expr> = arr
                .iter()
                .map(|v| Expr::Literal(LiteralValue::from_value(v)))
                .collect();
            LiteralValue::Array(elements)
        } else {
            LiteralValue::Null
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StructLiteralField {
    pub name: String,
    pub expr: Expr,
    pub declared_type: Option<DataType>,
}

impl Expr {
    pub fn column(name: impl Into<String>) -> Self {
        Self::Column {
            name: name.into(),
            table: None,
        }
    }

    pub fn qualified_column(table: impl Into<String>, name: impl Into<String>) -> Self {
        Self::Column {
            name: name.into(),
            table: Some(table.into()),
        }
    }

    pub fn literal(value: LiteralValue) -> Self {
        Self::Literal(value)
    }

    pub fn binary_op(left: Expr, op: BinaryOp, right: Expr) -> Self {
        Self::BinaryOp {
            left: Box::new(left),
            op,
            right: Box::new(right),
        }
    }

    pub fn unary_op(op: UnaryOp, expr: Expr) -> Self {
        Self::UnaryOp {
            op,
            expr: Box::new(expr),
        }
    }

    pub fn contains_subquery(&self) -> bool {
        match self {
            Expr::Subquery { .. }
            | Expr::Exists { .. }
            | Expr::InSubquery { .. }
            | Expr::TupleInSubquery { .. }
            | Expr::AnyOp { .. }
            | Expr::AllOp { .. }
            | Expr::ScalarSubquery { .. } => true,

            Expr::BinaryOp { left, right, .. } => {
                left.contains_subquery() || right.contains_subquery()
            }
            Expr::UnaryOp { expr, .. } => expr.contains_subquery(),
            Expr::Function { args, .. } | Expr::Aggregate { args, .. } => {
                args.iter().any(|arg| arg.contains_subquery())
            }
            Expr::Case {
                operand,
                when_then,
                else_expr,
            } => {
                operand.as_ref().is_some_and(|e| e.contains_subquery())
                    || when_then
                        .iter()
                        .any(|(when, then)| when.contains_subquery() || then.contains_subquery())
                    || else_expr.as_ref().is_some_and(|e| e.contains_subquery())
            }
            Expr::InList { expr, list, .. } => {
                expr.contains_subquery() || list.iter().any(|e| e.contains_subquery())
            }
            Expr::TupleInList { tuple, list, .. } => {
                tuple.iter().any(|e| e.contains_subquery())
                    || list
                        .iter()
                        .any(|tuple| tuple.iter().any(|e| e.contains_subquery()))
            }
            Expr::Between {
                expr, low, high, ..
            } => expr.contains_subquery() || low.contains_subquery() || high.contains_subquery(),
            Expr::Cast { expr, .. } | Expr::TryCast { expr, .. } => expr.contains_subquery(),
            Expr::WindowFunction {
                args,
                partition_by,
                order_by,
                frame_units: _,
                exclude: _,
                ..
            } => {
                args.iter().any(|arg| arg.contains_subquery())
                    || partition_by.iter().any(|e| e.contains_subquery())
                    || order_by.iter().any(|ob| ob.expr.contains_subquery())
            }
            Expr::ArrayIndex { array, index, .. } => {
                array.contains_subquery() || index.contains_subquery()
            }
            Expr::ArraySlice { array, start, end } => {
                array.contains_subquery()
                    || start.as_ref().is_some_and(|s| s.contains_subquery())
                    || end.as_ref().is_some_and(|e| e.contains_subquery())
            }
            Expr::StructLiteral { fields } => {
                fields.iter().any(|field| field.expr.contains_subquery())
            }
            Expr::StructFieldAccess { expr, .. } => expr.contains_subquery(),
            Expr::Tuple(exprs) => exprs.iter().any(|e| e.contains_subquery()),
            Expr::IsDistinctFrom { left, right, .. } => {
                left.contains_subquery() || right.contains_subquery()
            }

            Expr::Lambda { body, .. } => body.contains_subquery(),

            Expr::Column { .. }
            | Expr::Literal(_)
            | Expr::Wildcard
            | Expr::QualifiedWildcard { .. }
            | Expr::Grouping { .. }
            | Expr::Excluded { .. } => false,
        }
    }

    pub fn is_subquery_comparison(&self) -> bool {
        matches!(self, Expr::AnyOp { .. } | Expr::AllOp { .. })
    }

    pub fn subquery_comparison_left(&self) -> Option<&Expr> {
        match self {
            Expr::AnyOp { left, .. } | Expr::AllOp { left, .. } => Some(left),
            _ => None,
        }
    }

    pub fn is_trivial(&self) -> bool {
        matches!(
            self,
            Expr::Column { .. }
                | Expr::Literal(_)
                | Expr::Wildcard
                | Expr::QualifiedWildcard { .. }
        )
    }

    pub fn is_wildcard(&self) -> bool {
        matches!(self, Expr::Wildcard | Expr::QualifiedWildcard { .. })
    }
}
