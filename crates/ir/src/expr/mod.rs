mod datetime;
mod functions;
mod literal;
mod operators;
mod types;
mod window;

pub use datetime::*;
pub use functions::*;
pub use literal::*;
pub use operators::*;
use serde::{Deserialize, Serialize};
pub use types::*;
pub use window::*;
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
        filter: Option<Box<Expr>>,
        order_by: Vec<SortExpr>,
        limit: Option<usize>,
        ignore_nulls: bool,
    },

    Window {
        func: WindowFunction,
        args: Vec<Expr>,
        partition_by: Vec<Expr>,
        order_by: Vec<SortExpr>,
        frame: Option<WindowFrame>,
    },

    AggregateWindow {
        func: AggregateFunction,
        args: Vec<Expr>,
        distinct: bool,
        partition_by: Vec<Expr>,
        order_by: Vec<SortExpr>,
        frame: Option<WindowFrame>,
    },

    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<WhenClause>,
        else_result: Option<Box<Expr>>,
    },

    Cast {
        expr: Box<Expr>,
        data_type: DataType,
        safe: bool,
    },

    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },

    IsDistinctFrom {
        left: Box<Expr>,
        right: Box<Expr>,
        negated: bool,
    },

    InList {
        expr: Box<Expr>,
        list: Vec<Expr>,
        negated: bool,
    },

    InSubquery {
        expr: Box<Expr>,
        subquery: Box<crate::plan::LogicalPlan>,
        negated: bool,
    },

    InUnnest {
        expr: Box<Expr>,
        array_expr: Box<Expr>,
        negated: bool,
    },

    Exists {
        subquery: Box<crate::plan::LogicalPlan>,
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

    Extract {
        field: DateTimeField,
        expr: Box<Expr>,
    },

    Substring {
        expr: Box<Expr>,
        start: Option<Box<Expr>>,
        length: Option<Box<Expr>>,
    },

    Trim {
        expr: Box<Expr>,
        trim_what: Option<Box<Expr>>,
        trim_where: TrimWhere,
    },

    Position {
        substr: Box<Expr>,
        string: Box<Expr>,
    },

    Overlay {
        expr: Box<Expr>,
        overlay_what: Box<Expr>,
        overlay_from: Box<Expr>,
        overlay_for: Option<Box<Expr>>,
    },

    Array {
        elements: Vec<Expr>,
        element_type: Option<DataType>,
    },

    ArrayAccess {
        array: Box<Expr>,
        index: Box<Expr>,
    },

    Struct {
        fields: Vec<(Option<String>, Expr)>,
    },

    StructAccess {
        expr: Box<Expr>,
        field: String,
    },

    TypedString {
        data_type: DataType,
        value: String,
    },

    Interval {
        value: Box<Expr>,
        leading_field: Option<DateTimeField>,
    },

    Alias {
        expr: Box<Expr>,
        name: String,
    },

    Wildcard {
        table: Option<String>,
    },

    Subquery(Box<crate::plan::LogicalPlan>),

    ScalarSubquery(Box<crate::plan::LogicalPlan>),

    ArraySubquery(Box<crate::plan::LogicalPlan>),

    Parameter {
        name: String,
    },

    Variable {
        name: String,
    },

    Placeholder {
        id: String,
    },

    Lambda {
        params: Vec<String>,
        body: Box<Expr>,
    },

    AtTimeZone {
        timestamp: Box<Expr>,
        time_zone: Box<Expr>,
    },

    JsonAccess {
        expr: Box<Expr>,
        path: Vec<JsonPathElement>,
    },

    Default,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum JsonPathElement {
    Key(String),
    Index(i64),
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
