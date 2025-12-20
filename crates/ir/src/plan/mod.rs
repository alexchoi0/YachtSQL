mod cte;
mod ddl;
mod dml;
mod query;
mod scripting;
mod set_ops;
mod unnest;
mod window;

pub use cte::*;
pub use ddl::*;
pub use dml::*;
pub use query::*;
pub use scripting::*;
use serde::{Deserialize, Serialize};
pub use set_ops::*;
pub use unnest::*;
pub use window::*;
use yachtsql_common::types::DataType;

use crate::expr::{Expr, SortExpr};
use crate::schema::{Assignment, ColumnDef, EMPTY_SCHEMA, PlanSchema};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
    },

    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<LogicalPlan>,
        expressions: Vec<Expr>,
        schema: PlanSchema,
    },

    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
        grouping_sets: Option<Vec<Vec<usize>>>,
    },

    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: PlanSchema,
    },

    Sort {
        input: Box<LogicalPlan>,
        sort_exprs: Vec<SortExpr>,
    },

    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    Distinct {
        input: Box<LogicalPlan>,
    },

    Values {
        values: Vec<Vec<Expr>>,
        schema: PlanSchema,
    },

    Empty {
        schema: PlanSchema,
    },

    SetOperation {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        op: SetOperationType,
        all: bool,
        schema: PlanSchema,
    },

    Window {
        input: Box<LogicalPlan>,
        window_exprs: Vec<Expr>,
        schema: PlanSchema,
    },

    WithCte {
        ctes: Vec<CteDefinition>,
        body: Box<LogicalPlan>,
    },

    Unnest {
        input: Box<LogicalPlan>,
        columns: Vec<UnnestColumn>,
        schema: PlanSchema,
    },

    Qualify {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    Insert {
        table_name: String,
        columns: Vec<String>,
        source: Box<LogicalPlan>,
    },

    Update {
        table_name: String,
        assignments: Vec<Assignment>,
        filter: Option<Expr>,
    },

    Delete {
        table_name: String,
        filter: Option<Expr>,
    },

    Merge {
        target_table: String,
        source: Box<LogicalPlan>,
        on: Expr,
        clauses: Vec<MergeClause>,
    },

    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
        or_replace: bool,
    },

    DropTable {
        table_names: Vec<String>,
        if_exists: bool,
    },

    AlterTable {
        table_name: String,
        operation: AlterTableOp,
    },

    Truncate {
        table_name: String,
    },

    CreateView {
        name: String,
        query: Box<LogicalPlan>,
        query_sql: String,
        column_aliases: Vec<String>,
        or_replace: bool,
        if_not_exists: bool,
    },

    DropView {
        name: String,
        if_exists: bool,
    },

    CreateSchema {
        name: String,
        if_not_exists: bool,
    },

    DropSchema {
        name: String,
        if_exists: bool,
        cascade: bool,
    },

    CreateFunction {
        name: String,
        args: Vec<FunctionArg>,
        return_type: DataType,
        body: FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
    },

    DropFunction {
        name: String,
        if_exists: bool,
    },

    CreateProcedure {
        name: String,
        args: Vec<ProcedureArg>,
        body: Vec<LogicalPlan>,
        or_replace: bool,
    },

    DropProcedure {
        name: String,
        if_exists: bool,
    },

    Call {
        procedure_name: String,
        args: Vec<Expr>,
    },

    ExportData {
        options: ExportOptions,
        query: Box<LogicalPlan>,
    },

    Declare {
        name: String,
        data_type: DataType,
        default: Option<Expr>,
    },

    SetVariable {
        name: String,
        value: Expr,
    },

    If {
        condition: Expr,
        then_branch: Vec<LogicalPlan>,
        else_branch: Option<Vec<LogicalPlan>>,
    },

    While {
        condition: Expr,
        body: Vec<LogicalPlan>,
    },

    Loop {
        body: Vec<LogicalPlan>,
        label: Option<String>,
    },

    Repeat {
        body: Vec<LogicalPlan>,
        until_condition: Expr,
    },

    For {
        variable: String,
        query: Box<LogicalPlan>,
        body: Vec<LogicalPlan>,
    },

    Return {
        value: Option<Expr>,
    },

    Raise {
        message: Option<Expr>,
        level: RaiseLevel,
    },

    Break,

    Continue,

    CreateSnapshot {
        snapshot_name: String,
        source_name: String,
        if_not_exists: bool,
    },

    DropSnapshot {
        snapshot_name: String,
        if_exists: bool,
    },
}

impl LogicalPlan {
    pub fn schema(&self) -> &PlanSchema {
        match self {
            LogicalPlan::Scan { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema,
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Distinct { input, .. } => input.schema(),
            LogicalPlan::Values { schema, .. } => schema,
            LogicalPlan::Empty { schema } => schema,
            LogicalPlan::SetOperation { schema, .. } => schema,
            LogicalPlan::Window { schema, .. } => schema,
            LogicalPlan::WithCte { body, .. } => body.schema(),
            LogicalPlan::Unnest { schema, .. } => schema,
            LogicalPlan::Qualify { input, .. } => input.schema(),
            LogicalPlan::Insert { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Update { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Delete { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Merge { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::AlterTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Truncate { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateView { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropView { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateSchema { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropSchema { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateFunction { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropFunction { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateProcedure { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropProcedure { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Call { .. } => &EMPTY_SCHEMA,
            LogicalPlan::ExportData { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Declare { .. } => &EMPTY_SCHEMA,
            LogicalPlan::SetVariable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::If { .. } => &EMPTY_SCHEMA,
            LogicalPlan::While { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Loop { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Repeat { .. } => &EMPTY_SCHEMA,
            LogicalPlan::For { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Return { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Raise { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Break => &EMPTY_SCHEMA,
            LogicalPlan::Continue => &EMPTY_SCHEMA,
            LogicalPlan::CreateSnapshot { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropSnapshot { .. } => &EMPTY_SCHEMA,
        }
    }

    pub fn scan(table_name: impl Into<String>, schema: PlanSchema) -> Self {
        LogicalPlan::Scan {
            table_name: table_name.into(),
            schema,
            projection: None,
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    pub fn project(self, expressions: Vec<Expr>, schema: PlanSchema) -> Self {
        LogicalPlan::Project {
            input: Box::new(self),
            expressions,
            schema,
        }
    }

    pub fn sort(self, sort_exprs: Vec<SortExpr>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(self),
            sort_exprs,
        }
    }

    pub fn limit(self, limit: Option<usize>, offset: Option<usize>) -> Self {
        LogicalPlan::Limit {
            input: Box::new(self),
            limit,
            offset,
        }
    }

    pub fn distinct(self) -> Self {
        LogicalPlan::Distinct {
            input: Box::new(self),
        }
    }

    pub fn empty() -> Self {
        LogicalPlan::Empty {
            schema: PlanSchema::new(),
        }
    }
}
