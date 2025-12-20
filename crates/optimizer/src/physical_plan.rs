use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, ExportOptions, Expr, FunctionArg,
    FunctionBody, JoinType, MergeClause, PlanSchema, ProcedureArg, RaiseLevel, SortExpr,
    UnnestColumn,
};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PhysicalPlan {
    TableScan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
    },

    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<PhysicalPlan>,
        expressions: Vec<Expr>,
        schema: PlanSchema,
    },

    NestedLoopJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: PlanSchema,
    },

    CrossJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: PlanSchema,
    },

    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
        grouping_sets: Option<Vec<Vec<usize>>>,
    },

    Sort {
        input: Box<PhysicalPlan>,
        sort_exprs: Vec<SortExpr>,
    },

    Limit {
        input: Box<PhysicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    TopN {
        input: Box<PhysicalPlan>,
        sort_exprs: Vec<SortExpr>,
        limit: usize,
    },

    Distinct {
        input: Box<PhysicalPlan>,
    },

    Union {
        inputs: Vec<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Intersect {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Except {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Window {
        input: Box<PhysicalPlan>,
        window_exprs: Vec<Expr>,
        schema: PlanSchema,
    },

    Unnest {
        input: Box<PhysicalPlan>,
        columns: Vec<UnnestColumn>,
        schema: PlanSchema,
    },

    Qualify {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },

    WithCte {
        ctes: Vec<CteDefinition>,
        body: Box<PhysicalPlan>,
    },

    Values {
        values: Vec<Vec<Expr>>,
        schema: PlanSchema,
    },

    Empty {
        schema: PlanSchema,
    },

    Insert {
        table_name: String,
        columns: Vec<String>,
        source: Box<PhysicalPlan>,
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
        source: Box<PhysicalPlan>,
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
        query: Box<PhysicalPlan>,
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
        body: Vec<PhysicalPlan>,
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
        query: Box<PhysicalPlan>,
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
        then_branch: Vec<PhysicalPlan>,
        else_branch: Option<Vec<PhysicalPlan>>,
    },

    While {
        condition: Expr,
        body: Vec<PhysicalPlan>,
    },

    Loop {
        body: Vec<PhysicalPlan>,
        label: Option<String>,
    },

    Repeat {
        body: Vec<PhysicalPlan>,
        until_condition: Expr,
    },

    For {
        variable: String,
        query: Box<PhysicalPlan>,
        body: Vec<PhysicalPlan>,
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

impl PhysicalPlan {
    pub fn schema(&self) -> &PlanSchema {
        use yachtsql_ir::EMPTY_SCHEMA;
        match self {
            PhysicalPlan::TableScan { schema, .. } => schema,
            PhysicalPlan::Filter { input, .. } => input.schema(),
            PhysicalPlan::Project { schema, .. } => schema,
            PhysicalPlan::NestedLoopJoin { schema, .. } => schema,
            PhysicalPlan::CrossJoin { schema, .. } => schema,
            PhysicalPlan::HashAggregate { schema, .. } => schema,
            PhysicalPlan::Sort { input, .. } => input.schema(),
            PhysicalPlan::Limit { input, .. } => input.schema(),
            PhysicalPlan::TopN { input, .. } => input.schema(),
            PhysicalPlan::Distinct { input } => input.schema(),
            PhysicalPlan::Union { schema, .. } => schema,
            PhysicalPlan::Intersect { schema, .. } => schema,
            PhysicalPlan::Except { schema, .. } => schema,
            PhysicalPlan::Window { schema, .. } => schema,
            PhysicalPlan::Unnest { schema, .. } => schema,
            PhysicalPlan::Qualify { input, .. } => input.schema(),
            PhysicalPlan::WithCte { body, .. } => body.schema(),
            PhysicalPlan::Values { schema, .. } => schema,
            PhysicalPlan::Empty { schema } => schema,
            PhysicalPlan::Insert { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Update { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Delete { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Merge { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateTable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropTable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::AlterTable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Truncate { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateView { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropView { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateSchema { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropSchema { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateFunction { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropFunction { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::CreateProcedure { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropProcedure { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Call { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::ExportData { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Declare { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::SetVariable { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::If { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::While { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Loop { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Repeat { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::For { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Return { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Raise { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::Break => &EMPTY_SCHEMA,
            PhysicalPlan::Continue => &EMPTY_SCHEMA,
            PhysicalPlan::CreateSnapshot { .. } => &EMPTY_SCHEMA,
            PhysicalPlan::DropSnapshot { .. } => &EMPTY_SCHEMA,
        }
    }
}
