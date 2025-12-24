use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, DclResourceType, ExportOptions, Expr,
    FunctionArg, FunctionBody, JoinType, LoadOptions, MergeClause, PlanSchema, ProcedureArg,
    RaiseLevel, SortExpr, UnnestColumn,
};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub enum SampleType {
    Rows,
    Percent,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum OptimizedLogicalPlan {
    TableScan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
    },

    Sample {
        input: Box<OptimizedLogicalPlan>,
        sample_type: SampleType,
        sample_value: i64,
    },

    Filter {
        input: Box<OptimizedLogicalPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<OptimizedLogicalPlan>,
        expressions: Vec<Expr>,
        schema: PlanSchema,
    },

    NestedLoopJoin {
        left: Box<OptimizedLogicalPlan>,
        right: Box<OptimizedLogicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: PlanSchema,
    },

    CrossJoin {
        left: Box<OptimizedLogicalPlan>,
        right: Box<OptimizedLogicalPlan>,
        schema: PlanSchema,
    },

    HashAggregate {
        input: Box<OptimizedLogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
        grouping_sets: Option<Vec<Vec<usize>>>,
    },

    Sort {
        input: Box<OptimizedLogicalPlan>,
        sort_exprs: Vec<SortExpr>,
    },

    Limit {
        input: Box<OptimizedLogicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    TopN {
        input: Box<OptimizedLogicalPlan>,
        sort_exprs: Vec<SortExpr>,
        limit: usize,
    },

    Distinct {
        input: Box<OptimizedLogicalPlan>,
    },

    Union {
        inputs: Vec<OptimizedLogicalPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Intersect {
        left: Box<OptimizedLogicalPlan>,
        right: Box<OptimizedLogicalPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Except {
        left: Box<OptimizedLogicalPlan>,
        right: Box<OptimizedLogicalPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Window {
        input: Box<OptimizedLogicalPlan>,
        window_exprs: Vec<Expr>,
        schema: PlanSchema,
    },

    Unnest {
        input: Box<OptimizedLogicalPlan>,
        columns: Vec<UnnestColumn>,
        schema: PlanSchema,
    },

    Qualify {
        input: Box<OptimizedLogicalPlan>,
        predicate: Expr,
    },

    WithCte {
        ctes: Vec<CteDefinition>,
        body: Box<OptimizedLogicalPlan>,
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
        source: Box<OptimizedLogicalPlan>,
    },

    Update {
        table_name: String,
        alias: Option<String>,
        assignments: Vec<Assignment>,
        from: Option<Box<OptimizedLogicalPlan>>,
        filter: Option<Expr>,
    },

    Delete {
        table_name: String,
        filter: Option<Expr>,
    },

    Merge {
        target_table: String,
        source: Box<OptimizedLogicalPlan>,
        on: Expr,
        clauses: Vec<MergeClause>,
    },

    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
        or_replace: bool,
        query: Option<Box<OptimizedLogicalPlan>>,
    },

    DropTable {
        table_names: Vec<String>,
        if_exists: bool,
    },

    AlterTable {
        table_name: String,
        operation: AlterTableOp,
        if_exists: bool,
    },

    Truncate {
        table_name: String,
    },

    CreateView {
        name: String,
        query: Box<OptimizedLogicalPlan>,
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

    AlterSchema {
        name: String,
        options: Vec<(String, String)>,
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
        body: Vec<OptimizedLogicalPlan>,
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
        query: Box<OptimizedLogicalPlan>,
    },

    LoadData {
        table_name: String,
        options: LoadOptions,
        temp_table: bool,
        temp_schema: Option<Vec<ColumnDef>>,
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
        then_branch: Vec<OptimizedLogicalPlan>,
        else_branch: Option<Vec<OptimizedLogicalPlan>>,
    },

    While {
        condition: Expr,
        body: Vec<OptimizedLogicalPlan>,
    },

    Loop {
        body: Vec<OptimizedLogicalPlan>,
        label: Option<String>,
    },

    Repeat {
        body: Vec<OptimizedLogicalPlan>,
        until_condition: Expr,
    },

    For {
        variable: String,
        query: Box<OptimizedLogicalPlan>,
        body: Vec<OptimizedLogicalPlan>,
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

    Assert {
        condition: Expr,
        message: Option<Expr>,
    },

    Grant {
        roles: Vec<String>,
        resource_type: DclResourceType,
        resource_name: String,
        grantees: Vec<String>,
    },

    Revoke {
        roles: Vec<String>,
        resource_type: DclResourceType,
        resource_name: String,
        grantees: Vec<String>,
    },
}

impl OptimizedLogicalPlan {
    pub fn schema(&self) -> &PlanSchema {
        use yachtsql_ir::EMPTY_SCHEMA;
        match self {
            OptimizedLogicalPlan::TableScan { schema, .. } => schema,
            OptimizedLogicalPlan::Sample { input, .. } => input.schema(),
            OptimizedLogicalPlan::Filter { input, .. } => input.schema(),
            OptimizedLogicalPlan::Project { schema, .. } => schema,
            OptimizedLogicalPlan::NestedLoopJoin { schema, .. } => schema,
            OptimizedLogicalPlan::CrossJoin { schema, .. } => schema,
            OptimizedLogicalPlan::HashAggregate { schema, .. } => schema,
            OptimizedLogicalPlan::Sort { input, .. } => input.schema(),
            OptimizedLogicalPlan::Limit { input, .. } => input.schema(),
            OptimizedLogicalPlan::TopN { input, .. } => input.schema(),
            OptimizedLogicalPlan::Distinct { input } => input.schema(),
            OptimizedLogicalPlan::Union { schema, .. } => schema,
            OptimizedLogicalPlan::Intersect { schema, .. } => schema,
            OptimizedLogicalPlan::Except { schema, .. } => schema,
            OptimizedLogicalPlan::Window { schema, .. } => schema,
            OptimizedLogicalPlan::Unnest { schema, .. } => schema,
            OptimizedLogicalPlan::Qualify { input, .. } => input.schema(),
            OptimizedLogicalPlan::WithCte { body, .. } => body.schema(),
            OptimizedLogicalPlan::Values { schema, .. } => schema,
            OptimizedLogicalPlan::Empty { schema } => schema,
            OptimizedLogicalPlan::Insert { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Update { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Delete { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Merge { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::CreateTable { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::DropTable { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::AlterTable { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Truncate { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::CreateView { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::DropView { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::CreateSchema { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::DropSchema { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::AlterSchema { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::CreateFunction { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::DropFunction { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::CreateProcedure { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::DropProcedure { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Call { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::ExportData { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::LoadData { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Declare { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::SetVariable { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::If { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::While { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Loop { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Repeat { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::For { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Return { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Raise { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Break => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Continue => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::CreateSnapshot { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::DropSnapshot { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Assert { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Grant { .. } => &EMPTY_SCHEMA,
            OptimizedLogicalPlan::Revoke { .. } => &EMPTY_SCHEMA,
        }
    }
}
