use std::collections::BTreeMap;

use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, DclResourceType, ExportOptions, Expr,
    FunctionArg, FunctionBody, GapFillColumn, JoinType, LoadOptions, MergeClause, PlanSchema,
    ProcedureArg, RaiseLevel, SortExpr, UnnestColumn,
};
use yachtsql_optimizer::{OptimizedLogicalPlan, SampleType};

pub const PARALLEL_ROW_THRESHOLD: u64 = 5000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BoundType {
    Compute,
    Memory,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionHints {
    pub parallel: bool,
    pub bound_type: BoundType,
    pub estimated_rows: u64,
}

impl Default for ExecutionHints {
    fn default() -> Self {
        Self {
            parallel: false,
            bound_type: BoundType::Compute,
            estimated_rows: 0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Read,
    Write,
    WriteOptional,
}

#[derive(Debug, Clone, Default)]
pub struct TableAccessSet {
    pub accesses: BTreeMap<String, AccessType>,
}

impl TableAccessSet {
    pub fn new() -> Self {
        Self {
            accesses: BTreeMap::new(),
        }
    }

    pub fn add_read(&mut self, table_name: String) {
        self.accesses.entry(table_name).or_insert(AccessType::Read);
    }

    pub fn add_write(&mut self, table_name: String) {
        self.accesses.insert(table_name, AccessType::Write);
    }

    pub fn add_write_optional(&mut self, table_name: String) {
        self.accesses
            .entry(table_name)
            .or_insert(AccessType::WriteOptional);
    }

    pub fn is_empty(&self) -> bool {
        self.accesses.is_empty()
    }
}

#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    TableScan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
        row_count: Option<u64>,
    },

    Sample {
        input: Box<PhysicalPlan>,
        sample_type: SampleType,
        sample_value: i64,
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
        parallel: bool,
        hints: ExecutionHints,
    },

    CrossJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        schema: PlanSchema,
        parallel: bool,
        hints: ExecutionHints,
    },

    HashJoin {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        join_type: JoinType,
        left_keys: Vec<Expr>,
        right_keys: Vec<Expr>,
        schema: PlanSchema,
        parallel: bool,
        hints: ExecutionHints,
    },

    HashAggregate {
        input: Box<PhysicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
        grouping_sets: Option<Vec<Vec<usize>>>,
        hints: ExecutionHints,
    },

    Sort {
        input: Box<PhysicalPlan>,
        sort_exprs: Vec<SortExpr>,
        hints: ExecutionHints,
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
        parallel: bool,
        hints: ExecutionHints,
    },

    Intersect {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
        parallel: bool,
        hints: ExecutionHints,
    },

    Except {
        left: Box<PhysicalPlan>,
        right: Box<PhysicalPlan>,
        all: bool,
        schema: PlanSchema,
        parallel: bool,
        hints: ExecutionHints,
    },

    Window {
        input: Box<PhysicalPlan>,
        window_exprs: Vec<Expr>,
        schema: PlanSchema,
        hints: ExecutionHints,
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
        parallel_ctes: Vec<usize>,
        hints: ExecutionHints,
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
        alias: Option<String>,
        assignments: Vec<Assignment>,
        from: Option<Box<PhysicalPlan>>,
        filter: Option<Expr>,
    },

    Delete {
        table_name: String,
        alias: Option<String>,
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
        query: Option<Box<PhysicalPlan>>,
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
        or_replace: bool,
    },

    DropSchema {
        name: String,
        if_exists: bool,
        cascade: bool,
    },

    UndropSchema {
        name: String,
        if_not_exists: bool,
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
        is_aggregate: bool,
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
        if_not_exists: bool,
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

    SetMultipleVariables {
        names: Vec<String>,
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
        label: Option<String>,
    },

    Loop {
        body: Vec<PhysicalPlan>,
        label: Option<String>,
    },

    Block {
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

    ExecuteImmediate {
        sql_expr: Expr,
        into_variables: Vec<String>,
        using_params: Vec<(Expr, Option<String>)>,
    },

    Break {
        label: Option<String>,
    },

    Continue {
        label: Option<String>,
    },

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

    BeginTransaction,

    Commit,

    Rollback,

    TryCatch {
        try_block: Vec<(PhysicalPlan, Option<String>)>,
        catch_block: Vec<PhysicalPlan>,
    },

    GapFill {
        input: Box<PhysicalPlan>,
        ts_column: String,
        bucket_width: Expr,
        value_columns: Vec<GapFillColumn>,
        partitioning_columns: Vec<String>,
        origin: Option<Expr>,
        input_schema: PlanSchema,
        schema: PlanSchema,
    },
}

impl PhysicalPlan {
    pub fn from_physical(plan: &OptimizedLogicalPlan) -> Self {
        match plan {
            OptimizedLogicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => PhysicalPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
                row_count: None,
            },

            OptimizedLogicalPlan::Sample {
                input,
                sample_type,
                sample_value,
            } => PhysicalPlan::Sample {
                input: Box::new(Self::from_physical(input)),
                sample_type: *sample_type,
                sample_value: *sample_value,
            },

            OptimizedLogicalPlan::Filter { input, predicate } => PhysicalPlan::Filter {
                input: Box::new(Self::from_physical(input)),
                predicate: predicate.clone(),
            },

            OptimizedLogicalPlan::Project {
                input,
                expressions,
                schema,
            } => PhysicalPlan::Project {
                input: Box::new(Self::from_physical(input)),
                expressions: expressions.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::NestedLoopJoin {
                    left: left_plan,
                    right: right_plan,
                    join_type: *join_type,
                    condition: condition.clone(),
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::CrossJoin {
                    left: left_plan,
                    right: right_plan,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::HashJoin {
                left,
                right,
                join_type,
                left_keys,
                right_keys,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::HashJoin {
                    left: left_plan,
                    right: right_plan,
                    join_type: *join_type,
                    left_keys: left_keys.clone(),
                    right_keys: right_keys.clone(),
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => PhysicalPlan::HashAggregate {
                input: Box::new(Self::from_physical(input)),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
                grouping_sets: grouping_sets.clone(),
                hints: ExecutionHints::default(),
            },

            OptimizedLogicalPlan::Sort { input, sort_exprs } => PhysicalPlan::Sort {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
                hints: ExecutionHints::default(),
            },

            OptimizedLogicalPlan::Limit {
                input,
                limit,
                offset,
            } => PhysicalPlan::Limit {
                input: Box::new(Self::from_physical(input)),
                limit: *limit,
                offset: *offset,
            },

            OptimizedLogicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => PhysicalPlan::TopN {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
                limit: *limit,
            },

            OptimizedLogicalPlan::Distinct { input } => PhysicalPlan::Distinct {
                input: Box::new(Self::from_physical(input)),
            },

            OptimizedLogicalPlan::Union {
                inputs,
                all,
                schema,
            } => {
                let input_plans: Vec<_> = inputs.iter().map(Self::from_physical).collect();
                let parallel = Self::should_parallelize_union(&input_plans);
                PhysicalPlan::Union {
                    inputs: input_plans,
                    all: *all,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::Intersect {
                    left: left_plan,
                    right: right_plan,
                    all: *all,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => {
                let left_plan = Box::new(Self::from_physical(left));
                let right_plan = Box::new(Self::from_physical(right));
                let parallel = Self::should_parallelize(&left_plan, &right_plan);
                PhysicalPlan::Except {
                    left: left_plan,
                    right: right_plan,
                    all: *all,
                    schema: schema.clone(),
                    parallel,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => PhysicalPlan::Window {
                input: Box::new(Self::from_physical(input)),
                window_exprs: window_exprs.clone(),
                schema: schema.clone(),
                hints: ExecutionHints::default(),
            },

            OptimizedLogicalPlan::Unnest {
                input,
                columns,
                schema,
            } => PhysicalPlan::Unnest {
                input: Box::new(Self::from_physical(input)),
                columns: columns.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Qualify { input, predicate } => PhysicalPlan::Qualify {
                input: Box::new(Self::from_physical(input)),
                predicate: predicate.clone(),
            },

            OptimizedLogicalPlan::WithCte { ctes, body } => {
                let parallel_ctes: Vec<usize> = ctes
                    .iter()
                    .enumerate()
                    .filter(|(_, cte)| !cte.recursive)
                    .filter(|(_, cte)| {
                        if let Ok(optimized) = yachtsql_optimizer::optimize(&cte.query) {
                            let plan = PhysicalPlan::from_physical(&optimized);
                            plan.estimate_rows() >= PARALLEL_ROW_THRESHOLD
                        } else {
                            false
                        }
                    })
                    .map(|(i, _)| i)
                    .collect();
                PhysicalPlan::WithCte {
                    ctes: ctes.clone(),
                    body: Box::new(Self::from_physical(body)),
                    parallel_ctes,
                    hints: ExecutionHints::default(),
                }
            }

            OptimizedLogicalPlan::Values { values, schema } => PhysicalPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Empty { schema } => PhysicalPlan::Empty {
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Insert {
                table_name,
                columns,
                source,
            } => PhysicalPlan::Insert {
                table_name: table_name.clone(),
                columns: columns.clone(),
                source: Box::new(Self::from_physical(source)),
            },

            OptimizedLogicalPlan::Update {
                table_name,
                alias,
                assignments,
                from,
                filter,
            } => PhysicalPlan::Update {
                table_name: table_name.clone(),
                alias: alias.clone(),
                assignments: assignments.clone(),
                from: from.as_ref().map(|p| Box::new(Self::from_physical(p))),
                filter: filter.clone(),
            },

            OptimizedLogicalPlan::Delete {
                table_name,
                alias,
                filter,
            } => PhysicalPlan::Delete {
                table_name: table_name.clone(),
                alias: alias.clone(),
                filter: filter.clone(),
            },

            OptimizedLogicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => PhysicalPlan::Merge {
                target_table: target_table.clone(),
                source: Box::new(Self::from_physical(source)),
                on: on.clone(),
                clauses: clauses.clone(),
            },

            OptimizedLogicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
                query,
            } => PhysicalPlan::CreateTable {
                table_name: table_name.clone(),
                columns: columns.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
                query: query.as_ref().map(|q| Box::new(Self::from_physical(q))),
            },

            OptimizedLogicalPlan::DropTable {
                table_names,
                if_exists,
            } => PhysicalPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::AlterTable {
                table_name,
                operation,
                if_exists,
            } => PhysicalPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::Truncate { table_name } => PhysicalPlan::Truncate {
                table_name: table_name.clone(),
            },

            OptimizedLogicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => PhysicalPlan::CreateView {
                name: name.clone(),
                query: Box::new(Self::from_physical(query)),
                query_sql: query_sql.clone(),
                column_aliases: column_aliases.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::DropView { name, if_exists } => PhysicalPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::CreateSchema {
                name,
                if_not_exists,
                or_replace,
            } => PhysicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
            },

            OptimizedLogicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => PhysicalPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            },

            OptimizedLogicalPlan::UndropSchema {
                name,
                if_not_exists,
            } => PhysicalPlan::UndropSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::AlterSchema { name, options } => PhysicalPlan::AlterSchema {
                name: name.clone(),
                options: options.clone(),
            },

            OptimizedLogicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
                is_aggregate,
            } => PhysicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
                is_aggregate: *is_aggregate,
            },

            OptimizedLogicalPlan::DropFunction { name, if_exists } => PhysicalPlan::DropFunction {
                name: name.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
                if_not_exists,
            } => PhysicalPlan::CreateProcedure {
                name: name.clone(),
                args: args.clone(),
                body: body.iter().map(Self::from_physical).collect(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::DropProcedure { name, if_exists } => {
                PhysicalPlan::DropProcedure {
                    name: name.clone(),
                    if_exists: *if_exists,
                }
            }

            OptimizedLogicalPlan::Call {
                procedure_name,
                args,
            } => PhysicalPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            },

            OptimizedLogicalPlan::ExportData { options, query } => PhysicalPlan::ExportData {
                options: options.clone(),
                query: Box::new(Self::from_physical(query)),
            },

            OptimizedLogicalPlan::LoadData {
                table_name,
                options,
                temp_table,
                temp_schema,
            } => PhysicalPlan::LoadData {
                table_name: table_name.clone(),
                options: options.clone(),
                temp_table: *temp_table,
                temp_schema: temp_schema.clone(),
            },

            OptimizedLogicalPlan::Declare {
                name,
                data_type,
                default,
            } => PhysicalPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            },

            OptimizedLogicalPlan::SetVariable { name, value } => PhysicalPlan::SetVariable {
                name: name.clone(),
                value: value.clone(),
            },

            OptimizedLogicalPlan::SetMultipleVariables { names, value } => {
                PhysicalPlan::SetMultipleVariables {
                    names: names.clone(),
                    value: value.clone(),
                }
            }

            OptimizedLogicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => PhysicalPlan::If {
                condition: condition.clone(),
                then_branch: then_branch.iter().map(Self::from_physical).collect(),
                else_branch: else_branch
                    .as_ref()
                    .map(|b| b.iter().map(Self::from_physical).collect()),
            },

            OptimizedLogicalPlan::While {
                condition,
                body,
                label,
            } => PhysicalPlan::While {
                condition: condition.clone(),
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            OptimizedLogicalPlan::Loop { body, label } => PhysicalPlan::Loop {
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            OptimizedLogicalPlan::Block { body, label } => PhysicalPlan::Block {
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            OptimizedLogicalPlan::Repeat {
                body,
                until_condition,
            } => PhysicalPlan::Repeat {
                body: body.iter().map(Self::from_physical).collect(),
                until_condition: until_condition.clone(),
            },

            OptimizedLogicalPlan::For {
                variable,
                query,
                body,
            } => PhysicalPlan::For {
                variable: variable.clone(),
                query: Box::new(Self::from_physical(query)),
                body: body.iter().map(Self::from_physical).collect(),
            },

            OptimizedLogicalPlan::Return { value } => PhysicalPlan::Return {
                value: value.clone(),
            },

            OptimizedLogicalPlan::Raise { message, level } => PhysicalPlan::Raise {
                message: message.clone(),
                level: *level,
            },

            OptimizedLogicalPlan::ExecuteImmediate {
                sql_expr,
                into_variables,
                using_params,
            } => PhysicalPlan::ExecuteImmediate {
                sql_expr: sql_expr.clone(),
                into_variables: into_variables.clone(),
                using_params: using_params.clone(),
            },

            OptimizedLogicalPlan::Break { label } => PhysicalPlan::Break {
                label: label.clone(),
            },

            OptimizedLogicalPlan::Continue { label } => PhysicalPlan::Continue {
                label: label.clone(),
            },

            OptimizedLogicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => PhysicalPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            },

            OptimizedLogicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => PhysicalPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            },

            OptimizedLogicalPlan::Assert { condition, message } => PhysicalPlan::Assert {
                condition: condition.clone(),
                message: message.clone(),
            },

            OptimizedLogicalPlan::Grant {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => PhysicalPlan::Grant {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            },

            OptimizedLogicalPlan::Revoke {
                roles,
                resource_type,
                resource_name,
                grantees,
            } => PhysicalPlan::Revoke {
                roles: roles.clone(),
                resource_type: resource_type.clone(),
                resource_name: resource_name.clone(),
                grantees: grantees.clone(),
            },

            OptimizedLogicalPlan::BeginTransaction => PhysicalPlan::BeginTransaction,
            OptimizedLogicalPlan::Commit => PhysicalPlan::Commit,
            OptimizedLogicalPlan::Rollback => PhysicalPlan::Rollback,

            OptimizedLogicalPlan::TryCatch {
                try_block,
                catch_block,
            } => PhysicalPlan::TryCatch {
                try_block: try_block
                    .iter()
                    .map(|(p, sql)| (PhysicalPlan::from_physical(p), sql.clone()))
                    .collect(),
                catch_block: catch_block
                    .iter()
                    .map(PhysicalPlan::from_physical)
                    .collect(),
            },

            OptimizedLogicalPlan::GapFill {
                input,
                ts_column,
                bucket_width,
                value_columns,
                partitioning_columns,
                origin,
                input_schema,
                schema,
            } => PhysicalPlan::GapFill {
                input: Box::new(PhysicalPlan::from_physical(input)),
                ts_column: ts_column.clone(),
                bucket_width: bucket_width.clone(),
                value_columns: value_columns.clone(),
                partitioning_columns: partitioning_columns.clone(),
                origin: origin.clone(),
                input_schema: input_schema.clone(),
                schema: schema.clone(),
            },
        }
    }

    pub fn schema(&self) -> Option<&PlanSchema> {
        match self {
            PhysicalPlan::TableScan { schema, .. } => Some(schema),
            PhysicalPlan::Sample { input, .. } => input.schema(),
            PhysicalPlan::Filter { input, .. } => input.schema(),
            PhysicalPlan::Project { schema, .. } => Some(schema),
            PhysicalPlan::NestedLoopJoin { schema, .. } => Some(schema),
            PhysicalPlan::CrossJoin { schema, .. } => Some(schema),
            PhysicalPlan::HashJoin { schema, .. } => Some(schema),
            PhysicalPlan::HashAggregate { schema, .. } => Some(schema),
            PhysicalPlan::Sort { input, .. } => input.schema(),
            PhysicalPlan::Limit { input, .. } => input.schema(),
            PhysicalPlan::TopN { input, .. } => input.schema(),
            PhysicalPlan::Distinct { input } => input.schema(),
            PhysicalPlan::Union { schema, .. } => Some(schema),
            PhysicalPlan::Intersect { schema, .. } => Some(schema),
            PhysicalPlan::Except { schema, .. } => Some(schema),
            PhysicalPlan::Window { schema, .. } => Some(schema),
            PhysicalPlan::Unnest { schema, .. } => Some(schema),
            PhysicalPlan::Qualify { input, .. } => input.schema(),
            PhysicalPlan::WithCte { body, .. } => body.schema(),
            PhysicalPlan::Values { schema, .. } => Some(schema),
            PhysicalPlan::Empty { schema } => Some(schema),
            PhysicalPlan::GapFill { schema, .. } => Some(schema),
            _ => None,
        }
    }

    pub fn extract_table_accesses(&self) -> TableAccessSet {
        let mut accesses = TableAccessSet::new();
        let mut cte_names = std::collections::HashSet::new();
        self.collect_accesses(&mut accesses, &mut cte_names);
        accesses
    }

    fn collect_accesses(
        &self,
        accesses: &mut TableAccessSet,
        cte_names: &mut std::collections::HashSet<String>,
    ) {
        match self {
            PhysicalPlan::TableScan { table_name, .. } => {
                let table_upper = table_name.to_uppercase();
                if !cte_names.contains(&table_upper) {
                    accesses.add_read(table_name.clone());
                }
            }

            PhysicalPlan::Sample { input, .. }
            | PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Window { input, .. }
            | PhysicalPlan::Unnest { input, .. }
            | PhysicalPlan::Qualify { input, .. }
            | PhysicalPlan::HashAggregate { input, .. } => {
                input.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::CrossJoin { left, right, .. }
            | PhysicalPlan::HashJoin { left, right, .. }
            | PhysicalPlan::Intersect { left, right, .. }
            | PhysicalPlan::Except { left, right, .. } => {
                left.collect_accesses(accesses, cte_names);
                right.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::Union { inputs, .. } => {
                for input in inputs {
                    input.collect_accesses(accesses, cte_names);
                }
            }

            PhysicalPlan::WithCte { ctes, body, .. } => {
                for cte in ctes {
                    cte_names.insert(cte.name.to_uppercase());
                    if let Ok(physical_cte) = yachtsql_optimizer::optimize(&cte.query) {
                        let cte_plan = PhysicalPlan::from_physical(&physical_cte);
                        cte_plan.collect_accesses(accesses, cte_names);
                    }
                }
                body.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::Insert {
                table_name, source, ..
            } => {
                accesses.add_write(table_name.clone());
                source.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::Update { table_name, .. } => {
                accesses.add_write(table_name.clone());
            }

            PhysicalPlan::Delete { table_name, .. } => {
                accesses.add_write(table_name.clone());
            }

            PhysicalPlan::Merge {
                target_table,
                source,
                ..
            } => {
                accesses.add_write(target_table.clone());
                source.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::Truncate { table_name } => {
                accesses.add_write(table_name.clone());
            }

            PhysicalPlan::AlterTable {
                table_name,
                if_exists,
                ..
            } => {
                if *if_exists {
                    accesses.add_write_optional(table_name.clone());
                } else {
                    accesses.add_write(table_name.clone());
                }
            }

            PhysicalPlan::LoadData {
                table_name,
                temp_table,
                ..
            } => {
                if *temp_table {
                    accesses.add_write_optional(table_name.clone());
                } else {
                    accesses.add_write(table_name.clone());
                }
            }

            PhysicalPlan::CreateSnapshot { source_name, .. } => {
                accesses.add_read(source_name.clone());
            }

            PhysicalPlan::CreateView { query, .. } => {
                query.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::ExportData { query, .. } => {
                query.collect_accesses(accesses, cte_names);
            }

            PhysicalPlan::For { query, body, .. } => {
                query.collect_accesses(accesses, cte_names);
                for stmt in body {
                    stmt.collect_accesses(accesses, cte_names);
                }
            }

            PhysicalPlan::If {
                then_branch,
                else_branch,
                ..
            } => {
                for stmt in then_branch {
                    stmt.collect_accesses(accesses, cte_names);
                }
                if let Some(else_stmts) = else_branch {
                    for stmt in else_stmts {
                        stmt.collect_accesses(accesses, cte_names);
                    }
                }
            }

            PhysicalPlan::While { body, .. }
            | PhysicalPlan::Loop { body, .. }
            | PhysicalPlan::Block { body, .. }
            | PhysicalPlan::Repeat { body, .. } => {
                for stmt in body {
                    stmt.collect_accesses(accesses, cte_names);
                }
            }

            PhysicalPlan::CreateProcedure { body, .. } => {
                for stmt in body {
                    stmt.collect_accesses(accesses, cte_names);
                }
            }

            PhysicalPlan::CreateTable { query, .. } => {
                if let Some(q) = query {
                    q.collect_accesses(accesses, cte_names);
                }
            }

            PhysicalPlan::DropTable { .. }
            | PhysicalPlan::DropView { .. }
            | PhysicalPlan::CreateSchema { .. }
            | PhysicalPlan::DropSchema { .. }
            | PhysicalPlan::UndropSchema { .. }
            | PhysicalPlan::AlterSchema { .. }
            | PhysicalPlan::CreateFunction { .. }
            | PhysicalPlan::DropFunction { .. }
            | PhysicalPlan::DropProcedure { .. }
            | PhysicalPlan::Call { .. }
            | PhysicalPlan::Declare { .. }
            | PhysicalPlan::SetVariable { .. }
            | PhysicalPlan::SetMultipleVariables { .. }
            | PhysicalPlan::Return { .. }
            | PhysicalPlan::Raise { .. }
            | PhysicalPlan::ExecuteImmediate { .. }
            | PhysicalPlan::Break { .. }
            | PhysicalPlan::Continue { .. }
            | PhysicalPlan::DropSnapshot { .. }
            | PhysicalPlan::Assert { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::BeginTransaction
            | PhysicalPlan::Commit
            | PhysicalPlan::Rollback
            | PhysicalPlan::TryCatch { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::Empty { .. }
            | PhysicalPlan::GapFill { .. } => {}
        }
    }

    pub fn estimate_rows(&self) -> u64 {
        match self {
            PhysicalPlan::TableScan { row_count, .. } => row_count.unwrap_or(1000),
            PhysicalPlan::Values { values, .. } => values.len() as u64,
            PhysicalPlan::Empty { .. } => 0,
            PhysicalPlan::Filter { input, .. } => input.estimate_rows() / 2,
            PhysicalPlan::Project { input, .. } => input.estimate_rows(),
            PhysicalPlan::Sample { sample_value, .. } => *sample_value as u64,
            PhysicalPlan::NestedLoopJoin { left, right, .. } => {
                left.estimate_rows().saturating_mul(right.estimate_rows())
            }
            PhysicalPlan::HashJoin { left, right, .. } => {
                std::cmp::max(left.estimate_rows(), right.estimate_rows())
            }
            PhysicalPlan::CrossJoin { left, right, .. } => {
                left.estimate_rows().saturating_mul(right.estimate_rows())
            }
            PhysicalPlan::HashAggregate {
                input, group_by, ..
            } => {
                if group_by.is_empty() {
                    1
                } else {
                    std::cmp::max(1, input.estimate_rows() / 10)
                }
            }
            PhysicalPlan::Sort { input, .. } => input.estimate_rows(),
            PhysicalPlan::Limit { limit, input, .. } => {
                std::cmp::min(limit.unwrap_or(usize::MAX) as u64, input.estimate_rows())
            }
            PhysicalPlan::TopN { limit, input, .. } => {
                std::cmp::min(*limit as u64, input.estimate_rows())
            }
            PhysicalPlan::Distinct { input } => std::cmp::max(1, input.estimate_rows() / 2),
            PhysicalPlan::Union { inputs, .. } => inputs.iter().map(|p| p.estimate_rows()).sum(),
            PhysicalPlan::Intersect { left, right, .. } => {
                std::cmp::min(left.estimate_rows(), right.estimate_rows())
            }
            PhysicalPlan::Except { left, .. } => left.estimate_rows(),
            PhysicalPlan::Window { input, .. } => input.estimate_rows(),
            PhysicalPlan::Unnest { input, .. } => input.estimate_rows().saturating_mul(10),
            PhysicalPlan::Qualify { input, .. } => std::cmp::max(1, input.estimate_rows() / 2),
            PhysicalPlan::WithCte { body, .. } => body.estimate_rows(),
            PhysicalPlan::GapFill { input, .. } => input.estimate_rows().saturating_mul(2),
            _ => 1,
        }
    }

    pub fn bound_type(&self) -> BoundType {
        match self {
            PhysicalPlan::TableScan { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::Empty { .. } => BoundType::Memory,

            PhysicalPlan::Limit { input, .. } | PhysicalPlan::Sample { input, .. } => {
                input.bound_type()
            }

            PhysicalPlan::Filter { input, predicate } => {
                if input.bound_type() == BoundType::Memory && !Self::is_expensive_expr(predicate) {
                    BoundType::Memory
                } else {
                    BoundType::Compute
                }
            }

            PhysicalPlan::Project {
                input, expressions, ..
            } => {
                if input.bound_type() == BoundType::Memory
                    && expressions.iter().all(|e| !Self::is_expensive_expr(e))
                {
                    BoundType::Memory
                } else {
                    BoundType::Compute
                }
            }

            PhysicalPlan::Distinct { input } => input.bound_type(),

            PhysicalPlan::Sort { .. }
            | PhysicalPlan::TopN { .. }
            | PhysicalPlan::HashAggregate { .. }
            | PhysicalPlan::Window { .. }
            | PhysicalPlan::NestedLoopJoin { .. }
            | PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::CrossJoin { .. }
            | PhysicalPlan::Union { .. }
            | PhysicalPlan::Intersect { .. }
            | PhysicalPlan::Except { .. }
            | PhysicalPlan::Unnest { .. }
            | PhysicalPlan::Qualify { .. }
            | PhysicalPlan::GapFill { .. }
            | PhysicalPlan::Merge { .. } => BoundType::Compute,

            _ => BoundType::Compute,
        }
    }

    fn is_expensive_expr(expr: &Expr) -> bool {
        use yachtsql_ir::ScalarFunction as SF;
        match expr {
            Expr::ScalarFunction { name, args } => {
                let expensive = matches!(
                    name,
                    SF::RegexpContains
                        | SF::RegexpExtract
                        | SF::RegexpExtractAll
                        | SF::RegexpInstr
                        | SF::RegexpReplace
                        | SF::RegexpSubstr
                        | SF::JsonExtract
                        | SF::JsonExtractScalar
                        | SF::JsonExtractArray
                        | SF::JsonValue
                        | SF::JsonQuery
                        | SF::ParseJson
                        | SF::ToJson
                        | SF::ToJsonString
                        | SF::Sqrt
                        | SF::Power
                        | SF::Pow
                        | SF::Log
                        | SF::Log10
                        | SF::Exp
                        | SF::Sin
                        | SF::Cos
                        | SF::Tan
                        | SF::Asin
                        | SF::Acos
                        | SF::Atan
                        | SF::Atan2
                        | SF::Sinh
                        | SF::Cosh
                        | SF::Tanh
                        | SF::Md5
                        | SF::Sha1
                        | SF::Sha256
                        | SF::Sha512
                );
                expensive || args.iter().any(Self::is_expensive_expr)
            }
            Expr::Case {
                operand,
                when_clauses,
                else_result,
            } => {
                operand.as_ref().is_some_and(|e| Self::is_expensive_expr(e))
                    || when_clauses.iter().any(|wc| {
                        Self::is_expensive_expr(&wc.condition)
                            || Self::is_expensive_expr(&wc.result)
                    })
                    || else_result
                        .as_ref()
                        .is_some_and(|e| Self::is_expensive_expr(e))
            }
            Expr::Subquery(_) | Expr::ScalarSubquery(_) | Expr::ArraySubquery(_) => true,
            Expr::BinaryOp { left, right, .. } => {
                Self::is_expensive_expr(left) || Self::is_expensive_expr(right)
            }
            Expr::UnaryOp { expr, .. } => Self::is_expensive_expr(expr),
            Expr::Like { expr, pattern, .. } => {
                Self::is_expensive_expr(expr) || Self::is_expensive_expr(pattern)
            }
            Expr::InList { expr, list, .. } => {
                Self::is_expensive_expr(expr) || list.iter().any(Self::is_expensive_expr)
            }
            Expr::Between {
                expr, low, high, ..
            } => {
                Self::is_expensive_expr(expr)
                    || Self::is_expensive_expr(low)
                    || Self::is_expensive_expr(high)
            }
            Expr::Cast { expr, .. } => Self::is_expensive_expr(expr),
            _ => false,
        }
    }

    fn should_parallelize(left: &Self, right: &Self) -> bool {
        left.estimate_rows() >= PARALLEL_ROW_THRESHOLD
            && right.estimate_rows() >= PARALLEL_ROW_THRESHOLD
    }

    fn should_parallelize_union(inputs: &[Self]) -> bool {
        inputs.len() >= 2
            && inputs
                .iter()
                .filter(|p| p.estimate_rows() >= PARALLEL_ROW_THRESHOLD)
                .count()
                >= 2
    }

    pub fn populate_row_counts(&mut self, catalog: &crate::concurrent_catalog::ConcurrentCatalog) {
        match self {
            PhysicalPlan::TableScan {
                table_name,
                row_count,
                ..
            } => {
                if let Some(handle) = catalog.get_table_handle(table_name) {
                    *row_count = Some(handle.read().row_count() as u64);
                }
            }
            PhysicalPlan::Filter { input, .. }
            | PhysicalPlan::Project { input, .. }
            | PhysicalPlan::Sort { input, .. }
            | PhysicalPlan::Limit { input, .. }
            | PhysicalPlan::TopN { input, .. }
            | PhysicalPlan::Distinct { input }
            | PhysicalPlan::Window { input, .. }
            | PhysicalPlan::Unnest { input, .. }
            | PhysicalPlan::Qualify { input, .. }
            | PhysicalPlan::Sample { input, .. }
            | PhysicalPlan::GapFill { input, .. } => {
                input.populate_row_counts(catalog);
            }
            PhysicalPlan::HashAggregate { input, .. } => {
                input.populate_row_counts(catalog);
            }
            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                parallel,
                ..
            }
            | PhysicalPlan::CrossJoin {
                left,
                right,
                parallel,
                ..
            }
            | PhysicalPlan::HashJoin {
                left,
                right,
                parallel,
                ..
            } => {
                left.populate_row_counts(catalog);
                right.populate_row_counts(catalog);
                *parallel = Self::should_parallelize(left, right);
            }
            PhysicalPlan::Union {
                inputs, parallel, ..
            } => {
                for input in inputs.iter_mut() {
                    input.populate_row_counts(catalog);
                }
                *parallel = Self::should_parallelize_union(inputs);
            }
            PhysicalPlan::Intersect {
                left,
                right,
                parallel,
                ..
            }
            | PhysicalPlan::Except {
                left,
                right,
                parallel,
                ..
            } => {
                left.populate_row_counts(catalog);
                right.populate_row_counts(catalog);
                *parallel = Self::should_parallelize(left, right);
            }
            PhysicalPlan::WithCte {
                ctes,
                body,
                parallel_ctes,
                ..
            } => {
                body.populate_row_counts(catalog);
                *parallel_ctes = ctes
                    .iter()
                    .enumerate()
                    .filter(|(_, cte)| !cte.recursive)
                    .filter(|(_, cte)| {
                        if let Ok(optimized) = yachtsql_optimizer::optimize(&cte.query) {
                            let mut plan = PhysicalPlan::from_physical(&optimized);
                            plan.populate_row_counts(catalog);
                            plan.estimate_rows() >= PARALLEL_ROW_THRESHOLD
                        } else {
                            false
                        }
                    })
                    .map(|(i, _)| i)
                    .collect();
            }
            PhysicalPlan::Values { .. }
            | PhysicalPlan::Empty { .. }
            | PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::AlterTable { .. }
            | PhysicalPlan::Truncate { .. }
            | PhysicalPlan::Insert { .. }
            | PhysicalPlan::Update { .. }
            | PhysicalPlan::Delete { .. }
            | PhysicalPlan::Merge { .. }
            | PhysicalPlan::CreateView { .. }
            | PhysicalPlan::DropView { .. }
            | PhysicalPlan::CreateSchema { .. }
            | PhysicalPlan::DropSchema { .. }
            | PhysicalPlan::UndropSchema { .. }
            | PhysicalPlan::AlterSchema { .. }
            | PhysicalPlan::CreateFunction { .. }
            | PhysicalPlan::DropFunction { .. }
            | PhysicalPlan::CreateProcedure { .. }
            | PhysicalPlan::DropProcedure { .. }
            | PhysicalPlan::Call { .. }
            | PhysicalPlan::SetVariable { .. }
            | PhysicalPlan::SetMultipleVariables { .. }
            | PhysicalPlan::BeginTransaction
            | PhysicalPlan::Commit
            | PhysicalPlan::Rollback
            | PhysicalPlan::ExportData { .. }
            | PhysicalPlan::LoadData { .. }
            | PhysicalPlan::CreateSnapshot { .. }
            | PhysicalPlan::DropSnapshot { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::Raise { .. }
            | PhysicalPlan::Block { .. }
            | PhysicalPlan::If { .. }
            | PhysicalPlan::While { .. }
            | PhysicalPlan::Loop { .. }
            | PhysicalPlan::Repeat { .. }
            | PhysicalPlan::For { .. }
            | PhysicalPlan::Declare { .. }
            | PhysicalPlan::Return { .. }
            | PhysicalPlan::ExecuteImmediate { .. }
            | PhysicalPlan::Break { .. }
            | PhysicalPlan::Continue { .. }
            | PhysicalPlan::TryCatch { .. }
            | PhysicalPlan::Assert { .. } => {}
        }
    }
}
