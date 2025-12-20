use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, ExportOptions, Expr, FunctionArg,
    FunctionBody, JoinType, MergeClause, PlanSchema, ProcedureArg, RaiseLevel, SortExpr,
    UnnestColumn,
};
use yachtsql_optimizer::PhysicalPlan;

#[derive(Debug, Clone)]
pub enum ExecutorPlan {
    TableScan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
    },

    Filter {
        input: Box<ExecutorPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<ExecutorPlan>,
        expressions: Vec<Expr>,
        schema: PlanSchema,
    },

    NestedLoopJoin {
        left: Box<ExecutorPlan>,
        right: Box<ExecutorPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: PlanSchema,
    },

    CrossJoin {
        left: Box<ExecutorPlan>,
        right: Box<ExecutorPlan>,
        schema: PlanSchema,
    },

    HashAggregate {
        input: Box<ExecutorPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
        grouping_sets: Option<Vec<Vec<usize>>>,
    },

    Sort {
        input: Box<ExecutorPlan>,
        sort_exprs: Vec<SortExpr>,
    },

    Limit {
        input: Box<ExecutorPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    TopN {
        input: Box<ExecutorPlan>,
        sort_exprs: Vec<SortExpr>,
        limit: usize,
    },

    Distinct {
        input: Box<ExecutorPlan>,
    },

    Union {
        inputs: Vec<ExecutorPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Intersect {
        left: Box<ExecutorPlan>,
        right: Box<ExecutorPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Except {
        left: Box<ExecutorPlan>,
        right: Box<ExecutorPlan>,
        all: bool,
        schema: PlanSchema,
    },

    Window {
        input: Box<ExecutorPlan>,
        window_exprs: Vec<Expr>,
        schema: PlanSchema,
    },

    Unnest {
        input: Box<ExecutorPlan>,
        columns: Vec<UnnestColumn>,
        schema: PlanSchema,
    },

    Qualify {
        input: Box<ExecutorPlan>,
        predicate: Expr,
    },

    WithCte {
        ctes: Vec<CteDefinition>,
        body: Box<ExecutorPlan>,
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
        source: Box<ExecutorPlan>,
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
        source: Box<ExecutorPlan>,
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
        query: Box<ExecutorPlan>,
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
        body: Vec<ExecutorPlan>,
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
        query: Box<ExecutorPlan>,
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
        then_branch: Vec<ExecutorPlan>,
        else_branch: Option<Vec<ExecutorPlan>>,
    },

    While {
        condition: Expr,
        body: Vec<ExecutorPlan>,
    },

    Loop {
        body: Vec<ExecutorPlan>,
        label: Option<String>,
    },

    Repeat {
        body: Vec<ExecutorPlan>,
        until_condition: Expr,
    },

    For {
        variable: String,
        query: Box<ExecutorPlan>,
        body: Vec<ExecutorPlan>,
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

impl ExecutorPlan {
    pub fn from_physical(plan: &PhysicalPlan) -> Self {
        match plan {
            PhysicalPlan::TableScan {
                table_name,
                schema,
                projection,
            } => ExecutorPlan::TableScan {
                table_name: table_name.clone(),
                schema: schema.clone(),
                projection: projection.clone(),
            },

            PhysicalPlan::Filter { input, predicate } => ExecutorPlan::Filter {
                input: Box::new(Self::from_physical(input)),
                predicate: predicate.clone(),
            },

            PhysicalPlan::Project {
                input,
                expressions,
                schema,
            } => ExecutorPlan::Project {
                input: Box::new(Self::from_physical(input)),
                expressions: expressions.clone(),
                schema: schema.clone(),
            },

            PhysicalPlan::NestedLoopJoin {
                left,
                right,
                join_type,
                condition,
                schema,
            } => ExecutorPlan::NestedLoopJoin {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            },

            PhysicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => ExecutorPlan::CrossJoin {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                schema: schema.clone(),
            },

            PhysicalPlan::HashAggregate {
                input,
                group_by,
                aggregates,
                schema,
                grouping_sets,
            } => ExecutorPlan::HashAggregate {
                input: Box::new(Self::from_physical(input)),
                group_by: group_by.clone(),
                aggregates: aggregates.clone(),
                schema: schema.clone(),
                grouping_sets: grouping_sets.clone(),
            },

            PhysicalPlan::Sort { input, sort_exprs } => ExecutorPlan::Sort {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
            },

            PhysicalPlan::Limit {
                input,
                limit,
                offset,
            } => ExecutorPlan::Limit {
                input: Box::new(Self::from_physical(input)),
                limit: *limit,
                offset: *offset,
            },

            PhysicalPlan::TopN {
                input,
                sort_exprs,
                limit,
            } => ExecutorPlan::TopN {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
                limit: *limit,
            },

            PhysicalPlan::Distinct { input } => ExecutorPlan::Distinct {
                input: Box::new(Self::from_physical(input)),
            },

            PhysicalPlan::Union {
                inputs,
                all,
                schema,
            } => ExecutorPlan::Union {
                inputs: inputs.iter().map(Self::from_physical).collect(),
                all: *all,
                schema: schema.clone(),
            },

            PhysicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => ExecutorPlan::Intersect {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                all: *all,
                schema: schema.clone(),
            },

            PhysicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => ExecutorPlan::Except {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                all: *all,
                schema: schema.clone(),
            },

            PhysicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => ExecutorPlan::Window {
                input: Box::new(Self::from_physical(input)),
                window_exprs: window_exprs.clone(),
                schema: schema.clone(),
            },

            PhysicalPlan::Unnest {
                input,
                columns,
                schema,
            } => ExecutorPlan::Unnest {
                input: Box::new(Self::from_physical(input)),
                columns: columns.clone(),
                schema: schema.clone(),
            },

            PhysicalPlan::Qualify { input, predicate } => ExecutorPlan::Qualify {
                input: Box::new(Self::from_physical(input)),
                predicate: predicate.clone(),
            },

            PhysicalPlan::WithCte { ctes, body } => ExecutorPlan::WithCte {
                ctes: ctes.clone(),
                body: Box::new(Self::from_physical(body)),
            },

            PhysicalPlan::Values { values, schema } => ExecutorPlan::Values {
                values: values.clone(),
                schema: schema.clone(),
            },

            PhysicalPlan::Empty { schema } => ExecutorPlan::Empty {
                schema: schema.clone(),
            },

            PhysicalPlan::Insert {
                table_name,
                columns,
                source,
            } => ExecutorPlan::Insert {
                table_name: table_name.clone(),
                columns: columns.clone(),
                source: Box::new(Self::from_physical(source)),
            },

            PhysicalPlan::Update {
                table_name,
                assignments,
                filter,
            } => ExecutorPlan::Update {
                table_name: table_name.clone(),
                assignments: assignments.clone(),
                filter: filter.clone(),
            },

            PhysicalPlan::Delete { table_name, filter } => ExecutorPlan::Delete {
                table_name: table_name.clone(),
                filter: filter.clone(),
            },

            PhysicalPlan::Merge {
                target_table,
                source,
                on,
                clauses,
            } => ExecutorPlan::Merge {
                target_table: target_table.clone(),
                source: Box::new(Self::from_physical(source)),
                on: on.clone(),
                clauses: clauses.clone(),
            },

            PhysicalPlan::CreateTable {
                table_name,
                columns,
                if_not_exists,
                or_replace,
            } => ExecutorPlan::CreateTable {
                table_name: table_name.clone(),
                columns: columns.clone(),
                if_not_exists: *if_not_exists,
                or_replace: *or_replace,
            },

            PhysicalPlan::DropTable {
                table_names,
                if_exists,
            } => ExecutorPlan::DropTable {
                table_names: table_names.clone(),
                if_exists: *if_exists,
            },

            PhysicalPlan::AlterTable {
                table_name,
                operation,
            } => ExecutorPlan::AlterTable {
                table_name: table_name.clone(),
                operation: operation.clone(),
            },

            PhysicalPlan::Truncate { table_name } => ExecutorPlan::Truncate {
                table_name: table_name.clone(),
            },

            PhysicalPlan::CreateView {
                name,
                query,
                query_sql,
                column_aliases,
                or_replace,
                if_not_exists,
            } => ExecutorPlan::CreateView {
                name: name.clone(),
                query: Box::new(Self::from_physical(query)),
                query_sql: query_sql.clone(),
                column_aliases: column_aliases.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
            },

            PhysicalPlan::DropView { name, if_exists } => ExecutorPlan::DropView {
                name: name.clone(),
                if_exists: *if_exists,
            },

            PhysicalPlan::CreateSchema {
                name,
                if_not_exists,
            } => ExecutorPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
            },

            PhysicalPlan::DropSchema {
                name,
                if_exists,
                cascade,
            } => ExecutorPlan::DropSchema {
                name: name.clone(),
                if_exists: *if_exists,
                cascade: *cascade,
            },

            PhysicalPlan::CreateFunction {
                name,
                args,
                return_type,
                body,
                or_replace,
                if_not_exists,
                is_temp,
            } => ExecutorPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
            },

            PhysicalPlan::DropFunction { name, if_exists } => ExecutorPlan::DropFunction {
                name: name.clone(),
                if_exists: *if_exists,
            },

            PhysicalPlan::CreateProcedure {
                name,
                args,
                body,
                or_replace,
            } => ExecutorPlan::CreateProcedure {
                name: name.clone(),
                args: args.clone(),
                body: body.iter().map(Self::from_physical).collect(),
                or_replace: *or_replace,
            },

            PhysicalPlan::DropProcedure { name, if_exists } => ExecutorPlan::DropProcedure {
                name: name.clone(),
                if_exists: *if_exists,
            },

            PhysicalPlan::Call {
                procedure_name,
                args,
            } => ExecutorPlan::Call {
                procedure_name: procedure_name.clone(),
                args: args.clone(),
            },

            PhysicalPlan::ExportData { options, query } => ExecutorPlan::ExportData {
                options: options.clone(),
                query: Box::new(Self::from_physical(query)),
            },

            PhysicalPlan::Declare {
                name,
                data_type,
                default,
            } => ExecutorPlan::Declare {
                name: name.clone(),
                data_type: data_type.clone(),
                default: default.clone(),
            },

            PhysicalPlan::SetVariable { name, value } => ExecutorPlan::SetVariable {
                name: name.clone(),
                value: value.clone(),
            },

            PhysicalPlan::If {
                condition,
                then_branch,
                else_branch,
            } => ExecutorPlan::If {
                condition: condition.clone(),
                then_branch: then_branch.iter().map(Self::from_physical).collect(),
                else_branch: else_branch
                    .as_ref()
                    .map(|b| b.iter().map(Self::from_physical).collect()),
            },

            PhysicalPlan::While { condition, body } => ExecutorPlan::While {
                condition: condition.clone(),
                body: body.iter().map(Self::from_physical).collect(),
            },

            PhysicalPlan::Loop { body, label } => ExecutorPlan::Loop {
                body: body.iter().map(Self::from_physical).collect(),
                label: label.clone(),
            },

            PhysicalPlan::Repeat {
                body,
                until_condition,
            } => ExecutorPlan::Repeat {
                body: body.iter().map(Self::from_physical).collect(),
                until_condition: until_condition.clone(),
            },

            PhysicalPlan::For {
                variable,
                query,
                body,
            } => ExecutorPlan::For {
                variable: variable.clone(),
                query: Box::new(Self::from_physical(query)),
                body: body.iter().map(Self::from_physical).collect(),
            },

            PhysicalPlan::Return { value } => ExecutorPlan::Return {
                value: value.clone(),
            },

            PhysicalPlan::Raise { message, level } => ExecutorPlan::Raise {
                message: message.clone(),
                level: *level,
            },

            PhysicalPlan::Break => ExecutorPlan::Break,

            PhysicalPlan::Continue => ExecutorPlan::Continue,

            PhysicalPlan::CreateSnapshot {
                snapshot_name,
                source_name,
                if_not_exists,
            } => ExecutorPlan::CreateSnapshot {
                snapshot_name: snapshot_name.clone(),
                source_name: source_name.clone(),
                if_not_exists: *if_not_exists,
            },

            PhysicalPlan::DropSnapshot {
                snapshot_name,
                if_exists,
            } => ExecutorPlan::DropSnapshot {
                snapshot_name: snapshot_name.clone(),
                if_exists: *if_exists,
            },
        }
    }
}
