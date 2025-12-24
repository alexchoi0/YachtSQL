use std::collections::BTreeMap;

use yachtsql_common::types::DataType;
use yachtsql_ir::{
    AlterTableOp, Assignment, ColumnDef, CteDefinition, DclResourceType, ExportOptions, Expr,
    FunctionArg, FunctionBody, JoinType, LoadOptions, MergeClause, PlanSchema, ProcedureArg,
    RaiseLevel, SortExpr, UnnestColumn,
};
use yachtsql_optimizer::{OptimizedLogicalPlan, SampleType};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessType {
    Read,
    Write,
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
        alias: Option<String>,
        assignments: Vec<Assignment>,
        from: Option<Box<PhysicalPlan>>,
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
            } => PhysicalPlan::NestedLoopJoin {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                join_type: *join_type,
                condition: condition.clone(),
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::CrossJoin {
                left,
                right,
                schema,
            } => PhysicalPlan::CrossJoin {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                schema: schema.clone(),
            },

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
            },

            OptimizedLogicalPlan::Sort { input, sort_exprs } => PhysicalPlan::Sort {
                input: Box::new(Self::from_physical(input)),
                sort_exprs: sort_exprs.clone(),
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
            } => PhysicalPlan::Union {
                inputs: inputs.iter().map(Self::from_physical).collect(),
                all: *all,
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Intersect {
                left,
                right,
                all,
                schema,
            } => PhysicalPlan::Intersect {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                all: *all,
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Except {
                left,
                right,
                all,
                schema,
            } => PhysicalPlan::Except {
                left: Box::new(Self::from_physical(left)),
                right: Box::new(Self::from_physical(right)),
                all: *all,
                schema: schema.clone(),
            },

            OptimizedLogicalPlan::Window {
                input,
                window_exprs,
                schema,
            } => PhysicalPlan::Window {
                input: Box::new(Self::from_physical(input)),
                window_exprs: window_exprs.clone(),
                schema: schema.clone(),
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

            OptimizedLogicalPlan::WithCte { ctes, body } => PhysicalPlan::WithCte {
                ctes: ctes.clone(),
                body: Box::new(Self::from_physical(body)),
            },

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

            OptimizedLogicalPlan::Delete { table_name, filter } => PhysicalPlan::Delete {
                table_name: table_name.clone(),
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
            } => PhysicalPlan::CreateSchema {
                name: name.clone(),
                if_not_exists: *if_not_exists,
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
            } => PhysicalPlan::CreateFunction {
                name: name.clone(),
                args: args.clone(),
                return_type: return_type.clone(),
                body: body.clone(),
                or_replace: *or_replace,
                if_not_exists: *if_not_exists,
                is_temp: *is_temp,
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
            } => PhysicalPlan::CreateProcedure {
                name: name.clone(),
                args: args.clone(),
                body: body.iter().map(Self::from_physical).collect(),
                or_replace: *or_replace,
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

            OptimizedLogicalPlan::While { condition, body } => PhysicalPlan::While {
                condition: condition.clone(),
                body: body.iter().map(Self::from_physical).collect(),
            },

            OptimizedLogicalPlan::Loop { body, label } => PhysicalPlan::Loop {
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

            OptimizedLogicalPlan::Break => PhysicalPlan::Break,

            OptimizedLogicalPlan::Continue => PhysicalPlan::Continue,

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
        }
    }

    pub fn extract_table_accesses(&self) -> TableAccessSet {
        let mut accesses = TableAccessSet::new();
        self.collect_accesses(&mut accesses);
        accesses
    }

    fn collect_accesses(&self, accesses: &mut TableAccessSet) {
        match self {
            PhysicalPlan::TableScan { table_name, .. } => {
                accesses.add_read(table_name.clone());
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
                input.collect_accesses(accesses);
            }

            PhysicalPlan::NestedLoopJoin { left, right, .. }
            | PhysicalPlan::CrossJoin { left, right, .. }
            | PhysicalPlan::Intersect { left, right, .. }
            | PhysicalPlan::Except { left, right, .. } => {
                left.collect_accesses(accesses);
                right.collect_accesses(accesses);
            }

            PhysicalPlan::Union { inputs, .. } => {
                for input in inputs {
                    input.collect_accesses(accesses);
                }
            }

            PhysicalPlan::WithCte { ctes, body } => {
                for cte in ctes {
                    if let Ok(physical_cte) = yachtsql_optimizer::optimize(&cte.query) {
                        let cte_plan = PhysicalPlan::from_physical(&physical_cte);
                        cte_plan.collect_accesses(accesses);
                    }
                }
                body.collect_accesses(accesses);
            }

            PhysicalPlan::Insert {
                table_name, source, ..
            } => {
                accesses.add_write(table_name.clone());
                source.collect_accesses(accesses);
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
                source.collect_accesses(accesses);
            }

            PhysicalPlan::Truncate { table_name } => {
                accesses.add_write(table_name.clone());
            }

            PhysicalPlan::AlterTable { table_name, .. } => {
                accesses.add_write(table_name.clone());
            }

            PhysicalPlan::LoadData { table_name, .. } => {
                accesses.add_write(table_name.clone());
            }

            PhysicalPlan::CreateSnapshot { source_name, .. } => {
                accesses.add_read(source_name.clone());
            }

            PhysicalPlan::CreateView { query, .. } => {
                query.collect_accesses(accesses);
            }

            PhysicalPlan::ExportData { query, .. } => {
                query.collect_accesses(accesses);
            }

            PhysicalPlan::For { query, body, .. } => {
                query.collect_accesses(accesses);
                for stmt in body {
                    stmt.collect_accesses(accesses);
                }
            }

            PhysicalPlan::If {
                then_branch,
                else_branch,
                ..
            } => {
                for stmt in then_branch {
                    stmt.collect_accesses(accesses);
                }
                if let Some(else_stmts) = else_branch {
                    for stmt in else_stmts {
                        stmt.collect_accesses(accesses);
                    }
                }
            }

            PhysicalPlan::While { body, .. }
            | PhysicalPlan::Loop { body, .. }
            | PhysicalPlan::Repeat { body, .. } => {
                for stmt in body {
                    stmt.collect_accesses(accesses);
                }
            }

            PhysicalPlan::CreateProcedure { body, .. } => {
                for stmt in body {
                    stmt.collect_accesses(accesses);
                }
            }

            PhysicalPlan::CreateTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::DropView { .. }
            | PhysicalPlan::CreateSchema { .. }
            | PhysicalPlan::DropSchema { .. }
            | PhysicalPlan::AlterSchema { .. }
            | PhysicalPlan::CreateFunction { .. }
            | PhysicalPlan::DropFunction { .. }
            | PhysicalPlan::DropProcedure { .. }
            | PhysicalPlan::Call { .. }
            | PhysicalPlan::Declare { .. }
            | PhysicalPlan::SetVariable { .. }
            | PhysicalPlan::Return { .. }
            | PhysicalPlan::Raise { .. }
            | PhysicalPlan::Break
            | PhysicalPlan::Continue
            | PhysicalPlan::DropSnapshot { .. }
            | PhysicalPlan::Assert { .. }
            | PhysicalPlan::Grant { .. }
            | PhysicalPlan::Revoke { .. }
            | PhysicalPlan::Values { .. }
            | PhysicalPlan::Empty { .. } => {}
        }
    }
}
