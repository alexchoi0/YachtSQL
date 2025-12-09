use sqlparser::ast::CteAsMaterialized;
use yachtsql_core::types::DataType;
use yachtsql_storage::schema::GeneratedExpression;

use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupingMetadata {
    pub grouped_columns: Vec<String>,
    pub grouping_set_id: usize,
    pub total_sets: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SamplingMethod {
    Bernoulli,
    System,
}

#[derive(Debug, Clone, PartialEq)]
pub enum SampleSize {
    Percent(f64),
    Rows(usize),
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogicalPlan {
    pub root: Box<PlanNode>,
}

impl LogicalPlan {
    pub fn new(root: PlanNode) -> Self {
        Self {
            root: Box::new(root),
        }
    }

    pub fn root(&self) -> &PlanNode {
        &self.root
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum PlanNode {
    Scan {
        table_name: String,
        alias: Option<String>,
        projection: Option<Vec<String>>,
        only: bool,
    },

    IndexScan {
        table_name: String,
        alias: Option<String>,
        index_name: String,
        predicate: Expr,
        projection: Option<Vec<String>>,
    },

    Filter {
        predicate: Expr,
        input: Box<PlanNode>,
    },

    Projection {
        expressions: Vec<(Expr, Option<String>)>,
        input: Box<PlanNode>,
    },

    Join {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        on: Expr,
        join_type: JoinType,
    },

    LateralJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        on: Expr,
        join_type: JoinType,
    },

    Aggregate {
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        input: Box<PlanNode>,
        grouping_metadata: Option<GroupingMetadata>,
    },

    Sort {
        order_by: Vec<crate::expr::OrderByExpr>,
        input: Box<PlanNode>,
    },

    Limit {
        limit: usize,
        offset: usize,
        input: Box<PlanNode>,
    },

    Distinct {
        input: Box<PlanNode>,
    },

    DistinctOn {
        expressions: Vec<Expr>,
        input: Box<PlanNode>,
    },

    SubqueryScan {
        subquery: Box<PlanNode>,
        alias: String,
    },

    Union {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        all: bool,
    },

    Intersect {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        all: bool,
    },

    Except {
        left: Box<PlanNode>,
        right: Box<PlanNode>,
        all: bool,
    },

    Cte {
        name: String,
        cte_plan: Box<PlanNode>,
        input: Box<PlanNode>,
        recursive: bool,
        use_union_all: bool,
        materialization_hint: Option<CteAsMaterialized>,
        column_aliases: Option<Vec<String>>,
    },

    Update {
        table_name: String,
        assignments: Vec<(String, Expr)>,
        predicate: Option<Expr>,
    },

    Delete {
        table_name: String,
        predicate: Option<Expr>,
    },

    Insert {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Option<Vec<Vec<Expr>>>,
        source: Option<Box<PlanNode>>,
    },

    Truncate {
        table_name: String,
    },

    Unnest {
        array_expr: Expr,
        alias: Option<String>,
        column_alias: Option<String>,
        with_offset: bool,
        offset_alias: Option<String>,
    },

    TableValuedFunction {
        function_name: String,
        args: Vec<Expr>,
        alias: Option<String>,
    },

    ArrayJoin {
        input: Box<PlanNode>,
        arrays: Vec<(Expr, Option<String>)>,
        is_left: bool,
        is_unaligned: bool,
    },

    Window {
        window_exprs: Vec<(Expr, Option<String>)>,
        input: Box<PlanNode>,
    },

    AlterTable {
        table_name: String,
        operation: AlterTableOperation,
        if_exists: bool,
    },

    EmptyRelation,

    Values {
        rows: Vec<Vec<Expr>>,
    },

    InsertOnConflict {
        table_name: String,
        columns: Option<Vec<String>>,
        values: Vec<Vec<Expr>>,
        conflict_target: Vec<String>,
        conflict_action: ConflictAction,
        where_clause: Option<Expr>,
        returning: Option<Vec<(Expr, Option<String>)>>,
    },

    Merge {
        target_table: String,
        target_alias: Option<String>,
        source: Box<PlanNode>,
        source_alias: Option<String>,
        on_condition: Expr,
        when_matched: Vec<MergeWhenMatched>,
        when_not_matched: Vec<MergeWhenNotMatched>,
        when_not_matched_by_source: Vec<MergeWhenNotMatchedBySource>,
        returning: Option<Vec<(Expr, Option<String>)>>,
    },

    TableSample {
        input: Box<PlanNode>,
        method: SamplingMethod,
        size: SampleSize,
        seed: Option<u64>,
    },

    Pivot {
        input: Box<PlanNode>,
        aggregate_expr: Expr,
        aggregate_function: String,
        pivot_column: String,
        pivot_values: Vec<yachtsql_core::types::Value>,
        group_by_columns: Vec<String>,
    },

    Unpivot {
        input: Box<PlanNode>,
        value_column: String,
        name_column: String,
        unpivot_columns: Vec<String>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConflictAction {
    Update { assignments: Vec<(String, Expr)> },
    DoNothing,
}

#[derive(Debug, Clone, PartialEq)]
pub struct WhenClause {
    pub match_type: MatchType,
    pub condition: Option<Expr>,
    pub action: MergeAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchType {
    Matched,
    NotMatched,
    NotMatchedBySource,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeAction {
    Update {
        assignments: Vec<(String, Expr)>,
    },
    Insert {
        columns: Option<Vec<String>>,
        values: Vec<Expr>,
    },
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReferentialAction {
    Cascade,
    SetNull,
    SetDefault,
    Restrict,
    NoAction,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableConstraint {
    PrimaryKey {
        name: Option<String>,
        columns: Vec<String>,
    },
    Unique {
        name: Option<String>,
        columns: Vec<String>,
    },
    Check {
        name: Option<String>,
        expression: String,
    },
    ForeignKey {
        name: Option<String>,
        columns: Vec<String>,
        ref_table: String,
        ref_columns: Vec<String>,
        on_delete: Option<ReferentialAction>,
        on_update: Option<ReferentialAction>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum AlterTableOperation {
    AddColumn {
        column_name: String,
        data_type: DataType,
        if_not_exists: bool,
        not_null: bool,
        default_value: Option<Expr>,
        generated_expression: Option<GeneratedExpression>,
    },
    DropColumn {
        column_name: String,
        if_exists: bool,
        cascade: bool,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    AlterColumn {
        column_name: String,
        new_data_type: Option<DataType>,
        set_not_null: Option<bool>,
        set_default: Option<Expr>,
        drop_default: bool,
    },
    RenameTable {
        new_name: String,
    },
    AddConstraint {
        constraint: TableConstraint,
    },
    DropConstraint {
        constraint_name: String,
        if_exists: bool,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenMatched {
    Update {
        condition: Option<Expr>,
        assignments: Vec<(String, Expr)>,
    },
    Delete {
        condition: Option<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenNotMatched {
    Insert {
        condition: Option<Expr>,
        columns: Vec<String>,
        values: Vec<Expr>,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub enum MergeWhenNotMatchedBySource {
    Update {
        condition: Option<Expr>,
        assignments: Vec<(String, Expr)>,
    },
    Delete {
        condition: Option<Expr>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
    Semi,
    Anti,
}

impl PlanNode {
    pub fn aggregate(group_by: Vec<Expr>, aggregates: Vec<Expr>, input: Box<PlanNode>) -> Self {
        Self::Aggregate {
            group_by,
            aggregates,
            grouping_metadata: None,
            input,
        }
    }

    pub fn children(&self) -> Vec<&PlanNode> {
        match self {
            PlanNode::Scan { .. }
            | PlanNode::IndexScan { .. }
            | PlanNode::Update { .. }
            | PlanNode::Delete { .. }
            | PlanNode::Truncate { .. }
            | PlanNode::Unnest { .. }
            | PlanNode::TableValuedFunction { .. }
            | PlanNode::AlterTable { .. }
            | PlanNode::InsertOnConflict { .. }
            | PlanNode::EmptyRelation
            | PlanNode::Values { .. } => vec![],
            PlanNode::Insert { source, .. } => {
                if let Some(source_plan) = source {
                    vec![source_plan.as_ref()]
                } else {
                    vec![]
                }
            }
            PlanNode::ArrayJoin { input, .. } => vec![input.as_ref()],
            PlanNode::Filter { input, .. }
            | PlanNode::Projection { input, .. }
            | PlanNode::Aggregate { input, .. }
            | PlanNode::Sort { input, .. }
            | PlanNode::Limit { input, .. }
            | PlanNode::Distinct { input }
            | PlanNode::DistinctOn { input, .. }
            | PlanNode::Window { input, .. } => vec![input],
            PlanNode::Join { left, right, .. }
            | PlanNode::LateralJoin { left, right, .. }
            | PlanNode::Union { left, right, .. }
            | PlanNode::Intersect { left, right, .. }
            | PlanNode::Except { left, right, .. } => vec![left, right],
            PlanNode::SubqueryScan { subquery, .. } => vec![subquery],
            PlanNode::Cte {
                cte_plan, input, ..
            } => vec![cte_plan, input],
            PlanNode::Merge { source, .. } => vec![source],
            PlanNode::TableSample { input, .. } => vec![input],
            PlanNode::Pivot { input, .. } => vec![input],
            PlanNode::Unpivot { input, .. } => vec![input],
        }
    }
}
