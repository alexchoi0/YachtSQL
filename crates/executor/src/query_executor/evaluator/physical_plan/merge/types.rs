use yachtsql_storage::Schema;

use crate::storage::table::Row;

pub(super) enum MatchResult {
    Matched {
        target_idx: usize,
        join_row: Row,
        source_row: Row,
    },
    NotMatched,
}

pub(super) struct MergeContext<'a> {
    pub target_snapshot: Vec<Row>,

    pub source_schema: &'a Schema,

    pub target_schema: Schema,

    pub hash_index: Option<super::hash_index::MergeHashIndex>,
}

impl<'a> MergeContext<'a> {
    pub(super) fn new(
        target_table: &crate::storage::table::Table,
        source_schema: &'a Schema,
        equijoin_predicates: Option<Vec<EquijoinPredicate>>,
    ) -> yachtsql_common::error::Result<Self> {
        let target_schema = target_table.schema().clone();
        let target_row_count = target_table.row_count();

        let target_snapshot = (0..target_row_count)
            .map(|idx| target_table.get_row(idx))
            .collect::<yachtsql_common::error::Result<Vec<_>>>()?;

        let hash_index = equijoin_predicates.map(|preds| {
            super::hash_index::MergeHashIndex::build(
                &target_snapshot,
                preds,
                &target_schema,
                source_schema,
            )
        });

        Ok(Self {
            target_snapshot,
            source_schema,
            target_schema,
            hash_index,
        })
    }
}

pub(super) type JoinKey = Vec<String>;

pub(super) struct EquijoinPredicate {
    pub target_column: String,
    pub source_column: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MergeAction {
    Insert,
    Update,
    Delete,
}

impl MergeAction {
    pub fn as_str(&self) -> &'static str {
        match self {
            MergeAction::Insert => "INSERT",
            MergeAction::Update => "UPDATE",
            MergeAction::Delete => "DELETE",
        }
    }
}

#[derive(Clone)]
pub(super) struct MergeReturningRow {
    pub target_row: Row,
    pub source_row: Option<Row>,
    pub action: MergeAction,
}
