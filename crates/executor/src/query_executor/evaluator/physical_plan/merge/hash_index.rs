use std::collections::HashMap;

use yachtsql_common::types::Value;
use yachtsql_storage::Schema;

use super::types::{EquijoinPredicate, JoinKey};
use crate::storage::table::Row;

pub(super) struct MergeHashIndex {
    index: HashMap<JoinKey, Vec<usize>>,
    predicates: Vec<EquijoinPredicate>,
    #[allow(dead_code)]
    target_schema: Schema,
    source_schema: Schema,
}

impl MergeHashIndex {
    pub(super) fn build(
        target_snapshot: &[Row],
        predicates: Vec<EquijoinPredicate>,
        target_schema: &Schema,
        source_schema: &Schema,
    ) -> Self {
        let mut index: HashMap<JoinKey, Vec<usize>> = HashMap::new();

        for (row_idx, target_row) in target_snapshot.iter().enumerate() {
            let key = Self::extract_join_key(target_row, &predicates, true, target_schema);
            index.entry(key).or_default().push(row_idx);
        }

        Self {
            index,
            predicates,
            target_schema: target_schema.clone(),
            source_schema: source_schema.clone(),
        }
    }

    fn extract_join_key(
        row: &Row,
        predicates: &[EquijoinPredicate],
        is_target: bool,
        schema: &Schema,
    ) -> JoinKey {
        predicates
            .iter()
            .map(|pred| {
                let col_name = if is_target {
                    &pred.target_column
                } else {
                    &pred.source_column
                };
                let value = row
                    .get_by_name(schema, col_name)
                    .cloned()
                    .unwrap_or(Value::null());

                format!("{:?}", value)
            })
            .collect()
    }

    pub(super) fn lookup(&self, source_row: &Row) -> Vec<usize> {
        let key = Self::extract_join_key(source_row, &self.predicates, false, &self.source_schema);
        self.index.get(&key).cloned().unwrap_or_default()
    }
}
