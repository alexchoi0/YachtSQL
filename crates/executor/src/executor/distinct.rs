use std::collections::HashSet;

use yachtsql_common::error::Result;
use yachtsql_common::types::Value;
use yachtsql_storage::Table;

use super::PlanExecutor;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_distinct(&mut self, input: &ExecutorPlan) -> Result<Table> {
        let input_table = self.execute_plan(input)?;
        let schema = input_table.schema().clone();
        let mut result = Table::empty(schema);

        let mut seen: HashSet<Vec<ValueKey>> = HashSet::new();

        for record in input_table.rows()? {
            let key: Vec<ValueKey> = record.values().iter().map(ValueKey::from).collect();
            if seen.insert(key) {
                result.push_row(record.values().to_vec())?;
            }
        }

        Ok(result)
    }
}

#[derive(Hash, Eq, PartialEq)]
enum ValueKey {
    Null,
    Bool(bool),
    Int64(i64),
    Float64(u64),
    String(String),
    Bytes(Vec<u8>),
    Other(String),
}

impl From<&Value> for ValueKey {
    fn from(v: &Value) -> Self {
        match v {
            Value::Null => ValueKey::Null,
            Value::Bool(b) => ValueKey::Bool(*b),
            Value::Int64(n) => ValueKey::Int64(*n),
            Value::Float64(f) => ValueKey::Float64(f.0.to_bits()),
            Value::String(s) => ValueKey::String(s.clone()),
            Value::Bytes(b) => ValueKey::Bytes(b.clone()),
            _ => ValueKey::Other(format!("{:?}", v)),
        }
    }
}
