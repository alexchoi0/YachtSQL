use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

use crate::types::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnInfo {
    pub name: String,
    #[serde(rename = "type")]
    pub data_type: String,
}

impl ColumnInfo {
    pub fn new(name: impl Into<String>, data_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            data_type: data_type.into(),
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    pub fn values(&self) -> &[Value] {
        &self.values
    }

    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Value> {
        self.values.iter()
    }

    pub fn to_json(&self) -> Vec<JsonValue> {
        self.values.iter().map(|v| v.to_json()).collect()
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl<'a> IntoIterator for &'a Row {
    type Item = &'a Value;
    type IntoIter = std::slice::Iter<'a, Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.iter()
    }
}

impl IntoIterator for Row {
    type Item = Value;
    type IntoIter = std::vec::IntoIter<Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.values.into_iter()
    }
}

#[derive(Debug, Clone, Default)]
pub struct QueryResult {
    pub schema: Vec<ColumnInfo>,
    pub rows: Vec<Row>,
}

impl QueryResult {
    pub fn new(schema: Vec<ColumnInfo>, rows: Vec<Row>) -> Self {
        Self { schema, rows }
    }

    pub fn from_values(schema: Vec<ColumnInfo>, rows: Vec<Vec<Value>>) -> Self {
        Self {
            schema,
            rows: rows.into_iter().map(Row::new).collect(),
        }
    }

    pub fn empty() -> Self {
        Self::default()
    }

    pub fn with_schema(schema: Vec<ColumnInfo>) -> Self {
        Self {
            schema,
            rows: Vec::new(),
        }
    }

    pub fn row_count(&self) -> usize {
        self.rows.len()
    }

    pub fn column_count(&self) -> usize {
        self.schema.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    pub fn column_names(&self) -> Vec<&str> {
        self.schema.iter().map(|c| c.name.as_str()).collect()
    }

    pub fn get(&self, row: usize, col: usize) -> Option<&Value> {
        self.rows.get(row).and_then(|r| r.get(col))
    }

    pub fn get_by_name(&self, row: usize, col_name: &str) -> Option<&Value> {
        let col_idx = self.schema.iter().position(|c| c.name == col_name)?;
        self.get(row, col_idx)
    }

    pub fn first_row(&self) -> Option<&Row> {
        self.rows.first()
    }

    pub fn first_value(&self) -> Option<&Value> {
        self.rows.first().and_then(|r| r.get(0))
    }

    pub fn to_json_rows(&self) -> Vec<Vec<JsonValue>> {
        self.rows.iter().map(|row| row.to_json()).collect()
    }

    pub fn to_bq_response(&self) -> JsonValue {
        let schema_fields: Vec<JsonValue> = self
            .schema
            .iter()
            .map(|col| serde_json::json!({ "name": col.name, "type": col.data_type }))
            .collect();

        let rows: Vec<JsonValue> = self
            .rows
            .iter()
            .map(|row| {
                let fields: Vec<JsonValue> = row
                    .iter()
                    .map(|v| serde_json::json!({ "v": v.to_json() }))
                    .collect();
                serde_json::json!({ "f": fields })
            })
            .collect();

        serde_json::json!({
            "kind": "bigquery#queryResponse",
            "schema": { "fields": schema_fields },
            "rows": rows,
            "totalRows": self.rows.len().to_string(),
            "jobComplete": true
        })
    }
}
