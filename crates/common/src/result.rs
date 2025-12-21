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
pub struct QueryResult {
    pub schema: Vec<ColumnInfo>,
    pub rows: Vec<Vec<Value>>,
}

impl QueryResult {
    pub fn new(schema: Vec<ColumnInfo>, rows: Vec<Vec<Value>>) -> Self {
        Self { schema, rows }
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

    pub fn first_row(&self) -> Option<&Vec<Value>> {
        self.rows.first()
    }

    pub fn first_value(&self) -> Option<&Value> {
        self.rows.first().and_then(|r| r.first())
    }

    pub fn to_json_rows(&self) -> Vec<Vec<JsonValue>> {
        self.rows
            .iter()
            .map(|row| row.iter().map(|v| v.to_json()).collect())
            .collect()
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
