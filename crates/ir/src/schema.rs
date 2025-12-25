use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;

use crate::expr::Expr;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PlanSchema {
    pub fields: Vec<PlanField>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanField {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub table: Option<String>,
}

impl PlanField {
    pub fn new(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: true,
            table: None,
        }
    }

    pub fn required(name: impl Into<String>, data_type: DataType) -> Self {
        Self {
            name: name.into(),
            data_type,
            nullable: false,
            table: None,
        }
    }

    pub fn with_table(mut self, table: impl Into<String>) -> Self {
        self.table = Some(table.into());
        self
    }
}

impl PlanSchema {
    pub fn new() -> Self {
        Self { fields: Vec::new() }
    }

    pub fn from_fields(fields: Vec<PlanField>) -> Self {
        Self { fields }
    }

    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    pub fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub fn field(&self, name: &str) -> Option<&PlanField> {
        self.fields.iter().find(|f| f.name == name)
    }

    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| {
            f.name.eq_ignore_ascii_case(name) || {
                if let Some(dot_pos) = f.name.rfind('.') {
                    f.name[dot_pos + 1..].eq_ignore_ascii_case(name)
                } else {
                    false
                }
            }
        })
    }

    pub fn field_index_qualified(&self, name: &str, table: Option<&str>) -> Option<usize> {
        match table {
            Some(tbl) => {
                let qualified_name = format!("{}.{}", tbl, name);
                self.fields
                    .iter()
                    .position(|f| f.name.eq_ignore_ascii_case(&qualified_name))
                    .or_else(|| {
                        self.fields.iter().position(|f| {
                            f.name.eq_ignore_ascii_case(name)
                                && f.table
                                    .as_ref()
                                    .is_some_and(|t| t.eq_ignore_ascii_case(tbl))
                        })
                    })
            }
            None => self.field_index(name),
        }
    }

    pub fn field_by_name(&self, name: &str) -> Option<(usize, &PlanField)> {
        self.fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name.eq_ignore_ascii_case(name))
    }

    pub fn merge(self, other: PlanSchema) -> Self {
        let mut fields = self.fields;
        fields.extend(other.fields);
        Self { fields }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub default_value: Option<Expr>,
    pub collation: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

pub static EMPTY_SCHEMA: PlanSchema = PlanSchema { fields: Vec::new() };
