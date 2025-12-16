use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;

use crate::expr::{Expr, SortExpr};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum LogicalPlan {
    Scan {
        table_name: String,
        schema: PlanSchema,
        projection: Option<Vec<usize>>,
    },

    Filter {
        input: Box<LogicalPlan>,
        predicate: Expr,
    },

    Project {
        input: Box<LogicalPlan>,
        expressions: Vec<Expr>,
        schema: PlanSchema,
    },

    Aggregate {
        input: Box<LogicalPlan>,
        group_by: Vec<Expr>,
        aggregates: Vec<Expr>,
        schema: PlanSchema,
    },

    Join {
        left: Box<LogicalPlan>,
        right: Box<LogicalPlan>,
        join_type: JoinType,
        condition: Option<Expr>,
        schema: PlanSchema,
    },

    Sort {
        input: Box<LogicalPlan>,
        sort_exprs: Vec<SortExpr>,
    },

    Limit {
        input: Box<LogicalPlan>,
        limit: Option<usize>,
        offset: Option<usize>,
    },

    Distinct {
        input: Box<LogicalPlan>,
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
        source: Box<LogicalPlan>,
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

    CreateTable {
        table_name: String,
        columns: Vec<ColumnDef>,
        if_not_exists: bool,
        or_replace: bool,
    },

    DropTable {
        table_name: String,
        if_exists: bool,
    },

    AlterTable {
        table_name: String,
        operation: AlterTableOp,
    },

    Truncate {
        table_name: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Assignment {
    pub column: String,
    pub value: Expr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ColumnDef {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterTableOp {
    AddColumn { column: ColumnDef },
    DropColumn { name: String },
    RenameColumn { old_name: String, new_name: String },
    RenameTable { new_name: String },
}

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

    pub fn merge(self, other: PlanSchema) -> Self {
        let mut fields = self.fields;
        fields.extend(other.fields);
        Self { fields }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
    Cross,
}

impl LogicalPlan {
    pub fn schema(&self) -> &PlanSchema {
        match self {
            LogicalPlan::Scan { schema, .. } => schema,
            LogicalPlan::Filter { input, .. } => input.schema(),
            LogicalPlan::Project { schema, .. } => schema,
            LogicalPlan::Aggregate { schema, .. } => schema,
            LogicalPlan::Join { schema, .. } => schema,
            LogicalPlan::Sort { input, .. } => input.schema(),
            LogicalPlan::Limit { input, .. } => input.schema(),
            LogicalPlan::Distinct { input, .. } => input.schema(),
            LogicalPlan::Values { schema, .. } => schema,
            LogicalPlan::Empty { schema } => schema,
            LogicalPlan::Insert { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Update { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Delete { .. } => &EMPTY_SCHEMA,
            LogicalPlan::CreateTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::DropTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::AlterTable { .. } => &EMPTY_SCHEMA,
            LogicalPlan::Truncate { .. } => &EMPTY_SCHEMA,
        }
    }

    pub fn scan(table_name: impl Into<String>, schema: PlanSchema) -> Self {
        LogicalPlan::Scan {
            table_name: table_name.into(),
            schema,
            projection: None,
        }
    }

    pub fn filter(self, predicate: Expr) -> Self {
        LogicalPlan::Filter {
            input: Box::new(self),
            predicate,
        }
    }

    pub fn project(self, expressions: Vec<Expr>, schema: PlanSchema) -> Self {
        LogicalPlan::Project {
            input: Box::new(self),
            expressions,
            schema,
        }
    }

    pub fn sort(self, sort_exprs: Vec<SortExpr>) -> Self {
        LogicalPlan::Sort {
            input: Box::new(self),
            sort_exprs,
        }
    }

    pub fn limit(self, limit: Option<usize>, offset: Option<usize>) -> Self {
        LogicalPlan::Limit {
            input: Box::new(self),
            limit,
            offset,
        }
    }

    pub fn distinct(self) -> Self {
        LogicalPlan::Distinct {
            input: Box::new(self),
        }
    }

    pub fn empty() -> Self {
        LogicalPlan::Empty {
            schema: PlanSchema::new(),
        }
    }
}

static EMPTY_SCHEMA: PlanSchema = PlanSchema { fields: Vec::new() };
