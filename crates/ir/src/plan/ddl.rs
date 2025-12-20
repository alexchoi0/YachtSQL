use serde::{Deserialize, Serialize};
use yachtsql_common::types::DataType;

use crate::expr::Expr;
use crate::schema::ColumnDef;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterTableOp {
    AddColumn {
        column: ColumnDef,
    },
    DropColumn {
        name: String,
    },
    RenameColumn {
        old_name: String,
        new_name: String,
    },
    RenameTable {
        new_name: String,
    },
    SetOptions {
        options: Vec<(String, String)>,
    },
    AlterColumn {
        name: String,
        action: AlterColumnAction,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum AlterColumnAction {
    SetDataType { data_type: DataType },
    SetDefault { default: Expr },
    DropDefault,
    SetNotNull,
    DropNotNull,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionArg {
    pub name: String,
    pub data_type: DataType,
    pub default: Option<Expr>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum FunctionBody {
    Sql(Box<Expr>),
    JavaScript(String),
    Language { name: String, code: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProcedureArg {
    pub name: String,
    pub data_type: DataType,
    pub mode: ProcedureArgMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
pub enum ProcedureArgMode {
    #[default]
    In,
    Out,
    InOut,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExportOptions {
    pub uri: String,
    pub format: ExportFormat,
    pub compression: Option<String>,
    pub field_delimiter: Option<String>,
    pub header: Option<bool>,
    pub overwrite: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExportFormat {
    Csv,
    Json,
    Avro,
    Parquet,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LoadOptions {
    pub uris: Vec<String>,
    pub format: LoadFormat,
    pub overwrite: bool,
    pub allow_schema_update: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LoadFormat {
    Csv,
    Json,
    Parquet,
    Avro,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TableConstraint {
    pub name: Option<String>,
    pub constraint_type: ConstraintType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ConstraintType {
    PrimaryKey {
        columns: Vec<String>,
    },
    ForeignKey {
        columns: Vec<String>,
        references_table: String,
        references_columns: Vec<String>,
    },
    Unique {
        columns: Vec<String>,
    },
    Check {
        expr: Expr,
    },
}
