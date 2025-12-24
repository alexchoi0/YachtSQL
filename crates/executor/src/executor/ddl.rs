use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanBuilder, Date32Builder, Float64Builder, Int64Builder, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema};
use arrow::record_batch::RecordBatch;
use chrono::Datelike;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_writer::ArrowWriter;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AlterColumnAction, AlterTableOp, ColumnDef, ExportFormat, ExportOptions, FunctionArg,
    FunctionBody, LoadFormat, LoadOptions, ProcedureArg,
};
use yachtsql_storage::{Field, FieldMode, Schema, Table, TableSchemaOps};

use super::PlanExecutor;
use crate::catalog::{ColumnDefault, UserFunction, UserProcedure};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
        query: Option<&PhysicalPlan>,
    ) -> Result<Table> {
        if self.catalog.get_table(table_name).is_some() {
            if or_replace {
                self.catalog.drop_table(table_name)?;
            } else if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            } else {
                return Err(Error::InvalidQuery(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
        }

        if let Some(query_plan) = query {
            let result = self.execute_plan(query_plan)?;
            let schema = result.schema().clone();
            self.catalog.insert_table(table_name, result)?;
            return Ok(Table::empty(schema));
        }

        let mut schema = Schema::new();
        let mut defaults = Vec::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            schema.add_field(Field::new(&col.name, col.data_type.clone(), mode));
            if let Some(ref default_expr) = col.default_value {
                defaults.push(ColumnDefault {
                    column_name: col.name.clone(),
                    default_expr: default_expr.clone(),
                });
            }
        }

        let table = Table::empty(schema);
        self.catalog.insert_table(table_name, table)?;
        if !defaults.is_empty() {
            self.catalog.set_table_defaults(table_name, defaults);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_tables(
        &mut self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Table> {
        for table_name in table_names {
            if self.catalog.get_table(table_name).is_none() {
                if if_exists {
                    continue;
                }
                return Err(Error::TableNotFound(table_name.to_string()));
            }
            self.catalog.drop_table(table_name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_table(
        &mut self,
        table_name: &str,
        operation: &AlterTableOp,
        if_exists: bool,
    ) -> Result<Table> {
        let table_opt = self.catalog.get_table(table_name);
        if table_opt.is_none() {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        let _table = table_opt.unwrap();

        match operation {
            AlterTableOp::AddColumn {
                column,
                if_not_exists,
            } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

                if *if_not_exists && table.schema().field(&column.name).is_some() {
                    return Ok(Table::empty(Schema::new()));
                }

                let mode = if column.nullable {
                    FieldMode::Nullable
                } else {
                    FieldMode::Required
                };
                let field = Field::new(&column.name, column.data_type.clone(), mode);

                let default_value = match &column.default_value {
                    Some(default_expr) => {
                        let empty_schema = yachtsql_storage::Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema);
                        let empty_record = yachtsql_storage::Record::new();
                        evaluator.evaluate(default_expr, &empty_record).ok()
                    }
                    None => None,
                };

                table.add_column(field, default_value)?;

                if let Some(ref default_expr) = column.default_value {
                    let mut defaults = self
                        .catalog
                        .get_table_defaults(table_name)
                        .cloned()
                        .unwrap_or_default();
                    defaults.push(ColumnDefault {
                        column_name: column.name.clone(),
                        default_expr: default_expr.clone(),
                    });
                    self.catalog.set_table_defaults(table_name, defaults);
                }

                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::DropColumn { name, if_exists } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                if *if_exists && table.schema().field(name).is_none() {
                    return Ok(Table::empty(Schema::new()));
                }
                table.drop_column(name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::RenameColumn { old_name, new_name } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                table.rename_column(old_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::RenameTable { new_name } => {
                self.catalog.rename_table(table_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::SetOptions { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::AlterColumn { name, action } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                match action {
                    AlterColumnAction::SetNotNull => {
                        table.set_column_not_null(name)?;
                    }
                    AlterColumnAction::DropNotNull => {
                        table.set_column_nullable(name)?;
                    }
                    AlterColumnAction::SetDefault { default } => {
                        let empty_schema = yachtsql_storage::Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema);
                        let empty_record = yachtsql_storage::Record::new();
                        let default_value = evaluator.evaluate(default, &empty_record)?;
                        table.set_column_default(name, default_value)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .cloned()
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        defaults.push(ColumnDefault {
                            column_name: name.clone(),
                            default_expr: default.clone(),
                        });
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    AlterColumnAction::DropDefault => {
                        table.drop_column_default(name)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .cloned()
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    AlterColumnAction::SetDataType { data_type } => {
                        table.set_column_data_type(name, data_type.clone())?;
                    }
                }
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::AddConstraint { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::DropConstraint { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::DropPrimaryKey => Ok(Table::empty(Schema::new())),
        }
    }

    pub fn execute_truncate(&mut self, table_name: &str) -> Result<Table> {
        let table = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        table.clear();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_view(
        &mut self,
        name: &str,
        query_sql: &str,
        column_aliases: &[String],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        self.catalog.create_view(
            name,
            query_sql.to_string(),
            column_aliases.to_vec(),
            or_replace,
            if_not_exists,
        )?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        self.catalog.drop_view(name, if_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<Table> {
        self.catalog.create_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_schema(
        &mut self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        self.catalog.drop_schema(name, if_exists, cascade)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_schema(
        &mut self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<Table> {
        let option_map: std::collections::HashMap<String, String> =
            options.iter().cloned().collect();
        self.catalog.alter_schema_options(name, option_map)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_function(
        &mut self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.function_exists(name) {
            return Ok(Table::empty(Schema::new()));
        }
        let func = UserFunction {
            name: name.to_string(),
            parameters: args.to_vec(),
            return_type: return_type.clone(),
            body: body.clone(),
            is_temporary: is_temp,
        };
        self.catalog.create_function(func, or_replace)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_function(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.function_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!("Function not found: {}", name)));
        }
        self.catalog.drop_function(name)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_export(
        &mut self,
        options: &ExportOptions,
        query: &PhysicalPlan,
    ) -> Result<Table> {
        let data = self.execute_plan(query)?;

        let path = if options.uri.starts_with("file://") {
            options.uri.strip_prefix("file://").unwrap().to_string()
        } else if options.uri.starts_with("gs://") {
            let cloud_path = options.uri.strip_prefix("gs://").unwrap();
            cloud_path.replace('*', "data")
        } else if options.uri.starts_with("s3://") {
            let cloud_path = options.uri.strip_prefix("s3://").unwrap();
            cloud_path.replace('*', "data")
        } else {
            options.uri.replace('*', "data")
        };

        match options.format {
            ExportFormat::Parquet => self.export_to_parquet(&data, &path),
            ExportFormat::Json => self.export_to_json(&data, &path),
            ExportFormat::Csv => self.export_to_csv(&data, &path, options),
            ExportFormat::Avro => Err(Error::UnsupportedFeature(
                "AVRO export not yet supported".into(),
            )),
        }
    }

    fn export_to_parquet(&self, data: &Table, path: &str) -> Result<Table> {
        let schema = data.schema();
        let num_rows = data.num_rows();

        let arrow_fields: Vec<ArrowField> = schema
            .fields()
            .iter()
            .map(|f| {
                let arrow_type = Self::data_type_to_arrow(&f.data_type);
                ArrowField::new(&f.name, arrow_type, f.is_nullable())
            })
            .collect();
        let arrow_schema = Arc::new(ArrowSchema::new(arrow_fields));

        let arrays = Self::build_arrow_arrays(data, schema, num_rows)?;

        let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
            .map_err(|e| Error::internal(format!("Failed to create RecordBatch: {}", e)))?;

        let file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;
        let mut writer = ArrowWriter::try_new(file, arrow_schema, None)
            .map_err(|e| Error::internal(format!("Failed to create Parquet writer: {}", e)))?;
        writer
            .write(&batch)
            .map_err(|e| Error::internal(format!("Failed to write Parquet data: {}", e)))?;
        writer
            .close()
            .map_err(|e| Error::internal(format!("Failed to close Parquet writer: {}", e)))?;

        Ok(Table::empty(Schema::new()))
    }

    fn export_to_json(&self, data: &Table, path: &str) -> Result<Table> {
        let schema = data.schema();
        let mut file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;

        for record in data.rows()? {
            let mut obj = serde_json::Map::new();
            for (i, field) in schema.fields().iter().enumerate() {
                let val = &record.values()[i];
                obj.insert(field.name.clone(), Self::value_to_json(val));
            }
            let line = serde_json::to_string(&obj)
                .map_err(|e| Error::internal(format!("JSON serialization error: {}", e)))?;
            writeln!(file, "{}", line)
                .map_err(|e| Error::internal(format!("Failed to write JSON: {}", e)))?;
        }

        Ok(Table::empty(Schema::new()))
    }

    fn export_to_csv(&self, data: &Table, path: &str, options: &ExportOptions) -> Result<Table> {
        let schema = data.schema();
        let delimiter = options
            .field_delimiter
            .as_ref()
            .and_then(|d| d.chars().next())
            .unwrap_or(',');

        let mut file = File::create(path)
            .map_err(|e| Error::internal(format!("Failed to create file '{}': {}", path, e)))?;

        if options.header.unwrap_or(false) {
            let header: Vec<&str> = schema.fields().iter().map(|f| f.name.as_str()).collect();
            writeln!(file, "{}", header.join(&delimiter.to_string()))
                .map_err(|e| Error::internal(format!("Failed to write CSV header: {}", e)))?;
        }

        for record in data.rows()? {
            let values: Vec<String> = record
                .values()
                .iter()
                .map(Self::value_to_csv_string)
                .collect();
            writeln!(file, "{}", values.join(&delimiter.to_string()))
                .map_err(|e| Error::internal(format!("Failed to write CSV row: {}", e)))?;
        }

        Ok(Table::empty(Schema::new()))
    }

    fn value_to_json(val: &Value) -> serde_json::Value {
        match val {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int64(n) => serde_json::Value::Number((*n).into()),
            Value::Float64(f) => serde_json::json!(f.0),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Date(d) => serde_json::Value::String(d.to_string()),
            Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
            Value::Timestamp(ts) => serde_json::Value::String(ts.to_rfc3339()),
            Value::Time(t) => serde_json::Value::String(t.to_string()),
            Value::Numeric(n) => serde_json::Value::String(n.to_string()),
            Value::BigNumeric(n) => serde_json::Value::String(n.to_string()),
            Value::Array(arr) => {
                serde_json::Value::Array(arr.iter().map(Self::value_to_json).collect())
            }
            Value::Struct(fields) => {
                let obj: serde_json::Map<String, serde_json::Value> = fields
                    .iter()
                    .map(|(k, v)| (k.clone(), Self::value_to_json(v)))
                    .collect();
                serde_json::Value::Object(obj)
            }
            Value::Json(j) => j.clone(),
            Value::Bytes(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => serde_json::Value::String(format!("{:?}", val)),
        }
    }

    fn value_to_csv_string(val: &Value) -> String {
        match val {
            Value::Null => String::new(),
            Value::Bool(b) => b.to_string(),
            Value::Int64(n) => n.to_string(),
            Value::Float64(f) => f.0.to_string(),
            Value::String(s) => {
                if s.contains(',') || s.contains('"') || s.contains('\n') {
                    format!("\"{}\"", s.replace('"', "\"\""))
                } else {
                    s.clone()
                }
            }
            Value::Date(d) => d.to_string(),
            Value::DateTime(dt) => dt.to_string(),
            Value::Timestamp(ts) => ts.to_rfc3339(),
            Value::Time(t) => t.to_string(),
            Value::Numeric(n) => n.to_string(),
            Value::BigNumeric(n) => n.to_string(),
            Value::Bytes(_)
            | Value::Json(_)
            | Value::Array(_)
            | Value::Struct(_)
            | Value::Geography(_)
            | Value::Interval(_)
            | Value::Range(_)
            | Value::Default => format!("{:?}", val),
        }
    }

    fn data_type_to_arrow(data_type: &DataType) -> ArrowDataType {
        match data_type {
            DataType::Bool => ArrowDataType::Boolean,
            DataType::Int64 => ArrowDataType::Int64,
            DataType::Float64 => ArrowDataType::Float64,
            DataType::String => ArrowDataType::Utf8,
            DataType::Date => ArrowDataType::Date32,
            DataType::DateTime => {
                ArrowDataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None)
            }
            DataType::Timestamp => ArrowDataType::Timestamp(
                arrow::datatypes::TimeUnit::Microsecond,
                Some("UTC".into()),
            ),
            DataType::Time => ArrowDataType::Time64(arrow::datatypes::TimeUnit::Microsecond),
            DataType::Bytes => ArrowDataType::Binary,
            DataType::Numeric(_) => ArrowDataType::Utf8,
            DataType::Unknown
            | DataType::BigNumeric
            | DataType::Geography
            | DataType::Json
            | DataType::Struct(_)
            | DataType::Array(_)
            | DataType::Interval
            | DataType::Range(_) => ArrowDataType::Utf8,
        }
    }

    fn build_arrow_arrays(
        data: &Table,
        schema: &Schema,
        _num_rows: usize,
    ) -> Result<Vec<ArrayRef>> {
        let mut arrays: Vec<ArrayRef> = Vec::new();

        for (col_idx, field) in schema.fields().iter().enumerate() {
            let array: ArrayRef = match &field.data_type {
                DataType::Bool => {
                    let mut builder = BooleanBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        if let Value::Bool(b) = val {
                            builder.append_value(*b);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Int64 => {
                    let mut builder = Int64Builder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Int64(n) => builder.append_value(*n),
                            Value::Float64(f) => builder.append_value(f.0 as i64),
                            Value::Null
                            | Value::Bool(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::String(_)
                            | Value::Bytes(_)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::DateTime(_)
                            | Value::Timestamp(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Float64 => {
                    let mut builder = Float64Builder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Float64(f) => builder.append_value(f.0),
                            Value::Int64(n) => builder.append_value(*n as f64),
                            Value::Null
                            | Value::Bool(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::String(_)
                            | Value::Bytes(_)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::DateTime(_)
                            | Value::Timestamp(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Date => {
                    let mut builder = Date32Builder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        if let Value::Date(d) = val {
                            let days = d.num_days_from_ce() - 719163;
                            builder.append_value(days);
                        } else {
                            builder.append_null();
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::DateTime | DataType::Timestamp => {
                    let mut builder = TimestampMicrosecondBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Null => builder.append_null(),
                            Value::DateTime(dt) => {
                                let micros = dt.and_utc().timestamp_micros();
                                builder.append_value(micros);
                            }
                            Value::Timestamp(ts) => {
                                builder.append_value(ts.timestamp_micros());
                            }
                            Value::Bool(_)
                            | Value::Int64(_)
                            | Value::Float64(_)
                            | Value::String(_)
                            | Value::Date(_)
                            | Value::Time(_)
                            | Value::Numeric(_)
                            | Value::BigNumeric(_)
                            | Value::Bytes(_)
                            | Value::Json(_)
                            | Value::Array(_)
                            | Value::Struct(_)
                            | Value::Geography(_)
                            | Value::Interval(_)
                            | Value::Range(_)
                            | Value::Default => builder.append_null(),
                        }
                    }
                    Arc::new(builder.finish())
                }
                DataType::Unknown
                | DataType::Numeric(_)
                | DataType::BigNumeric
                | DataType::String
                | DataType::Bytes
                | DataType::Time
                | DataType::Geography
                | DataType::Json
                | DataType::Struct(_)
                | DataType::Array(_)
                | DataType::Interval
                | DataType::Range(_) => {
                    let mut builder = StringBuilder::new();
                    for record in data.rows()? {
                        let val = &record.values()[col_idx];
                        match val {
                            Value::Null => builder.append_null(),
                            Value::Bool(b) => builder.append_value(b.to_string()),
                            Value::Int64(n) => builder.append_value(n.to_string()),
                            Value::Float64(f) => builder.append_value(f.0.to_string()),
                            Value::String(s) => builder.append_value(s),
                            Value::Date(d) => builder.append_value(d.to_string()),
                            Value::DateTime(dt) => builder.append_value(dt.to_string()),
                            Value::Timestamp(ts) => builder.append_value(ts.to_rfc3339()),
                            Value::Time(t) => builder.append_value(t.to_string()),
                            Value::Numeric(n) => builder.append_value(n.to_string()),
                            Value::BigNumeric(n) => builder.append_value(n.to_string()),
                            Value::Bytes(b) => builder.append_value(hex::encode(b)),
                            Value::Json(j) => builder.append_value(j.to_string()),
                            Value::Array(a) => builder.append_value(format!("{:?}", a)),
                            Value::Struct(s) => builder.append_value(format!("{:?}", s)),
                            Value::Geography(g) => builder.append_value(g),
                            Value::Interval(i) => builder.append_value(format!("{:?}", i)),
                            Value::Range(r) => builder.append_value(format!("{:?}", r)),
                            Value::Default => builder.append_value("DEFAULT"),
                        }
                    }
                    Arc::new(builder.finish())
                }
            };
            arrays.push(array);
        }

        Ok(arrays)
    }

    pub fn execute_load(
        &mut self,
        table_name: &str,
        options: &LoadOptions,
        temp_table: bool,
        temp_schema: Option<&Vec<ColumnDef>>,
    ) -> Result<Table> {
        if temp_table && let Some(schema_def) = temp_schema {
            let fields: Vec<Field> = schema_def
                .iter()
                .map(|col| {
                    let mode = if col.nullable {
                        FieldMode::Nullable
                    } else {
                        FieldMode::Required
                    };
                    Field::new(&col.name, col.data_type.clone(), mode)
                })
                .collect();
            let schema = Schema::from_fields(fields);
            let table = Table::empty(schema);
            self.catalog.insert_table(table_name, table)?;
        }

        let table = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        if options.overwrite {
            table.clear();
        }

        let schema = table.schema().clone();

        for uri in &options.uris {
            let (path, is_cloud_uri) = if uri.starts_with("file://") {
                (uri.strip_prefix("file://").unwrap().to_string(), false)
            } else if uri.starts_with("gs://") {
                (
                    uri.strip_prefix("gs://").unwrap().replace('*', "data"),
                    true,
                )
            } else if uri.starts_with("s3://") {
                (
                    uri.strip_prefix("s3://").unwrap().replace('*', "data"),
                    true,
                )
            } else {
                (uri.clone(), false)
            };

            if is_cloud_uri && !std::path::Path::new(&path).exists() {
                continue;
            }

            let rows = match options.format {
                LoadFormat::Parquet => self.load_parquet(&path, &schema)?,
                LoadFormat::Json => self.load_json(&path, &schema)?,
                LoadFormat::Csv => self.load_csv(&path, &schema)?,
                LoadFormat::Avro => {
                    return Err(Error::UnsupportedFeature(
                        "AVRO load not yet supported".into(),
                    ));
                }
            };

            let table = self
                .catalog
                .get_table_mut(table_name)
                .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

            for row in rows {
                table.push_row(row)?;
            }
        }

        Ok(Table::empty(Schema::new()))
    }

    fn load_parquet(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        use arrow::array::Array;

        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;

        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| Error::internal(format!("Failed to read Parquet: {}", e)))?
            .build()
            .map_err(|e| Error::internal(format!("Failed to build Parquet reader: {}", e)))?;

        let mut rows = Vec::new();
        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        for batch_result in reader {
            let batch = batch_result
                .map_err(|e| Error::internal(format!("Failed to read batch: {}", e)))?;

            let parquet_schema = batch.schema();
            let parquet_columns: Vec<String> = parquet_schema
                .fields()
                .iter()
                .map(|f| f.name().to_string())
                .collect();

            let column_mapping: Vec<Option<usize>> = target_columns
                .iter()
                .map(|target_col| {
                    parquet_columns
                        .iter()
                        .position(|pc| pc.eq_ignore_ascii_case(target_col))
                })
                .collect();

            for row_idx in 0..batch.num_rows() {
                let mut row_values = Vec::with_capacity(target_columns.len());

                for (col_idx, parquet_col_idx) in column_mapping.iter().enumerate() {
                    let value = match parquet_col_idx {
                        Some(pci) => {
                            let array = batch.column(*pci);
                            self.arrow_array_to_value(array, row_idx, &target_types[col_idx])?
                        }
                        None => Value::null(),
                    };
                    row_values.push(value);
                }
                rows.push(row_values);
            }
        }

        Ok(rows)
    }

    fn arrow_array_to_value(
        &self,
        array: &ArrayRef,
        row_idx: usize,
        target_type: &DataType,
    ) -> Result<Value> {
        use arrow::array::{Array, AsArray};

        if array.is_null(row_idx) {
            return Ok(Value::null());
        }

        let value = match array.data_type() {
            ArrowDataType::Boolean => {
                let arr = array.as_boolean();
                Value::bool_val(arr.value(row_idx))
            }
            ArrowDataType::Int64 => {
                let arr = array.as_primitive::<arrow::datatypes::Int64Type>();
                Value::int64(arr.value(row_idx))
            }
            ArrowDataType::Int32 => {
                let arr = array.as_primitive::<arrow::datatypes::Int32Type>();
                Value::int64(arr.value(row_idx) as i64)
            }
            ArrowDataType::Float64 => {
                let arr = array.as_primitive::<arrow::datatypes::Float64Type>();
                Value::float64(arr.value(row_idx))
            }
            ArrowDataType::Float32 => {
                let arr = array.as_primitive::<arrow::datatypes::Float32Type>();
                Value::float64(arr.value(row_idx) as f64)
            }
            ArrowDataType::Utf8 => {
                let arr = array.as_string::<i32>();
                Value::string(arr.value(row_idx).to_string())
            }
            ArrowDataType::LargeUtf8 => {
                let arr = array.as_string::<i64>();
                Value::string(arr.value(row_idx).to_string())
            }
            ArrowDataType::Date32 => {
                let arr = array.as_primitive::<arrow::datatypes::Date32Type>();
                let days = arr.value(row_idx);
                let date =
                    chrono::NaiveDate::from_num_days_from_ce_opt(days + 719163).unwrap_or_default();
                Value::date(date)
            }
            ArrowDataType::Date64 => {
                let arr = array.as_primitive::<arrow::datatypes::Date64Type>();
                let millis = arr.value(row_idx);
                let date = chrono::DateTime::from_timestamp_millis(millis)
                    .map(|dt| dt.naive_utc().date())
                    .unwrap_or_default();
                Value::date(date)
            }
            ArrowDataType::Timestamp(_, _) => {
                let arr = array.as_primitive::<arrow::datatypes::TimestampMicrosecondType>();
                let micros = arr.value(row_idx);
                match target_type {
                    DataType::DateTime => {
                        let dt = chrono::DateTime::from_timestamp_micros(micros)
                            .map(|dt| dt.naive_utc())
                            .unwrap_or_default();
                        Value::datetime(dt)
                    }
                    _ => {
                        let ts =
                            chrono::DateTime::from_timestamp_micros(micros).unwrap_or_default();
                        Value::timestamp(ts)
                    }
                }
            }
            _ => Value::string(format!("{:?}", array)),
        };

        Ok(value)
    }

    fn load_json(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
        let reader = BufReader::new(file);

        let target_columns: Vec<String> = schema.fields().iter().map(|f| f.name.clone()).collect();
        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        let mut rows = Vec::new();

        for line in reader.lines() {
            let line =
                line.map_err(|e| Error::internal(format!("Failed to read JSON line: {}", e)))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let obj: HashMap<String, serde_json::Value> = serde_json::from_str(trimmed)
                .map_err(|e| Error::internal(format!("Failed to parse JSON: {}", e)))?;

            let mut row_values = Vec::with_capacity(target_columns.len());

            for (i, col_name) in target_columns.iter().enumerate() {
                let json_val = obj
                    .iter()
                    .find(|(k, _)| k.eq_ignore_ascii_case(col_name))
                    .map(|(_, v)| v);

                let value = match json_val {
                    Some(v) => self.json_to_value(v, &target_types[i])?,
                    None => Value::null(),
                };
                row_values.push(value);
            }

            rows.push(row_values);
        }

        Ok(rows)
    }

    fn json_to_value(&self, json_val: &serde_json::Value, target_type: &DataType) -> Result<Value> {
        match json_val {
            serde_json::Value::Null => Ok(Value::null()),
            serde_json::Value::Bool(b) => Ok(Value::bool_val(*b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    match target_type {
                        DataType::Float64 => Ok(Value::float64(i as f64)),
                        _ => Ok(Value::int64(i)),
                    }
                } else if let Some(f) = n.as_f64() {
                    Ok(Value::float64(f))
                } else {
                    Ok(Value::null())
                }
            }
            serde_json::Value::String(s) => match target_type {
                DataType::Date => {
                    let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                        .map_err(|e| Error::internal(format!("Invalid date: {}", e)))?;
                    Ok(Value::date(date))
                }
                DataType::DateTime => {
                    let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S"))
                        .map_err(|e| Error::internal(format!("Invalid datetime: {}", e)))?;
                    Ok(Value::datetime(dt))
                }
                DataType::Timestamp => {
                    let ts = chrono::DateTime::parse_from_rfc3339(s)
                        .map(|d| d.with_timezone(&chrono::Utc))
                        .map_err(|e| Error::internal(format!("Invalid timestamp: {}", e)))?;
                    Ok(Value::timestamp(ts))
                }
                _ => Ok(Value::string(s.clone())),
            },
            serde_json::Value::Array(arr) => {
                let values: Result<Vec<Value>> = arr
                    .iter()
                    .map(|v| self.json_to_value(v, &DataType::Unknown))
                    .collect();
                Ok(Value::array(values?))
            }
            serde_json::Value::Object(obj) => {
                let fields: Result<Vec<(String, Value)>> = obj
                    .iter()
                    .map(|(k, v)| {
                        let val = self.json_to_value(v, &DataType::Unknown)?;
                        Ok((k.clone(), val))
                    })
                    .collect();
                Ok(Value::Struct(fields?))
            }
        }
    }

    fn load_csv(&self, path: &str, schema: &Schema) -> Result<Vec<Vec<Value>>> {
        let file = File::open(path)
            .map_err(|e| Error::internal(format!("Failed to open file '{}': {}", path, e)))?;
        let reader = BufReader::new(file);

        let target_types: Vec<DataType> = schema
            .fields()
            .iter()
            .map(|f| f.data_type.clone())
            .collect();

        let mut rows = Vec::new();
        let mut lines = reader.lines();

        let _header = lines.next();

        for line in lines {
            let line =
                line.map_err(|e| Error::internal(format!("Failed to read CSV line: {}", e)))?;
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }

            let parts: Vec<&str> = trimmed.split(',').collect();
            let mut row_values = Vec::with_capacity(target_types.len());

            for (i, part) in parts.iter().enumerate() {
                if i >= target_types.len() {
                    break;
                }
                let value = self.csv_string_to_value(part.trim(), &target_types[i])?;
                row_values.push(value);
            }

            while row_values.len() < target_types.len() {
                row_values.push(Value::null());
            }

            rows.push(row_values);
        }

        Ok(rows)
    }

    fn csv_string_to_value(&self, s: &str, target_type: &DataType) -> Result<Value> {
        if s.is_empty() {
            return Ok(Value::null());
        }

        match target_type {
            DataType::Bool => {
                let lower = s.to_lowercase();
                Ok(Value::bool_val(lower == "true" || lower == "1"))
            }
            DataType::Int64 => {
                let n = s
                    .parse::<i64>()
                    .map_err(|e| Error::internal(format!("Invalid int64: {}", e)))?;
                Ok(Value::int64(n))
            }
            DataType::Float64 => {
                let f = s
                    .parse::<f64>()
                    .map_err(|e| Error::internal(format!("Invalid float64: {}", e)))?;
                Ok(Value::float64(f))
            }
            DataType::Date => {
                let date = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                    .map_err(|e| Error::internal(format!("Invalid date: {}", e)))?;
                Ok(Value::date(date))
            }
            _ => Ok(Value::string(s.to_string())),
        }
    }

    pub fn execute_create_snapshot(
        &mut self,
        snapshot_name: &str,
        source_name: &str,
        if_not_exists: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.get_table(snapshot_name).is_some() {
            return Ok(Table::empty(Schema::new()));
        }

        let source_table = self
            .catalog
            .get_table(source_name)
            .ok_or_else(|| Error::TableNotFound(source_name.to_string()))?;

        let snapshot = source_table.clone();
        self.catalog.insert_table(snapshot_name, snapshot)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_snapshot(&mut self, snapshot_name: &str, if_exists: bool) -> Result<Table> {
        if if_exists && self.catalog.get_table(snapshot_name).is_none() {
            return Ok(Table::empty(Schema::new()));
        }

        self.catalog
            .drop_table(snapshot_name)
            .map_err(|_| Error::TableNotFound(snapshot_name.to_string()))?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_procedure(
        &mut self,
        name: &str,
        args: &[ProcedureArg],
        body: &[PhysicalPlan],
        or_replace: bool,
    ) -> Result<Table> {
        let body_plans = body.iter().map(executor_plan_to_logical_plan).collect();
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: body_plans,
        };
        self.catalog.create_procedure(proc, or_replace)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_procedure(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.procedure_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!(
                "Procedure not found: {}",
                name
            )));
        }
        self.catalog.drop_procedure(name)?;
        Ok(Table::empty(Schema::new()))
    }
}

fn executor_plan_to_logical_plan(plan: &PhysicalPlan) -> yachtsql_ir::LogicalPlan {
    use yachtsql_ir::LogicalPlan;
    match plan {
        PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
        } => LogicalPlan::Scan {
            table_name: table_name.clone(),
            schema: schema.clone(),
            projection: projection.clone(),
        },
        PhysicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(executor_plan_to_logical_plan(input)),
            predicate: predicate.clone(),
        },
        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(executor_plan_to_logical_plan(input)),
            expressions: expressions.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            join_type: *join_type,
            condition: condition.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            join_type: yachtsql_ir::JoinType::Cross,
            condition: None,
            schema: schema.clone(),
        },
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
        } => LogicalPlan::Aggregate {
            input: Box::new(executor_plan_to_logical_plan(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            schema: schema.clone(),
            grouping_sets: grouping_sets.clone(),
        },
        PhysicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
            input: Box::new(executor_plan_to_logical_plan(input)),
            sort_exprs: sort_exprs.clone(),
        },
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(executor_plan_to_logical_plan(input)),
            limit: *limit,
            offset: *offset,
        },
        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(executor_plan_to_logical_plan(input)),
                sort_exprs: sort_exprs.clone(),
            }),
            limit: Some(*limit),
            offset: None,
        },
        PhysicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(executor_plan_to_logical_plan(input)),
        },
        PhysicalPlan::Union {
            inputs,
            all,
            schema,
        } => {
            let mut iter = inputs.iter();
            let first = iter.next().map(executor_plan_to_logical_plan);
            iter.fold(first, |acc, p| {
                Some(LogicalPlan::SetOperation {
                    left: Box::new(acc.unwrap()),
                    right: Box::new(executor_plan_to_logical_plan(p)),
                    op: yachtsql_ir::SetOperationType::Union,
                    all: *all,
                    schema: schema.clone(),
                })
            })
            .unwrap_or_else(|| LogicalPlan::Empty {
                schema: schema.clone(),
            })
        }
        PhysicalPlan::Intersect {
            left,
            right,
            all,
            schema,
        } => LogicalPlan::SetOperation {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            op: yachtsql_ir::SetOperationType::Intersect,
            all: *all,
            schema: schema.clone(),
        },
        PhysicalPlan::Except {
            left,
            right,
            all,
            schema,
        } => LogicalPlan::SetOperation {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            op: yachtsql_ir::SetOperationType::Except,
            all: *all,
            schema: schema.clone(),
        },
        PhysicalPlan::Window {
            input,
            window_exprs,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(executor_plan_to_logical_plan(input)),
            window_exprs: window_exprs.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => LogicalPlan::Unnest {
            input: Box::new(executor_plan_to_logical_plan(input)),
            columns: columns.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
            input: Box::new(executor_plan_to_logical_plan(input)),
            predicate: predicate.clone(),
        },
        PhysicalPlan::WithCte { ctes, body } => LogicalPlan::WithCte {
            ctes: ctes.clone(),
            body: Box::new(executor_plan_to_logical_plan(body)),
        },
        PhysicalPlan::Values { values, schema } => LogicalPlan::Values {
            values: values.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Empty { schema } => LogicalPlan::Empty {
            schema: schema.clone(),
        },
        PhysicalPlan::Insert {
            table_name,
            columns,
            source,
        } => LogicalPlan::Insert {
            table_name: table_name.clone(),
            columns: columns.clone(),
            source: Box::new(executor_plan_to_logical_plan(source)),
        },
        PhysicalPlan::Update {
            table_name,
            alias,
            assignments,
            from,
            filter,
        } => LogicalPlan::Update {
            table_name: table_name.clone(),
            alias: alias.clone(),
            assignments: assignments.clone(),
            from: from
                .as_ref()
                .map(|p| Box::new(executor_plan_to_logical_plan(p))),
            filter: filter.clone(),
        },
        PhysicalPlan::Delete { table_name, filter } => LogicalPlan::Delete {
            table_name: table_name.clone(),
            alias: None,
            filter: filter.clone(),
        },
        PhysicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => LogicalPlan::Merge {
            target_table: target_table.clone(),
            source: Box::new(executor_plan_to_logical_plan(source)),
            on: on.clone(),
            clauses: clauses.clone(),
        },
        PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query,
        } => LogicalPlan::CreateTable {
            table_name: table_name.clone(),
            columns: columns.clone(),
            if_not_exists: *if_not_exists,
            or_replace: *or_replace,
            query: query
                .as_ref()
                .map(|q| Box::new(executor_plan_to_logical_plan(q))),
        },
        PhysicalPlan::DropTable {
            table_names,
            if_exists,
        } => LogicalPlan::DropTable {
            table_names: table_names.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        } => LogicalPlan::AlterTable {
            table_name: table_name.clone(),
            operation: operation.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::Truncate { table_name } => LogicalPlan::Truncate {
            table_name: table_name.clone(),
        },
        PhysicalPlan::CreateView {
            name,
            query,
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        } => LogicalPlan::CreateView {
            name: name.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
            query_sql: query_sql.clone(),
            column_aliases: column_aliases.clone(),
            or_replace: *or_replace,
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::DropView { name, if_exists } => LogicalPlan::DropView {
            name: name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::CreateSchema {
            name,
            if_not_exists,
        } => LogicalPlan::CreateSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        } => LogicalPlan::DropSchema {
            name: name.clone(),
            if_exists: *if_exists,
            cascade: *cascade,
        },
        PhysicalPlan::AlterSchema { name, options } => LogicalPlan::AlterSchema {
            name: name.clone(),
            options: options.clone(),
        },
        PhysicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
        } => LogicalPlan::CreateFunction {
            name: name.clone(),
            args: args.clone(),
            return_type: return_type.clone(),
            body: body.clone(),
            or_replace: *or_replace,
            if_not_exists: *if_not_exists,
            is_temp: *is_temp,
        },
        PhysicalPlan::DropFunction { name, if_exists } => LogicalPlan::DropFunction {
            name: name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::CreateProcedure {
            name,
            args,
            body,
            or_replace,
        } => LogicalPlan::CreateProcedure {
            name: name.clone(),
            args: args.clone(),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            or_replace: *or_replace,
        },
        PhysicalPlan::DropProcedure { name, if_exists } => LogicalPlan::DropProcedure {
            name: name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::Call {
            procedure_name,
            args,
        } => LogicalPlan::Call {
            procedure_name: procedure_name.clone(),
            args: args.clone(),
        },
        PhysicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
            options: options.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
        },
        PhysicalPlan::Declare {
            name,
            data_type,
            default,
        } => LogicalPlan::Declare {
            name: name.clone(),
            data_type: data_type.clone(),
            default: default.clone(),
        },
        PhysicalPlan::SetVariable { name, value } => LogicalPlan::SetVariable {
            name: name.clone(),
            value: value.clone(),
        },
        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => LogicalPlan::If {
            condition: condition.clone(),
            then_branch: then_branch
                .iter()
                .map(executor_plan_to_logical_plan)
                .collect(),
            else_branch: else_branch
                .as_ref()
                .map(|b| b.iter().map(executor_plan_to_logical_plan).collect()),
        },
        PhysicalPlan::While { condition, body } => LogicalPlan::While {
            condition: condition.clone(),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
        },
        PhysicalPlan::Loop { body, label } => LogicalPlan::Loop {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            label: label.clone(),
        },
        PhysicalPlan::For {
            variable,
            query,
            body,
        } => LogicalPlan::For {
            variable: variable.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
        },
        PhysicalPlan::Return { value } => LogicalPlan::Return {
            value: value.clone(),
        },
        PhysicalPlan::Raise { message, level } => LogicalPlan::Raise {
            message: message.clone(),
            level: *level,
        },
        PhysicalPlan::Break => LogicalPlan::Break,
        PhysicalPlan::Continue => LogicalPlan::Continue,
        PhysicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        } => LogicalPlan::CreateSnapshot {
            snapshot_name: snapshot_name.clone(),
            source_name: source_name.clone(),
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        } => LogicalPlan::DropSnapshot {
            snapshot_name: snapshot_name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => LogicalPlan::Repeat {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            until_condition: until_condition.clone(),
        },
        PhysicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        } => LogicalPlan::LoadData {
            table_name: table_name.clone(),
            options: options.clone(),
            temp_table: *temp_table,
            temp_schema: temp_schema.clone(),
        },
        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => LogicalPlan::Sample {
            input: Box::new(executor_plan_to_logical_plan(input)),
            sample_type: match sample_type {
                yachtsql_optimizer::SampleType::Rows => yachtsql_ir::SampleType::Rows,
                yachtsql_optimizer::SampleType::Percent => yachtsql_ir::SampleType::Percent,
            },
            sample_value: *sample_value,
        },
        PhysicalPlan::Assert { condition, message } => LogicalPlan::Assert {
            condition: condition.clone(),
            message: message.clone(),
        },
        PhysicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => LogicalPlan::Grant {
            roles: roles.clone(),
            resource_type: resource_type.clone(),
            resource_name: resource_name.clone(),
            grantees: grantees.clone(),
        },
        PhysicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => LogicalPlan::Revoke {
            roles: roles.clone(),
            resource_type: resource_type.clone(),
            resource_name: resource_name.clone(),
            grantees: grantees.clone(),
        },
    }
}
