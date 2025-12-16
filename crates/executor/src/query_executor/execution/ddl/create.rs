use debug_print::debug_eprintln;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType as SqlDataType};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_parser::Sql2023Types;
use yachtsql_storage::{
    DefaultValue, Field, Schema, TableEngine, TableIndexOps, TablePartitionInfo,
    TablePartitionStrategy, TableSchemaOps,
};

fn parse_engine_from_sql(
    _sql: &str,
    _order_by: Option<&sqlparser::ast::OneOrManyWithParens<sqlparser::ast::Expr>>,
) -> TableEngine {
    TableEngine::Memory
}

fn parse_partition_strategy_from_sql(sql: &str) -> Option<TablePartitionStrategy> {
    let upper = sql.to_uppercase();

    let partition_by_idx = upper.find("PARTITION BY")?;
    let after_partition_by = &sql[partition_by_idx + 12..].trim_start();
    let after_partition_by_upper = after_partition_by.to_uppercase();

    let strategy_type = if after_partition_by_upper.starts_with("RANGE") {
        "RANGE"
    } else if after_partition_by_upper.starts_with("LIST") {
        "LIST"
    } else if after_partition_by_upper.starts_with("HASH") {
        "HASH"
    } else {
        return None;
    };

    let open_paren = after_partition_by.find('(')?;
    let close_paren = after_partition_by.find(')')?;
    if open_paren >= close_paren {
        return None;
    }

    let columns_str = &after_partition_by[open_paren + 1..close_paren];
    let columns: Vec<String> = columns_str
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();

    if columns.is_empty() {
        return None;
    }

    match strategy_type {
        "RANGE" => Some(TablePartitionStrategy::Range { columns }),
        "LIST" => Some(TablePartitionStrategy::List { columns }),
        "HASH" => Some(TablePartitionStrategy::Hash { columns }),
        _ => None,
    }
}

use super::super::QueryExecutor;

pub trait DdlExecutor {
    fn execute_create_table(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        _original_sql: &str,
    ) -> Result<()>;

    fn execute_create_view(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        original_sql: &str,
    ) -> Result<()>;

    fn execute_create_index(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        _original_sql: &str,
    ) -> Result<()>;

    fn parse_ddl_table_name(&self, table_name: &str) -> Result<(String, String)>;

    fn parse_columns_to_schema(
        &self,
        dataset_id: &str,
        columns: &[ColumnDef],
    ) -> Result<(Schema, Vec<sqlparser::ast::TableConstraint>)>;

    fn sql_type_to_data_type(&self, dataset_id: &str, sql_type: &SqlDataType) -> Result<DataType>;
}

impl DdlExecutor for QueryExecutor {
    fn execute_create_table(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        original_sql: &str,
    ) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::CreateTable(create_table) = stmt else {
            return Err(Error::InternalError(
                "Not a CREATE TABLE statement".to_string(),
            ));
        };

        let table_name = create_table.name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

        let (mut schema, column_level_fks) =
            self.parse_columns_to_schema(&dataset_id, &create_table.columns)?;

        if create_table.inherits.is_some() {
            return Err(Error::unsupported_feature(
                "Table inheritance (INHERITS) is not supported in BigQuery".to_string(),
            ));
        }

        let engine = parse_engine_from_sql(original_sql, create_table.order_by.as_ref());

        let needs_schema_inference = schema.fields().is_empty()
            || (schema.fields().len() == 1 && schema.fields()[0].name == "_dummy");

        debug_eprintln!(
            "[executor::ddl::create] Engine: {:?}, needs_schema_inference: {}, schema fields: {:?}",
            engine,
            needs_schema_inference,
            schema.fields().iter().map(|f| &f.name).collect::<Vec<_>>()
        );

        let _ = needs_schema_inference;

        if schema.fields().is_empty() {
            return Err(Error::InvalidQuery(
                "CREATE TABLE requires at least one column".to_string(),
            ));
        }

        let mut all_constraints = create_table.constraints.clone();
        all_constraints.extend(column_level_fks);

        self.parse_table_constraints(&mut schema, &all_constraints)?;

        if let Some(partition_strategy) = parse_partition_strategy_from_sql(original_sql) {
            let partition_info = TablePartitionInfo {
                parent_table: None,
                bound: None,
                strategy: Some(partition_strategy),
                child_partitions: Vec::new(),
                row_movement_enabled: false,
            };
            schema.set_table_partition(partition_info);
        }

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.clone())?;
        }

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        if dataset.get_table(&table_id).is_some() {
            if create_table.if_not_exists {
                return Ok(());
            } else {
                return Err(Error::InvalidQuery(format!(
                    "Table '{}.{}' already exists",
                    dataset_id, table_id
                )));
            }
        }

        dataset.create_table(table_id.clone(), schema.clone())?;

        if let Some(table) = dataset.get_table_mut(&table_id) {
            table.set_engine(engine.clone());
        }

        drop(storage);
        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }

    fn execute_create_view(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        _original_sql: &str,
    ) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::CreateView {
            name,
            query,
            or_replace,
            materialized,
            to,
            ..
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a CREATE VIEW statement".to_string(),
            ));
        };

        let view_name = name.to_string();
        let (dataset_id, view_id) = self.parse_ddl_table_name(&view_name)?;

        let query_sql = query.to_string();

        let dependencies = Vec::new();

        let where_clause = Self::extract_where_clause(query);

        debug_eprintln!(
            "[executor::ddl::create] Extracted WHERE clause: {:?}",
            where_clause
        );

        let should_populate = true;

        let mut view_def = if *materialized {
            debug_eprintln!(
                "[executor::ddl::create] Creating materialized view '{}'",
                view_id
            );
            yachtsql_storage::ViewDefinition::new_materialized(
                view_id.clone(),
                query_sql.clone(),
                dependencies,
            )
        } else {
            yachtsql_storage::ViewDefinition {
                name: view_id.clone(),
                sql: query_sql.clone(),
                dependencies,
                with_check_option: yachtsql_storage::WithCheckOption::None,
                where_clause: where_clause.clone(),
                materialized: false,
                materialized_data: None,
                materialized_schema: None,
            }
        };

        if !*materialized {
            view_def.where_clause = where_clause;
        }

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.clone())?;
        }

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        if !or_replace && dataset.views().exists(&view_id) {
            return Err(Error::InvalidQuery(format!(
                "View '{}.{}' already exists. Use CREATE OR REPLACE VIEW to replace it.",
                dataset_id, view_id
            )));
        }

        dataset
            .views_mut()
            .create_or_replace_view(view_def)
            .map_err(|e| Error::InvalidQuery(e.to_string()))?;

        if *materialized {
            drop(storage);

            let result = self.execute_sql(&query_sql)?;
            let result_schema = result.schema().clone();

            let mut storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let target_table_name = if let Some(to_table) = to {
                to_table.to_string()
            } else {
                view_id.clone()
            };

            if to.is_none() {
                dataset.create_table(view_id.clone(), result_schema.clone())?;
            }

            if should_populate {
                let result_rows: Vec<yachtsql_storage::Row> =
                    result.rows().map(|rows| rows.to_vec()).unwrap_or_default();

                debug_eprintln!(
                    "[executor::ddl::create] Populating materialized view '{}' with {} rows",
                    view_id,
                    result_rows.len()
                );

                let view = dataset.views_mut().get_view_mut(&view_id).ok_or_else(|| {
                    Error::InvalidQuery(format!("View '{}.{}' not found", dataset_id, view_id))
                })?;

                view.refresh_materialized_data(result_rows.clone(), result_schema.clone());

                if let Some(table) = dataset.get_table_mut(&target_table_name) {
                    for row in result_rows {
                        table.insert_row(row)?;
                    }
                }
            } else {
                let view = dataset.views_mut().get_view_mut(&view_id).ok_or_else(|| {
                    Error::InvalidQuery(format!("View '{}.{}' not found", dataset_id, view_id))
                })?;
                view.refresh_materialized_data(Vec::new(), result_schema.clone());
            }

            let source_tables = Self::extract_source_tables(query);
            debug_eprintln!(
                "[executor::ddl::create] Extracted source tables for MV '{}': {:?}",
                view_id,
                source_tables
            );
            for source_table in &source_tables {
                let (_, source_table_id) = self.parse_ddl_table_name(source_table)?;
                dataset.register_materialized_view_trigger(
                    &source_table_id,
                    &target_table_name,
                    query_sql.clone(),
                );
                debug_eprintln!(
                    "[executor::ddl::create] Registered MV trigger: {} -> {}",
                    source_table_id,
                    target_table_name
                );
            }
        }

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }

    fn execute_create_index(
        &mut self,
        stmt: &sqlparser::ast::Statement,
        _original_sql: &str,
    ) -> Result<()> {
        use sqlparser::ast::Statement;

        let Statement::CreateIndex(create_index) = stmt else {
            return Err(Error::InternalError(
                "Not a CREATE INDEX statement".to_string(),
            ));
        };

        let index_name = create_index
            .name
            .as_ref()
            .ok_or_else(|| Error::InvalidQuery("CREATE INDEX requires an index name".to_string()))?
            .to_string();

        let table_name = create_index.table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

        let mut index_columns = Vec::new();
        for col in &create_index.columns {
            let expr = &col.column.expr;

            let index_col = match expr {
                sqlparser::ast::Expr::Identifier(ident) => {
                    yachtsql_storage::index::IndexColumn::simple(ident.value.clone())
                }
                sqlparser::ast::Expr::CompoundIdentifier(parts) => {
                    if let Some(last) = parts.last() {
                        yachtsql_storage::index::IndexColumn::simple(last.value.clone())
                    } else {
                        return Err(Error::InvalidQuery(
                            "Invalid column identifier in index".to_string(),
                        ));
                    }
                }
                other_expr => yachtsql_storage::index::IndexColumn::expression(other_expr.clone()),
            };
            index_columns.push(index_col);
        }

        let index_type = if let Some(ref using) = create_index.using {
            yachtsql_storage::index::IndexType::from_str(&using.to_string())
                .unwrap_or(yachtsql_storage::index::IndexType::BTree)
        } else {
            yachtsql_storage::index::IndexType::BTree
        };

        let mut metadata = yachtsql_storage::index::IndexMetadata::new(
            index_name.clone(),
            table_id.clone(),
            index_columns,
        )
        .with_index_type(index_type)
        .with_unique(create_index.unique);

        if let Some(predicate) = &create_index.predicate {
            metadata = metadata.with_where_clause(predicate.clone());
        }

        let mut storage = self.storage.borrow_mut();

        let dataset = storage
            .get_dataset(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        if dataset.has_index(&index_name) {
            if create_index.if_not_exists {
                return Ok(());
            } else {
                return Err(Error::InvalidQuery(format!(
                    "Index '{}' already exists",
                    index_name
                )));
            }
        }

        let table = dataset.get_table(&table_id).ok_or_else(|| {
            Error::InvalidQuery(format!("Table '{}.{}' not found", dataset_id, table_id))
        })?;

        metadata.validate(table)?;

        let dataset_mut = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let table_mut = dataset_mut.get_table_mut(&table_id).ok_or_else(|| {
            Error::InvalidQuery(format!("Table '{}.{}' not found", dataset_id, table_id))
        })?;

        table_mut.add_index(metadata.clone())?;

        dataset_mut.create_index(metadata)?;

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }

    fn parse_ddl_table_name(&self, table_name: &str) -> Result<(String, String)> {
        if let Some(dot_pos) = table_name.find('.') {
            let dataset = table_name[..dot_pos].to_string();
            let table = table_name[dot_pos + 1..].to_string();
            Ok((dataset, table))
        } else {
            Ok(("default".to_string(), table_name.to_string()))
        }
    }

    fn parse_columns_to_schema(
        &self,
        dataset_id: &str,
        columns: &[ColumnDef],
    ) -> Result<(Schema, Vec<sqlparser::ast::TableConstraint>)> {
        let mut fields = Vec::new();
        let mut check_constraints = Vec::new();
        let column_level_fks = Vec::new();
        let mut column_names = std::collections::HashSet::new();
        let mut inline_pk_columns = Vec::new();

        for col in columns {
            let name = col.name.value.clone();

            if let SqlDataType::Nested(nested_fields) = &col.data_type {
                for nested_field in nested_fields {
                    let nested_col_name = format!("{}.{}", name, nested_field.name.value);
                    if !column_names.insert(nested_col_name.clone()) {
                        return Err(Error::InvalidQuery(format!(
                            "Duplicate column name '{}' in CREATE TABLE",
                            nested_col_name
                        )));
                    }
                    let inner_type =
                        self.sql_type_to_data_type(dataset_id, &nested_field.data_type)?;
                    let array_type = DataType::Array(Box::new(inner_type));
                    let field = Field::nullable(nested_col_name, array_type);
                    fields.push(field);
                }
                continue;
            }

            if !column_names.insert(name.clone()) {
                return Err(Error::InvalidQuery(format!(
                    "Duplicate column name '{}' in CREATE TABLE",
                    name
                )));
            }
            let data_type = self.sql_type_to_data_type(dataset_id, &col.data_type)?;

            let mut is_nullable = true;
            let mut is_unique = false;
            let mut is_primary_key = false;
            let mut generated_expr: Option<(
                String,
                Vec<String>,
                yachtsql_storage::schema::GenerationMode,
            )> = None;
            let mut default_value: Option<DefaultValue> = None;

            for opt in &col.options {
                match &opt.option {
                    ColumnOption::NotNull => {
                        is_nullable = false;
                    }
                    ColumnOption::Unique { is_primary, .. } => {
                        if *is_primary {
                            is_nullable = false;
                            is_primary_key = true;
                        }
                        is_unique = true;
                    }
                    ColumnOption::Check(expr) => {
                        let constraint_name = opt.name.as_ref().map(|n| n.value.clone());
                        check_constraints.push(yachtsql_storage::CheckConstraint {
                            name: constraint_name,
                            expression: expr.to_string(),
                            enforced: true,
                        });
                    }
                    ColumnOption::Generated {
                        generation_expr: Some(expr),
                        generation_expr_mode,
                        ..
                    } => {
                        let expr_sql = expr.to_string();

                        let mode = match generation_expr_mode {
                            Some(sqlparser::ast::GeneratedExpressionMode::Stored) => {
                                yachtsql_storage::schema::GenerationMode::Stored
                            }
                            Some(sqlparser::ast::GeneratedExpressionMode::Virtual) | None => {
                                yachtsql_storage::schema::GenerationMode::Virtual
                            }
                        };

                        generated_expr = Some((expr_sql, Vec::new(), mode));
                    }
                    ColumnOption::Generated { .. } => {
                        return Err(Error::unsupported_feature(
                            "IDENTITY columns are not supported in BigQuery".to_string(),
                        ));
                    }
                    ColumnOption::ForeignKey { .. } => {
                        return Err(Error::unsupported_feature(
                            "FOREIGN KEY constraints are not supported in BigQuery".to_string(),
                        ));
                    }
                    ColumnOption::Default(expr) => {
                        default_value = Some(parse_column_default(expr)?);
                    }
                    _ => {}
                }
            }

            if is_primary_key {
                inline_pk_columns.push(name.clone());
            }

            let mut field = if is_nullable {
                Field::nullable(name, data_type)
            } else {
                Field::required(name, data_type)
            };

            if is_unique {
                field = field.with_unique();
            }

            if let Some((expr_sql, dependencies, generation_mode)) = generated_expr {
                field = field.with_generated(expr_sql, dependencies, generation_mode);
            }

            if let Some(default) = default_value {
                field = field.with_default(default);
            }

            fields.push(field);
        }

        let mut schema = Schema::from_fields(fields);

        for constraint in check_constraints {
            schema.add_check_constraint(constraint);
        }

        if !inline_pk_columns.is_empty() {
            schema.set_primary_key(inline_pk_columns);
        }

        Ok((schema, column_level_fks))
    }

    fn sql_type_to_data_type(&self, dataset_id: &str, sql_type: &SqlDataType) -> Result<DataType> {
        match sql_type {
            SqlDataType::Int64
            | SqlDataType::Int(_)
            | SqlDataType::Integer(_)
            | SqlDataType::BigInt(_)
            | SqlDataType::TinyInt(_)
            | SqlDataType::SmallInt(_)
            | SqlDataType::Int8(_)
            | SqlDataType::Int16
            | SqlDataType::Int32
            | SqlDataType::Int128
            | SqlDataType::Int256
            | SqlDataType::UInt8
            | SqlDataType::UInt16
            | SqlDataType::UInt32
            | SqlDataType::UInt64
            | SqlDataType::UInt128
            | SqlDataType::UInt256 => Ok(DataType::Int64),
            SqlDataType::Float32 => Ok(DataType::Float32),
            SqlDataType::Float64
            | SqlDataType::Float(_)
            | SqlDataType::Real
            | SqlDataType::Double(_)
            | SqlDataType::DoublePrecision => Ok(DataType::Float64),
            SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Bool),
            SqlDataType::String(_)
            | SqlDataType::Varchar(_)
            | SqlDataType::Char(_)
            | SqlDataType::Text => Ok(DataType::String),
            SqlDataType::FixedString(n) => Ok(DataType::FixedString(*n as usize)),
            SqlDataType::Bytea | SqlDataType::Bytes(_) => Ok(DataType::Bytes),
            SqlDataType::Bit(_) | SqlDataType::BitVarying(_) => Ok(DataType::Bytes),
            SqlDataType::Date => Ok(DataType::Date),
            SqlDataType::Date32 => Ok(DataType::Date32),
            SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp),
            SqlDataType::Datetime(_) | SqlDataType::Datetime64(_, _) => Ok(DataType::DateTime),
            SqlDataType::Decimal(info) | SqlDataType::Numeric(info) => {
                use sqlparser::ast::ExactNumberInfo;
                let precision_scale = match info {
                    ExactNumberInfo::PrecisionAndScale(p, s) => Some((*p as u8, *s as u8)),
                    ExactNumberInfo::Precision(p) => Some((*p as u8, 0)),
                    ExactNumberInfo::None => None,
                };
                Ok(DataType::Numeric(precision_scale))
            }
            SqlDataType::Array(inner_def) => {
                use sqlparser::ast::ArrayElemTypeDef;
                let inner_type = match inner_def {
                    ArrayElemTypeDef::AngleBracket(inner) => inner,
                    ArrayElemTypeDef::SquareBracket(inner, _) => inner,
                    ArrayElemTypeDef::Parenthesis(inner) => inner,
                    ArrayElemTypeDef::None => {
                        return Err(Error::InvalidQuery(
                            "ARRAY type requires element type".to_string(),
                        ));
                    }
                };
                let inner_data_type = self.sql_type_to_data_type(dataset_id, inner_type)?;
                Ok(DataType::Array(Box::new(inner_data_type)))
            }
            SqlDataType::Map(key_type, value_type) => {
                let key_data_type = self.sql_type_to_data_type(dataset_id, key_type)?;
                let value_data_type = self.sql_type_to_data_type(dataset_id, value_type)?;
                Ok(DataType::Map(
                    Box::new(key_data_type),
                    Box::new(value_data_type),
                ))
            }
            SqlDataType::JSON => Ok(DataType::Json),
            SqlDataType::Uuid => Ok(DataType::Uuid),
            SqlDataType::Nullable(inner) => self.sql_type_to_data_type(dataset_id, inner),
            SqlDataType::LowCardinality(inner) => self.sql_type_to_data_type(dataset_id, inner),
            SqlDataType::Nested(fields) => {
                let struct_fields: Vec<yachtsql_core::types::StructField> = fields
                    .iter()
                    .map(|col| {
                        let dt = self
                            .sql_type_to_data_type(dataset_id, &col.data_type)
                            .unwrap_or(DataType::String);
                        yachtsql_core::types::StructField {
                            name: col.name.value.clone(),
                            data_type: DataType::Array(Box::new(dt)),
                        }
                    })
                    .collect();
                Ok(DataType::Struct(struct_fields))
            }
            SqlDataType::Tuple(fields) => {
                let struct_fields: Vec<yachtsql_core::types::StructField> = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let dt = self
                            .sql_type_to_data_type(dataset_id, &field.field_type)
                            .unwrap_or(DataType::String);
                        let name = field
                            .field_name
                            .as_ref()
                            .map(|ident| ident.value.clone())
                            .unwrap_or_else(|| (idx + 1).to_string());
                        yachtsql_core::types::StructField {
                            name,
                            data_type: dt,
                        }
                    })
                    .collect();
                Ok(DataType::Struct(struct_fields))
            }
            SqlDataType::Struct(fields, _bracket_style) => {
                let struct_fields: Vec<yachtsql_core::types::StructField> = fields
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| {
                        let dt = self
                            .sql_type_to_data_type(dataset_id, &field.field_type)
                            .unwrap_or(DataType::String);
                        let name = field
                            .field_name
                            .as_ref()
                            .map(|ident| ident.value.clone())
                            .unwrap_or_else(|| (idx + 1).to_string());
                        yachtsql_core::types::StructField {
                            name,
                            data_type: dt,
                        }
                    })
                    .collect();
                Ok(DataType::Struct(struct_fields))
            }
            SqlDataType::Enum(members, _bits) => {
                use sqlparser::ast::EnumMember;
                let labels: Vec<String> = members
                    .iter()
                    .map(|m| match m {
                        EnumMember::Name(name) => name.clone(),
                        EnumMember::NamedValue(name, _) => name.clone(),
                    })
                    .collect();
                Ok(DataType::Enum {
                    type_name: String::new(),
                    labels,
                })
            }
            SqlDataType::Interval { .. } => Ok(DataType::Interval),
            SqlDataType::GeometricType(kind) => Err(Error::unsupported_feature(format!(
                "Geometric type {:?} is not supported in BigQuery",
                kind
            ))),
            SqlDataType::Custom(name, modifiers) => {
                let type_name = name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .unwrap_or_default();
                let canonical = Sql2023Types::normalize_type_name(&type_name);
                let type_upper = type_name.to_uppercase();

                let _ = canonical;

                if type_upper == "VECTOR" {
                    let dims = modifiers
                        .first()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(0);
                    return Ok(DataType::Vector(dims));
                }

                if type_upper == "DECIMAL32" {
                    let scale = modifiers
                        .first()
                        .and_then(|s| s.parse::<u8>().ok())
                        .unwrap_or(0);
                    return Ok(DataType::Numeric(Some((9, scale))));
                }

                if type_upper == "DECIMAL64" {
                    let scale = modifiers
                        .first()
                        .and_then(|s| s.parse::<u8>().ok())
                        .unwrap_or(0);
                    return Ok(DataType::Numeric(Some((18, scale))));
                }

                if type_upper == "DECIMAL128" {
                    let scale = modifiers
                        .first()
                        .and_then(|s| s.parse::<u8>().ok())
                        .unwrap_or(0);
                    return Ok(DataType::Numeric(Some((38, scale))));
                }

                if type_upper == "DATETIME64" {
                    return Ok(DataType::Timestamp);
                }

                if type_upper == "FIXEDSTRING" {
                    let n = modifiers
                        .first()
                        .and_then(|s| s.parse::<usize>().ok())
                        .unwrap_or(1);
                    return Ok(DataType::FixedString(n));
                }

                {
                    let storage = self.storage.borrow_mut();
                    if let Some(dataset) = storage.get_dataset(dataset_id) {
                        if let Some(enum_type) = dataset.types().get_enum(&type_name) {
                            return Ok(DataType::Enum {
                                type_name,
                                labels: enum_type.labels.to_vec(),
                            });
                        }

                        if dataset.types().get_type(&type_name).is_some() {
                            return Ok(DataType::Custom(type_name));
                        }
                    }
                }

                match type_upper.as_str() {
                    "GEOGRAPHY" => Ok(DataType::Geography),
                    "JSON" => Ok(DataType::Json),
                    "IPV4" => Ok(DataType::IPv4),
                    "IPV6" => Ok(DataType::IPv6),
                    "DATE32" => Ok(DataType::Date32),
                    _ => Ok(DataType::Custom(type_name)),
                }
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Unsupported data type: {:?}",
                sql_type
            ))),
        }
    }
}

fn parse_column_default(expr: &sqlparser::ast::Expr) -> Result<DefaultValue> {
    use sqlparser::ast::{Expr, Value as SqlValue, ValueWithSpan as SqlValueWithSpan};

    match expr {
        Expr::Value(SqlValueWithSpan {
            value: SqlValue::Number(n, _),
            ..
        }) => {
            if let Ok(i) = n.parse::<i64>() {
                Ok(DefaultValue::Literal(Value::int64(i)))
            } else if let Ok(f) = n.parse::<f64>() {
                Ok(DefaultValue::Literal(Value::float64(f)))
            } else {
                Err(Error::invalid_query(format!(
                    "Invalid numeric literal in DEFAULT clause: {}",
                    n
                )))
            }
        }
        Expr::Value(SqlValueWithSpan {
            value: SqlValue::SingleQuotedString(s),
            ..
        })
        | Expr::Value(SqlValueWithSpan {
            value: SqlValue::DoubleQuotedString(s),
            ..
        }) => Ok(DefaultValue::Literal(Value::string(s.clone()))),
        Expr::Value(SqlValueWithSpan {
            value: SqlValue::Boolean(b),
            ..
        }) => Ok(DefaultValue::Literal(Value::bool_val(*b))),
        Expr::Value(SqlValueWithSpan {
            value: SqlValue::Null,
            ..
        }) => Ok(DefaultValue::Literal(Value::null())),
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("CURRENT_TIMESTAMP") => {
            Ok(DefaultValue::CurrentTimestamp)
        }
        Expr::Identifier(ident) if ident.value.eq_ignore_ascii_case("CURRENT_DATE") => {
            Ok(DefaultValue::CurrentDate)
        }
        Expr::Function(func) => {
            let name = func.name.to_string();
            if name.eq_ignore_ascii_case("CURRENT_TIMESTAMP") {
                Ok(DefaultValue::CurrentTimestamp)
            } else if name.eq_ignore_ascii_case("CURRENT_DATE") {
                Ok(DefaultValue::CurrentDate)
            } else {
                Err(Error::unsupported_feature(format!(
                    "DEFAULT expression function '{}' not supported",
                    name
                )))
            }
        }
        _ => Err(Error::unsupported_feature(format!(
            "DEFAULT expression {:?} not supported",
            expr
        ))),
    }
}

impl QueryExecutor {
    fn index_column_to_name(column: &sqlparser::ast::IndexColumn) -> Result<String> {
        match &column.column.expr {
            sqlparser::ast::Expr::Identifier(ident) => Ok(ident.value.clone()),
            sqlparser::ast::Expr::CompoundIdentifier(parts) => parts
                .last()
                .map(|ident| ident.value.clone())
                .ok_or_else(|| {
                    Error::invalid_query(
                        "Invalid compound identifier in table constraint".to_string(),
                    )
                }),
            other => Err(Error::unsupported_feature(format!(
                "Expression-based columns in table constraints are not supported: {}",
                other
            ))),
        }
    }

    fn extract_index_column_names(columns: &[sqlparser::ast::IndexColumn]) -> Result<Vec<String>> {
        columns.iter().map(Self::index_column_to_name).collect()
    }

    fn parse_table_constraints(
        &self,
        schema: &mut yachtsql_storage::Schema,
        constraints: &[sqlparser::ast::TableConstraint],
    ) -> Result<()> {
        use sqlparser::ast::TableConstraint;

        for constraint in constraints {
            match constraint {
                TableConstraint::Check {
                    name,
                    expr,
                    enforced,
                } => {
                    let constraint_name = name.as_ref().map(|n| n.value.clone());
                    schema.add_check_constraint(yachtsql_storage::CheckConstraint {
                        name: constraint_name,
                        expression: expr.to_string(),
                        enforced: enforced.unwrap_or(true),
                    });
                }
                TableConstraint::PrimaryKey { columns, .. } => {
                    let col_names = Self::extract_index_column_names(columns)?;

                    for col_name in &col_names {
                        if schema.field(col_name).is_none() {
                            return Err(Error::InvalidQuery(format!(
                                "PRIMARY KEY column '{}' does not exist in table",
                                col_name
                            )));
                        }
                    }

                    schema.set_primary_key(col_names);
                }
                TableConstraint::Unique {
                    columns,
                    name,
                    characteristics,
                    nulls_distinct,
                    ..
                } => {
                    let col_names = Self::extract_index_column_names(columns)?;

                    for col_name in &col_names {
                        if schema.field(col_name).is_none() {
                            return Err(Error::InvalidQuery(format!(
                                "UNIQUE constraint column '{}' does not exist in table",
                                col_name
                            )));
                        }
                    }

                    let enforced = characteristics
                        .as_ref()
                        .and_then(|c| c.enforced)
                        .unwrap_or(true);
                    let is_nulls_distinct =
                        *nulls_distinct != sqlparser::ast::NullsDistinctOption::NotDistinct;

                    schema.add_unique_constraint(yachtsql_storage::schema::UniqueConstraint {
                        name: name.as_ref().map(|n| n.to_string()),
                        columns: col_names,
                        enforced,
                        nulls_distinct: is_nulls_distinct,
                    });
                }
                TableConstraint::ForeignKey { .. } => {
                    return Err(Error::unsupported_feature(
                        "FOREIGN KEY constraints are not supported in BigQuery".to_string(),
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn extract_where_clause(query: &sqlparser::ast::Query) -> Option<String> {
        use sqlparser::ast::SetExpr;

        match query.body.as_ref() {
            SetExpr::Select(select) => select.selection.as_ref().map(|expr| expr.to_string()),
            _ => None,
        }
    }

    fn extract_source_tables(query: &sqlparser::ast::Query) -> Vec<String> {
        use sqlparser::ast::{SetExpr, TableFactor};

        let mut tables = Vec::new();

        if let SetExpr::Select(select) = query.body.as_ref() {
            for table_with_joins in &select.from {
                match &table_with_joins.relation {
                    TableFactor::Table { name, .. } => {
                        tables.push(name.to_string());
                    }
                    _ => {}
                }

                for join in &table_with_joins.joins {
                    if let TableFactor::Table { name, .. } = &join.relation {
                        tables.push(name.to_string());
                    }
                }
            }
        }

        tables
    }
}

impl QueryExecutor {
    pub fn execute_create_table_as(
        &mut self,
        new_table: &str,
        source_table: &str,
        engine_clause: &str,
    ) -> Result<crate::Table> {
        let (new_dataset_id, new_table_id) = self.parse_ddl_table_name(new_table)?;
        let (source_dataset_id, source_table_id) = self.parse_ddl_table_name(source_table)?;

        let source_schema = {
            let storage = self.storage.borrow();
            let dataset = storage.get_dataset(&source_dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", source_dataset_id))
            })?;
            let table = dataset
                .get_table(&source_table_id)
                .ok_or_else(|| Error::table_not_found(&source_table_id))?;
            table.schema().clone()
        };

        {
            let mut storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset_mut(&new_dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", new_dataset_id))
            })?;

            if dataset.get_table(&new_table_id).is_some() {
                return Err(Error::InvalidQuery(format!(
                    "Table '{}.{}' already exists",
                    new_dataset_id, new_table_id
                )));
            }

            dataset.create_table(new_table_id.clone(), source_schema)?;

            let engine = parse_engine_from_sql(engine_clause, None);
            if let Some(table) = dataset.get_table_mut(&new_table_id) {
                table.set_engine(engine);
            }
        }

        self.plan_cache.borrow_mut().invalidate_all();

        Ok(crate::Table::empty(yachtsql_storage::Schema::from_fields(
            vec![],
        )))
    }

    pub fn execute_comment_on(&mut self, stmt: &sqlparser::ast::Statement) -> Result<()> {
        use sqlparser::ast::{CommentObject, Statement};

        let Statement::Comment {
            object_type,
            object_name,
            comment,
            if_exists,
        } = stmt
        else {
            return Err(Error::InternalError("Not a COMMENT statement".to_string()));
        };

        match object_type {
            CommentObject::Table => {
                let table_name = object_name.to_string();
                let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

                let mut storage = self.storage.borrow_mut();
                if let Some(dataset) = storage.get_dataset_mut(&dataset_id) {
                    if let Some(table) = dataset.get_table_mut(&table_id) {
                        table.set_comment(comment.clone());
                        Ok(())
                    } else if *if_exists {
                        Ok(())
                    } else {
                        Err(Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        )))
                    }
                } else if *if_exists {
                    Ok(())
                } else {
                    Err(Error::table_not_found(format!(
                        "Schema '{}' not found",
                        dataset_id
                    )))
                }
            }
            CommentObject::Column => {
                let full_name = object_name.to_string();
                let parts: Vec<&str> = full_name.split('.').collect();

                let (table_name, column_name) = match parts.len() {
                    2 => (parts[0].to_string(), parts[1]),
                    3 => (format!("{}.{}", parts[0], parts[1]), parts[2]),
                    _ => {
                        return Err(Error::InvalidQuery(
                            "Invalid column reference for COMMENT ON COLUMN".to_string(),
                        ));
                    }
                };

                let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

                let mut storage = self.storage.borrow_mut();
                if let Some(dataset) = storage.get_dataset_mut(&dataset_id) {
                    if let Some(table) = dataset.get_table_mut(&table_id) {
                        let schema = table.schema_mut();
                        if let Some(field) = schema
                            .fields_mut()
                            .iter_mut()
                            .find(|f| f.name == column_name)
                        {
                            field.description = comment.clone();
                            Ok(())
                        } else if *if_exists {
                            Ok(())
                        } else {
                            Err(Error::column_not_found(format!(
                                "Column '{}' not found in table '{}.{}'",
                                column_name, dataset_id, table_id
                            )))
                        }
                    } else if *if_exists {
                        Ok(())
                    } else {
                        Err(Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        )))
                    }
                } else if *if_exists {
                    Ok(())
                } else {
                    Err(Error::table_not_found(format!(
                        "Schema '{}' not found",
                        dataset_id
                    )))
                }
            }
            _ => Ok(()),
        }
    }
}
