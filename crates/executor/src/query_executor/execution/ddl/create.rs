use debug_print::debug_eprintln;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType as SqlDataType};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_parser::Sql2023Types;
use yachtsql_storage::{DefaultValue, Field, Schema, TableEngine, TableIndexOps, TableSchemaOps};

fn parse_engine_from_sql(
    sql: &str,
    order_by: Option<&sqlparser::ast::OneOrManyWithParens<sqlparser::ast::Expr>>,
) -> TableEngine {
    let upper = sql.to_uppercase();

    let order_by_cols: Vec<String> = order_by
        .map(|o| match o {
            sqlparser::ast::OneOrManyWithParens::One(expr) => vec![expr.to_string()],
            sqlparser::ast::OneOrManyWithParens::Many(exprs) => {
                exprs.iter().map(|e| e.to_string()).collect()
            }
        })
        .unwrap_or_default();

    if let Some(engine_pos) = upper.find("ENGINE") {
        let after_engine = &sql[engine_pos + 6..].trim_start();
        if let Some(after_eq) = after_engine.strip_prefix('=').or(Some(after_engine)) {
            let after_eq = after_eq.trim_start();

            let engine_with_params: &str = after_eq.split_whitespace().next().unwrap_or("");
            let engine_upper = engine_with_params.to_uppercase();

            if engine_upper.starts_with("SUMMINGMERGETREE") {
                let sum_columns = extract_parenthesized_params(after_eq);
                return TableEngine::SummingMergeTree {
                    order_by: order_by_cols,
                    sum_columns,
                };
            }
            if engine_upper.starts_with("REPLACINGMERGETREE") {
                let params = extract_parenthesized_params(after_eq);
                let version_column = params.first().cloned();
                return TableEngine::ReplacingMergeTree {
                    order_by: order_by_cols,
                    version_column,
                };
            }
            if engine_upper.starts_with("COLLAPSINGMERGETREE") {
                let params = extract_parenthesized_params(after_eq);
                let sign_column = params
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "sign".to_string());
                return TableEngine::CollapsingMergeTree {
                    order_by: order_by_cols,
                    sign_column,
                };
            }
            if engine_upper.starts_with("VERSIONEDCOLLAPSINGMERGETREE") {
                let params = extract_parenthesized_params(after_eq);
                let sign_column = params
                    .first()
                    .cloned()
                    .unwrap_or_else(|| "sign".to_string());
                let version_column = params
                    .get(1)
                    .cloned()
                    .unwrap_or_else(|| "version".to_string());
                return TableEngine::VersionedCollapsingMergeTree {
                    order_by: order_by_cols,
                    sign_column,
                    version_column,
                };
            }
            if engine_upper.starts_with("AGGREGATINGMERGETREE") {
                return TableEngine::AggregatingMergeTree {
                    order_by: order_by_cols,
                };
            }
            if engine_upper.starts_with("MERGETREE") {
                return TableEngine::MergeTree {
                    order_by: order_by_cols,
                };
            }
            if engine_upper.starts_with("DISTRIBUTED") {
                let params = extract_parenthesized_params(after_eq);
                return TableEngine::Distributed {
                    cluster: params.first().cloned().unwrap_or_default(),
                    database: params.get(1).cloned().unwrap_or_default(),
                    table: params.get(2).cloned().unwrap_or_default(),
                    sharding_key: params.get(3).cloned(),
                };
            }
            if engine_upper.starts_with("BUFFER") {
                let params = extract_parenthesized_params(after_eq);
                return TableEngine::Buffer {
                    database: params.first().cloned().unwrap_or_default(),
                    table: params.get(1).cloned().unwrap_or_default(),
                };
            }
            if engine_upper.starts_with("LOG") {
                return TableEngine::Log;
            }
            if engine_upper.starts_with("TINYLOG") {
                return TableEngine::TinyLog;
            }
            if engine_upper.starts_with("STRIPELOG") {
                return TableEngine::StripeLog;
            }
            if engine_upper.starts_with("MEMORY") {
                return TableEngine::Memory;
            }
        }
    }
    TableEngine::Memory
}

fn extract_parenthesized_params(s: &str) -> Vec<String> {
    if let Some(start) = s.find('(') {
        if let Some(end) = s.find(')') {
            let params_str = &s[start + 1..end];
            return params_str
                .split(',')
                .map(|p| p.trim().trim_matches('\'').to_string())
                .filter(|p| !p.is_empty())
                .collect();
        }
    }
    Vec::new()
}

use super::super::QueryExecutor;

#[derive(Debug, Clone, Copy)]
enum SerialType {
    SmallSerial,
    Serial,
    BigSerial,
}

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
        table_name: &str,
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
            self.parse_columns_to_schema(&dataset_id, &table_id, &create_table.columns)?;

        if let Some(ref inherits) = create_table.inherits {
            self.apply_inheritance(&dataset_id, &table_id, &mut schema, inherits)?;
        }

        if schema.fields().is_empty() {
            return Err(Error::InvalidQuery(
                "CREATE TABLE requires at least one column".to_string(),
            ));
        }

        let mut all_constraints = create_table.constraints.clone();
        all_constraints.extend(column_level_fks);

        self.parse_table_constraints(&mut schema, &all_constraints)?;

        let table_full_name = format!("{}.{}", dataset_id, table_id);

        let mut tm = self.transaction_manager.borrow_mut();
        if let Some(_txn) = tm.get_active_transaction_mut() {
            drop(tm);

            {
                let storage = self.storage.borrow_mut();
                if storage.get_dataset(&dataset_id).is_some() {
                    let dataset = storage.get_dataset(&dataset_id).unwrap();
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
                }
            }

            let mut tm = self.transaction_manager.borrow_mut();
            if let Some(txn) = tm.get_active_transaction_mut() {
                txn.track_create_table(table_full_name, schema);
            }
        } else {
            drop(tm);

            let mut storage = self.storage.borrow_mut();

            if storage.get_dataset(&dataset_id).is_none() {
                storage.create_dataset(dataset_id.clone())?;
            }

            let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

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

            let engine = parse_engine_from_sql(original_sql, create_table.order_by.as_ref());
            if let Some(table) = dataset.get_table_mut(&table_id) {
                table.set_engine(engine);
            }

            for field in schema.fields() {
                if let (Some(seq_name), Some(config)) = (
                    &field.identity_sequence_name,
                    &field.identity_sequence_config,
                ) {
                    dataset.sequences_mut().create_sequence(
                        seq_name.clone(),
                        config.clone(),
                        true,
                    )?;
                    dataset.sequences_mut().set_owned_by(
                        seq_name,
                        table_id.clone(),
                        field.name.clone(),
                    )?;
                }
            }

            let child_full_name = format!("{}.{}", dataset_id, table_id);
            for parent_name in schema.parent_tables() {
                let (parent_dataset_id, parent_table_id) =
                    self.parse_ddl_table_name(parent_name)?;
                if let Some(parent_dataset) = storage.get_dataset_mut(&parent_dataset_id) {
                    if let Some(parent_table) = parent_dataset.get_table_mut(&parent_table_id) {
                        parent_table
                            .schema_mut()
                            .add_child_table(child_full_name.clone());
                    }
                }
            }
        }

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
            debug_eprintln!(
                "[executor::ddl::create] Populating materialized view '{}'",
                view_id
            );
            drop(storage);

            let result = self.execute_sql(&query_sql)?;
            let result_schema = result.schema().clone();
            let result_rows: Vec<yachtsql_storage::Row> =
                result.rows().map(|rows| rows.to_vec()).unwrap_or_default();

            debug_eprintln!(
                "[executor::ddl::create] Materialized view '{}' populated with {} rows",
                view_id,
                result_rows.len()
            );

            let mut storage = self.storage.borrow_mut();
            let dataset = storage.get_dataset_mut(&dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id))
            })?;

            let view = dataset.views_mut().get_view_mut(&view_id).ok_or_else(|| {
                Error::InvalidQuery(format!("View '{}.{}' not found", dataset_id, view_id))
            })?;

            view.refresh_materialized_data(result_rows, result_schema);
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
        table_name: &str,
        columns: &[ColumnDef],
    ) -> Result<(Schema, Vec<sqlparser::ast::TableConstraint>)> {
        let mut fields = Vec::new();
        let mut check_constraints = Vec::new();
        let mut column_level_fks = Vec::new();
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

            let serial_type = self.detect_serial_type(&col.data_type);

            let domain_name = self.extract_domain_name(&col.data_type)?;

            let mut is_nullable = true;
            let mut is_unique = false;
            let mut is_primary_key = false;
            let mut generated_expr: Option<(
                String,
                Vec<String>,
                yachtsql_storage::schema::GenerationMode,
            )> = None;
            let mut default_value: Option<DefaultValue> = None;
            let mut identity_info: Option<(
                yachtsql_storage::IdentityGeneration,
                yachtsql_storage::sequence::SequenceConfig,
            )> = None;

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
                    ColumnOption::Generated {
                        generated_as,
                        sequence_options,
                        generation_expr: None,
                        ..
                    } => {
                        use sqlparser::ast::GeneratedAs;

                        let identity_mode = match generated_as {
                            GeneratedAs::Always => yachtsql_storage::IdentityGeneration::Always,
                            GeneratedAs::ByDefault => {
                                yachtsql_storage::IdentityGeneration::ByDefault
                            }
                            GeneratedAs::ExpStored => continue,
                        };

                        let seq_config = parse_sequence_options(sequence_options);
                        identity_info = Some((identity_mode, seq_config));
                    }
                    ColumnOption::ForeignKey {
                        foreign_table,
                        referred_columns,
                        on_delete,
                        on_update,
                        characteristics,
                    } => {
                        use sqlparser::ast::{Ident, TableConstraint};

                        let fk_constraint = TableConstraint::ForeignKey {
                            name: opt.name.clone(),
                            index_name: None,
                            columns: vec![Ident::new(name.clone())],
                            foreign_table: foreign_table.clone(),
                            referred_columns: referred_columns.clone(),
                            on_delete: *on_delete,
                            on_update: *on_update,
                            characteristics: characteristics.clone(),
                        };
                        column_level_fks.push(fk_constraint);
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

            if let Some(ref domain) = domain_name {
                let domain_constraints = self.get_domain_constraints(domain)?;
                for constraint in domain_constraints {
                    let column_constraint = constraint.expression.replace("VALUE", &name);
                    check_constraints.push(yachtsql_storage::CheckConstraint {
                        name: constraint.name.map(|n| format!("{}_{}", name, n)),
                        expression: column_constraint,
                        enforced: constraint.enforced,
                    });
                }

                if self.is_domain_not_null(domain)? {
                    is_nullable = false;
                }
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

            if let Some(domain) = domain_name {
                field = field.with_domain(domain);
            }

            if let Some(serial) = serial_type {
                let seq_name = format!("{}_{}_seq", table_name, field.name);
                field = field.with_identity_by_default(
                    seq_name,
                    Some(yachtsql_storage::sequence::SequenceConfig {
                        start_value: 1,
                        increment: 1,
                        min_value: Some(1),
                        max_value: match serial {
                            SerialType::SmallSerial => Some(i16::MAX as i64),
                            SerialType::Serial => Some(i32::MAX as i64),
                            SerialType::BigSerial => None,
                        },
                        cycle: false,
                        cache: 1,
                    }),
                );
                field.is_auto_increment = true;
            }

            if let Some((identity_mode, seq_config)) = identity_info {
                let seq_name = format!("{}_{}_seq", table_name, field.name);
                field = match identity_mode {
                    yachtsql_storage::IdentityGeneration::Always => {
                        field.with_identity_always(seq_name, Some(seq_config))
                    }
                    yachtsql_storage::IdentityGeneration::ByDefault => {
                        field.with_identity_by_default(seq_name, Some(seq_config))
                    }
                };
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
            SqlDataType::JSON | SqlDataType::JSONB => Ok(DataType::Json),
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
            SqlDataType::GeometricType(kind) => {
                use sqlparser::ast::GeometricTypeKind;
                match kind {
                    GeometricTypeKind::Point => Ok(DataType::Point),
                    GeometricTypeKind::GeometricBox => Ok(DataType::PgBox),
                    GeometricTypeKind::Circle => Ok(DataType::Circle),
                    GeometricTypeKind::Line
                    | GeometricTypeKind::LineSegment
                    | GeometricTypeKind::GeometricPath
                    | GeometricTypeKind::Polygon => Err(Error::unsupported_feature(format!(
                        "Geometric type {:?} not yet supported",
                        kind
                    ))),
                }
            }
            SqlDataType::Custom(name, modifiers) => {
                let type_name = name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .unwrap_or_default();
                let canonical = Sql2023Types::normalize_type_name(&type_name);
                let type_upper = type_name.to_uppercase();

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

                let (domain_dataset_id, domain_name) = if type_name.contains('.') {
                    let parts: Vec<&str> = type_name.splitn(2, '.').collect();
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    ("default".to_string(), type_name.clone())
                };

                let base_type_opt = {
                    let storage = self.storage.borrow_mut();
                    if let Some(dataset) = storage.get_dataset(&domain_dataset_id) {
                        dataset
                            .domains()
                            .get_domain(&domain_name)
                            .map(|d| d.base_type.clone())
                    } else {
                        None
                    }
                };

                if let Some(base_type) = base_type_opt {
                    return self.parse_domain_base_type(&base_type);
                }

                match type_upper.as_str() {
                    "GEOGRAPHY" => Ok(DataType::Geography),
                    "JSON" => Ok(DataType::Json),
                    "HSTORE" => Ok(DataType::Hstore),
                    "MACADDR" => Ok(DataType::MacAddr),
                    "MACADDR8" => Ok(DataType::MacAddr8),
                    "POINT" => Ok(DataType::Point),
                    "BOX" => Ok(DataType::PgBox),
                    "CIRCLE" => Ok(DataType::Circle),
                    "INET" => Ok(DataType::Inet),
                    "CIDR" => Ok(DataType::Cidr),
                    "SERIAL" | "SERIAL4" => Ok(DataType::Int64),
                    "BIGSERIAL" | "SERIAL8" => Ok(DataType::Int64),
                    "SMALLSERIAL" | "SERIAL2" => Ok(DataType::Int64),
                    "IPV4" => Ok(DataType::IPv4),
                    "IPV6" => Ok(DataType::IPv6),
                    "DATE32" => Ok(DataType::Date32),
                    "RING" => Ok(DataType::GeoRing),
                    "POLYGON" => Ok(DataType::GeoPolygon),
                    "MULTIPOLYGON" => Ok(DataType::GeoMultiPolygon),

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

impl QueryExecutor {
    fn detect_serial_type(&self, sql_type: &SqlDataType) -> Option<SerialType> {
        if let SqlDataType::Custom(name, _) = sql_type {
            let type_name = name
                .0
                .last()
                .and_then(|part| part.as_ident())
                .map(|ident| ident.value.to_uppercase())
                .unwrap_or_default();

            match type_name.as_str() {
                "SERIAL" | "SERIAL4" => Some(SerialType::Serial),
                "BIGSERIAL" | "SERIAL8" => Some(SerialType::BigSerial),
                "SMALLSERIAL" | "SERIAL2" => Some(SerialType::SmallSerial),
                _ => None,
            }
        } else {
            None
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

fn expr_to_i64(expr: &sqlparser::ast::Expr) -> Option<i64> {
    use sqlparser::ast::{Expr, Value as SqlValue, ValueWithSpan as SqlValueWithSpan};

    match expr {
        Expr::Value(SqlValueWithSpan {
            value: SqlValue::Number(n, _),
            ..
        }) => n.parse::<i64>().ok(),
        Expr::UnaryOp {
            op: sqlparser::ast::UnaryOperator::Minus,
            expr,
        } => expr_to_i64(expr).map(|v| -v),
        _ => None,
    }
}

fn parse_sequence_options(
    options: &Option<Vec<sqlparser::ast::SequenceOptions>>,
) -> yachtsql_storage::sequence::SequenceConfig {
    use sqlparser::ast::SequenceOptions;

    let mut config = yachtsql_storage::sequence::SequenceConfig::default();

    if let Some(opts) = options {
        for opt in opts {
            match opt {
                SequenceOptions::StartWith(start, _) => {
                    if let Some(value) = expr_to_i64(start) {
                        config.start_value = value;
                    }
                }
                SequenceOptions::IncrementBy(inc, _) => {
                    if let Some(value) = expr_to_i64(inc) {
                        config.increment = value;
                    }
                }
                SequenceOptions::MinValue(Some(min)) => {
                    if let Some(value) = expr_to_i64(min) {
                        config.min_value = Some(value);
                    }
                }
                SequenceOptions::MaxValue(Some(max)) => {
                    if let Some(value) = expr_to_i64(max) {
                        config.max_value = Some(value);
                    }
                }
                SequenceOptions::Cycle(cycle) => {
                    config.cycle = *cycle;
                }
                SequenceOptions::Cache(cache) => {
                    if let Some(value) = expr_to_i64(cache) {
                        config.cache = value as u32;
                    }
                }
                _ => {}
            }
        }
    }

    config
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
                TableConstraint::Unique { columns, .. } => {
                    let col_names = Self::extract_index_column_names(columns)?;

                    for col_name in &col_names {
                        if schema.field(col_name).is_none() {
                            return Err(Error::InvalidQuery(format!(
                                "UNIQUE constraint column '{}' does not exist in table",
                                col_name
                            )));
                        }
                    }

                    schema.add_unique_constraint(col_names);
                }
                TableConstraint::ForeignKey {
                    name,
                    columns,
                    foreign_table,
                    referred_columns,
                    on_delete,
                    on_update,
                    characteristics,
                    ..
                } => {
                    let child_columns: Vec<String> =
                        columns.iter().map(|ident| ident.value.clone()).collect();

                    for col_name in &child_columns {
                        if schema.field(col_name).is_none() {
                            return Err(Error::InvalidQuery(format!(
                                "FOREIGN KEY column '{}' does not exist in table",
                                col_name
                            )));
                        }
                    }

                    let parent_table = foreign_table.to_string();

                    let parent_columns: Vec<String> = referred_columns
                        .iter()
                        .map(|ident| ident.value.clone())
                        .collect();

                    if child_columns.len() != parent_columns.len() {
                        return Err(Error::InvalidQuery(format!(
                            "FOREIGN KEY column count mismatch: {} child columns vs {} parent columns",
                            child_columns.len(),
                            parent_columns.len()
                        )));
                    }

                    let mut foreign_key = yachtsql_storage::ForeignKey::new(
                        child_columns,
                        parent_table,
                        parent_columns,
                    );

                    if let Some(fk_name) = name {
                        foreign_key = foreign_key.with_name(fk_name.to_string());
                    }

                    if let Some(action) = on_delete {
                        foreign_key =
                            foreign_key.with_on_delete(Self::map_referential_action(action)?);
                    }

                    if let Some(action) = on_update {
                        foreign_key =
                            foreign_key.with_on_update(Self::map_referential_action(action)?);
                    }

                    if let Some(chars) = characteristics {
                        if let Some(enforced) = chars.enforced {
                            foreign_key = foreign_key.with_enforced(enforced);
                        }
                        if chars.deferrable == Some(true) {
                            use sqlparser::ast::DeferrableInitial;
                            use yachtsql_storage::Deferrable;
                            let deferrable = match chars.initially {
                                Some(DeferrableInitial::Deferred) => Deferrable::InitiallyDeferred,
                                Some(DeferrableInitial::Immediate) | None => {
                                    Deferrable::InitiallyImmediate
                                }
                            };
                            foreign_key = foreign_key.with_deferrable(deferrable);
                        }
                    }

                    schema.add_foreign_key(foreign_key);
                }
                _ => {}
            }
        }

        Ok(())
    }

    pub(crate) fn map_referential_action(
        action: &sqlparser::ast::ReferentialAction,
    ) -> Result<yachtsql_storage::ReferentialAction> {
        use sqlparser::ast::ReferentialAction as SqlAction;
        use yachtsql_storage::ReferentialAction;

        match action {
            SqlAction::NoAction => Ok(ReferentialAction::NoAction),
            SqlAction::Restrict => Ok(ReferentialAction::Restrict),
            SqlAction::Cascade => Ok(ReferentialAction::Cascade),
            SqlAction::SetNull => Ok(ReferentialAction::SetNull),
            SqlAction::SetDefault => Ok(ReferentialAction::SetDefault),
        }
    }

    fn extract_where_clause(query: &sqlparser::ast::Query) -> Option<String> {
        use sqlparser::ast::SetExpr;

        match query.body.as_ref() {
            SetExpr::Select(select) => select.selection.as_ref().map(|expr| expr.to_string()),
            _ => None,
        }
    }

    fn extract_domain_name(&self, sql_type: &SqlDataType) -> Result<Option<String>> {
        match sql_type {
            SqlDataType::Custom(name, _) => {
                let type_name = name
                    .0
                    .last()
                    .and_then(|part| part.as_ident())
                    .map(|ident| ident.value.clone())
                    .unwrap_or_default();

                let canonical = Sql2023Types::normalize_type_name(&type_name);

                if matches!(canonical.as_str(), "GEOGRAPHY" | "JSON") {
                    return Ok(None);
                }

                let (dataset_id, domain_name) = if type_name.contains('.') {
                    let parts: Vec<&str> = type_name.splitn(2, '.').collect();
                    (parts[0].to_string(), parts[1].to_string())
                } else {
                    ("default".to_string(), type_name.clone())
                };

                let storage = self.storage.borrow_mut();
                if let Some(dataset) = storage.get_dataset(&dataset_id) {
                    if dataset.domains().domain_exists(&domain_name) {
                        return Ok(Some(format!("{}.{}", dataset_id, domain_name)));
                    }
                }

                Ok(None)
            }
            _ => Ok(None),
        }
    }

    fn parse_domain_base_type(&self, base_type: &str) -> Result<DataType> {
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        let sql = format!("CREATE TABLE _temp (_col {})", base_type);

        let dialect = PostgreSqlDialect {};
        let ast = Parser::parse_sql(&dialect, &sql).map_err(|e| {
            Error::invalid_query(format!(
                "Failed to parse domain base type '{}': {}",
                base_type, e
            ))
        })?;

        if let Some(sqlparser::ast::Statement::CreateTable(create_table)) = ast.first() {
            if let Some(col) = create_table.columns.first() {
                return self.sql_type_to_data_type("default", &col.data_type);
            }
        }

        Err(Error::invalid_query(format!(
            "Invalid domain base type: {}",
            base_type
        )))
    }

    fn get_domain_constraints(
        &self,
        domain_full_name: &str,
    ) -> Result<Vec<yachtsql_storage::DomainConstraint>> {
        let (dataset_id, domain_name) = if domain_full_name.contains('.') {
            let parts: Vec<&str> = domain_full_name.splitn(2, '.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            ("default".to_string(), domain_full_name.to_string())
        };

        let storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let domain = dataset
            .domains()
            .get_domain(&domain_name)
            .ok_or_else(|| Error::invalid_query(format!("Domain '{}' not found", domain_name)))?;

        Ok(domain.constraints.clone())
    }

    fn is_domain_not_null(&self, domain_full_name: &str) -> Result<bool> {
        let (dataset_id, domain_name) = if domain_full_name.contains('.') {
            let parts: Vec<&str> = domain_full_name.splitn(2, '.').collect();
            (parts[0].to_string(), parts[1].to_string())
        } else {
            ("default".to_string(), domain_full_name.to_string())
        };

        let storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let domain = dataset
            .domains()
            .get_domain(&domain_name)
            .ok_or_else(|| Error::invalid_query(format!("Domain '{}' not found", domain_name)))?;

        Ok(domain.not_null)
    }

    fn apply_inheritance(
        &self,
        dataset_id: &str,
        _table_id: &str,
        schema: &mut yachtsql_storage::Schema,
        inherits: &[sqlparser::ast::ObjectName],
    ) -> Result<()> {
        let storage = self.storage.borrow();
        let mut all_inherited_fields = Vec::new();
        let mut parent_names = Vec::new();

        for parent_name in inherits {
            let parent_name_str = parent_name.to_string();
            let (parent_dataset_id, parent_table_id) =
                self.parse_ddl_table_name(&parent_name_str)?;

            let parent_dataset_id =
                if parent_dataset_id == "default" && !parent_name_str.contains('.') {
                    dataset_id.to_string()
                } else {
                    parent_dataset_id
                };

            let parent_dataset = storage.get_dataset(&parent_dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Parent dataset '{}' not found", parent_dataset_id))
            })?;

            let parent_table = parent_dataset.get_table(&parent_table_id).ok_or_else(|| {
                Error::InvalidQuery(format!("Parent table '{}' does not exist", parent_name_str))
            })?;

            let parent_schema = parent_table.schema();
            for field in parent_schema.fields() {
                let already_exists = all_inherited_fields
                    .iter()
                    .any(|f: &yachtsql_storage::Field| f.name == field.name)
                    || schema.field(&field.name).is_some();
                if !already_exists {
                    all_inherited_fields.push(field.clone());
                }
            }

            parent_names.push(format!("{}.{}", parent_dataset_id, parent_table_id));
        }

        schema.prepend_inherited_fields(all_inherited_fields);
        schema.set_parent_tables(parent_names);

        Ok(())
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
}
