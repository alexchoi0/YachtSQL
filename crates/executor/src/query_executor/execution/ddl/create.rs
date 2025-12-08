use debug_print::debug_eprintln;
use sqlparser::ast::{ColumnDef, ColumnOption, DataType as SqlDataType};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_parser::Sql2023Types;
use yachtsql_storage::{DefaultValue, Field, Schema, TableIndexOps};

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
        _original_sql: &str,
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

        if fields.is_empty() {
            return Err(Error::InvalidQuery(
                "CREATE TABLE requires at least one column".to_string(),
            ));
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
            SqlDataType::Bytea | SqlDataType::Bytes(_) => Ok(DataType::Bytes),
            SqlDataType::Bit(_) | SqlDataType::BitVarying(_) => Ok(DataType::Bytes),
            SqlDataType::Date => Ok(DataType::Date),
            SqlDataType::Timestamp(_, _) => Ok(DataType::Timestamp),
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

                match canonical.as_str() {
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
}
