use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, MultirangeType, Value};
use yachtsql_parser::DialectType;
use yachtsql_storage::{Field, Row, Schema, Storage, TableIndexOps};

use crate::table::Table;

pub fn data_type_to_postgres_name(data_type: &DataType) -> String {
    use yachtsql_core::types::RangeType;
    match data_type {
        DataType::Int64 => "BIGINT".to_string(),
        DataType::Float32 => "REAL".to_string(),
        DataType::Float64 => "DOUBLE PRECISION".to_string(),
        DataType::String => "TEXT".to_string(),
        DataType::Bool => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::DateTime => "TIMESTAMP WITHOUT TIME ZONE".to_string(),
        DataType::Timestamp => "TIMESTAMP WITHOUT TIME ZONE".to_string(),
        DataType::TimestampTz => "TIMESTAMP WITH TIME ZONE".to_string(),
        DataType::Time => "TIME WITHOUT TIME ZONE".to_string(),
        DataType::Interval => "INTERVAL".to_string(),
        DataType::Numeric(Some((p, s))) => format!("NUMERIC({},{})", p, s),
        DataType::Numeric(None) => "NUMERIC".to_string(),
        DataType::BigNumeric => "NUMERIC".to_string(),
        DataType::Bytes => "BYTEA".to_string(),
        DataType::Json => "JSON".to_string(),
        DataType::Hstore => "HSTORE".to_string(),
        DataType::Uuid => "UUID".to_string(),
        DataType::Serial => "SERIAL".to_string(),
        DataType::BigSerial => "BIGSERIAL".to_string(),
        DataType::Array(inner) => format!("{}[]", data_type_to_postgres_name(inner)),
        DataType::Map(k, v) => {
            format!(
                "MAP({},{})",
                data_type_to_postgres_name(k),
                data_type_to_postgres_name(v)
            )
        }
        DataType::Point => "POINT".to_string(),
        DataType::Circle => "CIRCLE".to_string(),
        DataType::Line => "LINE".to_string(),
        DataType::Lseg => "LSEG".to_string(),
        DataType::Path => "PATH".to_string(),
        DataType::Polygon => "POLYGON".to_string(),
        DataType::PgBox => "BOX".to_string(),
        DataType::Inet => "INET".to_string(),
        DataType::Cidr => "CIDR".to_string(),
        DataType::MacAddr => "MACADDR".to_string(),
        DataType::MacAddr8 => "MACADDR8".to_string(),
        DataType::Vector(dim) => format!("VECTOR({})", dim),
        DataType::Range(range_type) => match range_type {
            RangeType::Int4Range => "INT4RANGE".to_string(),
            RangeType::Int8Range => "INT8RANGE".to_string(),
            RangeType::NumRange => "NUMRANGE".to_string(),
            RangeType::TsRange => "TSRANGE".to_string(),
            RangeType::TsTzRange => "TSTZRANGE".to_string(),
            RangeType::DateRange => "DATERANGE".to_string(),
        },
        DataType::Multirange(mr_type) => match mr_type {
            MultirangeType::Int4Multirange => "INT4MULTIRANGE".to_string(),
            MultirangeType::Int8Multirange => "INT8MULTIRANGE".to_string(),
            MultirangeType::NumMultirange => "NUMMULTIRANGE".to_string(),
            MultirangeType::TsMultirange => "TSMULTIRANGE".to_string(),
            MultirangeType::TsTzMultirange => "TSTZMULTIRANGE".to_string(),
            MultirangeType::DateMultirange => "DATEMULTIRANGE".to_string(),
        },
        DataType::Geography => "GEOGRAPHY".to_string(),
        DataType::Unknown => "UNKNOWN".to_string(),
        DataType::Struct(_) => "RECORD".to_string(),
        DataType::Enum { type_name, .. } => type_name.clone(),
        DataType::Custom(name) => name.clone(),
        DataType::IPv4 => "IPv4".to_string(),
        DataType::IPv6 => "IPv6".to_string(),
        DataType::Date32 => "Date32".to_string(),
        DataType::GeoPoint => "Point".to_string(),
        DataType::GeoRing => "Ring".to_string(),
        DataType::GeoPolygon => "Polygon".to_string(),
        DataType::GeoMultiPolygon => "MultiPolygon".to_string(),
        DataType::FixedString(n) => format!("FixedString({})", n),
        DataType::Xid => "XID".to_string(),
        DataType::Xid8 => "XID8".to_string(),
        DataType::Tid => "TID".to_string(),
        DataType::Cid => "CID".to_string(),
        DataType::Oid => "OID".to_string(),
        DataType::TsVector => "TSVECTOR".to_string(),
        DataType::TsQuery => "TSQUERY".to_string(),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InformationSchemaTable {
    Tables,
    Columns,
    Schemata,
    Views,
    TableConstraints,
    KeyColumnUsage,
    ColumnPrivileges,
    TablePrivileges,
    ReferentialConstraints,
    CheckConstraints,
    ViewRoutineUsage,
    Routines,
    Parameters,
    Triggers,
    Sequences,
    Domains,
    ColumnDomainUsage,
    ConstraintColumnUsage,
    ConstraintTableUsage,
    ElementTypes,
    RoleTableGrants,
    RoleColumnGrants,
    EnabledRoles,
    ApplicableRoles,
    ColumnUdtUsage,
    UserDefinedTypes,
    Attributes,
    DataTypePrivileges,
    RoutinePrivileges,
    UsagePrivileges,
    UdtPrivileges,
    ViewColumnUsage,
    ViewTableUsage,
    ColumnOptions,
    ForeignDataWrappers,
    ForeignServers,
    ForeignTables,
    ForeignTableOptions,
    UserMappings,
    UserMappingOptions,
    AdministrableRoleAuthorizations,
    CharacterSets,
    Collations,
    CollationCharacterSetApplicability,
    SqlFeatures,
    SqlImplementationInfo,
    SqlParts,
    SqlSizing,
    Statistics,
}

impl InformationSchemaTable {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "tables" => Ok(InformationSchemaTable::Tables),
            "columns" => Ok(InformationSchemaTable::Columns),
            "schemata" => Ok(InformationSchemaTable::Schemata),
            "views" => Ok(InformationSchemaTable::Views),
            "table_constraints" => Ok(InformationSchemaTable::TableConstraints),
            "key_column_usage" => Ok(InformationSchemaTable::KeyColumnUsage),
            "column_privileges" => Ok(InformationSchemaTable::ColumnPrivileges),
            "table_privileges" => Ok(InformationSchemaTable::TablePrivileges),
            "referential_constraints" => Ok(InformationSchemaTable::ReferentialConstraints),
            "check_constraints" => Ok(InformationSchemaTable::CheckConstraints),
            "view_routine_usage" => Ok(InformationSchemaTable::ViewRoutineUsage),
            "routines" => Ok(InformationSchemaTable::Routines),
            "parameters" => Ok(InformationSchemaTable::Parameters),
            "triggers" => Ok(InformationSchemaTable::Triggers),
            "sequences" => Ok(InformationSchemaTable::Sequences),
            "domains" => Ok(InformationSchemaTable::Domains),
            "column_domain_usage" => Ok(InformationSchemaTable::ColumnDomainUsage),
            "constraint_column_usage" => Ok(InformationSchemaTable::ConstraintColumnUsage),
            "constraint_table_usage" => Ok(InformationSchemaTable::ConstraintTableUsage),
            "element_types" => Ok(InformationSchemaTable::ElementTypes),
            "role_table_grants" => Ok(InformationSchemaTable::RoleTableGrants),
            "role_column_grants" => Ok(InformationSchemaTable::RoleColumnGrants),
            "enabled_roles" => Ok(InformationSchemaTable::EnabledRoles),
            "applicable_roles" => Ok(InformationSchemaTable::ApplicableRoles),
            "column_udt_usage" => Ok(InformationSchemaTable::ColumnUdtUsage),
            "user_defined_types" => Ok(InformationSchemaTable::UserDefinedTypes),
            "attributes" => Ok(InformationSchemaTable::Attributes),
            "data_type_privileges" => Ok(InformationSchemaTable::DataTypePrivileges),
            "routine_privileges" => Ok(InformationSchemaTable::RoutinePrivileges),
            "usage_privileges" => Ok(InformationSchemaTable::UsagePrivileges),
            "udt_privileges" => Ok(InformationSchemaTable::UdtPrivileges),
            "view_column_usage" => Ok(InformationSchemaTable::ViewColumnUsage),
            "view_table_usage" => Ok(InformationSchemaTable::ViewTableUsage),
            "column_options" => Ok(InformationSchemaTable::ColumnOptions),
            "foreign_data_wrappers" => Ok(InformationSchemaTable::ForeignDataWrappers),
            "foreign_servers" => Ok(InformationSchemaTable::ForeignServers),
            "foreign_tables" => Ok(InformationSchemaTable::ForeignTables),
            "foreign_table_options" => Ok(InformationSchemaTable::ForeignTableOptions),
            "user_mappings" => Ok(InformationSchemaTable::UserMappings),
            "user_mapping_options" => Ok(InformationSchemaTable::UserMappingOptions),
            "administrable_role_authorizations" => {
                Ok(InformationSchemaTable::AdministrableRoleAuthorizations)
            }
            "character_sets" => Ok(InformationSchemaTable::CharacterSets),
            "collations" => Ok(InformationSchemaTable::Collations),
            "collation_character_set_applicability" => {
                Ok(InformationSchemaTable::CollationCharacterSetApplicability)
            }
            "sql_features" => Ok(InformationSchemaTable::SqlFeatures),
            "sql_implementation_info" => Ok(InformationSchemaTable::SqlImplementationInfo),
            "sql_parts" => Ok(InformationSchemaTable::SqlParts),
            "sql_sizing" => Ok(InformationSchemaTable::SqlSizing),
            "statistics" => Ok(InformationSchemaTable::Statistics),
            _ => Err(Error::table_not_found(format!(
                "information_schema.{} is not supported",
                s
            ))),
        }
    }
}

pub struct InformationSchemaProvider {
    storage: Rc<RefCell<Storage>>,
    dialect: DialectType,
}

impl InformationSchemaProvider {
    pub fn new(storage: Rc<RefCell<Storage>>, dialect: DialectType) -> Self {
        Self { storage, dialect }
    }

    pub fn query(&self, table: InformationSchemaTable) -> Result<(Schema, Vec<Row>)> {
        match table {
            InformationSchemaTable::Tables => self.get_tables(),
            InformationSchemaTable::Columns => self.get_columns(),
            InformationSchemaTable::Schemata => self.get_schemata(),
            InformationSchemaTable::Views => self.get_views(),
            InformationSchemaTable::TableConstraints => self.get_table_constraints(),
            InformationSchemaTable::KeyColumnUsage => self.get_key_column_usage(),
            InformationSchemaTable::ColumnPrivileges => self.get_column_privileges(),
            InformationSchemaTable::TablePrivileges => self.get_table_privileges(),
            InformationSchemaTable::ReferentialConstraints => self.get_referential_constraints(),
            InformationSchemaTable::CheckConstraints => self.get_check_constraints(),
            InformationSchemaTable::ViewRoutineUsage => self.get_view_routine_usage(),
            InformationSchemaTable::Routines => self.get_routines(),
            InformationSchemaTable::Parameters => self.get_parameters(),
            InformationSchemaTable::Triggers => self.get_triggers(),
            InformationSchemaTable::Sequences => self.get_sequences(),
            InformationSchemaTable::Domains => self.get_domains(),
            InformationSchemaTable::ColumnDomainUsage => self.get_column_domain_usage(),
            InformationSchemaTable::ConstraintColumnUsage => self.get_constraint_column_usage(),
            InformationSchemaTable::ConstraintTableUsage => self.get_constraint_table_usage(),
            InformationSchemaTable::ElementTypes => self.get_element_types(),
            InformationSchemaTable::RoleTableGrants => self.get_role_table_grants(),
            InformationSchemaTable::RoleColumnGrants => self.get_role_column_grants(),
            InformationSchemaTable::EnabledRoles => self.get_enabled_roles(),
            InformationSchemaTable::ApplicableRoles => self.get_applicable_roles(),
            InformationSchemaTable::ColumnUdtUsage => self.get_column_udt_usage(),
            InformationSchemaTable::UserDefinedTypes => self.get_user_defined_types(),
            InformationSchemaTable::Attributes => self.get_attributes(),
            InformationSchemaTable::DataTypePrivileges => self.get_data_type_privileges(),
            InformationSchemaTable::RoutinePrivileges => self.get_routine_privileges(),
            InformationSchemaTable::UsagePrivileges => self.get_usage_privileges(),
            InformationSchemaTable::UdtPrivileges => self.get_udt_privileges(),
            InformationSchemaTable::ViewColumnUsage => self.get_view_column_usage(),
            InformationSchemaTable::ViewTableUsage => self.get_view_table_usage(),
            InformationSchemaTable::ColumnOptions => self.get_column_options(),
            InformationSchemaTable::ForeignDataWrappers => self.get_foreign_data_wrappers(),
            InformationSchemaTable::ForeignServers => self.get_foreign_servers(),
            InformationSchemaTable::ForeignTables => self.get_foreign_tables(),
            InformationSchemaTable::ForeignTableOptions => self.get_foreign_table_options(),
            InformationSchemaTable::UserMappings => self.get_user_mappings(),
            InformationSchemaTable::UserMappingOptions => self.get_user_mapping_options(),
            InformationSchemaTable::AdministrableRoleAuthorizations => {
                self.get_administrable_role_authorizations()
            }
            InformationSchemaTable::CharacterSets => self.get_character_sets(),
            InformationSchemaTable::Collations => self.get_collations(),
            InformationSchemaTable::CollationCharacterSetApplicability => {
                self.get_collation_character_set_applicability()
            }
            InformationSchemaTable::SqlFeatures => self.get_sql_features(),
            InformationSchemaTable::SqlImplementationInfo => self.get_sql_implementation_info(),
            InformationSchemaTable::SqlParts => self.get_sql_parts(),
            InformationSchemaTable::SqlSizing => self.get_sql_sizing(),
            InformationSchemaTable::Statistics => self.get_statistics(),
        }
    }

    fn dataset_to_schema_name(&self, dataset_id: &str) -> String {
        if dataset_id == "default" && self.dialect == DialectType::PostgreSQL {
            "public".to_string()
        } else {
            dataset_id.to_string()
        }
    }

    fn get_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("table_type", DataType::String),
            Field::nullable("engine", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for table_name in dataset.tables().keys() {
                    rows.push(Row::from_values(vec![
                        Value::string(schema_name.clone()),
                        Value::string(schema_name.clone()),
                        Value::string(table_name.clone()),
                        Value::string("BASE TABLE".to_string()),
                        Value::string("MergeTree".to_string()),
                    ]));
                }

                for view_name in dataset.views().list_views() {
                    let is_materialized = dataset
                        .views()
                        .get_view(&view_name)
                        .map(|v| v.is_materialized())
                        .unwrap_or(false);
                    let table_type = if is_materialized {
                        "MATERIALIZED VIEW"
                    } else {
                        "VIEW"
                    };
                    rows.push(Row::from_values(vec![
                        Value::string(schema_name.clone()),
                        Value::string(schema_name.clone()),
                        Value::string(view_name),
                        Value::string(table_type.to_string()),
                        Value::null(),
                    ]));
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_columns(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("ordinal_position", DataType::Int64),
            Field::nullable("column_default", DataType::String),
            Field::nullable("is_nullable", DataType::String),
            Field::nullable("data_type", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();
                    for (position, field) in table_schema.fields().iter().enumerate() {
                        let is_nullable = if field.is_nullable() { "YES" } else { "NO" };
                        let default_value = field
                            .default_value
                            .as_ref()
                            .map(|d| format!("{:?}", d))
                            .map(Value::string)
                            .unwrap_or(Value::null());

                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string(field.name.clone()),
                            Value::int64((position + 1) as i64),
                            default_value,
                            Value::string(is_nullable.to_string()),
                            Value::string(data_type_to_postgres_name(&field.data_type)),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_schemata(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("catalog_name", DataType::String),
            Field::nullable("schema_name", DataType::String),
            Field::nullable("schema_owner", DataType::String),
            Field::nullable("default_character_set_name", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            rows.push(Row::from_values(vec![
                Value::string(schema_name.clone()),
                Value::string(schema_name),
                Value::string("postgres".to_string()),
                Value::string("UTF8".to_string()),
            ]));
        }

        Ok((schema, rows))
    }

    fn get_views(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("view_definition", DataType::String),
            Field::nullable("is_updatable", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for view_name in dataset.views().list_views() {
                    if let Some(view) = dataset.views().get_view(&view_name) {
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(view_name),
                            Value::string(view.sql.clone()),
                            Value::string("NO".to_string()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_table_constraints(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("constraint_type", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();

                    if table_schema.primary_key().is_some() {
                        let constraint_name = format!("{}_pkey", table_name);
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string("PRIMARY KEY".to_string()),
                        ]));
                    }

                    for (idx, _unique_cols) in table_schema.unique_constraints().iter().enumerate()
                    {
                        let constraint_name = format!("{}_unique_{}", table_name, idx);
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string("UNIQUE".to_string()),
                        ]));
                    }

                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string("FOREIGN KEY".to_string()),
                        ]));
                    }

                    for (idx, check) in table_schema.check_constraints().iter().enumerate() {
                        let constraint_name = check
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table_name, idx));
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string("CHECK".to_string()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_key_column_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("ordinal_position", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();

                    if let Some(pk_columns) = table_schema.primary_key() {
                        let constraint_name = format!("{}_pkey", table_name);
                        for (position, col_name) in pk_columns.iter().enumerate() {
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(constraint_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(table_name.clone()),
                                Value::string(col_name.clone()),
                                Value::int64((position + 1) as i64),
                            ]));
                        }
                    }

                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        for (position, col_name) in fk.child_columns.iter().enumerate() {
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(constraint_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(table_name.clone()),
                                Value::string(col_name.clone()),
                                Value::int64((position + 1) as i64),
                            ]));
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_column_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_table_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
            Field::nullable("with_hierarchy", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_referential_constraints(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("unique_constraint_catalog", DataType::String),
            Field::nullable("unique_constraint_schema", DataType::String),
            Field::nullable("unique_constraint_name", DataType::String),
            Field::nullable("match_option", DataType::String),
            Field::nullable("update_rule", DataType::String),
            Field::nullable("delete_rule", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        let unique_constraint_name = format!("{}_pkey", fk.parent_table);

                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(unique_constraint_name),
                            Value::string("NONE".to_string()),
                            Value::string(format!("{:?}", fk.on_update)),
                            Value::string(format!("{:?}", fk.on_delete)),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_check_constraints(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
            Field::nullable("check_clause", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();
                    for (idx, check) in table_schema.check_constraints().iter().enumerate() {
                        let constraint_name = check
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table_name, idx));
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                            Value::string(check.expression.clone()),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_view_routine_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("specific_catalog", DataType::String),
            Field::nullable("specific_schema", DataType::String),
            Field::nullable("specific_name", DataType::String),
            Field::nullable("routine_catalog", DataType::String),
            Field::nullable("routine_schema", DataType::String),
            Field::nullable("routine_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_routines(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("specific_catalog", DataType::String),
            Field::nullable("specific_schema", DataType::String),
            Field::nullable("specific_name", DataType::String),
            Field::nullable("routine_catalog", DataType::String),
            Field::nullable("routine_schema", DataType::String),
            Field::nullable("routine_name", DataType::String),
            Field::nullable("routine_type", DataType::String),
            Field::nullable("data_type", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_parameters(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("specific_catalog", DataType::String),
            Field::nullable("specific_schema", DataType::String),
            Field::nullable("specific_name", DataType::String),
            Field::nullable("ordinal_position", DataType::Int64),
            Field::nullable("parameter_mode", DataType::String),
            Field::nullable("parameter_name", DataType::String),
            Field::nullable("data_type", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_triggers(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("trigger_catalog", DataType::String),
            Field::nullable("trigger_schema", DataType::String),
            Field::nullable("trigger_name", DataType::String),
            Field::nullable("event_manipulation", DataType::String),
            Field::nullable("event_object_catalog", DataType::String),
            Field::nullable("event_object_schema", DataType::String),
            Field::nullable("event_object_table", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_sequences(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("sequence_catalog", DataType::String),
            Field::nullable("sequence_schema", DataType::String),
            Field::nullable("sequence_name", DataType::String),
            Field::nullable("data_type", DataType::String),
            Field::nullable("start_value", DataType::Int64),
            Field::nullable("minimum_value", DataType::Int64),
            Field::nullable("maximum_value", DataType::Int64),
            Field::nullable("increment", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_domains(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("domain_catalog", DataType::String),
            Field::nullable("domain_schema", DataType::String),
            Field::nullable("domain_name", DataType::String),
            Field::nullable("data_type", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_column_domain_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("domain_catalog", DataType::String),
            Field::nullable("domain_schema", DataType::String),
            Field::nullable("domain_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_constraint_column_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();

                    if let Some(pk_columns) = table_schema.primary_key() {
                        let constraint_name = format!("{}_pkey", table_name);
                        for col_name in pk_columns {
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(table_name.clone()),
                                Value::string(col_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(constraint_name.clone()),
                            ]));
                        }
                    }

                    for (idx, unique_cols) in table_schema.unique_constraints().iter().enumerate() {
                        let constraint_name = format!("{}_unique_{}", table_name, idx);
                        for col_name in &unique_cols.columns {
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(table_name.clone()),
                                Value::string(col_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(constraint_name.clone()),
                            ]));
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_constraint_table_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("constraint_catalog", DataType::String),
            Field::nullable("constraint_schema", DataType::String),
            Field::nullable("constraint_name", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    for fk in table.foreign_keys() {
                        let constraint_name = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(fk.parent_table.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(schema_name.clone()),
                            Value::string(constraint_name),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_element_types(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("object_catalog", DataType::String),
            Field::nullable("object_schema", DataType::String),
            Field::nullable("object_name", DataType::String),
            Field::nullable("object_type", DataType::String),
            Field::nullable("collection_type_identifier", DataType::String),
            Field::nullable("data_type", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_role_table_grants(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
            Field::nullable("with_hierarchy", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_role_column_grants(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_enabled_roles(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![Field::nullable("role_name", DataType::String)]);

        let rows = vec![Row::from_values(vec![Value::string(
            "postgres".to_string(),
        )])];

        Ok((schema, rows))
    }

    fn get_applicable_roles(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantee", DataType::String),
            Field::nullable("role_name", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("postgres".to_string()),
            Value::string("postgres".to_string()),
            Value::string("YES".to_string()),
        ])];

        Ok((schema, rows))
    }

    fn get_column_udt_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("udt_catalog", DataType::String),
            Field::nullable("udt_schema", DataType::String),
            Field::nullable("udt_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_user_defined_types(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("user_defined_type_catalog", DataType::String),
            Field::nullable("user_defined_type_schema", DataType::String),
            Field::nullable("user_defined_type_name", DataType::String),
            Field::nullable("user_defined_type_category", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for type_name in dataset.types().list_types() {
                    rows.push(Row::from_values(vec![
                        Value::string(schema_name.clone()),
                        Value::string(schema_name.clone()),
                        Value::string(type_name.clone()),
                        Value::string("COMPOSITE".to_string()),
                    ]));
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_attributes(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("udt_catalog", DataType::String),
            Field::nullable("udt_schema", DataType::String),
            Field::nullable("udt_name", DataType::String),
            Field::nullable("attribute_name", DataType::String),
            Field::nullable("ordinal_position", DataType::Int64),
            Field::nullable("data_type", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for type_name in dataset.types().list_types() {
                    if let Some(udt) = dataset.types().get_type(type_name) {
                        if let Some(fields) = udt.definition.as_composite() {
                            for (idx, field) in fields.iter().enumerate() {
                                rows.push(Row::from_values(vec![
                                    Value::string(schema_name.clone()),
                                    Value::string(schema_name.clone()),
                                    Value::string(type_name.clone()),
                                    Value::string(field.name.clone()),
                                    Value::int64((idx + 1) as i64),
                                    Value::string(data_type_to_postgres_name(&field.data_type)),
                                ]));
                            }
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_data_type_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("object_catalog", DataType::String),
            Field::nullable("object_schema", DataType::String),
            Field::nullable("object_name", DataType::String),
            Field::nullable("object_type", DataType::String),
            Field::nullable("dtd_identifier", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_routine_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("specific_catalog", DataType::String),
            Field::nullable("specific_schema", DataType::String),
            Field::nullable("specific_name", DataType::String),
            Field::nullable("routine_catalog", DataType::String),
            Field::nullable("routine_schema", DataType::String),
            Field::nullable("routine_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_usage_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("object_catalog", DataType::String),
            Field::nullable("object_schema", DataType::String),
            Field::nullable("object_name", DataType::String),
            Field::nullable("object_type", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_udt_privileges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantor", DataType::String),
            Field::nullable("grantee", DataType::String),
            Field::nullable("udt_catalog", DataType::String),
            Field::nullable("udt_schema", DataType::String),
            Field::nullable("udt_name", DataType::String),
            Field::nullable("privilege_type", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_view_column_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("view_catalog", DataType::String),
            Field::nullable("view_schema", DataType::String),
            Field::nullable("view_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_view_table_usage(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("view_catalog", DataType::String),
            Field::nullable("view_schema", DataType::String),
            Field::nullable("view_name", DataType::String),
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_column_options(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("column_name", DataType::String),
            Field::nullable("option_name", DataType::String),
            Field::nullable("option_value", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_foreign_data_wrappers(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("foreign_data_wrapper_catalog", DataType::String),
            Field::nullable("foreign_data_wrapper_name", DataType::String),
            Field::nullable("authorization_identifier", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_foreign_servers(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("foreign_server_catalog", DataType::String),
            Field::nullable("foreign_server_name", DataType::String),
            Field::nullable("foreign_data_wrapper_catalog", DataType::String),
            Field::nullable("foreign_data_wrapper_name", DataType::String),
            Field::nullable("foreign_server_type", DataType::String),
            Field::nullable("foreign_server_version", DataType::String),
            Field::nullable("authorization_identifier", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_foreign_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("foreign_table_catalog", DataType::String),
            Field::nullable("foreign_table_schema", DataType::String),
            Field::nullable("foreign_table_name", DataType::String),
            Field::nullable("foreign_server_catalog", DataType::String),
            Field::nullable("foreign_server_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_foreign_table_options(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("foreign_table_catalog", DataType::String),
            Field::nullable("foreign_table_schema", DataType::String),
            Field::nullable("foreign_table_name", DataType::String),
            Field::nullable("option_name", DataType::String),
            Field::nullable("option_value", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_user_mappings(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("authorization_identifier", DataType::String),
            Field::nullable("foreign_server_catalog", DataType::String),
            Field::nullable("foreign_server_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_user_mapping_options(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("authorization_identifier", DataType::String),
            Field::nullable("foreign_server_catalog", DataType::String),
            Field::nullable("foreign_server_name", DataType::String),
            Field::nullable("option_name", DataType::String),
            Field::nullable("option_value", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_administrable_role_authorizations(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("grantee", DataType::String),
            Field::nullable("role_name", DataType::String),
            Field::nullable("is_grantable", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_character_sets(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("character_set_catalog", DataType::String),
            Field::nullable("character_set_schema", DataType::String),
            Field::nullable("character_set_name", DataType::String),
            Field::nullable("character_repertoire", DataType::String),
            Field::nullable("form_of_use", DataType::String),
            Field::nullable("default_collate_catalog", DataType::String),
            Field::nullable("default_collate_schema", DataType::String),
            Field::nullable("default_collate_name", DataType::String),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::null(),
            Value::null(),
            Value::string("UTF8".to_string()),
            Value::string("UCS".to_string()),
            Value::string("UTF8".to_string()),
            Value::null(),
            Value::null(),
            Value::string("en_US.UTF-8".to_string()),
        ])];

        Ok((schema, rows))
    }

    fn get_collations(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("collation_catalog", DataType::String),
            Field::nullable("collation_schema", DataType::String),
            Field::nullable("collation_name", DataType::String),
            Field::nullable("pad_attribute", DataType::String),
        ]);

        let collations = vec!["C", "POSIX", "en_US.UTF-8", "default"];

        let rows: Vec<Row> = collations
            .into_iter()
            .map(|name| {
                Row::from_values(vec![
                    Value::string("public".to_string()),
                    Value::string("pg_catalog".to_string()),
                    Value::string(name.to_string()),
                    Value::string("NO PAD".to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_collation_character_set_applicability(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("collation_catalog", DataType::String),
            Field::nullable("collation_schema", DataType::String),
            Field::nullable("collation_name", DataType::String),
            Field::nullable("character_set_catalog", DataType::String),
            Field::nullable("character_set_schema", DataType::String),
            Field::nullable("character_set_name", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_sql_features(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("feature_id", DataType::String),
            Field::nullable("feature_name", DataType::String),
            Field::nullable("sub_feature_id", DataType::String),
            Field::nullable("sub_feature_name", DataType::String),
            Field::nullable("is_supported", DataType::String),
            Field::nullable("comments", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_sql_implementation_info(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("implementation_info_id", DataType::String),
            Field::nullable("implementation_info_name", DataType::String),
            Field::nullable("integer_value", DataType::Int64),
            Field::nullable("character_value", DataType::String),
            Field::nullable("comments", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_sql_parts(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("feature_id", DataType::String),
            Field::nullable("feature_name", DataType::String),
            Field::nullable("is_supported", DataType::String),
            Field::nullable("comments", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_sql_sizing(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("sizing_id", DataType::Int64),
            Field::nullable("sizing_name", DataType::String),
            Field::nullable("supported_value", DataType::Int64),
            Field::nullable("comments", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_statistics(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("table_catalog", DataType::String),
            Field::nullable("table_schema", DataType::String),
            Field::nullable("table_name", DataType::String),
            Field::nullable("non_unique", DataType::Int64),
            Field::nullable("index_schema", DataType::String),
            Field::nullable("index_name", DataType::String),
            Field::nullable("seq_in_index", DataType::Int64),
            Field::nullable("column_name", DataType::String),
            Field::nullable("collation", DataType::String),
            Field::nullable("cardinality", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = self.dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    for idx_meta in table.index_metadata() {
                        for (seq, col) in idx_meta.columns.iter().enumerate() {
                            let col_name = col.column_name.clone().unwrap_or_default();
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(schema_name.clone()),
                                Value::string(table_name.clone()),
                                Value::int64(if idx_meta.is_unique { 0 } else { 1 }),
                                Value::string(schema_name.clone()),
                                Value::string(idx_meta.index_name.clone()),
                                Value::int64((seq + 1) as i64),
                                Value::string(col_name),
                                Value::string("A".to_string()),
                                Value::null(),
                            ]));
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }
}
