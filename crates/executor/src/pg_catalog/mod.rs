use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Field, Row, Schema, Storage, TableIndexOps};

use crate::information_schema::data_type_to_postgres_name;

fn data_type_to_oid(dt: &DataType) -> i64 {
    match dt {
        DataType::Bool => 16,
        DataType::Int64 => 20,
        DataType::Float32 => 700,
        DataType::Float64 => 701,
        DataType::String | DataType::FixedString(_) => 25,
        DataType::Bytes => 17,
        DataType::Date | DataType::Date32 => 1082,
        DataType::Time => 1083,
        DataType::Timestamp => 1114,
        DataType::DateTime | DataType::TimestampTz => 1184,
        DataType::Interval => 1186,
        DataType::Uuid => 2950,
        DataType::Json => 114,
        DataType::Numeric(_) | DataType::BigNumeric => 1700,
        DataType::Array(_) => 2277,
        DataType::Struct(_) | DataType::Map(_, _) => 2249,
        DataType::Unknown => 705,
        DataType::Serial | DataType::BigSerial => 23,
        DataType::Vector(_) => 16389,
        DataType::Range(_) => 3904,
        DataType::Inet | DataType::Cidr => 869,
        DataType::Point => 600,
        DataType::PgBox => 603,
        DataType::Circle => 718,
        DataType::Line => 628,
        DataType::Lseg => 601,
        DataType::Path => 602,
        DataType::Polygon => 604,
        DataType::MacAddr => 829,
        DataType::MacAddr8 => 774,
        DataType::IPv4 | DataType::IPv6 => 869,
        DataType::Geography => 16385,
        DataType::Hstore => 16387,
        DataType::GeoPoint
        | DataType::GeoRing
        | DataType::GeoPolygon
        | DataType::GeoMultiPolygon => 16390,
        DataType::Enum { .. } => 3500,
        DataType::Xid | DataType::Xid8 | DataType::Tid | DataType::Cid => 28,
        DataType::Oid => 26,
        DataType::Custom(_) => 705,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgCatalogTable {
    PgTables,
    PgIndexes,
    PgViews,
    PgClass,
    PgAttribute,
    PgNamespace,
    PgType,
    PgDescription,
    PgDatabase,
    PgRoles,
    PgSettings,
    PgConstraint,
    PgIndex,
    PgProc,
    PgTrigger,
    PgSequence,
    PgAttrdef,
    PgDepend,
    PgShdepend,
    PgTablespace,
    PgAuthid,
    PgAuthMembers,
    PgShadow,
    PgGroup,
    PgUser,
    PgStatActivity,
    PgLocks,
    PgStatUserTables,
    PgStatUserIndexes,
    PgStatioUserTables,
    PgStatioUserIndexes,
    PgStat,
    PgCursors,
    PgExtension,
    PgAvailableExtensions,
    PgEnum,
    PgRange,
    PgCollation,
    PgOperator,
    PgAggregate,
    PgCast,
    PgConversion,
    PgLanguage,
    PgAm,
    PgAmop,
    PgAmproc,
    PgOpclass,
    PgOpfamily,
    PgRewrite,
    PgStatistic,
    PgInherits,
    PgForeignDataWrapper,
    PgForeignServer,
    PgForeignTable,
    PgUserMapping,
    PgPolicy,
    PgEventTrigger,
    PgPublication,
    PgSubscription,
    PgStatReplication,
    PgStatWalReceiver,
    PgPreparedStatements,
    PgPreparedXacts,
    PgSecLabel,
    PgShseclabel,
    PgLargeobject,
    PgLargeobjectMetadata,
    PgShdescription,
    PgDbRoleSetting,
    PgTs,
    PgMatviews,
}

impl PgCatalogTable {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "pg_tables" => Ok(PgCatalogTable::PgTables),
            "pg_indexes" => Ok(PgCatalogTable::PgIndexes),
            "pg_views" => Ok(PgCatalogTable::PgViews),
            "pg_class" => Ok(PgCatalogTable::PgClass),
            "pg_attribute" => Ok(PgCatalogTable::PgAttribute),
            "pg_namespace" => Ok(PgCatalogTable::PgNamespace),
            "pg_type" => Ok(PgCatalogTable::PgType),
            "pg_description" => Ok(PgCatalogTable::PgDescription),
            "pg_database" => Ok(PgCatalogTable::PgDatabase),
            "pg_roles" => Ok(PgCatalogTable::PgRoles),
            "pg_settings" => Ok(PgCatalogTable::PgSettings),
            "pg_constraint" => Ok(PgCatalogTable::PgConstraint),
            "pg_index" => Ok(PgCatalogTable::PgIndex),
            "pg_proc" => Ok(PgCatalogTable::PgProc),
            "pg_trigger" => Ok(PgCatalogTable::PgTrigger),
            "pg_sequence" => Ok(PgCatalogTable::PgSequence),
            "pg_attrdef" => Ok(PgCatalogTable::PgAttrdef),
            "pg_depend" => Ok(PgCatalogTable::PgDepend),
            "pg_shdepend" => Ok(PgCatalogTable::PgShdepend),
            "pg_tablespace" => Ok(PgCatalogTable::PgTablespace),
            "pg_authid" => Ok(PgCatalogTable::PgAuthid),
            "pg_auth_members" => Ok(PgCatalogTable::PgAuthMembers),
            "pg_shadow" => Ok(PgCatalogTable::PgShadow),
            "pg_group" => Ok(PgCatalogTable::PgGroup),
            "pg_user" => Ok(PgCatalogTable::PgUser),
            "pg_stat_activity" => Ok(PgCatalogTable::PgStatActivity),
            "pg_locks" => Ok(PgCatalogTable::PgLocks),
            "pg_stat_user_tables" => Ok(PgCatalogTable::PgStatUserTables),
            "pg_stat_user_indexes" => Ok(PgCatalogTable::PgStatUserIndexes),
            "pg_statio_user_tables" => Ok(PgCatalogTable::PgStatioUserTables),
            "pg_statio_user_indexes" => Ok(PgCatalogTable::PgStatioUserIndexes),
            "pg_stat" => Ok(PgCatalogTable::PgStat),
            "pg_cursors" => Ok(PgCatalogTable::PgCursors),
            "pg_extension" => Ok(PgCatalogTable::PgExtension),
            "pg_available_extensions" => Ok(PgCatalogTable::PgAvailableExtensions),
            "pg_enum" => Ok(PgCatalogTable::PgEnum),
            "pg_range" => Ok(PgCatalogTable::PgRange),
            "pg_collation" => Ok(PgCatalogTable::PgCollation),
            "pg_operator" => Ok(PgCatalogTable::PgOperator),
            "pg_aggregate" => Ok(PgCatalogTable::PgAggregate),
            "pg_cast" => Ok(PgCatalogTable::PgCast),
            "pg_conversion" => Ok(PgCatalogTable::PgConversion),
            "pg_language" => Ok(PgCatalogTable::PgLanguage),
            "pg_am" => Ok(PgCatalogTable::PgAm),
            "pg_amop" => Ok(PgCatalogTable::PgAmop),
            "pg_amproc" => Ok(PgCatalogTable::PgAmproc),
            "pg_opclass" => Ok(PgCatalogTable::PgOpclass),
            "pg_opfamily" => Ok(PgCatalogTable::PgOpfamily),
            "pg_rewrite" => Ok(PgCatalogTable::PgRewrite),
            "pg_statistic" => Ok(PgCatalogTable::PgStatistic),
            "pg_inherits" => Ok(PgCatalogTable::PgInherits),
            "pg_foreign_data_wrapper" => Ok(PgCatalogTable::PgForeignDataWrapper),
            "pg_foreign_server" => Ok(PgCatalogTable::PgForeignServer),
            "pg_foreign_table" => Ok(PgCatalogTable::PgForeignTable),
            "pg_user_mapping" => Ok(PgCatalogTable::PgUserMapping),
            "pg_policy" => Ok(PgCatalogTable::PgPolicy),
            "pg_event_trigger" => Ok(PgCatalogTable::PgEventTrigger),
            "pg_publication" => Ok(PgCatalogTable::PgPublication),
            "pg_subscription" => Ok(PgCatalogTable::PgSubscription),
            "pg_stat_replication" => Ok(PgCatalogTable::PgStatReplication),
            "pg_stat_wal_receiver" => Ok(PgCatalogTable::PgStatWalReceiver),
            "pg_prepared_statements" => Ok(PgCatalogTable::PgPreparedStatements),
            "pg_prepared_xacts" => Ok(PgCatalogTable::PgPreparedXacts),
            "pg_seclabel" => Ok(PgCatalogTable::PgSecLabel),
            "pg_shseclabel" => Ok(PgCatalogTable::PgShseclabel),
            "pg_largeobject" => Ok(PgCatalogTable::PgLargeobject),
            "pg_largeobject_metadata" => Ok(PgCatalogTable::PgLargeobjectMetadata),
            "pg_shdescription" => Ok(PgCatalogTable::PgShdescription),
            "pg_db_role_setting" => Ok(PgCatalogTable::PgDbRoleSetting),
            "pg_ts_config" | "pg_ts_dict" | "pg_ts_parser" | "pg_ts_template" => {
                Ok(PgCatalogTable::PgTs)
            }
            "pg_matviews" => Ok(PgCatalogTable::PgMatviews),
            _ => Err(Error::table_not_found(format!(
                "pg_catalog.{} is not supported",
                s
            ))),
        }
    }
}

pub struct PgCatalogProvider {
    storage: Rc<RefCell<Storage>>,
}

impl PgCatalogProvider {
    pub fn new(storage: Rc<RefCell<Storage>>) -> Self {
        Self { storage }
    }

    fn dataset_to_schema_name(dataset_id: &str) -> String {
        if dataset_id == "default" {
            "public".to_string()
        } else {
            dataset_id.to_string()
        }
    }

    fn schema_name_to_nspoid(schema: &str) -> i64 {
        match schema {
            "pg_catalog" => 11,
            "information_schema" => 13,
            "public" | "default" => 2200,
            _ => {
                2200 + (schema
                    .bytes()
                    .fold(0i64, |acc, b| acc.wrapping_add(b as i64))
                    % 1000)
            }
        }
    }

    pub fn query(&self, table: PgCatalogTable) -> Result<(Schema, Vec<Row>)> {
        match table {
            PgCatalogTable::PgTables => self.get_pg_tables(),
            PgCatalogTable::PgIndexes => self.get_pg_indexes(),
            PgCatalogTable::PgViews => self.get_pg_views(),
            PgCatalogTable::PgClass => self.get_pg_class(),
            PgCatalogTable::PgAttribute => self.get_pg_attribute(),
            PgCatalogTable::PgNamespace => self.get_pg_namespace(),
            PgCatalogTable::PgType => self.get_pg_type(),
            PgCatalogTable::PgDescription => self.get_pg_description(),
            PgCatalogTable::PgDatabase => self.get_pg_database(),
            PgCatalogTable::PgRoles => self.get_pg_roles(),
            PgCatalogTable::PgSettings => self.get_pg_settings(),
            PgCatalogTable::PgConstraint => self.get_pg_constraint(),
            PgCatalogTable::PgIndex => self.get_pg_index(),
            PgCatalogTable::PgProc => self.get_pg_proc(),
            PgCatalogTable::PgTrigger => self.get_pg_trigger(),
            PgCatalogTable::PgSequence => self.get_pg_sequence(),
            PgCatalogTable::PgAttrdef => self.get_pg_attrdef(),
            PgCatalogTable::PgDepend => self.get_pg_depend(),
            PgCatalogTable::PgShdepend => self.get_pg_shdepend(),
            PgCatalogTable::PgTablespace => self.get_pg_tablespace(),
            PgCatalogTable::PgAuthid => self.get_pg_authid(),
            PgCatalogTable::PgAuthMembers => self.get_pg_auth_members(),
            PgCatalogTable::PgShadow => self.get_pg_shadow(),
            PgCatalogTable::PgGroup => self.get_pg_group(),
            PgCatalogTable::PgUser => self.get_pg_user(),
            PgCatalogTable::PgStatActivity => self.get_pg_stat_activity(),
            PgCatalogTable::PgLocks => self.get_pg_locks(),
            PgCatalogTable::PgStatUserTables => self.get_pg_stat_user_tables(),
            PgCatalogTable::PgStatUserIndexes => self.get_pg_stat_user_indexes(),
            PgCatalogTable::PgStatioUserTables => self.get_pg_statio_user_tables(),
            PgCatalogTable::PgStatioUserIndexes => self.get_pg_statio_user_indexes(),
            PgCatalogTable::PgStat => self.get_pg_stat(),
            PgCatalogTable::PgCursors => self.get_pg_cursors(),
            PgCatalogTable::PgExtension => self.get_pg_extension(),
            PgCatalogTable::PgAvailableExtensions => self.get_pg_available_extensions(),
            PgCatalogTable::PgEnum => self.get_pg_enum(),
            PgCatalogTable::PgRange => self.get_pg_range(),
            PgCatalogTable::PgCollation => self.get_pg_collation(),
            PgCatalogTable::PgOperator => self.get_pg_operator(),
            PgCatalogTable::PgAggregate => self.get_pg_aggregate(),
            PgCatalogTable::PgCast => self.get_pg_cast(),
            PgCatalogTable::PgConversion => self.get_pg_conversion(),
            PgCatalogTable::PgLanguage => self.get_pg_language(),
            PgCatalogTable::PgAm => self.get_pg_am(),
            PgCatalogTable::PgAmop => self.get_pg_amop(),
            PgCatalogTable::PgAmproc => self.get_pg_amproc(),
            PgCatalogTable::PgOpclass => self.get_pg_opclass(),
            PgCatalogTable::PgOpfamily => self.get_pg_opfamily(),
            PgCatalogTable::PgRewrite => self.get_pg_rewrite(),
            PgCatalogTable::PgStatistic => self.get_pg_statistic(),
            PgCatalogTable::PgInherits => self.get_pg_inherits(),
            PgCatalogTable::PgForeignDataWrapper => self.get_pg_foreign_data_wrapper(),
            PgCatalogTable::PgForeignServer => self.get_pg_foreign_server(),
            PgCatalogTable::PgForeignTable => self.get_pg_foreign_table(),
            PgCatalogTable::PgUserMapping => self.get_pg_user_mapping(),
            PgCatalogTable::PgPolicy => self.get_pg_policy(),
            PgCatalogTable::PgEventTrigger => self.get_pg_event_trigger(),
            PgCatalogTable::PgPublication => self.get_pg_publication(),
            PgCatalogTable::PgSubscription => self.get_pg_subscription(),
            PgCatalogTable::PgStatReplication => self.get_pg_stat_replication(),
            PgCatalogTable::PgStatWalReceiver => self.get_pg_stat_wal_receiver(),
            PgCatalogTable::PgPreparedStatements => self.get_pg_prepared_statements(),
            PgCatalogTable::PgPreparedXacts => self.get_pg_prepared_xacts(),
            PgCatalogTable::PgSecLabel => self.get_pg_seclabel(),
            PgCatalogTable::PgShseclabel => self.get_pg_shseclabel(),
            PgCatalogTable::PgLargeobject => self.get_pg_largeobject(),
            PgCatalogTable::PgLargeobjectMetadata => self.get_pg_largeobject_metadata(),
            PgCatalogTable::PgShdescription => self.get_pg_shdescription(),
            PgCatalogTable::PgDbRoleSetting => self.get_pg_db_role_setting(),
            PgCatalogTable::PgTs => self.get_pg_ts(),
            PgCatalogTable::PgMatviews => self.get_pg_matviews(),
        }
    }

    fn get_pg_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("schemaname", DataType::String),
            Field::nullable("tablename", DataType::String),
            Field::nullable("tableowner", DataType::String),
            Field::nullable("tablespace", DataType::String),
            Field::nullable("hasindexes", DataType::Bool),
            Field::nullable("hasrules", DataType::Bool),
            Field::nullable("hastriggers", DataType::Bool),
            Field::nullable("rowsecurity", DataType::Bool),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = Self::dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for table_name in dataset.tables().keys() {
                    rows.push(Row::from_values(vec![
                        Value::string(schema_name.clone()),
                        Value::string(table_name.clone()),
                        Value::string("postgres".to_string()),
                        Value::null(),
                        Value::bool_val(true),
                        Value::bool_val(false),
                        Value::bool_val(false),
                        Value::bool_val(false),
                    ]));
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_indexes(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("schemaname", DataType::String),
            Field::nullable("tablename", DataType::String),
            Field::nullable("indexname", DataType::String),
            Field::nullable("tablespace", DataType::String),
            Field::nullable("indexdef", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = Self::dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    for idx_meta in table.index_metadata() {
                        let unique_kw = if idx_meta.is_unique { "UNIQUE " } else { "" };
                        let cols: Vec<String> = idx_meta
                            .columns
                            .iter()
                            .filter_map(|c| c.column_name.clone())
                            .collect();
                        let cols_str = cols.join(", ");
                        let indexdef = format!(
                            "CREATE {}INDEX {} ON {}.{} USING btree ({})",
                            unique_kw, idx_meta.index_name, schema_name, table_name, cols_str
                        );
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string(idx_meta.index_name.clone()),
                            Value::null(),
                            Value::string(indexdef),
                        ]));
                    }

                    if let Some(pk_columns) = table.schema().primary_key() {
                        let idx_name = format!("{}_pkey", table_name);
                        let cols = pk_columns.join(", ");
                        let indexdef = format!(
                            "CREATE UNIQUE INDEX {} ON {}.{} USING btree ({})",
                            idx_name, schema_name, table_name, cols
                        );
                        rows.push(Row::from_values(vec![
                            Value::string(schema_name.clone()),
                            Value::string(table_name.clone()),
                            Value::string(idx_name),
                            Value::null(),
                            Value::string(indexdef),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_views(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("schemaname", DataType::String),
            Field::nullable("viewname", DataType::String),
            Field::nullable("viewowner", DataType::String),
            Field::nullable("definition", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = Self::dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for view_name in dataset.views().list_views() {
                    if let Some(view) = dataset.views().get_view(&view_name) {
                        if !view.is_materialized() {
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(view_name),
                                Value::string("postgres".to_string()),
                                Value::string(view.sql.clone()),
                            ]));
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_class(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("relname", DataType::String),
            Field::nullable("relnamespace", DataType::Oid),
            Field::nullable("reltype", DataType::Oid),
            Field::nullable("reloftype", DataType::Oid),
            Field::nullable("relowner", DataType::Oid),
            Field::nullable("relam", DataType::Oid),
            Field::nullable("relfilenode", DataType::Oid),
            Field::nullable("reltablespace", DataType::Oid),
            Field::nullable("relpages", DataType::Int64),
            Field::nullable("reltuples", DataType::Float64),
            Field::nullable("relallvisible", DataType::Int64),
            Field::nullable("reltoastrelid", DataType::Oid),
            Field::nullable("relhasindex", DataType::Bool),
            Field::nullable("relisshared", DataType::Bool),
            Field::nullable("relpersistence", DataType::String),
            Field::nullable("relkind", DataType::String),
            Field::nullable("relnatts", DataType::Int64),
            Field::nullable("relchecks", DataType::Int64),
            Field::nullable("relhasrules", DataType::Bool),
            Field::nullable("relhastriggers", DataType::Bool),
            Field::nullable("relhassubclass", DataType::Bool),
            Field::nullable("relrowsecurity", DataType::Bool),
            Field::nullable("relforcerowsecurity", DataType::Bool),
            Field::nullable("relispopulated", DataType::Bool),
            Field::nullable("relreplident", DataType::String),
            Field::nullable("relispartition", DataType::Bool),
            Field::nullable("relrewrite", DataType::Oid),
            Field::nullable("relfrozenxid", DataType::Int64),
            Field::nullable("relminmxid", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut oid: i64 = 16384;

        for dataset_id in storage.list_datasets() {
            let nspoid = Self::schema_name_to_nspoid(&Self::dataset_to_schema_name(dataset_id));
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();
                    let relkind = "r";
                    rows.push(Row::from_values(vec![
                        Value::int64(oid),
                        Value::string(table_name.clone()),
                        Value::int64(nspoid),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(10),
                        Value::int64(2),
                        Value::int64(oid),
                        Value::int64(0),
                        Value::int64(0),
                        Value::float64(-1.0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::bool_val(true),
                        Value::bool_val(false),
                        Value::string("p".to_string()),
                        Value::string(relkind.to_string()),
                        Value::int64(table_schema.fields().len() as i64),
                        Value::int64(table_schema.check_constraints().len() as i64),
                        Value::bool_val(false),
                        Value::bool_val(false),
                        Value::bool_val(false),
                        Value::bool_val(false),
                        Value::bool_val(false),
                        Value::bool_val(true),
                        Value::string("d".to_string()),
                        Value::bool_val(false),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                    ]));
                    oid += 1;
                }

                for view_name in dataset.views().list_views() {
                    if let Some(view) = dataset.views().get_view(&view_name) {
                        let relkind = if view.is_materialized() { "m" } else { "v" };
                        rows.push(Row::from_values(vec![
                            Value::int64(oid),
                            Value::string(view_name),
                            Value::int64(nspoid),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(10),
                            Value::int64(0),
                            Value::int64(oid),
                            Value::int64(0),
                            Value::int64(0),
                            Value::float64(-1.0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::string("p".to_string()),
                            Value::string(relkind.to_string()),
                            Value::int64(0),
                            Value::int64(0),
                            Value::bool_val(relkind == "v"),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(true),
                            Value::string("d".to_string()),
                            Value::bool_val(false),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                        ]));
                        oid += 1;
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_attribute(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("attrelid", DataType::Oid),
            Field::nullable("attname", DataType::String),
            Field::nullable("atttypid", DataType::Oid),
            Field::nullable("attstattarget", DataType::Int64),
            Field::nullable("attlen", DataType::Int64),
            Field::nullable("attnum", DataType::Int64),
            Field::nullable("attndims", DataType::Int64),
            Field::nullable("attcacheoff", DataType::Int64),
            Field::nullable("atttypmod", DataType::Int64),
            Field::nullable("attbyval", DataType::Bool),
            Field::nullable("attstorage", DataType::String),
            Field::nullable("attalign", DataType::String),
            Field::nullable("attnotnull", DataType::Bool),
            Field::nullable("atthasdef", DataType::Bool),
            Field::nullable("atthasmissing", DataType::Bool),
            Field::nullable("attidentity", DataType::String),
            Field::nullable("attgenerated", DataType::String),
            Field::nullable("attisdropped", DataType::Bool),
            Field::nullable("attislocal", DataType::Bool),
            Field::nullable("attinhcount", DataType::Int64),
            Field::nullable("attcollation", DataType::Oid),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut reloid: i64 = 16384;

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (_, table) in dataset.tables() {
                    let table_schema = table.schema();
                    for (attnum, field) in table_schema.fields().iter().enumerate() {
                        let typoid = data_type_to_oid(&field.data_type);
                        let attlen: i64 = match &field.data_type {
                            DataType::Bool => 1,
                            DataType::Float32 => 4,
                            DataType::Int64 | DataType::Float64 => 8,
                            _ => -1,
                        };
                        rows.push(Row::from_values(vec![
                            Value::int64(reloid),
                            Value::string(field.name.clone()),
                            Value::int64(typoid),
                            Value::int64(-1),
                            Value::int64(attlen),
                            Value::int64((attnum + 1) as i64),
                            Value::int64(0),
                            Value::int64(-1),
                            Value::int64(-1),
                            Value::bool_val(attlen > 0 && attlen <= 8),
                            Value::string("x".to_string()),
                            Value::string("i".to_string()),
                            Value::bool_val(!field.is_nullable()),
                            Value::bool_val(field.default_value.is_some()),
                            Value::bool_val(false),
                            Value::string("".to_string()),
                            Value::string("".to_string()),
                            Value::bool_val(false),
                            Value::bool_val(true),
                            Value::int64(0),
                            Value::int64(0),
                        ]));
                    }
                    reloid += 1;
                }

                for _ in dataset.views().list_views() {
                    reloid += 1;
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_namespace(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("nspname", DataType::String),
            Field::nullable("nspowner", DataType::Oid),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        rows.push(Row::from_values(vec![
            Value::int64(11),
            Value::string("pg_catalog".to_string()),
            Value::int64(10),
        ]));

        rows.push(Row::from_values(vec![
            Value::int64(13),
            Value::string("information_schema".to_string()),
            Value::int64(10),
        ]));

        for dataset_id in storage.list_datasets() {
            let schema_name = Self::dataset_to_schema_name(dataset_id);
            let nspoid = Self::schema_name_to_nspoid(&schema_name);
            rows.push(Row::from_values(vec![
                Value::int64(nspoid),
                Value::string(schema_name),
                Value::int64(10),
            ]));
        }

        Ok((schema, rows))
    }

    fn get_pg_type(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("typname", DataType::String),
            Field::nullable("typnamespace", DataType::Oid),
            Field::nullable("typowner", DataType::Oid),
            Field::nullable("typlen", DataType::Int64),
            Field::nullable("typbyval", DataType::Bool),
            Field::nullable("typtype", DataType::String),
            Field::nullable("typcategory", DataType::String),
            Field::nullable("typispreferred", DataType::Bool),
            Field::nullable("typisdefined", DataType::Bool),
            Field::nullable("typdelim", DataType::String),
            Field::nullable("typrelid", DataType::Oid),
            Field::nullable("typelem", DataType::Oid),
            Field::nullable("typarray", DataType::Oid),
            Field::nullable("typinput", DataType::String),
            Field::nullable("typoutput", DataType::String),
        ]);

        let types: Vec<(i64, &str, i64, bool, &str, &str)> = vec![
            (16, "bool", 1, true, "b", "B"),
            (17, "bytea", -1, false, "b", "U"),
            (18, "char", 1, true, "b", "S"),
            (19, "name", 64, false, "b", "S"),
            (20, "int8", 8, true, "b", "N"),
            (21, "int2", 2, true, "b", "N"),
            (23, "int4", 4, true, "b", "N"),
            (25, "text", -1, false, "b", "S"),
            (26, "oid", 4, true, "b", "N"),
            (114, "json", -1, false, "b", "U"),
            (700, "float4", 4, true, "b", "N"),
            (701, "float8", 8, true, "b", "N"),
            (1043, "varchar", -1, false, "b", "S"),
            (1082, "date", 4, true, "b", "D"),
            (1083, "time", 8, true, "b", "D"),
            (1114, "timestamp", 8, true, "b", "D"),
            (1184, "timestamptz", 8, true, "b", "D"),
            (1186, "interval", 16, false, "b", "T"),
            (1700, "numeric", -1, false, "b", "N"),
            (2950, "uuid", 16, false, "b", "U"),
            (3802, "jsonb", -1, false, "b", "U"),
        ];

        let mut rows = Vec::new();
        for (oid, typname, typlen, typbyval, typtype, typcategory) in types {
            rows.push(Row::from_values(vec![
                Value::int64(oid),
                Value::string(typname.to_string()),
                Value::int64(11),
                Value::int64(10),
                Value::int64(typlen),
                Value::bool_val(typbyval),
                Value::string(typtype.to_string()),
                Value::string(typcategory.to_string()),
                Value::bool_val(false),
                Value::bool_val(true),
                Value::string(",".to_string()),
                Value::int64(0),
                Value::int64(0),
                Value::int64(0),
                Value::string(format!("{}in", typname)),
                Value::string(format!("{}out", typname)),
            ]));
        }

        let storage = self.storage.borrow();
        let mut type_oid = 50000i64;
        for dataset_id in storage.list_datasets() {
            let nspoid = Self::schema_name_to_nspoid(&Self::dataset_to_schema_name(dataset_id));
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for type_name in dataset.types().list_types() {
                    rows.push(Row::from_values(vec![
                        Value::int64(type_oid),
                        Value::string(type_name.clone()),
                        Value::int64(nspoid),
                        Value::int64(10),
                        Value::int64(-1),
                        Value::bool_val(false),
                        Value::string("c".to_string()),
                        Value::string("C".to_string()),
                        Value::bool_val(false),
                        Value::bool_val(true),
                        Value::string(",".to_string()),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::string("record_in".to_string()),
                        Value::string("record_out".to_string()),
                    ]));
                    type_oid += 1;
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_description(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("objoid", DataType::Oid),
            Field::nullable("classoid", DataType::Oid),
            Field::nullable("objsubid", DataType::Int64),
            Field::nullable("description", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut reloid: i64 = 16384;

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (_, table) in dataset.tables() {
                    if let Some(table_comment) = table.comment() {
                        rows.push(Row::from_values(vec![
                            Value::int64(reloid),
                            Value::int64(1259),
                            Value::int64(0),
                            Value::string(table_comment.to_string()),
                        ]));
                    }

                    let table_schema = table.schema();
                    for (attnum, field) in table_schema.fields().iter().enumerate() {
                        if let Some(desc) = &field.description {
                            rows.push(Row::from_values(vec![
                                Value::int64(reloid),
                                Value::int64(1259),
                                Value::int64((attnum + 1) as i64),
                                Value::string(desc.clone()),
                            ]));
                        }
                    }
                    reloid += 1;
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_database(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("datname", DataType::String),
            Field::nullable("datdba", DataType::Oid),
            Field::nullable("encoding", DataType::Int64),
            Field::nullable("datcollate", DataType::String),
            Field::nullable("datctype", DataType::String),
            Field::nullable("datistemplate", DataType::Bool),
            Field::nullable("datallowconn", DataType::Bool),
            Field::nullable("datconnlimit", DataType::Int64),
            Field::nullable("datlastsysoid", DataType::Oid),
            Field::nullable("datfrozenxid", DataType::Int64),
            Field::nullable("datminmxid", DataType::Int64),
            Field::nullable("dattablespace", DataType::Oid),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::int64(16384),
            Value::string("postgres".to_string()),
            Value::int64(10),
            Value::int64(6),
            Value::string("en_US.UTF-8".to_string()),
            Value::string("en_US.UTF-8".to_string()),
            Value::bool_val(false),
            Value::bool_val(true),
            Value::int64(-1),
            Value::int64(16383),
            Value::int64(0),
            Value::int64(1),
            Value::int64(1663),
        ])];

        Ok((schema, rows))
    }

    fn get_pg_roles(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("rolname", DataType::String),
            Field::nullable("rolsuper", DataType::Bool),
            Field::nullable("rolinherit", DataType::Bool),
            Field::nullable("rolcreaterole", DataType::Bool),
            Field::nullable("rolcreatedb", DataType::Bool),
            Field::nullable("rolcanlogin", DataType::Bool),
            Field::nullable("rolreplication", DataType::Bool),
            Field::nullable("rolconnlimit", DataType::Int64),
            Field::nullable("rolpassword", DataType::String),
            Field::nullable("rolvaliduntil", DataType::Timestamp),
            Field::nullable("rolbypassrls", DataType::Bool),
            Field::nullable("rolconfig", DataType::Array(Box::new(DataType::String))),
            Field::nullable("oid", DataType::Oid),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("postgres".to_string()),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::int64(-1),
            Value::null(),
            Value::null(),
            Value::bool_val(true),
            Value::null(),
            Value::int64(10),
        ])];

        Ok((schema, rows))
    }

    fn get_pg_settings(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("setting", DataType::String),
            Field::nullable("unit", DataType::String),
            Field::nullable("category", DataType::String),
            Field::nullable("short_desc", DataType::String),
            Field::nullable("extra_desc", DataType::String),
            Field::nullable("context", DataType::String),
            Field::nullable("vartype", DataType::String),
            Field::nullable("source", DataType::String),
            Field::nullable("min_val", DataType::String),
            Field::nullable("max_val", DataType::String),
            Field::nullable("enumvals", DataType::Array(Box::new(DataType::String))),
            Field::nullable("boot_val", DataType::String),
            Field::nullable("reset_val", DataType::String),
            Field::nullable("sourcefile", DataType::String),
            Field::nullable("sourceline", DataType::Int64),
            Field::nullable("pending_restart", DataType::Bool),
        ]);

        let settings = vec![
            (
                "max_connections",
                "100",
                "Connections and Authentication",
                "Sets the maximum number of concurrent connections.",
            ),
            (
                "shared_buffers",
                "128MB",
                "Resource Usage / Memory",
                "Sets the number of shared memory buffers used by the server.",
            ),
            (
                "work_mem",
                "4MB",
                "Resource Usage / Memory",
                "Sets the maximum memory to be used for query workspaces.",
            ),
            (
                "maintenance_work_mem",
                "64MB",
                "Resource Usage / Memory",
                "Sets the maximum memory to be used for maintenance operations.",
            ),
            (
                "effective_cache_size",
                "4GB",
                "Query Tuning / Planner Cost Constants",
                "Sets the planner's assumption about the size of the disk cache.",
            ),
            (
                "server_version",
                "16.0",
                "Preset Options",
                "Shows the server version.",
            ),
            (
                "server_encoding",
                "UTF8",
                "Client Connection Defaults / Locale and Formatting",
                "Sets the server (database) character set encoding.",
            ),
            (
                "client_encoding",
                "UTF8",
                "Client Connection Defaults / Locale and Formatting",
                "Sets the client's character set encoding.",
            ),
            (
                "DateStyle",
                "ISO, MDY",
                "Client Connection Defaults / Locale and Formatting",
                "Sets the display format for date and time values.",
            ),
            (
                "TimeZone",
                "UTC",
                "Client Connection Defaults / Locale and Formatting",
                "Sets the time zone for displaying and interpreting time stamps.",
            ),
            (
                "standard_conforming_strings",
                "on",
                "Version and Platform Compatibility / Previous PostgreSQL Versions",
                "Causes '...' strings to treat backslashes literally.",
            ),
        ];

        let mut rows = Vec::new();
        for (name, setting, category, desc) in settings {
            rows.push(Row::from_values(vec![
                Value::string(name.to_string()),
                Value::string(setting.to_string()),
                Value::null(),
                Value::string(category.to_string()),
                Value::string(desc.to_string()),
                Value::null(),
                Value::string("default".to_string()),
                Value::string("string".to_string()),
                Value::string("default".to_string()),
                Value::null(),
                Value::null(),
                Value::null(),
                Value::string(setting.to_string()),
                Value::string(setting.to_string()),
                Value::null(),
                Value::null(),
                Value::bool_val(false),
            ]));
        }

        Ok((schema, rows))
    }

    fn get_pg_constraint(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("conname", DataType::String),
            Field::nullable("connamespace", DataType::Oid),
            Field::nullable("contype", DataType::String),
            Field::nullable("condeferrable", DataType::Bool),
            Field::nullable("condeferred", DataType::Bool),
            Field::nullable("convalidated", DataType::Bool),
            Field::nullable("conrelid", DataType::Oid),
            Field::nullable("contypid", DataType::Oid),
            Field::nullable("conindid", DataType::Oid),
            Field::nullable("conparentid", DataType::Oid),
            Field::nullable("confrelid", DataType::Oid),
            Field::nullable("confupdtype", DataType::String),
            Field::nullable("confdeltype", DataType::String),
            Field::nullable("confmatchtype", DataType::String),
            Field::nullable("conislocal", DataType::Bool),
            Field::nullable("coninhcount", DataType::Int64),
            Field::nullable("connoinherit", DataType::Bool),
            Field::nullable("conkey", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("confkey", DataType::Array(Box::new(DataType::Int64))),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut conoid = 30000i64;
        let mut reloid = 16384i64;

        for dataset_id in storage.list_datasets() {
            let nspoid = Self::schema_name_to_nspoid(&Self::dataset_to_schema_name(dataset_id));
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();

                    if let Some(pk_columns) = table_schema.primary_key() {
                        let conname = format!("{}_pkey", table_name);
                        let conkey: Vec<Value> = pk_columns
                            .iter()
                            .filter_map(|col| {
                                table_schema
                                    .fields()
                                    .iter()
                                    .position(|f| &f.name == col)
                                    .map(|i| Value::int64((i + 1) as i64))
                            })
                            .collect();
                        rows.push(Row::from_values(vec![
                            Value::int64(conoid),
                            Value::string(conname),
                            Value::int64(nspoid),
                            Value::string("p".to_string()),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(true),
                            Value::int64(reloid),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::string(" ".to_string()),
                            Value::string(" ".to_string()),
                            Value::string(" ".to_string()),
                            Value::bool_val(true),
                            Value::int64(0),
                            Value::bool_val(true),
                            Value::array(conkey),
                            Value::null(),
                        ]));
                        conoid += 1;
                    }

                    for (idx, unique_cols) in table_schema.unique_constraints().iter().enumerate() {
                        let conname = format!("{}_unique_{}", table_name, idx);
                        let conkey: Vec<Value> = unique_cols
                            .columns
                            .iter()
                            .filter_map(|col| {
                                table_schema
                                    .fields()
                                    .iter()
                                    .position(|f| &f.name == col)
                                    .map(|i| Value::int64((i + 1) as i64))
                            })
                            .collect();
                        rows.push(Row::from_values(vec![
                            Value::int64(conoid),
                            Value::string(conname),
                            Value::int64(nspoid),
                            Value::string("u".to_string()),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(true),
                            Value::int64(reloid),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::string(" ".to_string()),
                            Value::string(" ".to_string()),
                            Value::string(" ".to_string()),
                            Value::bool_val(true),
                            Value::int64(0),
                            Value::bool_val(true),
                            Value::array(conkey),
                            Value::null(),
                        ]));
                        conoid += 1;
                    }

                    for fk in table.foreign_keys() {
                        let conname = fk
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_fkey", table_name));
                        let conkey: Vec<Value> = fk
                            .child_columns
                            .iter()
                            .filter_map(|col| {
                                table_schema
                                    .fields()
                                    .iter()
                                    .position(|f| &f.name == col)
                                    .map(|i| Value::int64((i + 1) as i64))
                            })
                            .collect();
                        rows.push(Row::from_values(vec![
                            Value::int64(conoid),
                            Value::string(conname),
                            Value::int64(nspoid),
                            Value::string("f".to_string()),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(true),
                            Value::int64(reloid),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::string("a".to_string()),
                            Value::string("a".to_string()),
                            Value::string("s".to_string()),
                            Value::bool_val(true),
                            Value::int64(0),
                            Value::bool_val(true),
                            Value::array(conkey),
                            Value::null(),
                        ]));
                        conoid += 1;
                    }

                    for (idx, check) in table_schema.check_constraints().iter().enumerate() {
                        let conname = check
                            .name
                            .clone()
                            .unwrap_or_else(|| format!("{}_check_{}", table_name, idx));
                        rows.push(Row::from_values(vec![
                            Value::int64(conoid),
                            Value::string(conname),
                            Value::int64(nspoid),
                            Value::string("c".to_string()),
                            Value::bool_val(false),
                            Value::bool_val(false),
                            Value::bool_val(true),
                            Value::int64(reloid),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::int64(0),
                            Value::string(" ".to_string()),
                            Value::string(" ".to_string()),
                            Value::string(" ".to_string()),
                            Value::bool_val(true),
                            Value::int64(0),
                            Value::bool_val(true),
                            Value::null(),
                            Value::null(),
                        ]));
                        conoid += 1;
                    }

                    reloid += 1;
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_index(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("indexrelid", DataType::Oid),
            Field::nullable("indrelid", DataType::Oid),
            Field::nullable("indnatts", DataType::Int64),
            Field::nullable("indnkeyatts", DataType::Int64),
            Field::nullable("indisunique", DataType::Bool),
            Field::nullable("indisprimary", DataType::Bool),
            Field::nullable("indisexclusion", DataType::Bool),
            Field::nullable("indimmediate", DataType::Bool),
            Field::nullable("indisclustered", DataType::Bool),
            Field::nullable("indisvalid", DataType::Bool),
            Field::nullable("indcheckxmin", DataType::Bool),
            Field::nullable("indisready", DataType::Bool),
            Field::nullable("indislive", DataType::Bool),
            Field::nullable("indisreplident", DataType::Bool),
            Field::nullable("indkey", DataType::Array(Box::new(DataType::Int64))),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_proc(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("proname", DataType::String),
            Field::nullable("pronamespace", DataType::Oid),
            Field::nullable("proowner", DataType::Oid),
            Field::nullable("prolang", DataType::Oid),
            Field::nullable("procost", DataType::Float64),
            Field::nullable("prorows", DataType::Float64),
            Field::nullable("provariadic", DataType::Oid),
            Field::nullable("prosupport", DataType::String),
            Field::nullable("prokind", DataType::String),
            Field::nullable("prosecdef", DataType::Bool),
            Field::nullable("proleakproof", DataType::Bool),
            Field::nullable("proisstrict", DataType::Bool),
            Field::nullable("proretset", DataType::Bool),
            Field::nullable("provolatile", DataType::String),
            Field::nullable("proparallel", DataType::String),
            Field::nullable("pronargs", DataType::Int64),
            Field::nullable("pronargdefaults", DataType::Int64),
            Field::nullable("prorettype", DataType::Oid),
            Field::nullable("proargtypes", DataType::Array(Box::new(DataType::Oid))),
        ]);

        let rows = Vec::new();
        Ok((schema, rows))
    }

    fn get_pg_trigger(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("tgrelid", DataType::Oid),
            Field::nullable("tgparentid", DataType::Oid),
            Field::nullable("tgname", DataType::String),
            Field::nullable("tgfoid", DataType::Oid),
            Field::nullable("tgtype", DataType::Int64),
            Field::nullable("tgenabled", DataType::String),
            Field::nullable("tgisinternal", DataType::Bool),
            Field::nullable("tgconstrrelid", DataType::Oid),
            Field::nullable("tgconstrindid", DataType::Oid),
            Field::nullable("tgconstraint", DataType::Oid),
            Field::nullable("tgdeferrable", DataType::Bool),
            Field::nullable("tginitdeferred", DataType::Bool),
            Field::nullable("tgnargs", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_sequence(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("seqrelid", DataType::Oid),
            Field::nullable("seqtypid", DataType::Oid),
            Field::nullable("seqstart", DataType::Int64),
            Field::nullable("seqincrement", DataType::Int64),
            Field::nullable("seqmax", DataType::Int64),
            Field::nullable("seqmin", DataType::Int64),
            Field::nullable("seqcache", DataType::Int64),
            Field::nullable("seqcycle", DataType::Bool),
        ]);

        let rows = Vec::new();
        Ok((schema, rows))
    }

    fn get_pg_attrdef(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("adrelid", DataType::Oid),
            Field::nullable("adnum", DataType::Int64),
            Field::nullable("adbin", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut defoid = 70000i64;
        let mut reloid = 16384i64;

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (_, table) in dataset.tables() {
                    let table_schema = table.schema();
                    for (attnum, field) in table_schema.fields().iter().enumerate() {
                        if let Some(def) = &field.default_value {
                            rows.push(Row::from_values(vec![
                                Value::int64(defoid),
                                Value::int64(reloid),
                                Value::int64((attnum + 1) as i64),
                                Value::string(format!("{:?}", def)),
                            ]));
                            defoid += 1;
                        }
                    }
                    reloid += 1;
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_depend(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("classid", DataType::Oid),
            Field::nullable("objid", DataType::Oid),
            Field::nullable("objsubid", DataType::Int64),
            Field::nullable("refclassid", DataType::Oid),
            Field::nullable("refobjid", DataType::Oid),
            Field::nullable("refobjsubid", DataType::Int64),
            Field::nullable("deptype", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_shdepend(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("dbid", DataType::Oid),
            Field::nullable("classid", DataType::Oid),
            Field::nullable("objid", DataType::Oid),
            Field::nullable("objsubid", DataType::Int64),
            Field::nullable("refclassid", DataType::Oid),
            Field::nullable("refobjid", DataType::Oid),
            Field::nullable("deptype", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_tablespace(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("spcname", DataType::String),
            Field::nullable("spcowner", DataType::Oid),
        ]);

        let rows = vec![
            Row::from_values(vec![
                Value::int64(1663),
                Value::string("pg_default".to_string()),
                Value::int64(10),
            ]),
            Row::from_values(vec![
                Value::int64(1664),
                Value::string("pg_global".to_string()),
                Value::int64(10),
            ]),
        ];

        Ok((schema, rows))
    }

    fn get_pg_authid(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("rolname", DataType::String),
            Field::nullable("rolsuper", DataType::Bool),
            Field::nullable("rolinherit", DataType::Bool),
            Field::nullable("rolcreaterole", DataType::Bool),
            Field::nullable("rolcreatedb", DataType::Bool),
            Field::nullable("rolcanlogin", DataType::Bool),
            Field::nullable("rolreplication", DataType::Bool),
            Field::nullable("rolbypassrls", DataType::Bool),
            Field::nullable("rolconnlimit", DataType::Int64),
            Field::nullable("rolpassword", DataType::String),
            Field::nullable("rolvaliduntil", DataType::Timestamp),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::int64(10),
            Value::string("postgres".to_string()),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::int64(-1),
            Value::null(),
            Value::null(),
        ])];

        Ok((schema, rows))
    }

    fn get_pg_auth_members(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("roleid", DataType::Oid),
            Field::nullable("member", DataType::Oid),
            Field::nullable("grantor", DataType::Oid),
            Field::nullable("admin_option", DataType::Bool),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_shadow(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("usename", DataType::String),
            Field::nullable("usesysid", DataType::Oid),
            Field::nullable("usecreatedb", DataType::Bool),
            Field::nullable("usesuper", DataType::Bool),
            Field::nullable("userepl", DataType::Bool),
            Field::nullable("usebypassrls", DataType::Bool),
            Field::nullable("passwd", DataType::String),
            Field::nullable("valuntil", DataType::Timestamp),
            Field::nullable("useconfig", DataType::Array(Box::new(DataType::String))),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("postgres".to_string()),
            Value::int64(10),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::null(),
            Value::null(),
            Value::null(),
        ])];

        Ok((schema, rows))
    }

    fn get_pg_group(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("groname", DataType::String),
            Field::nullable("grosysid", DataType::Oid),
            Field::nullable("grolist", DataType::Array(Box::new(DataType::Oid))),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_user(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("usename", DataType::String),
            Field::nullable("usesysid", DataType::Oid),
            Field::nullable("usecreatedb", DataType::Bool),
            Field::nullable("usesuper", DataType::Bool),
            Field::nullable("userepl", DataType::Bool),
            Field::nullable("usebypassrls", DataType::Bool),
            Field::nullable("passwd", DataType::String),
            Field::nullable("valuntil", DataType::Timestamp),
            Field::nullable("useconfig", DataType::Array(Box::new(DataType::String))),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("postgres".to_string()),
            Value::int64(10),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::bool_val(true),
            Value::string("********".to_string()),
            Value::null(),
            Value::null(),
        ])];

        Ok((schema, rows))
    }

    fn get_pg_stat_activity(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("datid", DataType::Oid),
            Field::nullable("datname", DataType::String),
            Field::nullable("pid", DataType::Int64),
            Field::nullable("leader_pid", DataType::Int64),
            Field::nullable("usesysid", DataType::Oid),
            Field::nullable("usename", DataType::String),
            Field::nullable("application_name", DataType::String),
            Field::nullable("client_addr", DataType::String),
            Field::nullable("client_hostname", DataType::String),
            Field::nullable("client_port", DataType::Int64),
            Field::nullable("backend_start", DataType::Timestamp),
            Field::nullable("xact_start", DataType::Timestamp),
            Field::nullable("query_start", DataType::Timestamp),
            Field::nullable("state_change", DataType::Timestamp),
            Field::nullable("wait_event_type", DataType::String),
            Field::nullable("wait_event", DataType::String),
            Field::nullable("state", DataType::String),
            Field::nullable("backend_xid", DataType::Int64),
            Field::nullable("backend_xmin", DataType::Int64),
            Field::nullable("query_id", DataType::Int64),
            Field::nullable("query", DataType::String),
            Field::nullable("backend_type", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_locks(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("locktype", DataType::String),
            Field::nullable("database", DataType::Oid),
            Field::nullable("relation", DataType::Oid),
            Field::nullable("page", DataType::Int64),
            Field::nullable("tuple", DataType::Int64),
            Field::nullable("virtualxid", DataType::String),
            Field::nullable("transactionid", DataType::Int64),
            Field::nullable("classid", DataType::Oid),
            Field::nullable("objid", DataType::Oid),
            Field::nullable("objsubid", DataType::Int64),
            Field::nullable("virtualtransaction", DataType::String),
            Field::nullable("pid", DataType::Int64),
            Field::nullable("mode", DataType::String),
            Field::nullable("granted", DataType::Bool),
            Field::nullable("fastpath", DataType::Bool),
            Field::nullable("waitstart", DataType::Timestamp),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_stat_user_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("relid", DataType::Oid),
            Field::nullable("schemaname", DataType::String),
            Field::nullable("relname", DataType::String),
            Field::nullable("seq_scan", DataType::Int64),
            Field::nullable("seq_tup_read", DataType::Int64),
            Field::nullable("idx_scan", DataType::Int64),
            Field::nullable("idx_tup_fetch", DataType::Int64),
            Field::nullable("n_tup_ins", DataType::Int64),
            Field::nullable("n_tup_upd", DataType::Int64),
            Field::nullable("n_tup_del", DataType::Int64),
            Field::nullable("n_tup_hot_upd", DataType::Int64),
            Field::nullable("n_live_tup", DataType::Int64),
            Field::nullable("n_dead_tup", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut reloid = 16384i64;

        for dataset_id in storage.list_datasets() {
            let schema_name = Self::dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for table_name in dataset.tables().keys() {
                    rows.push(Row::from_values(vec![
                        Value::int64(reloid),
                        Value::string(schema_name.clone()),
                        Value::string(table_name.clone()),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                        Value::int64(0),
                    ]));
                    reloid += 1;
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_pg_stat_user_indexes(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("relid", DataType::Oid),
            Field::nullable("indexrelid", DataType::Oid),
            Field::nullable("schemaname", DataType::String),
            Field::nullable("relname", DataType::String),
            Field::nullable("indexrelname", DataType::String),
            Field::nullable("idx_scan", DataType::Int64),
            Field::nullable("idx_tup_read", DataType::Int64),
            Field::nullable("idx_tup_fetch", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_statio_user_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("relid", DataType::Oid),
            Field::nullable("schemaname", DataType::String),
            Field::nullable("relname", DataType::String),
            Field::nullable("heap_blks_read", DataType::Int64),
            Field::nullable("heap_blks_hit", DataType::Int64),
            Field::nullable("idx_blks_read", DataType::Int64),
            Field::nullable("idx_blks_hit", DataType::Int64),
            Field::nullable("toast_blks_read", DataType::Int64),
            Field::nullable("toast_blks_hit", DataType::Int64),
            Field::nullable("tidx_blks_read", DataType::Int64),
            Field::nullable("tidx_blks_hit", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_statio_user_indexes(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("relid", DataType::Oid),
            Field::nullable("indexrelid", DataType::Oid),
            Field::nullable("schemaname", DataType::String),
            Field::nullable("relname", DataType::String),
            Field::nullable("indexrelname", DataType::String),
            Field::nullable("idx_blks_read", DataType::Int64),
            Field::nullable("idx_blks_hit", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_stat(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("schemaname", DataType::String),
            Field::nullable("tablename", DataType::String),
            Field::nullable("attname", DataType::String),
            Field::nullable("inherited", DataType::Bool),
            Field::nullable("null_frac", DataType::Float64),
            Field::nullable("avg_width", DataType::Int64),
            Field::nullable("n_distinct", DataType::Float64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_cursors(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("statement", DataType::String),
            Field::nullable("is_holdable", DataType::Bool),
            Field::nullable("is_binary", DataType::Bool),
            Field::nullable("is_scrollable", DataType::Bool),
            Field::nullable("creation_time", DataType::Timestamp),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_extension(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("extname", DataType::String),
            Field::nullable("extowner", DataType::Oid),
            Field::nullable("extnamespace", DataType::Oid),
            Field::nullable("extrelocatable", DataType::Bool),
            Field::nullable("extversion", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_available_extensions(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("default_version", DataType::String),
            Field::nullable("installed_version", DataType::String),
            Field::nullable("comment", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_enum(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("enumtypid", DataType::Oid),
            Field::nullable("enumsortorder", DataType::Float64),
            Field::nullable("enumlabel", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_range(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("rngtypid", DataType::Oid),
            Field::nullable("rngsubtype", DataType::Oid),
            Field::nullable("rngmultitypid", DataType::Oid),
            Field::nullable("rngcollation", DataType::Oid),
            Field::nullable("rngsubopc", DataType::Oid),
            Field::nullable("rngcanonical", DataType::String),
            Field::nullable("rngsubdiff", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_collation(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("collname", DataType::String),
            Field::nullable("collnamespace", DataType::Oid),
            Field::nullable("collowner", DataType::Oid),
            Field::nullable("collprovider", DataType::String),
            Field::nullable("collisdeterministic", DataType::Bool),
            Field::nullable("collencoding", DataType::Int64),
            Field::nullable("collcollate", DataType::String),
            Field::nullable("collctype", DataType::String),
            Field::nullable("collversion", DataType::String),
        ]);

        let collations: Vec<(i64, &str, &str, i64)> = vec![
            (100, "default", "c", -1),
            (950, "C", "c", -1),
            (951, "POSIX", "c", -1),
        ];

        let mut rows = Vec::new();
        for (oid, name, provider, encoding) in collations {
            rows.push(Row::from_values(vec![
                Value::int64(oid),
                Value::string(name.to_string()),
                Value::int64(11),
                Value::int64(10),
                Value::string(provider.to_string()),
                Value::bool_val(true),
                Value::int64(encoding),
                Value::string(name.to_string()),
                Value::string(name.to_string()),
                Value::null(),
            ]));
        }

        Ok((schema, rows))
    }

    fn get_pg_operator(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("oprname", DataType::String),
            Field::nullable("oprnamespace", DataType::Oid),
            Field::nullable("oprowner", DataType::Oid),
            Field::nullable("oprkind", DataType::String),
            Field::nullable("oprcanmerge", DataType::Bool),
            Field::nullable("oprcanhash", DataType::Bool),
            Field::nullable("oprleft", DataType::Oid),
            Field::nullable("oprright", DataType::Oid),
            Field::nullable("oprresult", DataType::Oid),
            Field::nullable("oprcom", DataType::Oid),
            Field::nullable("oprnegate", DataType::Oid),
            Field::nullable("oprcode", DataType::String),
            Field::nullable("oprrest", DataType::String),
            Field::nullable("oprjoin", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_aggregate(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("aggfnoid", DataType::Oid),
            Field::nullable("aggkind", DataType::String),
            Field::nullable("aggnumdirectargs", DataType::Int64),
            Field::nullable("aggtransfn", DataType::String),
            Field::nullable("aggfinalfn", DataType::String),
            Field::nullable("aggcombinefn", DataType::String),
            Field::nullable("aggserialfn", DataType::String),
            Field::nullable("aggdeserialfn", DataType::String),
            Field::nullable("aggmtransfn", DataType::String),
            Field::nullable("aggminvtransfn", DataType::String),
            Field::nullable("aggmfinalfn", DataType::String),
            Field::nullable("aggfinalextra", DataType::Bool),
            Field::nullable("aggmfinalextra", DataType::Bool),
            Field::nullable("aggfinalmodify", DataType::String),
            Field::nullable("aggmfinalmodify", DataType::String),
            Field::nullable("aggsortop", DataType::Oid),
            Field::nullable("aggtranstype", DataType::Oid),
            Field::nullable("aggtransspace", DataType::Int64),
            Field::nullable("aggmtranstype", DataType::Oid),
            Field::nullable("aggmtransspace", DataType::Int64),
            Field::nullable("agginitval", DataType::String),
            Field::nullable("aggminitval", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_cast(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("castsource", DataType::Oid),
            Field::nullable("casttarget", DataType::Oid),
            Field::nullable("castfunc", DataType::Oid),
            Field::nullable("castcontext", DataType::String),
            Field::nullable("castmethod", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_conversion(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("conname", DataType::String),
            Field::nullable("connamespace", DataType::Oid),
            Field::nullable("conowner", DataType::Oid),
            Field::nullable("conforencoding", DataType::Int64),
            Field::nullable("contoencoding", DataType::Int64),
            Field::nullable("conproc", DataType::String),
            Field::nullable("condefault", DataType::Bool),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_language(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("lanname", DataType::String),
            Field::nullable("lanowner", DataType::Oid),
            Field::nullable("lanispl", DataType::Bool),
            Field::nullable("lanpltrusted", DataType::Bool),
            Field::nullable("lanplcallfoid", DataType::Oid),
            Field::nullable("laninline", DataType::Oid),
            Field::nullable("lanvalidator", DataType::Oid),
        ]);

        let rows = vec![
            Row::from_values(vec![
                Value::int64(12),
                Value::string("internal".to_string()),
                Value::int64(10),
                Value::bool_val(false),
                Value::bool_val(false),
                Value::int64(0),
                Value::int64(0),
                Value::int64(2246),
            ]),
            Row::from_values(vec![
                Value::int64(13),
                Value::string("c".to_string()),
                Value::int64(10),
                Value::bool_val(false),
                Value::bool_val(false),
                Value::int64(0),
                Value::int64(0),
                Value::int64(2247),
            ]),
            Row::from_values(vec![
                Value::int64(14),
                Value::string("sql".to_string()),
                Value::int64(10),
                Value::bool_val(false),
                Value::bool_val(true),
                Value::int64(0),
                Value::int64(0),
                Value::int64(2248),
            ]),
            Row::from_values(vec![
                Value::int64(13581),
                Value::string("plpgsql".to_string()),
                Value::int64(10),
                Value::bool_val(true),
                Value::bool_val(true),
                Value::int64(13578),
                Value::int64(13579),
                Value::int64(13580),
            ]),
        ];

        Ok((schema, rows))
    }

    fn get_pg_am(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("amname", DataType::String),
            Field::nullable("amhandler", DataType::String),
            Field::nullable("amtype", DataType::String),
        ]);

        let rows = vec![
            Row::from_values(vec![
                Value::int64(2),
                Value::string("heap".to_string()),
                Value::string("heap_tableam_handler".to_string()),
                Value::string("t".to_string()),
            ]),
            Row::from_values(vec![
                Value::int64(403),
                Value::string("btree".to_string()),
                Value::string("bthandler".to_string()),
                Value::string("i".to_string()),
            ]),
            Row::from_values(vec![
                Value::int64(405),
                Value::string("hash".to_string()),
                Value::string("hashhandler".to_string()),
                Value::string("i".to_string()),
            ]),
            Row::from_values(vec![
                Value::int64(783),
                Value::string("gist".to_string()),
                Value::string("gisthandler".to_string()),
                Value::string("i".to_string()),
            ]),
            Row::from_values(vec![
                Value::int64(2742),
                Value::string("gin".to_string()),
                Value::string("ginhandler".to_string()),
                Value::string("i".to_string()),
            ]),
            Row::from_values(vec![
                Value::int64(4000),
                Value::string("spgist".to_string()),
                Value::string("spghandler".to_string()),
                Value::string("i".to_string()),
            ]),
            Row::from_values(vec![
                Value::int64(3580),
                Value::string("brin".to_string()),
                Value::string("brinhandler".to_string()),
                Value::string("i".to_string()),
            ]),
        ];

        Ok((schema, rows))
    }

    fn get_pg_amop(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("amopfamily", DataType::Oid),
            Field::nullable("amoplefttype", DataType::Oid),
            Field::nullable("amoprighttype", DataType::Oid),
            Field::nullable("amopstrategy", DataType::Int64),
            Field::nullable("amoppurpose", DataType::String),
            Field::nullable("amopopr", DataType::Oid),
            Field::nullable("amopmethod", DataType::Oid),
            Field::nullable("amopsortfamily", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_amproc(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("amprocfamily", DataType::Oid),
            Field::nullable("amproclefttype", DataType::Oid),
            Field::nullable("amprocrighttype", DataType::Oid),
            Field::nullable("amprocnum", DataType::Int64),
            Field::nullable("amproc", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_opclass(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("opcmethod", DataType::Oid),
            Field::nullable("opcname", DataType::String),
            Field::nullable("opcnamespace", DataType::Oid),
            Field::nullable("opcowner", DataType::Oid),
            Field::nullable("opcfamily", DataType::Oid),
            Field::nullable("opcintype", DataType::Oid),
            Field::nullable("opcdefault", DataType::Bool),
            Field::nullable("opckeytype", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_opfamily(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("opfmethod", DataType::Oid),
            Field::nullable("opfname", DataType::String),
            Field::nullable("opfnamespace", DataType::Oid),
            Field::nullable("opfowner", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_rewrite(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("rulename", DataType::String),
            Field::nullable("ev_class", DataType::Oid),
            Field::nullable("ev_type", DataType::String),
            Field::nullable("ev_enabled", DataType::String),
            Field::nullable("is_instead", DataType::Bool),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_statistic(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("starelid", DataType::Oid),
            Field::nullable("staattnum", DataType::Int64),
            Field::nullable("stainherit", DataType::Bool),
            Field::nullable("stanullfrac", DataType::Float64),
            Field::nullable("stawidth", DataType::Int64),
            Field::nullable("stadistinct", DataType::Float64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_inherits(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("inhrelid", DataType::Oid),
            Field::nullable("inhparent", DataType::Oid),
            Field::nullable("inhseqno", DataType::Int64),
            Field::nullable("inhdetachpending", DataType::Bool),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_foreign_data_wrapper(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("fdwname", DataType::String),
            Field::nullable("fdwowner", DataType::Oid),
            Field::nullable("fdwhandler", DataType::Oid),
            Field::nullable("fdwvalidator", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_foreign_server(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("srvname", DataType::String),
            Field::nullable("srvowner", DataType::Oid),
            Field::nullable("srvfdw", DataType::Oid),
            Field::nullable("srvtype", DataType::String),
            Field::nullable("srvversion", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_foreign_table(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("ftrelid", DataType::Oid),
            Field::nullable("ftserver", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_user_mapping(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("umuser", DataType::Oid),
            Field::nullable("umserver", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_policy(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("polname", DataType::String),
            Field::nullable("polrelid", DataType::Oid),
            Field::nullable("polcmd", DataType::String),
            Field::nullable("polpermissive", DataType::Bool),
            Field::nullable("polroles", DataType::Array(Box::new(DataType::Oid))),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_event_trigger(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("evtname", DataType::String),
            Field::nullable("evtevent", DataType::String),
            Field::nullable("evtowner", DataType::Oid),
            Field::nullable("evtfoid", DataType::Oid),
            Field::nullable("evtenabled", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_publication(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("pubname", DataType::String),
            Field::nullable("pubowner", DataType::Oid),
            Field::nullable("puballtables", DataType::Bool),
            Field::nullable("pubinsert", DataType::Bool),
            Field::nullable("pubupdate", DataType::Bool),
            Field::nullable("pubdelete", DataType::Bool),
            Field::nullable("pubtruncate", DataType::Bool),
            Field::nullable("pubviaroot", DataType::Bool),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_subscription(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("subdbid", DataType::Oid),
            Field::nullable("subname", DataType::String),
            Field::nullable("subowner", DataType::Oid),
            Field::nullable("subenabled", DataType::Bool),
            Field::nullable("subconninfo", DataType::String),
            Field::nullable("subslotname", DataType::String),
            Field::nullable("subsynccommit", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_stat_replication(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("pid", DataType::Int64),
            Field::nullable("usesysid", DataType::Oid),
            Field::nullable("usename", DataType::String),
            Field::nullable("application_name", DataType::String),
            Field::nullable("client_addr", DataType::String),
            Field::nullable("client_hostname", DataType::String),
            Field::nullable("client_port", DataType::Int64),
            Field::nullable("backend_start", DataType::Timestamp),
            Field::nullable("backend_xmin", DataType::Int64),
            Field::nullable("state", DataType::String),
            Field::nullable("sent_lsn", DataType::String),
            Field::nullable("write_lsn", DataType::String),
            Field::nullable("flush_lsn", DataType::String),
            Field::nullable("replay_lsn", DataType::String),
            Field::nullable("write_lag", DataType::Interval),
            Field::nullable("flush_lag", DataType::Interval),
            Field::nullable("replay_lag", DataType::Interval),
            Field::nullable("sync_priority", DataType::Int64),
            Field::nullable("sync_state", DataType::String),
            Field::nullable("reply_time", DataType::Timestamp),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_stat_wal_receiver(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("pid", DataType::Int64),
            Field::nullable("status", DataType::String),
            Field::nullable("receive_start_lsn", DataType::String),
            Field::nullable("receive_start_tli", DataType::Int64),
            Field::nullable("written_lsn", DataType::String),
            Field::nullable("flushed_lsn", DataType::String),
            Field::nullable("received_tli", DataType::Int64),
            Field::nullable("last_msg_send_time", DataType::Timestamp),
            Field::nullable("last_msg_receipt_time", DataType::Timestamp),
            Field::nullable("latest_end_lsn", DataType::String),
            Field::nullable("latest_end_time", DataType::Timestamp),
            Field::nullable("slot_name", DataType::String),
            Field::nullable("sender_host", DataType::String),
            Field::nullable("sender_port", DataType::Int64),
            Field::nullable("conninfo", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_prepared_statements(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("statement", DataType::String),
            Field::nullable("prepare_time", DataType::Timestamp),
            Field::nullable("parameter_types", DataType::Array(Box::new(DataType::Oid))),
            Field::nullable("from_sql", DataType::Bool),
            Field::nullable("generic_plans", DataType::Int64),
            Field::nullable("custom_plans", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_prepared_xacts(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("transaction", DataType::Int64),
            Field::nullable("gid", DataType::String),
            Field::nullable("prepared", DataType::Timestamp),
            Field::nullable("owner", DataType::String),
            Field::nullable("database", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_seclabel(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("objoid", DataType::Oid),
            Field::nullable("classoid", DataType::Oid),
            Field::nullable("objsubid", DataType::Int64),
            Field::nullable("provider", DataType::String),
            Field::nullable("label", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_shseclabel(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("objoid", DataType::Oid),
            Field::nullable("classoid", DataType::Oid),
            Field::nullable("provider", DataType::String),
            Field::nullable("label", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_largeobject(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("loid", DataType::Oid),
            Field::nullable("pageno", DataType::Int64),
            Field::nullable("data", DataType::Bytes),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_largeobject_metadata(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("lomowner", DataType::Oid),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_shdescription(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("objoid", DataType::Oid),
            Field::nullable("classoid", DataType::Oid),
            Field::nullable("description", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_db_role_setting(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("setdatabase", DataType::Oid),
            Field::nullable("setrole", DataType::Oid),
            Field::nullable("setconfig", DataType::Array(Box::new(DataType::String))),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_pg_ts(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("oid", DataType::Oid),
            Field::nullable("cfgname", DataType::String),
            Field::nullable("cfgnamespace", DataType::Oid),
            Field::nullable("cfgowner", DataType::Oid),
            Field::nullable("cfgparser", DataType::Oid),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::int64(3748),
            Value::string("simple".to_string()),
            Value::int64(11),
            Value::int64(10),
            Value::int64(3722),
        ])];

        Ok((schema, rows))
    }

    fn get_pg_matviews(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("schemaname", DataType::String),
            Field::nullable("matviewname", DataType::String),
            Field::nullable("matviewowner", DataType::String),
            Field::nullable("tablespace", DataType::String),
            Field::nullable("hasindexes", DataType::Bool),
            Field::nullable("ispopulated", DataType::Bool),
            Field::nullable("definition", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            let schema_name = Self::dataset_to_schema_name(dataset_id);
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for view_name in dataset.views().list_views() {
                    if let Some(view) = dataset.views().get_view(&view_name) {
                        if view.is_materialized() {
                            rows.push(Row::from_values(vec![
                                Value::string(schema_name.clone()),
                                Value::string(view_name),
                                Value::string("postgres".to_string()),
                                Value::null(),
                                Value::bool_val(false),
                                Value::bool_val(true),
                                Value::string(view.sql.clone()),
                            ]));
                        }
                    }
                }
            }
        }

        Ok((schema, rows))
    }
}
