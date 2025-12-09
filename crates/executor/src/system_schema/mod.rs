use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{DataType, Value};
use yachtsql_storage::{Field, Row, Schema, Storage};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SystemTable {
    Tables,
    Columns,
    Databases,
    Functions,
    DataTypeFamilies,
    Settings,
    Processes,
    QueryLog,
    Parts,
    Merges,
    Mutations,
    Replicas,
    Dictionaries,
    Users,
    Roles,
    Grants,
    Quotas,
    RowPolicies,
    SettingsProfiles,
    Metrics,
    Events,
    AsynchronousMetrics,
    Disks,
    StoragePolicies,
    Clusters,
    TableEngines,
    Formats,
    Collations,
    Contributors,
    BuildOptions,
    TimeZones,
    One,
}

impl SystemTable {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "tables" => Ok(SystemTable::Tables),
            "columns" => Ok(SystemTable::Columns),
            "databases" => Ok(SystemTable::Databases),
            "functions" => Ok(SystemTable::Functions),
            "data_type_families" => Ok(SystemTable::DataTypeFamilies),
            "settings" => Ok(SystemTable::Settings),
            "processes" => Ok(SystemTable::Processes),
            "query_log" => Ok(SystemTable::QueryLog),
            "parts" => Ok(SystemTable::Parts),
            "merges" => Ok(SystemTable::Merges),
            "mutations" => Ok(SystemTable::Mutations),
            "replicas" => Ok(SystemTable::Replicas),
            "dictionaries" => Ok(SystemTable::Dictionaries),
            "users" => Ok(SystemTable::Users),
            "roles" => Ok(SystemTable::Roles),
            "grants" => Ok(SystemTable::Grants),
            "quotas" => Ok(SystemTable::Quotas),
            "row_policies" => Ok(SystemTable::RowPolicies),
            "settings_profiles" => Ok(SystemTable::SettingsProfiles),
            "metrics" => Ok(SystemTable::Metrics),
            "events" => Ok(SystemTable::Events),
            "asynchronous_metrics" => Ok(SystemTable::AsynchronousMetrics),
            "disks" => Ok(SystemTable::Disks),
            "storage_policies" => Ok(SystemTable::StoragePolicies),
            "clusters" => Ok(SystemTable::Clusters),
            "table_engines" => Ok(SystemTable::TableEngines),
            "formats" => Ok(SystemTable::Formats),
            "collations" => Ok(SystemTable::Collations),
            "contributors" => Ok(SystemTable::Contributors),
            "build_options" => Ok(SystemTable::BuildOptions),
            "time_zones" => Ok(SystemTable::TimeZones),
            "one" => Ok(SystemTable::One),
            _ => Err(Error::table_not_found(format!(
                "system.{} is not supported",
                s
            ))),
        }
    }
}

pub struct SystemSchemaProvider {
    storage: Rc<RefCell<Storage>>,
}

impl SystemSchemaProvider {
    pub fn new(storage: Rc<RefCell<Storage>>) -> Self {
        Self { storage }
    }

    pub fn query(&self, table: SystemTable) -> Result<(Schema, Vec<Row>)> {
        match table {
            SystemTable::Tables => self.get_tables(),
            SystemTable::Columns => self.get_columns(),
            SystemTable::Databases => self.get_databases(),
            SystemTable::Functions => self.get_functions(),
            SystemTable::DataTypeFamilies => self.get_data_type_families(),
            SystemTable::Settings => self.get_settings(),
            SystemTable::Processes => self.get_processes(),
            SystemTable::QueryLog => self.get_query_log(),
            SystemTable::Parts => self.get_parts(),
            SystemTable::Merges => self.get_merges(),
            SystemTable::Mutations => self.get_mutations(),
            SystemTable::Replicas => self.get_replicas(),
            SystemTable::Dictionaries => self.get_dictionaries(),
            SystemTable::Users => self.get_users(),
            SystemTable::Roles => self.get_roles(),
            SystemTable::Grants => self.get_grants(),
            SystemTable::Quotas => self.get_quotas(),
            SystemTable::RowPolicies => self.get_row_policies(),
            SystemTable::SettingsProfiles => self.get_settings_profiles(),
            SystemTable::Metrics => self.get_metrics(),
            SystemTable::Events => self.get_events(),
            SystemTable::AsynchronousMetrics => self.get_asynchronous_metrics(),
            SystemTable::Disks => self.get_disks(),
            SystemTable::StoragePolicies => self.get_storage_policies(),
            SystemTable::Clusters => self.get_clusters(),
            SystemTable::TableEngines => self.get_table_engines(),
            SystemTable::Formats => self.get_formats(),
            SystemTable::Collations => self.get_collations(),
            SystemTable::Contributors => self.get_contributors(),
            SystemTable::BuildOptions => self.get_build_options(),
            SystemTable::TimeZones => self.get_time_zones(),
            SystemTable::One => self.get_one(),
        }
    }

    fn get_one(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![Field::required("dummy", DataType::Int64)]);
        let rows = vec![Row::from_values(vec![Value::int64(0)])];
        Ok((schema, rows))
    }

    fn get_tables(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("database", DataType::String),
            Field::nullable("name", DataType::String),
            Field::nullable("engine", DataType::String),
            Field::nullable("is_temporary", DataType::Int64),
            Field::nullable("data_paths", DataType::Array(Box::new(DataType::String))),
            Field::nullable("metadata_path", DataType::String),
            Field::nullable("uuid", DataType::String),
            Field::nullable("engine_full", DataType::String),
            Field::nullable("create_table_query", DataType::String),
            Field::nullable("total_rows", DataType::Int64),
            Field::nullable("total_bytes", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for table_name in dataset.tables().keys() {
                    rows.push(Row::from_values(vec![
                        Value::string(dataset_id.clone()),
                        Value::string(table_name.clone()),
                        Value::string("MergeTree".to_string()),
                        Value::int64(0),
                        Value::array(vec![]),
                        Value::string("".to_string()),
                        Value::string("".to_string()),
                        Value::string("MergeTree".to_string()),
                        Value::string("".to_string()),
                        Value::int64(0),
                        Value::int64(0),
                    ]));
                }

                for view_name in dataset.views().list_views() {
                    let engine = if dataset
                        .views()
                        .get_view(&view_name)
                        .map(|v| v.is_materialized())
                        .unwrap_or(false)
                    {
                        "MaterializedView"
                    } else {
                        "View"
                    };
                    rows.push(Row::from_values(vec![
                        Value::string(dataset_id.clone()),
                        Value::string(view_name),
                        Value::string(engine.to_string()),
                        Value::int64(0),
                        Value::array(vec![]),
                        Value::string("".to_string()),
                        Value::string("".to_string()),
                        Value::string(engine.to_string()),
                        Value::string("".to_string()),
                        Value::int64(0),
                        Value::int64(0),
                    ]));
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_columns(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("name", DataType::String),
            Field::nullable("type", DataType::String),
            Field::nullable("position", DataType::Int64),
            Field::nullable("default_kind", DataType::String),
            Field::nullable("default_expression", DataType::String),
            Field::nullable("comment", DataType::String),
            Field::nullable("is_in_partition_key", DataType::Int64),
            Field::nullable("is_in_sorting_key", DataType::Int64),
            Field::nullable("is_in_primary_key", DataType::Int64),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();

        for dataset_id in storage.list_datasets() {
            if let Some(dataset) = storage.get_dataset(dataset_id) {
                for (table_name, table) in dataset.tables() {
                    let table_schema = table.schema();
                    let pk_columns = table_schema.primary_key();

                    for (position, field) in table_schema.fields().iter().enumerate() {
                        let is_in_pk = pk_columns
                            .map(|pk| pk.contains(&field.name))
                            .unwrap_or(false);

                        let default_kind = if field.default_value.is_some() {
                            "DEFAULT"
                        } else {
                            ""
                        };
                        let default_expr = field
                            .default_value
                            .as_ref()
                            .map(|d| format!("{:?}", d))
                            .unwrap_or_default();

                        rows.push(Row::from_values(vec![
                            Value::string(dataset_id.clone()),
                            Value::string(table_name.clone()),
                            Value::string(field.name.clone()),
                            Value::string(field.data_type.to_string()),
                            Value::int64((position + 1) as i64),
                            Value::string(default_kind.to_string()),
                            Value::string(default_expr),
                            Value::string(field.description.clone().unwrap_or_default()),
                            Value::int64(0),
                            Value::int64(if is_in_pk { 1 } else { 0 }),
                            Value::int64(if is_in_pk { 1 } else { 0 }),
                        ]));
                    }
                }
            }
        }

        Ok((schema, rows))
    }

    fn get_databases(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("engine", DataType::String),
            Field::nullable("data_path", DataType::String),
            Field::nullable("metadata_path", DataType::String),
            Field::nullable("uuid", DataType::String),
            Field::nullable("comment", DataType::String),
        ]);

        let storage = self.storage.borrow();
        let mut rows = Vec::new();
        let mut has_default = false;

        for dataset_id in storage.list_datasets() {
            if dataset_id == "default" {
                has_default = true;
            }
            rows.push(Row::from_values(vec![
                Value::string(dataset_id.clone()),
                Value::string("Atomic".to_string()),
                Value::string("".to_string()),
                Value::string("".to_string()),
                Value::string("".to_string()),
                Value::string("".to_string()),
            ]));
        }

        if !has_default {
            rows.insert(
                0,
                Row::from_values(vec![
                    Value::string("default".to_string()),
                    Value::string("Atomic".to_string()),
                    Value::string("".to_string()),
                    Value::string("".to_string()),
                    Value::string("".to_string()),
                    Value::string("".to_string()),
                ]),
            );
        }

        Ok((schema, rows))
    }

    fn get_functions(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("is_aggregate", DataType::Int64),
            Field::nullable("case_insensitive", DataType::Int64),
            Field::nullable("alias_to", DataType::String),
            Field::nullable("create_query", DataType::String),
            Field::nullable("origin", DataType::String),
            Field::nullable("description", DataType::String),
        ]);

        let scalar_functions = vec![
            "abs",
            "acos",
            "asin",
            "atan",
            "atan2",
            "ceil",
            "ceiling",
            "cos",
            "cot",
            "degrees",
            "exp",
            "floor",
            "ln",
            "log",
            "log10",
            "log2",
            "pi",
            "pow",
            "power",
            "radians",
            "round",
            "sign",
            "sin",
            "sqrt",
            "tan",
            "trunc",
            "truncate",
            "concat",
            "concat_ws",
            "length",
            "lower",
            "upper",
            "trim",
            "ltrim",
            "rtrim",
            "left",
            "right",
            "substr",
            "substring",
            "replace",
            "reverse",
            "repeat",
            "position",
            "locate",
            "instr",
            "char_length",
            "character_length",
            "bit_length",
            "octet_length",
            "ascii",
            "chr",
            "lpad",
            "rpad",
            "split_part",
            "format",
            "coalesce",
            "nullif",
            "ifnull",
            "nvl",
            "if",
            "case",
            "greatest",
            "least",
            "cast",
            "convert",
            "typeof",
            "toInt64",
            "toFloat64",
            "toString",
            "toDate",
            "toDateTime",
            "parseDateTimeBestEffort",
            "toUnixTimestamp",
            "now",
            "today",
            "yesterday",
            "toYear",
            "toMonth",
            "toDay",
            "toDayOfWeek",
            "toHour",
            "toMinute",
            "toSecond",
            "toStartOfDay",
            "toStartOfMonth",
            "toStartOfYear",
            "toStartOfWeek",
            "addDays",
            "addMonths",
            "addYears",
            "dateDiff",
            "dateTrunc",
            "formatDateTime",
            "arrayJoin",
            "array",
            "arrayElement",
            "arrayConcat",
            "arrayPushBack",
            "arrayPushFront",
            "arrayPopBack",
            "arrayPopFront",
            "arraySlice",
            "arraySort",
            "arrayReverse",
            "arrayDistinct",
            "arrayFlatten",
            "length",
            "empty",
            "notEmpty",
            "has",
            "hasAll",
            "hasAny",
            "MD5",
            "SHA1",
            "SHA256",
            "SHA512",
            "base64Encode",
            "base64Decode",
            "hex",
            "unhex",
            "URLEncode",
            "URLDecode",
        ];

        let aggregate_functions = vec![
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "any",
            "anyLast",
            "stddev",
            "stddevPop",
            "stddevSamp",
            "variance",
            "varPop",
            "varSamp",
            "groupArray",
            "groupArraySample",
            "groupUniqArray",
            "groupBitAnd",
            "groupBitOr",
            "groupBitXor",
            "argMin",
            "argMax",
            "median",
            "quantile",
            "quantiles",
            "topK",
            "topKWeighted",
            "uniq",
            "uniqExact",
            "uniqCombined",
            "sumWithOverflow",
            "sumMap",
            "avgWeighted",
            "first_value",
            "last_value",
            "nth_value",
            "row_number",
            "rank",
            "dense_rank",
            "percent_rank",
            "cume_dist",
            "ntile",
            "lead",
            "lag",
        ];

        let mut rows = Vec::new();

        for func_name in scalar_functions {
            rows.push(Row::from_values(vec![
                Value::string(func_name.to_string()),
                Value::int64(0),
                Value::int64(1),
                Value::string("".to_string()),
                Value::string("".to_string()),
                Value::string("System".to_string()),
                Value::string("".to_string()),
            ]));
        }

        for func_name in aggregate_functions {
            rows.push(Row::from_values(vec![
                Value::string(func_name.to_string()),
                Value::int64(1),
                Value::int64(1),
                Value::string("".to_string()),
                Value::string("".to_string()),
                Value::string("System".to_string()),
                Value::string("".to_string()),
            ]));
        }

        Ok((schema, rows))
    }

    fn get_data_type_families(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("case_insensitive", DataType::Int64),
            Field::nullable("alias_to", DataType::String),
        ]);

        let type_families = vec![
            "Int8",
            "Int16",
            "Int32",
            "Int64",
            "Int128",
            "Int256",
            "UInt8",
            "UInt16",
            "UInt32",
            "UInt64",
            "UInt128",
            "UInt256",
            "Float32",
            "Float64",
            "Decimal",
            "Decimal32",
            "Decimal64",
            "Decimal128",
            "Decimal256",
            "String",
            "FixedString",
            "UUID",
            "Date",
            "Date32",
            "DateTime",
            "DateTime64",
            "Enum8",
            "Enum16",
            "Array",
            "Tuple",
            "Map",
            "Nested",
            "Nullable",
            "LowCardinality",
            "IPv4",
            "IPv6",
            "Bool",
            "Boolean",
            "JSON",
            "Object",
            "Nothing",
            "Interval",
            "AggregateFunction",
            "SimpleAggregateFunction",
        ];

        let rows: Vec<Row> = type_families
            .into_iter()
            .map(|name| {
                Row::from_values(vec![
                    Value::string(name.to_string()),
                    Value::int64(1),
                    Value::string("".to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_settings(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("value", DataType::String),
            Field::nullable("changed", DataType::Int64),
            Field::nullable("description", DataType::String),
            Field::nullable("min", DataType::String),
            Field::nullable("max", DataType::String),
            Field::nullable("readonly", DataType::Int64),
            Field::nullable("type", DataType::String),
        ]);

        let settings = vec![
            (
                "max_memory_usage",
                "10000000000",
                "Maximum memory usage for processing a single query",
            ),
            (
                "max_threads",
                "8",
                "Maximum number of threads for query execution",
            ),
            (
                "max_execution_time",
                "0",
                "Maximum query execution time in seconds (0 = unlimited)",
            ),
            (
                "max_rows_to_read",
                "0",
                "Maximum rows to read from a table (0 = unlimited)",
            ),
            (
                "max_result_rows",
                "0",
                "Maximum rows in result (0 = unlimited)",
            ),
            (
                "timeout_before_checking_execution_speed",
                "10",
                "Timeout before checking execution speed",
            ),
            (
                "join_use_nulls",
                "0",
                "Use NULLs for non-joined rows in JOIN",
            ),
            (
                "any_join_distinct_right_table_keys",
                "0",
                "Enable old ANY JOIN behavior",
            ),
            (
                "input_format_null_as_default",
                "1",
                "Treat NULL as default value in input formats",
            ),
            (
                "output_format_json_quote_64bit_integers",
                "1",
                "Quote 64-bit integers in JSON output",
            ),
        ];

        let rows: Vec<Row> = settings
            .into_iter()
            .map(|(name, value, desc)| {
                Row::from_values(vec![
                    Value::string(name.to_string()),
                    Value::string(value.to_string()),
                    Value::int64(0),
                    Value::string(desc.to_string()),
                    Value::null(),
                    Value::null(),
                    Value::int64(0),
                    Value::string("String".to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_processes(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("is_initial_query", DataType::Int64),
            Field::nullable("user", DataType::String),
            Field::nullable("query_id", DataType::String),
            Field::nullable("address", DataType::String),
            Field::nullable("port", DataType::Int64),
            Field::nullable("initial_user", DataType::String),
            Field::nullable("initial_query_id", DataType::String),
            Field::nullable("query", DataType::String),
            Field::nullable("elapsed", DataType::Float64),
            Field::nullable("read_rows", DataType::Int64),
            Field::nullable("total_rows_approx", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_query_log(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("type", DataType::String),
            Field::nullable("event_date", DataType::Date),
            Field::nullable("event_time", DataType::DateTime),
            Field::nullable("query_id", DataType::String),
            Field::nullable("query", DataType::String),
            Field::nullable("query_duration_ms", DataType::Int64),
            Field::nullable("read_rows", DataType::Int64),
            Field::nullable("read_bytes", DataType::Int64),
            Field::nullable("written_rows", DataType::Int64),
            Field::nullable("written_bytes", DataType::Int64),
            Field::nullable("result_rows", DataType::Int64),
            Field::nullable("result_bytes", DataType::Int64),
            Field::nullable("memory_usage", DataType::Int64),
            Field::nullable("exception_code", DataType::Int64),
            Field::nullable("exception", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_parts(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("partition", DataType::String),
            Field::nullable("name", DataType::String),
            Field::nullable("active", DataType::Int64),
            Field::nullable("marks", DataType::Int64),
            Field::nullable("rows", DataType::Int64),
            Field::nullable("bytes_on_disk", DataType::Int64),
            Field::nullable("data_compressed_bytes", DataType::Int64),
            Field::nullable("data_uncompressed_bytes", DataType::Int64),
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("engine", DataType::String),
            Field::nullable("path", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_merges(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("elapsed", DataType::Float64),
            Field::nullable("progress", DataType::Float64),
            Field::nullable("num_parts", DataType::Int64),
            Field::nullable(
                "source_part_names",
                DataType::Array(Box::new(DataType::String)),
            ),
            Field::nullable("result_part_name", DataType::String),
            Field::nullable("total_size_bytes_compressed", DataType::Int64),
            Field::nullable("total_size_marks", DataType::Int64),
            Field::nullable("bytes_read_uncompressed", DataType::Int64),
            Field::nullable("rows_read", DataType::Int64),
            Field::nullable("bytes_written_uncompressed", DataType::Int64),
            Field::nullable("rows_written", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_mutations(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("mutation_id", DataType::String),
            Field::nullable("command", DataType::String),
            Field::nullable("create_time", DataType::DateTime),
            Field::nullable("parts_to_do", DataType::Int64),
            Field::nullable("is_done", DataType::Int64),
            Field::nullable("latest_failed_part", DataType::String),
            Field::nullable("latest_fail_time", DataType::DateTime),
            Field::nullable("latest_fail_reason", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_replicas(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("engine", DataType::String),
            Field::nullable("is_leader", DataType::Int64),
            Field::nullable("is_readonly", DataType::Int64),
            Field::nullable("is_session_expired", DataType::Int64),
            Field::nullable("future_parts", DataType::Int64),
            Field::nullable("parts_to_check", DataType::Int64),
            Field::nullable("zookeeper_path", DataType::String),
            Field::nullable("replica_name", DataType::String),
            Field::nullable("replica_path", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_dictionaries(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("database", DataType::String),
            Field::nullable("name", DataType::String),
            Field::nullable("uuid", DataType::String),
            Field::nullable("status", DataType::String),
            Field::nullable("origin", DataType::String),
            Field::nullable("type", DataType::String),
            Field::nullable("key", DataType::String),
            Field::nullable(
                "attribute.names",
                DataType::Array(Box::new(DataType::String)),
            ),
            Field::nullable(
                "attribute.types",
                DataType::Array(Box::new(DataType::String)),
            ),
            Field::nullable("bytes_allocated", DataType::Int64),
            Field::nullable("query_count", DataType::Int64),
            Field::nullable("hit_rate", DataType::Float64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_users(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("id", DataType::String),
            Field::nullable("storage", DataType::String),
            Field::nullable("auth_type", DataType::String),
            Field::nullable("auth_params", DataType::String),
            Field::nullable("host_ip", DataType::Array(Box::new(DataType::String))),
            Field::nullable("host_names", DataType::Array(Box::new(DataType::String))),
            Field::nullable("default_roles_all", DataType::Int64),
            Field::nullable(
                "default_roles_list",
                DataType::Array(Box::new(DataType::String)),
            ),
            Field::nullable(
                "default_roles_except",
                DataType::Array(Box::new(DataType::String)),
            ),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("default".to_string()),
            Value::string("".to_string()),
            Value::string("local_directory".to_string()),
            Value::string("no_password".to_string()),
            Value::string("".to_string()),
            Value::array(vec![]),
            Value::array(vec![]),
            Value::int64(1),
            Value::array(vec![]),
            Value::array(vec![]),
        ])];

        Ok((schema, rows))
    }

    fn get_roles(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("id", DataType::String),
            Field::nullable("storage", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_grants(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("user_name", DataType::String),
            Field::nullable("role_name", DataType::String),
            Field::nullable("access_type", DataType::String),
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("column", DataType::String),
            Field::nullable("is_partial_revoke", DataType::Int64),
            Field::nullable("grant_option", DataType::Int64),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_quotas(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("id", DataType::String),
            Field::nullable("storage", DataType::String),
            Field::nullable("keys", DataType::Array(Box::new(DataType::String))),
            Field::nullable("durations", DataType::Array(Box::new(DataType::Int64))),
            Field::nullable("apply_to_all", DataType::Int64),
            Field::nullable("apply_to_list", DataType::Array(Box::new(DataType::String))),
            Field::nullable(
                "apply_to_except",
                DataType::Array(Box::new(DataType::String)),
            ),
        ]);

        let storage = self.storage.borrow();
        let rows: Vec<Row> = storage
            .quotas()
            .map(|quota| {
                Row::from_values(vec![
                    Value::string(quota.name.clone()),
                    Value::string(quota.name.clone()),
                    Value::string("local_directory".to_string()),
                    Value::null(),
                    Value::null(),
                    Value::int64(0),
                    Value::null(),
                    Value::null(),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_row_policies(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("short_name", DataType::String),
            Field::nullable("database", DataType::String),
            Field::nullable("table", DataType::String),
            Field::nullable("id", DataType::String),
            Field::nullable("storage", DataType::String),
            Field::nullable("select_filter", DataType::String),
            Field::nullable("is_restrictive", DataType::Int64),
            Field::nullable("apply_to_all", DataType::Int64),
            Field::nullable("apply_to_list", DataType::Array(Box::new(DataType::String))),
            Field::nullable(
                "apply_to_except",
                DataType::Array(Box::new(DataType::String)),
            ),
        ]);

        let storage = self.storage.borrow();
        let rows: Vec<Row> = storage
            .row_policies()
            .map(|policy| {
                Row::from_values(vec![
                    Value::string(policy.name.clone()),
                    Value::string(policy.name.clone()),
                    Value::string(policy.database.clone()),
                    Value::string(policy.table.clone()),
                    Value::string(policy.name.clone()),
                    Value::string("local_directory".to_string()),
                    Value::null(),
                    Value::int64(0),
                    Value::int64(0),
                    Value::null(),
                    Value::null(),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_settings_profiles(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("id", DataType::String),
            Field::nullable("storage", DataType::String),
            Field::nullable("num_elements", DataType::Int64),
            Field::nullable("apply_to_all", DataType::Int64),
            Field::nullable("apply_to_list", DataType::Array(Box::new(DataType::String))),
            Field::nullable(
                "apply_to_except",
                DataType::Array(Box::new(DataType::String)),
            ),
        ]);

        let storage = self.storage.borrow();
        let rows: Vec<Row> = storage
            .settings_profiles()
            .map(|profile| {
                Row::from_values(vec![
                    Value::string(profile.name.clone()),
                    Value::string(profile.name.clone()),
                    Value::string("local_directory".to_string()),
                    Value::int64(0),
                    Value::int64(0),
                    Value::null(),
                    Value::null(),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_metrics(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("metric", DataType::String),
            Field::nullable("value", DataType::Int64),
            Field::nullable("description", DataType::String),
        ]);

        let metrics = vec![
            ("Query", 0i64, "Number of executing queries"),
            ("Merge", 0, "Number of executing background merges"),
            (
                "PartMutation",
                0,
                "Number of mutations (ALTER DELETE/UPDATE)",
            ),
            (
                "ReplicatedFetch",
                0,
                "Number of data parts being fetched from replica",
            ),
            (
                "ReplicatedSend",
                0,
                "Number of data parts being sent to replicas",
            ),
            (
                "ReplicatedChecks",
                0,
                "Number of data parts checking for consistency",
            ),
            (
                "BackgroundPoolTask",
                0,
                "Number of active background pool tasks",
            ),
            (
                "BackgroundSchedulePoolTask",
                0,
                "Number of active background schedule pool tasks",
            ),
            (
                "DiskSpaceReservedForMerge",
                0,
                "Disk space reserved for currently running background merges",
            ),
            (
                "DistributedSend",
                0,
                "Number of connections to remote servers sending data",
            ),
        ];

        let rows: Vec<Row> = metrics
            .into_iter()
            .map(|(metric, value, desc)| {
                Row::from_values(vec![
                    Value::string(metric.to_string()),
                    Value::int64(value),
                    Value::string(desc.to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_events(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("event", DataType::String),
            Field::nullable("value", DataType::Int64),
            Field::nullable("description", DataType::String),
        ]);

        let events = vec![
            (
                "Query",
                0i64,
                "Number of queries to be interpreted and potentially executed",
            ),
            ("SelectQuery", 0, "Number of SELECT queries"),
            ("InsertQuery", 0, "Number of INSERT queries"),
            ("FileOpen", 0, "Number of files opened"),
            (
                "ReadBufferFromFileDescriptorRead",
                0,
                "Number of reads from a file descriptor",
            ),
            (
                "ReadBufferFromFileDescriptorReadBytes",
                0,
                "Number of bytes read from file descriptors",
            ),
            (
                "WriteBufferFromFileDescriptorWrite",
                0,
                "Number of writes to a file descriptor",
            ),
            (
                "WriteBufferFromFileDescriptorWriteBytes",
                0,
                "Number of bytes written to file descriptors",
            ),
            (
                "MergeTreeDataWriterRows",
                0,
                "Number of rows inserted to MergeTree tables",
            ),
            (
                "MergeTreeDataWriterCompressedBytes",
                0,
                "Number of bytes compressed and written to MergeTree tables",
            ),
        ];

        let rows: Vec<Row> = events
            .into_iter()
            .map(|(event, value, desc)| {
                Row::from_values(vec![
                    Value::string(event.to_string()),
                    Value::int64(value),
                    Value::string(desc.to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_asynchronous_metrics(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("metric", DataType::String),
            Field::nullable("value", DataType::Float64),
            Field::nullable("description", DataType::String),
        ]);

        let metrics = vec![
            (
                "MaxPartCountForPartition",
                1.0,
                "Maximum number of parts per partition across all partitions",
            ),
            ("MarkCacheBytes", 0.0, "Total size of mark cache in bytes"),
            (
                "MarkCacheFiles",
                0.0,
                "Number of files cached in the mark cache",
            ),
            (
                "UncompressedCacheBytes",
                0.0,
                "Total size of uncompressed cache in bytes",
            ),
            (
                "UncompressedCacheCells",
                0.0,
                "Number of cells in uncompressed cache",
            ),
            (
                "CompiledExpressionCacheCount",
                0.0,
                "Number of compiled expressions cached",
            ),
            ("NumberOfDatabases", 1.0, "Number of databases"),
            ("NumberOfTables", 0.0, "Number of tables"),
            (
                "TotalBytesOfMergeTreeTables",
                0.0,
                "Total bytes of MergeTree tables",
            ),
            (
                "TotalRowsOfMergeTreeTables",
                0.0,
                "Total rows of MergeTree tables",
            ),
        ];

        let rows: Vec<Row> = metrics
            .into_iter()
            .map(|(metric, value, desc)| {
                Row::from_values(vec![
                    Value::string(metric.to_string()),
                    Value::float64(value),
                    Value::string(desc.to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_disks(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("path", DataType::String),
            Field::nullable("free_space", DataType::Int64),
            Field::nullable("total_space", DataType::Int64),
            Field::nullable("keep_free_space", DataType::Int64),
            Field::nullable("type", DataType::String),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("default".to_string()),
            Value::string("/var/lib/clickhouse/".to_string()),
            Value::int64(0),
            Value::int64(0),
            Value::int64(0),
            Value::string("local".to_string()),
        ])];

        Ok((schema, rows))
    }

    fn get_storage_policies(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("policy_name", DataType::String),
            Field::nullable("volume_name", DataType::String),
            Field::nullable("volume_priority", DataType::Int64),
            Field::nullable("disks", DataType::Array(Box::new(DataType::String))),
            Field::nullable("max_data_part_size", DataType::Int64),
            Field::nullable("move_factor", DataType::Float64),
        ]);

        let rows = vec![Row::from_values(vec![
            Value::string("default".to_string()),
            Value::string("default".to_string()),
            Value::int64(1),
            Value::array(vec![Value::string("default".to_string())]),
            Value::int64(0),
            Value::float64(0.1),
        ])];

        Ok((schema, rows))
    }

    fn get_clusters(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("cluster", DataType::String),
            Field::nullable("shard_num", DataType::Int64),
            Field::nullable("shard_weight", DataType::Int64),
            Field::nullable("replica_num", DataType::Int64),
            Field::nullable("host_name", DataType::String),
            Field::nullable("host_address", DataType::String),
            Field::nullable("port", DataType::Int64),
            Field::nullable("is_local", DataType::Int64),
            Field::nullable("user", DataType::String),
            Field::nullable("default_database", DataType::String),
        ]);

        Ok((schema, Vec::new()))
    }

    fn get_table_engines(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("supports_settings", DataType::Int64),
            Field::nullable("supports_skipping_indices", DataType::Int64),
            Field::nullable("supports_projections", DataType::Int64),
            Field::nullable("supports_sort_order", DataType::Int64),
            Field::nullable("supports_ttl", DataType::Int64),
            Field::nullable("supports_replication", DataType::Int64),
            Field::nullable("supports_deduplication", DataType::Int64),
            Field::nullable("supports_parallel_insert", DataType::Int64),
        ]);

        let engines = vec![
            ("MergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("ReplacingMergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("SummingMergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("AggregatingMergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("CollapsingMergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("VersionedCollapsingMergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("GraphiteMergeTree", 1, 1, 1, 1, 1, 0, 0, 1),
            ("ReplicatedMergeTree", 1, 1, 1, 1, 1, 1, 1, 1),
            ("Memory", 0, 0, 0, 0, 0, 0, 0, 1),
            ("Log", 0, 0, 0, 0, 0, 0, 0, 0),
            ("TinyLog", 0, 0, 0, 0, 0, 0, 0, 0),
            ("StripeLog", 0, 0, 0, 0, 0, 0, 0, 0),
            ("View", 0, 0, 0, 0, 0, 0, 0, 0),
            ("MaterializedView", 0, 0, 0, 0, 0, 0, 0, 0),
            ("Null", 0, 0, 0, 0, 0, 0, 0, 1),
            ("Set", 0, 0, 0, 0, 0, 0, 0, 0),
            ("Join", 0, 0, 0, 0, 0, 0, 0, 0),
            ("URL", 0, 0, 0, 0, 0, 0, 0, 0),
            ("File", 0, 0, 0, 0, 0, 0, 0, 0),
            ("Distributed", 0, 0, 0, 0, 0, 0, 0, 1),
            ("Dictionary", 0, 0, 0, 0, 0, 0, 0, 0),
            ("Buffer", 0, 0, 0, 0, 0, 0, 0, 1),
        ];

        let rows: Vec<Row> = engines
            .into_iter()
            .map(
                |(name, settings, skip_idx, proj, sort, ttl, repl, dedup, parallel)| {
                    Row::from_values(vec![
                        Value::string(name.to_string()),
                        Value::int64(settings),
                        Value::int64(skip_idx),
                        Value::int64(proj),
                        Value::int64(sort),
                        Value::int64(ttl),
                        Value::int64(repl),
                        Value::int64(dedup),
                        Value::int64(parallel),
                    ])
                },
            )
            .collect();

        Ok((schema, rows))
    }

    fn get_formats(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("is_input", DataType::Int64),
            Field::nullable("is_output", DataType::Int64),
        ]);

        let formats = vec![
            ("TabSeparated", 1, 1),
            ("TabSeparatedRaw", 1, 1),
            ("TabSeparatedWithNames", 1, 1),
            ("TabSeparatedWithNamesAndTypes", 1, 1),
            ("CSV", 1, 1),
            ("CSVWithNames", 1, 1),
            ("CSVWithNamesAndTypes", 1, 1),
            ("JSON", 0, 1),
            ("JSONCompact", 0, 1),
            ("JSONEachRow", 1, 1),
            ("TSKV", 1, 1),
            ("Pretty", 0, 1),
            ("PrettyCompact", 0, 1),
            ("PrettySpace", 0, 1),
            ("Values", 1, 1),
            ("Vertical", 0, 1),
            ("Native", 1, 1),
            ("RowBinary", 1, 1),
            ("RowBinaryWithNames", 1, 1),
            ("RowBinaryWithNamesAndTypes", 1, 1),
            ("XML", 0, 1),
            ("Parquet", 1, 1),
            ("Arrow", 1, 1),
            ("ArrowStream", 1, 1),
            ("ORC", 1, 1),
            ("Avro", 1, 1),
            ("Protobuf", 1, 1),
            ("MsgPack", 1, 1),
        ];

        let rows: Vec<Row> = formats
            .into_iter()
            .map(|(name, is_input, is_output)| {
                Row::from_values(vec![
                    Value::string(name.to_string()),
                    Value::int64(is_input),
                    Value::int64(is_output),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_collations(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("language", DataType::String),
        ]);

        let collations = vec![
            ("binary", ""),
            ("utf8_bin", ""),
            ("utf8_general_ci", ""),
            ("en_US", "English"),
            ("de_DE", "German"),
            ("fr_FR", "French"),
            ("es_ES", "Spanish"),
            ("ru_RU", "Russian"),
            ("zh_CN", "Chinese"),
            ("ja_JP", "Japanese"),
        ];

        let rows: Vec<Row> = collations
            .into_iter()
            .map(|(name, lang)| {
                Row::from_values(vec![
                    Value::string(name.to_string()),
                    Value::string(lang.to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_contributors(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![Field::nullable("name", DataType::String)]);

        let contributors = vec!["YachtSQL Team", "Open Source Community"];

        let rows: Vec<Row> = contributors
            .into_iter()
            .map(|name| Row::from_values(vec![Value::string(name.to_string())]))
            .collect();

        Ok((schema, rows))
    }

    fn get_build_options(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![
            Field::nullable("name", DataType::String),
            Field::nullable("value", DataType::String),
        ]);

        let options = vec![
            ("VERSION", env!("CARGO_PKG_VERSION")),
            ("BUILD_TYPE", "Release"),
            ("COMPILER", "rustc"),
            ("TARGET", std::env::consts::ARCH),
            ("OS", std::env::consts::OS),
        ];

        let rows: Vec<Row> = options
            .into_iter()
            .map(|(name, value)| {
                Row::from_values(vec![
                    Value::string(name.to_string()),
                    Value::string(value.to_string()),
                ])
            })
            .collect();

        Ok((schema, rows))
    }

    fn get_time_zones(&self) -> Result<(Schema, Vec<Row>)> {
        let schema = Schema::from_fields(vec![Field::nullable("time_zone", DataType::String)]);

        let time_zones = vec![
            "Africa/Cairo",
            "America/Chicago",
            "America/Los_Angeles",
            "America/New_York",
            "Asia/Hong_Kong",
            "Asia/Shanghai",
            "Asia/Singapore",
            "Asia/Tokyo",
            "Australia/Sydney",
            "Europe/Amsterdam",
            "Europe/Berlin",
            "Europe/London",
            "Europe/Moscow",
            "Europe/Paris",
            "Pacific/Auckland",
            "UTC",
        ];

        let rows: Vec<Row> = time_zones
            .into_iter()
            .map(|tz| Row::from_values(vec![Value::string(tz.to_string())]))
            .collect();

        Ok((schema, rows))
    }
}
