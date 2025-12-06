//! ClickHouse SQL dialect support.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

use yachtsql_capability::feature_ids::*;
use yachtsql_capability::{CapabilitySnapshot, Result as CapabilityResult};
use yachtsql_dialects_core::{DialectManifest, FeatureToggle};
use yachtsql_parser::DialectType;

pub const CLICKHOUSE_VERSION: &str = "23.8";

pub fn default_capabilities() -> CapabilitySnapshot {
    snapshot().expect("clickhouse manifest should be internally consistent")
}

fn snapshot() -> CapabilityResult<CapabilitySnapshot> {
    let core = core_features();
    let optional = optional_features();
    let extensions = extension_features();

    DialectManifest {
        dialect: DialectType::ClickHouse,
        version: CLICKHOUSE_VERSION,
        core_features: &core,
        optional_features: &optional,
        extensions: &extensions,
    }
    .snapshot()
}

fn core_features() -> Vec<FeatureToggle> {
    vec![
        (F001_BASIC_SQL, true),
        (F051_BASIC_DATE_TIME, true),
        (F052_INTERVALS_DATETIME_ARITHMETIC, true),
        (F311_SCHEMA_DEFINITION, true),
        (F312_SCALAR_SUBQUERY_AS_EXPRESSION, true),
        (F402_TRANSACTION_COMMIT, true),
        (F403_TRANSACTION_ROLLBACK, true),
        (F471_TUPLE_IN_CLAUSE, true),
        (F561_FULL_VALUE_EXPRESSIONS, true),
        (F591_DERIVED_TABLES, true),
        (F782_COMMIT_STATEMENT, true),
        (F783_ROLLBACK_STATEMENT, true),
    ]
}

fn optional_features() -> Vec<FeatureToggle> {
    vec![
        (T041_ROW_TYPE, true),
        (T042_ROW_TYPE_CONSTRUCTOR, true),
        (T043_ROW_TYPE_FIELD_ACCESS, true),
        (T121_WITH_CLAUSE, true),
        (T131_RECURSIVE_QUERY, true),
        (T611_WINDOW_FUNCTIONS, true),
        (T612_ADVANCED_OLAP, true),
        (T613_WINDOW_FRAME_CLAUSE, true),
        (T614_NTILE, true),
        (T615_LEAD_LAG, true),
        (T616_NULL_TREATMENT, true),
        (T617_FIRST_LAST_VALUE, true),
        (T618_NTH_VALUE, true),
        (T619_WINDOW_FRAME_GROUPS, true),
        (T620_WINDOW_FRAME_EXCLUDE, true),
        (T860_JSON_TYPE, true),
        (T865_JSON_OBJECT_CONSTRUCTOR, true),
        (T866_JSON_ARRAY_CONSTRUCTOR, true),
    ]
}

fn extension_features() -> Vec<FeatureToggle> {
    vec![
        (CH_ARRAY_JOIN, true),
        (CH_LEFT_ARRAY_JOIN, true),
        (CH_UNALIGNED_ARRAY_JOIN, true),
        (CH_ARRAY_FUNCTIONS, true),
        (CH_HIGHER_ORDER_ARRAY_FUNCS, true),
        (CH_ARRAY_ELEMENT_ACCESS, true),
        (CH_SETTINGS_CLAUSE, true),
        (CH_LIMIT_BY, true),
        (CH_SAMPLE, true),
        (CH_PREWHERE, true),
        (CH_FINAL_MODIFIER, true),
        (CH_GLOBAL_JOIN, true),
        (CH_JOIN_STRICTNESS, true),
        (CH_ASOF_JOIN, true),
        (CH_GROUP_BY_CUBE, true),
        (CH_GROUP_BY_ROLLUP, true),
        (CH_GROUP_BY_TOTALS, true),
        (CH_AGGREGATE_COMBINATORS, true),
        (CH_LAMBDA_FUNCTIONS, true),
        (CH_DATE_TIME_FUNCTIONS, true),
        (CH_TIME_INTERVALS, true),
        (CH_NULLABLE_TYPE, true),
        (CH_LOWCARDINALITY, true),
        (CH_TUPLE_TYPE, true),
        (CH_MAP_TYPE, true),
        (CH_TUPLE_ELEMENT_ACCESS, true),
        (CH_PARAMETERIZED_VIEWS, true),
        (CH_DISTRIBUTED_SUBQUERIES, true),
        (CH_LIMIT_BY_NORMALIZATION, false),
    ]
}
