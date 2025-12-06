//! BigQuery SQL dialect support.

#![warn(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]
#![warn(rustdoc::broken_intra_doc_links)]
#![allow(missing_docs)]

use yachtsql_capability::feature_ids::*;
use yachtsql_capability::{CapabilitySnapshot, Result as CapabilityResult};
use yachtsql_dialects_core::{DialectManifest, FeatureToggle};
use yachtsql_parser::DialectType;

pub const BIGQUERY_VERSION: &str = "2023.11";

pub fn default_capabilities() -> CapabilitySnapshot {
    snapshot().expect("bigquery manifest should be internally consistent")
}

fn snapshot() -> CapabilityResult<CapabilitySnapshot> {
    let core = core_features();
    let optional = optional_features();
    let extensions = extension_features();

    DialectManifest {
        dialect: DialectType::BigQuery,
        version: BIGQUERY_VERSION,
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
        (F351_MERGE_STATEMENT, true),
        (F402_TRANSACTION_COMMIT, true),
        (F403_TRANSACTION_ROLLBACK, true),
        (F471_TUPLE_IN_CLAUSE, true),
        (F561_FULL_VALUE_EXPRESSIONS, true),
        (F591_DERIVED_TABLES, true),
        (F781_SELF_REFERENCING_OPERATIONS, true),
        (F782_COMMIT_STATEMENT, true),
        (F783_ROLLBACK_STATEMENT, true),
        (F784_SAVEPOINT_STATEMENT, false),
        (F785_ROLLBACK_TO_SAVEPOINT_STATEMENT, false),
        (F786_SAVEPOINTS, false),
        (F851_WINDOW_FRAME_EXCLUDE, true),
    ]
}

fn optional_features() -> Vec<FeatureToggle> {
    vec![
        (T041_ROW_TYPE, true),
        (T042_ROW_TYPE_CONSTRUCTOR, true),
        (T043_ROW_TYPE_FIELD_ACCESS, true),
        (T121_WITH_CLAUSE, true),
        (T131_RECURSIVE_QUERY, true),
        (T176_SEQUENCE_GENERATOR, false),
        (T301_SAVEPOINTS, false),
        (T321_TRANSACTION_ISOLATION, false),
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
        (T801_MERGE_STATEMENT, true),
        (T802_MERGE_WHEN_MATCHED, true),
        (T803_MERGE_WHEN_NOT_MATCHED, true),
        (T860_JSON_TYPE, true),
        (T861_JSON_QUERY, true),
        (T862_JSON_VALUE, true),
        (T863_JSON_EXISTS, true),
        (T864_JSON_TABLE, false),
        (T865_JSON_OBJECT_CONSTRUCTOR, true),
        (T866_JSON_ARRAY_CONSTRUCTOR, true),
        (T867_JSON_IS_JSON_PREDICATE, true),
        (T868_JSON_SIMPLIFIED_ACCESSOR, true),
        (T869_JSON_NULL_CLAUSE, true),
        (T870_JSON_ARRAYAGG, true),
        (T871_JSON_OBJECTAGG, true),
        (T872_JSON_PATHS, true),
        (T873_JSON_PASSING_CLAUSE, true),
        (T874_JSON_RETURNING_CLAUSE, true),
        (T875_JSON_ERROR_BEHAVIOR, true),
        (T876_JSON_NESTED_PATH, true),
        (T877_JSON_WRAPPER_OPTION, true),
        (T878_JSON_QUOTES_OPTION, true),
    ]
}

fn extension_features() -> Vec<FeatureToggle> {
    vec![
        (BQ_BACKTICK_IDENTIFIERS, true),
        (BQ_SAFE_FUNCTIONS, true),
        (BQ_QUALIFY_CLAUSE, true),
        (BQ_STRUCT_CONSTRUCTOR, true),
        (BQ_ARRAY_SAFE_OFFSET, true),
        (BQ_DATASET_QUALIFIERS, true),
        (BQ_OPTIONS_CLAUSE, false),
    ]
}
