use std::collections::HashMap;

use chrono::{TimeZone, Utc};
use yachtsql_common::types::Value;
use yachtsql_executor::match_recognize::{
    AfterMatchSkip, MatchMode, MatchRecognizeClause, PatternElement, PatternExpression,
    PatternMatcher, PatternQuantifier, PatternVariable, RowPattern,
};
use yachtsql_executor::multiset_operations::Multiset;
use yachtsql_executor::temporal_queries::{TemporalClause, TemporalQueryBuilder};
use yachtsql_executor::temporal_tables::TemporalTableRegistry;

#[test]
fn test_match_recognize_stock_trend() {
    let pattern = RowPattern {
        elements: vec![
            PatternElement {
                variable: "UP1".to_string(),
                quantifier: PatternQuantifier::One,
            },
            PatternElement {
                variable: "UP2".to_string(),
                quantifier: PatternQuantifier::One,
            },
            PatternElement {
                variable: "UP3".to_string(),
                quantifier: PatternQuantifier::One,
            },
        ],
    };

    let clause = MatchRecognizeClause {
        partition_by: vec![],
        order_by: vec!["time".to_string()],
        measures: vec![],
        mode: MatchMode::OneRowPerMatch,
        after_match_skip: AfterMatchSkip::PastLastRow,
        pattern,
        definitions: vec![
            PatternVariable {
                name: "UP1".to_string(),
                condition: PatternExpression::Literal(Value::bool_val(true)),
            },
            PatternVariable {
                name: "UP2".to_string(),
                condition: PatternExpression::Literal(Value::bool_val(true)),
            },
            PatternVariable {
                name: "UP3".to_string(),
                condition: PatternExpression::Literal(Value::bool_val(true)),
            },
        ],
    };

    let matcher = PatternMatcher::new(clause);

    let rows = vec![
        {
            let mut row = HashMap::new();
            row.insert("price".to_string(), Value::int64(100));
            row
        },
        {
            let mut row = HashMap::new();
            row.insert("price".to_string(), Value::int64(105));
            row
        },
        {
            let mut row = HashMap::new();
            row.insert("price".to_string(), Value::int64(110));
            row
        },
    ];

    let matches = matcher.find_matches(&rows).unwrap();
    assert_eq!(matches.len(), 1);
    assert_eq!(matches[0].start_index, 0);
    assert_eq!(matches[0].end_index, 2);
}

#[test]
fn test_multiset_union() {
    let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2), Value::int64(2)]);

    let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(3)]);

    let result = ms1.union(&ms2);

    assert_eq!(result.cardinality(), 5);
    assert_eq!(result.multiplicity(&Value::int64(1)), 1);
    assert_eq!(result.multiplicity(&Value::int64(2)), 3);
    assert_eq!(result.multiplicity(&Value::int64(3)), 1);
}

#[test]
fn test_multiset_except() {
    let ms1 = Multiset::from_vec(vec![
        Value::int64(1),
        Value::int64(2),
        Value::int64(2),
        Value::int64(2),
    ]);

    let ms2 = Multiset::from_vec(vec![Value::int64(2)]);

    let result = ms1.except(&ms2);

    assert_eq!(result.cardinality(), 3);
    assert_eq!(result.multiplicity(&Value::int64(1)), 1);
    assert_eq!(result.multiplicity(&Value::int64(2)), 2);
}

#[test]
fn test_multiset_intersect() {
    let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2), Value::int64(2)]);

    let ms2 = Multiset::from_vec(vec![Value::int64(2), Value::int64(2), Value::int64(3)]);

    let result = ms1.intersect(&ms2);

    assert_eq!(result.cardinality(), 2);
    assert_eq!(result.multiplicity(&Value::int64(2)), 2);
}

#[test]
fn test_multiset_subset() {
    let ms1 = Multiset::from_vec(vec![Value::int64(1), Value::int64(2)]);

    let ms2 = Multiset::from_vec(vec![
        Value::int64(1),
        Value::int64(2),
        Value::int64(2),
        Value::int64(3),
    ]);

    assert!(ms1.is_subset_of(&ms2));
    assert!(!ms2.is_subset_of(&ms1));
}

#[test]
fn test_temporal_as_of_query() {
    let timestamp = Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
    let table_ref = TemporalQueryBuilder::new("employees".to_string()).as_of(timestamp);

    assert_eq!(table_ref.table_name, "employees");
    assert_eq!(table_ref.temporal_clause, TemporalClause::AsOf(timestamp));

    let predicate = table_ref.generate_predicate("sys_start", "sys_end");
    assert!(predicate.contains("sys_start"));
    assert!(predicate.contains("sys_end"));
    assert!(predicate.contains("2024-06-15"));
}

#[test]
fn test_temporal_from_to_query() {
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

    let table_ref = TemporalQueryBuilder::new("employees".to_string())
        .from_to(start, end)
        .unwrap();

    match &table_ref.temporal_clause {
        TemporalClause::FromTo { start: s, end: e } => {
            assert_eq!(*s, start);
            assert_eq!(*e, end);
        }
        TemporalClause::AsOf(_)
        | TemporalClause::Between { .. }
        | TemporalClause::All
        | TemporalClause::ContainedIn { .. } => panic!("Expected FromTo clause"),
    }
}

#[test]
fn test_temporal_all_query() {
    let table_ref = TemporalQueryBuilder::new("employees".to_string()).all();

    assert_eq!(table_ref.temporal_clause, TemporalClause::All);

    let predicate = table_ref.generate_predicate("sys_start", "sys_end");
    assert_eq!(predicate, "1=1");
}

#[test]
fn test_temporal_table_registry() {
    let mut registry = TemporalTableRegistry::new();

    registry
        .register_system_versioned_table(
            "employees".to_string(),
            Some("employees_history".to_string()),
        )
        .unwrap();

    assert!(registry.is_system_versioned("employees"));
    assert!(!registry.is_system_versioned("departments"));

    let metadata = registry.get_metadata("employees").unwrap();
    assert_eq!(metadata.table_name, "employees");
    assert!(metadata.is_system_versioned);
}

#[test]
fn test_temporal_matches_condition() {
    let query_time = Utc.with_ymd_and_hms(2024, 6, 15, 12, 0, 0).unwrap();
    let table_ref = TemporalQueryBuilder::new("employees".to_string()).as_of(query_time);

    let row_start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let row_end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

    assert!(table_ref.matches_condition(row_start, row_end));

    let row_start_future = Utc.with_ymd_and_hms(2024, 7, 1, 0, 0, 0).unwrap();
    assert!(!table_ref.matches_condition(row_start_future, row_end));
}

#[test]
fn test_multiset_element() {
    let ms = Multiset::from_vec(vec![Value::int64(42)]);

    let element = ms.element().unwrap();
    assert_eq!(element, Value::int64(42));
}

#[test]
fn test_multiset_element_error() {
    let ms = Multiset::from_vec(vec![Value::int64(1), Value::int64(2)]);

    let result = ms.element();
    assert!(result.is_err());
}

#[test]
fn test_temporal_between_query() {
    let start = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let end = Utc.with_ymd_and_hms(2024, 12, 31, 23, 59, 59).unwrap();

    let table_ref = TemporalQueryBuilder::new("employees".to_string())
        .between(start, end)
        .unwrap();

    let predicate = table_ref.generate_predicate("sys_start", "sys_end");
    assert!(predicate.contains("<="));
    assert!(predicate.contains(">="));
}

#[test]
fn test_multiset_to_set_conversion() {
    let ms = Multiset::from_vec(vec![
        Value::int64(1),
        Value::int64(2),
        Value::int64(2),
        Value::int64(3),
    ]);

    let set = ms.to_set();
    assert_eq!(set.len(), 3);
}

#[test]
fn test_temporal_query_with_alias() {
    let timestamp = Utc.with_ymd_and_hms(2024, 1, 1, 0, 0, 0).unwrap();
    let table_ref = TemporalQueryBuilder::new("employees".to_string())
        .as_of(timestamp)
        .with_alias("e".to_string());

    assert_eq!(table_ref.alias, Some("e".to_string()));
}
