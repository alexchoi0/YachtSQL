use crate::phase::StorageType;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum RuleTier {
    AlwaysRun = 3,

    RunIfTimePermits = 2,

    SkipInFastMode = 1,
}

pub fn rule_tier(rule_name: &str) -> RuleTier {
    match rule_name {
        "predicate_pushdown"
        | "projection_pushdown"
        | "partition_pruning"
        | "limit_pushdown"
        | "constant_folding"
        | "filter_reorder"
        | "range_merge"
        | "subquery_flattening" => RuleTier::AlwaysRun,

        "join_reorder"
        | "aggregate_pushdown"
        | "boolean_simplification"
        | "sort_elimination"
        | "late_materialization" => RuleTier::RunIfTimePermits,

        _ => RuleTier::SkipInFastMode,
    }
}

pub fn rule_priority(rule_name: &str, storage_type: StorageType) -> u8 {
    match (rule_name, storage_type) {
        ("projection_pushdown", StorageType::Columnar) => 100,
        ("partition_pruning", StorageType::Columnar) => 90,
        ("predicate_pushdown", StorageType::Columnar) => 80,

        ("predicate_pushdown", StorageType::Row) => 100,
        ("partition_pruning", StorageType::Row) => 85,
        ("projection_pushdown", StorageType::Row) => 60,

        ("limit_pushdown", _) => 95,

        ("filter_reorder", _) => 88,
        ("range_merge", _) => 82,

        ("join_reorder", _) => 75,

        ("late_materialization", StorageType::Columnar) => 95,
        ("late_materialization", StorageType::Row) => 30,

        ("aggregate_pushdown", _) => 70,

        ("subquery_flattening", _) => 85,

        ("sort_elimination", _) => 65,

        ("constant_folding", _) => 50,
        ("boolean_simplification", _) => 55,
        ("expression_normalization", _) => 45,
        ("null_propagation", _) => 45,

        ("filter_merge" | "FilterMergeV2", _) => 40,
        ("dead_code_elimination", _) => 35,
        ("distinct_elimination", _) => 30,
        ("eliminate_redundant", _) => 30,
        ("RemoveTrueFilters", _) => 25,

        _ => 50,
    }
}

pub fn should_skip_rule(rule_name: &str, time_remaining_micros: u64, fast_mode: bool) -> bool {
    let tier = rule_tier(rule_name);

    match tier {
        RuleTier::AlwaysRun => false,

        RuleTier::RunIfTimePermits => fast_mode || time_remaining_micros < 100,

        RuleTier::SkipInFastMode => fast_mode || time_remaining_micros < 200,
    }
}

pub fn default_phase_budget_micros(phase: crate::phase::Phase) -> u64 {
    use crate::phase::Phase;

    match phase {
        Phase::Pushdown => 500,
        Phase::Simplification => 250,
        Phase::Reordering => 166,
        Phase::Cleanup => 84,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::phase::Phase;

    #[test]
    fn test_rule_tier_always_run() {
        let tier1_rules = vec![
            "predicate_pushdown",
            "projection_pushdown",
            "partition_pruning",
            "limit_pushdown",
            "constant_folding",
        ];

        for rule in tier1_rules {
            assert_eq!(
                rule_tier(rule),
                RuleTier::AlwaysRun,
                "{} should be AlwaysRun",
                rule
            );
        }
    }

    #[test]
    fn test_rule_tier_run_if_time_permits() {
        let tier2_rules = vec![
            "join_reorder",
            "aggregate_pushdown",
            "boolean_simplification",
            "sort_elimination",
            "late_materialization",
        ];

        for rule in tier2_rules {
            assert_eq!(
                rule_tier(rule),
                RuleTier::RunIfTimePermits,
                "{} should be RunIfTimePermits",
                rule
            );
        }
    }

    #[test]
    fn test_rule_tier_skip_in_fast_mode() {
        let tier3_rules = vec![
            "filter_merge",
            "FilterMergeV2",
            "dead_code_elimination",
            "distinct_elimination",
            "eliminate_redundant",
            "expression_normalization",
            "null_propagation",
            "in_list_conversion",
            "common_subexpression_elimination",
        ];

        for rule in tier3_rules {
            assert_eq!(
                rule_tier(rule),
                RuleTier::SkipInFastMode,
                "{} should be SkipInFastMode",
                rule
            );
        }
    }

    #[test]
    fn test_tier_ordering() {
        assert!(RuleTier::AlwaysRun > RuleTier::RunIfTimePermits);
        assert!(RuleTier::RunIfTimePermits > RuleTier::SkipInFastMode);
    }

    #[test]
    fn test_storage_aware_priority_columnar() {
        assert_eq!(
            rule_priority("projection_pushdown", StorageType::Columnar),
            100
        );
        assert_eq!(
            rule_priority("predicate_pushdown", StorageType::Columnar),
            80
        );
        assert!(
            rule_priority("projection_pushdown", StorageType::Columnar)
                > rule_priority("predicate_pushdown", StorageType::Columnar)
        );
    }

    #[test]
    fn test_storage_aware_priority_row() {
        assert_eq!(rule_priority("predicate_pushdown", StorageType::Row), 100);
        assert_eq!(rule_priority("projection_pushdown", StorageType::Row), 60);
        assert!(
            rule_priority("predicate_pushdown", StorageType::Row)
                > rule_priority("projection_pushdown", StorageType::Row)
        );
    }

    #[test]
    fn test_priority_limit_pushdown_high() {
        assert_eq!(rule_priority("limit_pushdown", StorageType::Row), 95);
        assert_eq!(rule_priority("limit_pushdown", StorageType::Columnar), 95);
    }

    #[test]
    fn test_priority_partition_pruning() {
        assert_eq!(
            rule_priority("partition_pruning", StorageType::Columnar),
            90
        );
        assert_eq!(rule_priority("partition_pruning", StorageType::Row), 85);
    }

    #[test]
    fn test_priority_join_reorder() {
        assert_eq!(rule_priority("join_reorder", StorageType::Row), 75);
        assert_eq!(rule_priority("join_reorder", StorageType::Columnar), 75);
    }

    #[test]
    fn test_priority_cleanup_rules_low() {
        assert!(rule_priority("filter_merge", StorageType::Row) < 50);
        assert!(rule_priority("dead_code_elimination", StorageType::Row) < 40);
        assert!(rule_priority("RemoveTrueFilters", StorageType::Row) < 30);
    }

    #[test]
    fn test_should_skip_rule_always_run_never_skipped() {
        let tier1_rules = vec![
            "predicate_pushdown",
            "projection_pushdown",
            "limit_pushdown",
        ];

        for rule in tier1_rules {
            assert!(!should_skip_rule(rule, 1, true));
            assert!(!should_skip_rule(rule, 1, false));
            assert!(!should_skip_rule(rule, 1000, true));
            assert!(!should_skip_rule(rule, 1000, false));
        }
    }

    #[test]
    fn test_should_skip_rule_tier2_fast_mode() {
        let tier2_rules = vec!["join_reorder", "aggregate_pushdown"];

        for rule in tier2_rules {
            assert!(should_skip_rule(rule, 1000, true));
            assert!(should_skip_rule(rule, 500, true));

            assert!(!should_skip_rule(rule, 500, false));
            assert!(!should_skip_rule(rule, 200, false));
        }
    }

    #[test]
    fn test_should_skip_rule_tier2_low_time() {
        assert!(should_skip_rule("join_reorder", 99, false));
        assert!(should_skip_rule("join_reorder", 50, false));
        assert!(should_skip_rule("join_reorder", 10, false));

        assert!(!should_skip_rule("join_reorder", 100, false));
        assert!(!should_skip_rule("join_reorder", 150, false));
    }

    #[test]
    fn test_should_skip_rule_tier3_fast_mode() {
        let tier3_rules = vec!["filter_merge", "dead_code_elimination"];

        for rule in tier3_rules {
            assert!(should_skip_rule(rule, 10000, true));
            assert!(should_skip_rule(rule, 1000, true));
            assert!(should_skip_rule(rule, 500, true));
        }
    }

    #[test]
    fn test_should_skip_rule_tier3_low_time() {
        assert!(should_skip_rule("filter_merge", 199, false));
        assert!(should_skip_rule("filter_merge", 150, false));
        assert!(should_skip_rule("filter_merge", 50, false));

        assert!(!should_skip_rule("filter_merge", 200, false));
        assert!(!should_skip_rule("filter_merge", 300, false));
    }

    #[test]
    fn test_phase_budget_allocation_totals_1ms() {
        let total: u64 = Phase::all()
            .iter()
            .map(|p| default_phase_budget_micros(*p))
            .sum();

        assert_eq!(total, 1000, "Phase budgets should total 1000μs (1ms)");
    }

    #[test]
    fn test_phase_budget_pushdown_gets_most() {
        assert_eq!(default_phase_budget_micros(Phase::Pushdown), 500);

        for phase in Phase::all() {
            if phase != Phase::Pushdown {
                assert!(
                    default_phase_budget_micros(Phase::Pushdown)
                        > default_phase_budget_micros(phase)
                );
            }
        }
    }

    #[test]
    fn test_phase_budget_individual_allocations() {
        assert_eq!(
            default_phase_budget_micros(Phase::Pushdown),
            500,
            "Pushdown: 50%"
        );
        assert_eq!(
            default_phase_budget_micros(Phase::Simplification),
            250,
            "Simplification: 25%"
        );
        assert_eq!(
            default_phase_budget_micros(Phase::Reordering),
            166,
            "Reordering: 16.6%"
        );
        assert_eq!(
            default_phase_budget_micros(Phase::Cleanup),
            84,
            "Cleanup: 8.4%"
        );
    }

    #[test]
    fn test_priority_range_valid() {
        let all_rules = vec![
            "predicate_pushdown",
            "projection_pushdown",
            "partition_pruning",
            "limit_pushdown",
            "constant_folding",
            "join_reorder",
            "aggregate_pushdown",
            "filter_merge",
            "dead_code_elimination",
        ];

        for rule in all_rules {
            for storage in [StorageType::Row, StorageType::Columnar] {
                let priority = rule_priority(rule, storage);
                assert!(
                    priority <= 100,
                    "Priority for {} should be <=100, got {}",
                    rule,
                    priority
                );
            }
        }
    }

    #[test]
    fn test_columnar_specific_optimizations() {
        let proj = rule_priority("projection_pushdown", StorageType::Columnar);
        let part = rule_priority("partition_pruning", StorageType::Columnar);
        let pred = rule_priority("predicate_pushdown", StorageType::Columnar);

        assert_eq!(proj, 100);
        assert_eq!(part, 90);
        assert_eq!(pred, 80);
        assert!(proj > part);
        assert!(part > pred);
    }

    #[test]
    fn test_row_specific_optimizations() {
        let pred = rule_priority("predicate_pushdown", StorageType::Row);
        let part = rule_priority("partition_pruning", StorageType::Row);
        let proj = rule_priority("projection_pushdown", StorageType::Row);

        assert_eq!(pred, 100);
        assert_eq!(part, 85);
        assert_eq!(proj, 60);
        assert!(pred > part);
        assert!(part > proj);
    }
}
