# Worker 4: BigQuery Analytical & Advanced Features

## Objective
Implement ignored tests in BigQuery test modules focusing on analytical queries and advanced SQL features.

## Current Status (Session Progress)

### Tests Enabled (8 tests)
The following tests were enabled after fixing underlying infrastructure issues:

**analytical.rs:**
- `test_running_total` - Window function SUM() OVER with ORDER BY
- `test_moving_average` - Window function AVG() OVER with ROWS BETWEEN
- `test_lead_lag_analysis` - LEAD/LAG window functions
- `test_sessionization` - Window functions for session detection
- `test_variance_analysis` - VAR_SAMP, STDDEV_SAMP with window functions
- `test_monthly_revenue_trend` - GROUP BY with DATE_TRUNC alias
- `test_cohort_analysis` - DATE_TRUNC, DATE_DIFF with GROUP BY
- `test_retention_analysis` - DATE_DIFF with WEEK unit

**datetime.rs:**
- `test_date_trunc` - DATE_TRUNC BigQuery syntax

### Fixes Implemented

1. **GROUP BY Alias Validation** (`crates/executor/src/query_executor/statement_validator.rs`)
   - When GROUP BY references a SELECT alias, the validator now resolves the alias to check the expression

2. **GROUP BY Alias Resolution** (`crates/parser/src/ast_visitor/query/mod.rs`)
   - Added `resolve_group_by_alias` function to substitute the aliased expression when GROUP BY uses a SELECT alias name

3. **BigQuery Date Part Syntax** (`crates/parser/src/ast_visitor/expr/mod.rs`)
   - Added handling for DATE_TRUNC, TIMESTAMP_TRUNC, DATE_ADD, DATE_SUB, DATE_DIFF, TIMESTAMP_DIFF
   - Converts BigQuery date part identifiers (MONTH, YEAR, etc.) to string literals

4. **DateTime Type Inference** (`crates/executor/src/query_executor/evaluator/physical_plan/aggregate.rs`)
   - Added datetime functions (DATE_TRUNC, DATE_ADD, etc.) to `infer_expr_type` so GROUP BY columns get correct data types

5. **DATE_DIFF WEEK Support** (`crates/executor/src/query_executor/execution/utility.rs`)
   - Added WEEK unit support to `calculate_date_diff` function

## Remaining Work

### analytical.rs (22 remaining ignored tests)
Issues identified:
- **Self-join column swap**: Self-joins swap column values (test_year_over_year_comparison)
- **Scalar subqueries in SELECT with GROUP BY**: Division by zero errors (test_funnel_analysis, test_pareto_analysis)
- **UNION schema mismatch**: test_data_quality_checks
- **Window function cumulative sum issues**: test_abc_analysis
- **UNNEST-related issues**: Multiple tests with ColumnNotFound errors

### scripting.rs (25 tests)
All fail - BigQuery procedural SQL features (DECLARE, IF/THEN, LOOP, etc.) not implemented

### ddl/functions.rs (21 tests)
All fail - CREATE FUNCTION/PROCEDURE DDL not implemented

### workloads.rs (19 tests, 1 passes)
- Nested aggregates in window functions (`SUM(SUM(...)) OVER (...)`) return NULL
- Self-join issues similar to analytical tests

### interval.rs (3 failing tests)
- MAKE_INTERVAL function not available in BigQuery dialect (PostgreSQL function)

## Test Results
- Total BigQuery tests: 755 passing, 471 ignored
- analytical.rs: 20 passing, 22 ignored

## Running Tests
```bash
cargo test --test bigquery 'queries::analytical'
cargo test --test bigquery 'queries::scripting' -- --ignored
cargo test --test bigquery 'ddl::functions' -- --ignored
cargo test --test bigquery 'queries::workloads' -- --ignored
cargo test --test bigquery 'data_types::interval' -- --ignored
```
