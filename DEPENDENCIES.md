# Feature Dependencies

This document maps feature dependencies and build order for YachtSQL development.

## Crate Dependency Graph

```
                    ┌─────────────┐
                    │   common    │
                    │ types/error │
                    └──────┬──────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
        ┌─────────┐  ┌─────────┐  ┌─────────────┐
        │ storage │  │   ir    │  │  functions  │
        │ Table   │  │  Plan   │  │   impls     │
        └────┬────┘  └────┬────┘  └──────┬──────┘
             │            │              │
             │       ┌────┴────┐         │
             │       ▼         │         │
             │  ┌─────────┐    │         │
             │  │ parser  │    │         │
             │  │ Planner │    │         │
             │  └────┬────┘    │         │
             │       │         │         │
             │       ▼         │         │
             │  ┌───────────┐  │         │
             │  │ optimizer │──┘         │
             │  │ Physical  │            │
             │  └─────┬─────┘            │
             │        │                  │
             │        ▼                  │
             │  ┌───────────┐            │
             └──│ executor  │────────────┘
                │ Runtime   │
                └─────┬─────┘
                      │
                      ▼
                ┌───────────┐
                │ yachtsql  │  (main crate, re-exports)
                └─────┬─────┘
                      │
                      ▼
                ┌───────────┐
                │test-utils │
                └───────────┘
```

## Feature Dependency Map

### Core Features (Build Order)

```
1. DataType/Value (common)
   └── Basis for all other features

2. Storage Layer (storage)
   ├── Table, Column, Record
   └── Depends on: DataType/Value

3. Expression IR (ir/expr)
   ├── Expr enum
   └── Depends on: DataType

4. Plan IR (ir/plan)
   ├── LogicalPlan enum
   └── Depends on: Expr

5. SQL Parser (parser)
   ├── Uses sqlparser-rs
   └── Depends on: Plan IR

6. Physical Planning (optimizer)
   ├── LogicalPlan → PhysicalPlan
   └── Depends on: Plan IR

7. Execution (executor)
   ├── PhysicalPlan → Table
   └── Depends on: All above
```

### Feature Categories

#### A. Scalar Functions

No interdependencies. Each function is independent.

```
ScalarFunction variant
    │
    ├── Parser recognition (expr_planner.rs)
    │
    └── Evaluation (ir_evaluator.rs)
```

#### B. Aggregate Functions

```
AggregateFunction variant
    │
    ├── Parser recognition
    │
    ├── AggregateState implementation
    │
    └── Aggregate executor handling
```

#### C. Window Functions

```
WindowFunction variant
    │
    ├── Parser recognition
    │
    └── Window executor handling
        │
        └── Depends on: Frame handling
```

#### D. SQL Clauses

```
GROUP BY ─────────────────┐
    │                     │
    └── HAVING ───────────┼──► ROLLUP/CUBE
                          │
ORDER BY ─────────────────┤
    │                     │
    └── LIMIT/OFFSET      │
                          │
SELECT DISTINCT ──────────┘
```

#### E. Join Types

```
INNER JOIN (base)
    │
    ├── LEFT JOIN
    │
    ├── RIGHT JOIN
    │
    ├── FULL OUTER JOIN
    │
    └── CROSS JOIN
        │
        └── LATERAL (depends on correlated subquery support)
```

#### F. Subqueries

```
Scalar Subquery (base)
    │
    ├── IN (subquery)
    │
    ├── EXISTS (subquery)
    │
    └── Correlated Subqueries
        │
        └── LATERAL joins
```

## Implementation Order Recommendations

### Phase 1: Core Completeness

These should be complete before adding advanced features:

1. All basic scalar functions
2. All aggregate functions
3. All window functions
4. Complete join types
5. Subquery patterns

### Phase 2: Analytical Features

Build on Phase 1:

1. ROLLUP (requires aggregate changes)
2. CUBE (requires aggregate changes)
3. GROUPING SETS (requires aggregate changes)
4. QUALIFY enhancements
5. Recursive CTEs (requires CTE executor changes)

### Phase 3: Convenience Features

Independent additions:

1. PIVOT/UNPIVOT
2. TABLESAMPLE
3. More JSON functions
4. Geography functions
5. Array functions

## Conflict Prevention

### Shared Code Locations

Multiple features may need to modify:

| File | Coordinates With |
|------|------------------|
| `ir/src/expr/functions.rs` | Any new function |
| `ir/src/plan/mod.rs` | Any new plan node |
| `parser/src/planner.rs` | Any new syntax |
| `executor/src/ir_evaluator.rs` | Any new function |

### Merge Strategies

1. **Add-only changes**: Functions, plan nodes → Low conflict risk
2. **Behavioral changes**: Existing executor logic → Test thoroughly
3. **Structural changes**: Plan representation → Coordinate with all workers

## Testing Dependencies

```
Unit Tests (per module)
    │
    └── Integration Tests (test-utils)
        │
        ├── Query tests (tests/bigquery/)
        │   │
        │   ├── data_types/
        │   ├── functions/
        │   ├── operators/
        │   ├── queries/
        │   ├── ddl/
        │   └── dml/
        │
        └── Uses: assert_batch_values, exec_ok, query, etc.
```

## External Dependencies

| Crate | Purpose | Version Sensitivity |
|-------|---------|---------------------|
| `sqlparser` | SQL parsing | High (syntax support) |
| `chrono` | Date/time | Low |
| `rust_decimal` | Numeric precision | Low |
| `ordered_float` | NaN-safe floats | Low |
| `serde` | Serialization | Low |
| `serde_json` | JSON handling | Low |
| `regex` | Pattern matching | Low |
| `hex` | Bytes encoding | Low |
| `boa_engine` | JavaScript UDFs | Medium |

## Versioning Impact

Adding features may require:

1. **IR version bump**: New plan nodes or expressions
2. **API changes**: New public interfaces
3. **Test updates**: New test cases for features
4. **Documentation**: Update ARCHITECTURE.md

## Cross-Cutting Concerns

Features that affect multiple modules:

| Concern | Affected Modules |
|---------|------------------|
| NULL handling | All evaluators |
| Type coercion | Parser, evaluator |
| Error messages | All modules |
| Timezone handling | Date/time functions |
| Case sensitivity | Catalog, parser |
| Unicode support | String functions |
| Decimal precision | Numeric operations |

## Recommended Development Flow

```
1. Check REMAINING_WORK.md for feature status
        │
        ▼
2. Read ARCHITECTURE.md for patterns
        │
        ▼
3. Check this file for dependencies
        │
        ▼
4. Implement in dependency order:
   a. IR changes (if needed)
   b. Parser changes
   c. Optimizer changes (if needed)
   d. Executor changes
        │
        ▼
5. Add tests in tests/bigquery/
        │
        ▼
6. Update documentation as needed
```
