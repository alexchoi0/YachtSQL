# Remaining Work

This document catalogs features and improvements that can be implemented in YachtSQL.

## Status Summary

- **No ignored tests** - All test files pass
- **Architecture is extensible** - Adding new features follows established patterns
- **BigQuery compatibility** - Primary goal is BigQuery SQL dialect support

## Features Implementable with Current Architecture

These features fit naturally into the existing plan structure and can be added without architectural changes.

### Scalar Functions (Low Effort)

Additional BigQuery functions that follow existing patterns:

| Category | Functions |
|----------|-----------|
| String | `ASCII`, `CHAR_LENGTH`, `CHR`, `CODE_POINTS_TO_STRING`, `COLLATE`, `FORMAT`, `FROM_BASE32`, `FROM_BASE64`, `FROM_HEX`, `INITCAP`, `INSTR`, `NORMALIZE`, `NORMALIZE_AND_CASEFOLD`, `REGEXP_CONTAINS`, `REGEXP_INSTR`, `SAFE_CONVERT_BYTES_TO_STRING`, `SOUNDEX`, `SPLIT`, `TO_BASE32`, `TO_BASE64`, `TO_HEX`, `TRANSLATE`, `UNICODE` |
| Math | `ACOS`, `ACOSH`, `ASIN`, `ASINH`, `ATAN`, `ATAN2`, `ATANH`, `CBRT`, `COS`, `COSH`, `COT`, `COTH`, `CSC`, `CSCH`, `DIV`, `IEEE_DIVIDE`, `IS_INF`, `IS_NAN`, `RANGE_BUCKET`, `SAFE_DIVIDE`, `SAFE_MULTIPLY`, `SAFE_NEGATE`, `SEC`, `SECH`, `SIN`, `SINH`, `TAN`, `TANH`, `TRUNC` |
| Date/Time | `DATE_ADD`, `DATE_DIFF`, `DATE_FROM_UNIX_DATE`, `DATE_SUB`, `DATE_TRUNC`, `DATETIME_ADD`, `DATETIME_DIFF`, `DATETIME_SUB`, `DATETIME_TRUNC`, `LAST_DAY`, `TIME_ADD`, `TIME_DIFF`, `TIME_SUB`, `TIME_TRUNC`, `TIMESTAMP_ADD`, `TIMESTAMP_DIFF`, `TIMESTAMP_MICROS`, `TIMESTAMP_MILLIS`, `TIMESTAMP_SECONDS`, `TIMESTAMP_SUB`, `TIMESTAMP_TRUNC`, `UNIX_DATE`, `UNIX_MICROS`, `UNIX_MILLIS`, `UNIX_SECONDS` |
| JSON | `JSON_ARRAY`, `JSON_ARRAY_APPEND`, `JSON_ARRAY_INSERT`, `JSON_OBJECT`, `JSON_REMOVE`, `JSON_SET`, `JSON_STRIP_NULLS`, `JSON_TYPE`, `LAX_BOOL`, `LAX_FLOAT64`, `LAX_INT64`, `LAX_STRING`, `PARSE_JSON`, `TO_JSON`, `TO_JSON_STRING` |
| Array | `ARRAY_FILTER`, `ARRAY_FIRST`, `ARRAY_INCLUDES`, `ARRAY_INCLUDES_ALL`, `ARRAY_INCLUDES_ANY`, `ARRAY_LAST`, `ARRAY_MAX`, `ARRAY_MIN`, `ARRAY_SLICE`, `ARRAY_SUM`, `ARRAY_TO_STRING`, `ARRAY_TRANSFORM`, `ARRAY_ZIP`, `FLATTEN` |
| Geography | `ST_AREA`, `ST_ASBINARY`, `ST_ASGEOJSON`, `ST_ASTEXT`, `ST_BOUNDARY`, `ST_CENTROID`, `ST_CONTAINS`, `ST_CONVEXHULL`, `ST_COVERS`, `ST_DIFFERENCE`, `ST_DIMENSION`, `ST_DISTANCE`, `ST_DWITHIN`, `ST_EQUALS`, `ST_GEOGPOINT`, `ST_GEOGPOINTFROMGEOHASH`, `ST_INTERSECTION`, `ST_INTERSECTS`, `ST_LENGTH`, `ST_MAKELINE`, `ST_MAKEPOLYGON`, `ST_NUMPOINTS`, `ST_PERIMETER`, `ST_SIMPLIFY`, `ST_SNAPTOGRID`, `ST_TOUCHES`, `ST_UNION`, `ST_WITHIN`, `ST_X`, `ST_Y` |

### Aggregate Functions (Low Effort)

| Function | Description |
|----------|-------------|
| `BIT_AND` | Bitwise AND of values |
| `BIT_OR` | Bitwise OR of values |
| `BIT_XOR` | Bitwise XOR of values |
| `COUNTIF` | Count with condition |
| `LOGICAL_AND` | Boolean AND of values |
| `LOGICAL_OR` | Boolean OR of values |
| `GROUPING` | Grouping set indicator |

### Window Functions (Low Effort)

| Function | Description |
|----------|-------------|
| `CUME_DIST` | Cumulative distribution |
| `PERCENT_RANK` | Percentile rank |
| `PERCENTILE_CONT` | Continuous percentile |
| `PERCENTILE_DISC` | Discrete percentile |

### SQL Syntax (Medium Effort)

| Feature | Description |
|---------|-------------|
| `TABLESAMPLE` | Random row sampling |
| `PIVOT`/`UNPIVOT` | Table rotation |
| `QUALIFY` (enhanced) | Complex window predicates |
| `ROLLUP` | Hierarchical grouping |
| `CUBE` | Multi-dimensional grouping |
| `GROUPING SETS` | Custom grouping combinations |
| Recursive CTEs | WITH RECURSIVE |
| Lateral joins | `LATERAL` subqueries |
| `ARRAY` subqueries | `ARRAY(SELECT ...)` |
| `STRUCT` subqueries | `(SELECT AS STRUCT ...)` |

### DML Enhancements (Medium Effort)

| Feature | Description |
|---------|-------------|
| `INSERT ... ON CONFLICT` | Upsert patterns |
| Multi-table UPDATE | JOIN in UPDATE |
| Returning clauses | `RETURNING *` |
| Batch operations | Bulk insert optimization |

## Features Requiring Architectural Changes

These features need significant work beyond adding new nodes/functions.

### Persistence Layer

**Current**: All data in-memory, lost on process exit

**Required for**:
- Materialized views
- Table statistics
- Query result caching
- Transaction logs

**Changes needed**:
- Storage abstraction trait
- Write-ahead log
- Checkpoint/recovery
- File format (Parquet?)

### Time Travel

**Current**: No versioning of data

**Required for**:
- `FOR SYSTEM_TIME AS OF`
- `FOR SYSTEM_TIME BETWEEN`
- Snapshot queries

**Changes needed**:
- MVCC or snapshot storage
- Timestamp tracking per row
- Garbage collection for old versions

### Distributed Execution

**Current**: Single-threaded, single-node

**Required for**:
- Large dataset processing
- Parallel query execution
- Horizontal scaling

**Changes needed**:
- Shuffle operators
- Exchange operators
- Distributed catalog
- Query coordinator

### Cost-Based Optimizer

**Current**: No optimization (plans execute as written)

**Required for**:
- Automatic join ordering
- Index selection
- Predicate pushdown

**Changes needed**:
- Statistics collection
- Cardinality estimation
- Cost model
- Plan enumeration

### Prepared Statements

**Current**: Each query parsed fresh

**Required for**:
- Parameter binding
- Plan caching
- SQL injection prevention

**Changes needed**:
- Statement cache
- Placeholder system
- Parameter type inference

## Out of Scope

These features are unlikely to be implemented:

| Feature | Reason |
|---------|--------|
| External data sources | Design is for standalone execution |
| Real-time streaming | Batch processing focus |
| Federated queries | Single catalog design |
| Machine learning (BQML) | Not a core SQL feature |
| Access control (IAM) | No authentication layer |
| Quotas/billing | Not a managed service |

## Test Infrastructure Improvements

### Current State

- Test helpers in `crates/test-utils/src/lib.rs`
- BigQuery tests in `tests/bigquery/`
- Uses `cargo nextest`

### Proposed Improvements

1. **Snapshot testing**: Record expected outputs for regression detection
2. **Fuzzing**: Property-based testing for edge cases
3. **Performance benchmarks**: Track query performance over time
4. **Coverage reports**: Identify untested code paths
5. **Shared fixtures**: Reduce test setup duplication
6. **Parameterized tests**: Data-driven test cases
7. **Integration tests**: End-to-end query scenarios

## Quick Wins

High-value, low-effort improvements:

1. **More scalar functions**: Each is ~50 lines of code
2. **Better error messages**: Include position information
3. **EXPLAIN plan output**: Show plan structure for debugging
4. **Query logging**: Debug output for executed plans
5. **Batch assert macro**: Simplify test assertions
6. **Function documentation**: Add doc comments to all functions

## Contributing Guidelines

When adding new features:

1. Check this document for priority and complexity
2. Follow patterns in `ARCHITECTURE.md`
3. Add tests in appropriate `tests/bigquery/` file
4. Update `DEPENDENCIES.md` if adding cross-cutting changes
5. Keep commits focused and well-documented
