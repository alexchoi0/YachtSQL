# Worker 7: LATERAL Subqueries

## Objective
Implement LATERAL subquery support to remove `#[ignore]` tags from `tests/postgresql/queries/lateral.rs`.

## Test File
- `tests/postgresql/queries/lateral.rs` (9 ignored tests)

## Features to Implement

### 1. Basic LATERAL
- `FROM t1, LATERAL (SELECT * FROM t2 WHERE t2.id = t1.id) sub`
- Subquery can reference columns from preceding FROM items

### 2. LATERAL with CROSS JOIN
- `FROM t1 CROSS JOIN LATERAL (subquery referencing t1)`
- Equivalent to comma syntax

### 3. LATERAL with LEFT JOIN
- `FROM t1 LEFT JOIN LATERAL (subquery) sub ON TRUE`
- Preserves left rows even when subquery returns empty

### 4. LATERAL Top-N Per Group
- Common pattern: get top N rows per group
- `LATERAL (SELECT * FROM t2 WHERE ... ORDER BY ... LIMIT n)`

### 5. LATERAL with Aggregates
- `LATERAL (SELECT AVG(x), MAX(y) FROM t2 WHERE t2.fk = t1.pk)`
- Compute aggregates correlated to outer row

### 6. LATERAL with VALUES
- `FROM (VALUES (1), (2)) AS t(n), LATERAL (SELECT n * 2)`
- LATERAL referencing VALUES table

### 7. LATERAL with UNNEST
- `FROM t, LATERAL UNNEST(t.array_col) AS u(elem)`
- Flatten arrays with row correlation

### 8. LATERAL with GENERATE_SERIES
- `FROM t, LATERAL GENERATE_SERIES(1, t.n) AS gs(val)`
- Generate rows based on outer column

### 9. Multiple LATERAL
- `FROM t1 CROSS JOIN LATERAL (...) a CROSS JOIN LATERAL (...) b`
- Chained LATERAL references

## Implementation Steps

1. **Parser Changes**
   - Parse `LATERAL` keyword before subquery in FROM
   - Mark subquery as lateral in AST

2. **Name Resolution**
   - For LATERAL subqueries, include preceding FROM items in scope
   - Allow column references to left-side tables

3. **Logical Plan**
   - Create `LogicalPlan::Lateral` or mark join as lateral
   - Track column dependencies across the lateral boundary

4. **Physical Plan / Execution**
   - For each row from left side:
     - Bind referenced columns
     - Execute the lateral subquery
     - Join results
   - Handle empty subquery results (for LEFT JOIN LATERAL)

5. **Optimization**
   - Consider decorrelation strategies
   - Cache subquery results when possible

## Key Files to Modify
- `crates/parser/src/` - LATERAL parsing
- `crates/executor/src/query_executor/` - Plan nodes and execution

## Testing
```bash
cargo test --test postgresql queries::lateral
```

## Notes
- Some LATERAL tests already pass (with CTE, JSON_EACH)
- Key challenge is correlated subquery execution model
- LATERAL is essentially a "for each" loop in SQL

## Execution Model
```
for each row in left_table:
    bind outer columns to current row values
    execute lateral subquery
    join current row with subquery results
```
