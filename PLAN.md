# Worker 5: VALUES Clause

## Objective
Implement the standalone VALUES clause to remove `#[ignore]` tags from `tests/postgresql/queries/values.rs`.

## Test File
- `tests/postgresql/queries/values.rs` (20 ignored tests)

## Features to Implement

### 1. Basic VALUES Statement
- `VALUES (1), (2), (3)` - Standalone VALUES returning rows
- `VALUES (1, 'a'), (2, 'b')` - Multiple columns
- Returns a virtual table with columns named `column1`, `column2`, etc.

### 2. VALUES with Clauses
- `VALUES (...) ORDER BY 1` - Order results
- `VALUES (...) LIMIT n` - Limit rows
- `VALUES (...) OFFSET n` - Skip rows

### 3. VALUES as Subquery
- `SELECT * FROM (VALUES (1, 'a'), (2, 'b')) AS t(id, name)`
- Column aliasing with AS clause

### 4. VALUES in Set Operations
- `VALUES (1) UNION VALUES (2)` - Union with VALUES
- `VALUES (1) UNION ALL VALUES (1)` - Union all
- `VALUES (1), (2) INTERSECT VALUES (2), (3)` - Intersect
- `VALUES (1), (2) EXCEPT VALUES (2)` - Except

### 5. VALUES in CTE
- `WITH data AS (VALUES (1, 'a')) SELECT * FROM data`

### 6. VALUES with Expressions
- `VALUES (1 + 2), (3 * 4)` - Expressions in VALUES
- `VALUES (1, NULL)` - NULL values
- Mixed types inferred from values

### 7. VALUES in INSERT
- `INSERT INTO t SELECT * FROM (VALUES ...) AS v(cols)`

### 8. VALUES with EXISTS/IN
- `WHERE id IN (SELECT column1 FROM (VALUES (1), (2)) AS v)`
- `WHERE EXISTS (SELECT 1 FROM (VALUES ...) AS v WHERE ...)`

## Implementation Steps

1. **Parser Changes**
   - Parse `VALUES` as a standalone statement
   - Parse `VALUES` in FROM clause as table expression
   - Handle `AS alias(col1, col2)` syntax

2. **Logical Plan**
   - Create `LogicalPlan::Values` node
   - Store list of row expressions
   - Infer column types from values

3. **Physical Plan**
   - Create `PhysicalPlan::Values` operator
   - Evaluate each row's expressions
   - Produce result batches

4. **Query Execution**
   - Execute VALUES as a table source
   - Support ORDER BY, LIMIT, OFFSET on VALUES
   - Integrate with set operations (UNION, INTERSECT, EXCEPT)

5. **Column Naming**
   - Default names: `column1`, `column2`, ...
   - Apply aliases from AS clause

## Key Files to Modify
- `crates/parser/src/` - VALUES parsing
- `crates/executor/src/query_executor/` - Logical/physical plan
- Set operation handling

## Testing
```bash
cargo test --test postgresql queries::values
```

## Notes
- VALUES is essentially an inline table constructor
- Should work anywhere a SELECT can appear
