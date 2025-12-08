# PLAN_4: PostgreSQL Subqueries (37 tests)

## Overview
Implement various PostgreSQL subquery patterns.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/postgresql/queries/subqueries.rs`

## Current Implementation Status

### Existing Subquery Support
**File:** `crates/ir/src/expr.rs`

The IR already supports these subquery expression types:
```rust
pub enum Expr {
    Subquery { plan: Box<PlanNode> },
    Exists { plan: Box<PlanNode>, negated: bool },
    InSubquery { expr: Box<Expr>, plan: Box<PlanNode>, negated: bool },
    ScalarSubquery { subquery: Box<PlanNode> },
    TupleInSubquery { tuple: Vec<Expr>, plan: Box<PlanNode>, negated: bool },
}
```

### Parser Support
**File:** `crates/parser/src/ast_visitor/expr/subqueries.rs`

Conversion methods exist:
- `convert_scalar_subquery()` - Wraps query as scalar
- `convert_exists()` - EXISTS/NOT EXISTS
- `convert_in_subquery()` - IN (SELECT...)

### Optimizer
**File:** `crates/optimizer/src/rules/subquery_flattening.rs`

Subquery flattening converts correlated subqueries to joins.

---

## Subquery Types to Implement/Fix

### 1. Scalar Subqueries
```sql
-- In SELECT clause
SELECT name, (SELECT MAX(salary) FROM employees) as max_sal FROM departments

-- In WHERE clause
SELECT * FROM employees WHERE salary > (SELECT AVG(salary) FROM employees)
```

### 2. EXISTS Subqueries
```sql
-- EXISTS
SELECT * FROM departments d
WHERE EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)

-- NOT EXISTS
SELECT * FROM departments d
WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)
```

### 3. IN Subqueries
```sql
-- IN
SELECT * FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE active)

-- NOT IN
SELECT * FROM employees WHERE dept_id NOT IN (SELECT id FROM departments WHERE closed)

-- Tuple IN
SELECT * FROM orders WHERE (customer_id, product_id) IN (SELECT cust, prod FROM special_orders)
```

### 4. ANY/SOME/ALL Subqueries
```sql
-- ANY (same as SOME)
SELECT * FROM products WHERE price > ANY (SELECT price FROM sale_items)

-- ALL
SELECT * FROM products WHERE price > ALL (SELECT price FROM budget_items)
```

### 5. Correlated Subqueries
```sql
-- Correlated scalar
SELECT e.name, (SELECT d.name FROM departments d WHERE d.id = e.dept_id) as dept_name
FROM employees e

-- Correlated EXISTS
SELECT * FROM orders o
WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.quantity > 10)
```

### 6. Derived Tables (Subqueries in FROM)
```sql
SELECT * FROM (SELECT id, name FROM employees WHERE active) as active_emp
WHERE active_emp.id > 100
```

### 7. LATERAL Subqueries
```sql
SELECT d.name, e.top_salary
FROM departments d,
LATERAL (SELECT MAX(salary) as top_salary FROM employees WHERE dept_id = d.id) e
```

---

## Architecture Deep Dive

### Subquery Execution Flow

1. **Parsing:** SQL → AST → IR (subquery becomes `Expr::Subquery` or similar)
2. **Planning:** IR expressions attached to plan nodes
3. **Optimization:** Subquery flattening may convert to joins
4. **Execution:** Executor evaluates subquery for each outer row (or once for uncorrelated)

### Key Executor Code
**File:** `crates/executor/src/query_executor/execution/expr.rs`

```rust
fn evaluate_expr(&self, expr: &Expr, row: &Row, schema: &Schema) -> Result<Value> {
    match expr {
        Expr::ScalarSubquery { subquery } => {
            // Execute subquery, return single value
            let result = self.execute_plan(subquery)?;
            if result.num_rows() != 1 || result.num_columns() != 1 {
                return Err(Error::subquery_returns_multiple_rows());
            }
            Ok(result.get_value(0, 0).clone())
        }
        Expr::Exists { plan, negated } => {
            let result = self.execute_plan(plan)?;
            let exists = result.num_rows() > 0;
            Ok(Value::bool(if *negated { !exists } else { exists }))
        }
        Expr::InSubquery { expr, plan, negated } => {
            let value = self.evaluate_expr(expr, row, schema)?;
            let result = self.execute_plan(plan)?;
            let found = result.rows().any(|r| r.get(0) == Some(&value));
            Ok(Value::bool(if *negated { !found } else { found }))
        }
        // ...
    }
}
```

---

## Functions/Features to Implement

### 1. ANY/SOME/ALL Operators

**Add to IR:**
```rust
// In crates/ir/src/expr.rs
pub enum Expr {
    AnySubquery {
        left: Box<Expr>,
        op: BinaryOperator,
        subquery: Box<PlanNode>,
    },
    AllSubquery {
        left: Box<Expr>,
        op: BinaryOperator,
        subquery: Box<PlanNode>,
    },
}
```

**Parser support:**
```rust
// In crates/parser/src/ast_visitor/expr/subqueries.rs
fn convert_any_subquery(&self, left: &SqlExpr, op: &BinaryOperator, subquery: &Query) -> Result<Expr> {
    let plan = self.query_to_plan(subquery)?;
    Ok(Expr::AnySubquery {
        left: Box::new(self.sql_expr_to_expr(left)?),
        op: op.clone(),
        subquery: Box::new(plan.root),
    })
}
```

**Executor:**
```rust
Expr::AnySubquery { left, op, subquery } => {
    let left_val = self.evaluate_expr(left, row, schema)?;
    let result = self.execute_plan(subquery)?;

    for r in result.rows() {
        let right_val = r.get(0).cloned().unwrap_or(Value::null());
        if self.evaluate_binary_op(&left_val, op, &right_val)?.as_bool() == Some(true) {
            return Ok(Value::bool(true));
        }
    }
    Ok(Value::bool(false))
}

Expr::AllSubquery { left, op, subquery } => {
    let left_val = self.evaluate_expr(left, row, schema)?;
    let result = self.execute_plan(subquery)?;

    for r in result.rows() {
        let right_val = r.get(0).cloned().unwrap_or(Value::null());
        if self.evaluate_binary_op(&left_val, op, &right_val)?.as_bool() != Some(true) {
            return Ok(Value::bool(false));
        }
    }
    Ok(Value::bool(true))
}
```

### 2. Correlated Subquery Execution

For correlated subqueries, the subquery must be re-executed for each outer row with the correlation values bound.

**Current approach (subquery flattening):**
- Convert to semi-join for EXISTS
- Convert to anti-join for NOT EXISTS
- Convert to left join for scalar subqueries

**Alternative (direct execution):**
```rust
fn execute_correlated_subquery(
    &self,
    subquery: &PlanNode,
    outer_row: &Row,
    correlations: &[(String, usize)],  // (inner_col, outer_idx)
) -> Result<RecordBatch> {
    // Create a context with outer row values bound
    let mut ctx = self.create_context();
    for (col_name, outer_idx) in correlations {
        ctx.bind_value(col_name, outer_row.get(*outer_idx).clone());
    }
    self.execute_plan_with_context(subquery, &ctx)
}
```

### 3. LATERAL Joins

**Ensure plan node exists:**
```rust
// In crates/ir/src/plan.rs
pub enum PlanNode {
    LateralJoin {
        left: Box<PlanNode>,
        right: Box<PlanNode>,  // Can reference left's columns
        join_type: JoinType,
    },
}
```

**Executor:**
```rust
PlanNode::LateralJoin { left, right, join_type } => {
    let left_result = self.execute_plan(left)?;
    let mut output_rows = Vec::new();

    for left_row in left_result.rows() {
        // Execute right side with left row in scope
        let right_result = self.execute_lateral_subquery(right, &left_row)?;

        match join_type {
            JoinType::Inner => {
                for right_row in right_result.rows() {
                    output_rows.push(join_rows(&left_row, &right_row));
                }
            }
            JoinType::Left => {
                if right_result.is_empty() {
                    output_rows.push(join_with_nulls(&left_row, right.schema()));
                } else {
                    for right_row in right_result.rows() {
                        output_rows.push(join_rows(&left_row, &right_row));
                    }
                }
            }
            // ...
        }
    }
    Ok(RecordBatch::from_rows(output_rows))
}
```

---

## Key Files to Modify

1. **IR Expressions:** `crates/ir/src/expr.rs`
   - Add `AnySubquery`, `AllSubquery` if not present

2. **Parser:** `crates/parser/src/ast_visitor/expr/subqueries.rs`
   - Add conversion for ANY/ALL

3. **Executor:** `crates/executor/src/query_executor/execution/expr.rs`
   - Implement evaluation for all subquery types

4. **Optimizer:** `crates/optimizer/src/rules/subquery_flattening.rs`
   - Ensure correlated subqueries are properly handled

---

## Implementation Order

### Phase 1: Basic Subqueries
1. Verify scalar subqueries work
2. Verify EXISTS/NOT EXISTS work
3. Verify IN/NOT IN work

### Phase 2: ANY/ALL
1. Add IR expressions
2. Add parser support
3. Add executor evaluation

### Phase 3: Correlated Subqueries
1. Verify correlation detection in parser
2. Implement correlated execution
3. Test with various correlation patterns

### Phase 4: LATERAL
1. Verify LATERAL parsing
2. Implement LATERAL execution
3. Test LATERAL with aggregates

---

## Testing Pattern

```rust
#[test]
fn test_scalar_subquery() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE t (id INT64, val INT64)").unwrap();
    executor.execute_sql("INSERT INTO t VALUES (1, 10), (2, 20), (3, 30)").unwrap();

    let result = executor.execute_sql(
        "SELECT id, (SELECT MAX(val) FROM t) as max_val FROM t"
    ).unwrap();
    assert_batch_eq!(result, [
        [1, 30],
        [2, 30],
        [3, 30],
    ]);
}

#[test]
fn test_exists_correlated() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE orders (id INT64, customer_id INT64)").unwrap();
    executor.execute_sql("CREATE TABLE customers (id INT64, name STRING)").unwrap();
    executor.execute_sql("INSERT INTO customers VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    executor.execute_sql("INSERT INTO orders VALUES (1, 1)").unwrap();

    let result = executor.execute_sql(
        "SELECT name FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)"
    ).unwrap();
    assert_batch_eq!(result, [["Alice"]]);
}

#[test]
fn test_any_subquery() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE t1 (val INT64)").unwrap();
    executor.execute_sql("CREATE TABLE t2 (val INT64)").unwrap();
    executor.execute_sql("INSERT INTO t1 VALUES (5), (10), (15)").unwrap();
    executor.execute_sql("INSERT INTO t2 VALUES (8), (12)").unwrap();

    let result = executor.execute_sql(
        "SELECT val FROM t1 WHERE val > ANY (SELECT val FROM t2)"
    ).unwrap();
    assert_batch_eq!(result, [[10], [15]]);
}
```

---

## Verification Steps

1. Run: `cargo test --test postgresql -- queries::subqueries --ignored`
2. Identify which subquery patterns fail
3. Implement missing patterns
4. Remove `#[ignore = "Implement me!"]` as tests pass