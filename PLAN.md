# PLAN_7: ClickHouse Higher-Order Functions (33 tests)

## Overview
Implement ClickHouse higher-order functions that operate on arrays using lambda expressions.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/clickhouse/functions/higher_order.rs`

---

## What Are Higher-Order Functions?

Higher-order functions take other functions (lambdas) as arguments. In ClickHouse:

```sql
-- Lambda syntax: x -> expression
arrayMap(x -> x * 2, [1, 2, 3])  -- Returns [2, 4, 6]

-- Multi-argument lambda: (x, y) -> expression
arrayMap((x, y) -> x + y, [1, 2], [10, 20])  -- Returns [11, 22]
```

---

## Functions to Implement

### Array Transformation Functions

| Function | Description | Example |
|----------|-------------|---------|
| `arrayMap(func, arr)` | Apply func to each element | `arrayMap(x -> x*2, [1,2,3])` → `[2,4,6]` |
| `arrayFilter(func, arr)` | Keep elements where func is true | `arrayFilter(x -> x>2, [1,2,3,4])` → `[3,4]` |
| `arrayTransform(arr, func)` | Same as arrayMap (alias) | |
| `arrayFill(func, arr)` | Fill elements based on condition | |
| `arrayReverseFill(func, arr)` | Fill in reverse | |
| `arraySplit(func, arr)` | Split array where func is true | |
| `arrayReverseSplit(func, arr)` | Split in reverse | |

### Array Reduction Functions

| Function | Description | Example |
|----------|-------------|---------|
| `arrayReduce(agg, arr)` | Apply aggregate to array | `arrayReduce('sum', [1,2,3])` → `6` |
| `arrayReduceInRanges(agg, ranges, arr)` | Reduce within ranges | |
| `arrayFold(func, arr, init)` | Fold with accumulator | `arrayFold((acc,x)->acc+x, [1,2,3], 0)` → `6` |

### Array Testing Functions

| Function | Description | Example |
|----------|-------------|---------|
| `arrayExists(func, arr)` | Any element satisfies func | `arrayExists(x -> x>5, [1,2,6])` → `1` |
| `arrayAll(func, arr)` | All elements satisfy func | `arrayAll(x -> x>0, [1,2,3])` → `1` |
| `arrayFirst(func, arr)` | First element satisfying func | `arrayFirst(x -> x>2, [1,2,3])` → `3` |
| `arrayLast(func, arr)` | Last element satisfying func | |
| `arrayFirstIndex(func, arr)` | Index of first match | `arrayFirstIndex(x -> x>2, [1,2,3])` → `3` |
| `arrayLastIndex(func, arr)` | Index of last match | |
| `arrayCount(func, arr)` | Count matching elements | `arrayCount(x -> x>1, [1,2,3])` → `2` |

### Array Sorting Functions

| Function | Description | Example |
|----------|-------------|---------|
| `arraySort(func, arr)` | Sort by func result | `arraySort(x -> -x, [1,3,2])` → `[3,2,1]` |
| `arrayReverseSort(func, arr)` | Sort descending | |
| `arrayPartialSort(limit, func, arr)` | Partial sort | |

### Multi-Array Functions

| Function | Description | Example |
|----------|-------------|---------|
| `arrayMap(func, arr1, arr2)` | Map over multiple arrays | `arrayMap((x,y)->x+y, [1,2], [10,20])` |
| `arrayZip(arr1, arr2, ...)` | Combine arrays into tuples | `arrayZip([1,2], ['a','b'])` → `[(1,'a'),(2,'b')]` |

---

## Architecture

### Lambda Expression IR

**File:** `crates/ir/src/expr.rs`

Need to add or verify lambda expression support:

```rust
pub enum Expr {
    Lambda {
        params: Vec<String>,       // Parameter names
        body: Box<Expr>,           // Lambda body expression
    },
    // ... other variants
}
```

### Lambda Parsing

**File:** `crates/parser/src/ast_visitor/expr/mod.rs`

Parse ClickHouse lambda syntax `x -> expr` or `(x, y) -> expr`:

```rust
fn parse_lambda(&self, lambda: &SqlLambda) -> Result<Expr> {
    let params = lambda.params.iter()
        .map(|p| p.to_string())
        .collect();

    let body = self.sql_expr_to_expr(&lambda.body)?;

    Ok(Expr::Lambda {
        params,
        body: Box::new(body),
    })
}
```

### Lambda Evaluation

**File:** `crates/executor/src/query_executor/execution/expr.rs`

```rust
pub struct LambdaContext {
    bindings: HashMap<String, Value>,
}

impl LambdaContext {
    pub fn bind(&mut self, name: &str, value: Value) {
        self.bindings.insert(name.to_string(), value);
    }

    pub fn get(&self, name: &str) -> Option<&Value> {
        self.bindings.get(name)
    }
}

fn evaluate_lambda(
    &self,
    params: &[String],
    body: &Expr,
    args: &[Value],
    outer_context: &EvalContext,
) -> Result<Value> {
    let mut lambda_ctx = LambdaContext::new();

    for (param, arg) in params.iter().zip(args.iter()) {
        lambda_ctx.bind(param, arg.clone());
    }

    self.evaluate_expr_with_lambda_context(body, outer_context, &lambda_ctx)
}
```

---

## Implementation Details

### arrayMap Implementation

```rust
pub fn array_map(lambda: &Lambda, arrays: &[&Vec<Value>]) -> Result<Value> {
    // Verify all arrays have same length
    let len = arrays[0].len();
    for arr in arrays.iter().skip(1) {
        if arr.len() != len {
            return Err(Error::array_length_mismatch());
        }
    }

    let mut result = Vec::with_capacity(len);

    for i in 0..len {
        let args: Vec<Value> = arrays.iter().map(|arr| arr[i].clone()).collect();
        let value = evaluate_lambda(&lambda.params, &lambda.body, &args)?;
        result.push(value);
    }

    Ok(Value::array(result))
}
```

### arrayFilter Implementation

```rust
pub fn array_filter(lambda: &Lambda, array: &Vec<Value>) -> Result<Value> {
    let mut result = Vec::new();

    for elem in array {
        let keep = evaluate_lambda(&lambda.params, &lambda.body, &[elem.clone()])?;
        if keep.as_bool() == Some(true) {
            result.push(elem.clone());
        }
    }

    Ok(Value::array(result))
}
```

### arrayExists Implementation

```rust
pub fn array_exists(lambda: &Lambda, array: &Vec<Value>) -> Result<Value> {
    for elem in array {
        let result = evaluate_lambda(&lambda.params, &lambda.body, &[elem.clone()])?;
        if result.as_bool() == Some(true) {
            return Ok(Value::bool(true));
        }
    }
    Ok(Value::bool(false))
}
```

### arrayFold Implementation

```rust
pub fn array_fold(
    lambda: &Lambda,  // (acc, x) -> new_acc
    array: &Vec<Value>,
    initial: Value,
) -> Result<Value> {
    let mut acc = initial;

    for elem in array {
        acc = evaluate_lambda(&lambda.params, &lambda.body, &[acc, elem.clone()])?;
    }

    Ok(acc)
}
```

### arrayReduce Implementation

```rust
pub fn array_reduce(
    agg_name: &str,
    array: &Vec<Value>,
    registry: &FunctionRegistry,
) -> Result<Value> {
    let agg_func = registry.get_aggregate(agg_name)
        .ok_or_else(|| Error::unknown_function(agg_name))?;

    let mut acc = agg_func.create_accumulator();

    for elem in array {
        acc.accumulate(elem)?;
    }

    acc.finalize()
}
```

### arraySort Implementation

```rust
pub fn array_sort(
    lambda: Option<&Lambda>,
    array: &Vec<Value>,
) -> Result<Value> {
    let mut indexed: Vec<(usize, Value)> = array.iter()
        .enumerate()
        .map(|(i, v)| (i, v.clone()))
        .collect();

    if let Some(lambda) = lambda {
        // Sort by lambda result
        indexed.sort_by(|a, b| {
            let key_a = evaluate_lambda(&lambda.params, &lambda.body, &[a.1.clone()]).unwrap();
            let key_b = evaluate_lambda(&lambda.params, &lambda.body, &[b.1.clone()]).unwrap();
            key_a.partial_cmp(&key_b).unwrap_or(std::cmp::Ordering::Equal)
        });
    } else {
        // Natural sort
        indexed.sort_by(|a, b| {
            a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    Ok(Value::array(indexed.into_iter().map(|(_, v)| v).collect()))
}
```

---

## Key Files to Modify

1. **IR:** `crates/ir/src/expr.rs`
   - Add/verify `Lambda` expression variant

2. **Parser:** `crates/parser/src/ast_visitor/expr/mod.rs`
   - Parse lambda expressions

3. **Executor:** `crates/executor/src/query_executor/execution/expr.rs`
   - Add lambda evaluation context
   - Implement lambda evaluation

4. **Functions:** `crates/functions/src/array/higher_order.rs` (new)
   - Implement all higher-order array functions

5. **Registry:** `crates/functions/src/registry/array_funcs.rs`
   - Register higher-order functions

---

## Implementation Order

### Phase 1: Lambda Infrastructure
1. Add Lambda expression to IR
2. Parse lambda expressions
3. Implement lambda evaluation with context

### Phase 2: Basic Functions
1. `arrayMap` - Single and multi-array
2. `arrayFilter`
3. `arrayExists`, `arrayAll`

### Phase 3: Search Functions
1. `arrayFirst`, `arrayLast`
2. `arrayFirstIndex`, `arrayLastIndex`
3. `arrayCount`

### Phase 4: Transformation Functions
1. `arrayFold`
2. `arrayReduce`
3. `arraySort`, `arrayReverseSort`

### Phase 5: Advanced Functions
1. `arrayFill`, `arrayReverseFill`
2. `arraySplit`, `arrayReverseSplit`
3. `arrayZip`

---

## Testing Pattern

```rust
#[test]
fn test_array_map() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT arrayMap(x -> x * 2, [1, 2, 3])"
    ).unwrap();
    assert_batch_eq!(result, [[[2, 4, 6]]]);
}

#[test]
fn test_array_filter() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT arrayFilter(x -> x > 2, [1, 2, 3, 4, 5])"
    ).unwrap();
    assert_batch_eq!(result, [[[3, 4, 5]]]);
}

#[test]
fn test_array_exists() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT arrayExists(x -> x > 10, [1, 2, 3])"
    ).unwrap();
    assert_batch_eq!(result, [[0]]);  // false
}

#[test]
fn test_array_fold() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT arrayFold((acc, x) -> acc + x, [1, 2, 3, 4], 0)"
    ).unwrap();
    assert_batch_eq!(result, [[10]]);
}

#[test]
fn test_array_map_multi() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT arrayMap((x, y) -> x + y, [1, 2, 3], [10, 20, 30])"
    ).unwrap();
    assert_batch_eq!(result, [[[11, 22, 33]]]);
}

#[test]
fn test_array_sort_lambda() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT arraySort(x -> -x, [3, 1, 2])"
    ).unwrap();
    assert_batch_eq!(result, [[[3, 2, 1]]]);
}
```

---

## Edge Cases to Handle

1. **Empty arrays:** Return empty array for map/filter, false for exists, null for first/last
2. **Null elements:** Propagate nulls or filter based on function
3. **Type mismatches:** Clear error messages for lambda type errors
4. **Variable capture:** Lambdas should be able to reference outer scope variables
5. **Nested lambdas:** Support lambdas within lambdas if needed

---

## Verification Steps

1. Run: `cargo test --test clickhouse -- functions::higher_order --ignored`
2. Implement lambda parsing first
3. Add basic functions (map, filter)
4. Build up to complex functions
5. Remove `#[ignore = "Implement me!"]` as tests pass