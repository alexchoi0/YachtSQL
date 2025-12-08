# PLAN_5: PostgreSQL Range & Vector Types (37 + 34 = 71 tests)

## Overview
Complete implementation of PostgreSQL Range and Vector (pgvector) data types.

## Test File Locations
- `tests/postgresql/data_types/range.rs` (37 tests)
- `tests/postgresql/data_types/vector.rs` (34 tests)

---

# PART A: RANGE TYPE (37 tests)

## Current Implementation Status

### DataType Definition
**File:** `crates/core/src/types/mod.rs`

```rust
pub enum RangeType {
    Int4Range,
    Int8Range,
    NumRange,
    TsRange,
    TsTzRange,
    DateRange,
}

pub struct Range {
    pub range_type: RangeType,
    pub lower: Option<Value>,
    pub upper: Option<Value>,
    pub lower_inclusive: bool,
    pub upper_inclusive: bool,
}
```

### Existing Functions
**File:** `crates/functions/src/registry/range_funcs.rs`

- `INT4RANGE(lower, upper, bounds)` - Create int4 range
- `INT8RANGE(lower, upper, bounds)` - Create int8 range
- `RANGE_CONTAINS_ELEM` - Test if element in range
- `RANGE_CONTAINS` - Test if range contains range
- `RANGE_OVERLAPS` - Test if ranges overlap

---

## Range Functions to Implement

### Constructor Functions

| Function | Description | Implementation |
|----------|-------------|----------------|
| `int4range(low, high)` | Create int4 range | ✅ Exists |
| `int4range(low, high, bounds)` | With bounds spec | ✅ Exists |
| `int8range(low, high)` | Create int8 range | ✅ Exists |
| `numrange(low, high)` | Create numeric range | Implement |
| `tsrange(low, high)` | Timestamp range | Implement |
| `tstzrange(low, high)` | Timestamp with tz range | Implement |
| `daterange(low, high)` | Date range | Implement |

### Range Operators (as functions)

| Operator | Function | Description |
|----------|----------|-------------|
| `@>` | `range_contains_elem(r, e)` | Range contains element |
| `@>` | `range_contains(r1, r2)` | Range contains range |
| `<@` | `elem_contained_by_range(e, r)` | Element in range |
| `<@` | `range_contained_by(r1, r2)` | Range contained by |
| `&&` | `range_overlaps(r1, r2)` | Ranges overlap |
| `<<` | `range_left_of(r1, r2)` | Strictly left of |
| `>>` | `range_right_of(r1, r2)` | Strictly right of |
| `&<` | `range_not_extends_right(r1, r2)` | Does not extend right |
| `&>` | `range_not_extends_left(r1, r2)` | Does not extend left |
| `-|-` | `range_adjacent(r1, r2)` | Ranges are adjacent |
| `+` | `range_union(r1, r2)` | Union of ranges |
| `*` | `range_intersection(r1, r2)` | Intersection |
| `-` | `range_difference(r1, r2)` | Difference |

### Range Functions

| Function | Description | Implementation |
|----------|-------------|----------------|
| `lower(range)` | Get lower bound | Extract lower |
| `upper(range)` | Get upper bound | Extract upper |
| `isempty(range)` | Test if empty | Check bounds |
| `lower_inc(range)` | Lower inclusive? | Return bool |
| `upper_inc(range)` | Upper inclusive? | Return bool |
| `lower_inf(range)` | Lower infinite? | lower is None |
| `upper_inf(range)` | Upper infinite? | upper is None |
| `range_merge(r1, r2)` | Smallest range containing both | Compute merged |

---

## Implementation Details

### Range Parsing
```rust
// Parse range literals like '[1,5)' or '(,10]'
pub fn parse_range_literal(s: &str, range_type: RangeType) -> Result<Range> {
    let s = s.trim();
    if s == "empty" {
        return Ok(Range::empty(range_type));
    }

    let lower_inclusive = s.starts_with('[');
    let upper_inclusive = s.ends_with(']');

    let inner = &s[1..s.len()-1];
    let parts: Vec<&str> = inner.split(',').collect();

    let lower = if parts[0].is_empty() {
        None  // Unbounded
    } else {
        Some(parse_range_value(parts[0], &range_type)?)
    };

    let upper = if parts[1].is_empty() {
        None  // Unbounded
    } else {
        Some(parse_range_value(parts[1], &range_type)?)
    };

    Ok(Range {
        range_type,
        lower,
        upper,
        lower_inclusive,
        upper_inclusive,
    })
}
```

### Range Operators Implementation
```rust
impl Range {
    pub fn contains_elem(&self, elem: &Value) -> bool {
        let in_lower = match &self.lower {
            None => true,
            Some(l) => {
                if self.lower_inclusive {
                    elem >= l
                } else {
                    elem > l
                }
            }
        };

        let in_upper = match &self.upper {
            None => true,
            Some(u) => {
                if self.upper_inclusive {
                    elem <= u
                } else {
                    elem < u
                }
            }
        };

        in_lower && in_upper
    }

    pub fn overlaps(&self, other: &Range) -> bool {
        // Ranges overlap if neither is strictly left/right of the other
        !self.strictly_left_of(other) && !self.strictly_right_of(other)
    }

    pub fn strictly_left_of(&self, other: &Range) -> bool {
        match (&self.upper, &other.lower) {
            (Some(u), Some(l)) => {
                if self.upper_inclusive && other.lower_inclusive {
                    u < l
                } else {
                    u <= l
                }
            }
            _ => false,
        }
    }

    pub fn adjacent(&self, other: &Range) -> bool {
        // Adjacent if union would be a single range but they don't overlap
        match (&self.upper, &other.lower) {
            (Some(u), Some(l)) if u == l => {
                self.upper_inclusive != other.lower_inclusive
            }
            _ => false,
        } ||
        match (&other.upper, &self.lower) {
            (Some(u), Some(l)) if u == l => {
                other.upper_inclusive != self.lower_inclusive
            }
            _ => false,
        }
    }
}
```

---

# PART B: VECTOR TYPE (34 tests)

## Current Implementation Status

### DataType Definition
**File:** `crates/core/src/types/mod.rs`

```rust
pub enum DataType {
    Vector(usize),  // Parameterized by dimension
}
```

### Value Storage
- Tag: `TAG_VECTOR` (137)
- Stored as: `Rc<Vec<f64>>`
- Accessor: `value.as_vector() -> Option<&Vec<f64>>`

---

## Vector Functions to Implement

### Constructor/Conversion

| Function | Description | Implementation |
|----------|-------------|----------------|
| `vector(array)` | Create from array | Convert array to vector |
| `::vector` | Cast to vector | Type coercion |

### Distance Functions

| Function | Description | Formula |
|----------|-------------|---------|
| `<->` / `l2_distance(a, b)` | Euclidean distance | `sqrt(Σ(ai-bi)²)` |
| `<#>` / `negative_inner_product(a, b)` | Negative inner product | `-Σ(ai*bi)` |
| `<=>` / `cosine_distance(a, b)` | Cosine distance | `1 - (a·b)/(‖a‖*‖b‖)` |
| `<+>` / `l1_distance(a, b)` | Manhattan distance | `Σ|ai-bi|` |

### Vector Operations

| Function | Description | Implementation |
|----------|-------------|----------------|
| `inner_product(a, b)` | Dot product | `Σ(ai*bi)` |
| `l2_norm(a)` | L2 norm (magnitude) | `sqrt(Σai²)` |
| `l1_norm(a)` | L1 norm | `Σ|ai|` |
| `vector_dims(a)` | Get dimensions | `a.len()` |
| `vector_norm(a)` | Same as l2_norm | Alias |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `avg(vector)` | Element-wise average |
| `sum(vector)` | Element-wise sum |

---

## Implementation Details

### Distance Functions
```rust
pub fn l2_distance(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(Error::dimension_mismatch(a.len(), b.len()));
    }

    let sum: f64 = a.iter().zip(b.iter())
        .map(|(x, y)| (x - y).powi(2))
        .sum();

    Ok(sum.sqrt())
}

pub fn cosine_distance(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(Error::dimension_mismatch(a.len(), b.len()));
    }

    let dot: f64 = a.iter().zip(b.iter()).map(|(x, y)| x * y).sum();
    let norm_a: f64 = a.iter().map(|x| x.powi(2)).sum::<f64>().sqrt();
    let norm_b: f64 = b.iter().map(|x| x.powi(2)).sum::<f64>().sqrt();

    if norm_a == 0.0 || norm_b == 0.0 {
        return Ok(f64::NAN);
    }

    Ok(1.0 - (dot / (norm_a * norm_b)))
}

pub fn l1_distance(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(Error::dimension_mismatch(a.len(), b.len()));
    }

    Ok(a.iter().zip(b.iter()).map(|(x, y)| (x - y).abs()).sum())
}

pub fn inner_product(a: &[f64], b: &[f64]) -> Result<f64> {
    if a.len() != b.len() {
        return Err(Error::dimension_mismatch(a.len(), b.len()));
    }

    Ok(a.iter().zip(b.iter()).map(|(x, y)| x * y).sum())
}
```

### Operator Registration
```rust
// In operator registry or as functions

// <-> operator (L2 distance)
registry.register_binary_operator(
    "<->".to_string(),
    DataType::Vector(0),  // Any dimension
    DataType::Vector(0),
    DataType::Float64,
    |left, right| {
        let a = left.as_vector().ok_or_else(|| Error::type_mismatch("expected vector"))?;
        let b = right.as_vector().ok_or_else(|| Error::type_mismatch("expected vector"))?;
        Ok(Value::float64(l2_distance(a, b)?))
    },
);
```

### Vector Aggregate Implementation
```rust
#[derive(Debug)]
pub struct VectorAvgAccumulator {
    sum: Vec<f64>,
    count: usize,
}

impl Accumulator for VectorAvgAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() { return Ok(()); }

        let vec = value.as_vector()
            .ok_or_else(|| Error::type_mismatch("expected vector"))?;

        if self.sum.is_empty() {
            self.sum = vec.clone();
        } else if self.sum.len() != vec.len() {
            return Err(Error::dimension_mismatch(self.sum.len(), vec.len()));
        } else {
            for (s, v) in self.sum.iter_mut().zip(vec.iter()) {
                *s += v;
            }
        }
        self.count += 1;
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            return Ok(Value::null());
        }
        let avg: Vec<f64> = self.sum.iter().map(|s| s / self.count as f64).collect();
        Ok(Value::vector(avg))
    }

    fn reset(&mut self) {
        self.sum.clear();
        self.count = 0;
    }

    fn merge(&mut self, other: &dyn Accumulator) -> Result<()> {
        if let Some(o) = other.as_any().downcast_ref::<VectorAvgAccumulator>() {
            if self.sum.is_empty() {
                self.sum = o.sum.clone();
            } else {
                for (s, os) in self.sum.iter_mut().zip(o.sum.iter()) {
                    *s += os;
                }
            }
            self.count += o.count;
        }
        Ok(())
    }

    fn as_any(&self) -> &dyn std::any::Any { self }
}
```

---

## Key Files to Modify

### Range Type
1. `crates/functions/src/range.rs` - Range operations
2. `crates/functions/src/registry/range_funcs.rs` - Function registration
3. `crates/parser/src/` - Range literal parsing

### Vector Type
1. `crates/functions/src/vector.rs` (new) - Vector operations
2. `crates/functions/src/registry/vector_funcs.rs` (new) - Function registration
3. `crates/functions/src/aggregate/vector.rs` (new) - Vector aggregates

---

## Implementation Order

### Range (Phase 1)
1. `numrange`, `daterange`, `tsrange` constructors
2. `lower()`, `upper()`, `isempty()` functions
3. `lower_inc()`, `upper_inc()`, `lower_inf()`, `upper_inf()`
4. Range operators as functions
5. `range_merge()`

### Vector (Phase 1)
1. `l2_distance()`, `cosine_distance()`, `l1_distance()`
2. `inner_product()`, `l2_norm()`, `l1_norm()`
3. `vector_dims()`
4. Operator registration (`<->`, `<=>`, etc.)

### Vector (Phase 2)
1. Vector aggregates (`avg`, `sum`)
2. Index support (future)

---

## Testing Pattern

### Range Tests
```rust
#[test]
fn test_int4range_contains() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT int4range(1, 10) @> 5"
    ).unwrap();
    assert_batch_eq!(result, [[true]]);
}

#[test]
fn test_range_functions() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT lower(int4range(1, 10)), upper(int4range(1, 10))"
    ).unwrap();
    assert_batch_eq!(result, [[1, 10]]);
}
```

### Vector Tests
```rust
#[test]
fn test_vector_l2_distance() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT '[1,0,0]'::vector <-> '[0,1,0]'::vector"
    ).unwrap();
    // sqrt(2) ≈ 1.414
    assert_batch_eq!(result, [[1.4142135623730951]]);
}

#[test]
fn test_vector_cosine_distance() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT '[1,0]'::vector <=> '[0,1]'::vector"
    ).unwrap();
    // Orthogonal vectors have cosine distance of 1
    assert_batch_eq!(result, [[1.0]]);
}
```

---

## Verification Steps

1. Run: `cargo test --test postgresql -- data_types::range --ignored`
2. Run: `cargo test --test postgresql -- data_types::vector --ignored`
3. Implement missing functions
4. Remove `#[ignore = "Implement me!"]` as tests pass