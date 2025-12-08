# PLAN_8: PostgreSQL JSON Type (33 tests)

## Overview
Complete implementation of PostgreSQL JSON/JSONB data type operations.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/postgresql/data_types/json_type.rs`

---

## Current Implementation Status

### DataType Definition
**File:** `crates/core/src/types/mod.rs`

```rust
pub enum DataType {
    Json,  // Stored as serde_json::Value
}
```

- Tag: `TAG_JSON` (132)
- Storage: `Rc<serde_json::Value>`
- Accessor: `value.as_json() -> Option<&serde_json::Value>`

### Existing JSON Module
**File:** `crates/functions/src/json/`

- `conversion.rs` - SQL to JSON conversion
- `functions.rs` - JSON manipulation
- `extract.rs` - JSONPath extraction
- `path.rs` - JSONPath parsing
- `postgres.rs` - PostgreSQL JSON functions

---

## JSON Operators to Implement

### Extraction Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `->` | Get JSON object field (as JSON) | `'{"a":1}'::json -> 'a'` → `1` |
| `->>` | Get JSON object field (as text) | `'{"a":1}'::json ->> 'a'` → `'1'` |
| `->` | Get JSON array element | `'[1,2,3]'::json -> 0` → `1` |
| `->>` | Get JSON array element (as text) | `'[1,2,3]'::json ->> 0` → `'1'` |
| `#>` | Get JSON at path (as JSON) | `'{"a":{"b":1}}'::json #> '{a,b}'` → `1` |
| `#>>` | Get JSON at path (as text) | `'{"a":{"b":1}}'::json #>> '{a,b}'` → `'1'` |

### Containment Operators (JSONB only)

| Operator | Description | Example |
|----------|-------------|---------|
| `@>` | Contains | `'{"a":1,"b":2}'::jsonb @> '{"a":1}'` → `true` |
| `<@` | Contained by | `'{"a":1}'::jsonb <@ '{"a":1,"b":2}'` → `true` |
| `?` | Key exists | `'{"a":1}'::jsonb ? 'a'` → `true` |
| `?\|` | Any key exists | `'{"a":1}'::jsonb ?\| array['a','b']` → `true` |
| `?&` | All keys exist | `'{"a":1,"b":2}'::jsonb ?& array['a','b']` → `true` |

### Modification Operators (JSONB only)

| Operator | Description | Example |
|----------|-------------|---------|
| `\|\|` | Concatenate | `'{"a":1}'::jsonb \|\| '{"b":2}'` → `'{"a":1,"b":2}'` |
| `-` | Delete key | `'{"a":1,"b":2}'::jsonb - 'a'` → `'{"b":2}'` |
| `-` | Delete at index | `'[1,2,3]'::jsonb - 1` → `'[1,3]'` |
| `#-` | Delete at path | `'{"a":{"b":1}}'::jsonb #- '{a,b}'` → `'{"a":{}}'` |

---

## JSON Functions to Implement

### Type & Inspection Functions

| Function | Description |
|----------|-------------|
| `json_typeof(json)` | Get JSON value type as text |
| `jsonb_typeof(jsonb)` | Same for JSONB |
| `json_array_length(json)` | Get array length |
| `jsonb_array_length(jsonb)` | Same for JSONB |

### Construction Functions

| Function | Description |
|----------|-------------|
| `to_json(value)` | Convert SQL value to JSON |
| `to_jsonb(value)` | Convert SQL value to JSONB |
| `json_build_array(...)` | Build JSON array |
| `jsonb_build_array(...)` | Same for JSONB |
| `json_build_object(...)` | Build JSON object |
| `jsonb_build_object(...)` | Same for JSONB |
| `json_object(keys, values)` | Build object from arrays |
| `jsonb_object(keys, values)` | Same for JSONB |

### Query Functions

| Function | Description |
|----------|-------------|
| `json_extract_path(json, ...)` | Extract at path |
| `json_extract_path_text(json, ...)` | Extract as text |
| `jsonb_extract_path(jsonb, ...)` | Same for JSONB |
| `jsonb_extract_path_text(jsonb, ...)` | Same for JSONB |

### Expansion Functions

| Function | Description |
|----------|-------------|
| `json_array_elements(json)` | Expand array to rows |
| `jsonb_array_elements(jsonb)` | Same for JSONB |
| `json_array_elements_text(json)` | Expand array to text rows |
| `jsonb_array_elements_text(jsonb)` | Same for JSONB |
| `json_each(json)` | Expand object to key/value rows |
| `jsonb_each(jsonb)` | Same for JSONB |
| `json_each_text(json)` | Expand to text values |
| `jsonb_each_text(jsonb)` | Same for JSONB |
| `json_object_keys(json)` | Get object keys |
| `jsonb_object_keys(jsonb)` | Same for JSONB |

### Modification Functions (JSONB)

| Function | Description |
|----------|-------------|
| `jsonb_set(jsonb, path, value)` | Set value at path |
| `jsonb_set_lax(jsonb, path, value, ...)` | Set with null handling |
| `jsonb_insert(jsonb, path, value)` | Insert at path |
| `jsonb_delete_path(jsonb, path)` | Delete at path |
| `jsonb_strip_nulls(jsonb)` | Remove null values |
| `jsonb_path_exists(jsonb, path)` | Check if path exists |
| `jsonb_path_match(jsonb, path)` | Match JSONPath |
| `jsonb_path_query(jsonb, path)` | Query with JSONPath |

### Aggregate Functions

| Function | Description |
|----------|-------------|
| `json_agg(value)` | Aggregate to JSON array |
| `jsonb_agg(value)` | Same for JSONB |
| `json_object_agg(key, value)` | Aggregate to JSON object |
| `jsonb_object_agg(key, value)` | Same for JSONB |

---

## Implementation Details

### Operator Implementation

```rust
// In crates/functions/src/json/postgres.rs

pub fn json_arrow(json: &serde_json::Value, key: &Value) -> Result<Value> {
    match (json, key) {
        // Object field access
        (serde_json::Value::Object(map), key) if key.is_string() => {
            let key_str = key.as_str().unwrap();
            match map.get(key_str) {
                Some(v) => Ok(Value::json(v.clone())),
                None => Ok(Value::null()),
            }
        }
        // Array index access
        (serde_json::Value::Array(arr), key) if key.is_integer() => {
            let idx = key.as_i64().unwrap();
            let idx = if idx < 0 {
                (arr.len() as i64 + idx) as usize
            } else {
                idx as usize
            };
            match arr.get(idx) {
                Some(v) => Ok(Value::json(v.clone())),
                None => Ok(Value::null()),
            }
        }
        _ => Err(Error::type_mismatch("invalid JSON access")),
    }
}

pub fn json_arrow_text(json: &serde_json::Value, key: &Value) -> Result<Value> {
    let result = json_arrow(json, key)?;
    if result.is_null() {
        Ok(Value::null())
    } else {
        let json_val = result.as_json().unwrap();
        Ok(Value::string(json_to_text(json_val)))
    }
}

fn json_to_text(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Null => "null".to_string(),
        _ => v.to_string(),
    }
}
```

### Path Extraction

```rust
pub fn json_extract_path(json: &serde_json::Value, path: &[&str]) -> Result<Value> {
    let mut current = json;

    for key in path {
        match current {
            serde_json::Value::Object(map) => {
                current = map.get(*key).ok_or_else(|| Error::json_path_not_found())?;
            }
            serde_json::Value::Array(arr) => {
                let idx: usize = key.parse()
                    .map_err(|_| Error::invalid_json_index())?;
                current = arr.get(idx).ok_or_else(|| Error::json_path_not_found())?;
            }
            _ => return Err(Error::json_path_not_found()),
        }
    }

    Ok(Value::json(current.clone()))
}
```

### Containment Check

```rust
pub fn jsonb_contains(container: &serde_json::Value, contained: &serde_json::Value) -> bool {
    match (container, contained) {
        (serde_json::Value::Object(c_map), serde_json::Value::Object(t_map)) => {
            t_map.iter().all(|(k, v)| {
                c_map.get(k).map_or(false, |cv| jsonb_contains(cv, v))
            })
        }
        (serde_json::Value::Array(c_arr), serde_json::Value::Array(t_arr)) => {
            t_arr.iter().all(|tv| {
                c_arr.iter().any(|cv| jsonb_contains(cv, tv))
            })
        }
        _ => container == contained,
    }
}
```

### Key Exists

```rust
pub fn jsonb_exists(json: &serde_json::Value, key: &str) -> bool {
    match json {
        serde_json::Value::Object(map) => map.contains_key(key),
        serde_json::Value::Array(arr) => {
            arr.iter().any(|v| {
                if let serde_json::Value::String(s) = v {
                    s == key
                } else {
                    false
                }
            })
        }
        _ => false,
    }
}
```

### Set-Returning Functions

```rust
// json_array_elements returns a table function
pub struct JsonArrayElementsFunction;

impl TableFunction for JsonArrayElementsFunction {
    fn name(&self) -> &str { "json_array_elements" }

    fn execute(&self, args: &[Value]) -> Result<RecordBatch> {
        let json = args[0].as_json()
            .ok_or_else(|| Error::type_mismatch("expected json"))?;

        let arr = match json {
            serde_json::Value::Array(arr) => arr,
            _ => return Err(Error::type_mismatch("expected json array")),
        };

        let values: Vec<Value> = arr.iter()
            .map(|v| Value::json(v.clone()))
            .collect();

        RecordBatch::from_column("value", values)
    }
}

// json_each returns key-value pairs
pub struct JsonEachFunction;

impl TableFunction for JsonEachFunction {
    fn name(&self) -> &str { "json_each" }

    fn execute(&self, args: &[Value]) -> Result<RecordBatch> {
        let json = args[0].as_json()
            .ok_or_else(|| Error::type_mismatch("expected json"))?;

        let obj = match json {
            serde_json::Value::Object(map) => map,
            _ => return Err(Error::type_mismatch("expected json object")),
        };

        let keys: Vec<Value> = obj.keys()
            .map(|k| Value::string(k.clone()))
            .collect();
        let values: Vec<Value> = obj.values()
            .map(|v| Value::json(v.clone()))
            .collect();

        RecordBatch::from_columns(vec![
            ("key", keys),
            ("value", values),
        ])
    }
}
```

### JSONB Modification

```rust
pub fn jsonb_set(
    target: &serde_json::Value,
    path: &[String],
    new_value: &serde_json::Value,
    create_missing: bool,
) -> Result<serde_json::Value> {
    if path.is_empty() {
        return Ok(new_value.clone());
    }

    let key = &path[0];
    let rest = &path[1..];

    match target {
        serde_json::Value::Object(map) => {
            let mut new_map = map.clone();
            if rest.is_empty() {
                new_map.insert(key.clone(), new_value.clone());
            } else {
                let existing = map.get(key).cloned().unwrap_or_else(|| {
                    if create_missing {
                        serde_json::Value::Object(serde_json::Map::new())
                    } else {
                        serde_json::Value::Null
                    }
                });
                let updated = jsonb_set(&existing, rest, new_value, create_missing)?;
                new_map.insert(key.clone(), updated);
            }
            Ok(serde_json::Value::Object(new_map))
        }
        serde_json::Value::Array(arr) => {
            let idx: usize = key.parse()
                .map_err(|_| Error::invalid_json_index())?;
            let mut new_arr = arr.clone();
            if idx < new_arr.len() {
                if rest.is_empty() {
                    new_arr[idx] = new_value.clone();
                } else {
                    new_arr[idx] = jsonb_set(&arr[idx], rest, new_value, create_missing)?;
                }
            }
            Ok(serde_json::Value::Array(new_arr))
        }
        _ => Err(Error::type_mismatch("cannot set in scalar")),
    }
}
```

---

## Key Files to Modify

1. **JSON Functions:** `crates/functions/src/json/postgres.rs`
   - Add operator implementations
   - Add query functions

2. **Table Functions:** `crates/functions/src/json/table_funcs.rs` (new)
   - `json_array_elements`
   - `json_each`
   - `json_object_keys`

3. **Aggregate Functions:** `crates/functions/src/aggregate/json_agg.rs`
   - Verify/fix `json_agg`, `jsonb_agg`
   - Add `json_object_agg`, `jsonb_object_agg`

4. **Registry:** `crates/functions/src/registry/json_funcs.rs`
   - Register all JSON functions

5. **Operators:** `crates/parser/src/ast_visitor/expr/mod.rs`
   - Ensure JSON operators are parsed

---

## Implementation Order

### Phase 1: Operators
1. `->` and `->>` for objects
2. `->` and `->>` for arrays
3. `#>` and `#>>` path access

### Phase 2: Query Functions
1. `json_typeof`, `json_array_length`
2. `json_extract_path`, `json_extract_path_text`
3. `json_object_keys`

### Phase 3: Containment (JSONB)
1. `@>` contains
2. `<@` contained by
3. `?`, `?|`, `?&` key exists

### Phase 4: Set-Returning Functions
1. `json_array_elements`
2. `json_each`, `json_each_text`

### Phase 5: Modification (JSONB)
1. `||` concatenation
2. `-` delete key/index
3. `jsonb_set`, `jsonb_insert`

### Phase 6: Construction
1. `json_build_array`, `json_build_object`
2. `to_json`, `to_jsonb`

---

## Testing Pattern

```rust
#[test]
fn test_json_arrow_object() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT '{\"a\": 1, \"b\": 2}'::json -> 'a'"
    ).unwrap();
    assert_batch_eq!(result, [[json!(1)]]);
}

#[test]
fn test_json_arrow_text() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT '{\"a\": 1}'::json ->> 'a'"
    ).unwrap();
    assert_batch_eq!(result, [["1"]]);
}

#[test]
fn test_jsonb_contains() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT '{\"a\": 1, \"b\": 2}'::jsonb @> '{\"a\": 1}'"
    ).unwrap();
    assert_batch_eq!(result, [[true]]);
}

#[test]
fn test_json_array_elements() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT * FROM json_array_elements('[1, 2, 3]'::json)"
    ).unwrap();
    assert_batch_eq!(result, [
        [json!(1)],
        [json!(2)],
        [json!(3)],
    ]);
}

#[test]
fn test_jsonb_set() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT jsonb_set('{\"a\": 1}'::jsonb, '{b}', '2'::jsonb)"
    ).unwrap();
    assert_batch_eq!(result, [[json!({"a": 1, "b": 2})]]);
}
```

---

## Verification Steps

1. Run: `cargo test --test postgresql -- data_types::json_type --ignored`
2. Implement operators first (most commonly used)
3. Add query functions
4. Add set-returning functions
5. Add modification functions
6. Remove `#[ignore = "Implement me!"]` as tests pass