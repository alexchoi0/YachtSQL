# PLAN_2: ClickHouse Information Schema (48 tests)

## Overview
Implement ClickHouse information_schema queries in `tests/clickhouse/queries/information_schema.rs`.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/clickhouse/queries/information_schema.rs`

## Current Status
Information schema is explicitly unsupported with error:
```rust
// crates/executor/src/query_executor/execution/query.rs
if dataset_id.eq_ignore_ascii_case("information_schema") {
    return Err(Error::unsupported_feature(
        "information_schema is not supported".to_string(),
    ));
}
```

## Architecture Reference

### Key Files to Modify
1. **Query Execution:** `crates/executor/src/query_executor/execution/query.rs`
2. **Catalog/Metadata:** `crates/executor/src/catalog/` (may need to create)
3. **System Tables:** New module for virtual system tables

### ClickHouse Information Schema Tables

ClickHouse provides these information_schema tables:

| Table | Description |
|-------|-------------|
| `information_schema.tables` | All tables in the system |
| `information_schema.columns` | All columns across tables |
| `information_schema.schemata` | Database/schema information |
| `information_schema.views` | View definitions |
| `information_schema.key_column_usage` | Primary key columns |
| `information_schema.table_constraints` | Table constraints |
| `information_schema.referential_constraints` | Foreign key relationships |

### Required Columns

#### information_schema.tables
```sql
CREATE VIEW information_schema.tables AS
SELECT
    database AS table_catalog,
    database AS table_schema,
    name AS table_name,
    'BASE TABLE' AS table_type,  -- or 'VIEW'
    engine AS table_engine,
    create_table_query AS table_definition
FROM system.tables
```

#### information_schema.columns
```sql
CREATE VIEW information_schema.columns AS
SELECT
    database AS table_catalog,
    database AS table_schema,
    table AS table_name,
    name AS column_name,
    position AS ordinal_position,
    default_expression AS column_default,
    is_in_primary_key AS is_nullable,  -- inverted logic
    type AS data_type,
    -- character_maximum_length, numeric_precision, etc.
FROM system.columns
```

#### information_schema.schemata
```sql
CREATE VIEW information_schema.schemata AS
SELECT
    name AS catalog_name,
    name AS schema_name,
    'default' AS schema_owner
FROM system.databases
```

---

## Implementation Strategy

### Option A: Virtual Table Approach (Recommended)

Create virtual tables that query the internal catalog:

1. **Add InformationSchemaTable enum:**
```rust
pub enum InformationSchemaTable {
    Tables,
    Columns,
    Schemata,
    Views,
    KeyColumnUsage,
    TableConstraints,
}
```

2. **Implement catalog queries:**
```rust
impl Executor {
    fn query_information_schema(&self, table: InformationSchemaTable) -> Result<RecordBatch> {
        match table {
            InformationSchemaTable::Tables => self.get_tables_info(),
            InformationSchemaTable::Columns => self.get_columns_info(),
            // ...
        }
    }

    fn get_tables_info(&self) -> Result<RecordBatch> {
        let mut rows = Vec::new();
        for (dataset_id, dataset) in &self.datasets {
            for (table_name, table) in dataset.tables() {
                rows.push(vec![
                    Value::string(dataset_id.clone()),  // table_catalog
                    Value::string(dataset_id.clone()),  // table_schema
                    Value::string(table_name.clone()),  // table_name
                    Value::string("BASE TABLE"),        // table_type
                ]);
            }
        }
        // Build RecordBatch from rows
    }
}
```

3. **Intercept in query execution:**
```rust
// In execute_scan or similar
if schema_name.eq_ignore_ascii_case("information_schema") {
    let info_table = InformationSchemaTable::from_str(table_name)?;
    return self.query_information_schema(info_table);
}
```

### Option B: System Tables Approach

Create actual system tables that are populated on startup and updated on DDL:

1. Create `system` dataset with read-only tables
2. Create views in `information_schema` that query `system` tables
3. Update system tables on CREATE/DROP/ALTER operations

---

## Key Files to Create/Modify

### New Files
- `crates/executor/src/information_schema/mod.rs` - Main module
- `crates/executor/src/information_schema/tables.rs` - Tables view
- `crates/executor/src/information_schema/columns.rs` - Columns view
- `crates/executor/src/information_schema/schemata.rs` - Schemata view

### Modified Files
- `crates/executor/src/query_executor/execution/query.rs` - Remove unsupported error, add routing
- `crates/executor/src/lib.rs` - Add information_schema module

---

## Implementation Order

1. **Create InformationSchemaProvider trait:**
```rust
pub trait InformationSchemaProvider {
    fn get_tables(&self) -> Result<RecordBatch>;
    fn get_columns(&self) -> Result<RecordBatch>;
    fn get_schemata(&self) -> Result<RecordBatch>;
}
```

2. **Implement for Executor:**
```rust
impl InformationSchemaProvider for Executor {
    fn get_tables(&self) -> Result<RecordBatch> {
        // Iterate self.datasets, collect table metadata
    }
}
```

3. **Route queries to provider:**
```rust
fn execute_scan(&self, plan: &ScanNode) -> Result<RecordBatch> {
    if plan.schema == "information_schema" {
        return self.handle_information_schema_query(&plan.table);
    }
    // Normal table scan
}
```

4. **Implement each table view:**
   - `tables` - List all tables
   - `columns` - List all columns with types
   - `schemata` - List all schemas/databases
   - `views` - List all views
   - `key_column_usage` - Primary key info
   - `table_constraints` - Constraint info

---

## Testing Pattern

```rust
#[test]
fn test_information_schema_tables() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE users (id INT64, name STRING)").unwrap();
    executor.execute_sql("CREATE TABLE orders (id INT64, user_id INT64)").unwrap();

    let result = executor.execute_sql(
        "SELECT table_name FROM information_schema.tables ORDER BY table_name"
    ).unwrap();

    assert_batch_eq!(result, [
        ["orders"],
        ["users"],
    ]);
}

#[test]
fn test_information_schema_columns() {
    let mut executor = create_executor();
    executor.execute_sql("CREATE TABLE users (id INT64, name STRING)").unwrap();

    let result = executor.execute_sql(
        "SELECT column_name, data_type FROM information_schema.columns
         WHERE table_name = 'users' ORDER BY ordinal_position"
    ).unwrap();

    assert_batch_eq!(result, [
        ["id", "INT64"],
        ["name", "STRING"],
    ]);
}
```

---

## ClickHouse-Specific Considerations

1. **Case Sensitivity:** ClickHouse is case-sensitive for identifiers
2. **Engine Info:** Include table engine in tables view
3. **Nullable:** Track nullable columns properly
4. **Default Expressions:** Store and return default column values
5. **Comments:** Support table/column comments

---

## Verification Steps

1. Run: `cargo test --test clickhouse -- queries::information_schema --ignored`
2. Implement information_schema.tables first
3. Add columns, schemata, views incrementally
4. Remove `#[ignore = "Implement me!"]` as each test passes