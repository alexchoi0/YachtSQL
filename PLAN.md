# Work Stream 7: Constraint Enforcement on UPDATE

## Overview
Implement constraint checking during UPDATE and enhance DELETE constraint enforcement.

## Tests to Enable
**File:** `tests/postgresql/ddl/constraints.rs`
- `test_check_constraint_enforcement_on_update`
- `test_unique_enforcement_on_update`
- `test_foreign_key_violation_update`
- `test_foreign_key_on_delete_restrict`
- `test_foreign_key_on_delete_no_action`
- `test_foreign_key_on_update_restrict`

## Constraints to Enforce

### CHECK Constraint on UPDATE
```sql
CREATE TABLE t (id INT, age INT CHECK (age >= 0));
INSERT INTO t VALUES (1, 25);
UPDATE t SET age = -5 WHERE id = 1;  -- ERROR: violates check constraint
```
- Evaluate CHECK expression against new row values
- Error if constraint violated

### UNIQUE Constraint on UPDATE
```sql
CREATE TABLE t (id INT, email TEXT UNIQUE);
INSERT INTO t VALUES (1, 'a@b.com'), (2, 'c@d.com');
UPDATE t SET email = 'a@b.com' WHERE id = 2;  -- ERROR: duplicate key
```
- Check if new value conflicts with existing rows
- Exclude the row being updated from check

### Foreign Key on UPDATE (child table)
```sql
CREATE TABLE parent (id INT PRIMARY KEY);
CREATE TABLE child (id INT, pid INT REFERENCES parent(id));
INSERT INTO parent VALUES (1);
INSERT INTO child VALUES (1, 1);
UPDATE child SET pid = 999 WHERE id = 1;  -- ERROR: FK violation
```
- Verify new foreign key value exists in parent table

### ON DELETE RESTRICT
```sql
CREATE TABLE child (pid INT REFERENCES parent(id) ON DELETE RESTRICT);
DELETE FROM parent WHERE id = 1;  -- ERROR if child rows exist
```
- Prevent delete if any child rows reference this row
- Check immediately (not at statement end)

### ON DELETE NO ACTION
```sql
CREATE TABLE child (pid INT REFERENCES parent(id) ON DELETE NO ACTION);
DELETE FROM parent WHERE id = 1;  -- ERROR if child rows exist
```
- Same as RESTRICT but checked at statement end
- Allows other triggers/rules to fix the reference first

### ON UPDATE RESTRICT
```sql
CREATE TABLE child (pid INT REFERENCES parent(id) ON UPDATE RESTRICT);
UPDATE parent SET id = 2 WHERE id = 1;  -- ERROR if child rows exist
```
- Prevent update of referenced column if child rows exist

## Key Files to Modify

### UPDATE Execution
- `crates/executor/src/query_executor/execution/dml/update.rs`
- Before updating each row:
  1. Evaluate CHECK constraints on new values
  2. Check UNIQUE constraints (excluding current row)
  3. Verify FK references exist in parent tables

### DELETE Execution
- `crates/executor/src/query_executor/execution/dml/delete.rs`
- Before deleting each row:
  1. Check for RESTRICT foreign keys referencing this row
  2. For NO ACTION, defer check to statement end

### Constraint Checking Utilities
- `crates/storage/src/constraints.rs` or new module
- `check_constraint(table, row, constraint) -> Result<()>`
- `check_unique(table, row, columns, exclude_row) -> Result<()>`
- `check_foreign_key(child_table, row, fk) -> Result<()>`
- `check_references(parent_table, row, pk_columns) -> Result<()>`

## Implementation Notes

1. **CHECK on UPDATE**
   - Get constraint expression
   - Bind new row values to expression variables
   - Evaluate expression, error if false

2. **UNIQUE on UPDATE**
   - Query for existing rows with same values
   - Exclude the row being updated (by primary key or row ID)
   - Error if any matches found

3. **FK on UPDATE (child)**
   - Get parent table and referenced columns
   - Query parent table for matching row
   - Error if not found

4. **RESTRICT vs NO ACTION**
   - RESTRICT: check immediately per-row
   - NO ACTION: collect checks, verify at statement end
   - In practice, often behave the same

5. **Performance considerations**
   - May want to batch constraint checks
   - Index lookups for FK and UNIQUE checks

## Test Commands
```bash
cargo test --test postgresql test_check_constraint_enforcement_on_update
cargo test --test postgresql test_unique_enforcement_on_update
cargo test --test postgresql test_foreign_key_violation_update
cargo test --test postgresql test_foreign_key_on_delete_restrict
cargo test --test postgresql test_foreign_key_on_delete_no_action
cargo test --test postgresql test_foreign_key_on_update_restrict
```

## Dependencies
None - extends existing constraint checking (INSERT already has some)

## Coordination
- Work Stream 6 (DROP CONSTRAINT) adds `is_valid` flag - check it before enforcing
- Work Stream 8 (Deferred Constraints) needs these checks to defer
- Share constraint checking utilities
