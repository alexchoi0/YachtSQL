use yachtsql_core::error::Result;
use yachtsql_storage::{
    ConstraintTiming, DMLOperation, DeferredFKCheck, DeferredFKState, Row, TableConstraintOps,
};

#[derive(Debug, Clone)]
pub struct ForeignKeyEnforcer {}

impl ForeignKeyEnforcer {
    pub fn new() -> Self {
        Self {}
    }

    pub fn validate_update(
        &self,
        table_name: &str,
        row: &Row,
        storage: &yachtsql_storage::Storage,
    ) -> Result<()> {
        let (dataset_id, table_id) = Self::parse_table_name(table_name);

        let (child_schema, foreign_keys) = {
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found for FK validation",
                    dataset_id
                ))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}.{}' not found for FK validation",
                    dataset_id, table_id
                ))
            })?;
            (table.schema().clone(), table.foreign_keys().to_vec())
        };

        for fk in foreign_keys {
            if !fk.is_enforced() {
                continue;
            }

            let fk_values = Self::extract_column_values(&child_schema, row, &fk.child_columns)?;

            if fk_values.is_empty() {
                continue;
            }

            let parent_exists =
                Self::parent_row_exists(&fk.parent_table, &fk.parent_columns, &fk_values, storage)?;

            if !parent_exists {
                return Err(yachtsql_core::error::Error::constraint_violation(format!(
                    "Foreign key constraint '{}' violated: no matching row in parent table '{}' for values {:?}",
                    fk.name.as_deref().unwrap_or("unnamed"),
                    fk.parent_table,
                    fk_values
                )));
            }
        }

        Ok(())
    }

    fn check_restrict_on_delete(
        &self,
        table_name: &str,
        deleted_row: &Row,
        storage: &yachtsql_storage::Storage,
    ) -> Result<()> {
        use yachtsql_storage::foreign_keys::ReferentialAction;

        let (dataset_id, table_id) = Self::parse_table_name(table_name);

        let parent_schema = {
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found for RESTRICT check",
                    dataset_id
                ))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}.{}' not found for RESTRICT check",
                    dataset_id, table_id
                ))
            })?;
            table.schema().clone()
        };

        let child_tables = Self::find_child_tables_with_fks(table_name, storage)?;

        for (child_dataset, child_table_name, fk) in child_tables {
            let should_restrict = matches!(
                fk.on_delete,
                ReferentialAction::Restrict | ReferentialAction::NoAction
            );

            if !should_restrict {
                continue;
            }

            let parent_key_values =
                Self::extract_column_values(&parent_schema, deleted_row, &fk.parent_columns)?;

            if parent_key_values.is_empty() {
                continue;
            }

            let has_child_references = Self::has_child_references(
                &child_dataset,
                &child_table_name,
                &fk,
                &parent_key_values,
                storage,
            )?;

            if has_child_references {
                return Err(yachtsql_core::error::Error::constraint_violation(format!(
                    "Foreign key constraint '{}' on table '{}.{}' violated: cannot delete row from '{}' because it is referenced by rows in child table",
                    fk.name.as_deref().unwrap_or("unnamed"),
                    child_dataset,
                    child_table_name,
                    table_name
                )));
            }
        }

        Ok(())
    }

    fn check_restrict_on_update(
        &self,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        storage: &yachtsql_storage::Storage,
    ) -> Result<()> {
        use yachtsql_storage::foreign_keys::ReferentialAction;

        let (dataset_id, table_id) = Self::parse_table_name(table_name);

        let parent_schema = {
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found for RESTRICT check",
                    dataset_id
                ))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}.{}' not found for RESTRICT check",
                    dataset_id, table_id
                ))
            })?;
            table.schema().clone()
        };

        let child_tables = Self::find_child_tables_with_fks(table_name, storage)?;

        for (child_dataset, child_table_name, fk) in child_tables {
            let should_restrict = matches!(
                fk.on_update,
                ReferentialAction::Restrict | ReferentialAction::NoAction
            );

            if !should_restrict {
                continue;
            }

            let old_parent_values =
                Self::extract_column_values(&parent_schema, old_row, &fk.parent_columns)?;
            let new_parent_values =
                Self::extract_column_values(&parent_schema, new_row, &fk.parent_columns)?;

            if old_parent_values == new_parent_values {
                continue;
            }

            if old_parent_values.is_empty() {
                continue;
            }

            let has_child_references = Self::has_child_references(
                &child_dataset,
                &child_table_name,
                &fk,
                &old_parent_values,
                storage,
            )?;

            if has_child_references {
                return Err(yachtsql_core::error::Error::constraint_violation(format!(
                    "Foreign key constraint '{}' on table '{}.{}' violated: cannot update row in '{}' because it is referenced by rows in child table",
                    fk.name.as_deref().unwrap_or("unnamed"),
                    child_dataset,
                    child_table_name,
                    table_name
                )));
            }
        }

        Ok(())
    }

    fn has_child_references(
        child_dataset: &str,
        child_table_name: &str,
        fk: &yachtsql_storage::foreign_keys::ForeignKey,
        parent_key_values: &[yachtsql_core::types::Value],
        storage: &yachtsql_storage::Storage,
    ) -> Result<bool> {
        let (child_schema, all_rows) = {
            let dataset = storage.get_dataset(child_dataset).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found",
                    child_dataset
                ))
            })?;
            let table = dataset.get_table(child_table_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}' not found",
                    child_table_name
                ))
            })?;
            (table.schema().clone(), table.get_all_rows())
        };

        for row in all_rows {
            let fk_values = Self::extract_column_values(&child_schema, &row, &fk.child_columns)?;
            if !fk_values.is_empty() && fk_values == parent_key_values {
                return Ok(true);
            }
        }

        Ok(false)
    }

    pub fn cascade_delete(
        &self,
        table_name: &str,
        deleted_row: &Row,
        storage: &mut yachtsql_storage::Storage,
    ) -> Result<()> {
        use yachtsql_storage::foreign_keys::ReferentialAction;

        self.check_restrict_on_delete(table_name, deleted_row, storage)?;

        let (dataset_id, table_id) = Self::parse_table_name(table_name);

        let parent_schema = {
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found for CASCADE DELETE",
                    dataset_id
                ))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}.{}' not found for CASCADE DELETE",
                    dataset_id, table_id
                ))
            })?;
            table.schema().clone()
        };

        let child_tables = Self::find_child_tables_with_fks(table_name, storage)?;

        for (child_dataset, child_table_name, fk) in child_tables {
            let parent_key_values =
                Self::extract_column_values(&parent_schema, deleted_row, &fk.parent_columns)?;
            if parent_key_values.is_empty() {
                continue;
            }

            match fk.on_delete {
                ReferentialAction::Cascade => {
                    Self::cascade_delete_children(
                        &child_dataset,
                        &child_table_name,
                        &fk,
                        &parent_key_values,
                        storage,
                        self,
                    )?;
                }
                ReferentialAction::SetNull => {
                    Self::set_null_in_children(
                        &child_dataset,
                        &child_table_name,
                        &fk,
                        &parent_key_values,
                        storage,
                    )?;
                }
                ReferentialAction::SetDefault => {
                    Self::set_default_in_children(
                        &child_dataset,
                        &child_table_name,
                        &fk,
                        &parent_key_values,
                        storage,
                    )?;
                }
                ReferentialAction::Restrict | ReferentialAction::NoAction => {}
            }
        }

        Ok(())
    }

    pub fn cascade_update(
        &self,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        storage: &mut yachtsql_storage::Storage,
    ) -> Result<()> {
        use yachtsql_storage::foreign_keys::ReferentialAction;

        self.check_restrict_on_update(table_name, old_row, new_row, storage)?;

        let (dataset_id, table_id) = Self::parse_table_name(table_name);

        let parent_schema = {
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found for CASCADE UPDATE",
                    dataset_id
                ))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}.{}' not found for CASCADE UPDATE",
                    dataset_id, table_id
                ))
            })?;
            table.schema().clone()
        };

        let child_tables = Self::find_child_tables_with_fks(table_name, storage)?;

        for (child_dataset, child_table_name, fk) in child_tables {
            let old_parent_values =
                Self::extract_column_values(&parent_schema, old_row, &fk.parent_columns)?;
            let new_parent_values =
                Self::extract_column_values(&parent_schema, new_row, &fk.parent_columns)?;

            if old_parent_values == new_parent_values {
                continue;
            }

            if old_parent_values.is_empty() {
                continue;
            }

            match fk.on_update {
                ReferentialAction::Cascade => {
                    Self::cascade_update_children(
                        &child_dataset,
                        &child_table_name,
                        &fk,
                        &old_parent_values,
                        &new_parent_values,
                        storage,
                        self,
                    )?;
                }
                ReferentialAction::SetNull => {
                    Self::set_null_in_children(
                        &child_dataset,
                        &child_table_name,
                        &fk,
                        &old_parent_values,
                        storage,
                    )?;
                }
                ReferentialAction::SetDefault => {
                    Self::set_default_in_children(
                        &child_dataset,
                        &child_table_name,
                        &fk,
                        &old_parent_values,
                        storage,
                    )?;
                }
                ReferentialAction::Restrict | ReferentialAction::NoAction => {}
            }
        }

        Ok(())
    }

    pub fn validate_insert(
        &self,
        table_name: &str,
        row: &Row,
        storage: &yachtsql_storage::Storage,
    ) -> Result<()> {
        self.validate_insert_with_deferral(table_name, row, storage, None)
            .map(|_| ())
    }

    pub fn validate_insert_with_deferral(
        &self,
        table_name: &str,
        row: &Row,
        storage: &yachtsql_storage::Storage,
        deferred_state: Option<&DeferredFKState>,
    ) -> Result<Vec<DeferredFKCheck>> {
        let (dataset_id, table_id) = Self::parse_table_name(table_name);

        let (child_schema, foreign_keys) = {
            let dataset = storage.get_dataset(&dataset_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found for FK validation",
                    dataset_id
                ))
            })?;
            let table = dataset.get_table(&table_id).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}.{}' not found for FK validation",
                    dataset_id, table_id
                ))
            })?;
            (table.schema().clone(), table.foreign_keys().to_vec())
        };

        let mut deferred_checks = Vec::new();

        for fk in foreign_keys {
            if !fk.is_enforced() {
                continue;
            }

            let fk_values = Self::extract_column_values(&child_schema, row, &fk.child_columns)?;

            if fk_values.is_empty() {
                continue;
            }

            let constraint_name = fk.constraint_name();
            let should_defer = if let Some(state) = deferred_state {
                fk.is_deferrable()
                    && state.get_timing(&constraint_name, fk.initial_timing())
                        == ConstraintTiming::Deferred
            } else {
                false
            };

            if should_defer {
                deferred_checks.push(DeferredFKCheck {
                    constraint_name,
                    child_table: table_name.to_string(),
                    child_columns: fk.child_columns.clone(),
                    parent_table: fk.parent_table.clone(),
                    parent_columns: fk.parent_columns.clone(),
                    fk_values,
                    operation: DMLOperation::Insert,
                });
            } else {
                let parent_exists = Self::parent_row_exists(
                    &fk.parent_table,
                    &fk.parent_columns,
                    &fk_values,
                    storage,
                )?;

                if !parent_exists {
                    return Err(yachtsql_core::error::Error::constraint_violation(format!(
                        "Foreign key constraint '{}' violated: no matching row in parent table '{}' for values {:?}",
                        fk.name.as_deref().unwrap_or("unnamed"),
                        fk.parent_table,
                        fk_values
                    )));
                }
            }
        }

        Ok(deferred_checks)
    }

    fn parse_table_name(table_name: &str) -> (String, String) {
        if let Some((dataset, table)) = table_name.split_once('.') {
            (dataset.to_string(), table.to_string())
        } else {
            ("default".to_string(), table_name.to_string())
        }
    }

    fn find_child_tables_with_fks(
        parent_table_name: &str,
        storage: &yachtsql_storage::Storage,
    ) -> Result<Vec<(String, String, yachtsql_storage::foreign_keys::ForeignKey)>> {
        let (parent_dataset, parent_table) = Self::parse_table_name(parent_table_name);
        let mut child_tables = Vec::new();

        for dataset_name in storage.list_datasets() {
            let dataset = storage.get_dataset(dataset_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found while searching for FK references",
                    dataset_name
                ))
            })?;

            for table_name in dataset.list_tables() {
                let table = dataset.get_table(table_name).ok_or_else(|| {
                    yachtsql_core::error::Error::InvalidOperation(format!(
                        "Table '{}' not found while searching for FK references",
                        table_name
                    ))
                })?;

                for fk in table.foreign_keys() {
                    if !fk.is_enforced() {
                        continue;
                    }

                    let (fk_parent_dataset, fk_parent_table) =
                        Self::parse_table_name(&fk.parent_table);

                    let fk_parent_dataset =
                        if fk_parent_dataset.is_empty() || fk_parent_dataset == "default" {
                            dataset_name
                        } else {
                            &fk_parent_dataset
                        };

                    if *fk_parent_dataset == parent_dataset && fk_parent_table == parent_table {
                        child_tables.push((
                            dataset_name.to_string(),
                            table_name.to_string(),
                            fk.clone(),
                        ));
                    }
                }
            }
        }

        Ok(child_tables)
    }

    fn extract_column_values(
        schema: &yachtsql_storage::Schema,
        row: &Row,
        columns: &[String],
    ) -> Result<Vec<yachtsql_core::types::Value>> {
        let mut values = Vec::new();

        for col in columns {
            let value = row.get_by_name(schema, col).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Column '{}' not found in row",
                    col
                ))
            })?;

            if value.is_null() {
                return Ok(Vec::new());
            }

            values.push(value.clone());
        }

        Ok(values)
    }

    fn cascade_delete_children(
        child_dataset: &str,
        child_table_name: &str,
        fk: &yachtsql_storage::foreign_keys::ForeignKey,
        parent_key_values: &[yachtsql_core::types::Value],
        storage: &mut yachtsql_storage::Storage,
        enforcer: &ForeignKeyEnforcer,
    ) -> Result<()> {
        let (child_schema, all_rows) = {
            let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found",
                    child_dataset
                ))
            })?;
            let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}' not found",
                    child_table_name
                ))
            })?;
            (table.schema().clone(), table.get_all_rows())
        };

        let mut rows_to_delete = Vec::new();
        let mut rows_to_keep = Vec::new();

        for row in all_rows {
            let fk_values = Self::extract_column_values(&child_schema, &row, &fk.child_columns)?;
            if !fk_values.is_empty() && fk_values == parent_key_values {
                rows_to_delete.push(row);
            } else {
                rows_to_keep.push(row);
            }
        }

        let child_table_full_name = format!("{}.{}", child_dataset, child_table_name);
        for row in &rows_to_delete {
            enforcer.cascade_delete(&child_table_full_name, row, storage)?;
        }

        let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Dataset '{}' not found during CASCADE DELETE",
                child_dataset
            ))
        })?;
        let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Table '{}' not found during CASCADE DELETE",
                child_table_name
            ))
        })?;
        table.clear_rows()?;
        for row in rows_to_keep {
            table.insert_row(row)?;
        }

        Ok(())
    }

    fn set_null_in_children(
        child_dataset: &str,
        child_table_name: &str,
        fk: &yachtsql_storage::foreign_keys::ForeignKey,
        parent_key_values: &[yachtsql_core::types::Value],
        storage: &mut yachtsql_storage::Storage,
    ) -> Result<()> {
        let (child_schema, all_rows) = {
            let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found",
                    child_dataset
                ))
            })?;
            let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}' not found",
                    child_table_name
                ))
            })?;
            (table.schema().clone(), table.get_all_rows())
        };

        let mut updated_rows = Vec::new();

        for row in all_rows {
            let fk_values = Self::extract_column_values(&child_schema, &row, &fk.child_columns)?;
            if !fk_values.is_empty() && fk_values == parent_key_values {
                let mut new_values = row.values().to_vec();
                for col_name in &fk.child_columns {
                    if let Some(col_idx) = child_schema
                        .fields()
                        .iter()
                        .position(|f| f.name == *col_name)
                    {
                        new_values[col_idx] = yachtsql_core::types::Value::null();
                    }
                }
                updated_rows.push(Row::from_values(new_values));
            } else {
                updated_rows.push(row);
            }
        }

        let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Dataset '{}' not found during SET NULL",
                child_dataset
            ))
        })?;
        let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Table '{}' not found during SET NULL",
                child_table_name
            ))
        })?;
        table.clear_rows()?;
        for row in updated_rows {
            table.insert_row(row)?;
        }

        Ok(())
    }

    fn set_default_in_children(
        child_dataset: &str,
        child_table_name: &str,
        fk: &yachtsql_storage::foreign_keys::ForeignKey,
        parent_key_values: &[yachtsql_core::types::Value],
        storage: &mut yachtsql_storage::Storage,
    ) -> Result<()> {
        let (child_schema, all_rows) = {
            let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found",
                    child_dataset
                ))
            })?;
            let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}' not found",
                    child_table_name
                ))
            })?;
            (table.schema().clone(), table.get_all_rows())
        };

        let mut updated_rows = Vec::new();

        for row in all_rows {
            let fk_values = Self::extract_column_values(&child_schema, &row, &fk.child_columns)?;
            if !fk_values.is_empty() && fk_values == parent_key_values {
                let mut new_values = row.values().to_vec();
                for col_name in &fk.child_columns {
                    if let Some(col_idx) = child_schema
                        .fields()
                        .iter()
                        .position(|f| f.name == *col_name)
                    {
                        if let Some(default) = &child_schema.fields()[col_idx].default_value {
                            new_values[col_idx] = match default {
                                yachtsql_storage::DefaultValue::Literal(val) => val.clone(),
                                yachtsql_storage::DefaultValue::CurrentTimestamp => {
                                    yachtsql_core::types::Value::datetime(chrono::Utc::now())
                                }
                                yachtsql_storage::DefaultValue::CurrentDate => {
                                    yachtsql_core::types::Value::date(
                                        chrono::Utc::now().date_naive(),
                                    )
                                }
                                yachtsql_storage::DefaultValue::GenRandomUuid => {
                                    yachtsql_core::types::Value::uuid(uuid::Uuid::new_v4())
                                }
                            };
                        } else {
                            new_values[col_idx] = yachtsql_core::types::Value::null();
                        }
                    }
                }
                updated_rows.push(Row::from_values(new_values));
            } else {
                updated_rows.push(row);
            }
        }

        let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Dataset '{}' not found during SET DEFAULT",
                child_dataset
            ))
        })?;
        let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Table '{}' not found during SET DEFAULT",
                child_table_name
            ))
        })?;
        table.clear_rows()?;
        for row in updated_rows {
            table.insert_row(row)?;
        }

        Ok(())
    }

    fn parent_row_exists(
        parent_table_name: &str,
        parent_columns: &[String],
        fk_values: &[yachtsql_core::types::Value],
        storage: &yachtsql_storage::Storage,
    ) -> Result<bool> {
        let (parent_dataset, parent_table) = Self::parse_table_name(parent_table_name);

        let dataset = storage.get_dataset(&parent_dataset).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Parent dataset '{}' not found",
                parent_dataset
            ))
        })?;

        let table = dataset.get_table(&parent_table).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Parent table '{}' not found",
                parent_table_name
            ))
        })?;

        let parent_schema = table.schema().clone();

        for row in table.get_all_rows() {
            let row_values = Self::extract_column_values(&parent_schema, &row, parent_columns)?;
            if !row_values.is_empty() && row_values == fk_values {
                return Ok(true);
            }
        }

        Ok(false)
    }

    fn cascade_update_children(
        child_dataset: &str,
        child_table_name: &str,
        fk: &yachtsql_storage::foreign_keys::ForeignKey,
        old_parent_values: &[yachtsql_core::types::Value],
        new_parent_values: &[yachtsql_core::types::Value],
        storage: &mut yachtsql_storage::Storage,
        enforcer: &ForeignKeyEnforcer,
    ) -> Result<()> {
        let (child_schema, all_rows) = {
            let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Dataset '{}' not found",
                    child_dataset
                ))
            })?;
            let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
                yachtsql_core::error::Error::InvalidOperation(format!(
                    "Table '{}' not found",
                    child_table_name
                ))
            })?;
            (table.schema().clone(), table.get_all_rows())
        };

        let mut updated_rows = Vec::new();

        for row in all_rows {
            let fk_values = Self::extract_column_values(&child_schema, &row, &fk.child_columns)?;
            if !fk_values.is_empty() && fk_values == old_parent_values {
                let old_child_row = row.clone();

                let mut new_values = row.values().to_vec();
                for (idx, col_name) in fk.child_columns.iter().enumerate() {
                    if let Some(col_idx) = child_schema
                        .fields()
                        .iter()
                        .position(|f| f.name == *col_name)
                    {
                        new_values[col_idx] = new_parent_values[idx].clone();
                    }
                }
                let new_child_row = Row::from_values(new_values);

                let child_table_full_name = format!("{}.{}", child_dataset, child_table_name);
                enforcer.cascade_update(
                    &child_table_full_name,
                    &old_child_row,
                    &new_child_row,
                    storage,
                )?;

                updated_rows.push(new_child_row);
            } else {
                updated_rows.push(row);
            }
        }

        let dataset = storage.get_dataset_mut(child_dataset).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Dataset '{}' not found during CASCADE UPDATE",
                child_dataset
            ))
        })?;
        let table = dataset.get_table_mut(child_table_name).ok_or_else(|| {
            yachtsql_core::error::Error::InvalidOperation(format!(
                "Table '{}' not found during CASCADE UPDATE",
                child_table_name
            ))
        })?;
        table.clear_rows()?;
        for row in updated_rows {
            table.insert_row(row)?;
        }

        Ok(())
    }
}

impl Default for ForeignKeyEnforcer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_foreign_key_enforcer_creation() {
        let enforcer = ForeignKeyEnforcer::new();
        let _ = &enforcer;
    }

    #[test]
    fn test_cascade_delete_no_op() {
        let enforcer = ForeignKeyEnforcer::new();
        let mut storage = yachtsql_storage::Storage::new();

        storage.create_dataset("default".to_string()).unwrap();
        let schema =
            yachtsql_storage::Schema::from_fields(vec![yachtsql_storage::Field::required(
                "id".to_string(),
                yachtsql_core::types::DataType::Int64,
            )]);
        storage
            .get_dataset_mut("default")
            .unwrap()
            .create_table("test_table".to_string(), schema)
            .unwrap();

        let row = Row::empty();
        let result = enforcer.cascade_delete("test_table", &row, &mut storage);
        assert!(result.is_ok());
    }
}
