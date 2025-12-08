use sqlparser::ast::{AlterTableOperation, Statement as SqlStatement};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_storage::{CheckConstraint, Field, ForeignKey, TableConstraintOps, TableSchemaOps};

use super::super::QueryExecutor;
use super::create::DdlExecutor;

pub trait AlterTableExecutor {
    fn execute_alter_table(&mut self, stmt: &SqlStatement) -> Result<()>;
}

impl AlterTableExecutor for QueryExecutor {
    fn execute_alter_table(&mut self, stmt: &SqlStatement) -> Result<()> {
        let SqlStatement::AlterTable {
            name, operations, ..
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not an ALTER TABLE statement".to_string(),
            ));
        };

        let table_name = name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name)?;

        let mut storage = self.storage.borrow_mut();

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        for operation in operations {
            match operation {
                AlterTableOperation::RenameTable { table_name } => {
                    use sqlparser::ast::RenameTableNameKind;
                    let new_table_name = match table_name {
                        RenameTableNameKind::As(name) | RenameTableNameKind::To(name) => {
                            name.to_string()
                        }
                    };
                    let (new_dataset_id, new_table_id) =
                        self.parse_ddl_table_name(&new_table_name)?;

                    if new_dataset_id != dataset_id {
                        return Err(Error::invalid_query(format!(
                            "Cannot rename table to different dataset: {} -> {}",
                            dataset_id, new_dataset_id
                        )));
                    }

                    if dataset.get_table(&new_table_id).is_some() {
                        return Err(Error::invalid_query(format!(
                            "Table '{}' already exists",
                            new_table_id
                        )));
                    }

                    dataset.rename_table(&table_id, &new_table_id)?;

                    drop(storage);
                    self.plan_cache.borrow_mut().invalidate_all();

                    return Ok(());
                }
                AlterTableOperation::DropColumn { column_names, .. } => {
                    for col_ident in column_names {
                        let col_name = col_ident.value.clone();

                        let is_referenced = dataset.tables().values().any(|other_table| {
                            other_table.foreign_keys().iter().any(|fk| {
                                fk.parent_table == table_id && fk.parent_columns.contains(&col_name)
                            })
                        });

                        if is_referenced {
                            return Err(Error::invalid_query(format!(
                                "Cannot drop column '{}' because it is referenced by a FOREIGN KEY constraint",
                                col_name
                            )));
                        }
                    }
                }
                _ => {}
            }
        }

        let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
            Error::TableNotFound(format!("Table '{}.{}' not found", dataset_id, table_id))
        })?;

        for operation in operations {
            match operation {
                AlterTableOperation::AddColumn {
                    column_def,
                    if_not_exists,
                    ..
                } => {
                    let col_name = column_def.name.value.clone();
                    if table.schema().field_index(&col_name).is_some() {
                        if *if_not_exists {
                            continue;
                        }
                        return Err(Error::invalid_query(format!(
                            "Column '{}' already exists",
                            col_name
                        )));
                    }

                    let data_type =
                        self.sql_type_to_data_type(&dataset_id, &column_def.data_type)?;

                    let mut is_not_null = false;
                    let mut default_value: Option<Value> = None;
                    let mut is_auto_increment = false;

                    for option in &column_def.options {
                        match &option.option {
                            sqlparser::ast::ColumnOption::NotNull => is_not_null = true,
                            sqlparser::ast::ColumnOption::Null => is_not_null = false,
                            sqlparser::ast::ColumnOption::Default(expr) => {
                                default_value = Some(self.evaluate_default_expr(expr)?);
                            }
                            sqlparser::ast::ColumnOption::Unique { is_primary, .. } => {
                                if *is_primary {
                                    is_not_null = true;
                                }
                            }
                            sqlparser::ast::ColumnOption::DialectSpecific(tokens) => {
                                let token_str: String = tokens
                                    .iter()
                                    .map(|t| t.to_string().to_uppercase())
                                    .collect::<Vec<_>>()
                                    .join(" ");
                                if token_str.contains("AUTO_INCREMENT")
                                    || token_str.contains("IDENTITY")
                                {
                                    is_auto_increment = true;
                                }
                            }
                            _ => {}
                        }
                    }

                    let mut field = if is_not_null {
                        Field::required(&col_name, data_type)
                    } else {
                        Field::nullable(&col_name, data_type)
                    };

                    if is_auto_increment {
                        field = field.with_auto_increment();
                    }

                    table.add_column(field, default_value)?;
                }

                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    for col_ident in column_names {
                        let col_name = col_ident.value.clone();
                        if table.schema().field_index(&col_name).is_none() {
                            if *if_exists {
                                continue;
                            }
                            return Err(Error::invalid_query(format!(
                                "Column '{}' does not exist",
                                col_name
                            )));
                        }

                        if let Some(pk_cols) = table.schema().primary_key() {
                            if pk_cols.contains(&col_name) {
                                return Err(Error::invalid_query(format!(
                                    "Cannot drop column '{}' because it is part of the PRIMARY KEY constraint",
                                    col_name
                                )));
                            }
                        }

                        table.drop_column(&col_name)?;
                    }
                }

                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let old_name = old_column_name.value.clone();
                    let new_name = new_column_name.value.clone();
                    table.rename_column(&old_name, &new_name)?;
                }

                AlterTableOperation::AlterColumn { column_name, op } => {
                    let col_name = column_name.value.clone();

                    if table.schema().field_index(&col_name).is_none() {
                        return Err(Error::invalid_query(format!(
                            "Column '{}' does not exist",
                            col_name
                        )));
                    }

                    match op {
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            table.alter_column(&col_name, None, Some(true), None, false)?;
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            table.alter_column(&col_name, None, Some(false), None, false)?;
                        }
                        sqlparser::ast::AlterColumnOperation::SetDefault { value } => {
                            let default_val = self.evaluate_default_expr(value)?;
                            table.alter_column(&col_name, None, None, Some(default_val), false)?;
                        }
                        sqlparser::ast::AlterColumnOperation::DropDefault => {
                            table.alter_column(&col_name, None, None, None, true)?;
                        }
                        sqlparser::ast::AlterColumnOperation::SetDataType {
                            data_type,
                            using: _,
                            had_set: _,
                        } => {
                            let new_type = self.sql_type_to_data_type(&dataset_id, data_type)?;
                            table.alter_column(&col_name, Some(new_type), None, None, false)?;
                        }
                        _ => {
                            return Err(Error::unsupported_feature(format!(
                                "ALTER COLUMN operation {:?} not yet supported",
                                op
                            )));
                        }
                    }
                }

                AlterTableOperation::RenameTable { .. } => {
                    unreachable!("RENAME TABLE should be handled in pre-processing");
                }

                AlterTableOperation::AddConstraint {
                    constraint,
                    not_valid,
                } => {
                    use sqlparser::ast::TableConstraint;
                    let skip_validation = *not_valid;
                    match constraint {
                        TableConstraint::Unique { columns, .. } => {
                            let col_names: Vec<String> =
                                columns.iter().map(|c| c.column.expr.to_string()).collect();

                            if !skip_validation {
                                self.validate_unique_constraint(table, &col_names)?;
                            }

                            table.schema_mut().add_unique_constraint(col_names);
                        }
                        TableConstraint::PrimaryKey { columns, .. } => {
                            let col_names: Vec<String> =
                                columns.iter().map(|c| c.column.expr.to_string()).collect();

                            if !skip_validation {
                                self.validate_primary_key_constraint(table, &col_names)?;
                            }

                            table.schema_mut().set_primary_key(col_names);
                        }
                        TableConstraint::Check {
                            expr,
                            name,
                            enforced,
                        } => {
                            let constraint_name = name.as_ref().map(|n| n.to_string());
                            let expr_str = expr.to_string();
                            let is_enforced = enforced.unwrap_or(true);

                            if is_enforced && !skip_validation {
                                self.validate_check_constraint(table, &expr_str)?;
                            }

                            table.schema_mut().add_check_constraint_with_validity(
                                CheckConstraint {
                                    name: constraint_name,
                                    expression: expr_str,
                                    enforced: is_enforced,
                                },
                                !skip_validation,
                            );
                        }
                        TableConstraint::ForeignKey {
                            name,
                            columns,
                            foreign_table,
                            referred_columns,
                            on_delete,
                            on_update,
                            characteristics,
                            ..
                        } => {
                            let child_columns: Vec<String> =
                                columns.iter().map(|c| c.value.clone()).collect();

                            let parent_table = foreign_table.to_string();

                            let parent_columns: Vec<String> =
                                referred_columns.iter().map(|c| c.value.clone()).collect();

                            let mut foreign_key =
                                ForeignKey::new(child_columns, parent_table, parent_columns);

                            if let Some(fk_name) = name {
                                foreign_key = foreign_key.with_name(fk_name.to_string());
                            }

                            if let Some(action) = on_delete {
                                foreign_key = foreign_key
                                    .with_on_delete(Self::map_referential_action(action)?);
                            }

                            if let Some(action) = on_update {
                                foreign_key = foreign_key
                                    .with_on_update(Self::map_referential_action(action)?);
                            }

                            if let Some(chars) = characteristics {
                                if let Some(enforced) = chars.enforced {
                                    foreign_key = foreign_key.with_enforced(enforced);
                                }
                            }

                            table.schema_mut().add_foreign_key_with_validity(
                                foreign_key.clone(),
                                !skip_validation,
                            );
                            table.add_foreign_key(foreign_key)?;
                        }
                        _ => {
                            return Err(Error::unsupported_feature(format!(
                                "ADD CONSTRAINT {:?} not yet supported",
                                constraint
                            )));
                        }
                    }
                }

                AlterTableOperation::DropConstraint {
                    name, if_exists, ..
                } => {
                    let constraint_name = name.to_string();

                    let removed_check = table
                        .schema_mut()
                        .remove_check_constraint_by_name(&constraint_name);

                    let removed_fk = table.remove_foreign_key(&constraint_name).is_ok();

                    if removed_fk {
                        table
                            .schema_mut()
                            .remove_foreign_key_by_name(&constraint_name);
                    }

                    if !removed_check && !removed_fk && !if_exists {
                        return Err(Error::invalid_query(format!(
                            "Constraint '{}' does not exist",
                            constraint_name
                        )));
                    }
                }

                AlterTableOperation::DropPrimaryKey { .. } => {
                    table.schema_mut().drop_primary_key()?;
                }

                AlterTableOperation::RenameConstraint { old_name, new_name } => {
                    let old_constraint_name = old_name.to_string();
                    let new_constraint_name = new_name.to_string();

                    if !table
                        .schema_mut()
                        .rename_constraint(&old_constraint_name, &new_constraint_name)
                    {
                        return Err(Error::invalid_query(format!(
                            "Constraint '{}' does not exist",
                            old_constraint_name
                        )));
                    }
                }

                AlterTableOperation::ValidateConstraint { name } => {
                    use yachtsql_storage::schema::ConstraintTypeTag;

                    let constraint_name = name.to_string();

                    let constraint_metadata = table
                        .schema()
                        .get_constraint_metadata(&constraint_name)
                        .ok_or_else(|| {
                            Error::invalid_query(format!(
                                "Constraint '{}' does not exist",
                                constraint_name
                            ))
                        })?
                        .clone();

                    match constraint_metadata.constraint_type {
                        ConstraintTypeTag::Check => {
                            self.validate_check_constraint(table, &constraint_metadata.definition)?;
                        }
                        ConstraintTypeTag::ForeignKey
                        | ConstraintTypeTag::Unique
                        | ConstraintTypeTag::PrimaryKey => {}
                    }

                    table.schema_mut().set_constraint_valid(&constraint_name);
                }

                AlterTableOperation::ModifyColumn {
                    col_name,
                    data_type,
                    options,
                    ..
                } => {
                    let column_name = col_name.value.clone();

                    if table.schema().field_index(&column_name).is_none() {
                        return Err(Error::invalid_query(format!(
                            "Column '{}' does not exist",
                            column_name
                        )));
                    }

                    let new_type = self.sql_type_to_data_type(&dataset_id, data_type)?;

                    let mut set_not_null: Option<bool> = None;
                    for option in options {
                        match option {
                            sqlparser::ast::ColumnOption::NotNull => set_not_null = Some(true),
                            sqlparser::ast::ColumnOption::Null => set_not_null = Some(false),
                            _ => {}
                        }
                    }

                    table.alter_column(&column_name, Some(new_type), set_not_null, None, false)?;
                }

                _ => {
                    return Err(Error::unsupported_feature(format!(
                        "ALTER TABLE operation {:?} not yet supported",
                        operation
                    )));
                }
            }
        }

        drop(storage);
        self.plan_cache.borrow_mut().invalidate_all();

        Ok(())
    }
}

impl QueryExecutor {
    fn validate_primary_key_constraint(
        &self,
        table: &yachtsql_storage::Table,
        col_names: &[String],
    ) -> Result<()> {
        use std::collections::HashSet;

        use yachtsql_storage::storage_backend::TableStorage;

        let row_count = table.row_count();
        if row_count == 0 {
            return Ok(());
        }

        let col_indices: Vec<usize> = col_names
            .iter()
            .map(|name| {
                table
                    .schema()
                    .field_index(name)
                    .ok_or_else(|| Error::invalid_query(format!("Column '{}' not found", name)))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut seen_values: HashSet<Vec<String>> = HashSet::new();

        for row_idx in 0..row_count {
            let row = table.get_row(row_idx)?;

            let pk_values: Vec<String> = col_indices
                .iter()
                .map(|&col_idx| {
                    let value = &row.values()[col_idx];

                    if value.is_null() {
                        return Err(Error::invalid_query(format!(
                            "Cannot add PRIMARY KEY constraint: column '{}' contains NULL values",
                            col_names[col_indices.iter().position(|&i| i == col_idx).unwrap()]
                        )));
                    }
                    Ok(value.to_string())
                })
                .collect::<Result<Vec<_>>>()?;

            if !seen_values.insert(pk_values.clone()) {
                return Err(Error::invalid_query(format!(
                    "Cannot add PRIMARY KEY constraint: duplicate values found in column(s) {:?}",
                    col_names
                )));
            }
        }

        Ok(())
    }

    fn validate_unique_constraint(
        &self,
        table: &yachtsql_storage::Table,
        col_names: &[String],
    ) -> Result<()> {
        use std::collections::HashSet;

        use yachtsql_storage::storage_backend::TableStorage;

        let row_count = table.row_count();
        if row_count == 0 {
            return Ok(());
        }

        let col_indices: Vec<usize> = col_names
            .iter()
            .map(|name| {
                table
                    .schema()
                    .field_index(name)
                    .ok_or_else(|| Error::invalid_query(format!("Column '{}' not found", name)))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut seen_values: HashSet<Vec<String>> = HashSet::new();

        for row_idx in 0..row_count {
            let row = table.get_row(row_idx)?;

            let unique_values: Vec<String> = col_indices
                .iter()
                .map(|&col_idx| {
                    let value = &row.values()[col_idx];

                    value.to_string()
                })
                .collect();

            let has_null = col_indices
                .iter()
                .any(|&col_idx| row.values()[col_idx].is_null());
            if has_null {
                continue;
            }

            if !seen_values.insert(unique_values.clone()) {
                return Err(Error::invalid_query(format!(
                    "Cannot add UNIQUE constraint: duplicate values found in column(s) {:?}",
                    col_names
                )));
            }
        }

        Ok(())
    }

    fn validate_check_constraint(
        &self,
        table: &yachtsql_storage::Table,
        expr_str: &str,
    ) -> Result<()> {
        use yachtsql_storage::storage_backend::TableStorage;

        let row_count = table.row_count();
        if row_count == 0 {
            return Ok(());
        }

        let check_expr = format!(
            "SELECT {} AS check_result FROM dummy WHERE {}",
            expr_str, expr_str
        );

        for field in table.schema().fields() {
            if expr_str.contains(&field.name) {}
        }

        if row_count > 0 {
            return Err(Error::invalid_query(
                "Cannot add CHECK constraint: existing data validation not yet fully implemented. \
                 For now, CHECK constraints can only be added to empty tables."
                    .to_string(),
            ));
        }

        Ok(())
    }

    fn evaluate_default_expr(&self, expr: &sqlparser::ast::Expr) -> Result<Value> {
        use sqlparser::ast::{Expr, Value as SqlValue};

        match expr {
            Expr::Value(val_with_span) => match &val_with_span.value {
                SqlValue::Number(n, _) => {
                    if let Ok(i) = n.parse::<i64>() {
                        Ok(Value::int64(i))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Value::float64(f))
                    } else {
                        Err(Error::invalid_query(format!("Cannot parse number: {}", n)))
                    }
                }
                SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                    Ok(Value::string(s.clone()))
                }
                SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
                SqlValue::Null => Ok(Value::null()),
                _ => Err(Error::unsupported_feature(format!(
                    "Default value expression {:?} not supported",
                    val_with_span
                ))),
            },
            Expr::UnaryOp { op, expr } => {
                use sqlparser::ast::UnaryOperator;
                if matches!(op, UnaryOperator::Minus) {
                    let inner = self.evaluate_default_expr(expr)?;
                    match inner.as_i64() {
                        Some(i) => Ok(Value::int64(-i)),
                        None => match inner.as_f64() {
                            Some(f) => Ok(Value::float64(-f)),
                            None => Err(Error::invalid_query(
                                "Unary minus only valid for numeric types".to_string(),
                            )),
                        },
                    }
                } else {
                    Err(Error::unsupported_feature(format!(
                        "Unary operator {:?} not supported in default values",
                        op
                    )))
                }
            }
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                match func_name.as_str() {
                    "CURRENT_TIMESTAMP" | "NOW" => {
                        use chrono::Utc;
                        Ok(Value::timestamp(Utc::now()))
                    }
                    "CURRENT_DATE" => {
                        use chrono::Utc;
                        Ok(Value::date(Utc::now().date_naive()))
                    }
                    _ => Err(Error::unsupported_feature(format!(
                        "Function {} not supported in default values",
                        func_name
                    ))),
                }
            }
            _ => Err(Error::unsupported_feature(format!(
                "Default value expression {:?} not supported",
                expr
            ))),
        }
    }
}
