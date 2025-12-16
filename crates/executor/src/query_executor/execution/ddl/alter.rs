use sqlparser::ast::{AlterTableOperation, Statement as SqlStatement};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_storage::{CheckConstraint, Field, TableConstraintOps, TableSchemaOps};

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

        if dataset.views().exists(&table_id) {
            drop(storage);
            self.plan_cache.borrow_mut().invalidate_all();
            return Ok(());
        }

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
                        return Err(Error::unsupported_feature(
                            "Cross-dataset table rename is not supported".to_string(),
                        ));
                    }

                    dataset.rename_table(&table_id, &new_table_id)?;
                }

                AlterTableOperation::RenameColumn {
                    old_column_name,
                    new_column_name,
                } => {
                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

                    let old_name = old_column_name.value.clone();
                    let new_name = new_column_name.value.clone();

                    table.rename_column(&old_name, &new_name)?;
                }

                AlterTableOperation::AddColumn { column_def, .. } => {
                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

                    let col_name = column_def.name.value.clone();
                    let data_type =
                        self.sql_type_to_data_type(&dataset_id, &column_def.data_type)?;

                    let mut is_nullable = true;
                    let mut default_value: Option<Value> = None;

                    for opt in &column_def.options {
                        match &opt.option {
                            sqlparser::ast::ColumnOption::NotNull => {
                                is_nullable = false;
                            }
                            sqlparser::ast::ColumnOption::Null => {
                                is_nullable = true;
                            }
                            sqlparser::ast::ColumnOption::Default(expr) => {
                                default_value = Some(self.evaluate_default_expr(expr)?);
                            }
                            _ => {}
                        }
                    }

                    let field = if is_nullable {
                        Field::nullable(col_name, data_type)
                    } else {
                        Field::required(col_name, data_type)
                    };

                    table.add_column(field, default_value)?;
                }

                AlterTableOperation::DropColumn {
                    column_names,
                    if_exists,
                    ..
                } => {
                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

                    for col in column_names {
                        let col_name = col.value.clone();

                        if table.schema().field(&col_name).is_none() {
                            if *if_exists {
                                continue;
                            }
                            return Err(Error::column_not_found(format!(
                                "Column '{}' does not exist in table '{}'",
                                col_name, table_id
                            )));
                        }

                        table.drop_column(&col_name)?;
                    }
                }

                AlterTableOperation::AlterColumn { column_name, op } => {
                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

                    let col_name = column_name.value.clone();

                    match op {
                        sqlparser::ast::AlterColumnOperation::SetNotNull => {
                            table.alter_column(&col_name, None, Some(false), None, false)?;
                        }
                        sqlparser::ast::AlterColumnOperation::DropNotNull => {
                            table.alter_column(&col_name, None, Some(true), None, false)?;
                        }
                        sqlparser::ast::AlterColumnOperation::SetDataType {
                            data_type,
                            using,
                            ..
                        } => {
                            let new_type = self.sql_type_to_data_type(&dataset_id, data_type)?;
                            table.alter_column(
                                &col_name,
                                Some(new_type),
                                None,
                                None,
                                using.is_some(),
                            )?;
                        }
                        sqlparser::ast::AlterColumnOperation::SetDefault { value } => {
                            let default_val = self.evaluate_default_expr(value)?;
                            table.alter_column(&col_name, None, None, Some(default_val), false)?;
                        }
                        sqlparser::ast::AlterColumnOperation::DropDefault => {
                            table.alter_column(
                                &col_name,
                                None,
                                None,
                                Some(Value::null()),
                                false,
                            )?;
                        }
                        _ => {
                            return Err(Error::unsupported_feature(format!(
                                "ALTER COLUMN operation {:?} not supported",
                                op
                            )));
                        }
                    }
                }

                AlterTableOperation::DropConstraint { .. } => {}

                AlterTableOperation::AddConstraint { constraint, .. } => {
                    use sqlparser::ast::TableConstraint;

                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

                    match constraint {
                        TableConstraint::PrimaryKey { columns, .. } => {
                            let col_names: Vec<String> =
                                columns.iter().map(|c| c.column.expr.to_string()).collect();

                            self.validate_primary_key_constraint(table, &col_names)?;

                            table.schema_mut().set_primary_key(col_names.clone());
                        }
                        TableConstraint::Unique { columns, name, .. } => {
                            let col_names: Vec<String> =
                                columns.iter().map(|c| c.column.expr.to_string()).collect();

                            self.validate_unique_constraint(table, &col_names)?;

                            table.schema_mut().add_unique_constraint(
                                yachtsql_storage::schema::UniqueConstraint {
                                    name: name.as_ref().map(|n| n.to_string()),
                                    columns: col_names,
                                    enforced: true,
                                    nulls_distinct: true,
                                },
                            );
                        }
                        TableConstraint::Check { name, expr, .. } => {
                            let constraint_name = name.as_ref().map(|n| n.to_string());
                            let expr_str = expr.to_string();

                            self.validate_check_constraint(table, &expr_str)?;

                            table.schema_mut().add_check_constraint_with_validity(
                                CheckConstraint {
                                    name: constraint_name,
                                    expression: expr_str,
                                    enforced: true,
                                },
                                true,
                            );
                        }
                        TableConstraint::ForeignKey { .. } => {
                            return Err(Error::unsupported_feature(
                                "FOREIGN KEY constraints are not supported in BigQuery".to_string(),
                            ));
                        }
                        _ => {
                            return Err(Error::unsupported_feature(format!(
                                "Constraint type {:?} not supported",
                                constraint
                            )));
                        }
                    }
                }

                AlterTableOperation::RenameConstraint { old_name, new_name } => {
                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

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

                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

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
                        ConstraintTypeTag::Unique | ConstraintTypeTag::PrimaryKey => {}
                    }

                    table.schema_mut().set_constraint_valid(&constraint_name);
                }

                AlterTableOperation::ModifyColumn {
                    col_name,
                    data_type,
                    options,
                    ..
                } => {
                    let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
                        Error::table_not_found(format!(
                            "Table '{}.{}' not found",
                            dataset_id, table_id
                        ))
                    })?;

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

                AlterTableOperation::EnableTrigger { .. }
                | AlterTableOperation::DisableTrigger { .. }
                | AlterTableOperation::EnableRule { .. }
                | AlterTableOperation::DisableRule { .. }
                | AlterTableOperation::EnableReplicaTrigger { .. }
                | AlterTableOperation::EnableAlwaysTrigger { .. }
                | AlterTableOperation::DisableRowLevelSecurity
                | AlterTableOperation::EnableRowLevelSecurity => {
                    return Err(Error::unsupported_feature(
                        "Triggers, rules, and row-level security are not supported in BigQuery"
                            .to_string(),
                    ));
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
        _expr_str: &str,
    ) -> Result<()> {
        use yachtsql_storage::storage_backend::TableStorage;

        let row_count = table.row_count();
        if row_count == 0 {
            return Ok(());
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
                SqlValue::null() => Ok(Value::null()),
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
