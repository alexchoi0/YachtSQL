use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::StructField;
use yachtsql_parser::validator::CustomStatement;
use yachtsql_storage::TypeDefinition;
use yachtsql_storage::custom_types::EnumType;

use super::super::QueryExecutor;
use super::create::DdlExecutor;
use crate::Table;

pub trait TypeExecutor {
    fn execute_create_type(&mut self, stmt: &sqlparser::ast::Statement) -> Result<Table>;

    fn execute_drop_type(&mut self, stmt: &sqlparser::ast::Statement) -> Result<Table>;

    fn execute_create_composite_type(&mut self, stmt: &CustomStatement) -> Result<Table>;

    fn execute_drop_composite_type(&mut self, stmt: &CustomStatement) -> Result<Table>;
}

impl TypeExecutor for QueryExecutor {
    fn execute_create_type(&mut self, stmt: &sqlparser::ast::Statement) -> Result<Table> {
        let sqlparser::ast::Statement::CreateType {
            name,
            representation,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a CREATE TYPE statement".to_string(),
            ));
        };

        let sqlparser::ast::UserDefinedTypeRepresentation::Enum { labels } = representation else {
            return Err(Error::UnsupportedFeature(
                "Only ENUM types are currently supported".to_string(),
            ));
        };

        let type_name = name.to_string();
        let (dataset_id, type_id) = self.parse_ddl_table_name(&type_name)?;

        let label_strings: Vec<String> = labels.iter().map(|ident| ident.value.clone()).collect();

        let enum_type = EnumType::new(type_id.clone(), None, label_strings)?;

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.to_string())?;
        }

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        dataset.types_mut().create_enum(enum_type, false)?;

        Self::empty_result()
    }

    fn execute_drop_type(&mut self, stmt: &sqlparser::ast::Statement) -> Result<Table> {
        let sqlparser::ast::Statement::Drop {
            object_type,
            if_exists,
            names,
            cascade,
            ..
        } = stmt
        else {
            return Err(Error::InternalError("Not a DROP statement".to_string()));
        };

        if !matches!(object_type, sqlparser::ast::ObjectType::Type) {
            return Err(Error::InternalError(
                "Not a DROP TYPE statement".to_string(),
            ));
        }

        let mut storage = self.storage.borrow_mut();

        for name in names {
            let type_name = name.to_string();
            let (dataset_id, type_id) = self.parse_ddl_table_name(&type_name)?;

            let Some(dataset) = storage.get_dataset_mut(&dataset_id) else {
                if *if_exists {
                    continue;
                }
                return Err(Error::DatasetNotFound(format!(
                    "Dataset '{}' not found",
                    dataset_id
                )));
            };

            if *cascade {
                dataset
                    .types_mut()
                    .drop_type_cascade(&type_id, *if_exists)?;
            } else {
                dataset.types_mut().drop_type(&type_id, *if_exists)?;
            }
        }

        Self::empty_result()
    }

    fn execute_create_composite_type(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::CreateType {
            if_not_exists,
            name,
            fields,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a CREATE TYPE statement".to_string(),
            ));
        };

        let type_name = name.to_string();
        let (dataset_id, type_id) = self.parse_ddl_table_name(&type_name)?;

        let struct_fields: Vec<StructField> = fields
            .iter()
            .map(|f| {
                let data_type = self.sql_type_to_data_type(&dataset_id, &f.data_type)?;
                Ok(StructField {
                    name: f.name.clone(),
                    data_type,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        for field in &struct_fields {
            if Self::type_references_name(&field.data_type, &type_id) {
                return Err(Error::invalid_query(format!(
                    "Cannot create type '{}': field '{}' creates a self-referential type",
                    type_id, field.name
                )));
            }
        }

        let definition = TypeDefinition::Composite(struct_fields);

        let mut storage = self.storage.borrow_mut();

        if storage.get_dataset(&dataset_id).is_none() {
            storage.create_dataset(dataset_id.to_string())?;
        }

        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        dataset
            .types_mut()
            .create_type(type_id, definition, *if_not_exists)?;

        Self::empty_result()
    }

    fn execute_drop_composite_type(&mut self, stmt: &CustomStatement) -> Result<Table> {
        let CustomStatement::DropType {
            if_exists,
            names,
            cascade,
            restrict: _,
        } = stmt
        else {
            return Err(Error::InternalError(
                "Not a DROP TYPE statement".to_string(),
            ));
        };

        let mut storage = self.storage.borrow_mut();

        for name in names {
            let type_name = name.to_string();
            let (dataset_id, type_id) = self.parse_ddl_table_name(&type_name)?;

            let Some(dataset) = storage.get_dataset_mut(&dataset_id) else {
                if *if_exists {
                    continue;
                }
                return Err(Error::DatasetNotFound(format!(
                    "Dataset '{}' not found",
                    dataset_id
                )));
            };

            if !*cascade {
                for (table_name, table) in dataset.tables() {
                    for field in table.schema().fields() {
                        if Self::type_uses_composite(&field.data_type, &type_id) {
                            return Err(Error::invalid_query(format!(
                                "Cannot drop type '{}' because column '{}.{}' uses it. Use DROP TYPE ... CASCADE.",
                                type_id, table_name, field.name
                            )));
                        }
                    }
                }
            }

            dataset.types_mut().drop_type(&type_id, *if_exists)?;
        }

        Self::empty_result()
    }
}

impl QueryExecutor {
    fn type_uses_composite(data_type: &yachtsql_core::types::DataType, type_name: &str) -> bool {
        use yachtsql_core::types::DataType;

        match data_type {
            DataType::Custom(name) => name.eq_ignore_ascii_case(type_name),
            DataType::Struct(fields) => fields
                .iter()
                .any(|f| Self::type_uses_composite(&f.data_type, type_name)),
            DataType::Array(inner) => Self::type_uses_composite(inner, type_name),
            _ => false,
        }
    }

    fn type_references_name(data_type: &yachtsql_core::types::DataType, type_name: &str) -> bool {
        use yachtsql_core::types::DataType;

        match data_type {
            DataType::Custom(name) => name.eq_ignore_ascii_case(type_name),
            DataType::Struct(fields) => fields
                .iter()
                .any(|f| Self::type_references_name(&f.data_type, type_name)),
            DataType::Array(inner) => Self::type_references_name(inner, type_name),
            _ => false,
        }
    }
}
