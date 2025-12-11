use sqlparser::ast::ObjectName;
use yachtsql_core::error::{Error, Result};
use yachtsql_parser::validator::{PostgresPartitionBoundDef, PostgresPartitionStrategyDef};
use yachtsql_storage::{
    PostgresPartitionBound, PostgresPartitionInfo, PostgresPartitionStrategy, Schema,
    TableSchemaOps,
};

use super::super::QueryExecutor;
use super::create::DdlExecutor;
use crate::Table;

pub trait PartitionExecutor {
    fn execute_create_partition(
        &mut self,
        partition_name: &ObjectName,
        parent_name: &ObjectName,
        bound: &PostgresPartitionBoundDef,
        sub_partition: &Option<PostgresPartitionStrategyDef>,
    ) -> Result<Table>;

    fn execute_detach_partition(
        &mut self,
        parent_name: &ObjectName,
        partition_name: &ObjectName,
        concurrently: bool,
        finalize: bool,
    ) -> Result<Table>;

    fn execute_attach_partition(
        &mut self,
        parent_name: &ObjectName,
        partition_name: &ObjectName,
        bound: &PostgresPartitionBoundDef,
    ) -> Result<Table>;

    fn execute_enable_row_movement(&mut self, table_name: &ObjectName) -> Result<Table>;

    fn execute_disable_row_movement(&mut self, table_name: &ObjectName) -> Result<Table>;
}

impl PartitionExecutor for QueryExecutor {
    fn execute_create_partition(
        &mut self,
        partition_name: &ObjectName,
        parent_name: &ObjectName,
        bound: &PostgresPartitionBoundDef,
        sub_partition: &Option<PostgresPartitionStrategyDef>,
    ) -> Result<Table> {
        let partition_name_str = partition_name.to_string();
        let parent_name_str = parent_name.to_string();

        let (partition_dataset_id, partition_table_id) =
            self.parse_ddl_table_name(&partition_name_str)?;
        let (parent_dataset_id, parent_table_id) = self.parse_ddl_table_name(&parent_name_str)?;

        let parent_dataset_id = if parent_dataset_id == "default" && !parent_name_str.contains('.')
        {
            partition_dataset_id.clone()
        } else {
            parent_dataset_id
        };

        let schema = {
            let storage = self.storage.borrow();
            let parent_dataset = storage.get_dataset(&parent_dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Parent dataset '{}' not found", parent_dataset_id))
            })?;

            let parent_table = parent_dataset.get_table(&parent_table_id).ok_or_else(|| {
                Error::InvalidQuery(format!("Parent table '{}' does not exist", parent_name_str))
            })?;

            let parent_schema = parent_table.schema();

            if !parent_schema.is_partitioned() {
                return Err(Error::InvalidQuery(format!(
                    "Table '{}' is not partitioned",
                    parent_name_str
                )));
            }

            let mut child_schema = yachtsql_storage::Schema::new();

            for field in parent_schema.fields() {
                child_schema.add_field(field.clone());
            }

            if let Some(pk) = parent_schema.primary_key() {
                child_schema.set_primary_key(pk.to_vec());
            }

            let storage_bound = convert_bound_def_to_storage(bound);
            let sub_strategy = sub_partition.as_ref().map(convert_strategy_def_to_storage);

            let partition_info = PostgresPartitionInfo {
                parent_table: Some(format!("{}.{}", parent_dataset_id, parent_table_id)),
                bound: Some(storage_bound),
                strategy: sub_strategy,
                child_partitions: Vec::new(),
                row_movement_enabled: false,
            };
            child_schema.set_postgres_partition(partition_info);

            child_schema
        };

        {
            let mut storage = self.storage.borrow_mut();
            if storage.get_dataset(&partition_dataset_id).is_none() {
                storage.create_dataset(partition_dataset_id.clone())?;
            }
            let dataset = storage
                .get_dataset_mut(&partition_dataset_id)
                .ok_or_else(|| {
                    Error::DatasetNotFound(format!("Dataset '{}' not found", partition_dataset_id))
                })?;
            dataset.create_table(partition_table_id.clone(), schema)?;
        }

        {
            let mut storage = self.storage.borrow_mut();
            if let Some(parent_dataset) = storage.get_dataset_mut(&parent_dataset_id) {
                if let Some(parent_table) = parent_dataset.get_table_mut(&parent_table_id) {
                    parent_table.schema_mut().add_partition_child(format!(
                        "{}.{}",
                        partition_dataset_id, partition_table_id
                    ));
                }
            }
        }

        Ok(Table::empty(Schema::from_fields(vec![])))
    }

    fn execute_detach_partition(
        &mut self,
        parent_name: &ObjectName,
        partition_name: &ObjectName,
        _concurrently: bool,
        _finalize: bool,
    ) -> Result<Table> {
        let parent_name_str = parent_name.to_string();
        let partition_name_str = partition_name.to_string();

        let (parent_dataset_id, parent_table_id) = self.parse_ddl_table_name(&parent_name_str)?;
        let (partition_dataset_id, partition_table_id) =
            self.parse_ddl_table_name(&partition_name_str)?;

        let partition_dataset_id =
            if partition_dataset_id == "default" && !partition_name_str.contains('.') {
                parent_dataset_id.clone()
            } else {
                partition_dataset_id
            };

        {
            let mut storage = self.storage.borrow_mut();
            if let Some(parent_dataset) = storage.get_dataset_mut(&parent_dataset_id) {
                if let Some(parent_table) = parent_dataset.get_table_mut(&parent_table_id) {
                    let child_full_name =
                        format!("{}.{}", partition_dataset_id, partition_table_id);
                    parent_table
                        .schema_mut()
                        .remove_partition_child(&child_full_name);
                }
            }
        }

        {
            let mut storage = self.storage.borrow_mut();
            if let Some(partition_dataset) = storage.get_dataset_mut(&partition_dataset_id) {
                if let Some(partition_table) = partition_dataset.get_table_mut(&partition_table_id)
                {
                    if let Some(partition_info) =
                        partition_table.schema_mut().postgres_partition_mut()
                    {
                        partition_info.parent_table = None;
                        partition_info.bound = None;
                    }
                }
            }
        }

        Ok(Table::empty(Schema::from_fields(vec![])))
    }

    fn execute_attach_partition(
        &mut self,
        parent_name: &ObjectName,
        partition_name: &ObjectName,
        bound: &PostgresPartitionBoundDef,
    ) -> Result<Table> {
        let parent_name_str = parent_name.to_string();
        let partition_name_str = partition_name.to_string();

        let (parent_dataset_id, parent_table_id) = self.parse_ddl_table_name(&parent_name_str)?;
        let (partition_dataset_id, partition_table_id) =
            self.parse_ddl_table_name(&partition_name_str)?;

        let partition_dataset_id =
            if partition_dataset_id == "default" && !partition_name_str.contains('.') {
                parent_dataset_id.clone()
            } else {
                partition_dataset_id
            };

        {
            let storage = self.storage.borrow();
            let parent_dataset = storage.get_dataset(&parent_dataset_id).ok_or_else(|| {
                Error::DatasetNotFound(format!("Dataset '{}' not found", parent_dataset_id))
            })?;

            let parent_table = parent_dataset.get_table(&parent_table_id).ok_or_else(|| {
                Error::InvalidQuery(format!("Table '{}' does not exist", parent_name_str))
            })?;

            if !parent_table.schema().is_partitioned() {
                return Err(Error::InvalidQuery(format!(
                    "Table '{}' is not partitioned",
                    parent_name_str
                )));
            }
        }

        {
            let mut storage = self.storage.borrow_mut();
            if let Some(partition_dataset) = storage.get_dataset_mut(&partition_dataset_id) {
                if let Some(partition_table) = partition_dataset.get_table_mut(&partition_table_id)
                {
                    let storage_bound = convert_bound_def_to_storage(bound);
                    let partition_info = PostgresPartitionInfo {
                        parent_table: Some(format!("{}.{}", parent_dataset_id, parent_table_id)),
                        bound: Some(storage_bound),
                        strategy: None,
                        child_partitions: Vec::new(),
                        row_movement_enabled: false,
                    };
                    partition_table
                        .schema_mut()
                        .set_postgres_partition(partition_info);
                }
            }
        }

        {
            let mut storage = self.storage.borrow_mut();
            if let Some(parent_dataset) = storage.get_dataset_mut(&parent_dataset_id) {
                if let Some(parent_table) = parent_dataset.get_table_mut(&parent_table_id) {
                    let child_full_name =
                        format!("{}.{}", partition_dataset_id, partition_table_id);
                    parent_table
                        .schema_mut()
                        .add_partition_child(child_full_name);
                }
            }
        }

        Ok(Table::empty(Schema::from_fields(vec![])))
    }

    fn execute_enable_row_movement(&mut self, table_name: &ObjectName) -> Result<Table> {
        let table_name_str = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name_str)?;

        let mut storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
            Error::InvalidQuery(format!("Table '{}' does not exist", table_name_str))
        })?;

        if !table.schema().is_partitioned() {
            return Err(Error::InvalidQuery(format!(
                "Table '{}' is not partitioned",
                table_name_str
            )));
        }

        if let Some(partition_info) = table.schema_mut().postgres_partition_mut() {
            partition_info.row_movement_enabled = true;
        }

        Ok(Table::empty(Schema::from_fields(vec![])))
    }

    fn execute_disable_row_movement(&mut self, table_name: &ObjectName) -> Result<Table> {
        let table_name_str = table_name.to_string();
        let (dataset_id, table_id) = self.parse_ddl_table_name(&table_name_str)?;

        let mut storage = self.storage.borrow_mut();
        let dataset = storage
            .get_dataset_mut(&dataset_id)
            .ok_or_else(|| Error::DatasetNotFound(format!("Dataset '{}' not found", dataset_id)))?;

        let table = dataset.get_table_mut(&table_id).ok_or_else(|| {
            Error::InvalidQuery(format!("Table '{}' does not exist", table_name_str))
        })?;

        if !table.schema().is_partitioned() {
            return Err(Error::InvalidQuery(format!(
                "Table '{}' is not partitioned",
                table_name_str
            )));
        }

        if let Some(partition_info) = table.schema_mut().postgres_partition_mut() {
            partition_info.row_movement_enabled = false;
        }

        Ok(Table::empty(Schema::from_fields(vec![])))
    }
}

fn convert_bound_def_to_storage(bound: &PostgresPartitionBoundDef) -> PostgresPartitionBound {
    match bound {
        PostgresPartitionBoundDef::Range { from, to } => PostgresPartitionBound::Range {
            from: from.clone(),
            to: to.clone(),
        },
        PostgresPartitionBoundDef::List { values } => PostgresPartitionBound::List {
            values: values.clone(),
        },
        PostgresPartitionBoundDef::Hash { modulus, remainder } => PostgresPartitionBound::Hash {
            modulus: *modulus,
            remainder: *remainder,
        },
        PostgresPartitionBoundDef::Default => PostgresPartitionBound::Default,
    }
}

fn convert_strategy_def_to_storage(
    strategy: &PostgresPartitionStrategyDef,
) -> PostgresPartitionStrategy {
    match strategy {
        PostgresPartitionStrategyDef::Range { columns } => PostgresPartitionStrategy::Range {
            columns: columns.clone(),
        },
        PostgresPartitionStrategyDef::List { columns } => PostgresPartitionStrategy::List {
            columns: columns.clone(),
        },
        PostgresPartitionStrategyDef::Hash { columns } => PostgresPartitionStrategy::Hash {
            columns: columns.clone(),
        },
    }
}
