use yachtsql_core::types::Value;
use yachtsql_storage::Schema;
use yachtsql_storage::schema::Field;

use crate::Table;
use crate::storage::table::Row;

pub(super) fn extract_source_row(
    batch: &Table,
    row_idx: usize,
    schema: &Schema,
) -> yachtsql_core::error::Result<Row> {
    let mut row = Row::for_schema(schema);
    for (col_idx, field) in schema.fields().iter().enumerate() {
        let value = if let Some(column) = batch.column(col_idx) {
            column.get(row_idx)?
        } else {
            Value::null()
        };
        row.set_by_name(schema, &field.name, value)?;
    }
    Ok(row)
}

pub(super) fn build_join_row(
    source_row: &Row,
    target_row: &Row,
    source_schema: &Schema,
    target_schema: &Schema,
    source_alias: Option<&str>,
    target_alias: Option<&str>,
    target_table: &str,
) -> yachtsql_core::error::Result<Row> {
    let source_prefix = source_alias.unwrap_or(target_table);
    let target_prefix = target_alias.unwrap_or(target_table);

    let combined_schema =
        build_join_schema(source_schema, target_schema, source_prefix, target_prefix);

    let mut joined = Row::for_schema(&combined_schema);

    add_row_columns_to_join(
        &mut joined,
        source_row,
        source_schema,
        &combined_schema,
        source_prefix,
        Some(target_schema),
    )?;

    add_row_columns_to_join(
        &mut joined,
        target_row,
        target_schema,
        &combined_schema,
        target_prefix,
        None,
    )?;

    Ok(joined)
}

pub(super) fn build_join_schema(
    source_schema: &Schema,
    target_schema: &Schema,
    source_prefix: &str,
    target_prefix: &str,
) -> Schema {
    let mut fields = Vec::new();

    for field in source_schema.fields() {
        fields.push(Field::nullable(
            format!("{}.{}", source_prefix, field.name),
            field.data_type.clone(),
        ));

        if target_schema.field(&field.name).is_none() {
            fields.push(Field::nullable(field.name.clone(), field.data_type.clone()));
        }
    }

    for field in target_schema.fields() {
        fields.push(Field::nullable(
            format!("{}.{}", target_prefix, field.name),
            field.data_type.clone(),
        ));

        fields.push(Field::nullable(field.name.clone(), field.data_type.clone()));
    }

    Schema::from_fields(fields)
}

fn add_row_columns_to_join(
    joined: &mut Row,
    source_row: &Row,
    row_schema: &Schema,
    combined_schema: &Schema,
    prefix: &str,
    conflict_schema: Option<&Schema>,
) -> yachtsql_core::error::Result<()> {
    for field in row_schema.fields() {
        let value = source_row
            .get_by_name(row_schema, &field.name)
            .cloned()
            .unwrap_or(Value::null());

        let qualified_name = format!("{}.{}", prefix, field.name);
        joined.set_by_name(combined_schema, &qualified_name, value.clone())?;

        let has_conflict = conflict_schema
            .map(|s| s.field(&field.name).is_some())
            .unwrap_or(false);

        if !has_conflict {
            joined.set_by_name(combined_schema, &field.name, value)?;
        }
    }
    Ok(())
}
