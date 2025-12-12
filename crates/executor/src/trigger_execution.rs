use debug_print::debug_eprintln;
use yachtsql_common::error::{Error, Result};
use yachtsql_storage::{Row, TriggerEvent, TriggerMetadata, TriggerTiming};

use crate::QueryExecutor;

pub struct TriggerContext {
    pub new_row: Option<Row>,
    pub old_row: Option<Row>,
    pub table_name: String,
    pub schema: Option<yachtsql_storage::Schema>,
}

impl TriggerContext {
    pub fn for_insert(table_name: String, new_row: Row, schema: yachtsql_storage::Schema) -> Self {
        Self {
            new_row: Some(new_row),
            old_row: None,
            table_name,
            schema: Some(schema),
        }
    }

    pub fn for_update(
        table_name: String,
        old_row: Row,
        new_row: Row,
        schema: yachtsql_storage::Schema,
    ) -> Self {
        Self {
            new_row: Some(new_row),
            old_row: Some(old_row),
            table_name,
            schema: Some(schema),
        }
    }

    pub fn for_delete(table_name: String, old_row: Row, schema: yachtsql_storage::Schema) -> Self {
        Self {
            new_row: None,
            old_row: Some(old_row),
            table_name,
            schema: Some(schema),
        }
    }

    pub fn get_old_value(
        &self,
        column_name: &str,
    ) -> Result<Option<yachtsql_common::types::Value>> {
        let Some(ref old_row) = self.old_row else {
            return Ok(None);
        };

        let Some(ref schema) = self.schema else {
            return Err(Error::internal("No schema in trigger context"));
        };

        let field = schema.field(column_name).ok_or_else(|| {
            Error::column_not_found(format!("Column '{}' not found in OLD", column_name))
        })?;

        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name == field.name)
            .ok_or_else(|| Error::internal("Column position not found"))?;

        Ok(Some(old_row.values()[col_idx].clone()))
    }

    pub fn get_new_value(
        &self,
        column_name: &str,
    ) -> Result<Option<yachtsql_common::types::Value>> {
        let Some(ref new_row) = self.new_row else {
            return Ok(None);
        };

        let Some(ref schema) = self.schema else {
            return Err(Error::internal("No schema in trigger context"));
        };

        let field = schema.field(column_name).ok_or_else(|| {
            Error::column_not_found(format!("Column '{}' not found in NEW", column_name))
        })?;

        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name == field.name)
            .ok_or_else(|| Error::internal("Column position not found"))?;

        Ok(Some(new_row.values()[col_idx].clone()))
    }
}

#[derive(Debug)]
pub struct TriggerExecutionResult {
    pub triggers_fired: usize,

    pub skip_operation: bool,
}

impl QueryExecutor {
    pub fn fire_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        timing: TriggerTiming,
        event: &TriggerEvent,
        context: &TriggerContext,
    ) -> Result<TriggerExecutionResult> {
        let triggers_to_fire: Vec<TriggerMetadata> = {
            let storage = self.storage.borrow_mut();

            let dataset = storage.get_dataset(dataset_id).ok_or_else(|| {
                Error::dataset_not_found(format!("Dataset '{}' not found", dataset_id))
            })?;

            dataset
                .triggers()
                .get_triggers_for_event(table_name, timing, event)
                .into_iter()
                .cloned()
                .collect()
        };

        let mut triggers_fired = 0;
        let skip_operation = false;

        for trigger in triggers_to_fire {
            self.execute_single_trigger(&trigger, context)?;
            triggers_fired += 1;
        }

        Ok(TriggerExecutionResult {
            triggers_fired,
            skip_operation,
        })
    }

    fn execute_single_trigger(
        &mut self,
        trigger: &TriggerMetadata,
        context: &TriggerContext,
    ) -> Result<()> {
        if trigger.action_body.contains("EXECUTE FUNCTION") {
            return Ok(());
        }

        let substituted_sql = self.substitute_old_new_references(&trigger.action_body, context)?;

        let statements = self.parse_trigger_statements(&substituted_sql)?;

        for (idx, stmt) in statements.iter().enumerate() {
            match self.execute_sql(stmt) {
                Ok(_) => {}
                Err(e) => {
                    debug_eprintln!(
                        "[executor::trigger] Trigger '{}' statement {} execution failed: {:?}",
                        trigger.name,
                        idx + 1,
                        e
                    );
                }
            }
        }

        Ok(())
    }

    fn parse_trigger_statements(&self, sql: &str) -> Result<Vec<String>> {
        let trimmed = sql.trim();

        let upper = trimmed.to_uppercase();
        if upper.starts_with("BEGIN") && upper.contains("END") {
            let begin_pos = if let Some(pos) = trimmed.to_uppercase().find("BEGIN") {
                pos + 5
            } else {
                return Err(Error::invalid_query("Invalid BEGIN...END block"));
            };

            let end_pos = if let Some(pos) = trimmed.to_uppercase().rfind("END") {
                pos
            } else {
                return Err(Error::invalid_query(
                    "BEGIN...END block missing END keyword",
                ));
            };

            if begin_pos >= end_pos {
                return Err(Error::invalid_query("BEGIN...END block is malformed"));
            }

            let body = &trimmed[begin_pos..end_pos].trim();

            let statements: Vec<String> = body
                .split(';')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();

            if statements.is_empty() {
                return Err(Error::invalid_query(
                    "BEGIN...END block contains no statements",
                ));
            }

            return Ok(statements);
        }

        Ok(vec![trimmed.to_string()])
    }

    fn substitute_old_new_references(
        &mut self,
        sql: &str,
        context: &TriggerContext,
    ) -> Result<String> {
        use regex::Regex;

        let mut result = sql.to_string();

        let old_regex = Regex::new(r"\bOLD\.(\w+)\b").unwrap();
        for cap in old_regex.captures_iter(sql) {
            let full_match = cap.get(0).unwrap().as_str();
            let column_name = cap.get(1).unwrap().as_str();

            if let Some(value) = context.get_old_value(column_name)? {
                let value_literal = Self::value_to_sql_literal(&value);
                result = result.replace(full_match, &value_literal);
            }
        }

        let new_regex = Regex::new(r"\bNEW\.(\w+)\b").unwrap();
        for cap in new_regex.captures_iter(sql) {
            let full_match = cap.get(0).unwrap().as_str();
            let column_name = cap.get(1).unwrap().as_str();

            if let Some(value) = context.get_new_value(column_name)? {
                let value_literal = Self::value_to_sql_literal(&value);
                result = result.replace(full_match, &value_literal);
            }
        }

        Ok(result)
    }

    fn value_to_sql_literal(value: &yachtsql_common::types::Value) -> String {
        if value.is_null() {
            return "NULL".to_string();
        }

        if let Some(b) = value.as_bool() {
            return b.to_string().to_uppercase();
        }
        if let Some(i) = value.as_i64() {
            return i.to_string();
        }
        if let Some(f) = value.as_f64() {
            return f.to_string();
        }
        if let Some(n) = value.as_numeric() {
            return n.to_string();
        }
        if let Some(s) = value.as_str() {
            return format!("'{}'", s.replace('\'', "''"));
        }
        if let Some(b) = value.as_bytes() {
            return format!("X'{}'", hex::encode(b));
        }
        if let Some(d) = value.as_date() {
            return format!("DATE '{}'", d);
        }
        if let Some(dt) = value.as_datetime() {
            return format!("DATETIME '{}'", dt.format("%Y-%m-%d %H:%M:%S"));
        }
        if let Some(t) = value.as_time() {
            return format!("TIME '{}'", t);
        }
        if let Some(ts) = value.as_timestamp() {
            return format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.3f"));
        }
        if let Some(u) = value.as_uuid() {
            return format!("'{}'", u);
        }
        if let Some(arr) = value.as_array() {
            let elements: Vec<String> = arr.iter().map(Self::value_to_sql_literal).collect();
            return format!("ARRAY[{}]", elements.join(", "));
        }
        if let Some(fields) = value.as_struct() {
            let pairs: Vec<String> = fields
                .iter()
                .map(|(k, v)| format!("{}: {}", k, Self::value_to_sql_literal(v)))
                .collect();
            return format!("STRUCT({})", pairs.join(", "));
        }
        if let Some(wkt) = value.as_geography() {
            return format!("ST_GeogFromText('{}')", wkt.replace('\'', "''"));
        }
        if let Some(j) = value.as_json() {
            return format!("'{}'", j.to_string().replace('\'', "''"));
        }

        "DEFAULT".to_string()
    }

    pub fn fire_instead_of_insert_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        new_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<TriggerExecutionResult> {
        let context =
            TriggerContext::for_insert(table_name.to_string(), new_row.clone(), schema.clone());

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::InsteadOf,
            &TriggerEvent::Insert,
            &context,
        )
    }

    pub fn fire_instead_of_update_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<TriggerExecutionResult> {
        let context = TriggerContext::for_update(
            table_name.to_string(),
            old_row.clone(),
            new_row.clone(),
            schema.clone(),
        );

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::InsteadOf,
            &TriggerEvent::Update { columns: None },
            &context,
        )
    }

    pub fn fire_instead_of_delete_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        old_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<TriggerExecutionResult> {
        let context =
            TriggerContext::for_delete(table_name.to_string(), old_row.clone(), schema.clone());

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::InsteadOf,
            &TriggerEvent::Delete,
            &context,
        )
    }

    pub fn fire_before_insert_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        new_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<TriggerExecutionResult> {
        let context =
            TriggerContext::for_insert(table_name.to_string(), new_row.clone(), schema.clone());

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::Before,
            &TriggerEvent::Insert,
            &context,
        )
    }

    pub fn fire_after_insert_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        new_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<()> {
        let context =
            TriggerContext::for_insert(table_name.to_string(), new_row.clone(), schema.clone());

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::After,
            &TriggerEvent::Insert,
            &context,
        )?;

        Ok(())
    }

    pub fn fire_before_update_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<TriggerExecutionResult> {
        let context = TriggerContext::for_update(
            table_name.to_string(),
            old_row.clone(),
            new_row.clone(),
            schema.clone(),
        );

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::Before,
            &TriggerEvent::Update { columns: None },
            &context,
        )
    }

    pub fn fire_after_update_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        old_row: &Row,
        new_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<()> {
        let context = TriggerContext::for_update(
            table_name.to_string(),
            old_row.clone(),
            new_row.clone(),
            schema.clone(),
        );

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::After,
            &TriggerEvent::Update { columns: None },
            &context,
        )?;

        Ok(())
    }

    pub fn fire_before_delete_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        old_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<TriggerExecutionResult> {
        let context =
            TriggerContext::for_delete(table_name.to_string(), old_row.clone(), schema.clone());

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::Before,
            &TriggerEvent::Delete,
            &context,
        )
    }

    pub fn fire_after_delete_triggers(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        old_row: &Row,
        schema: &yachtsql_storage::Schema,
    ) -> Result<()> {
        let context =
            TriggerContext::for_delete(table_name.to_string(), old_row.clone(), schema.clone());

        self.fire_triggers(
            dataset_id,
            table_name,
            TriggerTiming::After,
            &TriggerEvent::Delete,
            &context,
        )?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_trigger_context_insert() {
        let row = Row::empty();
        let schema = yachtsql_storage::Schema::from_fields(vec![]);
        let context = TriggerContext::for_insert("users".to_string(), row, schema);

        assert!(context.new_row.is_some());
        assert!(context.old_row.is_none());
        assert_eq!(context.table_name, "users");
    }

    #[test]
    fn test_trigger_context_update() {
        let old_row = Row::empty();
        let new_row = Row::empty();
        let schema = yachtsql_storage::Schema::from_fields(vec![]);
        let context = TriggerContext::for_update("users".to_string(), old_row, new_row, schema);

        assert!(context.new_row.is_some());
        assert!(context.old_row.is_some());
        assert_eq!(context.table_name, "users");
    }

    #[test]
    fn test_trigger_context_delete() {
        let row = Row::empty();
        let schema = yachtsql_storage::Schema::from_fields(vec![]);
        let context = TriggerContext::for_delete("users".to_string(), row, schema);

        assert!(context.new_row.is_none());
        assert!(context.old_row.is_some());
        assert_eq!(context.table_name, "users");
    }
}
