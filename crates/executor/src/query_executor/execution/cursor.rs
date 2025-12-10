use sqlparser::ast::{CloseCursor, FetchDirection, Statement as SqlStatement, Value as SqlValue};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_storage::{Field, Schema};

use super::CursorOperation;
use super::session::CursorState;
use crate::Table;
use crate::query_executor::execution::QueryExecutor;

impl QueryExecutor {
    pub fn execute_cursor_operation(&mut self, operation: CursorOperation) -> Result<Table> {
        match operation {
            CursorOperation::Declare { stmt } => self.execute_declare_cursor(&stmt),
            CursorOperation::Fetch { stmt } => self.execute_fetch_cursor(&stmt),
            CursorOperation::Close { stmt } => self.execute_close_cursor(&stmt),
            CursorOperation::Move { stmt } => self.execute_move_cursor(&stmt),
        }
    }

    fn execute_declare_cursor(&mut self, stmt: &SqlStatement) -> Result<Table> {
        let SqlStatement::Declare { stmts } = stmt else {
            return Err(Error::internal("Expected DECLARE statement"));
        };

        if stmts.is_empty() {
            return Err(Error::invalid_query("DECLARE requires cursor definition"));
        }

        let decl = &stmts[0];
        let cursor_name = decl
            .names
            .first()
            .map(|ident| ident.value.clone())
            .ok_or_else(|| Error::invalid_query("Cursor name required"))?;

        let query = decl
            .for_query
            .clone()
            .ok_or_else(|| Error::invalid_query("DECLARE CURSOR requires FOR query clause"))?;

        let is_scrollable = decl.scroll.unwrap_or(false);
        let with_hold = decl.hold.unwrap_or(false);
        let is_binary = decl.binary.unwrap_or(false);

        let query_sql = format!("SELECT * FROM ({}) AS __cursor_query", query);
        let result_set = self.execute_sql(&query_sql)?;

        let cursor = CursorState {
            name: cursor_name.clone(),
            query,
            result_set: Some(result_set),
            position: 0,
            is_scrollable,
            with_hold,
            is_binary,
        };

        self.session.declare_cursor(cursor)?;
        Self::empty_result()
    }

    fn execute_fetch_cursor(&mut self, stmt: &SqlStatement) -> Result<Table> {
        let SqlStatement::Fetch {
            name, direction, ..
        } = stmt
        else {
            return Err(Error::internal("Expected FETCH statement"));
        };

        let cursor_name = name.value.clone();

        let (total_rows, current_position, is_scrollable, schema) = {
            let cursor = self.session.get_cursor(&cursor_name).ok_or_else(|| {
                Error::invalid_cursor_name(format!("cursor \"{}\" does not exist", cursor_name))
            })?;

            let result_set = cursor
                .result_set
                .as_ref()
                .ok_or_else(|| Error::invalid_cursor_state("cursor has no result set"))?;

            (
                result_set.num_rows() as i64,
                cursor.position,
                cursor.is_scrollable,
                result_set.schema().clone(),
            )
        };

        let (new_position, count) = match direction {
            FetchDirection::Next => (current_position + 1, 1),
            FetchDirection::Prior => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                (current_position - 1, 1)
            }
            FetchDirection::First => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                (1, 1)
            }
            FetchDirection::Last => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                (total_rows, 1)
            }
            FetchDirection::Absolute { limit } => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                let n = sql_value_to_i64(limit)?;
                let pos = if n < 0 { total_rows + n + 1 } else { n };
                (pos, 1)
            }
            FetchDirection::Relative { limit } => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                let n = sql_value_to_i64(limit)?;
                (current_position + n, 1)
            }
            FetchDirection::Count { limit } => {
                let n = sql_value_to_i64(limit)?;
                if n > 0 {
                    (current_position + 1, n)
                } else if n < 0 {
                    if !is_scrollable {
                        return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                    }
                    (current_position + n, -n)
                } else {
                    (current_position, 0)
                }
            }
            FetchDirection::All => {
                let remaining = total_rows - current_position;
                (current_position + 1, remaining.max(0))
            }
            FetchDirection::Forward { limit } => {
                let n = limit
                    .as_ref()
                    .map(|v| sql_value_to_i64(v))
                    .transpose()?
                    .unwrap_or(1);
                (current_position + 1, n)
            }
            FetchDirection::ForwardAll => {
                let remaining = total_rows - current_position;
                (current_position + 1, remaining.max(0))
            }
            FetchDirection::Backward { limit } => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                let n = limit
                    .as_ref()
                    .map(|v| sql_value_to_i64(v))
                    .transpose()?
                    .unwrap_or(1);
                (current_position - 1, n)
            }
            FetchDirection::BackwardAll => {
                if !is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                (1, current_position)
            }
        };

        let is_backward = matches!(
            direction,
            FetchDirection::Prior | FetchDirection::Backward { .. } | FetchDirection::BackwardAll
        ) || matches!(direction, FetchDirection::Count { limit } if sql_value_to_i64(limit).unwrap_or(0) < 0);

        let (start_idx, end_idx, final_position) = if is_backward {
            let start = (new_position - count + 1).max(1) - 1;
            let end = new_position.min(total_rows);
            let final_pos = start;
            (start as usize, end as usize, final_pos)
        } else {
            let start = new_position.max(1) - 1;
            let end = (new_position + count - 1).min(total_rows);
            let final_pos = end;
            (start as usize, end as usize, final_pos)
        };

        let cursor = self.session.get_cursor_mut(&cursor_name).unwrap();
        cursor.position = final_position;

        if start_idx >= end_idx || start_idx >= total_rows as usize {
            return Ok(Table::empty(schema));
        }

        let result_set = cursor.result_set.as_ref().unwrap();
        result_set.slice(start_idx, end_idx - start_idx)
    }

    fn execute_close_cursor(&mut self, stmt: &SqlStatement) -> Result<Table> {
        let SqlStatement::Close { cursor } = stmt else {
            return Err(Error::internal("Expected CLOSE statement"));
        };

        match cursor {
            CloseCursor::All => {
                self.session.close_all_cursors();
            }
            CloseCursor::Specific { name } => {
                self.session.close_cursor(&name.value)?;
            }
        }

        Self::empty_result()
    }

    fn execute_move_cursor(&mut self, stmt: &SqlStatement) -> Result<Table> {
        let SqlStatement::Fetch {
            name, direction, ..
        } = stmt
        else {
            return Err(Error::internal("Expected MOVE statement"));
        };

        let cursor_name = &name.value;
        let cursor = self.session.get_cursor_mut(cursor_name).ok_or_else(|| {
            Error::invalid_cursor_name(format!("cursor \"{}\" does not exist", cursor_name))
        })?;

        let total_rows = cursor
            .result_set
            .as_ref()
            .map(|rs| rs.num_rows() as i64)
            .unwrap_or(0);

        let new_position = match direction {
            FetchDirection::Next => cursor.position + 1,
            FetchDirection::Prior => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                cursor.position - 1
            }
            FetchDirection::First => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                1
            }
            FetchDirection::Last => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                total_rows
            }
            FetchDirection::Absolute { limit } => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                let n = sql_value_to_i64(limit)?;
                if n < 0 { total_rows + n + 1 } else { n }
            }
            FetchDirection::Relative { limit } => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                cursor.position + sql_value_to_i64(limit)?
            }
            FetchDirection::Count { limit } => {
                let n = sql_value_to_i64(limit)?;
                cursor.position + n
            }
            FetchDirection::All => total_rows,
            FetchDirection::Forward { limit } => {
                let n = limit
                    .as_ref()
                    .map(|v| sql_value_to_i64(v))
                    .transpose()?
                    .unwrap_or(1);
                cursor.position + n
            }
            FetchDirection::ForwardAll => total_rows,
            FetchDirection::Backward { limit } => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                let n = limit
                    .as_ref()
                    .map(|v| sql_value_to_i64(v))
                    .transpose()?
                    .unwrap_or(1);
                cursor.position - n
            }
            FetchDirection::BackwardAll => {
                if !cursor.is_scrollable {
                    return Err(Error::invalid_cursor_state("cursor is not scrollable"));
                }
                0
            }
        };

        cursor.position = new_position.clamp(0, total_rows);

        let schema = Schema::from_fields(vec![Field::required(
            "move".to_string(),
            yachtsql_core::types::DataType::String,
        )]);
        let rows_moved = (new_position - cursor.position).unsigned_abs() as i64;
        Table::from_values(
            schema,
            vec![vec![Value::string(format!("MOVE {}", rows_moved))]],
        )
    }
}

fn sql_value_to_i64(value: &SqlValue) -> Result<i64> {
    match value {
        SqlValue::Number(s, _) => s
            .parse::<i64>()
            .map_err(|_| Error::invalid_query(format!("Invalid number: {}", s))),
        _ => Err(Error::invalid_query("Expected numeric value")),
    }
}
