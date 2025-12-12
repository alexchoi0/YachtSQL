use yachtsql_common::error::Result;

use super::Table;
use crate::row::Row;

pub struct TableIterator<'a> {
    pub(super) table: &'a Table,
    pub(super) current_row: usize,
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_row < self.table.row_count() {
            let row = self.table.get_row(self.current_row);
            self.current_row += 1;
            Some(row)
        } else {
            None
        }
    }
}

impl<'a> TableIterator<'a> {
    pub fn len(&self) -> usize {
        self.table.row_count().saturating_sub(self.current_row)
    }

    pub fn is_empty(&self) -> bool {
        self.current_row >= self.table.row_count()
    }
}
