use std::cell::RefCell;
use std::rc::Rc;

use yachtsql_common::error::{Error, Result};

pub struct AutoIncrementManager {
    counter: Option<Rc<RefCell<i64>>>,
    column: Option<String>,
}

impl AutoIncrementManager {
    pub fn new(counter: Option<Rc<RefCell<i64>>>, column: Option<String>) -> Self {
        Self { counter, column }
    }

    pub fn column(&self) -> Option<&str> {
        self.column.as_deref()
    }

    pub fn next(&self) -> Result<i64> {
        match &self.counter {
            Some(counter) => {
                let mut val = counter.borrow_mut();
                let current = *val;
                *val += 1;
                Ok(current)
            }
            None => Err(Error::InvalidOperation(
                "Table does not have AUTO_INCREMENT column".to_string(),
            )),
        }
    }

    pub fn update_if_greater(&self, explicit_value: i64) -> Result<()> {
        match &self.counter {
            Some(counter) => {
                let mut val = counter.borrow_mut();
                if explicit_value >= *val {
                    *val = explicit_value + 1;
                }
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn reset(&self, start_value: i64) -> Result<()> {
        match &self.counter {
            Some(counter) => {
                *counter.borrow_mut() = start_value;
                Ok(())
            }
            None => Ok(()),
        }
    }

    pub fn set(&self, value: i64) -> Result<()> {
        self.reset(value)
    }

    pub fn init(&mut self, column_name: String, start_value: i64) -> Result<()> {
        if self.counter.is_some() {
            return Err(Error::InvalidOperation(
                "Table already has an AUTO_INCREMENT column".to_string(),
            ));
        }

        self.counter = Some(Rc::new(RefCell::new(start_value)));
        self.column = Some(column_name);
        Ok(())
    }

    pub fn remove(&mut self) -> Result<()> {
        self.counter = None;
        self.column = None;
        Ok(())
    }

    pub fn counter(&self) -> Option<Rc<RefCell<i64>>> {
        self.counter.clone()
    }

    pub fn column_name(&self) -> Option<String> {
        self.column.clone()
    }
}

impl Clone for AutoIncrementManager {
    fn clone(&self) -> Self {
        Self {
            counter: self.counter.clone(),
            column: self.column.clone(),
        }
    }
}
