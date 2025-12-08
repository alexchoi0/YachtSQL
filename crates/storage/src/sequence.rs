use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use yachtsql_core::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SequenceConfig {
    pub start_value: i64,

    pub increment: i64,

    pub min_value: Option<i64>,

    pub max_value: Option<i64>,

    pub cycle: bool,

    pub cache: u32,
}

impl Default for SequenceConfig {
    fn default() -> Self {
        Self {
            start_value: 1,
            increment: 1,
            min_value: None,
            max_value: None,
            cycle: false,
            cache: 1,
        }
    }
}

impl SequenceConfig {
    pub fn validate(&self) -> Result<()> {
        if self.increment == 0 {
            return Err(Error::invalid_query(
                "SEQUENCE INCREMENT cannot be zero".to_string(),
            ));
        }

        if let (Some(min), Some(max)) = (self.min_value, self.max_value)
            && min >= max
        {
            return Err(Error::invalid_query(format!(
                "SEQUENCE MINVALUE ({}) must be less than MAXVALUE ({})",
                min, max
            )));
        }

        let min = self.min();
        let max = self.max();

        if self.start_value < min || self.start_value > max {
            return Err(Error::invalid_query(format!(
                "SEQUENCE START value ({}) must be between MINVALUE ({}) and MAXVALUE ({})",
                self.start_value, min, max
            )));
        }

        self.validate_direction_consistency(min, max)?;

        Ok(())
    }

    fn validate_direction_consistency(&self, min: i64, max: i64) -> Result<()> {
        if self.increment > 0 {
            if self.max_value.is_some() && self.start_value > max {
                return Err(Error::invalid_query(format!(
                    "SEQUENCE with positive INCREMENT ({}) cannot have START ({}) greater than MAXVALUE ({})",
                    self.increment, self.start_value, max
                )));
            }
        } else {
            if self.min_value.is_some() && self.start_value < min {
                return Err(Error::invalid_query(format!(
                    "SEQUENCE with negative INCREMENT ({}) cannot have START ({}) less than MINVALUE ({})",
                    self.increment, self.start_value, min
                )));
            }

            if self.max_value.is_some() && self.start_value < max {
                return Err(Error::invalid_query(format!(
                    "SEQUENCE with negative INCREMENT ({}) has START ({}) below MAXVALUE ({}), which is unreachable",
                    self.increment, self.start_value, max
                )));
            }
        }
        Ok(())
    }

    pub fn min(&self) -> i64 {
        self.min_value.unwrap_or(i64::MIN)
    }

    pub fn max(&self) -> i64 {
        self.max_value.unwrap_or(i64::MAX)
    }
}

#[derive(Debug, Clone)]
pub struct Sequence {
    pub name: String,

    current_value: Rc<RefCell<i64>>,

    pub config: SequenceConfig,

    last_session_value: Option<i64>,

    pub owned_by: Option<(String, String)>,

    is_exhausted: Rc<RefCell<bool>>,
}

impl Sequence {
    pub fn new(name: String, config: SequenceConfig) -> Result<Self> {
        config.validate()?;

        Ok(Self {
            name,
            current_value: Rc::new(RefCell::new(config.start_value)),
            config,
            last_session_value: None,
            owned_by: None,
            is_exhausted: Rc::new(RefCell::new(false)),
        })
    }

    pub fn nextval(&mut self) -> Result<i64> {
        if *self.is_exhausted.borrow() {
            return self.exhausted_error();
        }

        let mut current = self.current_value.borrow_mut();
        let value_to_return = *current;

        match self.advance(*current) {
            Ok(next) => {
                *current = next;
            }
            Err(_) => {
                drop(current);
                *self.is_exhausted.borrow_mut() = true;
            }
        }

        self.last_session_value = Some(value_to_return);
        Ok(value_to_return)
    }

    fn exhausted_error(&self) -> Result<i64> {
        let is_ascending = self.config.increment > 0;
        let limit = if is_ascending {
            self.config.max()
        } else {
            self.config.min()
        };
        let limit_type = if is_ascending { "maximum" } else { "minimum" };

        Err(Error::InvalidOperation(format!(
            "Sequence '{}' has reached its {} value ({})",
            self.name, limit_type, limit
        )))
    }

    pub fn currval(&self) -> Result<i64> {
        self.last_session_value.ok_or_else(|| {
            Error::InvalidOperation(format!(
                "CURRVAL of sequence '{}' is not yet defined in this session",
                self.name
            ))
        })
    }

    pub fn setval(&mut self, value: i64, is_called: bool) -> Result<i64> {
        let new_current = if is_called {
            self.last_session_value = Some(value);
            value.checked_add(self.config.increment).ok_or_else(|| {
                Error::InvalidOperation(
                    "SETVAL value would cause overflow when computing next value".to_string(),
                )
            })?
        } else {
            value
        };

        *self.current_value.borrow_mut() = new_current;
        *self.is_exhausted.borrow_mut() = false;

        Ok(value)
    }

    pub fn restart(&mut self, new_start: Option<i64>) -> Result<()> {
        let start = new_start.unwrap_or(self.config.start_value);

        let min = self.config.min();
        let max = self.config.max();

        if start < min || start > max {
            return Err(Error::invalid_query(format!(
                "RESTART value ({}) must be between MINVALUE ({}) and MAXVALUE ({})",
                start, min, max
            )));
        }

        *self.current_value.borrow_mut() = start;
        self.last_session_value = None;

        *self.is_exhausted.borrow_mut() = false;

        Ok(())
    }

    fn advance(&self, current: i64) -> Result<i64> {
        let min = self.config.min();
        let max = self.config.max();
        let is_ascending = self.config.increment > 0;

        let next = if is_ascending {
            current.checked_add(self.config.increment)
        } else {
            current.checked_sub(self.config.increment.abs())
        };

        let next = match next {
            Some(n) => n,
            None => return self.handle_limit_reached(is_ascending, min, max),
        };

        if is_ascending && next > max {
            self.handle_limit_reached(true, min, max)
        } else if !is_ascending && next < min {
            self.handle_limit_reached(false, min, max)
        } else {
            Ok(next)
        }
    }

    fn handle_limit_reached(&self, is_max_limit: bool, _min: i64, _max: i64) -> Result<i64> {
        if self.config.cycle {
            let cycle_to = if is_max_limit {
                self.config.min_value.unwrap_or(1)
            } else {
                self.config.max_value.unwrap_or(-1)
            };
            Ok(cycle_to)
        } else {
            let limit_type = if is_max_limit { "maximum" } else { "minimum" };
            let limit_value = if is_max_limit {
                self.config.max()
            } else {
                self.config.min()
            };
            Err(Error::InvalidOperation(format!(
                "Sequence '{}' has reached its {} value ({})",
                self.name, limit_type, limit_value
            )))
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SequenceRegistry {
    sequences: HashMap<String, Sequence>,

    last_global_value: Option<i64>,
}

impl SequenceRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn has_sequences(&self) -> bool {
        !self.sequences.is_empty()
    }

    fn get_sequence_or_err(&mut self, name: &str) -> Result<&mut Sequence> {
        self.sequences
            .get_mut(name)
            .ok_or_else(|| Error::invalid_query(format!("Sequence '{}' does not exist", name)))
    }

    fn get_sequence_ref_or_err(&self, name: &str) -> Result<&Sequence> {
        self.sequences
            .get(name)
            .ok_or_else(|| Error::invalid_query(format!("Sequence '{}' does not exist", name)))
    }

    pub fn create_sequence(
        &mut self,
        name: String,
        config: SequenceConfig,
        if_not_exists: bool,
    ) -> Result<()> {
        if self.sequences.contains_key(&name) {
            if if_not_exists {
                return Ok(());
            } else {
                return Err(Error::invalid_query(format!(
                    "Sequence '{}' already exists",
                    name
                )));
            }
        }

        let sequence = Sequence::new(name.clone(), config)?;
        self.sequences.insert(name, sequence);
        Ok(())
    }

    pub fn drop_sequence(&mut self, name: &str, if_exists: bool, cascade: bool) -> Result<()> {
        if !self.sequences.contains_key(name) {
            if if_exists {
                return Ok(());
            } else {
                return Err(Error::invalid_query(format!(
                    "Sequence '{}' does not exist",
                    name
                )));
            }
        }

        let sequence = self
            .sequences
            .get(name)
            .expect("sequence should exist after contains_key check");

        if let Some((table, column)) = &sequence.owned_by
            && !cascade
        {
            return Err(Error::invalid_query(format!(
                "Cannot drop sequence '{}' because column '{}.{}' depends on it. Use CASCADE to drop dependents.",
                name, table, column
            )));
        }

        self.sequences.remove(name);
        Ok(())
    }

    pub fn nextval(&mut self, name: &str) -> Result<i64> {
        let sequence = self.get_sequence_or_err(name)?;
        let value = sequence.nextval()?;
        self.last_global_value = Some(value);
        Ok(value)
    }

    pub fn currval(&self, name: &str) -> Result<i64> {
        let sequence = self.get_sequence_ref_or_err(name)?;
        sequence.currval()
    }

    pub fn setval(&mut self, name: &str, value: i64, is_called: bool) -> Result<i64> {
        let sequence = self.get_sequence_or_err(name)?;
        let result = sequence.setval(value, is_called)?;
        if is_called {
            self.last_global_value = Some(value);
        }
        Ok(result)
    }

    pub fn lastval(&self) -> Result<i64> {
        self.last_global_value.ok_or_else(|| {
            Error::InvalidOperation("LASTVAL is not yet defined in this session".to_string())
        })
    }

    pub fn get_sequence(&self, name: &str) -> Option<&Sequence> {
        self.sequences.get(name)
    }

    pub fn get_sequence_mut(&mut self, name: &str) -> Option<&mut Sequence> {
        self.sequences.get_mut(name)
    }

    pub fn alter_sequence(
        &mut self,
        name: &str,
        new_increment: Option<i64>,
        new_min: Option<Option<i64>>,
        new_max: Option<Option<i64>>,
        new_cycle: Option<bool>,
        restart: Option<Option<i64>>,
    ) -> Result<()> {
        let sequence = self.get_sequence_or_err(name)?;

        if let Some(increment) = new_increment {
            sequence.config.increment = increment;
        }
        if let Some(min) = new_min {
            sequence.config.min_value = min;
        }
        if let Some(max) = new_max {
            sequence.config.max_value = max;
        }
        if let Some(cycle) = new_cycle {
            sequence.config.cycle = cycle;
        }

        sequence.config.validate()?;

        if let Some(new_start) = restart {
            sequence.restart(new_start)?;
        }

        Ok(())
    }

    pub fn set_owned_by(&mut self, name: &str, table: String, column: String) -> Result<()> {
        let sequence = self.get_sequence_or_err(name)?;
        sequence.owned_by = Some((table, column));
        Ok(())
    }

    pub fn drop_owned_by_table(&mut self, table_name: &str) {
        let to_drop: Vec<String> = self
            .sequences
            .iter()
            .filter_map(|(name, seq)| {
                if let Some((table, _)) = &seq.owned_by
                    && table == table_name
                {
                    return Some(name.clone());
                }
                None
            })
            .collect();

        for seq_name in to_drop {
            self.sequences.remove(&seq_name);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sequence_basic() {
        let config = SequenceConfig::default();
        let mut seq = Sequence::new("test_seq".to_string(), config).unwrap();

        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
        assert_eq!(seq.nextval().unwrap(), 3);
    }

    #[test]
    fn test_sequence_with_increment() {
        let config = SequenceConfig {
            start_value: 10,
            increment: 5,
            ..Default::default()
        };
        let mut seq = Sequence::new("test_seq".to_string(), config).unwrap();

        assert_eq!(seq.nextval().unwrap(), 10);
        assert_eq!(seq.nextval().unwrap(), 15);
        assert_eq!(seq.nextval().unwrap(), 20);
    }

    #[test]
    fn test_sequence_currval_before_nextval() {
        let config = SequenceConfig::default();
        let seq = Sequence::new("test_seq".to_string(), config).unwrap();

        assert!(seq.currval().is_err());
    }

    #[test]
    fn test_sequence_currval_after_nextval() {
        let config = SequenceConfig::default();
        let mut seq = Sequence::new("test_seq".to_string(), config).unwrap();

        seq.nextval().unwrap();
        assert_eq!(seq.currval().unwrap(), 1);
        assert_eq!(seq.currval().unwrap(), 1);
    }

    #[test]
    fn test_sequence_overflow_without_cycle() {
        let config = SequenceConfig {
            start_value: 9223372036854775805,
            increment: 1,
            max_value: Some(9223372036854775807),
            cycle: false,
            ..Default::default()
        };
        let mut seq = Sequence::new("test_seq".to_string(), config).unwrap();

        assert_eq!(seq.nextval().unwrap(), 9223372036854775805);
        assert_eq!(seq.nextval().unwrap(), 9223372036854775806);
        assert_eq!(seq.nextval().unwrap(), 9223372036854775807);
        assert!(seq.nextval().is_err());
    }

    #[test]
    fn test_sequence_cycle() {
        let config = SequenceConfig {
            start_value: 1,
            increment: 1,
            min_value: Some(1),
            max_value: Some(3),
            cycle: true,
            ..Default::default()
        };
        let mut seq = Sequence::new("test_seq".to_string(), config).unwrap();

        assert_eq!(seq.nextval().unwrap(), 1);
        assert_eq!(seq.nextval().unwrap(), 2);
        assert_eq!(seq.nextval().unwrap(), 3);
        assert_eq!(seq.nextval().unwrap(), 1);
    }

    #[test]
    fn test_sequence_registry() {
        let mut registry = SequenceRegistry::new();

        registry
            .create_sequence("seq1".to_string(), SequenceConfig::default(), false)
            .unwrap();

        assert_eq!(registry.nextval("seq1").unwrap(), 1);
        assert_eq!(registry.nextval("seq1").unwrap(), 2);
        assert_eq!(registry.currval("seq1").unwrap(), 2);
    }

    #[test]
    fn test_invalid_config_increment_zero() {
        let config = SequenceConfig {
            increment: 0,
            ..Default::default()
        };
        assert!(Sequence::new("test".to_string(), config).is_err());
    }

    #[test]
    fn test_invalid_config_min_greater_than_max() {
        let config = SequenceConfig {
            min_value: Some(100),
            max_value: Some(50),
            ..Default::default()
        };
        assert!(Sequence::new("test".to_string(), config).is_err());
    }
}
