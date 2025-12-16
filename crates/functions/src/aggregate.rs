use rust_decimal::Decimal;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

pub trait AggregateState: Default {
    fn update(&mut self, value: &Value) -> Result<()>;
    fn merge(&mut self, other: &Self) -> Result<()>;
    fn finalize(&self) -> Result<Value>;
}

#[derive(Default)]
pub struct CountState {
    count: i64,
}

impl AggregateState for CountState {
    fn update(&mut self, value: &Value) -> Result<()> {
        if !matches!(value, Value::Null) {
            self.count += 1;
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.count += other.count;
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::Int64(self.count))
    }
}

#[derive(Default)]
pub struct CountStarState {
    count: i64,
}

impl CountStarState {
    pub fn update(&mut self) {
        self.count += 1;
    }

    pub fn merge(&mut self, other: &Self) {
        self.count += other.count;
    }

    pub fn finalize(&self) -> Value {
        Value::Int64(self.count)
    }
}

#[derive(Default)]
pub struct SumState {
    sum_int: i64,
    sum_float: f64,
    sum_decimal: Decimal,
    is_float: bool,
    is_decimal: bool,
    has_value: bool,
}

impl AggregateState for SumState {
    fn update(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Int64(n) => {
                if self.is_decimal {
                    self.sum_decimal += Decimal::from(*n);
                } else if self.is_float {
                    self.sum_float += *n as f64;
                } else {
                    self.sum_int += n;
                }
                self.has_value = true;
            }
            Value::Float64(f) => {
                if !self.is_float && !self.is_decimal {
                    self.sum_float = self.sum_int as f64;
                    self.is_float = true;
                }
                if self.is_decimal {
                    self.sum_decimal += Decimal::try_from(f.0).unwrap_or_default();
                } else {
                    self.sum_float += f.0;
                }
                self.has_value = true;
            }
            Value::Numeric(d) => {
                if !self.is_decimal {
                    if self.is_float {
                        self.sum_decimal = Decimal::try_from(self.sum_float).unwrap_or_default();
                    } else {
                        self.sum_decimal = Decimal::from(self.sum_int);
                    }
                    self.is_decimal = true;
                }
                self.sum_decimal += d;
                self.has_value = true;
            }
            Value::Null => {}
            _ => return Err(Error::type_mismatch("SUM requires numeric values")),
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if other.is_decimal || self.is_decimal {
            if !self.is_decimal {
                if self.is_float {
                    self.sum_decimal = Decimal::try_from(self.sum_float).unwrap_or_default();
                } else {
                    self.sum_decimal = Decimal::from(self.sum_int);
                }
                self.is_decimal = true;
            }
            let other_decimal = if other.is_decimal {
                other.sum_decimal
            } else if other.is_float {
                Decimal::try_from(other.sum_float).unwrap_or_default()
            } else {
                Decimal::from(other.sum_int)
            };
            self.sum_decimal += other_decimal;
        } else if other.is_float || self.is_float {
            if !self.is_float {
                self.sum_float = self.sum_int as f64;
                self.is_float = true;
            }
            let other_float = if other.is_float {
                other.sum_float
            } else {
                other.sum_int as f64
            };
            self.sum_float += other_float;
        } else {
            self.sum_int += other.sum_int;
        }
        self.has_value = self.has_value || other.has_value;
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if !self.has_value {
            return Ok(Value::Null);
        }
        if self.is_decimal {
            Ok(Value::Numeric(self.sum_decimal))
        } else if self.is_float {
            Ok(Value::Float64(ordered_float::OrderedFloat(self.sum_float)))
        } else {
            Ok(Value::Int64(self.sum_int))
        }
    }
}

#[derive(Default)]
pub struct AvgState {
    sum: f64,
    count: i64,
}

impl AggregateState for AvgState {
    fn update(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Int64(n) => {
                self.sum += *n as f64;
                self.count += 1;
            }
            Value::Float64(f) => {
                self.sum += f.0;
                self.count += 1;
            }
            Value::Numeric(d) => {
                self.sum += f64::try_from(*d).unwrap_or(0.0);
                self.count += 1;
            }
            Value::Null => {}
            _ => return Err(Error::type_mismatch("AVG requires numeric values")),
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.sum += other.sum;
        self.count += other.count;
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.count == 0 {
            Ok(Value::Null)
        } else {
            Ok(Value::Float64(ordered_float::OrderedFloat(
                self.sum / self.count as f64,
            )))
        }
    }
}

#[derive(Default)]
pub struct MinState {
    min: Option<Value>,
}

impl AggregateState for MinState {
    fn update(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current) => {
                if value < current {
                    self.min = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        match (&self.min, &other.min) {
            (None, Some(v)) => self.min = Some(v.clone()),
            (Some(a), Some(b)) if b < a => self.min = Some(b.clone()),
            _ => {}
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.min.clone().unwrap_or(Value::Null))
    }
}

#[derive(Default)]
pub struct MaxState {
    max: Option<Value>,
}

impl AggregateState for MaxState {
    fn update(&mut self, value: &Value) -> Result<()> {
        if matches!(value, Value::Null) {
            return Ok(());
        }
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current) => {
                if value > current {
                    self.max = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        match (&self.max, &other.max) {
            (None, Some(v)) => self.max = Some(v.clone()),
            (Some(a), Some(b)) if b > a => self.max = Some(b.clone()),
            _ => {}
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.max.clone().unwrap_or(Value::Null))
    }
}

#[derive(Default)]
pub struct ArrayAggState {
    values: Vec<Value>,
}

impl AggregateState for ArrayAggState {
    fn update(&mut self, value: &Value) -> Result<()> {
        self.values.push(value.clone());
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.values.extend(other.values.iter().cloned());
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(Value::Array(self.values.clone()))
    }
}

#[derive(Default)]
pub struct StringAggState {
    values: Vec<String>,
    delimiter: String,
}

impl StringAggState {
    pub fn with_delimiter(delimiter: &str) -> Self {
        Self {
            values: Vec::new(),
            delimiter: delimiter.to_string(),
        }
    }
}

impl AggregateState for StringAggState {
    fn update(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::String(s) => self.values.push(s.clone()),
            Value::Null => {}
            v => self.values.push(v.to_string()),
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        self.values.extend(other.values.iter().cloned());
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        if self.values.is_empty() {
            Ok(Value::Null)
        } else {
            Ok(Value::String(self.values.join(&self.delimiter)))
        }
    }
}

#[derive(Default)]
pub struct AnyValueState {
    value: Option<Value>,
}

impl AggregateState for AnyValueState {
    fn update(&mut self, value: &Value) -> Result<()> {
        if self.value.is_none() && !matches!(value, Value::Null) {
            self.value = Some(value.clone());
        }
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> Result<()> {
        if self.value.is_none() {
            self.value = other.value.clone();
        }
        Ok(())
    }

    fn finalize(&self) -> Result<Value> {
        Ok(self.value.clone().unwrap_or(Value::Null))
    }
}
