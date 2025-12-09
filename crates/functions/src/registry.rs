use std::rc::Rc;

use yachtsql_core::types::Value;

use crate::aggregate::AggregateFunction;
use crate::scalar::ScalarFunction;

mod array_funcs;
mod clickhouse_array_funcs;
mod clickhouse_datetime;
mod clickhouse_higher_order;
mod conditional_funcs;
mod conversion_funcs;
mod datetime_funcs;
mod encryption_funcs;
mod extension_funcs;
mod fulltext_funcs;
mod geography_funcs;
mod geometric_funcs;
mod hstore_funcs;
mod interval_funcs;
#[cfg(feature = "json")]
mod json_funcs;
mod math_funcs;
mod network_funcs;
mod null_funcs;
mod random_funcs;
mod range_funcs;
mod regex_funcs;
mod rounding_funcs;
mod safe_arithmetic;
mod sequence_funcs;
mod statistical_aggregates;
mod string_funcs;
mod time_window_funcs;
mod ulid_funcs;
mod uuid_funcs;
mod vector_funcs;

fn month_name(month: u32) -> &'static str {
    match month {
        1 => "January",
        2 => "February",
        3 => "March",
        4 => "April",
        5 => "May",
        6 => "June",
        7 => "July",
        8 => "August",
        9 => "September",
        10 => "October",
        11 => "November",
        12 => "December",
        _ => "Invalid",
    }
}

fn month_abbr(month: u32) -> &'static str {
    match month {
        1 => "Jan",
        2 => "Feb",
        3 => "Mar",
        4 => "Apr",
        5 => "May",
        6 => "Jun",
        7 => "Jul",
        8 => "Aug",
        9 => "Sep",
        10 => "Oct",
        11 => "Nov",
        12 => "Dec",
        _ => "Inv",
    }
}

fn weekday_name(weekday: chrono::Weekday) -> &'static str {
    match weekday {
        chrono::Weekday::Mon => "Monday",
        chrono::Weekday::Tue => "Tuesday",
        chrono::Weekday::Wed => "Wednesday",
        chrono::Weekday::Thu => "Thursday",
        chrono::Weekday::Fri => "Friday",
        chrono::Weekday::Sat => "Saturday",
        chrono::Weekday::Sun => "Sunday",
    }
}

fn weekday_abbr(weekday: chrono::Weekday) -> &'static str {
    match weekday {
        chrono::Weekday::Mon => "Mon",
        chrono::Weekday::Tue => "Tue",
        chrono::Weekday::Wed => "Wed",
        chrono::Weekday::Thu => "Thu",
        chrono::Weekday::Fri => "Fri",
        chrono::Weekday::Sat => "Sat",
        chrono::Weekday::Sun => "Sun",
    }
}

fn eval_boolean_condition(value: &Value) -> yachtsql_core::error::Result<bool> {
    use yachtsql_core::error::Error;

    if value.is_null() {
        return Ok(false);
    }

    if let Some(b) = value.as_bool() {
        return Ok(b);
    }

    if let Some(i) = value.as_i64() {
        return Ok(i != 0);
    }

    if let Some(f) = value.as_f64() {
        return Ok(f != 0.0);
    }

    Err(Error::TypeMismatch {
        expected: "BOOL or numeric".to_string(),
        actual: value.data_type().to_string(),
    })
}

#[derive(Debug, Default)]
pub struct FunctionRegistry {
    scalar_functions: std::collections::HashMap<String, Rc<dyn ScalarFunction>>,
    aggregate_functions: std::collections::HashMap<String, Rc<dyn AggregateFunction>>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut registry = Self::default();
        registry.register_builtins();
        registry
    }

    fn register_builtins(&mut self) {
        string_funcs::register(self);
        conditional_funcs::register(self);
        conversion_funcs::register(self);
        null_funcs::register(self);
        datetime_funcs::register(self);
        array_funcs::register(self);
        clickhouse_array_funcs::register(self);
        clickhouse_datetime::register(self);
        clickhouse_higher_order::register(self);
        math_funcs::register(self);
        datetime_funcs::register_arithmetic(self);
        regex_funcs::register(self);
        safe_arithmetic::register(self);
        datetime_funcs::register_formatters(self);
        statistical_aggregates::register(self);
        geography_funcs::register(self);
        sequence_funcs::register(self);

        vector_funcs::register(self);
        interval_funcs::register(self);
        range_funcs::register(self);
        geometric_funcs::register(self);
        hstore_funcs::register(self);

        fulltext_funcs::register(self);

        network_funcs::register(self);

        extension_funcs::register(self);

        random_funcs::register(self);
        uuid_funcs::register(self);
        ulid_funcs::register(self);
        time_window_funcs::register(self);
        rounding_funcs::register(self);
        encryption_funcs::register(self);

        #[cfg(feature = "json")]
        json_funcs::register(self);
    }

    pub fn register_scalar(&mut self, name: String, func: Rc<dyn ScalarFunction>) {
        self.scalar_functions.insert(name.to_uppercase(), func);
    }

    pub fn register_aggregate(&mut self, name: String, func: Rc<dyn AggregateFunction>) {
        self.aggregate_functions.insert(name.to_uppercase(), func);
    }

    pub fn get_scalar(&self, name: &str) -> Option<Rc<dyn ScalarFunction>> {
        self.scalar_functions.get(&name.to_uppercase()).cloned()
    }

    pub fn get_aggregate(&self, name: &str) -> Option<Rc<dyn AggregateFunction>> {
        self.aggregate_functions.get(&name.to_uppercase()).cloned()
    }

    pub fn has_aggregate(&self, name: &str) -> bool {
        self.aggregate_functions.contains_key(&name.to_uppercase())
    }

    pub fn has_scalar(&self, name: &str) -> bool {
        self.scalar_functions.contains_key(&name.to_uppercase())
    }
}
