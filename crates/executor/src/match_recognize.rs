use std::collections::HashMap;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchMode {
    AllRowsPerMatch,
    OneRowPerMatch,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AfterMatchSkip {
    ToNextRow,
    PastLastRow,
    ToFirst(String),
    ToLast(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum RowPatternNavigation {
    Prev {
        expr: Box<PatternExpression>,
        offset: usize,
    },
    Next {
        expr: Box<PatternExpression>,
        offset: usize,
    },
    First(Box<PatternExpression>),
    Last(Box<PatternExpression>),
}

#[derive(Debug, Clone, PartialEq)]
pub enum PatternExpression {
    Column(String),
    Literal(Value),
    BinaryOp {
        left: Box<PatternExpression>,
        op: String,
        right: Box<PatternExpression>,
    },
    Navigation(RowPatternNavigation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PatternQuantifier {
    One,
    ZeroOrOne,
    ZeroOrMore,
    OneOrMore,
    Exactly(usize),
    Range { min: usize, max: usize },
}

#[derive(Debug, Clone)]
pub struct PatternVariable {
    pub name: String,
    pub condition: PatternExpression,
}

#[derive(Debug, Clone)]
pub struct PatternElement {
    pub variable: String,
    pub quantifier: PatternQuantifier,
}

#[derive(Debug, Clone)]
pub struct RowPattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone)]
pub struct PatternMeasure {
    pub name: String,
    pub expr: PatternExpression,
}

#[derive(Debug, Clone)]
pub struct MatchRecognizeClause {
    pub partition_by: Vec<String>,
    pub order_by: Vec<String>,
    pub measures: Vec<PatternMeasure>,
    pub mode: MatchMode,
    pub after_match_skip: AfterMatchSkip,
    pub pattern: RowPattern,
    pub definitions: Vec<PatternVariable>,
}

#[derive(Debug, Clone)]
pub struct PatternMatch {
    pub start_index: usize,
    pub end_index: usize,
    pub assignments: HashMap<usize, String>,
    pub measures: HashMap<String, Value>,
}

pub struct PatternMatcher {
    clause: MatchRecognizeClause,
}

impl PatternMatcher {
    pub fn new(clause: MatchRecognizeClause) -> Self {
        Self { clause }
    }

    pub fn find_matches(&self, rows: &[HashMap<String, Value>]) -> Result<Vec<PatternMatch>> {
        let mut matches = Vec::new();
        let mut current_index = 0;

        while current_index < rows.len() {
            if let Some(pattern_match) = self.try_match_at(rows, current_index)? {
                matches.push(pattern_match.clone());

                current_index = match &self.clause.after_match_skip {
                    AfterMatchSkip::ToNextRow => current_index + 1,
                    AfterMatchSkip::PastLastRow => pattern_match.end_index + 1,
                    AfterMatchSkip::ToFirst(var) => self
                        .find_first_occurrence(&pattern_match, var)
                        .unwrap_or(pattern_match.end_index + 1),
                    AfterMatchSkip::ToLast(var) => self
                        .find_last_occurrence(&pattern_match, var)
                        .unwrap_or(pattern_match.end_index + 1),
                };
            } else {
                current_index += 1;
            }
        }

        Ok(matches)
    }

    fn try_match_at(
        &self,
        rows: &[HashMap<String, Value>],
        start_index: usize,
    ) -> Result<Option<PatternMatch>> {
        let mut current_index = start_index;
        let mut assignments = HashMap::new();

        for element in &self.clause.pattern.elements {
            let matched =
                self.match_element(rows, &mut current_index, element, &mut assignments)?;

            if !matched {
                return Ok(None);
            }
        }

        let measures = self.compute_measures(rows, &assignments)?;

        Ok(Some(PatternMatch {
            start_index,
            end_index: current_index - 1,
            assignments,
            measures,
        }))
    }

    fn match_element(
        &self,
        rows: &[HashMap<String, Value>],
        current_index: &mut usize,
        element: &PatternElement,
        assignments: &mut HashMap<usize, String>,
    ) -> Result<bool> {
        let variable = self.find_variable(&element.variable)?;
        let mut match_count = 0;

        match &element.quantifier {
            PatternQuantifier::One => {
                if *current_index < rows.len()
                    && self.evaluate_condition(&rows[*current_index], &variable.condition)?
                {
                    assignments.insert(*current_index, element.variable.clone());
                    *current_index += 1;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            PatternQuantifier::ZeroOrOne => {
                if *current_index < rows.len()
                    && self.evaluate_condition(&rows[*current_index], &variable.condition)?
                {
                    assignments.insert(*current_index, element.variable.clone());
                    *current_index += 1;
                }
                Ok(true)
            }
            PatternQuantifier::ZeroOrMore => {
                while *current_index < rows.len()
                    && self.evaluate_condition(&rows[*current_index], &variable.condition)?
                {
                    assignments.insert(*current_index, element.variable.clone());
                    *current_index += 1;
                    match_count += 1;
                }
                Ok(true)
            }
            PatternQuantifier::OneOrMore => {
                while *current_index < rows.len()
                    && self.evaluate_condition(&rows[*current_index], &variable.condition)?
                {
                    assignments.insert(*current_index, element.variable.clone());
                    *current_index += 1;
                    match_count += 1;
                }
                Ok(match_count >= 1)
            }
            PatternQuantifier::Exactly(n) => {
                for _ in 0..*n {
                    if *current_index >= rows.len()
                        || !self.evaluate_condition(&rows[*current_index], &variable.condition)?
                    {
                        return Ok(false);
                    }
                    assignments.insert(*current_index, element.variable.clone());
                    *current_index += 1;
                }
                Ok(true)
            }
            PatternQuantifier::Range { min, max } => {
                while *current_index < rows.len()
                    && match_count < *max
                    && self.evaluate_condition(&rows[*current_index], &variable.condition)?
                {
                    assignments.insert(*current_index, element.variable.clone());
                    *current_index += 1;
                    match_count += 1;
                }
                Ok(match_count >= *min)
            }
        }
    }

    fn find_variable(&self, name: &str) -> Result<&PatternVariable> {
        self.clause
            .definitions
            .iter()
            .find(|v| v.name == name)
            .ok_or_else(|| Error::invalid_query(format!("Pattern variable not defined: {}", name)))
    }

    fn evaluate_condition(
        &self,
        row: &HashMap<String, Value>,
        expr: &PatternExpression,
    ) -> Result<bool> {
        let value = self.evaluate_expression(row, expr)?;

        if let Some(b) = value.as_bool() {
            Ok(b)
        } else if value.is_null() {
            Ok(false)
        } else {
            Err(Error::type_mismatch("BOOLEAN", &value.data_type()))
        }
    }

    fn evaluate_expression(
        &self,
        row: &HashMap<String, Value>,
        expr: &PatternExpression,
    ) -> Result<Value> {
        match expr {
            PatternExpression::Column(name) => Ok(row.get(name).cloned().unwrap_or(Value::null())),
            PatternExpression::Literal(val) => Ok(val.clone()),
            PatternExpression::BinaryOp { left, op, right } => {
                let left_val = self.evaluate_expression(row, left)?;
                let right_val = self.evaluate_expression(row, right)?;
                self.apply_binary_op(&left_val, op, &right_val)
            }
            PatternExpression::Navigation(_) => Ok(Value::null()),
        }
    }

    fn apply_binary_op(&self, left: &Value, op: &str, right: &Value) -> Result<Value> {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::bool_val(match op {
                ">" => l > r,
                "<" => l < r,
                "=" => l == r,
                ">=" => l >= r,
                "<=" => l <= r,
                _ => {
                    return Err(Error::invalid_query(format!(
                        "Unsupported operator: {}",
                        op
                    )));
                }
            }));
        }

        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(Value::bool_val(match op {
                ">" => l > r,
                "<" => l < r,
                "=" => (l - r).abs() < f64::EPSILON,
                ">=" => l >= r,
                "<=" => l <= r,
                _ => {
                    return Err(Error::invalid_query(format!(
                        "Unsupported operator: {}",
                        op
                    )));
                }
            }));
        }

        Err(Error::invalid_query(format!(
            "Unsupported binary operation: {} {} {}",
            left.data_type(),
            op,
            right.data_type()
        )))
    }

    fn compute_measures(
        &self,
        _rows: &[HashMap<String, Value>],
        _assignments: &HashMap<usize, String>,
    ) -> Result<HashMap<String, Value>> {
        Ok(HashMap::new())
    }

    fn find_first_occurrence(&self, pattern_match: &PatternMatch, var: &str) -> Option<usize> {
        pattern_match
            .assignments
            .iter()
            .filter(|(_, v)| *v == var)
            .map(|(idx, _)| *idx)
            .min()
    }

    fn find_last_occurrence(&self, pattern_match: &PatternMatch, var: &str) -> Option<usize> {
        pattern_match
            .assignments
            .iter()
            .filter(|(_, v)| *v == var)
            .map(|(idx, _)| *idx)
            .max()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_pattern_match() {
        let pattern = RowPattern {
            elements: vec![
                PatternElement {
                    variable: "A".to_string(),
                    quantifier: PatternQuantifier::One,
                },
                PatternElement {
                    variable: "B".to_string(),
                    quantifier: PatternQuantifier::One,
                },
                PatternElement {
                    variable: "C".to_string(),
                    quantifier: PatternQuantifier::One,
                },
            ],
        };

        let clause = MatchRecognizeClause {
            partition_by: vec![],
            order_by: vec!["time".to_string()],
            measures: vec![],
            mode: MatchMode::OneRowPerMatch,
            after_match_skip: AfterMatchSkip::PastLastRow,
            pattern,
            definitions: vec![
                PatternVariable {
                    name: "A".to_string(),
                    condition: PatternExpression::Literal(Value::bool_val(true)),
                },
                PatternVariable {
                    name: "B".to_string(),
                    condition: PatternExpression::Literal(Value::bool_val(true)),
                },
                PatternVariable {
                    name: "C".to_string(),
                    condition: PatternExpression::Literal(Value::bool_val(true)),
                },
            ],
        };

        let matcher = PatternMatcher::new(clause);

        let rows = vec![
            {
                let mut row = HashMap::new();
                row.insert("price".to_string(), Value::int64(10));
                row
            },
            {
                let mut row = HashMap::new();
                row.insert("price".to_string(), Value::int64(20));
                row
            },
            {
                let mut row = HashMap::new();
                row.insert("price".to_string(), Value::int64(30));
                row
            },
        ];

        let matches = matcher.find_matches(&rows).unwrap();
        assert_eq!(matches.len(), 1);
        assert_eq!(matches[0].start_index, 0);
        assert_eq!(matches[0].end_index, 2);
    }

    #[test]
    fn test_zero_or_more_quantifier() {
        let pattern = RowPattern {
            elements: vec![PatternElement {
                variable: "A".to_string(),
                quantifier: PatternQuantifier::ZeroOrMore,
            }],
        };

        let clause = MatchRecognizeClause {
            partition_by: vec![],
            order_by: vec![],
            measures: vec![],
            mode: MatchMode::OneRowPerMatch,
            after_match_skip: AfterMatchSkip::PastLastRow,
            pattern,
            definitions: vec![PatternVariable {
                name: "A".to_string(),
                condition: PatternExpression::Literal(Value::bool_val(true)),
            }],
        };

        let matcher = PatternMatcher::new(clause);

        let rows = vec![
            {
                let mut row = HashMap::new();
                row.insert("value".to_string(), Value::int64(1));
                row
            },
            {
                let mut row = HashMap::new();
                row.insert("value".to_string(), Value::int64(2));
                row
            },
        ];

        let matches = matcher.find_matches(&rows).unwrap();
        assert!(!matches.is_empty());
    }
}
