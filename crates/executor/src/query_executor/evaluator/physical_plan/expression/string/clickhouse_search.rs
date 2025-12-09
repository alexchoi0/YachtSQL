use regex::Regex;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_position_case_insensitive(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("POSITIONCASEINSENSITIVE", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needle = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let haystack_lower = haystack.to_lowercase();
        let needle_lower = needle.to_lowercase();

        let pos = haystack_lower
            .find(&needle_lower)
            .map(|i| i + 1)
            .unwrap_or(0);
        Ok(Value::int64(pos as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_position_utf8(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("POSITIONUTF8", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needle = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let pos = haystack
            .char_indices()
            .enumerate()
            .find(|(_, (byte_idx, _))| haystack[*byte_idx..].starts_with(needle))
            .map(|(char_idx, _)| char_idx + 1)
            .unwrap_or(0);
        Ok(Value::int64(pos as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_position_case_insensitive_utf8(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("POSITIONCASEINSENSITIVEUTF8", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needle = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let haystack_lower = haystack.to_lowercase();
        let needle_lower = needle.to_lowercase();

        let pos = haystack_lower
            .char_indices()
            .enumerate()
            .find(|(_, (byte_idx, _))| haystack_lower[*byte_idx..].starts_with(&needle_lower))
            .map(|(char_idx, _)| char_idx + 1)
            .unwrap_or(0);
        Ok(Value::int64(pos as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_count_substrings(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("COUNTSUBSTRINGS", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needle = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        if needle.is_empty() {
            return Ok(Value::int64(0));
        }

        let count = haystack.matches(needle).count();
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_count_substrings_case_insensitive(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("COUNTSUBSTRINGSCASEINSENSITIVE", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needle = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        if needle.is_empty() {
            return Ok(Value::int64(0));
        }

        let haystack_lower = haystack.to_lowercase();
        let needle_lower = needle.to_lowercase();
        let count = haystack_lower.matches(&needle_lower).count();
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_count_matches(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("COUNTMATCHES", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let pattern = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let re = Regex::new(pattern).map_err(|e| Error::invalid_query(e.to_string()))?;
        let count = re.find_iter(haystack).count();
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_count_matches_case_insensitive(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("COUNTMATCHESCASEINSENSITIVE", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let pattern = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let pattern_ci = format!("(?i){}", pattern);
        let re = Regex::new(&pattern_ci).map_err(|e| Error::invalid_query(e.to_string()))?;
        let count = re.find_iter(haystack).count();
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_has_token(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("HASTOKEN", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let token = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let tokens: Vec<&str> = haystack.split(|c: char| !c.is_alphanumeric()).collect();
        let found = tokens.contains(&token);
        Ok(Value::int64(if found { 1 } else { 0 }))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_has_token_case_insensitive(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("HASTOKENCASEINSENSITIVE", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let token = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let haystack_lower = haystack.to_lowercase();
        let token_lower = token.to_lowercase();
        let tokens: Vec<&str> = haystack_lower
            .split(|c: char| !c.is_alphanumeric())
            .collect();
        let found = tokens.iter().any(|t| *t == token_lower);
        Ok(Value::int64(if found { 1 } else { 0 }))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_match(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MATCH", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let pattern = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let re = Regex::new(pattern).map_err(|e| Error::invalid_query(e.to_string()))?;
        let matched = re.is_match(haystack);
        Ok(Value::int64(if matched { 1 } else { 0 }))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_search_any(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTISEARCHANY", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needles = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        for needle in needles {
            if let Some(s) = needle.as_str() {
                if haystack.contains(s) {
                    return Ok(Value::int64(1));
                }
            }
        }
        Ok(Value::int64(0))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_search_first_index(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTISEARCHFIRSTINDEX", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needles = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let mut first_pos = usize::MAX;
        let mut first_idx = 0i64;
        for (i, needle) in needles.iter().enumerate() {
            if let Some(s) = needle.as_str() {
                if let Some(pos) = haystack.find(s) {
                    if pos < first_pos {
                        first_pos = pos;
                        first_idx = (i + 1) as i64;
                    }
                }
            }
        }
        Ok(Value::int64(first_idx))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_search_first_position(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTISEARCHFIRSTPOSITION", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needles = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let mut first_pos = usize::MAX;
        for needle in needles {
            if let Some(s) = needle.as_str() {
                if let Some(pos) = haystack.find(s) {
                    if pos < first_pos {
                        first_pos = pos;
                    }
                }
            }
        }
        let result = if first_pos == usize::MAX {
            0
        } else {
            (first_pos + 1) as i64
        };
        Ok(Value::int64(result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_search_all_positions(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTISEARCHALLPOSITIONS", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needles = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let mut result: Vec<Value> = Vec::new();
        for needle in needles {
            if let Some(s) = needle.as_str() {
                let mut positions: Vec<Value> = Vec::new();
                let mut start = 0;
                while let Some(pos) = haystack[start..].find(s) {
                    positions.push(Value::int64((start + pos + 1) as i64));
                    start = start + pos + 1;
                }
                result.push(Value::array(positions));
            } else {
                result.push(Value::array(vec![]));
            }
        }
        Ok(Value::array(result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_match_any(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTIMATCHANY", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let patterns = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        for pattern in patterns {
            if let Some(p) = pattern.as_str() {
                let re = Regex::new(p).map_err(|e| Error::invalid_query(e.to_string()))?;
                if re.is_match(haystack) {
                    return Ok(Value::int64(1));
                }
            }
        }
        Ok(Value::int64(0))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_match_any_index(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTIMATCHANYINDEX", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let patterns = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        for (i, pattern) in patterns.iter().enumerate() {
            if let Some(p) = pattern.as_str() {
                let re = Regex::new(p).map_err(|e| Error::invalid_query(e.to_string()))?;
                if re.is_match(haystack) {
                    return Ok(Value::int64((i + 1) as i64));
                }
            }
        }
        Ok(Value::int64(0))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_multi_match_all_indices(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("MULTIMATCHALLINDICES", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let patterns = values[1].as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let mut indices: Vec<Value> = Vec::new();
        for (i, pattern) in patterns.iter().enumerate() {
            if let Some(p) = pattern.as_str() {
                let re = Regex::new(p).map_err(|e| Error::invalid_query(e.to_string()))?;
                if re.is_match(haystack) {
                    indices.push(Value::int64((i + 1) as i64));
                }
            }
        }
        Ok(Value::array(indices))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_extract_groups(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("EXTRACTGROUPS", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let pattern = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        let re = Regex::new(pattern).map_err(|e| Error::invalid_query(e.to_string()))?;
        if let Some(captures) = re.captures(haystack) {
            let groups: Vec<Value> = captures
                .iter()
                .skip(1)
                .map(|m| {
                    m.map(|m| Value::string(m.as_str().into()))
                        .unwrap_or(Value::null())
                })
                .collect();
            Ok(Value::array(groups))
        } else {
            Ok(Value::array(vec![]))
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_ngram_distance(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("NGRAMDISTANCE", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let s1 = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let s2 = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        fn get_ngrams(s: &str, n: usize) -> std::collections::HashSet<String> {
            if s.len() < n {
                let mut set = std::collections::HashSet::new();
                set.insert(s.to_string());
                return set;
            }
            (0..=s.len() - n).map(|i| s[i..i + n].to_string()).collect()
        }

        let ngrams1 = get_ngrams(s1, 4);
        let ngrams2 = get_ngrams(s2, 4);

        if ngrams1.is_empty() && ngrams2.is_empty() {
            return Ok(Value::float64(0.0));
        }

        let intersection = ngrams1.intersection(&ngrams2).count();
        let union = ngrams1.union(&ngrams2).count();

        let distance = 1.0 - (intersection as f64 / union as f64);
        Ok(Value::float64(distance))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_ngram_search(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("NGRAMSEARCH", args, 2)?;
        let values = Self::evaluate_args(args, batch, row_idx)?;
        if values[0].is_null() || values[1].is_null() {
            return Ok(Value::null());
        }

        let haystack = values[0].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[0].data_type().to_string(),
        })?;
        let needle = values[1].as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: values[1].data_type().to_string(),
        })?;

        fn get_ngrams(s: &str, n: usize) -> std::collections::HashSet<String> {
            if s.len() < n {
                let mut set = std::collections::HashSet::new();
                set.insert(s.to_string());
                return set;
            }
            (0..=s.len() - n).map(|i| s[i..i + n].to_string()).collect()
        }

        let needle_ngrams = get_ngrams(needle, 4);
        if needle_ngrams.is_empty() {
            return Ok(Value::float64(0.0));
        }

        let mut max_score = 0.0f64;
        for window_start in 0..haystack.len().saturating_sub(needle.len()) + 1 {
            let window_end = (window_start + needle.len()).min(haystack.len());
            let window = &haystack[window_start..window_end];
            let window_ngrams = get_ngrams(window, 4);
            let intersection = needle_ngrams.intersection(&window_ngrams).count();
            let score = intersection as f64 / needle_ngrams.len() as f64;
            if score > max_score {
                max_score = score;
            }
        }

        Ok(Value::float64(max_score))
    }
}
