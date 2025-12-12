mod ascii;
mod casefold;
mod chr;
mod clickhouse_search;
mod concat;
mod ends_with;
mod format;
mod initcap;
mod left;
mod length;
mod lower;
mod ltrim;
mod pad;
mod position;
mod quote_ident;
mod quote_literal;
mod regexp_contains;
mod regexp_extract;
mod regexp_replace;
mod repeat;
mod replace;
mod reverse;
mod right;
mod rtrim;
mod split;
mod starts_with;
mod strpos;
mod substr;
mod translate;
mod trim;
mod upper;

use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_string_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "CONCAT" => Self::evaluate_concat(args, batch, row_idx),
            "TRIM" => Self::evaluate_trim(args, batch, row_idx),
            "TRIM_CHARS" => Self::evaluate_trim_chars(args, batch, row_idx),
            "LTRIM" => Self::evaluate_ltrim(args, batch, row_idx),
            "LTRIM_CHARS" => Self::evaluate_ltrim_chars(args, batch, row_idx),
            "RTRIM" => Self::evaluate_rtrim(args, batch, row_idx),
            "RTRIM_CHARS" => Self::evaluate_rtrim_chars(args, batch, row_idx),
            "UPPER" => Self::evaluate_upper(args, batch, row_idx),
            "LOWER" => Self::evaluate_lower(args, batch, row_idx),
            "REPLACE" => Self::evaluate_replace(args, batch, row_idx),
            "SUBSTR" | "SUBSTRING" => Self::evaluate_substr(args, batch, row_idx),
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                Self::evaluate_length(args, batch, row_idx)
            }
            "OCTET_LENGTH" => Self::evaluate_octet_length(args, batch, row_idx),
            "BYTE_LENGTH" => Self::evaluate_byte_length(args, batch, row_idx),
            "SPLIT" | "STRING_TO_ARRAY" => Self::evaluate_split(args, batch, row_idx),
            "SPLIT_PART" => Self::evaluate_split_part(args, batch, row_idx),
            "SPLITBYCHAR" => Self::evaluate_split_by_char(args, batch, row_idx),
            "SPLITBYSTRING" => Self::evaluate_split_by_string(args, batch, row_idx),
            "STARTS_WITH" | "STARTSWITH" => Self::evaluate_starts_with(args, batch, row_idx),
            "ENDS_WITH" | "ENDSWITH" => Self::evaluate_ends_with(args, batch, row_idx),
            "REGEXP_CONTAINS" => Self::evaluate_regexp_contains(args, batch, row_idx),
            "REGEXP_REPLACE" | "REPLACEREGEXPALL" | "REPLACEREGEXPONE" => {
                Self::evaluate_regexp_replace(args, batch, row_idx)
            }
            "REGEXP_EXTRACT" => Self::evaluate_regexp_extract(args, batch, row_idx),
            "REGEXP_EXTRACT_ALL" => Self::evaluate_regexp_extract_all(args, batch, row_idx),
            "POSITION" => Self::evaluate_position(args, batch, row_idx),
            "STRPOS" | "LOCATE" => Self::evaluate_strpos(args, batch, row_idx),
            "LEFT" => Self::evaluate_left(args, batch, row_idx),
            "RIGHT" => Self::evaluate_right(args, batch, row_idx),
            "REPEAT" => Self::evaluate_repeat(args, batch, row_idx),
            "REVERSE" => Self::evaluate_reverse(args, batch, row_idx),
            "LPAD" => Self::evaluate_lpad(args, batch, row_idx),
            "RPAD" => Self::evaluate_rpad(args, batch, row_idx),
            "ASCII" => Self::evaluate_ascii(args, batch, row_idx),
            "CHR" => Self::evaluate_chr(args, batch, row_idx),
            "INITCAP" => Self::evaluate_initcap(args, batch, row_idx),
            "TRANSLATE" => Self::evaluate_translate(args, batch, row_idx),
            "FORMAT" => Self::evaluate_format(args, batch, row_idx),
            "QUOTE_IDENT" => Self::evaluate_quote_ident(args, batch, row_idx),
            "QUOTE_LITERAL" => Self::evaluate_quote_literal(args, batch, row_idx),
            "CASEFOLD" => Self::evaluate_casefold(args, batch, row_idx),
            "BIT_COUNT" => Self::evaluate_bit_count(args, batch, row_idx),
            "GET_BIT" => Self::evaluate_get_bit(args, batch, row_idx),
            "SET_BIT" => Self::evaluate_set_bit(args, batch, row_idx),
            "POSITIONCASEINSENSITIVE" => {
                Self::evaluate_position_case_insensitive(args, batch, row_idx)
            }
            "POSITIONUTF8" => Self::evaluate_position_utf8(args, batch, row_idx),
            "POSITIONCASEINSENSITIVEUTF8" => {
                Self::evaluate_position_case_insensitive_utf8(args, batch, row_idx)
            }
            "COUNTSUBSTRINGS" => Self::evaluate_count_substrings(args, batch, row_idx),
            "COUNTSUBSTRINGSCASEINSENSITIVE" => {
                Self::evaluate_count_substrings_case_insensitive(args, batch, row_idx)
            }
            "COUNTMATCHES" => Self::evaluate_count_matches(args, batch, row_idx),
            "COUNTMATCHESCASEINSENSITIVE" => {
                Self::evaluate_count_matches_case_insensitive(args, batch, row_idx)
            }
            "HASTOKEN" => Self::evaluate_has_token(args, batch, row_idx),
            "HASTOKENCASEINSENSITIVE" => {
                Self::evaluate_has_token_case_insensitive(args, batch, row_idx)
            }
            "MATCH" => Self::evaluate_match(args, batch, row_idx),
            "MULTISEARCHANY" => Self::evaluate_multi_search_any(args, batch, row_idx),
            "MULTISEARCHFIRSTINDEX" => {
                Self::evaluate_multi_search_first_index(args, batch, row_idx)
            }
            "MULTISEARCHFIRSTPOSITION" => {
                Self::evaluate_multi_search_first_position(args, batch, row_idx)
            }
            "MULTISEARCHALLPOSITIONS" => {
                Self::evaluate_multi_search_all_positions(args, batch, row_idx)
            }
            "MULTIMATCHANY" => Self::evaluate_multi_match_any(args, batch, row_idx),
            "MULTIMATCHANYINDEX" => Self::evaluate_multi_match_any_index(args, batch, row_idx),
            "MULTIMATCHALLINDICES" => Self::evaluate_multi_match_all_indices(args, batch, row_idx),
            "EXTRACTGROUPS" => Self::evaluate_extract_groups(args, batch, row_idx),
            "NGRAMDISTANCE" => Self::evaluate_ngram_distance(args, batch, row_idx),
            "NGRAMSEARCH" => Self::evaluate_ngram_search(args, batch, row_idx),
            "SPLITBYREGEXP" => Self::evaluate_split_by_regexp(args, batch, row_idx),
            "SPLITBYWHITESPACE" => Self::evaluate_split_by_whitespace(args, batch, row_idx),
            "SPLITBYNONALPHA" => Self::evaluate_split_by_non_alpha(args, batch, row_idx),
            "ALPHATOKENS" => Self::evaluate_alpha_tokens(args, batch, row_idx),
            "TOKENS" => Self::evaluate_tokens(args, batch, row_idx),
            "NGRAMS" => Self::evaluate_ngrams(args, batch, row_idx),
            "ARRAYSTRINGCONCAT" => Self::evaluate_array_string_concat(args, batch, row_idx),
            "EXTRACTALL" => Self::evaluate_extract_all(args, batch, row_idx),
            "EXTRACTALLGROUPSHORIZONTAL" => {
                Self::evaluate_extract_all_groups_horizontal(args, batch, row_idx)
            }
            "EXTRACTALLGROUPSVERTICAL" => {
                Self::evaluate_extract_all_groups_vertical(args, batch, row_idx)
            }
            "REPLACEONE" => Self::evaluate_replace_one(args, batch, row_idx),
            "REPLACEALL" => Self::evaluate_replace_all(args, batch, row_idx),
            "TRIMLEFT" => Self::evaluate_trim_left(args, batch, row_idx),
            "TRIMRIGHT" => Self::evaluate_trim_right(args, batch, row_idx),
            "TRIMBOTH" => Self::evaluate_trim_both(args, batch, row_idx),
            "LEFTPAD" => Self::evaluate_left_pad(args, batch, row_idx),
            "RIGHTPAD" => Self::evaluate_right_pad(args, batch, row_idx),
            "REGEXPQUOTEMETA" => Self::evaluate_regexp_quote_meta(args, batch, row_idx),
            "TRANSLATEUTF8" => Self::evaluate_translate_utf8(args, batch, row_idx),
            "NORMALIZEUTF8NFC" => Self::evaluate_normalize_utf8_nfc(args, batch, row_idx),
            "NORMALIZEUTF8NFD" => Self::evaluate_normalize_utf8_nfd(args, batch, row_idx),
            "NORMALIZEUTF8NFKC" => Self::evaluate_normalize_utf8_nfkc(args, batch, row_idx),
            "NORMALIZEUTF8NFKD" => Self::evaluate_normalize_utf8_nfkd(args, batch, row_idx),
            "BTRIM" => Self::evaluate_btrim(args, batch, row_idx),
            "BIT_LENGTH" => Self::evaluate_bit_length(args, batch, row_idx),
            "CONCAT_WS" => Self::evaluate_concat_ws(args, batch, row_idx),
            "QUOTE_NULLABLE" => Self::evaluate_quote_nullable(args, batch, row_idx),
            "REGEXP_COUNT" => Self::evaluate_regexp_count(args, batch, row_idx),
            "REGEXP_INSTR" => Self::evaluate_regexp_instr(args, batch, row_idx),
            "REGEXP_SUBSTR" => Self::evaluate_regexp_substr(args, batch, row_idx),
            "PARSE_IDENT" => Self::evaluate_parse_ident(args, batch, row_idx),
            "NORMALIZE" => Self::evaluate_normalize(args, batch, row_idx),
            "IS_NORMALIZED" => Self::evaluate_is_normalized(args, batch, row_idx),
            "OVERLAY" => Self::evaluate_overlay(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "Unknown string function: {}",
                name
            ))),
        }
    }
}

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_bit_length(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("BIT_LENGTH", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(s) = val.as_str() {
            return Ok(Value::int64((s.len() * 8) as i64));
        }
        if let Some(b) = val.as_bytes() {
            return Ok(Value::int64((b.len() * 8) as i64));
        }
        Err(Error::TypeMismatch {
            expected: "STRING or BYTES".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_btrim(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query("BTRIM requires 1 or 2 arguments"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        let chars_to_trim = if args.len() == 2 {
            let trim_arg = Self::evaluate_expr(&args[1], batch, row_idx)?;
            if trim_arg.is_null() {
                return Ok(Value::null());
            }
            trim_arg
                .as_str()
                .ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: trim_arg.data_type().to_string(),
                })?
                .to_string()
        } else {
            " ".to_string()
        };
        let result = s.trim_matches(|c: char| chars_to_trim.contains(c));
        Ok(Value::string(result.to_string()))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_concat_ws(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "CONCAT_WS requires at least 1 argument",
            ));
        }
        let sep_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if sep_val.is_null() {
            return Ok(Value::null());
        }
        let separator = sep_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: sep_val.data_type().to_string(),
        })?;
        let mut parts = Vec::new();
        for arg in &args[1..] {
            let val = Self::evaluate_expr(arg, batch, row_idx)?;
            if !val.is_null() {
                if let Some(s) = val.as_str() {
                    parts.push(s.to_string());
                } else {
                    parts.push(val.to_string());
                }
            }
        }
        Ok(Value::string(parts.join(separator)))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_quote_nullable(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("QUOTE_NULLABLE", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::string("NULL".to_string()));
        }
        let s = if let Some(s) = val.as_str() {
            s.to_string()
        } else {
            val.to_string()
        };
        let escaped = s.replace('\'', "''");
        Ok(Value::string(format!("'{}'", escaped)))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_regexp_count(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("REGEXP_COUNT", args, 2)?;
        let string_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let pattern_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if string_val.is_null() || pattern_val.is_null() {
            return Ok(Value::null());
        }
        let string = string_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: string_val.data_type().to_string(),
        })?;
        let pattern = pattern_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pattern_val.data_type().to_string(),
        })?;
        let re = regex::Regex::new(pattern)
            .map_err(|e| Error::invalid_query(format!("Invalid regex pattern: {}", e)))?;
        let count = re.find_iter(string).count();
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_regexp_instr(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("REGEXP_INSTR", args, 2)?;
        let string_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let pattern_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if string_val.is_null() || pattern_val.is_null() {
            return Ok(Value::null());
        }
        let string = string_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: string_val.data_type().to_string(),
        })?;
        let pattern = pattern_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pattern_val.data_type().to_string(),
        })?;
        let re = regex::Regex::new(pattern)
            .map_err(|e| Error::invalid_query(format!("Invalid regex pattern: {}", e)))?;
        if let Some(mat) = re.find(string) {
            Ok(Value::int64((mat.start() + 1) as i64))
        } else {
            Ok(Value::int64(0))
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_regexp_substr(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("REGEXP_SUBSTR", args, 2)?;
        let string_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let pattern_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if string_val.is_null() || pattern_val.is_null() {
            return Ok(Value::null());
        }
        let string = string_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: string_val.data_type().to_string(),
        })?;
        let pattern = pattern_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pattern_val.data_type().to_string(),
        })?;
        let re = regex::Regex::new(pattern)
            .map_err(|e| Error::invalid_query(format!("Invalid regex pattern: {}", e)))?;
        if let Some(mat) = re.find(string) {
            Ok(Value::string(mat.as_str().to_string()))
        } else {
            Ok(Value::null())
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_regexp_extract_all(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("REGEXP_EXTRACT_ALL", args, 2)?;
        let string_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let pattern_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if string_val.is_null() || pattern_val.is_null() {
            return Ok(Value::null());
        }
        let string = string_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: string_val.data_type().to_string(),
        })?;
        let pattern = pattern_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pattern_val.data_type().to_string(),
        })?;
        let re = regex::Regex::new(pattern)
            .map_err(|e| Error::invalid_query(format!("Invalid regex pattern: {}", e)))?;
        let results: Vec<Value> = re
            .captures_iter(string)
            .map(|caps| {
                if caps.len() > 1 {
                    Value::string(caps.get(1).unwrap().as_str().to_string())
                } else {
                    Value::string(caps.get(0).unwrap().as_str().to_string())
                }
            })
            .collect();
        Ok(Value::array(results))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_parse_ident(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("PARSE_IDENT", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        let parts: Vec<Value> = s
            .split('.')
            .map(|part| {
                let trimmed = part.trim();
                let unquoted =
                    if trimmed.starts_with('"') && trimmed.ends_with('"') && trimmed.len() >= 2 {
                        trimmed[1..trimmed.len() - 1].replace("\"\"", "\"")
                    } else {
                        trimmed.to_lowercase()
                    };
                Value::string(unquoted)
            })
            .collect();
        Ok(Value::array(parts))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_normalize(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query("NORMALIZE requires 1 or 2 arguments"));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;
        Ok(Value::string(s.to_string()))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_is_normalized(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() || args.len() > 2 {
            return Err(Error::invalid_query(
                "IS_NORMALIZED requires 1 or 2 arguments",
            ));
        }
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        if val.as_str().is_some() {
            Ok(Value::bool_val(true))
        } else {
            Err(Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: val.data_type().to_string(),
            })
        }
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_overlay(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 3 || args.len() > 4 {
            return Err(Error::invalid_query("OVERLAY requires 3 or 4 arguments"));
        }
        let string_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let replacement_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let start_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        if string_val.is_null() || replacement_val.is_null() || start_val.is_null() {
            return Ok(Value::null());
        }
        let string = string_val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: string_val.data_type().to_string(),
        })?;
        let replacement = replacement_val
            .as_str()
            .ok_or_else(|| Error::TypeMismatch {
                expected: "STRING".to_string(),
                actual: replacement_val.data_type().to_string(),
            })?;
        let start = start_val.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: start_val.data_type().to_string(),
        })?;
        let length = if args.len() == 4 {
            let length_val = Self::evaluate_expr(&args[3], batch, row_idx)?;
            if length_val.is_null() {
                return Ok(Value::null());
            }
            length_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                expected: "INT64".to_string(),
                actual: length_val.data_type().to_string(),
            })? as usize
        } else {
            replacement.chars().count()
        };
        let chars: Vec<char> = string.chars().collect();
        let start_idx = (start - 1).max(0) as usize;
        let mut result = String::new();
        for (i, ch) in chars.iter().enumerate() {
            if i < start_idx {
                result.push(*ch);
            }
        }
        result.push_str(replacement);
        let skip_end = start_idx + length;
        for (i, ch) in chars.iter().enumerate() {
            if i >= skip_end {
                result.push(*ch);
            }
        }
        Ok(Value::string(result))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_bit_count(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("BIT_COUNT", args, 1)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        yachtsql_functions::scalar::eval_bit_count(&val)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_get_bit(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("GET_BIT", args, 2)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let position = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let pos = position.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: position.data_type().to_string(),
        })?;
        yachtsql_functions::scalar::eval_get_bit(&val, pos)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_set_bit(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("SET_BIT", args, 3)?;
        let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let position = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let new_value = Self::evaluate_expr(&args[2], batch, row_idx)?;
        let pos = position.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: position.data_type().to_string(),
        })?;
        let new_val = new_value.as_i64().ok_or_else(|| Error::TypeMismatch {
            expected: "INT64".to_string(),
            actual: new_value.data_type().to_string(),
        })?;
        yachtsql_functions::scalar::eval_set_bit(&val, pos, new_val)
    }
}
