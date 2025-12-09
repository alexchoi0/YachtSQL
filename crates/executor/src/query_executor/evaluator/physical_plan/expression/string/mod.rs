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
            _ => Err(Error::unsupported_feature(format!(
                "Unknown string function: {}",
                name
            ))),
        }
    }
}

impl ProjectionWithExprExec {
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
