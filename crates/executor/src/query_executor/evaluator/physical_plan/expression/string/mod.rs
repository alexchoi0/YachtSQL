mod ascii;
mod casefold;
mod chr;
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
            "SPLIT" | "STRING_TO_ARRAY" => Self::evaluate_split(args, batch, row_idx),
            "SPLIT_PART" => Self::evaluate_split_part(args, batch, row_idx),
            "SPLITBYCHAR" | "SPLITBYSTRING" => Self::evaluate_split(args, batch, row_idx),
            "STARTS_WITH" | "STARTSWITH" => Self::evaluate_starts_with(args, batch, row_idx),
            "ENDS_WITH" | "ENDSWITH" => Self::evaluate_ends_with(args, batch, row_idx),
            "REGEXP_CONTAINS" => Self::evaluate_regexp_contains(args, batch, row_idx),
            "REGEXP_REPLACE" | "REPLACEREGEXPALL" | "REPLACEREGEXPONE" => {
                Self::evaluate_regexp_replace(args, batch, row_idx)
            }
            "REGEXP_EXTRACT" => Self::evaluate_regexp_extract(args, batch, row_idx),
            "POSITION" => Self::evaluate_position(args, batch, row_idx),
            "STRPOS" => Self::evaluate_strpos(args, batch, row_idx),
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
            _ => Err(Error::unsupported_feature(format!(
                "Unknown string function: {}",
                name
            ))),
        }
    }
}
