use sqlparser::ast::ObjectName;
use sqlparser::tokenizer::Token;
use yachtsql_core::error::{Error, Result};

use super::helpers::ParserHelpers;
use crate::validator::{ClickHouseTtlOperation, CustomStatement};

#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseIndexType {
    MinMax,
    Set {
        max_rows: Option<u64>,
    },
    BloomFilter {
        false_positive: Option<f64>,
    },
    NgramBF {
        ngram_size: u64,
        filter_size: u64,
        hash_count: u64,
        seed: u64,
    },
    TokenBF {
        filter_size: u64,
        hash_count: u64,
        seed: u64,
    },
    Inverted,
    FullText,
    Annoy {
        distance: String,
        num_trees: u64,
    },
    Usearch {
        distance: String,
    },
    Hypothesis,
}

pub struct ClickHouseParser;

impl ClickHouseParser {
    pub fn parse_alter_column_codec(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return Ok(None);
        }

        let table_name = Self::parse_qualified_name(tokens, &mut idx)?;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "MODIFY") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "COLUMN") {
            return Ok(None);
        }

        let column_name = Self::parse_identifier(tokens, &mut idx)?;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CODEC") {
            return Ok(None);
        }

        let codec_args = Self::collect_remaining_parens(tokens, &mut idx)?;

        Ok(Some(CustomStatement::ClickHouseAlterColumnCodec {
            table_name,
            column_name,
            codec: codec_args,
        }))
    }

    pub fn parse_alter_table_ttl(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return Ok(None);
        }

        let table_name = Self::parse_qualified_name(tokens, &mut idx)?;

        let operation = if ParserHelpers::consume_keyword(tokens, &mut idx, "MODIFY") {
            if !ParserHelpers::expect_keyword(tokens, &mut idx, "TTL") {
                return Ok(None);
            }
            let expr = Self::collect_remaining_tokens(tokens, &mut idx);
            ClickHouseTtlOperation::Modify { expression: expr }
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "REMOVE") {
            if !ParserHelpers::expect_keyword(tokens, &mut idx, "TTL") {
                return Ok(None);
            }
            ClickHouseTtlOperation::Remove
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MATERIALIZE") {
            if !ParserHelpers::expect_keyword(tokens, &mut idx, "TTL") {
                return Ok(None);
            }
            ClickHouseTtlOperation::Materialize
        } else {
            return Ok(None);
        };

        Ok(Some(CustomStatement::ClickHouseAlterTableTtl {
            table_name,
            operation,
        }))
    }

    pub fn is_clickhouse_alter_table_ttl(tokens: &[&Token]) -> bool {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return false;
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return false;
        }

        for token in tokens.iter().skip(idx) {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("TTL")
            {
                for t in tokens.iter().skip(idx) {
                    if let Token::Word(w) = t
                        && (w.value.eq_ignore_ascii_case("MODIFY")
                            || w.value.eq_ignore_ascii_case("REMOVE")
                            || w.value.eq_ignore_ascii_case("MATERIALIZE"))
                    {
                        return true;
                    }
                }
            }
        }
        false
    }

    fn collect_remaining_tokens(tokens: &[&Token], idx: &mut usize) -> String {
        let mut result = String::new();

        while *idx < tokens.len() {
            match tokens.get(*idx) {
                Some(Token::Word(w)) => {
                    if !result.is_empty() && !result.ends_with(' ') {
                        result.push(' ');
                    }
                    result.push_str(&w.value);
                    *idx += 1;
                }
                Some(Token::Number(n, _)) => {
                    if !result.is_empty() && !result.ends_with(' ') {
                        result.push(' ');
                    }
                    result.push_str(n);
                    *idx += 1;
                }
                Some(Token::Plus) => {
                    result.push_str(" + ");
                    *idx += 1;
                }
                Some(Token::Minus) => {
                    result.push_str(" - ");
                    *idx += 1;
                }
                Some(Token::LParen) => {
                    result.push('(');
                    *idx += 1;
                }
                Some(Token::RParen) => {
                    result.push(')');
                    *idx += 1;
                }
                Some(Token::Comma) => {
                    result.push(',');
                    *idx += 1;
                }
                Some(Token::Period) => {
                    result.push('.');
                    *idx += 1;
                }
                Some(Token::SingleQuotedString(s)) => {
                    result.push('\'');
                    result.push_str(s);
                    result.push('\'');
                    *idx += 1;
                }
                _ => {
                    *idx += 1;
                }
            }
        }

        result.trim().to_string()
    }

    pub fn is_clickhouse_alter_column_codec(tokens: &[&Token]) -> bool {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return false;
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return false;
        }

        for token in tokens.iter().skip(idx) {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("CODEC")
            {
                let mut has_modify_column = false;
                for t in tokens.iter().skip(idx) {
                    if let Token::Word(w) = t
                        && w.value.eq_ignore_ascii_case("MODIFY")
                    {
                        has_modify_column = true;
                    }
                }
                return has_modify_column;
            }
        }
        false
    }

    fn collect_remaining_parens(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        let mut result = String::new();
        let mut paren_depth = 0;

        while *idx < tokens.len() {
            match tokens.get(*idx) {
                Some(Token::LParen) => {
                    paren_depth += 1;
                    result.push('(');
                    *idx += 1;
                }
                Some(Token::RParen) => {
                    result.push(')');
                    paren_depth -= 1;
                    *idx += 1;
                    if paren_depth == 0 {
                        break;
                    }
                }
                Some(Token::Comma) => {
                    result.push(',');
                    *idx += 1;
                }
                Some(Token::Word(w)) => {
                    if !result.is_empty()
                        && !result.ends_with('(')
                        && !result.ends_with(',')
                        && !result.ends_with(' ')
                    {
                        result.push(' ');
                    }
                    result.push_str(&w.value);
                    *idx += 1;
                }
                Some(Token::Number(n, _)) => {
                    result.push_str(n);
                    *idx += 1;
                }
                _ => {
                    *idx += 1;
                }
            }
        }

        Ok(result)
    }

    pub fn parse_create_index(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "INDEX") {
            return Ok(None);
        }

        let if_not_exists = if ParserHelpers::consume_keyword(tokens, &mut idx, "IF") {
            if !ParserHelpers::expect_keyword(tokens, &mut idx, "NOT") {
                return Err(Error::parse_error("Expected NOT after IF".to_string()));
            }
            if !ParserHelpers::expect_keyword(tokens, &mut idx, "EXISTS") {
                return Err(Error::parse_error("Expected EXISTS after NOT".to_string()));
            }
            true
        } else {
            false
        };

        let index_name = Self::parse_identifier(tokens, &mut idx)?;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ON") {
            return Err(Error::parse_error(
                "Expected ON after index name".to_string(),
            ));
        }

        let table_name = Self::parse_qualified_name(tokens, &mut idx)?;

        let columns = Self::parse_column_list(tokens, &mut idx)?;

        if !ParserHelpers::consume_keyword(tokens, &mut idx, "TYPE") {
            return Ok(None);
        }

        let index_type = Self::parse_index_type(tokens, &mut idx)?;

        let granularity = if ParserHelpers::consume_keyword(tokens, &mut idx, "GRANULARITY") {
            Some(Self::parse_number(tokens, &mut idx)?)
        } else {
            None
        };

        Ok(Some(CustomStatement::ClickHouseCreateIndex {
            if_not_exists,
            index_name,
            table_name,
            columns,
            index_type,
            granularity,
        }))
    }

    pub fn parse_quota(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseQuota {
            statement: sql.to_string(),
        })
    }

    pub fn is_quota_statement(tokens: &[&Token]) -> bool {
        for token in tokens.iter() {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("QUOTA")
            {
                return true;
            }
        }
        false
    }

    pub fn parse_row_policy(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseRowPolicy {
            statement: sql.to_string(),
        })
    }

    pub fn is_row_policy_statement(tokens: &[&Token]) -> bool {
        let mut found_row = false;
        let mut found_policy = false;
        for token in tokens.iter() {
            if let Token::Word(w) = token {
                if w.value.eq_ignore_ascii_case("ROW") {
                    found_row = true;
                }
                if w.value.eq_ignore_ascii_case("POLICY") && found_row {
                    found_policy = true;
                }
            }
        }
        found_row && found_policy
    }

    pub fn parse_settings_profile(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseSettingsProfile {
            statement: sql.to_string(),
        })
    }

    pub fn is_settings_profile_statement(tokens: &[&Token]) -> bool {
        let mut found_settings = false;
        let mut found_profile = false;
        for token in tokens.iter() {
            if let Token::Word(w) = token {
                if w.value.eq_ignore_ascii_case("SETTINGS") {
                    found_settings = true;
                }
                if w.value.eq_ignore_ascii_case("PROFILE") && found_settings {
                    found_profile = true;
                }
            }
        }
        found_settings && found_profile
    }

    pub fn parse_dictionary(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseDictionary {
            statement: sql.to_string(),
        })
    }

    pub fn is_dictionary_statement(tokens: &[&Token]) -> bool {
        for token in tokens.iter() {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("DICTIONARY")
            {
                return true;
            }
        }
        false
    }

    pub fn parse_show(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseShow {
            statement: sql.to_string(),
        })
    }

    pub fn is_show_statement(tokens: &[&Token]) -> bool {
        if let Some(Token::Word(w)) = tokens.first()
            && w.value.eq_ignore_ascii_case("SHOW")
        {
            return true;
        }
        false
    }

    pub fn parse_function(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseFunction {
            statement: sql.to_string(),
        })
    }

    pub fn is_function_statement(tokens: &[&Token]) -> bool {
        let first_keyword = tokens
            .iter()
            .find_map(|t| {
                if let Token::Word(w) = t {
                    Some(w.value.to_uppercase())
                } else {
                    None
                }
            })
            .unwrap_or_default();

        if !matches!(first_keyword.as_str(), "CREATE" | "DROP") {
            return false;
        }

        for token in tokens.iter() {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("FUNCTION")
            {
                return true;
            }
        }
        false
    }

    pub fn parse_materialized_view(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseMaterializedView {
            statement: sql.to_string(),
        })
    }

    pub fn is_materialized_view_statement(tokens: &[&Token]) -> bool {
        let mut found_materialized = false;
        let mut found_view = false;

        for token in tokens.iter() {
            if let Token::Word(w) = token {
                if w.value.eq_ignore_ascii_case("MATERIALIZED") {
                    found_materialized = true;
                }
                if w.value.eq_ignore_ascii_case("VIEW") && found_materialized {
                    found_view = true;
                }
            }
        }
        found_materialized && found_view
    }

    pub fn parse_projection(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseProjection {
            statement: sql.to_string(),
        })
    }

    pub fn is_projection_statement(tokens: &[&Token]) -> bool {
        for token in tokens.iter() {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("PROJECTION")
            {
                return true;
            }
        }
        false
    }

    pub fn parse_alter_user(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseAlterUser {
            statement: sql.to_string(),
        })
    }

    pub fn is_alter_user_statement(tokens: &[&Token]) -> bool {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return false;
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "USER") {
            return false;
        }
        true
    }

    pub fn parse_grant(sql: &str) -> Option<CustomStatement> {
        Some(CustomStatement::ClickHouseGrant {
            statement: sql.to_string(),
        })
    }

    pub fn is_grant_statement(tokens: &[&Token]) -> bool {
        if let Some(Token::Word(w)) = tokens.first()
            && (w.value.eq_ignore_ascii_case("GRANT") || w.value.eq_ignore_ascii_case("REVOKE"))
        {
            for token in tokens.iter() {
                if let Token::Word(w) = token
                    && w.value.eq_ignore_ascii_case("ROLE")
                {
                    return true;
                }
            }
        }
        false
    }

    pub fn is_clickhouse_create_index(tokens: &[&Token]) -> bool {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return false;
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "INDEX") {
            return false;
        }

        for token in tokens.iter() {
            if let Token::Word(w) = token
                && w.value.eq_ignore_ascii_case("TYPE")
            {
                return true;
            }
        }
        false
    }

    fn parse_identifier(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        match tokens.get(*idx) {
            Some(Token::Word(w)) => {
                *idx += 1;
                Ok(w.value.clone())
            }
            Some(Token::DoubleQuotedString(s)) | Some(Token::SingleQuotedString(s)) => {
                *idx += 1;
                Ok(s.clone())
            }
            other => Err(Error::parse_error(format!(
                "Expected identifier, found {:?}",
                other
            ))),
        }
    }

    fn parse_qualified_name(tokens: &[&Token], idx: &mut usize) -> Result<ObjectName> {
        use sqlparser::ast::ObjectNamePart;

        let mut parts = vec![];

        let first = Self::parse_identifier(tokens, idx)?;
        parts.push(ObjectNamePart::Identifier(sqlparser::ast::Ident::new(
            first,
        )));

        while matches!(tokens.get(*idx), Some(Token::Period)) {
            *idx += 1;
            let next = Self::parse_identifier(tokens, idx)?;
            parts.push(ObjectNamePart::Identifier(sqlparser::ast::Ident::new(next)));
        }

        Ok(ObjectName(parts))
    }

    fn parse_column_list(tokens: &[&Token], idx: &mut usize) -> Result<Vec<String>> {
        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            return Err(Error::parse_error(
                "Expected '(' for column list".to_string(),
            ));
        }
        *idx += 1;

        let mut columns = vec![];
        let mut paren_depth = 1;
        let mut current_col = String::new();

        while *idx < tokens.len() && paren_depth > 0 {
            match tokens.get(*idx) {
                Some(Token::LParen) => {
                    paren_depth += 1;
                    current_col.push('(');
                    *idx += 1;
                }
                Some(Token::RParen) => {
                    paren_depth -= 1;
                    if paren_depth > 0 {
                        current_col.push(')');
                    } else {
                        let trimmed = current_col.trim().to_string();
                        if !trimmed.is_empty() {
                            columns.push(trimmed);
                        }
                    }
                    *idx += 1;
                }
                Some(Token::Comma) if paren_depth == 1 => {
                    let trimmed = current_col.trim().to_string();
                    if !trimmed.is_empty() {
                        columns.push(trimmed);
                    }
                    current_col.clear();
                    *idx += 1;
                }
                Some(Token::Word(w)) => {
                    if !current_col.is_empty() && !current_col.ends_with('(') {
                        current_col.push(' ');
                    }
                    current_col.push_str(&w.value);
                    *idx += 1;
                }
                Some(Token::Number(n, _)) => {
                    current_col.push_str(n);
                    *idx += 1;
                }
                Some(Token::Mul) => {
                    current_col.push('*');
                    *idx += 1;
                }
                Some(Token::Plus) => {
                    current_col.push_str(" + ");
                    *idx += 1;
                }
                Some(Token::Minus) => {
                    current_col.push_str(" - ");
                    *idx += 1;
                }
                Some(Token::Div) => {
                    current_col.push_str(" / ");
                    *idx += 1;
                }
                Some(Token::Period) => {
                    current_col.push('.');
                    *idx += 1;
                }
                Some(Token::Comma) => {
                    current_col.push(',');
                    *idx += 1;
                }
                Some(Token::SingleQuotedString(s)) => {
                    current_col.push('\'');
                    current_col.push_str(s);
                    current_col.push('\'');
                    *idx += 1;
                }
                _ => {
                    *idx += 1;
                }
            }
        }

        Ok(columns)
    }

    fn parse_index_type(tokens: &[&Token], idx: &mut usize) -> Result<ClickHouseIndexType> {
        let type_name = Self::parse_identifier(tokens, idx)?;
        let type_lower = type_name.to_lowercase();

        match type_lower.as_str() {
            "minmax" => Ok(ClickHouseIndexType::MinMax),
            "inverted" => Ok(ClickHouseIndexType::Inverted),
            "full_text" => Ok(ClickHouseIndexType::FullText),
            "hypothesis" => Ok(ClickHouseIndexType::Hypothesis),

            "set" => {
                let args = Self::parse_optional_args(tokens, idx)?;
                let max_rows = args.first().and_then(|s| s.parse().ok());
                Ok(ClickHouseIndexType::Set { max_rows })
            }

            "bloom_filter" => {
                let args = Self::parse_optional_args(tokens, idx)?;
                let false_positive = args.first().and_then(|s| s.parse().ok());
                Ok(ClickHouseIndexType::BloomFilter { false_positive })
            }

            "ngrambf_v1" => {
                let args = Self::parse_optional_args(tokens, idx)?;
                if args.len() != 4 {
                    return Err(Error::parse_error(
                        "ngrambf_v1 requires 4 arguments".to_string(),
                    ));
                }
                Ok(ClickHouseIndexType::NgramBF {
                    ngram_size: args[0]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid ngram_size"))?,
                    filter_size: args[1]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid filter_size"))?,
                    hash_count: args[2]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid hash_count"))?,
                    seed: args[3]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid seed"))?,
                })
            }

            "tokenbf_v1" => {
                let args = Self::parse_optional_args(tokens, idx)?;
                if args.len() != 3 {
                    return Err(Error::parse_error(
                        "tokenbf_v1 requires 3 arguments".to_string(),
                    ));
                }
                Ok(ClickHouseIndexType::TokenBF {
                    filter_size: args[0]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid filter_size"))?,
                    hash_count: args[1]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid hash_count"))?,
                    seed: args[2]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid seed"))?,
                })
            }

            "annoy" => {
                let args = Self::parse_optional_args(tokens, idx)?;
                if args.len() != 2 {
                    return Err(Error::parse_error("annoy requires 2 arguments".to_string()));
                }
                Ok(ClickHouseIndexType::Annoy {
                    distance: args[0].trim_matches('\'').to_string(),
                    num_trees: args[1]
                        .parse()
                        .map_err(|_| Error::parse_error("Invalid num_trees"))?,
                })
            }

            "usearch" => {
                let args = Self::parse_optional_args(tokens, idx)?;
                if args.is_empty() {
                    return Err(Error::parse_error(
                        "usearch requires distance argument".to_string(),
                    ));
                }
                Ok(ClickHouseIndexType::Usearch {
                    distance: args[0].trim_matches('\'').to_string(),
                })
            }

            _ => Err(Error::parse_error(format!(
                "Unknown ClickHouse index type: {}",
                type_name
            ))),
        }
    }

    fn parse_optional_args(tokens: &[&Token], idx: &mut usize) -> Result<Vec<String>> {
        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            return Ok(vec![]);
        }
        *idx += 1;

        let mut args = vec![];
        let mut current_arg = String::new();

        while *idx < tokens.len() {
            match tokens.get(*idx) {
                Some(Token::RParen) => {
                    let trimmed = current_arg.trim().to_string();
                    if !trimmed.is_empty() {
                        args.push(trimmed);
                    }
                    *idx += 1;
                    break;
                }
                Some(Token::Comma) => {
                    let trimmed = current_arg.trim().to_string();
                    if !trimmed.is_empty() {
                        args.push(trimmed);
                    }
                    current_arg.clear();
                    *idx += 1;
                }
                Some(Token::Number(n, _)) => {
                    current_arg.push_str(n);
                    *idx += 1;
                }
                Some(Token::SingleQuotedString(s)) => {
                    current_arg.push('\'');
                    current_arg.push_str(s);
                    current_arg.push('\'');
                    *idx += 1;
                }
                Some(Token::Word(w)) => {
                    current_arg.push_str(&w.value);
                    *idx += 1;
                }
                _ => {
                    *idx += 1;
                }
            }
        }

        Ok(args)
    }

    fn parse_number(tokens: &[&Token], idx: &mut usize) -> Result<u64> {
        match tokens.get(*idx) {
            Some(Token::Number(n, _)) => {
                *idx += 1;
                n.parse()
                    .map_err(|_| Error::parse_error(format!("Invalid number: {}", n)))
            }
            other => Err(Error::parse_error(format!(
                "Expected number, found {:?}",
                other
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use sqlparser::dialect::ClickHouseDialect;
    use sqlparser::tokenizer::Tokenizer;

    use super::*;

    fn tokenize(sql: &str) -> Vec<Token> {
        Tokenizer::new(&ClickHouseDialect {}, sql)
            .tokenize()
            .unwrap()
    }

    fn filter_tokens(tokens: &[Token]) -> Vec<&Token> {
        tokens
            .iter()
            .filter(|t| !matches!(t, Token::Whitespace(_)))
            .collect()
    }

    #[test]
    fn test_parse_minmax_index() {
        let sql = "CREATE INDEX idx_minmax ON test_table (value) TYPE minmax GRANULARITY 4";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::parse_create_index(&filtered).unwrap();
        assert!(result.is_some());

        match result.unwrap() {
            CustomStatement::ClickHouseCreateIndex {
                index_name,
                table_name,
                index_type,
                granularity,
                ..
            } => {
                assert_eq!(index_name, "idx_minmax");
                assert_eq!(table_name.to_string(), "test_table");
                assert!(matches!(index_type, ClickHouseIndexType::MinMax));
                assert_eq!(granularity, Some(4));
            }
            _ => panic!("Expected ClickHouseCreateIndex"),
        }
    }

    #[test]
    fn test_parse_bloom_filter_index() {
        let sql = "CREATE INDEX idx_bloom ON test_table (value) TYPE bloom_filter(0.01)";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::parse_create_index(&filtered).unwrap();
        assert!(result.is_some());

        match result.unwrap() {
            CustomStatement::ClickHouseCreateIndex { index_type, .. } => match index_type {
                ClickHouseIndexType::BloomFilter { false_positive } => {
                    assert_eq!(false_positive, Some(0.01));
                }
                _ => panic!("Expected BloomFilter"),
            },
            _ => panic!("Expected ClickHouseCreateIndex"),
        }
    }

    #[test]
    fn test_parse_set_index() {
        let sql = "CREATE INDEX idx_set ON test_table (value) TYPE set(100)";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::parse_create_index(&filtered).unwrap();
        assert!(result.is_some());

        match result.unwrap() {
            CustomStatement::ClickHouseCreateIndex { index_type, .. } => match index_type {
                ClickHouseIndexType::Set { max_rows } => {
                    assert_eq!(max_rows, Some(100));
                }
                _ => panic!("Expected Set"),
            },
            _ => panic!("Expected ClickHouseCreateIndex"),
        }
    }

    #[test]
    fn test_is_clickhouse_create_index() {
        let sql_with_type = "CREATE INDEX idx ON t (c) TYPE minmax";
        let tokens = tokenize(sql_with_type);
        let filtered: Vec<&Token> = filter_tokens(&tokens);
        assert!(ClickHouseParser::is_clickhouse_create_index(&filtered));

        let sql_without_type = "CREATE INDEX idx ON t (c)";
        let tokens = tokenize(sql_without_type);
        let filtered: Vec<&Token> = filter_tokens(&tokens);
        assert!(!ClickHouseParser::is_clickhouse_create_index(&filtered));
    }
}
