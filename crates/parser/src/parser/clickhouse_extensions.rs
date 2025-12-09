use sqlparser::ast::ObjectName;
use sqlparser::tokenizer::Token;
use yachtsql_core::error::{Error, Result};

use super::helpers::ParserHelpers;
use crate::validator::{ClickHouseSystemCommand, CustomStatement};

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

    pub fn is_system_command(tokens: &[&Token]) -> bool {
        matches!(tokens.first(), Some(Token::Word(w)) if w.value.eq_ignore_ascii_case("SYSTEM"))
    }

    pub fn is_create_dictionary(tokens: &[&Token]) -> bool {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return false;
        }
        ParserHelpers::expect_keyword(tokens, &mut idx, "DICTIONARY")
    }

    pub fn parse_create_dictionary(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DICTIONARY") {
            return Ok(None);
        }

        let name = Self::parse_qualified_name(tokens, &mut idx)?;

        Ok(Some(CustomStatement::ClickHouseCreateDictionary { name }))
    }

    pub fn parse_system_command(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SYSTEM") {
            return Ok(None);
        }

        let command = Self::parse_system_command_type(tokens, &mut idx)?;

        Ok(Some(CustomStatement::ClickHouseSystem { command }))
    }

    fn parse_system_command_type(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<ClickHouseSystemCommand> {
        let keyword = Self::get_keyword(tokens, *idx)?;
        *idx += 1;

        match keyword.to_uppercase().as_str() {
            "RELOAD" => Self::parse_reload_command(tokens, idx),
            "DROP" => Self::parse_drop_cache_command(tokens, idx),
            "FLUSH" => Self::parse_flush_command(tokens, idx),
            "STOP" => Self::parse_stop_command(tokens, idx),
            "START" => Self::parse_start_command(tokens, idx),
            "SYNC" => Self::parse_sync_command(tokens, idx),
            "SHUTDOWN" => Ok(ClickHouseSystemCommand::Shutdown),
            "KILL" => Ok(ClickHouseSystemCommand::Kill),
            _ => Err(Error::parse_error(format!(
                "Unknown SYSTEM command: {}",
                keyword
            ))),
        }
    }

    fn parse_reload_command(tokens: &[&Token], idx: &mut usize) -> Result<ClickHouseSystemCommand> {
        let keyword = Self::get_keyword(tokens, *idx)?;
        *idx += 1;

        match keyword.to_uppercase().as_str() {
            "DICTIONARY" => {
                let name = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::ReloadDictionary { name })
            }
            "DICTIONARIES" => Ok(ClickHouseSystemCommand::ReloadDictionaries),
            "CONFIG" => Ok(ClickHouseSystemCommand::ReloadConfig),
            _ => Err(Error::parse_error(format!(
                "Unknown RELOAD command: {}",
                keyword
            ))),
        }
    }

    fn parse_drop_cache_command(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<ClickHouseSystemCommand> {
        let mut keywords = Vec::new();
        while *idx < tokens.len() {
            if let Some(Token::Word(w)) = tokens.get(*idx) {
                keywords.push(w.value.to_uppercase());
                *idx += 1;
            } else {
                break;
            }
        }

        let combined = keywords.join(" ");
        match combined.as_str() {
            "DNS CACHE" => Ok(ClickHouseSystemCommand::DropDnsCache),
            "MARK CACHE" => Ok(ClickHouseSystemCommand::DropMarkCache),
            "UNCOMPRESSED CACHE" => Ok(ClickHouseSystemCommand::DropUncompressedCache),
            "COMPILED EXPRESSION CACHE" => Ok(ClickHouseSystemCommand::DropCompiledExpressionCache),
            s if s.starts_with("REPLICA") => {
                let replica_name = if keywords.len() >= 2 {
                    keywords.get(1).cloned()
                } else {
                    Self::try_parse_string_literal(tokens, idx)
                };
                let replica_name = replica_name.ok_or_else(|| {
                    Error::parse_error("Expected replica name after DROP REPLICA".to_string())
                })?;

                let from_table = if ParserHelpers::consume_keyword(tokens, idx, "FROM") {
                    ParserHelpers::consume_keyword(tokens, idx, "TABLE");
                    Self::try_parse_qualified_name_string(tokens, idx)
                } else {
                    None
                };

                Ok(ClickHouseSystemCommand::DropReplica {
                    replica_name: replica_name.trim_matches('\'').to_string(),
                    from_table,
                })
            }
            _ => Err(Error::parse_error(format!(
                "Unknown DROP cache command: {}",
                combined
            ))),
        }
    }

    fn parse_flush_command(tokens: &[&Token], idx: &mut usize) -> Result<ClickHouseSystemCommand> {
        let keyword = Self::get_keyword(tokens, *idx)?;
        *idx += 1;

        match keyword.to_uppercase().as_str() {
            "LOGS" => Ok(ClickHouseSystemCommand::FlushLogs),
            _ => Err(Error::parse_error(format!(
                "Unknown FLUSH command: {}",
                keyword
            ))),
        }
    }

    fn parse_stop_command(tokens: &[&Token], idx: &mut usize) -> Result<ClickHouseSystemCommand> {
        let keyword = Self::get_keyword(tokens, *idx)?;
        *idx += 1;

        match keyword.to_uppercase().as_str() {
            "MERGES" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StopMerges { table })
            }
            "TTL" => {
                if !ParserHelpers::consume_keyword(tokens, idx, "MERGES") {
                    return Err(Error::parse_error("Expected MERGES after TTL".to_string()));
                }
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StopTtlMerges { table })
            }
            "MOVES" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StopMoves { table })
            }
            "FETCHES" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StopFetches { table })
            }
            "SENDS" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StopSends { table })
            }
            "REPLICATION" => {
                if !ParserHelpers::consume_keyword(tokens, idx, "QUEUES") {
                    return Err(Error::parse_error(
                        "Expected QUEUES after REPLICATION".to_string(),
                    ));
                }
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StopReplicationQueues { table })
            }
            _ => Err(Error::parse_error(format!(
                "Unknown STOP command: {}",
                keyword
            ))),
        }
    }

    fn parse_start_command(tokens: &[&Token], idx: &mut usize) -> Result<ClickHouseSystemCommand> {
        let keyword = Self::get_keyword(tokens, *idx)?;
        *idx += 1;

        match keyword.to_uppercase().as_str() {
            "MERGES" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StartMerges { table })
            }
            "TTL" => {
                if !ParserHelpers::consume_keyword(tokens, idx, "MERGES") {
                    return Err(Error::parse_error("Expected MERGES after TTL".to_string()));
                }
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StartTtlMerges { table })
            }
            "MOVES" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StartMoves { table })
            }
            "FETCHES" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StartFetches { table })
            }
            "SENDS" => {
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StartSends { table })
            }
            "REPLICATION" => {
                if !ParserHelpers::consume_keyword(tokens, idx, "QUEUES") {
                    return Err(Error::parse_error(
                        "Expected QUEUES after REPLICATION".to_string(),
                    ));
                }
                let table = Self::try_parse_identifier(tokens, idx);
                Ok(ClickHouseSystemCommand::StartReplicationQueues { table })
            }
            _ => Err(Error::parse_error(format!(
                "Unknown START command: {}",
                keyword
            ))),
        }
    }

    fn parse_sync_command(tokens: &[&Token], idx: &mut usize) -> Result<ClickHouseSystemCommand> {
        let keyword = Self::get_keyword(tokens, *idx)?;
        *idx += 1;

        match keyword.to_uppercase().as_str() {
            "REPLICA" => {
                let table = Self::try_parse_identifier(tokens, idx).ok_or_else(|| {
                    Error::parse_error("Expected table name after SYNC REPLICA".to_string())
                })?;
                Ok(ClickHouseSystemCommand::SyncReplica { table })
            }
            _ => Err(Error::parse_error(format!(
                "Unknown SYNC command: {}",
                keyword
            ))),
        }
    }

    fn get_keyword(tokens: &[&Token], idx: usize) -> Result<String> {
        match tokens.get(idx) {
            Some(Token::Word(w)) => Ok(w.value.clone()),
            other => Err(Error::parse_error(format!(
                "Expected keyword, found {:?}",
                other
            ))),
        }
    }

    fn try_parse_identifier(tokens: &[&Token], idx: &mut usize) -> Option<String> {
        match tokens.get(*idx) {
            Some(Token::Word(w)) => {
                *idx += 1;
                Some(w.value.clone())
            }
            Some(Token::DoubleQuotedString(s)) | Some(Token::SingleQuotedString(s)) => {
                *idx += 1;
                Some(s.clone())
            }
            _ => None,
        }
    }

    fn try_parse_string_literal(tokens: &[&Token], idx: &mut usize) -> Option<String> {
        match tokens.get(*idx) {
            Some(Token::SingleQuotedString(s)) | Some(Token::DoubleQuotedString(s)) => {
                *idx += 1;
                Some(s.clone())
            }
            _ => None,
        }
    }

    fn try_parse_qualified_name_string(tokens: &[&Token], idx: &mut usize) -> Option<String> {
        let mut parts = vec![];

        if let Some(first) = Self::try_parse_identifier(tokens, idx) {
            parts.push(first);
        } else {
            return None;
        }

        while matches!(tokens.get(*idx), Some(Token::Period)) {
            *idx += 1;
            if let Some(next) = Self::try_parse_identifier(tokens, idx) {
                parts.push(next);
            } else {
                break;
            }
        }

        Some(parts.join("."))
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
