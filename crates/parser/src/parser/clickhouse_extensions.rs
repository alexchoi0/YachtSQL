use sqlparser::ast::ObjectName;
use sqlparser::tokenizer::Token;
use yachtsql_core::error::{Error, Result};

use super::helpers::ParserHelpers;
use crate::validator::{ClickHouseSystemCommand, ClickHouseTtlOperation, CustomStatement};

#[derive(Debug, Clone, PartialEq)]
pub enum ClickHouseExplainVariant {
    Ast,
    Syntax,
    Plan,
    Pipeline,
    Estimate,
    Default,
}

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

#[derive(Clone)]
pub enum KeywordPattern {
    StartsWith(&'static [&'static str]),
    Contains(&'static str),
    Consecutive(&'static [&'static str]),
    StartsWithAndContains {
        prefix: &'static [&'static str],
        contains: &'static str,
    },
    FirstKeywordThenContains {
        first: &'static [&'static str],
        contains: &'static str,
    },
}

impl KeywordPattern {
    pub fn matches(&self, tokens: &[&Token]) -> bool {
        match self {
            KeywordPattern::StartsWith(keywords) => {
                let mut idx = 0;
                for kw in *keywords {
                    if !ParserHelpers::expect_keyword(tokens, &mut idx, kw) {
                        return false;
                    }
                }
                true
            }
            KeywordPattern::Contains(keyword) => tokens
                .iter()
                .any(|t| matches!(t, Token::Word(w) if w.value.eq_ignore_ascii_case(keyword))),
            KeywordPattern::Consecutive(keywords) => {
                for (i, token) in tokens.iter().enumerate() {
                    if let Token::Word(w) = token
                        && w.value.eq_ignore_ascii_case(keywords[0])
                    {
                        let mut all_match = true;
                        for (offset, kw) in keywords.iter().enumerate().skip(1) {
                            match tokens.get(i + offset) {
                                Some(Token::Word(w2)) if w2.value.eq_ignore_ascii_case(kw) => {}
                                _ => {
                                    all_match = false;
                                    break;
                                }
                            }
                        }
                        if all_match {
                            return true;
                        }
                    }
                }
                false
            }
            KeywordPattern::StartsWithAndContains { prefix, contains } => {
                let mut idx = 0;
                for kw in *prefix {
                    if !ParserHelpers::expect_keyword(tokens, &mut idx, kw) {
                        return false;
                    }
                }
                tokens
                    .iter()
                    .skip(idx)
                    .any(|t| matches!(t, Token::Word(w) if w.value.eq_ignore_ascii_case(contains)))
            }
            KeywordPattern::FirstKeywordThenContains { first, contains } => {
                let first_keyword = tokens.iter().find_map(|t| {
                    if let Token::Word(w) = t {
                        Some(w.value.to_uppercase())
                    } else {
                        None
                    }
                });
                let Some(kw) = first_keyword else {
                    return false;
                };
                if !first.iter().any(|f| kw.eq_ignore_ascii_case(f)) {
                    return false;
                }
                tokens
                    .iter()
                    .any(|t| matches!(t, Token::Word(w) if w.value.eq_ignore_ascii_case(contains)))
            }
        }
    }
}

pub trait ClickHouseStatementParser: Sync {
    fn pattern(&self) -> KeywordPattern;
    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>>;

    fn matches(&self, tokens: &[&Token]) -> bool {
        self.pattern().matches(tokens)
    }
}

struct PassthroughParser {
    pattern: KeywordPattern,
    constructor: fn(&str) -> CustomStatement,
}

impl PassthroughParser {
    const fn new(pattern: KeywordPattern, constructor: fn(&str) -> CustomStatement) -> Self {
        Self {
            pattern,
            constructor,
        }
    }
}

impl ClickHouseStatementParser for PassthroughParser {
    fn pattern(&self) -> KeywordPattern {
        self.pattern.clone()
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some((self.constructor)(sql)))
    }
}

struct CreateIndexParser;

impl ClickHouseStatementParser for CreateIndexParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "INDEX"],
            contains: "TYPE",
        }
    }

    fn parse(&self, tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        Parsing::parse_create_index(tokens)
    }
}

struct AlterColumnCodecParser;

impl ClickHouseStatementParser for AlterColumnCodecParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["ALTER", "TABLE"],
            contains: "CODEC",
        }
    }

    fn parse(&self, tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        if !has_modify_column_before_codec(tokens) {
            return Ok(None);
        }
        Parsing::parse_alter_column_codec(tokens)
    }
}

fn has_modify_column_before_codec(tokens: &[&Token]) -> bool {
    let mut found_modify = false;
    for token in tokens.iter() {
        if let Token::Word(w) = token {
            if w.value.eq_ignore_ascii_case("MODIFY") {
                found_modify = true;
            }
            if w.value.eq_ignore_ascii_case("CODEC") {
                return found_modify;
            }
        }
    }
    false
}

struct AlterTableTtlParser;

impl ClickHouseStatementParser for AlterTableTtlParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["ALTER", "TABLE"],
            contains: "TTL",
        }
    }

    fn parse(&self, tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        if !has_ttl_operation(tokens) {
            return Ok(None);
        }
        Parsing::parse_alter_table_ttl(tokens)
    }
}

fn has_ttl_operation(tokens: &[&Token]) -> bool {
    for token in tokens.iter() {
        if let Token::Word(w) = token
            && (w.value.eq_ignore_ascii_case("MODIFY")
                || w.value.eq_ignore_ascii_case("REMOVE")
                || w.value.eq_ignore_ascii_case("MATERIALIZE"))
        {
            return true;
        }
    }
    false
}

struct SystemCommandParser;

impl ClickHouseStatementParser for SystemCommandParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWith(&["SYSTEM"])
    }

    fn parse(&self, tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        Parsing::parse_system_command(tokens)
    }
}

struct CreateDictionaryParser;

impl ClickHouseStatementParser for CreateDictionaryParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWith(&["CREATE", "DICTIONARY"])
    }

    fn parse(&self, tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        Parsing::parse_create_dictionary(tokens)
    }
}

struct GrantRoleParser;

impl ClickHouseStatementParser for GrantRoleParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::FirstKeywordThenContains {
            first: &["GRANT", "REVOKE"],
            contains: "ROLE",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseGrant {
            statement: sql.to_string(),
        }))
    }
}

struct GrantRoleToUserParser;

impl ClickHouseStatementParser for GrantRoleToUserParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWith(&["GRANT"])
    }

    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_to_without_on(tokens) {
            return Ok(None);
        }
        Ok(Some(CustomStatement::ClickHouseGrant {
            statement: sql.to_string(),
        }))
    }
}

fn has_to_without_on(tokens: &[&Token]) -> bool {
    let mut found_to = false;
    let mut found_on = false;
    for token in tokens.iter() {
        if let Token::Word(w) = token {
            if w.value.eq_ignore_ascii_case("TO") {
                found_to = true;
            }
            if w.value.eq_ignore_ascii_case("ON") {
                found_on = true;
            }
        }
    }
    found_to && !found_on
}

struct RevokeRoleFromUserParser;

impl ClickHouseStatementParser for RevokeRoleFromUserParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWith(&["REVOKE"])
    }

    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_from_without_on(tokens) {
            return Ok(None);
        }
        Ok(Some(CustomStatement::ClickHouseGrant {
            statement: sql.to_string(),
        }))
    }
}

fn has_from_without_on(tokens: &[&Token]) -> bool {
    let mut found_from = false;
    let mut found_on = false;
    for token in tokens.iter() {
        if let Token::Word(w) = token {
            if w.value.eq_ignore_ascii_case("FROM") {
                found_from = true;
            }
            if w.value.eq_ignore_ascii_case("ON") {
                found_on = true;
            }
        }
    }
    found_from && !found_on
}

struct CreateUserIdentifiedParser;

impl ClickHouseStatementParser for CreateUserIdentifiedParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "USER"],
            contains: "IDENTIFIED",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseCreateUser {
            statement: sql.to_string(),
        }))
    }
}

struct AlterUserIdentifiedParser;

impl ClickHouseStatementParser for AlterUserIdentifiedParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["ALTER", "USER"],
            contains: "IDENTIFIED",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseAlterUser {
            statement: sql.to_string(),
        }))
    }
}

struct SetDefaultRoleParser;

impl ClickHouseStatementParser for SetDefaultRoleParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::Consecutive(&["SET", "DEFAULT", "ROLE"])
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseSetDefaultRole {
            statement: sql.to_string(),
        }))
    }
}

struct FunctionParser;

impl ClickHouseStatementParser for FunctionParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::FirstKeywordThenContains {
            first: &["CREATE", "DROP"],
            contains: "FUNCTION",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseFunction {
            statement: sql.to_string(),
        }))
    }
}

static PASSTHROUGH_QUOTA: PassthroughParser =
    PassthroughParser::new(KeywordPattern::Contains("QUOTA"), |sql| {
        CustomStatement::ClickHouseQuota {
            statement: sql.to_string(),
        }
    });

static PASSTHROUGH_ROW_POLICY: PassthroughParser =
    PassthroughParser::new(KeywordPattern::Consecutive(&["ROW", "POLICY"]), |sql| {
        CustomStatement::ClickHouseRowPolicy {
            statement: sql.to_string(),
        }
    });

static PASSTHROUGH_SETTINGS_PROFILE: PassthroughParser = PassthroughParser::new(
    KeywordPattern::Consecutive(&["SETTINGS", "PROFILE"]),
    |sql| CustomStatement::ClickHouseSettingsProfile {
        statement: sql.to_string(),
    },
);

static PASSTHROUGH_DICTIONARY: PassthroughParser =
    PassthroughParser::new(KeywordPattern::Contains("DICTIONARY"), |sql| {
        CustomStatement::ClickHouseDictionary {
            statement: sql.to_string(),
        }
    });

static PASSTHROUGH_SHOW: PassthroughParser =
    PassthroughParser::new(KeywordPattern::StartsWith(&["SHOW"]), |sql| {
        CustomStatement::ClickHouseShow {
            statement: sql.to_string(),
        }
    });

static PASSTHROUGH_MATERIALIZED_VIEW: PassthroughParser = PassthroughParser::new(
    KeywordPattern::Consecutive(&["MATERIALIZED", "VIEW"]),
    |sql| CustomStatement::ClickHouseMaterializedView {
        statement: sql.to_string(),
    },
);

static PASSTHROUGH_PROJECTION: PassthroughParser = PassthroughParser::new(
    KeywordPattern::StartsWithAndContains {
        prefix: &["ALTER"],
        contains: "PROJECTION",
    },
    |sql| CustomStatement::ClickHouseProjection {
        statement: sql.to_string(),
    },
);

fn strip_column_ttl(sql: &str) -> String {
    let result = sql.to_string();
    let upper = result.to_uppercase();

    let Some(paren_start) = upper.find('(') else {
        return result;
    };
    let Some(paren_end) = upper.rfind(')') else {
        return result;
    };
    let Some(engine_pos) = upper.find("ENGINE") else {
        return result;
    };
    if engine_pos <= paren_end {
        return result;
    }

    let columns_part = &result[paren_start + 1..paren_end];
    let mut new_columns = String::new();
    let mut current_col = String::new();
    let mut paren_depth = 0;

    for c in columns_part.chars() {
        match c {
            '(' => {
                paren_depth += 1;
                current_col.push(c);
            }
            ')' => {
                paren_depth -= 1;
                current_col.push(c);
            }
            ',' if paren_depth == 0 => {
                let stripped = strip_ttl_from_column(&current_col);
                if !new_columns.is_empty() {
                    new_columns.push_str(", ");
                }
                new_columns.push_str(&stripped);
                current_col.clear();
            }
            _ => current_col.push(c),
        }
    }

    if !current_col.is_empty() {
        let stripped = strip_ttl_from_column(&current_col);
        if !new_columns.is_empty() {
            new_columns.push_str(", ");
        }
        new_columns.push_str(&stripped);
    }

    let before = &result[..paren_start + 1];
    let after = &result[paren_end..];
    format!("{}{}{}", before, new_columns, after)
}

fn strip_ttl_from_column(col: &str) -> String {
    let upper = col.to_uppercase();
    if let Some(ttl_pos) = upper.find(" TTL ") {
        col[..ttl_pos].trim().to_string()
    } else {
        col.trim().to_string()
    }
}

fn strip_inline_indexes(sql: &str) -> String {
    let mut result = sql.to_string();
    let upper = result.to_uppercase();

    let mut positions_to_remove: Vec<(usize, usize)> = Vec::new();
    let mut pos = 0;

    while let Some(idx) = upper[pos..].find("INDEX") {
        let abs_idx = pos + idx;

        let before_ok = abs_idx == 0
            || !result.as_bytes()[abs_idx - 1].is_ascii_alphanumeric()
                && result.as_bytes()[abs_idx - 1] != b'_';

        let after_idx = abs_idx + "INDEX".len();
        let after_ok = after_idx >= result.len()
            || (!result.as_bytes()[after_idx].is_ascii_alphanumeric()
                && result.as_bytes()[after_idx] != b'_');

        if before_ok && after_ok {
            let before_index = &result[..abs_idx];
            let in_create_table = before_index.to_uppercase().contains("CREATE TABLE");
            let after_create_index = before_index
                .to_uppercase()
                .rfind("CREATE")
                .map(|create_pos| {
                    let after_create = &before_index[create_pos + 6..].trim_start().to_uppercase();
                    after_create.starts_with("INDEX") || after_create.starts_with("UNIQUE")
                })
                .unwrap_or(false);

            if in_create_table && !after_create_index {
                let rest = &result[abs_idx..];
                let mut end_pos = rest.len();
                let mut paren_depth = 0;
                let mut in_string = false;
                let mut prev_char = ' ';

                for (i, c) in rest.chars().enumerate() {
                    if c == '\'' && prev_char != '\\' {
                        in_string = !in_string;
                    }
                    if !in_string {
                        match c {
                            '(' => paren_depth += 1,
                            ')' => {
                                if paren_depth == 0 {
                                    end_pos = i;
                                    break;
                                }
                                paren_depth -= 1;
                            }
                            ',' if paren_depth == 0 => {
                                end_pos = i;
                                break;
                            }
                            _ => {}
                        }
                    }
                    prev_char = c;
                }

                let comma_before = if abs_idx > 0 && result[..abs_idx].trim_end().ends_with(',') {
                    result[..abs_idx].rfind(',').unwrap()
                } else {
                    abs_idx
                };

                positions_to_remove.push((comma_before, abs_idx + end_pos));
            }
        }

        pos = abs_idx + 1;
    }

    positions_to_remove.reverse();
    for (start, end) in positions_to_remove {
        result = format!("{}{}", &result[..start], &result[end..]);
    }

    result
}

fn strip_inline_projections(sql: &str) -> String {
    let mut result = sql.to_string();
    while let Some(start) = result.find("PROJECTION") {
        if let Some(paren_start) = result[start..].find('(') {
            let abs_paren_start = start + paren_start;
            let mut paren_count = 1;
            let mut paren_end = abs_paren_start + 1;
            for (i, c) in result[abs_paren_start + 1..].chars().enumerate() {
                match c {
                    '(' => paren_count += 1,
                    ')' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            paren_end = abs_paren_start + 1 + i + 1;
                            break;
                        }
                    }
                    _ => {}
                }
            }
            let comma_before = if start > 0 && result[..start].trim_end().ends_with(',') {
                result[..start].rfind(',').unwrap()
            } else {
                start
            };
            result = format!("{}{}", &result[..comma_before], &result[paren_end..]);
        } else {
            break;
        }
    }
    result
}

struct CreateTableWithProjectionParser;

impl ClickHouseStatementParser for CreateTableWithProjectionParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "PROJECTION",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        let stripped = strip_inline_projections(sql);
        Ok(Some(CustomStatement::ClickHouseCreateTableWithProjection {
            original: sql.to_string(),
            stripped,
        }))
    }
}

static CREATE_TABLE_WITH_PROJECTION_PARSER: CreateTableWithProjectionParser =
    CreateTableWithProjectionParser;

struct CreateTableWithInlineIndexParser;

impl ClickHouseStatementParser for CreateTableWithInlineIndexParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "INDEX",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_inline_index(sql) {
            return Ok(None);
        }
        let stripped = strip_inline_indexes(sql);
        Ok(Some(CustomStatement::ClickHouseCreateTablePassthrough {
            original: sql.to_string(),
            stripped,
        }))
    }
}

fn has_inline_index(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    if !upper.contains("CREATE TABLE") {
        return false;
    }

    let mut pos = 0;
    while let Some(idx) = upper[pos..].find("INDEX") {
        let abs_idx = pos + idx;

        let before_ok = abs_idx == 0
            || !sql.as_bytes()[abs_idx - 1].is_ascii_alphanumeric()
                && sql.as_bytes()[abs_idx - 1] != b'_';

        let after_idx = abs_idx + "INDEX".len();
        let after_ok = after_idx >= sql.len()
            || (!sql.as_bytes()[after_idx].is_ascii_alphanumeric()
                && sql.as_bytes()[after_idx] != b'_');

        if before_ok && after_ok {
            let before_index = &upper[..abs_idx];
            if before_index.contains("CREATE TABLE") {
                let after_last_create = before_index.rfind("CREATE");
                if let Some(create_pos) = after_last_create {
                    let after_create = &before_index[create_pos + 6..].trim_start();
                    if !after_create.starts_with("INDEX") && !after_create.starts_with("UNIQUE") {
                        return true;
                    }
                }
            }
        }

        pos = abs_idx + 1;
    }
    false
}

static CREATE_TABLE_WITH_INLINE_INDEX_PARSER: CreateTableWithInlineIndexParser =
    CreateTableWithInlineIndexParser;

fn find_keyword_partition_by(sql: &str) -> Option<usize> {
    let upper = sql.to_uppercase();
    let mut pos = 0;
    while let Some(idx) = upper[pos..].find("PARTITION") {
        let abs_idx = pos + idx;
        let before_ok = abs_idx == 0
            || !sql.as_bytes()[abs_idx - 1].is_ascii_alphanumeric()
                && sql.as_bytes()[abs_idx - 1] != b'_';
        let after_idx = abs_idx + "PARTITION".len();
        let after = &upper[after_idx..].trim_start();
        let after_ok = after.starts_with("BY");
        if before_ok && after_ok {
            return Some(abs_idx);
        }
        pos = abs_idx + 1;
    }
    None
}

fn strip_partition_by(sql: &str) -> String {
    if let Some(partition_start) = find_keyword_partition_by(sql) {
        let after_partition = &sql[partition_start + "PARTITION".len()..].trim_start();
        if let Some(rest) = after_partition.strip_prefix("BY") {
            let rest = rest.trim_start();
            let mut paren_count = 0;
            let mut actual_end = 0;
            let mut found_closing = false;

            for (i, c) in rest.chars().enumerate() {
                match c {
                    '(' => paren_count += 1,
                    ')' => {
                        paren_count -= 1;
                        if paren_count == 0 {
                            actual_end = i + 1;
                            found_closing = true;
                        }
                    }
                    c if paren_count == 0 && c.is_ascii_alphabetic() => {
                        actual_end = i;
                        break;
                    }
                    _ => {
                        if paren_count == 0 && !c.is_whitespace() {
                            actual_end = i + 1;
                        }
                    }
                }
            }

            if !found_closing && paren_count == 0 {
                actual_end = rest.len();
            }

            return format!(
                "{} {}",
                sql[..partition_start].trim_end(),
                rest[actual_end..].trim_start()
            );
        }
    }
    sql.to_string()
}

struct CreateTableWithPartitionParser;

impl ClickHouseStatementParser for CreateTableWithPartitionParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "PARTITION",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if find_keyword_partition_by(sql).is_none() {
            return Ok(None);
        }
        let stripped = strip_partition_by(sql);
        Ok(Some(CustomStatement::ClickHouseCreateTablePassthrough {
            original: sql.to_string(),
            stripped,
        }))
    }
}

static CREATE_TABLE_WITH_PARTITION_PARSER: CreateTableWithPartitionParser =
    CreateTableWithPartitionParser;

fn find_settings_clause(sql: &str) -> Option<usize> {
    let upper = sql.to_uppercase();
    let mut pos = 0;
    while let Some(idx) = upper[pos..].find("SETTINGS") {
        let abs_idx = pos + idx;
        let before_ok = abs_idx == 0
            || (!sql.as_bytes()[abs_idx - 1].is_ascii_alphanumeric()
                && sql.as_bytes()[abs_idx - 1] != b'_');
        let after_idx = abs_idx + "SETTINGS".len();
        let after_ok = after_idx >= sql.len()
            || (!sql.as_bytes()[after_idx].is_ascii_alphanumeric()
                && sql.as_bytes()[after_idx] != b'_');
        if before_ok && after_ok {
            let after_settings = sql[after_idx..].trim_start();
            let starts_with_ident = after_settings
                .chars()
                .next()
                .map(|c| c.is_ascii_alphabetic() || c == '_')
                .unwrap_or(false);
            if starts_with_ident && let Some(eq_pos) = after_settings.find('=') {
                let between = after_settings[..eq_pos].trim();
                if !between.is_empty()
                    && between
                        .chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    return Some(abs_idx);
                }
            }
        }
        pos = abs_idx + 1;
    }
    None
}

fn strip_settings(sql: &str) -> String {
    if let Some(settings_pos) = find_settings_clause(sql) {
        return sql[..settings_pos].trim_end().to_string();
    }
    sql.to_string()
}

struct CreateTableWithSettingsParser;

impl ClickHouseStatementParser for CreateTableWithSettingsParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "SETTINGS",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        let stripped = strip_settings(sql);
        if stripped == sql {
            return Ok(None);
        }
        Ok(Some(CustomStatement::ClickHouseCreateTablePassthrough {
            original: sql.to_string(),
            stripped,
        }))
    }
}

static CREATE_TABLE_WITH_SETTINGS_PARSER: CreateTableWithSettingsParser =
    CreateTableWithSettingsParser;

fn find_engine_clause(sql: &str) -> Option<usize> {
    let upper = sql.to_uppercase();
    let mut pos = 0;
    while let Some(idx) = upper[pos..].find("ENGINE") {
        let abs_idx = pos + idx;
        let before_ok = abs_idx == 0
            || (!sql.as_bytes()[abs_idx - 1].is_ascii_alphanumeric()
                && sql.as_bytes()[abs_idx - 1] != b'_');
        let after_idx = abs_idx + "ENGINE".len();
        let after_ok = after_idx >= sql.len()
            || (!sql.as_bytes()[after_idx].is_ascii_alphanumeric()
                && sql.as_bytes()[after_idx] != b'_');
        if before_ok && after_ok {
            let after_engine = sql[after_idx..].trim_start();
            if after_engine.starts_with('=') {
                return Some(abs_idx);
            }
        }
        pos = abs_idx + 1;
    }
    None
}

fn has_complex_engine(sql: &str) -> bool {
    if let Some(engine_pos) = find_engine_clause(sql) {
        let after_engine = &sql[engine_pos + "ENGINE".len()..].trim_start();
        if let Some(after_eq) = after_engine.strip_prefix('=') {
            let after_eq = after_eq.trim_start();
            let complex_engine_names = ["Distributed", "Merge", "Buffer", "GenerateRandom"];
            for name in &complex_engine_names {
                let upper_after_eq = after_eq.to_uppercase();
                let upper_name = name.to_uppercase();
                if upper_after_eq.starts_with(&upper_name) {
                    let after_name = &after_eq[name.len()..];
                    let next_char = after_name.chars().next();
                    if next_char == Some('(') || next_char == Some(' ') || next_char.is_none() {
                        return true;
                    }
                }
            }
        }
    }
    false
}

fn add_dummy_column_if_needed(sql: &str) -> String {
    let upper = sql.to_uppercase();
    if let Some(table_pos) = upper.find("CREATE TABLE") {
        let after_create_table = &sql[table_pos + "CREATE TABLE".len()..].trim_start();

        let mut name_end = 0;
        for (i, c) in after_create_table.chars().enumerate() {
            if c.is_whitespace() || c == '(' {
                name_end = i;
                break;
            }
        }

        if name_end == 0 {
            name_end = after_create_table.len();
        }

        let rest = &after_create_table[name_end..].trim_start();

        if !rest.starts_with('(') {
            let table_name = &after_create_table[..name_end];
            return format!("CREATE TABLE {} (_dummy Int64) {}", table_name, rest);
        }
    }
    sql.to_string()
}

fn strip_clickhouse_clauses(rest: &str) -> String {
    let upper = rest.to_uppercase();
    let clauses_to_strip = [
        "SAMPLE BY",
        "ORDER BY",
        "PARTITION BY",
        "PRIMARY KEY",
        "TTL ",
        "SETTINGS ",
    ];
    let mut result = rest.to_string();
    for clause in clauses_to_strip {
        if let Some(pos) = upper.find(clause) {
            let before = &result[..pos];
            let after = &result[pos + clause.len()..];
            let after_upper = after.to_uppercase();
            let end_pos = clauses_to_strip
                .iter()
                .filter_map(|c| after_upper.find(c))
                .min()
                .unwrap_or(after.len());
            let remaining = &after[end_pos..];
            result = format!("{}{}", before.trim_end(), remaining);
        }
    }
    result
}

fn strip_engine_params(sql: &str) -> String {
    if let Some(engine_pos) = find_engine_clause(sql) {
        let after_engine = &sql[engine_pos + "ENGINE".len()..].trim_start();
        if let Some(after_eq) = after_engine.strip_prefix('=') {
            let after_eq = after_eq.trim_start();
            let mut engine_name_end = 0;
            for (i, c) in after_eq.chars().enumerate() {
                if c == '(' || c.is_whitespace() {
                    break;
                }
                engine_name_end = i + 1;
            }
            let rest = &after_eq[engine_name_end..].trim_start();

            let before_engine = &sql[..engine_pos];

            if rest.starts_with('(') {
                let mut paren_count = 0;
                let mut param_end = 0;
                for (i, c) in rest.chars().enumerate() {
                    match c {
                        '(' => paren_count += 1,
                        ')' => {
                            paren_count -= 1;
                            if paren_count == 0 {
                                param_end = i + 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                }

                let after_params = if param_end > 0 {
                    &rest[param_end..]
                } else {
                    ""
                };

                let cleaned = strip_clickhouse_clauses(after_params.trim_start());
                return format!(
                    "{} ENGINE = Memory {}",
                    before_engine.trim_end(),
                    cleaned.trim()
                );
            } else {
                let cleaned = strip_clickhouse_clauses(rest);
                return format!(
                    "{} ENGINE = Memory {}",
                    before_engine.trim_end(),
                    cleaned.trim()
                );
            }
        }
    }
    sql.to_string()
}

struct CreateTableWithComplexEngineParser;

impl ClickHouseStatementParser for CreateTableWithComplexEngineParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "ENGINE",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_complex_engine(sql) {
            return Ok(None);
        }
        let with_dummy = add_dummy_column_if_needed(sql);
        let stripped = strip_engine_params(&with_dummy);
        Ok(Some(CustomStatement::ClickHouseCreateTablePassthrough {
            original: sql.to_string(),
            stripped,
        }))
    }
}

static CREATE_TABLE_WITH_COMPLEX_ENGINE_PARSER: CreateTableWithComplexEngineParser =
    CreateTableWithComplexEngineParser;

struct CreateTableWithSampleByParser;

impl ClickHouseStatementParser for CreateTableWithSampleByParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "SAMPLE",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        let upper = sql.to_uppercase();
        if !upper.contains("SAMPLE BY") {
            return Ok(None);
        }
        let with_dummy = add_dummy_column_if_needed(sql);
        let stripped = strip_engine_params(&with_dummy);
        Ok(Some(CustomStatement::ClickHouseCreateTablePassthrough {
            original: sql.to_string(),
            stripped,
        }))
    }
}

static CREATE_TABLE_WITH_SAMPLE_BY_PARSER: CreateTableWithSampleByParser =
    CreateTableWithSampleByParser;

struct CreateTableWithColumnTtlParser;

impl ClickHouseStatementParser for CreateTableWithColumnTtlParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "TABLE"],
            contains: "TTL",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_column_ttl(sql) {
            return Ok(None);
        }
        let with_dummy = add_dummy_column_if_needed(sql);
        let stripped = strip_column_ttl(&with_dummy);
        let stripped = strip_engine_params(&stripped);
        Ok(Some(CustomStatement::ClickHouseCreateTablePassthrough {
            original: sql.to_string(),
            stripped,
        }))
    }
}

fn has_column_ttl(sql: &str) -> bool {
    let upper = sql.to_uppercase();
    let Some(paren_start) = upper.find('(') else {
        return false;
    };
    let Some(paren_end) = upper.rfind(')') else {
        return false;
    };
    let Some(engine_pos) = upper.find("ENGINE") else {
        return false;
    };
    if engine_pos <= paren_end {
        return false;
    }
    let columns_part = &upper[paren_start + 1..paren_end];
    columns_part.contains(" TTL ")
}

static CREATE_TABLE_WITH_COLUMN_TTL_PARSER: CreateTableWithColumnTtlParser =
    CreateTableWithColumnTtlParser;

struct AlterTableModifyParser;

impl ClickHouseStatementParser for AlterTableModifyParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["ALTER", "TABLE"],
            contains: "MODIFY",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseAlterTable {
            statement: sql.to_string(),
        }))
    }
}

static ALTER_TABLE_MODIFY_PARSER: AlterTableModifyParser = AlterTableModifyParser;

struct AlterTableMaterializeIndexParser;

impl ClickHouseStatementParser for AlterTableMaterializeIndexParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["ALTER", "TABLE"],
            contains: "MATERIALIZE",
        }
    }

    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_materialize_index(tokens) {
            return Ok(None);
        }
        Ok(Some(CustomStatement::ClickHouseAlterTable {
            statement: sql.to_string(),
        }))
    }
}

fn has_materialize_index(tokens: &[&Token]) -> bool {
    let mut found_materialize = false;
    for token in tokens.iter() {
        if let Token::Word(w) = token {
            if w.value.eq_ignore_ascii_case("MATERIALIZE") {
                found_materialize = true;
            }
            if found_materialize && w.value.eq_ignore_ascii_case("INDEX") {
                return true;
            }
        }
    }
    false
}

static ALTER_TABLE_MATERIALIZE_INDEX_PARSER: AlterTableMaterializeIndexParser =
    AlterTableMaterializeIndexParser;

struct AlterTableClearIndexParser;

impl ClickHouseStatementParser for AlterTableClearIndexParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["ALTER", "TABLE"],
            contains: "CLEAR",
        }
    }

    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        if !has_clear_index(tokens) {
            return Ok(None);
        }
        Ok(Some(CustomStatement::ClickHouseAlterTable {
            statement: sql.to_string(),
        }))
    }
}

fn has_clear_index(tokens: &[&Token]) -> bool {
    let mut found_clear = false;
    for token in tokens.iter() {
        if let Token::Word(w) = token {
            if w.value.eq_ignore_ascii_case("CLEAR") {
                found_clear = true;
            }
            if found_clear && w.value.eq_ignore_ascii_case("INDEX") {
                return true;
            }
        }
    }
    false
}

static ALTER_TABLE_CLEAR_INDEX_PARSER: AlterTableClearIndexParser = AlterTableClearIndexParser;

static PASSTHROUGH_ALTER_USER: PassthroughParser =
    PassthroughParser::new(KeywordPattern::StartsWith(&["ALTER", "USER"]), |sql| {
        CustomStatement::ClickHouseAlterUser {
            statement: sql.to_string(),
        }
    });

static PASSTHROUGH_RENAME_DATABASE: PassthroughParser =
    PassthroughParser::new(KeywordPattern::StartsWith(&["RENAME", "DATABASE"]), |sql| {
        CustomStatement::ClickHouseRenameDatabase {
            statement: sql.to_string(),
        }
    });

struct CreateDatabaseEngineParser;

impl ClickHouseStatementParser for CreateDatabaseEngineParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "DATABASE"],
            contains: "ENGINE",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseDatabase {
            statement: sql.to_string(),
        }))
    }
}

struct CreateDatabaseCommentParser;

impl ClickHouseStatementParser for CreateDatabaseCommentParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWithAndContains {
            prefix: &["CREATE", "DATABASE"],
            contains: "COMMENT",
        }
    }

    fn parse(&self, _tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        Ok(Some(CustomStatement::ClickHouseDatabase {
            statement: sql.to_string(),
        }))
    }
}

struct UseParser;

impl ClickHouseStatementParser for UseParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWith(&["USE"])
    }

    fn parse(&self, tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "USE") {
            return Ok(None);
        }

        let database = match tokens.get(idx) {
            Some(Token::Word(w)) => w.value.clone(),
            _ => {
                return Err(Error::parse_error("Expected database name after USE"));
            }
        };

        Ok(Some(CustomStatement::ClickHouseUse { database }))
    }
}

static CREATE_INDEX_PARSER: CreateIndexParser = CreateIndexParser;
static ALTER_CODEC_PARSER: AlterColumnCodecParser = AlterColumnCodecParser;
static ALTER_TTL_PARSER: AlterTableTtlParser = AlterTableTtlParser;
static SYSTEM_PARSER: SystemCommandParser = SystemCommandParser;
static CREATE_DICT_PARSER: CreateDictionaryParser = CreateDictionaryParser;
static GRANT_ROLE_PARSER: GrantRoleParser = GrantRoleParser;
static GRANT_ROLE_TO_USER_PARSER: GrantRoleToUserParser = GrantRoleToUserParser;
static REVOKE_ROLE_FROM_USER_PARSER: RevokeRoleFromUserParser = RevokeRoleFromUserParser;
static CREATE_USER_IDENTIFIED_PARSER: CreateUserIdentifiedParser = CreateUserIdentifiedParser;
static ALTER_USER_IDENTIFIED_PARSER: AlterUserIdentifiedParser = AlterUserIdentifiedParser;
static SET_DEFAULT_ROLE_PARSER: SetDefaultRoleParser = SetDefaultRoleParser;
static FUNCTION_PARSER: FunctionParser = FunctionParser;
static CREATE_DATABASE_ENGINE_PARSER: CreateDatabaseEngineParser = CreateDatabaseEngineParser;
static CREATE_DATABASE_COMMENT_PARSER: CreateDatabaseCommentParser = CreateDatabaseCommentParser;
static USE_PARSER: UseParser = UseParser;

struct CreateTableAsParser;

impl ClickHouseStatementParser for CreateTableAsParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::Consecutive(&["CREATE", "TABLE"])
    }

    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        let mut idx = 0;
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return Ok(None);
        }

        let new_table = match tokens.get(idx) {
            Some(Token::Word(w)) => {
                idx += 1;
                let mut name = w.value.clone();
                if let Some(Token::Period) = tokens.get(idx) {
                    idx += 1;
                    if let Some(Token::Word(w2)) = tokens.get(idx) {
                        name = format!("{}.{}", name, w2.value);
                        idx += 1;
                    }
                }
                name
            }
            _ => return Ok(None),
        };

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "AS") {
            return Ok(None);
        }

        if let Some(Token::Word(w)) = tokens.get(idx)
            && w.value.eq_ignore_ascii_case("SELECT")
        {
            return Ok(None);
        }

        let source_table = match tokens.get(idx) {
            Some(Token::Word(w)) => {
                let mut current_idx = idx + 1;
                let mut name = w.value.clone();
                if let Some(Token::Period) = tokens.get(current_idx) {
                    current_idx += 1;
                    if let Some(Token::Word(w2)) = tokens.get(current_idx) {
                        name = format!("{}.{}", name, w2.value);
                    }
                }
                name
            }
            _ => return Ok(None),
        };

        let upper = sql.to_uppercase();
        let engine_pos = upper.find("ENGINE");
        if engine_pos.is_none() {
            return Ok(None);
        }
        let engine_clause = sql[engine_pos.unwrap()..].to_string();

        Ok(Some(CustomStatement::ClickHouseCreateTableAs {
            new_table,
            source_table,
            engine_clause,
        }))
    }
}

static CREATE_TABLE_AS_PARSER: CreateTableAsParser = CreateTableAsParser;

struct ExplainVariantParser;

impl ClickHouseStatementParser for ExplainVariantParser {
    fn pattern(&self) -> KeywordPattern {
        KeywordPattern::StartsWith(&["EXPLAIN"])
    }

    fn parse(&self, tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "EXPLAIN") {
            return Ok(None);
        }

        let mut variant = ClickHouseExplainVariant::Default;
        let mut options = Vec::new();

        while idx < tokens.len() {
            match tokens.get(idx) {
                Some(Token::Word(w)) => {
                    let upper = w.value.to_uppercase();
                    match upper.as_str() {
                        "AST" => {
                            variant = ClickHouseExplainVariant::Ast;
                            idx += 1;
                        }
                        "SYNTAX" => {
                            variant = ClickHouseExplainVariant::Syntax;
                            idx += 1;
                        }
                        "PLAN" => {
                            variant = ClickHouseExplainVariant::Plan;
                            idx += 1;
                        }
                        "PIPELINE" => {
                            variant = ClickHouseExplainVariant::Pipeline;
                            idx += 1;
                        }
                        "ESTIMATE" => {
                            variant = ClickHouseExplainVariant::Estimate;
                            idx += 1;
                        }
                        "SELECT" | "INSERT" | "UPDATE" | "DELETE" | "WITH" => {
                            break;
                        }
                        _ => {
                            if let Some(Token::Eq) = tokens.get(idx + 1) {
                                let key = w.value.clone();
                                idx += 2;
                                if let Some(Token::Number(n, _)) = tokens.get(idx) {
                                    options.push((key, n.clone()));
                                    idx += 1;
                                } else if let Some(Token::Word(v)) = tokens.get(idx) {
                                    options.push((key, v.value.clone()));
                                    idx += 1;
                                }
                                if let Some(Token::Comma) = tokens.get(idx) {
                                    idx += 1;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                }
                _ => break,
            }
        }

        if matches!(variant, ClickHouseExplainVariant::Default) && options.is_empty() {
            return Ok(None);
        }

        let mut statement_tokens = Vec::new();
        for token in tokens.iter().skip(idx) {
            statement_tokens.push(format!("{}", token));
        }
        let statement = statement_tokens.join(" ").trim().to_string();

        let statement = if statement.is_empty() {
            let upper = sql.to_uppercase();
            if let Some(select_pos) = upper.find("SELECT") {
                sql[select_pos..].to_string()
            } else if let Some(insert_pos) = upper.find("INSERT") {
                sql[insert_pos..].to_string()
            } else if let Some(with_pos) = upper.find("WITH") {
                sql[with_pos..].to_string()
            } else {
                sql.to_string()
            }
        } else {
            statement
        };

        Ok(Some(CustomStatement::ClickHouseExplain {
            variant,
            statement,
            options,
        }))
    }
}

static EXPLAIN_VARIANT_PARSER: ExplainVariantParser = ExplainVariantParser;

static PARSERS: &[&dyn ClickHouseStatementParser] = &[
    &EXPLAIN_VARIANT_PARSER,
    &CREATE_TABLE_AS_PARSER,
    &CREATE_DICT_PARSER,
    &CREATE_INDEX_PARSER,
    &CREATE_DATABASE_ENGINE_PARSER,
    &CREATE_DATABASE_COMMENT_PARSER,
    &CREATE_USER_IDENTIFIED_PARSER,
    &PASSTHROUGH_RENAME_DATABASE,
    &USE_PARSER,
    &ALTER_USER_IDENTIFIED_PARSER,
    &ALTER_CODEC_PARSER,
    &ALTER_TTL_PARSER,
    &SYSTEM_PARSER,
    &SET_DEFAULT_ROLE_PARSER,
    &PASSTHROUGH_QUOTA,
    &PASSTHROUGH_ROW_POLICY,
    &PASSTHROUGH_SETTINGS_PROFILE,
    &PASSTHROUGH_MATERIALIZED_VIEW,
    &PASSTHROUGH_DICTIONARY,
    &PASSTHROUGH_SHOW,
    &FUNCTION_PARSER,
    &CREATE_TABLE_WITH_COMPLEX_ENGINE_PARSER,
    &CREATE_TABLE_WITH_SAMPLE_BY_PARSER,
    &CREATE_TABLE_WITH_COLUMN_TTL_PARSER,
    &CREATE_TABLE_WITH_PARTITION_PARSER,
    &CREATE_TABLE_WITH_SETTINGS_PARSER,
    &CREATE_TABLE_WITH_PROJECTION_PARSER,
    &CREATE_TABLE_WITH_INLINE_INDEX_PARSER,
    &ALTER_TABLE_MODIFY_PARSER,
    &ALTER_TABLE_MATERIALIZE_INDEX_PARSER,
    &ALTER_TABLE_CLEAR_INDEX_PARSER,
    &PASSTHROUGH_PROJECTION,
    &PASSTHROUGH_ALTER_USER,
    &GRANT_ROLE_PARSER,
    &GRANT_ROLE_TO_USER_PARSER,
    &REVOKE_ROLE_FROM_USER_PARSER,
];

pub struct ClickHouseParser;

impl ClickHouseParser {
    pub fn try_parse(tokens: &[&Token], sql: &str) -> Result<Option<CustomStatement>> {
        for parser in PARSERS {
            if parser.matches(tokens) {
                match parser.parse(tokens, sql) {
                    Ok(Some(stmt)) => return Ok(Some(stmt)),
                    Ok(None) => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        Ok(None)
    }
}

struct Parsing;

impl Parsing {
    fn parse_alter_column_codec(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
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

    fn parse_alter_table_ttl(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
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

    fn parse_create_index(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
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

    fn parse_create_dictionary(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        use crate::validator::{DictionaryLayoutDef, DictionaryLifetimeDef, DictionarySourceDef};

        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DICTIONARY") {
            return Ok(None);
        }

        let name = Self::parse_qualified_name(tokens, &mut idx)?;

        let columns = Self::parse_dictionary_columns(tokens, &mut idx)?;

        let mut primary_key = Vec::new();
        let mut source = DictionarySourceDef::default();
        let mut layout = DictionaryLayoutDef::default();
        let mut lifetime = DictionaryLifetimeDef::default();

        while idx < tokens.len() {
            if ParserHelpers::consume_keyword(tokens, &mut idx, "PRIMARY") {
                if ParserHelpers::expect_keyword(tokens, &mut idx, "KEY") {
                    primary_key = Self::parse_primary_key_columns(tokens, &mut idx)?;
                }
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "SOURCE") {
                source = Self::parse_dictionary_source(tokens, &mut idx)?;
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "LAYOUT") {
                layout = Self::parse_dictionary_layout(tokens, &mut idx)?;
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "LIFETIME") {
                lifetime = Self::parse_dictionary_lifetime(tokens, &mut idx)?;
            } else {
                break;
            }
        }

        Ok(Some(CustomStatement::ClickHouseCreateDictionary {
            name,
            columns,
            primary_key,
            source,
            layout,
            lifetime,
        }))
    }

    fn parse_dictionary_columns(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<Vec<crate::validator::DictionaryColumnDef>> {
        use crate::validator::DictionaryColumnDef;

        let mut columns = Vec::new();

        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            return Ok(columns);
        }
        *idx += 1;

        while *idx < tokens.len() {
            if matches!(tokens.get(*idx), Some(Token::RParen)) {
                *idx += 1;
                break;
            }

            if matches!(tokens.get(*idx), Some(Token::Comma)) {
                *idx += 1;
                continue;
            }

            let col_name = Self::parse_identifier(tokens, idx)?;

            let mut data_type = String::new();
            while *idx < tokens.len() {
                match tokens.get(*idx) {
                    Some(Token::Word(w)) => {
                        let upper = w.value.to_uppercase();
                        if upper == "HIERARCHICAL" || upper == "DEFAULT" {
                            break;
                        }
                        if !data_type.is_empty() {
                            data_type.push(' ');
                        }
                        data_type.push_str(&w.value);
                        *idx += 1;
                    }
                    Some(Token::LParen) => {
                        data_type.push('(');
                        *idx += 1;
                        let mut paren_depth = 1;
                        while *idx < tokens.len() && paren_depth > 0 {
                            match tokens.get(*idx) {
                                Some(Token::LParen) => {
                                    data_type.push('(');
                                    paren_depth += 1;
                                }
                                Some(Token::RParen) => {
                                    paren_depth -= 1;
                                    if paren_depth > 0 {
                                        data_type.push(')');
                                    }
                                }
                                Some(Token::Comma) if paren_depth == 1 => {
                                    data_type.push(',');
                                }
                                Some(Token::Number(n, _)) => {
                                    data_type.push_str(n);
                                }
                                Some(Token::Word(w)) => {
                                    data_type.push_str(&w.value);
                                }
                                _ => {}
                            }
                            *idx += 1;
                        }
                        data_type.push(')');
                    }
                    _ => break,
                }
            }

            let is_hierarchical = ParserHelpers::consume_keyword(tokens, idx, "HIERARCHICAL");

            let default_value = if ParserHelpers::consume_keyword(tokens, idx, "DEFAULT") {
                Some(Self::parse_default_value(tokens, idx)?)
            } else {
                None
            };

            columns.push(DictionaryColumnDef {
                name: col_name,
                data_type,
                is_hierarchical,
                default_value,
            });
        }

        Ok(columns)
    }

    fn parse_default_value(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        let mut value = String::new();
        match tokens.get(*idx) {
            Some(Token::Number(n, _)) => {
                value.push_str(n);
                *idx += 1;
            }
            Some(Token::SingleQuotedString(s)) => {
                value.push('\'');
                value.push_str(s);
                value.push('\'');
                *idx += 1;
            }
            Some(Token::Word(w)) => {
                value.push_str(&w.value);
                *idx += 1;
            }
            _ => {}
        }
        Ok(value)
    }

    fn parse_primary_key_columns(tokens: &[&Token], idx: &mut usize) -> Result<Vec<String>> {
        let mut columns = Vec::new();

        if matches!(tokens.get(*idx), Some(Token::LParen)) {
            *idx += 1;
            while *idx < tokens.len() {
                if matches!(tokens.get(*idx), Some(Token::RParen)) {
                    *idx += 1;
                    break;
                }
                if matches!(tokens.get(*idx), Some(Token::Comma)) {
                    *idx += 1;
                    continue;
                }
                if let Some(Token::Word(w)) = tokens.get(*idx) {
                    columns.push(w.value.clone());
                    *idx += 1;
                } else {
                    break;
                }
            }
        } else if let Some(Token::Word(w)) = tokens.get(*idx) {
            columns.push(w.value.clone());
            *idx += 1;
        }

        Ok(columns)
    }

    fn parse_dictionary_source(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<crate::validator::DictionarySourceDef> {
        use crate::validator::DictionarySourceDef;

        let mut source = DictionarySourceDef::default();

        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            return Ok(source);
        }
        *idx += 1;

        if let Some(Token::Word(w)) = tokens.get(*idx) {
            source.source_type = w.value.to_uppercase();
            *idx += 1;
        }

        if matches!(tokens.get(*idx), Some(Token::LParen)) {
            *idx += 1;
            while *idx < tokens.len() {
                if matches!(tokens.get(*idx), Some(Token::RParen)) {
                    *idx += 1;
                    break;
                }
                if let Some(Token::Word(w)) = tokens.get(*idx) {
                    if w.value.eq_ignore_ascii_case("TABLE") {
                        *idx += 1;
                        if let Some(Token::SingleQuotedString(s)) = tokens.get(*idx) {
                            source.table = Some(s.clone());
                            *idx += 1;
                        } else if let Some(Token::Word(w)) = tokens.get(*idx) {
                            source.table = Some(w.value.clone());
                            *idx += 1;
                        }
                    } else {
                        *idx += 1;
                    }
                } else {
                    *idx += 1;
                }
            }
        }

        if matches!(tokens.get(*idx), Some(Token::RParen)) {
            *idx += 1;
        }

        Ok(source)
    }

    fn parse_dictionary_layout(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<crate::validator::DictionaryLayoutDef> {
        use crate::validator::DictionaryLayoutDef;

        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            return Ok(DictionaryLayoutDef::default());
        }
        *idx += 1;

        let layout = if let Some(Token::Word(w)) = tokens.get(*idx) {
            let upper = w.value.to_uppercase();
            *idx += 1;

            if matches!(tokens.get(*idx), Some(Token::LParen)) {
                *idx += 1;
                while *idx < tokens.len() && !matches!(tokens.get(*idx), Some(Token::RParen)) {
                    *idx += 1;
                }
                if matches!(tokens.get(*idx), Some(Token::RParen)) {
                    *idx += 1;
                }
            }

            match upper.as_str() {
                "FLAT" => DictionaryLayoutDef::Flat,
                "HASHED" => DictionaryLayoutDef::Hashed,
                "RANGE_HASHED" => DictionaryLayoutDef::RangeHashed,
                "CACHE" => DictionaryLayoutDef::Cache,
                "COMPLEX_KEY_HASHED" => DictionaryLayoutDef::ComplexKeyHashed,
                "COMPLEX_KEY_CACHE" => DictionaryLayoutDef::ComplexKeyCache,
                "DIRECT" => DictionaryLayoutDef::Direct,
                _ => DictionaryLayoutDef::Flat,
            }
        } else {
            DictionaryLayoutDef::default()
        };

        if matches!(tokens.get(*idx), Some(Token::RParen)) {
            *idx += 1;
        }

        Ok(layout)
    }

    fn parse_dictionary_lifetime(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<crate::validator::DictionaryLifetimeDef> {
        use crate::validator::DictionaryLifetimeDef;

        let mut lifetime = DictionaryLifetimeDef::default();

        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            if let Some(Token::Number(n, _)) = tokens.get(*idx) {
                if let Ok(val) = n.parse::<u64>() {
                    lifetime.min_seconds = val;
                    lifetime.max_seconds = val;
                }
                *idx += 1;
            }
            return Ok(lifetime);
        }
        *idx += 1;

        while *idx < tokens.len() {
            if matches!(tokens.get(*idx), Some(Token::RParen)) {
                *idx += 1;
                break;
            }

            if let Some(Token::Word(w)) = tokens.get(*idx) {
                let upper = w.value.to_uppercase();
                *idx += 1;

                if let Some(Token::Number(n, _)) = tokens.get(*idx) {
                    if let Ok(val) = n.parse::<u64>() {
                        match upper.as_str() {
                            "MIN" => lifetime.min_seconds = val,
                            "MAX" => lifetime.max_seconds = val,
                            _ => {}
                        }
                    }
                    *idx += 1;
                }
            } else if let Some(Token::Number(n, _)) = tokens.get(*idx) {
                if let Ok(val) = n.parse::<u64>() {
                    lifetime.min_seconds = val;
                    lifetime.max_seconds = val;
                }
                *idx += 1;
            } else {
                *idx += 1;
            }
        }

        Ok(lifetime)
    }

    fn parse_system_command(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
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

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
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

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
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

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
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
        assert!(
            ClickHouseParser::try_parse(&filtered, sql_with_type)
                .unwrap()
                .is_some()
        );

        let sql_without_type = "CREATE INDEX idx ON t (c)";
        let tokens = tokenize(sql_without_type);
        let filtered: Vec<&Token> = filter_tokens(&tokens);
        assert!(
            ClickHouseParser::try_parse(&filtered, sql_without_type)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn test_passthrough_quota() {
        let sql = "CREATE QUOTA my_quota";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
        assert!(matches!(
            result,
            Some(CustomStatement::ClickHouseQuota { .. })
        ));
    }

    #[test]
    fn test_passthrough_row_policy() {
        let sql = "CREATE ROW POLICY my_policy ON table";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
        assert!(matches!(
            result,
            Some(CustomStatement::ClickHouseRowPolicy { .. })
        ));
    }

    #[test]
    fn test_passthrough_show() {
        let sql = "SHOW TABLES";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
        assert!(matches!(
            result,
            Some(CustomStatement::ClickHouseShow { .. })
        ));
    }

    #[test]
    fn test_system_command() {
        let sql = "SYSTEM FLUSH LOGS";
        let tokens = tokenize(sql);
        let filtered: Vec<&Token> = filter_tokens(&tokens);

        let result = ClickHouseParser::try_parse(&filtered, sql).unwrap();
        assert!(matches!(
            result,
            Some(CustomStatement::ClickHouseSystem {
                command: ClickHouseSystemCommand::FlushLogs
            })
        ));
    }

    #[test]
    fn test_strip_inline_indexes() {
        let sql = "CREATE TABLE fulltext_search (
                id UInt64,
                content String,
                INDEX idx_content content TYPE inverted GRANULARITY 1
            ) ENGINE = MergeTree()
            ORDER BY id";

        let stripped = super::strip_inline_indexes(sql);
        assert!(
            stripped.contains("id UInt64"),
            "Should contain id column: {}",
            stripped
        );
        assert!(
            stripped.contains("content String"),
            "Should contain content column: {}",
            stripped
        );
        assert!(
            !stripped.contains("INDEX"),
            "Should NOT contain INDEX: {}",
            stripped
        );
        assert!(
            stripped.contains("ENGINE"),
            "Should contain ENGINE: {}",
            stripped
        );
    }
}
