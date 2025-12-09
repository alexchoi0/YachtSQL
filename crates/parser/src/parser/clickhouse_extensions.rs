use sqlparser::ast::ObjectName;
use sqlparser::tokenizer::Token;
use yachtsql_core::error::{Error, Result};

use super::helpers::ParserHelpers;
use crate::validator::{ClickHouseSystemCommand, ClickHouseTtlOperation, CustomStatement};

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

#[allow(dead_code)]
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

#[allow(dead_code)]
fn has_complex_engine(sql: &str) -> bool {
    if let Some(engine_pos) = find_engine_clause(sql) {
        let after_engine = &sql[engine_pos + "ENGINE".len()..].trim_start();
        if let Some(after_eq) = after_engine.strip_prefix('=') {
            let after_eq = after_eq.trim_start();
            let engine_names = [
                "Distributed",
                "Merge",
                "Buffer",
                "GenerateRandom",
                "Set",
                "Null",
            ];
            for name in &engine_names {
                if after_eq.to_uppercase().starts_with(&name.to_uppercase()) {
                    return true;
                }
            }
        }
    }
    false
}

#[allow(dead_code)]
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

#[allow(dead_code)]
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

                return format!(
                    "{} ENGINE = Memory {}",
                    before_engine.trim_end(),
                    after_params.trim_start()
                );
            } else {
                return format!("{} ENGINE = Memory {}", before_engine.trim_end(), rest);
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

    fn parse(&self, _tokens: &[&Token], _sql: &str) -> Result<Option<CustomStatement>> {
        Ok(None)
    }
}

static CREATE_TABLE_WITH_COMPLEX_ENGINE_PARSER: CreateTableWithComplexEngineParser =
    CreateTableWithComplexEngineParser;

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
static FUNCTION_PARSER: FunctionParser = FunctionParser;
static CREATE_DATABASE_ENGINE_PARSER: CreateDatabaseEngineParser = CreateDatabaseEngineParser;
static CREATE_DATABASE_COMMENT_PARSER: CreateDatabaseCommentParser = CreateDatabaseCommentParser;
static USE_PARSER: UseParser = UseParser;

static PARSERS: &[&dyn ClickHouseStatementParser] = &[
    &CREATE_DICT_PARSER,
    &CREATE_INDEX_PARSER,
    &CREATE_DATABASE_ENGINE_PARSER,
    &CREATE_DATABASE_COMMENT_PARSER,
    &PASSTHROUGH_RENAME_DATABASE,
    &USE_PARSER,
    &ALTER_CODEC_PARSER,
    &ALTER_TTL_PARSER,
    &SYSTEM_PARSER,
    &PASSTHROUGH_QUOTA,
    &PASSTHROUGH_ROW_POLICY,
    &PASSTHROUGH_SETTINGS_PROFILE,
    &PASSTHROUGH_MATERIALIZED_VIEW,
    &PASSTHROUGH_DICTIONARY,
    &PASSTHROUGH_SHOW,
    &FUNCTION_PARSER,
    &CREATE_TABLE_WITH_COMPLEX_ENGINE_PARSER,
    &CREATE_TABLE_WITH_PARTITION_PARSER,
    &CREATE_TABLE_WITH_SETTINGS_PARSER,
    &CREATE_TABLE_WITH_PROJECTION_PARSER,
    &ALTER_TABLE_MODIFY_PARSER,
    &PASSTHROUGH_PROJECTION,
    &PASSTHROUGH_ALTER_USER,
    &GRANT_ROLE_PARSER,
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
}
