use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;

use serde::{Deserialize, Serialize};
use yachtsql_core::error::Result;

pub type Position = u16;

#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize, Default,
)]
pub enum Weight {
    A,

    B,

    C,

    #[default]
    D,
}

impl Weight {
    pub fn from_char(c: char) -> Option<Self> {
        match c.to_ascii_uppercase() {
            'A' => Some(Weight::A),
            'B' => Some(Weight::B),
            'C' => Some(Weight::C),
            'D' => Some(Weight::D),
            _ => None,
        }
    }

    pub fn to_char(self) -> char {
        match self {
            Weight::A => 'A',
            Weight::B => 'B',
            Weight::C => 'C',
            Weight::D => 'D',
        }
    }

    pub fn rank_weight(self) -> f64 {
        match self {
            Weight::A => 1.0,
            Weight::B => 0.4,
            Weight::C => 0.2,
            Weight::D => 0.1,
        }
    }
}

impl fmt::Display for Weight {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_char())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LexemeEntry {
    pub positions: Vec<(Position, Weight)>,
}

impl LexemeEntry {
    pub fn new() -> Self {
        Self {
            positions: Vec::new(),
        }
    }

    pub fn add_position(&mut self, pos: Position, weight: Weight) {
        let entry = (pos, weight);
        match self.positions.binary_search_by_key(&pos, |&(p, _)| p) {
            Ok(_) => {}
            Err(idx) => self.positions.insert(idx, entry),
        }
    }

    pub fn merge(&mut self, other: &LexemeEntry) {
        for &(pos, weight) in &other.positions {
            self.add_position(pos, weight);
        }
    }

    pub fn set_weight(&mut self, weight: Weight) {
        for entry in &mut self.positions {
            entry.1 = weight;
        }
    }

    pub fn strip(&mut self) {
        self.positions.clear();
    }
}

impl Default for LexemeEntry {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct TSVector {
    pub lexemes: BTreeMap<String, LexemeEntry>,
}

impl TSVector {
    pub fn new() -> Self {
        Self {
            lexemes: BTreeMap::new(),
        }
    }

    pub fn add_lexeme(&mut self, lexeme: &str, pos: Position, weight: Weight) {
        let normalized = normalize_lexeme(lexeme);
        if normalized.is_empty() {
            return;
        }

        self.lexemes
            .entry(normalized)
            .or_default()
            .add_position(pos, weight);
    }

    pub fn add_lexeme_no_pos(&mut self, lexeme: &str) {
        let normalized = normalize_lexeme(lexeme);
        if normalized.is_empty() {
            return;
        }

        self.lexemes.entry(normalized).or_default();
    }

    pub fn len(&self) -> usize {
        self.lexemes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.lexemes.is_empty()
    }

    pub fn contains(&self, lexeme: &str) -> bool {
        self.lexemes.contains_key(&normalize_lexeme(lexeme))
    }

    pub fn merge(&mut self, other: &TSVector) {
        for (lexeme, entry) in &other.lexemes {
            self.lexemes.entry(lexeme.clone()).or_default().merge(entry);
        }
    }

    pub fn set_weight(&mut self, weight: Weight) {
        for entry in self.lexemes.values_mut() {
            entry.set_weight(weight);
        }
    }

    pub fn strip(&mut self) {
        for entry in self.lexemes.values_mut() {
            entry.strip();
        }
    }

    pub fn get_positions(&self, lexeme: &str) -> Option<&[(Position, Weight)]> {
        self.lexemes
            .get(&normalize_lexeme(lexeme))
            .map(|e| e.positions.as_slice())
    }
}

impl fmt::Display for TSVector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let parts: Vec<String> = self
            .lexemes
            .iter()
            .map(|(lexeme, entry)| {
                if entry.positions.is_empty() {
                    format!("'{}'", lexeme)
                } else {
                    let positions: Vec<String> = entry
                        .positions
                        .iter()
                        .map(|(pos, weight)| {
                            if *weight == Weight::D {
                                pos.to_string()
                            } else {
                                format!("{}{}", pos, weight)
                            }
                        })
                        .collect();
                    format!("'{}':{}", lexeme, positions.join(","))
                }
            })
            .collect();
        write!(f, "{}", parts.join(" "))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum TSQueryOp {
    Lexeme(String, Option<Weight>),

    Prefix(String),

    And(Box<TSQuery>, Box<TSQuery>),

    Or(Box<TSQuery>, Box<TSQuery>),

    Not(Box<TSQuery>),

    Phrase(Box<TSQuery>, Box<TSQuery>, i32),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TSQuery {
    pub expr: Option<TSQueryOp>,
}

impl TSQuery {
    pub fn new() -> Self {
        Self { expr: None }
    }

    pub fn lexeme(s: &str) -> Self {
        let normalized = normalize_lexeme(s);
        if normalized.is_empty() {
            Self::new()
        } else {
            Self {
                expr: Some(TSQueryOp::Lexeme(normalized, None)),
            }
        }
    }

    pub fn lexeme_weighted(s: &str, weight: Weight) -> Self {
        let normalized = normalize_lexeme(s);
        if normalized.is_empty() {
            Self::new()
        } else {
            Self {
                expr: Some(TSQueryOp::Lexeme(normalized, Some(weight))),
            }
        }
    }

    pub fn prefix(s: &str) -> Self {
        let normalized = normalize_lexeme(s);
        if normalized.is_empty() {
            Self::new()
        } else {
            Self {
                expr: Some(TSQueryOp::Prefix(normalized)),
            }
        }
    }

    pub fn and(self, other: TSQuery) -> TSQuery {
        match (self.expr, other.expr) {
            (None, other) => TSQuery { expr: other },
            (this, None) => TSQuery { expr: this },
            (Some(a), Some(b)) => TSQuery {
                expr: Some(TSQueryOp::And(
                    Box::new(TSQuery { expr: Some(a) }),
                    Box::new(TSQuery { expr: Some(b) }),
                )),
            },
        }
    }

    pub fn or(self, other: TSQuery) -> TSQuery {
        match (self.expr, other.expr) {
            (None, other) => TSQuery { expr: other },
            (this, None) => TSQuery { expr: this },
            (Some(a), Some(b)) => TSQuery {
                expr: Some(TSQueryOp::Or(
                    Box::new(TSQuery { expr: Some(a) }),
                    Box::new(TSQuery { expr: Some(b) }),
                )),
            },
        }
    }

    pub fn negate(self) -> TSQuery {
        match self.expr {
            None => TSQuery::new(),
            Some(e) => TSQuery {
                expr: Some(TSQueryOp::Not(Box::new(TSQuery { expr: Some(e) }))),
            },
        }
    }

    pub fn phrase(self, other: TSQuery) -> TSQuery {
        self.phrase_distance(other, 1)
    }

    pub fn phrase_distance(self, other: TSQuery, distance: i32) -> TSQuery {
        match (self.expr, other.expr) {
            (None, _) | (_, None) => TSQuery::new(),
            (Some(a), Some(b)) => TSQuery {
                expr: Some(TSQueryOp::Phrase(
                    Box::new(TSQuery { expr: Some(a) }),
                    Box::new(TSQuery { expr: Some(b) }),
                    distance,
                )),
            },
        }
    }

    pub fn is_empty(&self) -> bool {
        self.expr.is_none()
    }

    pub fn matches(&self, vector: &TSVector) -> bool {
        match &self.expr {
            None => false,
            Some(op) => Self::match_op(op, vector),
        }
    }

    fn match_op(op: &TSQueryOp, vector: &TSVector) -> bool {
        match op {
            TSQueryOp::Lexeme(lexeme, weight) => {
                if let Some(entry) = vector.lexemes.get(lexeme) {
                    match weight {
                        None => true,
                        Some(w) => entry
                            .positions
                            .iter()
                            .any(|(_, entry_weight)| entry_weight == w),
                    }
                } else {
                    false
                }
            }
            TSQueryOp::Prefix(prefix) => vector
                .lexemes
                .keys()
                .any(|k| k.starts_with(prefix.as_str())),
            TSQueryOp::And(a, b) => {
                a.expr
                    .as_ref()
                    .map(|e| Self::match_op(e, vector))
                    .unwrap_or(false)
                    && b.expr
                        .as_ref()
                        .map(|e| Self::match_op(e, vector))
                        .unwrap_or(false)
            }
            TSQueryOp::Or(a, b) => {
                a.expr
                    .as_ref()
                    .map(|e| Self::match_op(e, vector))
                    .unwrap_or(false)
                    || b.expr
                        .as_ref()
                        .map(|e| Self::match_op(e, vector))
                        .unwrap_or(false)
            }
            TSQueryOp::Not(q) => !q
                .expr
                .as_ref()
                .map(|e| Self::match_op(e, vector))
                .unwrap_or(false),
            TSQueryOp::Phrase(a, b, distance) => Self::match_phrase(a, b, *distance, vector),
        }
    }

    fn match_phrase(a: &TSQuery, b: &TSQuery, distance: i32, vector: &TSVector) -> bool {
        let a_positions = Self::get_positions(&a.expr, vector);
        let b_positions = Self::get_positions(&b.expr, vector);

        for &(a_pos, _) in &a_positions {
            for &(b_pos, _) in &b_positions {
                let actual_distance = (b_pos as i32) - (a_pos as i32);
                if actual_distance == distance {
                    return true;
                }
            }
        }
        false
    }

    fn get_positions(expr: &Option<TSQueryOp>, vector: &TSVector) -> Vec<(Position, Weight)> {
        match expr {
            Some(TSQueryOp::Lexeme(lexeme, _)) => vector
                .lexemes
                .get(lexeme)
                .map(|e| e.positions.clone())
                .unwrap_or_default(),
            _ => Vec::new(),
        }
    }
}

impl Default for TSQuery {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for TSQuery {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.expr {
            None => write!(f, ""),
            Some(op) => write!(f, "{}", format_tsquery_op(op)),
        }
    }
}

fn format_tsquery_op(op: &TSQueryOp) -> String {
    match op {
        TSQueryOp::Lexeme(lexeme, weight) => match weight {
            Some(w) => format!("'{}':{}", lexeme, w),
            None => format!("'{}'", lexeme),
        },
        TSQueryOp::Prefix(prefix) => format!("'{}':*", prefix),
        TSQueryOp::And(a, b) => {
            let a_str = a.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            let b_str = b.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            format!("{} & {}", a_str, b_str)
        }
        TSQueryOp::Or(a, b) => {
            let a_str = a.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            let b_str = b.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            format!("( {} | {} )", a_str, b_str)
        }
        TSQueryOp::Not(q) => {
            let q_str = q.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            format!("!{}", q_str)
        }
        TSQueryOp::Phrase(a, b, distance) => {
            let a_str = a.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            let b_str = b.expr.as_ref().map(format_tsquery_op).unwrap_or_default();
            if *distance == 1 {
                format!("{} <-> {}", a_str, b_str)
            } else {
                format!("{} <{}> {}", a_str, distance, b_str)
            }
        }
    }
}

fn normalize_lexeme(word: &str) -> String {
    word.chars()
        .filter(|c| c.is_alphanumeric())
        .collect::<String>()
        .to_lowercase()
}

fn tokenize(text: &str) -> Vec<(String, Position)> {
    let mut tokens = Vec::new();
    let mut pos: Position = 1;

    for word in text.split(|c: char| !c.is_alphanumeric() && c != '\'') {
        let normalized = normalize_lexeme(word);
        if !normalized.is_empty() && !is_stop_word(&normalized) {
            tokens.push((normalized, pos));
            pos += 1;
        }
    }

    tokens
}

fn is_stop_word(word: &str) -> bool {
    const STOP_WORDS: &[&str] = &[
        "a", "an", "and", "are", "as", "at", "be", "by", "for", "from", "has", "he", "in", "is",
        "it", "its", "of", "on", "or", "that", "the", "to", "was", "were", "will", "with",
    ];
    STOP_WORDS.contains(&word)
}

pub fn to_tsvector(text: &str) -> TSVector {
    let mut vector = TSVector::new();
    for (word, pos) in tokenize(text) {
        vector.add_lexeme(&word, pos, Weight::D);
    }
    vector
}

pub fn to_tsvector_with_config(_config: &str, text: &str) -> TSVector {
    to_tsvector(text)
}

pub fn to_tsquery(text: &str) -> Result<TSQuery> {
    parse_tsquery(text)
}

pub fn plainto_tsquery(text: &str) -> TSQuery {
    let tokens = tokenize(text);
    let mut result = TSQuery::new();

    for (word, _) in tokens {
        let term_query = TSQuery::lexeme(&word);
        result = result.and(term_query);
    }

    result
}

pub fn phraseto_tsquery(text: &str) -> TSQuery {
    let tokens = tokenize(text);
    let mut result = TSQuery::new();

    for (word, _) in tokens {
        let term_query = TSQuery::lexeme(&word);
        if result.is_empty() {
            result = term_query;
        } else {
            result = result.phrase(term_query);
        }
    }

    result
}

pub fn websearch_to_tsquery(text: &str) -> TSQuery {
    parse_websearch(text)
}

fn parse_tsquery(text: &str) -> Result<TSQuery> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(TSQuery::new());
    }

    parse_or_expr(text)
}

fn parse_or_expr(text: &str) -> Result<TSQuery> {
    let parts: Vec<&str> = text.split('|').collect();
    if parts.len() == 1 {
        return parse_and_expr(text);
    }

    let mut result = TSQuery::new();
    for part in parts {
        let parsed = parse_and_expr(part.trim())?;
        result = result.or(parsed);
    }
    Ok(result)
}

fn parse_and_expr(text: &str) -> Result<TSQuery> {
    let parts: Vec<&str> = text.split('&').collect();
    if parts.len() == 1 {
        return parse_term(text);
    }

    let mut result = TSQuery::new();
    for part in parts {
        let parsed = parse_term(part.trim())?;
        result = result.and(parsed);
    }
    Ok(result)
}

fn parse_term(text: &str) -> Result<TSQuery> {
    let text = text.trim();
    if text.is_empty() {
        return Ok(TSQuery::new());
    }

    if let Some(stripped) = text.strip_prefix('!') {
        let inner = parse_term(stripped)?;
        return Ok(inner.negate());
    }

    if let Some(inner) = text.strip_prefix('(').and_then(|s| s.strip_suffix(')')) {
        return parse_or_expr(inner);
    }

    if let Some(after_quote) = text.strip_prefix('\'') {
        let end_quote = after_quote.find('\'');
        if let Some(end) = end_quote {
            let lexeme = &after_quote[..end];
            let rest = &after_quote[end + 1..];

            if let Some(suffix) = rest.strip_prefix(':') {
                if suffix == "*" {
                    return Ok(TSQuery::prefix(lexeme));
                } else if suffix.len() == 1 {
                    if let Some(weight) = Weight::from_char(suffix.chars().next().unwrap()) {
                        return Ok(TSQuery::lexeme_weighted(lexeme, weight));
                    }
                }
            }

            return Ok(TSQuery::lexeme(lexeme));
        }
    }

    if let Some(pos) = text.find("<->") {
        let left = parse_term(&text[..pos])?;
        let right = parse_term(&text[pos + 3..])?;
        return Ok(left.phrase(right));
    }

    if let Some(start) = text.find('<') {
        if let Some(end) = text[start..].find('>') {
            let distance_str = &text[start + 1..start + end];
            if let Ok(distance) = distance_str.parse::<i32>() {
                let left = parse_term(&text[..start])?;
                let right = parse_term(&text[start + end + 1..])?;
                return Ok(left.phrase_distance(right, distance));
            }
        }
    }

    let normalized = normalize_lexeme(text);
    if normalized.is_empty() {
        Ok(TSQuery::new())
    } else if let Some(word) = text.strip_suffix(":*") {
        Ok(TSQuery::prefix(word))
    } else {
        Ok(TSQuery::lexeme(&normalized))
    }
}

fn parse_websearch(text: &str) -> TSQuery {
    let mut result = TSQuery::new();
    let mut chars = text.chars().peekable();
    let mut current_word = String::new();
    let mut in_quotes = false;
    let mut phrase_words: Vec<String> = Vec::new();
    let mut negate_next = false;

    while let Some(c) = chars.next() {
        match c {
            '"' => {
                if in_quotes {
                    if !phrase_words.is_empty() {
                        let mut phrase_query = TSQuery::new();
                        for word in phrase_words.drain(..) {
                            let term = TSQuery::lexeme(&word);
                            if phrase_query.is_empty() {
                                phrase_query = term;
                            } else {
                                phrase_query = phrase_query.phrase(term);
                            }
                        }
                        if negate_next {
                            phrase_query = phrase_query.negate();
                            negate_next = false;
                        }
                        result = result.and(phrase_query);
                    }
                    in_quotes = false;
                } else {
                    if !current_word.is_empty() {
                        let word = normalize_lexeme(&current_word);
                        if !word.is_empty() && !is_stop_word(&word) {
                            let term = TSQuery::lexeme(&word);
                            let term = if negate_next {
                                negate_next = false;
                                term.negate()
                            } else {
                                term
                            };
                            result = result.and(term);
                        }
                        current_word.clear();
                    }
                    in_quotes = true;
                }
            }
            '-' if !in_quotes && current_word.is_empty() => {
                negate_next = true;
            }
            ' ' | '\t' | '\n' => {
                if in_quotes {
                    if !current_word.is_empty() {
                        let word = normalize_lexeme(&current_word);
                        if !word.is_empty() && !is_stop_word(&word) {
                            phrase_words.push(word);
                        }
                        current_word.clear();
                    }
                } else if !current_word.is_empty() {
                    let upper = current_word.to_uppercase();
                    if upper == "OR" {
                        while chars.peek() == Some(&' ') || chars.peek() == Some(&'\t') {
                            chars.next();
                        }

                        let remaining: String = chars.collect();
                        let next = parse_websearch(&remaining);
                        result = result.or(next);
                        return result;
                    }

                    let word = normalize_lexeme(&current_word);
                    if !word.is_empty() && !is_stop_word(&word) {
                        let term = TSQuery::lexeme(&word);
                        let term = if negate_next {
                            negate_next = false;
                            term.negate()
                        } else {
                            term
                        };
                        result = result.and(term);
                    }
                    current_word.clear();
                }
            }
            _ => {
                current_word.push(c);
            }
        }
    }

    if !current_word.is_empty() {
        let word = normalize_lexeme(&current_word);
        if !word.is_empty() && !is_stop_word(&word) {
            let term = TSQuery::lexeme(&word);
            let term = if negate_next { term.negate() } else { term };
            result = result.and(term);
        }
    }

    result
}

pub fn ts_rank(vector: &TSVector, query: &TSQuery) -> f64 {
    ts_rank_with_weights(vector, query, &[0.1, 0.2, 0.4, 1.0])
}

pub fn ts_rank_with_weights(vector: &TSVector, query: &TSQuery, weights: &[f64; 4]) -> f64 {
    if vector.is_empty() || query.is_empty() {
        return 0.0;
    }

    let query_lexemes = collect_query_lexemes(&query.expr);
    if query_lexemes.is_empty() {
        return 0.0;
    }

    let mut score = 0.0;
    let mut match_count = 0;

    for lexeme in &query_lexemes {
        if let Some(entry) = vector.lexemes.get(lexeme) {
            match_count += 1;

            for (_, weight) in &entry.positions {
                let weight_idx = match weight {
                    Weight::D => 0,
                    Weight::C => 1,
                    Weight::B => 2,
                    Weight::A => 3,
                };
                score += weights[weight_idx];
            }

            if entry.positions.is_empty() {
                score += weights[0];
            }
        }
    }

    if match_count == 0 {
        return 0.0;
    }

    let doc_len = vector.len() as f64;
    let query_len = query_lexemes.len() as f64;

    score / (1.0 + doc_len.ln()) / query_len
}

pub fn ts_rank_cd(vector: &TSVector, query: &TSQuery) -> f64 {
    ts_rank_cd_with_weights(vector, query, &[0.1, 0.2, 0.4, 1.0])
}

pub fn ts_rank_cd_with_weights(vector: &TSVector, query: &TSQuery, weights: &[f64; 4]) -> f64 {
    if vector.is_empty() || query.is_empty() {
        return 0.0;
    }

    let query_lexemes = collect_query_lexemes(&query.expr);
    if query_lexemes.is_empty() {
        return 0.0;
    }

    let mut all_positions: Vec<(Position, Weight, &str)> = Vec::new();
    for lexeme in &query_lexemes {
        if let Some(entry) = vector.lexemes.get(lexeme) {
            for &(pos, weight) in &entry.positions {
                all_positions.push((pos, weight, lexeme.as_str()));
            }
        }
    }

    if all_positions.is_empty() {
        return 0.0;
    }

    all_positions.sort_by_key(|&(pos, _, _)| pos);

    let mut score = 0.0;
    let query_set: BTreeSet<&str> = query_lexemes.iter().map(|s| s.as_str()).collect();

    let mut i = 0;
    while i < all_positions.len() {
        let mut found: HashMap<&str, (Position, Weight)> = HashMap::new();
        let mut j = i;

        while j < all_positions.len() && found.len() < query_set.len() {
            let (pos, weight, lexeme) = all_positions[j];
            found.entry(lexeme).or_insert((pos, weight));
            j += 1;
        }

        if found.len() == query_set.len() {
            let positions: Vec<Position> = found.values().map(|&(p, _)| p).collect();
            let min_pos = *positions.iter().min().unwrap();
            let max_pos = *positions.iter().max().unwrap();
            let cover_width = (max_pos - min_pos + 1) as f64;

            let cover_score = query_set.len() as f64 / cover_width;

            let weight_sum: f64 = found
                .values()
                .map(|&(_, w)| {
                    let idx = match w {
                        Weight::D => 0,
                        Weight::C => 1,
                        Weight::B => 2,
                        Weight::A => 3,
                    };
                    weights[idx]
                })
                .sum();

            score += cover_score * weight_sum / query_set.len() as f64;
        }

        i += 1;
    }

    let doc_len = vector.len() as f64;
    score / (1.0 + doc_len.ln())
}

fn collect_query_lexemes(expr: &Option<TSQueryOp>) -> Vec<String> {
    let mut lexemes = Vec::new();
    if let Some(op) = expr {
        collect_lexemes_from_op(op, &mut lexemes);
    }
    lexemes
}

fn collect_lexemes_from_op(op: &TSQueryOp, lexemes: &mut Vec<String>) {
    match op {
        TSQueryOp::Lexeme(s, _) => lexemes.push(s.clone()),
        TSQueryOp::Prefix(s) => lexemes.push(s.clone()),
        TSQueryOp::And(a, b) | TSQueryOp::Or(a, b) | TSQueryOp::Phrase(a, b, _) => {
            if let Some(ref op) = a.expr {
                collect_lexemes_from_op(op, lexemes);
            }
            if let Some(ref op) = b.expr {
                collect_lexemes_from_op(op, lexemes);
            }
        }
        TSQueryOp::Not(q) => {
            if let Some(ref op) = q.expr {
                collect_lexemes_from_op(op, lexemes);
            }
        }
    }
}

pub struct HeadlineOptions {
    pub start_sel: String,

    pub stop_sel: String,

    pub max_words: usize,

    pub min_words: usize,

    pub short_word: usize,

    pub fragment_delimiter: String,

    pub max_fragments: usize,
}

impl Default for HeadlineOptions {
    fn default() -> Self {
        Self {
            start_sel: "<b>".to_string(),
            stop_sel: "</b>".to_string(),
            max_words: 35,
            min_words: 15,
            short_word: 3,
            fragment_delimiter: " ... ".to_string(),
            max_fragments: 0,
        }
    }
}

pub fn ts_headline(document: &str, query: &TSQuery, options: &HeadlineOptions) -> String {
    if query.is_empty() {
        let words: Vec<&str> = document.split_whitespace().collect();
        let take = words.len().min(options.max_words);
        return words[..take].join(" ");
    }

    let query_lexemes: BTreeSet<String> = collect_query_lexemes(&query.expr).into_iter().collect();

    let words: Vec<&str> = document.split_whitespace().collect();
    if words.is_empty() {
        return String::new();
    }

    let mut match_positions: Vec<usize> = Vec::new();
    for (i, word) in words.iter().enumerate() {
        let normalized = normalize_lexeme(word);
        if query_lexemes.contains(&normalized)
            || query_lexemes.iter().any(|q| normalized.starts_with(q))
        {
            match_positions.push(i);
        }
    }

    if match_positions.is_empty() {
        let take = words.len().min(options.max_words);
        return words[..take].join(" ");
    }

    let center = match_positions[match_positions.len() / 2];
    let half_window = options.max_words / 2;
    let start = center.saturating_sub(half_window);
    let end = (center + half_window).min(words.len());

    let mut result = String::new();
    for i in start..end {
        if i > start {
            result.push(' ');
        }

        let word = words[i];
        let normalized = normalize_lexeme(word);
        let should_highlight = normalized.len() > options.short_word
            && (query_lexemes.contains(&normalized)
                || query_lexemes.iter().any(|q| normalized.starts_with(q)));

        if should_highlight {
            result.push_str(&options.start_sel);
            result.push_str(word);
            result.push_str(&options.stop_sel);
        } else {
            result.push_str(word);
        }
    }

    result
}

pub fn tsvector_length(vector: &TSVector) -> i64 {
    vector.len() as i64
}

pub fn tsvector_strip(vector: &TSVector) -> TSVector {
    let mut result = vector.clone();
    result.strip();
    result
}

pub fn tsvector_setweight(vector: &TSVector, weight: Weight) -> TSVector {
    let mut result = vector.clone();
    result.set_weight(weight);
    result
}

pub fn tsvector_concat(a: &TSVector, b: &TSVector) -> TSVector {
    let mut result = a.clone();
    result.merge(b);
    result
}

pub fn tsvector_to_string(vector: &TSVector) -> String {
    vector.to_string()
}

pub fn parse_tsvector(s: &str) -> Result<TSVector> {
    let mut vector = TSVector::new();

    let mut chars = s.chars().peekable();

    while chars.peek().is_some() {
        while chars.peek() == Some(&' ') {
            chars.next();
        }

        if chars.peek() != Some(&'\'') {
            break;
        }
        chars.next();

        let mut lexeme = String::new();
        while let Some(&c) = chars.peek() {
            if c == '\'' {
                chars.next();
                break;
            }
            lexeme.push(c);
            chars.next();
        }

        if chars.peek() == Some(&':') {
            chars.next();

            loop {
                let mut pos_str = String::new();
                let mut weight = Weight::D;

                while let Some(&c) = chars.peek() {
                    if c.is_ascii_digit() {
                        pos_str.push(c);
                        chars.next();
                    } else if c.is_ascii_alphabetic() {
                        if let Some(w) = Weight::from_char(c) {
                            weight = w;
                        }
                        chars.next();
                    } else {
                        break;
                    }
                }

                if let Ok(pos) = pos_str.parse::<Position>() {
                    vector.add_lexeme(&lexeme, pos, weight);
                }

                if chars.peek() == Some(&',') {
                    chars.next();
                } else {
                    break;
                }
            }
        } else {
            vector.add_lexeme_no_pos(&lexeme);
        }
    }

    Ok(vector)
}

pub fn tsquery_to_string(query: &TSQuery) -> String {
    query.to_string()
}

pub fn ts_match(tsvector_str: &str, tsquery_str: &str) -> Result<bool> {
    let vector = parse_tsvector(tsvector_str)?;
    let query = to_tsquery(tsquery_str)?;
    Ok(query.matches(&vector))
}

pub fn tsquery_and(left_str: &str, right_str: &str) -> Result<String> {
    let left = to_tsquery(left_str)?;
    let right = to_tsquery(right_str)?;
    Ok(tsquery_to_string(&left.and(right)))
}

pub fn tsquery_negate(query_str: &str) -> Result<String> {
    let query = to_tsquery(query_str)?;
    Ok(tsquery_to_string(&query.negate()))
}

pub fn numnode(query: &TSQuery) -> i64 {
    count_nodes(&query.expr)
}

fn count_nodes(expr: &Option<TSQueryOp>) -> i64 {
    match expr {
        None => 0,
        Some(op) => match op {
            TSQueryOp::Lexeme(_, _) | TSQueryOp::Prefix(_) => 1,
            TSQueryOp::And(a, b) | TSQueryOp::Or(a, b) | TSQueryOp::Phrase(a, b, _) => {
                1 + count_nodes(&a.expr) + count_nodes(&b.expr)
            }
            TSQueryOp::Not(q) => 1 + count_nodes(&q.expr),
        },
    }
}

pub fn querytree(query: &TSQuery) -> String {
    match &query.expr {
        None => String::new(),
        Some(op) => format_querytree(op),
    }
}

fn format_querytree(op: &TSQueryOp) -> String {
    match op {
        TSQueryOp::Lexeme(lexeme, _) => format!("'{}'", lexeme),
        TSQueryOp::Prefix(prefix) => format!("'{}':*", prefix),
        TSQueryOp::And(a, b) => {
            let a_str = a.expr.as_ref().map(format_querytree).unwrap_or_default();
            let b_str = b.expr.as_ref().map(format_querytree).unwrap_or_default();
            format!("( {} & {} )", a_str, b_str)
        }
        TSQueryOp::Or(a, b) => {
            let a_str = a.expr.as_ref().map(format_querytree).unwrap_or_default();
            let b_str = b.expr.as_ref().map(format_querytree).unwrap_or_default();
            format!("( {} | {} )", a_str, b_str)
        }
        TSQueryOp::Not(q) => {
            let q_str = q.expr.as_ref().map(format_querytree).unwrap_or_default();
            format!("!( {} )", q_str)
        }
        TSQueryOp::Phrase(a, b, distance) => {
            let a_str = a.expr.as_ref().map(format_querytree).unwrap_or_default();
            let b_str = b.expr.as_ref().map(format_querytree).unwrap_or_default();
            format!("( {} <{}> {} )", a_str, distance, b_str)
        }
    }
}

pub fn ts_rewrite(query: &TSQuery, old_query: &TSQuery, new_query: &TSQuery) -> TSQuery {
    TSQuery {
        expr: rewrite_expr(&query.expr, old_query, new_query),
    }
}

fn rewrite_expr(
    expr: &Option<TSQueryOp>,
    old_query: &TSQuery,
    new_query: &TSQuery,
) -> Option<TSQueryOp> {
    let current = TSQuery { expr: expr.clone() };
    if current == *old_query {
        return new_query.expr.clone();
    }

    match expr {
        None => None,
        Some(op) => match op {
            TSQueryOp::Lexeme(_, _) | TSQueryOp::Prefix(_) => expr.clone(),
            TSQueryOp::And(a, b) => Some(TSQueryOp::And(
                Box::new(TSQuery {
                    expr: rewrite_expr(&a.expr, old_query, new_query),
                }),
                Box::new(TSQuery {
                    expr: rewrite_expr(&b.expr, old_query, new_query),
                }),
            )),
            TSQueryOp::Or(a, b) => Some(TSQueryOp::Or(
                Box::new(TSQuery {
                    expr: rewrite_expr(&a.expr, old_query, new_query),
                }),
                Box::new(TSQuery {
                    expr: rewrite_expr(&b.expr, old_query, new_query),
                }),
            )),
            TSQueryOp::Not(q) => Some(TSQueryOp::Not(Box::new(TSQuery {
                expr: rewrite_expr(&q.expr, old_query, new_query),
            }))),
            TSQueryOp::Phrase(a, b, distance) => Some(TSQueryOp::Phrase(
                Box::new(TSQuery {
                    expr: rewrite_expr(&a.expr, old_query, new_query),
                }),
                Box::new(TSQuery {
                    expr: rewrite_expr(&b.expr, old_query, new_query),
                }),
                *distance,
            )),
        },
    }
}

pub fn ts_delete(vector: &TSVector, lexemes_to_delete: &[String]) -> TSVector {
    let mut result = TSVector::new();
    for (lexeme, entry) in &vector.lexemes {
        let normalized = normalize_lexeme(lexeme);
        let should_delete = lexemes_to_delete
            .iter()
            .any(|l| normalize_lexeme(l) == normalized);
        if !should_delete {
            result.lexemes.insert(lexeme.clone(), entry.clone());
        }
    }
    result
}

pub fn ts_filter(vector: &TSVector, weights: &[Weight]) -> TSVector {
    let mut result = TSVector::new();
    for (lexeme, entry) in &vector.lexemes {
        let filtered_positions: Vec<(Position, Weight)> = entry
            .positions
            .iter()
            .filter(|(_, w)| weights.contains(w))
            .copied()
            .collect();
        if !filtered_positions.is_empty() {
            result.lexemes.insert(
                lexeme.clone(),
                LexemeEntry {
                    positions: filtered_positions,
                },
            );
        }
    }
    result
}

pub fn array_to_tsvector(lexemes: &[String]) -> TSVector {
    let mut vector = TSVector::new();
    for lexeme in lexemes {
        vector.add_lexeme_no_pos(lexeme);
    }
    vector
}

pub fn get_current_ts_config() -> String {
    "english".to_string()
}

pub fn tsvector_to_array(vector: &TSVector) -> Vec<String> {
    vector.lexemes.keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_normalize_lexeme() {
        assert_eq!(normalize_lexeme("Hello"), "hello");
        assert_eq!(normalize_lexeme("WORLD"), "world");
        assert_eq!(normalize_lexeme("can't"), "cant");
        assert_eq!(normalize_lexeme("123abc"), "123abc");
    }

    #[test]
    fn test_to_tsvector() {
        let vector = to_tsvector("The quick brown fox");
        assert!(vector.contains("quick"));
        assert!(vector.contains("brown"));
        assert!(vector.contains("fox"));
        assert!(!vector.contains("the"));
    }

    #[test]
    fn test_tsvector_display() {
        let mut vector = TSVector::new();
        vector.add_lexeme("cat", 1, Weight::D);
        vector.add_lexeme("dog", 2, Weight::A);

        let display = vector.to_string();
        assert!(display.contains("'cat'"));
        assert!(display.contains("'dog'"));
    }

    #[test]
    fn test_plainto_tsquery() {
        let query = plainto_tsquery("fat cat");
        assert!(!query.is_empty());

        let vector = to_tsvector("The fat cat sat on the mat");
        assert!(query.matches(&vector));
    }

    #[test]
    fn test_phraseto_tsquery() {
        let query = phraseto_tsquery("brown fox");

        let vector = to_tsvector("The quick brown fox jumps");
        assert!(query.matches(&vector));
    }

    #[test]
    fn test_tsquery_and() {
        let query = TSQuery::lexeme("cat").and(TSQuery::lexeme("dog"));

        let vector1 = to_tsvector("The cat and dog played");
        assert!(query.matches(&vector1));

        let vector2 = to_tsvector("The cat slept");
        assert!(!query.matches(&vector2));
    }

    #[test]
    fn test_tsquery_or() {
        let query = TSQuery::lexeme("cat").or(TSQuery::lexeme("dog"));

        let vector1 = to_tsvector("The cat played");
        assert!(query.matches(&vector1));

        let vector2 = to_tsvector("The dog played");
        assert!(query.matches(&vector2));

        let vector3 = to_tsvector("The bird flew");
        assert!(!query.matches(&vector3));
    }

    #[test]
    fn test_tsquery_not() {
        let query = TSQuery::lexeme("cat").and(TSQuery::lexeme("dog").negate());

        let vector1 = to_tsvector("The cat played alone");
        assert!(query.matches(&vector1));

        let vector2 = to_tsvector("The cat and dog played");
        assert!(!query.matches(&vector2));
    }

    #[test]
    fn test_ts_rank() {
        let vector = to_tsvector("The quick brown fox jumps over the lazy dog");
        let query = plainto_tsquery("quick fox");

        let score = ts_rank(&vector, &query);
        assert!(score > 0.0);
    }

    #[test]
    fn test_ts_headline() {
        let document = "The quick brown fox jumps over the lazy dog. The fox is very quick.";
        let query = plainto_tsquery("quick fox");
        let options = HeadlineOptions::default();

        let headline = ts_headline(document, &query, &options);
        assert!(headline.contains("<b>"));
        assert!(headline.contains("</b>"));
    }

    #[test]
    fn test_tsvector_length() {
        let vector = to_tsvector("The quick brown fox");
        assert_eq!(tsvector_length(&vector), 3);
    }

    #[test]
    fn test_tsvector_setweight() {
        let vector = to_tsvector("Hello world");
        let weighted = tsvector_setweight(&vector, Weight::A);

        for entry in weighted.lexemes.values() {
            for (_, weight) in &entry.positions {
                assert_eq!(*weight, Weight::A);
            }
        }
    }

    #[test]
    fn test_websearch_to_tsquery() {
        let query = websearch_to_tsquery("cat dog");
        let vector = to_tsvector("The cat and dog played");
        assert!(query.matches(&vector));

        let query = websearch_to_tsquery("cat -bird");
        let vector1 = to_tsvector("The cat played");
        let vector2 = to_tsvector("The cat and bird played");
        assert!(query.matches(&vector1));
        assert!(!query.matches(&vector2));
    }

    #[test]
    fn test_parse_tsvector() {
        let vector = parse_tsvector("'cat':1 'dog':2A").unwrap();
        assert!(vector.contains("cat"));
        assert!(vector.contains("dog"));
        assert_eq!(vector.len(), 2);
    }
}
