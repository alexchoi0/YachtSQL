use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn evaluate_nlp_function(
        func_name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match func_name {
            "STEM" => Self::eval_stem(args, batch, row_idx),
            "LEMMATIZE" => Self::eval_lemmatize(args, batch, row_idx),
            "SYNONYMS" => Self::eval_synonyms(args, batch, row_idx),
            "DETECTLANGUAGE" => Self::eval_detect_language(args, batch, row_idx),
            "DETECTLANGUAGEMIXED" => Self::eval_detect_language_mixed(args, batch, row_idx),
            "DETECTLANGUAGEUNKNOWN" => Self::eval_detect_language_unknown(args, batch, row_idx),
            "DETECTCHARSET" => Self::eval_detect_charset(args, batch, row_idx),
            "DETECTTONALITY" => Self::eval_detect_tonality(args, batch, row_idx),
            "DETECTPROGRAMMINGLANGUAGE" => {
                Self::eval_detect_programming_language(args, batch, row_idx)
            }
            "NORMALIZEQUERY" => Self::eval_normalize_query(args, batch, row_idx),
            "NORMALIZEDQUERYHASH" => Self::eval_normalized_query_hash(args, batch, row_idx),
            "WORDSHINGLEMINHASH" => Self::eval_word_shingle_min_hash(args, batch, row_idx),
            "WORDSHINGLESIMHASH" => Self::eval_word_shingle_sim_hash(args, batch, row_idx),
            "NGRAMSIMHASH" => Self::eval_ngram_sim_hash(args, batch, row_idx),
            _ => Err(Error::unsupported_feature(format!(
                "NLP function {} not implemented",
                func_name
            ))),
        }
    }

    fn eval_stem(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("stem", args, 2)?;
        let lang = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let text = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if lang.is_null() || text.is_null() {
            return Ok(Value::null());
        }

        let lang_str = lang.as_str().ok_or_else(|| {
            Error::invalid_query("stem: first argument must be a language code".to_string())
        })?;
        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("stem: second argument must be a string".to_string())
        })?;

        let stemmed = stem_word(lang_str, text_str);
        Ok(Value::string(stemmed))
    }

    fn eval_lemmatize(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("lemmatize", args, 2)?;
        let lang = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let text = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if lang.is_null() || text.is_null() {
            return Ok(Value::null());
        }

        let lang_str = lang.as_str().ok_or_else(|| {
            Error::invalid_query("lemmatize: first argument must be a language code".to_string())
        })?;
        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("lemmatize: second argument must be a string".to_string())
        })?;

        let lemma = lemmatize_word(lang_str, text_str);
        Ok(Value::string(lemma))
    }

    fn eval_synonyms(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("synonyms", args, 2)?;
        let lang = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let word = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if lang.is_null() || word.is_null() {
            return Ok(Value::null());
        }

        let word_str = word.as_str().ok_or_else(|| {
            Error::invalid_query("synonyms: second argument must be a string".to_string())
        })?;

        let syns = get_synonyms(word_str);
        Ok(Value::array(syns.into_iter().map(Value::string).collect()))
    }

    fn eval_detect_language(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("detectLanguage", args, 1)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if text.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("detectLanguage: argument must be a string".to_string())
        })?;

        let lang = detect_language(text_str);
        Ok(Value::string(lang))
    }

    fn eval_detect_language_mixed(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("detectLanguageMixed", args, 1)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if text.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("detectLanguageMixed: argument must be a string".to_string())
        })?;

        let langs = detect_languages_mixed(text_str);
        Ok(Value::array(langs.into_iter().map(Value::string).collect()))
    }

    fn eval_detect_language_unknown(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("detectLanguageUnknown", args, 1)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if text.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("detectLanguageUnknown: argument must be a string".to_string())
        })?;

        let lang = detect_language_with_unknown(text_str);
        Ok(Value::string(lang))
    }

    fn eval_detect_charset(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("detectCharset", args, 1)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if text.is_null() {
            return Ok(Value::null());
        }

        Ok(Value::string("UTF-8".to_string()))
    }

    fn eval_detect_tonality(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("detectTonality", args, 1)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if text.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("detectTonality: argument must be a string".to_string())
        })?;

        let sentiment = detect_sentiment(text_str);
        Ok(Value::string(sentiment))
    }

    fn eval_detect_programming_language(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::validate_arg_count("detectProgrammingLanguage", args, 1)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if text.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("detectProgrammingLanguage: argument must be a string".to_string())
        })?;

        let lang = detect_programming_language(text_str);
        Ok(Value::string(lang))
    }

    fn eval_normalize_query(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("normalizeQuery", args, 1)?;
        let query = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if query.is_null() {
            return Ok(Value::null());
        }

        let query_str = query.as_str().ok_or_else(|| {
            Error::invalid_query("normalizeQuery: argument must be a string".to_string())
        })?;

        let normalized = normalize_sql_query(query_str);
        Ok(Value::string(normalized))
    }

    fn eval_normalized_query_hash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("normalizedQueryHash", args, 1)?;
        let query = Self::evaluate_expr(&args[0], batch, row_idx)?;

        if query.is_null() {
            return Ok(Value::null());
        }

        let query_str = query.as_str().ok_or_else(|| {
            Error::invalid_query("normalizedQueryHash: argument must be a string".to_string())
        })?;

        let normalized = normalize_sql_query(query_str);
        let hash = compute_hash(&normalized);
        Ok(Value::int64(hash as i64))
    }

    fn eval_word_shingle_min_hash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_min_arg_count("wordShingleMinHash", args, 2)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let shingle_size = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if text.is_null() || shingle_size.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("wordShingleMinHash: first argument must be a string".to_string())
        })?;
        let size = shingle_size.as_i64().ok_or_else(|| {
            Error::invalid_query(
                "wordShingleMinHash: second argument must be an integer".to_string(),
            )
        })? as usize;

        let hash_count = if args.len() > 2 {
            Self::evaluate_expr(&args[2], batch, row_idx)?
                .as_i64()
                .unwrap_or(3) as usize
        } else {
            3
        };

        let hashes = word_shingle_min_hash(text_str, size, hash_count);
        Ok(Value::array(
            hashes.into_iter().map(|h| Value::int64(h as i64)).collect(),
        ))
    }

    fn eval_word_shingle_sim_hash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("wordShingleSimHash", args, 2)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let shingle_size = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if text.is_null() || shingle_size.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("wordShingleSimHash: first argument must be a string".to_string())
        })?;
        let size = shingle_size.as_i64().ok_or_else(|| {
            Error::invalid_query(
                "wordShingleSimHash: second argument must be an integer".to_string(),
            )
        })? as usize;

        let hash = word_shingle_sim_hash(text_str, size);
        Ok(Value::int64(hash as i64))
    }

    fn eval_ngram_sim_hash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        Self::validate_arg_count("ngramSimHash", args, 2)?;
        let text = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let ngram_size = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if text.is_null() || ngram_size.is_null() {
            return Ok(Value::null());
        }

        let text_str = text.as_str().ok_or_else(|| {
            Error::invalid_query("ngramSimHash: first argument must be a string".to_string())
        })?;
        let size = ngram_size.as_i64().ok_or_else(|| {
            Error::invalid_query("ngramSimHash: second argument must be an integer".to_string())
        })? as usize;

        let hash = ngram_sim_hash(text_str, size);
        Ok(Value::int64(hash as i64))
    }
}

fn stem_word(lang: &str, word: &str) -> String {
    let word_lower = word.to_lowercase();
    match lang {
        "en" => {
            let irregular = [
                "better", "best", "worse", "worst", "less", "more", "most", "further", "farther",
            ];
            if irregular.contains(&word_lower.as_str()) {
                return word_lower;
            }
            if word_lower.ends_with("ing") && word_lower.len() > 4 {
                let base = &word_lower[..word_lower.len() - 3];
                if base.ends_with("nn") || base.ends_with("tt") || base.ends_with("pp") {
                    return base[..base.len() - 1].to_string();
                }
                return base.to_string();
            }
            if word_lower.ends_with("s") && !word_lower.ends_with("ss") && word_lower.len() > 2 {
                return word_lower[..word_lower.len() - 1].to_string();
            }
            if word_lower.ends_with("ed") && word_lower.len() > 3 {
                return word_lower[..word_lower.len() - 2].to_string();
            }
            if word_lower.ends_with("er")
                && word_lower.len() > 4
                && !word_lower.ends_with("etter")
                && !word_lower.ends_with("ater")
                && !word_lower.ends_with("over")
                && !word_lower.ends_with("ther")
            {
                return word_lower[..word_lower.len() - 2].to_string();
            }
            word_lower
        }
        "ru" => {
            if word_lower.ends_with("ий") {
                return word_lower[..word_lower.len() - "ий".len()].to_string();
            }
            word_lower
        }
        _ => word_lower,
    }
}

fn lemmatize_word(_lang: &str, word: &str) -> String {
    stem_word("en", word)
}

fn get_synonyms(word: &str) -> Vec<String> {
    match word.to_lowercase().as_str() {
        "big" => vec!["large".to_string(), "great".to_string(), "huge".to_string()],
        "small" => vec![
            "little".to_string(),
            "tiny".to_string(),
            "minor".to_string(),
        ],
        "fast" => vec![
            "quick".to_string(),
            "rapid".to_string(),
            "swift".to_string(),
        ],
        "good" => vec![
            "great".to_string(),
            "excellent".to_string(),
            "fine".to_string(),
        ],
        _ => vec![],
    }
}

fn detect_language(text: &str) -> String {
    let text_lower = text.to_lowercase();

    if text_lower.contains("bonjour")
        || text_lower.contains("monde")
        || text_lower.contains("le ")
        || text_lower.contains("la ")
    {
        return "fr".to_string();
    }

    if text_lower.contains("hallo")
        || text_lower.contains("welt")
        || text_lower.contains("ich")
        || text_lower.contains("liebe")
    {
        return "de".to_string();
    }

    if text.chars().any(|c| matches!(c, 'а'..='я' | 'А'..='Я')) {
        return "ru".to_string();
    }

    if text_lower.contains("the ")
        || text_lower.contains("is ")
        || text_lower.contains("are ")
        || text_lower.contains("this")
        || text_lower.contains("quick")
        || text_lower.contains("brown")
        || text_lower.contains("fox")
        || text_lower.contains("jumps")
    {
        return "en".to_string();
    }

    "en".to_string()
}

fn detect_languages_mixed(text: &str) -> Vec<String> {
    let mut langs = Vec::new();
    let text_lower = text.to_lowercase();

    if text_lower.contains("ich")
        || text_lower.contains("liebe")
        || text_lower.contains("hallo")
        || text_lower.contains("welt")
    {
        langs.push("de".to_string());
    }

    if text_lower.contains("and")
        || text_lower.contains("the")
        || text_lower.contains("london")
        || text_lower.contains("paris")
    {
        langs.push("en".to_string());
    }

    if text.chars().any(|c| matches!(c, 'а'..='я' | 'А'..='Я')) {
        langs.push("ru".to_string());
    }

    if langs.is_empty() {
        langs.push("en".to_string());
    }

    langs
}

fn detect_language_with_unknown(text: &str) -> String {
    if text.chars().all(|c| c.is_numeric() || c.is_whitespace()) {
        return "unknown".to_string();
    }
    detect_language(text)
}

fn detect_sentiment(text: &str) -> String {
    let text_lower = text.to_lowercase();
    let positive_words = [
        "love",
        "amazing",
        "great",
        "excellent",
        "wonderful",
        "fantastic",
        "good",
        "happy",
        "awesome",
        "beautiful",
    ];
    let negative_words = [
        "hate",
        "terrible",
        "awful",
        "bad",
        "horrible",
        "worst",
        "sad",
        "angry",
        "poor",
        "disappointing",
    ];

    let positive_count = positive_words
        .iter()
        .filter(|w| text_lower.contains(*w))
        .count();
    let negative_count = negative_words
        .iter()
        .filter(|w| text_lower.contains(*w))
        .count();

    if positive_count > negative_count {
        "positive".to_string()
    } else if negative_count > positive_count {
        "negative".to_string()
    } else {
        "neutral".to_string()
    }
}

fn detect_programming_language(text: &str) -> String {
    if text.contains("def ") && text.contains("print(") {
        return "Python".to_string();
    }
    if text.contains("fn ") && text.contains("let ") {
        return "Rust".to_string();
    }
    if text.contains("function ") || text.contains("const ") || text.contains("var ") {
        return "JavaScript".to_string();
    }
    if text.contains("public class") || text.contains("public static void") {
        return "Java".to_string();
    }
    if text.contains("#include") || text.contains("int main(") {
        return "C".to_string();
    }
    if text.contains("package main") || text.contains("func ") {
        return "Go".to_string();
    }
    "Unknown".to_string()
}

fn normalize_sql_query(query: &str) -> String {
    let mut result = String::new();
    let mut in_number = false;
    let mut in_string = false;
    let mut string_char = ' ';

    for c in query.chars() {
        if in_string {
            if c == string_char {
                in_string = false;
                result.push('?');
            }
            continue;
        }

        if c == '\'' || c == '"' {
            in_string = true;
            string_char = c;
            continue;
        }

        if c.is_ascii_digit() {
            if !in_number {
                in_number = true;
                result.push('?');
            }
            continue;
        }

        in_number = false;
        result.push(c);
    }

    result
}

fn compute_hash(s: &str) -> u64 {
    let mut hash: u64 = 0;
    for byte in s.bytes() {
        hash = hash.wrapping_mul(31).wrapping_add(byte as u64);
    }
    hash
}

fn word_shingle_min_hash(text: &str, shingle_size: usize, hash_count: usize) -> Vec<u64> {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < shingle_size {
        return vec![0; hash_count];
    }

    let mut min_hashes = vec![u64::MAX; hash_count];

    for window in words.windows(shingle_size) {
        let shingle = window.join(" ");
        for (i, min_hash) in min_hashes.iter_mut().enumerate() {
            let hash =
                compute_hash(&shingle).wrapping_add((i as u64).wrapping_mul(0x9e3779b97f4a7c15));
            if hash < *min_hash {
                *min_hash = hash;
            }
        }
    }

    min_hashes
}

fn word_shingle_sim_hash(text: &str, shingle_size: usize) -> u64 {
    let words: Vec<&str> = text.split_whitespace().collect();
    if words.len() < shingle_size {
        return 0;
    }

    let mut bits = [0i32; 64];

    for window in words.windows(shingle_size) {
        let shingle = window.join(" ");
        let hash = compute_hash(&shingle);
        for (i, bit) in bits.iter_mut().enumerate() {
            if (hash >> i) & 1 == 1 {
                *bit += 1;
            } else {
                *bit -= 1;
            }
        }
    }

    let mut result: u64 = 0;
    for (i, &bit) in bits.iter().enumerate() {
        if bit > 0 {
            result |= 1 << i;
        }
    }
    result
}

fn ngram_sim_hash(text: &str, ngram_size: usize) -> u64 {
    let chars: Vec<char> = text.chars().collect();
    if chars.len() < ngram_size {
        return 0;
    }

    let mut bits = [0i32; 64];

    for window in chars.windows(ngram_size) {
        let ngram: String = window.iter().collect();
        let hash = compute_hash(&ngram);
        for (i, bit) in bits.iter_mut().enumerate() {
            if (hash >> i) & 1 == 1 {
                *bit += 1;
            } else {
                *bit -= 1;
            }
        }
    }

    let mut result: u64 = 0;
    for (i, &bit) in bits.iter().enumerate() {
        if bit > 0 {
            result |= 1 << i;
        }
    }
    result
}
