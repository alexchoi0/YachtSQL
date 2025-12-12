use sqlparser::ast::{Ident, ObjectName, ObjectNamePart};
use sqlparser::tokenizer::{Span, Token, Word};
use yachtsql_common::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TokenPattern {
    Keyword(&'static str),
    OptionalKeyword(&'static str),
    ObjectName,
    KeywordPair(&'static str, &'static str),
    OptionalKeywordPair(&'static str, &'static str),
}

#[derive(Debug, Clone)]
pub struct PatternMatch {
    pub object_names: Vec<ObjectName>,
    pub matched_optional_keywords: Vec<String>,
    pub matched_optional_pairs: Vec<(String, String)>,
}

impl PatternMatch {
    fn new() -> Self {
        Self {
            object_names: Vec::new(),
            matched_optional_keywords: Vec::new(),
            matched_optional_pairs: Vec::new(),
        }
    }

    pub fn has_optional_keyword(&self, keyword: &str) -> bool {
        self.matched_optional_keywords
            .iter()
            .any(|k| k.eq_ignore_ascii_case(keyword))
    }

    pub fn has_optional_pair(&self, first: &str, second: &str) -> bool {
        self.matched_optional_pairs
            .iter()
            .any(|(k1, k2)| k1.eq_ignore_ascii_case(first) && k2.eq_ignore_ascii_case(second))
    }

    pub fn first_object_name(&self) -> Option<&ObjectName> {
        self.object_names.first()
    }

    #[deprecated(
        since = "0.1.0",
        note = "Use first_object_name() instead for safer code"
    )]
    pub fn object_name(&self) -> &ObjectName {
        self.first_object_name()
            .expect("No object names in pattern match")
    }
}

pub struct PatternMatcher;

impl PatternMatcher {
    pub fn match_pattern(
        tokens: &[&Token],
        pattern: &[TokenPattern],
    ) -> Result<Option<PatternMatch>> {
        let mut match_result = PatternMatch::new();
        let mut token_idx = 0;

        for pat in pattern {
            match pat {
                TokenPattern::Keyword(expected) => {
                    if !Self::match_keyword(tokens, token_idx, expected) {
                        return Ok(None);
                    }
                    token_idx += 1;
                }

                TokenPattern::OptionalKeyword(expected) => {
                    if Self::match_keyword(tokens, token_idx, expected) {
                        match_result
                            .matched_optional_keywords
                            .push(expected.to_string());
                        token_idx += 1;
                    }
                }

                TokenPattern::ObjectName => {
                    let (name, consumed) = Self::extract_object_name(&tokens[token_idx..])?;
                    match_result.object_names.push(name);
                    token_idx += consumed;
                }

                TokenPattern::KeywordPair(first, second) => {
                    if !Self::match_keyword(tokens, token_idx, first)
                        || !Self::match_keyword(tokens, token_idx + 1, second)
                    {
                        return Ok(None);
                    }
                    token_idx += 2;
                }

                TokenPattern::OptionalKeywordPair(first, second) => {
                    if Self::match_keyword(tokens, token_idx, first)
                        && Self::match_keyword(tokens, token_idx + 1, second)
                    {
                        match_result
                            .matched_optional_pairs
                            .push((first.to_string(), second.to_string()));
                        token_idx += 2;
                    }
                }
            }
        }

        Ok(Some(match_result))
    }

    fn match_keyword(tokens: &[&Token], idx: usize, expected: &str) -> bool {
        if let Some(Token::Word(w)) = tokens.get(idx) {
            w.value.eq_ignore_ascii_case(expected)
        } else {
            false
        }
    }

    fn extract_object_name(tokens: &[&Token]) -> Result<(ObjectName, usize)> {
        if tokens.is_empty() {
            return Err(Error::invalid_query("Missing object name"));
        }

        let mut parts = Vec::new();
        let mut i = 0;

        while i < tokens.len() {
            if let Some(Token::Word(w)) = tokens.get(i) {
                parts.push(ObjectNamePart::Identifier(Self::word_to_ident(w)));
                i += 1;

                if let Some(Token::Period) = tokens.get(i) {
                    i += 1;
                    continue;
                } else {
                    break;
                }
            } else {
                break;
            }
        }

        if parts.is_empty() {
            return Err(Error::invalid_query("Invalid object name"));
        }

        Ok((ObjectName(parts), i))
    }

    fn word_to_ident(word: &Word) -> Ident {
        Ident {
            value: word.value.clone(),
            quote_style: word.quote_style,
            span: Span::empty(),
        }
    }
}

#[cfg(test)]
#[allow(clippy::useless_vec)]
mod tests {
    use sqlparser::tokenizer::{Token, Word};
    use yachtsql_common::error::Error;

    use super::*;

    fn make_word(s: &str) -> Token {
        Token::Word(Word {
            value: s.to_string(),
            quote_style: None,
            keyword: sqlparser::keywords::Keyword::NoKeyword,
        })
    }

    #[test]
    fn test_match_simple_keyword_pattern() -> Result<()> {
        let tokens = [
            make_word("REFRESH"),
            make_word("MATERIALIZED"),
            make_word("VIEW"),
        ];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("REFRESH"),
            TokenPattern::Keyword("MATERIALIZED"),
            TokenPattern::Keyword("VIEW"),
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        assert!(result.is_some());
        Ok(())
    }

    #[test]
    fn test_match_with_object_name() -> Result<()> {
        let tokens = vec![
            make_word("REFRESH"),
            make_word("MATERIALIZED"),
            make_word("VIEW"),
            make_word("my_view"),
        ];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("REFRESH"),
            TokenPattern::Keyword("MATERIALIZED"),
            TokenPattern::Keyword("VIEW"),
            TokenPattern::ObjectName,
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        let match_result = result.ok_or_else(|| Error::invalid_query("Expected pattern match"))?;

        assert_eq!(match_result.object_names.len(), 1);
        assert_eq!(match_result.object_names[0].to_string(), "my_view");
        Ok(())
    }

    #[test]
    fn test_match_with_qualified_object_name() -> Result<()> {
        let tokens = vec![
            make_word("DROP"),
            make_word("VIEW"),
            make_word("public"),
            Token::Period,
            make_word("my_view"),
        ];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("DROP"),
            TokenPattern::Keyword("VIEW"),
            TokenPattern::ObjectName,
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        let match_result = result.ok_or_else(|| Error::invalid_query("Expected pattern match"))?;

        assert_eq!(match_result.object_names.len(), 1);
        assert_eq!(match_result.object_names[0].to_string(), "public.my_view");
        Ok(())
    }

    #[test]
    fn test_match_with_optional_keyword_pair() -> Result<()> {
        let tokens = vec![
            make_word("DROP"),
            make_word("VIEW"),
            make_word("IF"),
            make_word("EXISTS"),
            make_word("my_view"),
        ];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("DROP"),
            TokenPattern::Keyword("VIEW"),
            TokenPattern::OptionalKeywordPair("IF", "EXISTS"),
            TokenPattern::ObjectName,
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        let match_result = result.ok_or_else(|| Error::invalid_query("Expected pattern match"))?;

        assert!(match_result.has_optional_pair("IF", "EXISTS"));
        let object_name = match_result
            .first_object_name()
            .ok_or_else(|| Error::invalid_query("Expected object name"))?;
        assert_eq!(object_name.to_string(), "my_view");
        Ok(())
    }

    #[test]
    fn test_optional_keyword_pair_not_present() -> Result<()> {
        let tokens = [make_word("DROP"), make_word("VIEW"), make_word("my_view")];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("DROP"),
            TokenPattern::Keyword("VIEW"),
            TokenPattern::OptionalKeywordPair("IF", "EXISTS"),
            TokenPattern::ObjectName,
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        let match_result = result.ok_or_else(|| Error::invalid_query("Expected pattern match"))?;

        assert!(!match_result.has_optional_pair("IF", "EXISTS"));
        let object_name = match_result
            .first_object_name()
            .ok_or_else(|| Error::invalid_query("Expected object name"))?;
        assert_eq!(object_name.to_string(), "my_view");
        Ok(())
    }

    #[test]
    fn test_pattern_mismatch() -> Result<()> {
        let tokens = [make_word("CREATE"), make_word("TABLE"), make_word("foo")];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("DROP"),
            TokenPattern::Keyword("TABLE"),
            TokenPattern::ObjectName,
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        assert!(result.is_none());
        Ok(())
    }

    #[test]
    fn test_case_insensitive_matching() -> Result<()> {
        let tokens = [
            make_word("refresh"),
            make_word("MATERIALIZED"),
            make_word("View"),
        ];
        let token_refs: Vec<&Token> = tokens.iter().collect();

        let pattern = vec![
            TokenPattern::Keyword("REFRESH"),
            TokenPattern::Keyword("materialized"),
            TokenPattern::Keyword("VIEW"),
        ];

        let result = PatternMatcher::match_pattern(&token_refs, &pattern)?;
        assert!(result.is_some());
        Ok(())
    }
}
