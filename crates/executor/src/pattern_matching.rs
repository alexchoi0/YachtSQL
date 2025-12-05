use regex::Regex;

pub fn matches_pattern(text: &str, pattern: &str) -> bool {
    let regex_pattern = like_to_regex(pattern, false);
    match Regex::new(&regex_pattern) {
        Ok(re) => re.is_match(text),
        Err(_) => false,
    }
}

pub fn matches_pattern_case_insensitive(text: &str, pattern: &str) -> bool {
    let regex_pattern = like_to_regex(pattern, true);
    match Regex::new(&regex_pattern) {
        Ok(re) => re.is_match(text),
        Err(_) => false,
    }
}

pub fn matches_similar_to(text: &str, pattern: &str) -> Result<bool, String> {
    matches_similar_to_with_escape(text, pattern, None)
}

pub fn matches_similar_to_with_escape(
    text: &str,
    pattern: &str,
    escape_char: Option<char>,
) -> Result<bool, String> {
    let regex_pattern = similar_to_regex_with_escape(pattern, escape_char);
    Regex::new(&regex_pattern)
        .map(|re| re.is_match(text))
        .map_err(|e| format!("Invalid SIMILAR TO pattern: {}", e))
}

pub fn matches_regex(text: &str, pattern: &str) -> Result<bool, String> {
    Regex::new(pattern)
        .map(|re| re.is_match(text))
        .map_err(|e| format!("Invalid regex pattern: {}", e))
}

pub fn matches_regex_case_insensitive(text: &str, pattern: &str) -> Result<bool, String> {
    let case_insensitive_pattern = format!("(?i){}", pattern);
    Regex::new(&case_insensitive_pattern)
        .map(|re| re.is_match(text))
        .map_err(|e| format!("Invalid regex pattern: {}", e))
}

fn like_to_regex(pattern: &str, case_insensitive: bool) -> String {
    let mut regex = String::from("^");

    if case_insensitive {
        regex.push_str("(?i)");
    }

    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        match ch {
            '%' => regex.push_str(".*"),
            '_' => regex.push('.'),
            '\\' => {
                if let Some(&next_ch) = chars.peek() {
                    if next_ch == '%' || next_ch == '_' {
                        regex.push_str(&regex::escape(&next_ch.to_string()));
                        chars.next();
                    } else {
                        regex.push_str(&regex::escape("\\"));
                    }
                } else {
                    regex.push_str(&regex::escape("\\"));
                }
            }
            _ => regex.push_str(&regex::escape(&ch.to_string())),
        }
    }

    regex.push('$');
    regex
}

fn similar_to_regex(pattern: &str) -> String {
    similar_to_regex_with_escape(pattern, None)
}

fn similar_to_regex_with_escape(pattern: &str, escape_char: Option<char>) -> String {
    let mut regex = String::from("^");
    let escape = escape_char.unwrap_or('\\');
    let mut in_character_class = false;

    let mut chars = pattern.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == escape && !in_character_class {
            if let Some(&next_ch) = chars.peek() {
                regex.push_str(&regex::escape(&next_ch.to_string()));
                chars.next();
            } else {
                regex.push_str(&regex::escape(&ch.to_string()));
            }
        } else if in_character_class {
            match ch {
                ']' => {
                    regex.push(']');
                    in_character_class = false;
                }

                '^' | '-' => regex.push(ch),

                '\\' => {
                    regex.push('\\');
                    if let Some(&next_ch) = chars.peek() {
                        regex.push(next_ch);
                        chars.next();
                    }
                }

                _ => regex.push(ch),
            }
        } else {
            match ch {
                '%' => regex.push_str(".*"),
                '_' => regex.push('.'),
                '|' => regex.push('|'),
                '*' => regex.push('*'),
                '+' => regex.push('+'),
                '?' => regex.push('?'),
                '(' => regex.push('('),
                ')' => regex.push(')'),
                '[' => {
                    regex.push('[');
                    in_character_class = true;
                }
                ']' => regex.push(']'),
                '{' => regex.push('{'),
                '}' => regex.push('}'),
                _ => regex.push_str(&regex::escape(&ch.to_string())),
            }
        }
    }

    regex.push('$');
    regex
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_like_basic() {
        assert!(matches_pattern("hello", "hello"));
        assert!(matches_pattern("hello", "h%"));
        assert!(matches_pattern("hello", "%lo"));
        assert!(matches_pattern("hello", "h_llo"));
        assert!(!matches_pattern("hello", "h_lo"));
    }

    #[test]
    fn test_ilike() {
        assert!(matches_pattern_case_insensitive("Hello", "hello"));
        assert!(matches_pattern_case_insensitive("HELLO", "h%"));
        assert!(matches_pattern_case_insensitive("hello", "H_LLO"));
    }

    #[test]
    fn test_regex() {
        assert!(matches_regex("hello123", r"hello\d+").unwrap());
        assert!(matches_regex_case_insensitive("HELLO", "hello").unwrap());
    }

    #[test]
    fn test_similar_to_case_sensitive() {
        assert!(matches_similar_to("Alice", "Alice").unwrap());
        assert!(!matches_similar_to("ALICE", "Alice").unwrap());
        assert!(!matches_similar_to("alice", "Alice").unwrap());
    }

    #[test]
    fn test_similar_to_character_range() {
        assert!(matches_similar_to("abc", "[a-z]+").unwrap());
        assert!(!matches_similar_to("ABC", "[a-z]+").unwrap());
    }
}
