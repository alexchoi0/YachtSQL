use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::OnceLock;

#[inline]
fn hash_string(s: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    s.hash(&mut hasher);
    hasher.finish()
}

pub trait CollationImpl: Send + Sync {
    fn name(&self) -> &str;

    fn compare(&self, a: &str, b: &str) -> Ordering;

    fn hash(&self, s: &str) -> u64;

    fn normalize(&self, s: &str) -> String;
}

#[derive(Debug)]
pub struct BinaryCollation;

impl CollationImpl for BinaryCollation {
    fn name(&self) -> &str {
        "binary"
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        a.cmp(b)
    }

    fn hash(&self, s: &str) -> u64 {
        hash_string(s)
    }

    fn normalize(&self, s: &str) -> String {
        s.to_owned()
    }
}

#[derive(Debug)]
pub struct CaseInsensitiveCollation;

impl CollationImpl for CaseInsensitiveCollation {
    fn name(&self) -> &str {
        "case_insensitive"
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        a.to_lowercase().cmp(&b.to_lowercase())
    }

    fn hash(&self, s: &str) -> u64 {
        hash_string(&s.to_lowercase())
    }

    fn normalize(&self, s: &str) -> String {
        s.to_lowercase()
    }
}

#[derive(Debug, Clone, PartialEq)]
enum Segment {
    Text(String),
    Number(f64),
}

impl Segment {
    #[inline]
    fn is_number_char(ch: char) -> bool {
        ch.is_ascii_digit() || matches!(ch, '.' | '-' | '+')
    }

    fn try_parse_number(buffer: String) -> Self {
        buffer
            .parse::<f64>()
            .map(Self::Number)
            .unwrap_or_else(|_| Self::Text(buffer))
    }
}

#[derive(Debug)]
pub struct NumericCollation;

impl NumericCollation {
    fn extract_segments(s: &str) -> Vec<Segment> {
        let mut segments = Vec::new();
        let mut text_buffer = String::new();
        let mut number_buffer = String::new();
        let mut in_number = false;

        for ch in s.chars() {
            if Segment::is_number_char(ch) {
                if !in_number && !text_buffer.is_empty() {
                    segments.push(Segment::Text(std::mem::take(&mut text_buffer)));
                }
                number_buffer.push(ch);
                in_number = true;
            } else {
                if in_number && !number_buffer.is_empty() {
                    segments.push(Segment::try_parse_number(std::mem::take(
                        &mut number_buffer,
                    )));
                }
                text_buffer.push(ch);
                in_number = false;
            }
        }

        if !text_buffer.is_empty() {
            segments.push(Segment::Text(text_buffer));
        }
        if !number_buffer.is_empty() {
            segments.push(Segment::try_parse_number(number_buffer));
        }

        segments
    }
}

impl CollationImpl for NumericCollation {
    fn name(&self) -> &str {
        "numeric"
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        let segments_a = Self::extract_segments(a);
        let segments_b = Self::extract_segments(b);

        for (seg_a, seg_b) in segments_a.iter().zip(segments_b.iter()) {
            match (seg_a, seg_b) {
                (Segment::Text(t1), Segment::Text(t2)) => {
                    let ord = t1.cmp(t2);
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
                (Segment::Number(n1), Segment::Number(n2)) => {
                    if n1 < n2 {
                        return Ordering::Less;
                    } else if n1 > n2 {
                        return Ordering::Greater;
                    }
                }
                (Segment::Text(_), Segment::Number(_)) => return Ordering::Less,
                (Segment::Number(_), Segment::Text(_)) => return Ordering::Greater,
            }
        }

        segments_a.len().cmp(&segments_b.len())
    }

    fn hash(&self, s: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        for seg in Self::extract_segments(s) {
            match seg {
                Segment::Text(t) => t.hash(&mut hasher),
                Segment::Number(n) => n.to_bits().hash(&mut hasher),
            }
        }
        hasher.finish()
    }

    fn normalize(&self, s: &str) -> String {
        s.to_owned()
    }
}

#[derive(Debug)]
pub struct UnicodeCollation;

impl UnicodeCollation {
    #[inline]
    fn normalize_nfc(s: &str) -> String {
        use unicode_normalization::UnicodeNormalization;
        s.nfc().collect()
    }
}

impl CollationImpl for UnicodeCollation {
    fn name(&self) -> &str {
        "unicode"
    }

    fn compare(&self, a: &str, b: &str) -> Ordering {
        Self::normalize_nfc(a).cmp(&Self::normalize_nfc(b))
    }

    fn hash(&self, s: &str) -> u64 {
        hash_string(&Self::normalize_nfc(s))
    }

    fn normalize(&self, s: &str) -> String {
        Self::normalize_nfc(s)
    }
}

pub struct CollationRegistry {
    collations: HashMap<String, Box<dyn CollationImpl>>,
}

impl CollationRegistry {
    const BINARY: &'static str = "binary";
    const CASE_INSENSITIVE: &'static str = "case_insensitive";
    const NUMERIC: &'static str = "numeric";
    const UNICODE: &'static str = "unicode";

    fn new() -> Self {
        let mut collations = HashMap::with_capacity(4);

        collations.insert(
            Self::BINARY.to_owned(),
            Box::new(BinaryCollation) as Box<dyn CollationImpl>,
        );
        collations.insert(
            Self::CASE_INSENSITIVE.to_owned(),
            Box::new(CaseInsensitiveCollation) as Box<dyn CollationImpl>,
        );
        collations.insert(
            Self::NUMERIC.to_owned(),
            Box::new(NumericCollation) as Box<dyn CollationImpl>,
        );
        collations.insert(
            Self::UNICODE.to_owned(),
            Box::new(UnicodeCollation) as Box<dyn CollationImpl>,
        );

        Self { collations }
    }

    pub fn global() -> &'static CollationRegistry {
        static REGISTRY: OnceLock<CollationRegistry> = OnceLock::new();
        REGISTRY.get_or_init(Self::new)
    }

    pub fn get(&self, name: &str) -> Option<&dyn CollationImpl> {
        self.collations.get(name).map(|b| b.as_ref())
    }

    pub fn default_collation() -> &'static dyn CollationImpl {
        Self::global()
            .get(Self::BINARY)
            .expect("binary collation must always be available")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_collation() {
        let collation = BinaryCollation;
        assert_eq!(collation.compare("abc", "ABC"), Ordering::Greater);
        assert_eq!(collation.compare("abc", "abc"), Ordering::Equal);
        assert_eq!(collation.compare("ABC", "abc"), Ordering::Less);
    }

    #[test]
    fn test_case_insensitive_collation() {
        let collation = CaseInsensitiveCollation;
        assert_eq!(collation.compare("abc", "ABC"), Ordering::Equal);
        assert_eq!(collation.compare("ABC", "abc"), Ordering::Equal);
        assert_eq!(collation.compare("abc", "def"), Ordering::Less);
    }

    #[test]
    fn test_numeric_collation() {
        let collation = NumericCollation;
        assert_eq!(collation.compare("file2.txt", "file10.txt"), Ordering::Less);
        assert_eq!(
            collation.compare("file10.txt", "file2.txt"),
            Ordering::Greater
        );
        assert_eq!(
            collation.compare("file10.txt", "file10.txt"),
            Ordering::Equal
        );
    }

    #[test]
    fn test_unicode_collation() {
        let collation = UnicodeCollation;

        let composed = "café";
        let decomposed = "cafe\u{0301}";
        assert_eq!(collation.compare(composed, decomposed), Ordering::Equal);
    }

    #[test]
    fn test_registry() {
        let registry = CollationRegistry::global();
        assert!(registry.get("binary").is_some());
        assert!(registry.get("case_insensitive").is_some());
        assert!(registry.get("numeric").is_some());
        assert!(registry.get("unicode").is_some());
        assert!(registry.get("nonexistent").is_none());
    }
}
