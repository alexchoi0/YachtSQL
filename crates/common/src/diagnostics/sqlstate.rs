use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlState {
    code: [u8; 5],
}

impl SqlState {
    pub const fn new(code: [u8; 5]) -> Self {
        Self { code }
    }

    pub fn from_string(s: &str) -> Result<Self, &'static str> {
        if s.len() != 5 {
            return Err("SQLSTATE must be exactly 5 characters");
        }
        if !s.is_ascii() {
            return Err("SQLSTATE must be ASCII");
        }
        let bytes = s.as_bytes();
        Ok(Self::new([
            bytes[0], bytes[1], bytes[2], bytes[3], bytes[4],
        ]))
    }

    pub fn as_str(&self) -> &str {
        self.bytes_to_str(&self.code)
    }

    pub fn class(&self) -> &str {
        self.bytes_to_str(&self.code[0..2])
    }

    pub fn subclass(&self) -> &str {
        self.bytes_to_str(&self.code[2..5])
    }

    #[inline]
    fn bytes_to_str<'a>(&self, bytes: &'a [u8]) -> &'a str {
        unsafe { std::str::from_utf8_unchecked(bytes) }
    }

    #[inline]
    const fn class_bytes(&self) -> [u8; 2] {
        [self.code[0], self.code[1]]
    }

    #[inline]
    pub fn is_success(&self) -> bool {
        self.class_bytes() == *b"00"
    }

    #[inline]
    pub fn is_warning(&self) -> bool {
        self.class_bytes() == *b"01"
    }

    #[inline]
    pub fn is_no_data(&self) -> bool {
        self.class_bytes() == *b"02"
    }

    #[inline]
    pub fn is_error(&self) -> bool {
        let class = self.class_bytes();
        class != *b"00" && class != *b"01" && class != *b"02"
    }
}

impl fmt::Display for SqlState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl From<&str> for SqlState {
    fn from(s: &str) -> Self {
        match Self::from_string(s) {
            Ok(state) => state,
            Err(msg) => panic!("Invalid SQLSTATE '{}': {}", s, msg),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sqlstate_creation() {
        let state = SqlState::new(*b"00000");
        assert_eq!(state.as_str(), "00000");
        assert_eq!(state.class(), "00");
        assert_eq!(state.subclass(), "000");
    }

    #[test]
    fn test_sqlstate_from_str() {
        let state = SqlState::from_string("22012").unwrap();
        assert_eq!(state.as_str(), "22012");
        assert_eq!(state.class(), "22");
        assert_eq!(state.subclass(), "012");
    }

    #[test]
    fn test_sqlstate_classification() {
        assert!(SqlState::from_string("00000").unwrap().is_success());
        assert!(SqlState::from_string("01000").unwrap().is_warning());
        assert!(SqlState::from_string("02000").unwrap().is_no_data());
        assert!(SqlState::from_string("22012").unwrap().is_error());
    }

    #[test]
    fn test_invalid_sqlstate() {
        assert!(SqlState::from_string("123").is_err());
        assert!(SqlState::from_string("1234567").is_err());
    }
}
