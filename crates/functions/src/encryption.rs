use sha2::{Digest, Sha256};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

const AEAD_VERSION: u8 = 1;
const DETERMINISTIC_VERSION: u8 = 2;
const NONCE_SIZE: usize = 12;
const HASH_SIZE: usize = 32;
const KEYSET_PREFIX: &str = "KEYSET:";

#[derive(Debug, Clone)]
pub struct Keyset {
    key_id: String,
}

impl Keyset {
    fn new(kms_uri: &str) -> Self {
        Self {
            key_id: kms_uri.to_string(),
        }
    }

    fn key_id(&self) -> &str {
        &self.key_id
    }
}

pub fn keys_keyset_chain(kms_uri: &str) -> Result<Value> {
    if kms_uri.is_empty() {
        return Err(Error::invalid_query("KMS URI cannot be empty"));
    }

    let keyset_bytes = format!("{}{}", KEYSET_PREFIX, kms_uri).into_bytes();
    Ok(Value::bytes(keyset_bytes))
}

fn parse_keyset(keyset_bytes: &[u8]) -> Result<Keyset> {
    let keyset_str = std::str::from_utf8(keyset_bytes)
        .map_err(|_| Error::invalid_query("Invalid keyset format"))?;

    let kms_uri = keyset_str
        .strip_prefix(KEYSET_PREFIX)
        .ok_or_else(|| Error::invalid_query("Invalid keyset format"))?;

    Ok(Keyset::new(kms_uri))
}

fn write_length_prefixed(buffer: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    if data.len() > u16::MAX as usize {
        return Err(Error::invalid_query("Data too long"));
    }
    buffer.extend_from_slice(&(data.len() as u16).to_be_bytes());
    buffer.extend_from_slice(data);
    Ok(())
}

fn read_length_prefixed<'a>(buffer: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    if *pos + 2 > buffer.len() {
        return Err(Error::invalid_query("Invalid ciphertext format"));
    }

    let len = u16::from_be_bytes([buffer[*pos], buffer[*pos + 1]]) as usize;
    *pos += 2;

    if *pos + len > buffer.len() {
        return Err(Error::invalid_query("Invalid ciphertext format"));
    }

    let data = &buffer[*pos..*pos + len];
    *pos += len;
    Ok(data)
}

fn verify_version(ciphertext: &[u8], expected_version: u8) -> Result<()> {
    if ciphertext.is_empty() {
        return Err(Error::invalid_query("Ciphertext is empty"));
    }

    let version = ciphertext[0];
    if version != expected_version {
        return Err(Error::invalid_query(format!(
            "Unsupported ciphertext version: {} (expected {})",
            version, expected_version
        )));
    }
    Ok(())
}

pub fn aead_encrypt(keyset_bytes: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Value> {
    let keyset = parse_keyset(keyset_bytes)?;

    let mut ciphertext = Vec::new();

    ciphertext.push(AEAD_VERSION);

    write_length_prefixed(&mut ciphertext, keyset.key_id().as_bytes())?;

    ciphertext.extend_from_slice(&[0x42; NONCE_SIZE]);

    write_length_prefixed(&mut ciphertext, aad)?;

    ciphertext.extend_from_slice(plaintext);

    Ok(Value::bytes(ciphertext))
}

pub fn aead_decrypt_bytes(keyset_bytes: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Value> {
    let keyset = parse_keyset(keyset_bytes)?;
    verify_version(ciphertext, AEAD_VERSION)?;

    let mut pos = 1;

    let stored_key_id = read_length_prefixed(ciphertext, &mut pos)?;
    let stored_key_id_str = std::str::from_utf8(stored_key_id)
        .map_err(|_| Error::invalid_query("Invalid key ID in ciphertext"))?;

    if stored_key_id_str != keyset.key_id() {
        return Err(Error::invalid_query(
            "Key ID mismatch - wrong keyset for decryption".to_string(),
        ));
    }

    if pos + NONCE_SIZE > ciphertext.len() {
        return Err(Error::invalid_query("Invalid ciphertext format"));
    }
    pos += NONCE_SIZE;

    let stored_aad = read_length_prefixed(ciphertext, &mut pos)?;
    if stored_aad != aad {
        return Err(Error::invalid_query(
            "AAD mismatch - authentication failed".to_string(),
        ));
    }

    let plaintext = ciphertext[pos..].to_vec();
    Ok(Value::bytes(plaintext))
}

pub fn aead_decrypt_string(keyset_bytes: &[u8], ciphertext: &[u8], aad: &[u8]) -> Result<Value> {
    decrypt_to_string(aead_decrypt_bytes(keyset_bytes, ciphertext, aad)?)
}

pub fn deterministic_encrypt(keyset_bytes: &[u8], plaintext: &[u8], aad: &[u8]) -> Result<Value> {
    let keyset = parse_keyset(keyset_bytes)?;

    let mut hasher = Sha256::new();
    hasher.update(keyset.key_id().as_bytes());
    hasher.update(plaintext);
    hasher.update(aad);
    let hash = hasher.finalize();

    let mut ciphertext = Vec::new();

    ciphertext.push(DETERMINISTIC_VERSION);

    write_length_prefixed(&mut ciphertext, keyset.key_id().as_bytes())?;

    ciphertext.extend_from_slice(&hash);

    ciphertext.extend_from_slice(plaintext);

    Ok(Value::bytes(ciphertext))
}

pub fn deterministic_decrypt_bytes(
    keyset_bytes: &[u8],
    ciphertext: &[u8],
    _aad: &[u8],
) -> Result<Value> {
    let keyset = parse_keyset(keyset_bytes)?;
    verify_version(ciphertext, DETERMINISTIC_VERSION)?;

    let mut pos = 1;

    let stored_key_id = read_length_prefixed(ciphertext, &mut pos)?;
    let stored_key_id_str = std::str::from_utf8(stored_key_id)
        .map_err(|_| Error::invalid_query("Invalid key ID in ciphertext"))?;

    if stored_key_id_str != keyset.key_id() {
        return Err(Error::invalid_query(
            "Key ID mismatch - wrong keyset for decryption".to_string(),
        ));
    }

    if pos + HASH_SIZE > ciphertext.len() {
        return Err(Error::invalid_query("Invalid ciphertext format"));
    }
    pos += HASH_SIZE;

    let plaintext = ciphertext[pos..].to_vec();
    Ok(Value::bytes(plaintext))
}

pub fn deterministic_decrypt_string(
    keyset_bytes: &[u8],
    ciphertext: &[u8],
    aad: &[u8],
) -> Result<Value> {
    decrypt_to_string(deterministic_decrypt_bytes(keyset_bytes, ciphertext, aad)?)
}

fn decrypt_to_string(bytes_value: Value) -> Result<Value> {
    if let Some(bytes) = bytes_value.as_bytes() {
        let s = std::str::from_utf8(bytes)
            .map_err(|_| Error::invalid_query("Decrypted data is not valid UTF-8"))?;
        Ok(Value::string(s.to_string()))
    } else {
        unreachable!("decrypt_to_string expects Bytes value")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_keyset() -> Vec<u8> {
        keys_keyset_chain("gcp-kms://test-key")
            .unwrap()
            .as_bytes()
            .expect("Expected bytes")
            .to_vec()
    }

    #[test]
    fn test_keyset_chain() {
        let result = keys_keyset_chain("gcp-kms://my-key");
        assert!(result.is_ok());

        let value = result.unwrap();
        let keyset_bytes = value.as_bytes().expect("Expected bytes");

        let keyset = parse_keyset(keyset_bytes).unwrap();
        assert_eq!(keyset.key_id(), "gcp-kms://my-key");
    }

    #[test]
    fn test_aead_roundtrip() {
        let keyset_bytes = create_test_keyset();
        let plaintext = b"secret message";
        let aad = b"additional data";

        let encrypted = aead_encrypt(&keyset_bytes, plaintext, aad).unwrap();
        let ciphertext = encrypted.as_bytes().expect("Expected bytes");

        let decrypted = aead_decrypt_bytes(&keyset_bytes, ciphertext, aad).unwrap();
        let decrypted_bytes = decrypted.as_bytes().expect("Expected bytes");
        assert_eq!(decrypted_bytes, plaintext);
    }

    #[test]
    fn test_aead_string_roundtrip() {
        let keyset_bytes = create_test_keyset();
        let plaintext = "secret message";
        let aad = b"";

        let encrypted = aead_encrypt(&keyset_bytes, plaintext.as_bytes(), aad).unwrap();
        let ciphertext = encrypted.as_bytes().expect("Expected bytes");

        let decrypted = aead_decrypt_string(&keyset_bytes, ciphertext, aad).unwrap();
        let decrypted_str = decrypted.as_str().expect("Expected string");
        assert_eq!(decrypted_str, plaintext);
    }

    #[test]
    fn test_deterministic_same_input_same_output() {
        let keyset_bytes = create_test_keyset();
        let plaintext = b"data";
        let aad = b"";

        let encrypted1 = deterministic_encrypt(&keyset_bytes, plaintext, aad).unwrap();
        let encrypted2 = deterministic_encrypt(&keyset_bytes, plaintext, aad).unwrap();

        assert_eq!(
            encrypted1, encrypted2,
            "Deterministic encryption should produce same output"
        );
    }

    #[test]
    fn test_aead_wrong_key_fails() {
        let keyset1 = create_test_keyset();
        let keyset2 = keys_keyset_chain("gcp-kms://other-key")
            .unwrap()
            .as_bytes()
            .expect("Expected bytes")
            .to_vec();

        let plaintext = b"secret";
        let aad = b"";

        let encrypted = aead_encrypt(&keyset1, plaintext, aad).unwrap();
        let ciphertext = encrypted.as_bytes().expect("Expected bytes");

        let result = aead_decrypt_bytes(&keyset2, ciphertext, aad);
        assert!(result.is_err(), "Decryption with wrong key should fail");
    }

    #[test]
    fn test_aead_wrong_aad_fails() {
        let keyset_bytes = create_test_keyset();
        let plaintext = b"secret";
        let aad1 = b"aad1";
        let aad2 = b"aad2";

        let encrypted = aead_encrypt(&keyset_bytes, plaintext, aad1).unwrap();
        let ciphertext = encrypted.as_bytes().expect("Expected bytes");

        let result = aead_decrypt_bytes(&keyset_bytes, ciphertext, aad2);
        assert!(result.is_err(), "Decryption with wrong AAD should fail");
    }
}
