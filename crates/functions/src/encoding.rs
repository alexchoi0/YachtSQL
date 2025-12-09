use base64::Engine;
use base64::engine::general_purpose::{STANDARD, URL_SAFE};
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

pub fn hex_encode(input: &str) -> Result<Value> {
    let hex_string = input
        .as_bytes()
        .iter()
        .map(|b| format!("{:02X}", b))
        .collect::<String>();
    Ok(Value::string(hex_string))
}

pub fn hex_encode_bytes(input: &[u8]) -> Result<Value> {
    let hex_string = input
        .iter()
        .map(|b| format!("{:02X}", b))
        .collect::<String>();
    Ok(Value::string(hex_string))
}

pub fn unhex(input: &str) -> Result<Value> {
    let input = input.trim();
    if !input.len().is_multiple_of(2) {
        return Err(Error::invalid_query("Invalid hex string: odd length"));
    }

    let bytes: Result<Vec<u8>> = (0..input.len())
        .step_by(2)
        .map(|i| {
            u8::from_str_radix(&input[i..i + 2], 16)
                .map_err(|_| Error::invalid_query("Invalid hex character"))
        })
        .collect();

    let bytes = bytes?;
    match String::from_utf8(bytes.clone()) {
        Ok(s) => Ok(Value::string(s)),
        Err(_) => Ok(Value::bytes(bytes)),
    }
}

pub fn base64_encode(input: &str) -> Result<Value> {
    let encoded = STANDARD.encode(input.as_bytes());
    Ok(Value::string(encoded))
}

pub fn base64_decode(input: &str) -> Result<Value> {
    let decoded = STANDARD
        .decode(input.trim())
        .map_err(|e| Error::invalid_query(format!("Invalid base64 string: {}", e)))?;

    match String::from_utf8(decoded.clone()) {
        Ok(s) => Ok(Value::string(s)),
        Err(_) => Ok(Value::bytes(decoded)),
    }
}

pub fn try_base64_decode(input: &str) -> Result<Value> {
    match STANDARD.decode(input.trim()) {
        Ok(decoded) => match String::from_utf8(decoded.clone()) {
            Ok(s) => Ok(Value::string(s)),
            Err(_) => Ok(Value::bytes(decoded)),
        },
        Err(_) => Ok(Value::null()),
    }
}

pub fn base64_url_encode(input: &str) -> Result<Value> {
    let encoded = URL_SAFE.encode(input.as_bytes());
    Ok(Value::string(encoded))
}

pub fn base64_url_decode(input: &str) -> Result<Value> {
    let decoded = URL_SAFE
        .decode(input.trim())
        .map_err(|e| Error::invalid_query(format!("Invalid base64 URL string: {}", e)))?;

    match String::from_utf8(decoded.clone()) {
        Ok(s) => Ok(Value::string(s)),
        Err(_) => Ok(Value::bytes(decoded)),
    }
}

pub fn try_base64_url_decode(input: &str) -> Result<Value> {
    match URL_SAFE.decode(input.trim()) {
        Ok(decoded) => match String::from_utf8(decoded.clone()) {
            Ok(s) => Ok(Value::string(s)),
            Err(_) => Ok(Value::bytes(decoded)),
        },
        Err(_) => Ok(Value::null()),
    }
}

const BASE58_ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

pub fn base58_encode(input: &str) -> Result<Value> {
    let bytes = input.as_bytes();
    if bytes.is_empty() {
        return Ok(Value::string("".to_string()));
    }

    let mut digits = vec![0u8];
    for &byte in bytes {
        let mut carry = byte as usize;
        for digit in &mut digits {
            carry += (*digit as usize) * 256;
            *digit = (carry % 58) as u8;
            carry /= 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }

    let leading_zeros = bytes.iter().take_while(|&&b| b == 0).count();
    let mut result = vec![b'1'; leading_zeros];
    for &digit in digits.iter().rev() {
        result.push(BASE58_ALPHABET[digit as usize]);
    }

    Ok(Value::string(String::from_utf8(result).unwrap()))
}

pub fn base58_decode(input: &str) -> Result<Value> {
    let input = input.trim();
    if input.is_empty() {
        return Ok(Value::string("".to_string()));
    }

    let mut digits = vec![0u8];
    for ch in input.chars() {
        let index = BASE58_ALPHABET.iter().position(|&c| c == ch as u8);
        let index = match index {
            Some(i) => i,
            None => {
                return Err(Error::invalid_query(format!(
                    "Invalid base58 character: {}",
                    ch
                )));
            }
        };

        let mut carry = index;
        for digit in &mut digits {
            carry += (*digit as usize) * 58;
            *digit = (carry % 256) as u8;
            carry /= 256;
        }
        while carry > 0 {
            digits.push((carry % 256) as u8);
            carry /= 256;
        }
    }

    let leading_ones = input.chars().take_while(|&c| c == '1').count();
    let mut result = vec![0u8; leading_ones];
    for &digit in digits.iter().rev() {
        result.push(digit);
    }

    match String::from_utf8(result.clone()) {
        Ok(s) => Ok(Value::string(s)),
        Err(_) => Ok(Value::bytes(result)),
    }
}

pub fn bin(value: i64) -> Result<Value> {
    Ok(Value::string(format!("{:b}", value)))
}

pub fn unbin(input: &str) -> Result<Value> {
    let value = i64::from_str_radix(input.trim(), 2)
        .map_err(|_| Error::invalid_query("Invalid binary string"))?;
    Ok(Value::int64(value))
}

pub fn bit_shift_left(value: i64, shift: i64) -> Result<Value> {
    if shift < 0 || shift >= 64 {
        return Ok(Value::int64(0));
    }
    Ok(Value::int64(value << shift))
}

pub fn bit_shift_right(value: i64, shift: i64) -> Result<Value> {
    if shift < 0 || shift >= 64 {
        return Ok(Value::int64(0));
    }
    Ok(Value::int64(value >> shift))
}

pub fn bit_and(a: i64, b: i64) -> Result<Value> {
    Ok(Value::int64(a & b))
}

pub fn bit_or(a: i64, b: i64) -> Result<Value> {
    Ok(Value::int64(a | b))
}

pub fn bit_xor(a: i64, b: i64) -> Result<Value> {
    Ok(Value::int64(a ^ b))
}

pub fn bit_not(value: i64) -> Result<Value> {
    Ok(Value::int64(!value))
}

pub fn bit_count(value: i64) -> Result<Value> {
    Ok(Value::int64(value.count_ones() as i64))
}

pub fn bit_test(value: i64, bit_pos: i64) -> Result<Value> {
    if bit_pos < 0 || bit_pos >= 64 {
        return Ok(Value::int64(0));
    }
    let result = (value >> bit_pos) & 1;
    Ok(Value::int64(result))
}

pub fn bit_test_all(value: i64, positions: &[i64]) -> Result<Value> {
    for &pos in positions {
        if pos < 0 || pos >= 64 {
            return Ok(Value::int64(0));
        }
        if (value >> pos) & 1 == 0 {
            return Ok(Value::int64(0));
        }
    }
    Ok(Value::int64(1))
}

pub fn bit_test_any(value: i64, positions: &[i64]) -> Result<Value> {
    for &pos in positions {
        if pos >= 0 && pos < 64 && (value >> pos) & 1 == 1 {
            return Ok(Value::int64(1));
        }
    }
    Ok(Value::int64(0))
}

pub fn bit_rotate_left(value: u8, shift: i64) -> Result<Value> {
    let shift = (shift % 8) as u32;
    let result = value.rotate_left(shift);
    Ok(Value::int64(result as i64))
}

pub fn bit_rotate_right(value: u8, shift: i64) -> Result<Value> {
    let shift = (shift % 8) as u32;
    let result = value.rotate_right(shift);
    Ok(Value::int64(result as i64))
}

pub fn bit_hamming_distance(a: i64, b: i64) -> Result<Value> {
    let xor = a ^ b;
    Ok(Value::int64(xor.count_ones() as i64))
}

pub fn bit_slice(value: u8, start: i64, length: i64) -> Result<Value> {
    if length <= 0 {
        return Ok(Value::int64(0));
    }
    let start = start.max(0) as u32;
    let length = length.min(8 - start as i64) as u32;
    if start >= 8 {
        return Ok(Value::int64(0));
    }
    let mask = ((1u16 << length) - 1) as u8;
    let result = (value >> start) & mask;
    Ok(Value::int64(result as i64))
}

pub fn byte_swap_16(value: u16) -> Result<Value> {
    Ok(Value::int64(value.swap_bytes() as i64))
}

pub fn byte_swap_32(value: u32) -> Result<Value> {
    Ok(Value::int64(value.swap_bytes() as i64))
}

pub fn byte_swap_64(value: u64) -> Result<Value> {
    Ok(Value::int64(value.swap_bytes() as i64))
}

pub fn bit_positions_to_array(value: i64) -> Result<Value> {
    let mut positions = Vec::new();
    for i in 0..64 {
        if (value >> i) & 1 == 1 {
            positions.push(Value::int64(i));
        }
    }
    Ok(Value::array(positions))
}

pub fn char_fn(codes: &[i64]) -> Result<Value> {
    let chars: String = codes
        .iter()
        .filter_map(|&code| {
            if code >= 0 && code <= 255 {
                Some(code as u8 as char)
            } else {
                None
            }
        })
        .collect();
    Ok(Value::string(chars))
}
