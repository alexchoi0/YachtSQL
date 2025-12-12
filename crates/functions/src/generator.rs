use uuid::Uuid;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;

const MAX_ARRAY_SIZE: i64 = 1_000_000;

pub fn generate_uuid() -> Result<Value> {
    let uuid = Uuid::new_v4();

    Ok(Value::string(uuid.to_string()))
}

pub fn generate_uuid_array(count: i64) -> Result<Value> {
    if count < 0 {
        return Err(Error::invalid_query(
            "GENERATE_UUID_ARRAY count must be non-negative".to_string(),
        ));
    }

    if count > MAX_ARRAY_SIZE {
        return Err(Error::invalid_query(format!(
            "GENERATE_UUID_ARRAY count exceeds maximum ({})",
            MAX_ARRAY_SIZE
        )));
    }

    let mut uuids = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let uuid = generate_uuid()?;
        uuids.push(uuid);
    }

    Ok(Value::array(uuids))
}

pub fn generate_array(start: i64, end: i64, step: i64) -> Result<Value> {
    if step == 0 {
        return Err(Error::invalid_query(
            "GENERATE_ARRAY step cannot be zero".to_string(),
        ));
    }

    if (step > 0 && start > end) || (step < 0 && start < end) {
        return Ok(Value::array(vec![]));
    }

    let range = if step > 0 {
        match end.checked_sub(start) {
            Some(r) => r,
            None => {
                return Ok(Value::array(vec![]));
            }
        }
    } else {
        match start.checked_sub(end) {
            Some(r) => r,
            None => {
                return Ok(Value::array(vec![]));
            }
        }
    };

    let count = (range / step.abs()) + 1;

    if count > MAX_ARRAY_SIZE {
        return Err(Error::invalid_query(format!(
            "GENERATE_ARRAY would generate {} elements (max: {})",
            count, MAX_ARRAY_SIZE
        )));
    }

    let mut result = Vec::with_capacity(count as usize);
    let mut current = start;

    loop {
        if (step > 0 && current > end) || (step < 0 && current < end) {
            break;
        }

        result.push(Value::int64(current));

        match current.checked_add(step) {
            Some(next) => current = next,
            None => {
                break;
            }
        }
    }

    Ok(Value::array(result))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_uuid_format() {
        let result = generate_uuid().unwrap();

        if let Some(uuid_str) = result.as_str() {
            assert_eq!(uuid_str.len(), 36);

            assert_eq!(&uuid_str[8..9], "-");
            assert_eq!(&uuid_str[13..14], "-");
            assert_eq!(&uuid_str[18..19], "-");
            assert_eq!(&uuid_str[23..24], "-");

            assert_eq!(&uuid_str[14..15], "4");

            let variant = &uuid_str[19..20];
            assert!(
                variant == "8" || variant == "9" || variant == "a" || variant == "b",
                "UUID variant should be 8/9/a/b, got {}",
                variant
            );

            assert_eq!(uuid_str, &uuid_str.to_lowercase());
        } else {
            panic!("Expected String value, got {:?}", result);
        }
    }

    #[test]
    fn test_generate_uuid_uniqueness() {
        let mut uuids = std::collections::HashSet::new();

        for _ in 0..100 {
            let result = generate_uuid().unwrap();
            if let Some(uuid_str) = result.as_str() {
                assert!(
                    uuids.insert(uuid_str.to_string()),
                    "Generated duplicate UUID: {}",
                    uuid_str
                );
            }
        }

        assert_eq!(uuids.len(), 100, "Should generate 100 unique UUIDs");
    }

    #[test]
    fn test_generate_uuid_many() {
        let mut uuids = std::collections::HashSet::new();

        for _ in 0..100 {
            if let Ok(result) = generate_uuid() {
                if let Some(uuid_str) = result.as_str() {
                    uuids.insert(uuid_str.to_string());
                }
            }
        }

        assert_eq!(uuids.len(), 100, "Should generate 100 unique UUIDs");
    }
}
