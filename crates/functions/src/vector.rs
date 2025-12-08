use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

pub fn l2_distance(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let sum_squared_diff: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| (a - b).powi(2)).sum();

    Ok(Value::float64(sum_squared_diff.sqrt()))
}

pub fn l1_distance(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let sum_abs_diff: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| (a - b).abs()).sum();

    Ok(Value::float64(sum_abs_diff))
}

pub fn negative_inner_product(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let product: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    Ok(Value::float64(-product))
}

pub fn cosine_similarity(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let dot_product: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    let norm1: f64 = v1.iter().map(|x| x * x).sum::<f64>().sqrt();
    let norm2: f64 = v2.iter().map(|x| x * x).sum::<f64>().sqrt();

    if norm1 == 0.0 || norm2 == 0.0 {
        return Ok(Value::float64(0.0));
    }

    Ok(Value::float64(dot_product / (norm1 * norm2)))
}

pub fn cosine_distance(vec1: &Value, vec2: &Value) -> Result<Value> {
    let similarity = cosine_similarity(vec1, vec2)?;
    if similarity.is_null() {
        return Ok(Value::null());
    }
    let sim_val = similarity.as_f64().unwrap();
    Ok(Value::float64(1.0 - sim_val))
}

pub fn inner_product(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let product: f64 = v1.iter().zip(v2.iter()).map(|(a, b)| a * b).sum();
    Ok(Value::float64(product))
}

pub fn vector_norm(vec: &Value) -> Result<Value> {
    if vec.is_null() {
        return Ok(Value::null());
    }

    let v = vec.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec.data_type().to_string(),
    })?;

    let norm: f64 = v.iter().map(|x| x * x).sum::<f64>().sqrt();
    Ok(Value::float64(norm))
}

pub fn vector_normalize(vec: &Value) -> Result<Value> {
    if vec.is_null() {
        return Ok(Value::null());
    }

    let v = vec.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec.data_type().to_string(),
    })?;

    let norm: f64 = v.iter().map(|x| x * x).sum::<f64>().sqrt();

    if norm == 0.0 {
        return Err(Error::invalid_query(
            "Cannot normalize zero vector".to_string(),
        ));
    }

    let normalized: Vec<f64> = v.iter().map(|x| x / norm).collect();
    Ok(Value::vector(normalized))
}

pub fn vector_dims(vec: &Value) -> Result<Value> {
    if vec.is_null() {
        return Ok(Value::null());
    }

    let v = vec.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec.data_type().to_string(),
    })?;

    Ok(Value::int64(v.len() as i64))
}

pub fn vector_add(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let result: Vec<f64> = v1.iter().zip(v2.iter()).map(|(a, b)| a + b).collect();
    Ok(Value::vector(result))
}

pub fn vector_subtract(vec1: &Value, vec2: &Value) -> Result<Value> {
    if vec1.is_null() || vec2.is_null() {
        return Ok(Value::null());
    }

    let v1 = vec1.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec1.data_type().to_string(),
    })?;

    let v2 = vec2.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec2.data_type().to_string(),
    })?;

    if v1.len() != v2.len() {
        return Err(Error::invalid_query(format!(
            "Vector dimension mismatch: {} vs {}",
            v1.len(),
            v2.len()
        )));
    }

    let result: Vec<f64> = v1.iter().zip(v2.iter()).map(|(a, b)| a - b).collect();
    Ok(Value::vector(result))
}

pub fn vector_scalar_multiply(vec: &Value, scalar: &Value) -> Result<Value> {
    if vec.is_null() || scalar.is_null() {
        return Ok(Value::null());
    }

    let v = vec.as_vector().ok_or_else(|| Error::TypeMismatch {
        expected: "VECTOR".to_string(),
        actual: vec.data_type().to_string(),
    })?;

    let s = scalar.as_f64().ok_or_else(|| Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: scalar.data_type().to_string(),
    })?;

    let result: Vec<f64> = v.iter().map(|x| x * s).collect();
    Ok(Value::vector(result))
}

#[cfg(test)]
#[allow(clippy::approx_constant)]
mod tests {
    use super::*;

    #[test]
    fn test_l2_distance() {
        let v1 = Value::vector(vec![1.0, 0.0, 0.0]);
        let v2 = Value::vector(vec![0.0, 1.0, 0.0]);
        let dist = l2_distance(&v1, &v2).unwrap();
        assert!((dist.as_f64().unwrap() - 1.414213).abs() < 0.0001);
    }

    #[test]
    fn test_cosine_similarity() {
        let v1 = Value::vector(vec![1.0, 0.0, 0.0]);
        let v2 = Value::vector(vec![1.0, 0.0, 0.0]);
        let sim = cosine_similarity(&v1, &v2).unwrap();
        assert!((sim.as_f64().unwrap() - 1.0).abs() < 0.0001);
    }

    #[test]
    fn test_inner_product() {
        let v1 = Value::vector(vec![1.0, 2.0, 3.0]);
        let v2 = Value::vector(vec![4.0, 5.0, 6.0]);
        let prod = inner_product(&v1, &v2).unwrap();
        assert_eq!(prod.as_f64().unwrap(), 32.0);
    }

    #[test]
    fn test_vector_norm() {
        let v = Value::vector(vec![3.0, 4.0]);
        let norm = vector_norm(&v).unwrap();
        assert_eq!(norm.as_f64().unwrap(), 5.0);
    }

    #[test]
    fn test_vector_normalize() {
        let v = Value::vector(vec![3.0, 4.0]);
        let normalized = vector_normalize(&v).unwrap();
        let n = normalized.as_vector().unwrap();
        assert!((n[0] - 0.6).abs() < 0.0001);
        assert!((n[1] - 0.8).abs() < 0.0001);
    }

    #[test]
    fn test_vector_dims() {
        let v = Value::vector(vec![1.0, 2.0, 3.0, 4.0, 5.0]);
        let dims = vector_dims(&v).unwrap();
        assert_eq!(dims.as_i64().unwrap(), 5);
    }

    #[test]
    fn test_vector_add() {
        let v1 = Value::vector(vec![1.0, 2.0, 3.0]);
        let v2 = Value::vector(vec![4.0, 5.0, 6.0]);
        let sum = vector_add(&v1, &v2).unwrap();
        let s = sum.as_vector().unwrap();
        assert_eq!(s, &vec![5.0, 7.0, 9.0]);
    }

    #[test]
    fn test_vector_subtract() {
        let v1 = Value::vector(vec![4.0, 5.0, 6.0]);
        let v2 = Value::vector(vec![1.0, 2.0, 3.0]);
        let diff = vector_subtract(&v1, &v2).unwrap();
        let d = diff.as_vector().unwrap();
        assert_eq!(d, &vec![3.0, 3.0, 3.0]);
    }

    #[test]
    fn test_vector_scalar_multiply() {
        let v = Value::vector(vec![1.0, 2.0, 3.0]);
        let scalar = Value::float64(2.0);
        let result = vector_scalar_multiply(&v, &scalar).unwrap();
        let r = result.as_vector().unwrap();
        assert_eq!(r, &vec![2.0, 4.0, 6.0]);
    }
}
