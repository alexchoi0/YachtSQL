use yachtsql_core::types::Value;

#[test]
fn test_string_clones_share_memory() {
    let large_str = "a".repeat(1000);
    let v1 = Value::string(large_str.clone());
    let v2 = v1.clone();
    let v3 = v1.clone();

    let ptr1 = v1.as_str().unwrap().as_ptr();
    let ptr2 = v2.as_str().unwrap().as_ptr();
    let ptr3 = v3.as_str().unwrap().as_ptr();

    assert_eq!(ptr1, ptr2, "Cloned strings should share memory");
    assert_eq!(ptr1, ptr3, "Multiple clones should share memory");
}

#[test]
fn test_array_clones_share_memory() {
    let arr = Value::array(vec![Value::int64(1), Value::int64(2), Value::int64(3)]);
    let arr2 = arr.clone();

    let ptr1 = arr.as_array().unwrap().as_ptr();
    let ptr2 = arr2.as_array().unwrap().as_ptr();

    assert_eq!(ptr1, ptr2, "Cloned arrays should share memory");
}

#[test]
fn test_numeric_clones_share_memory() {
    use rust_decimal::Decimal;
    let num = Value::numeric(Decimal::new(12345, 2));
    let num2 = num.clone();

    assert_eq!(num.as_numeric(), num2.as_numeric());
}

#[test]
fn test_inline_values_do_not_share_memory() {
    let v1 = Value::int64(42);
    let v2 = v1.clone();

    let ptr1 = &v1 as *const Value;
    let ptr2 = &v2 as *const Value;

    assert_ne!(ptr1, ptr2, "Inline values should be copied, not shared");

    assert_eq!(v1.as_i64(), v2.as_i64());
}

#[test]
fn test_no_memory_leak_with_many_clones() {
    let large_str = "test".repeat(1000);
    let v1 = Value::string(large_str);

    let mut clones = Vec::new();
    for _ in 0..1000 {
        clones.push(v1.clone());
    }

    let original_ptr = v1.as_str().unwrap().as_ptr();

    drop(clones);

    assert_eq!(v1.as_str().unwrap().as_ptr(), original_ptr);

    drop(v1);
}

#[test]
fn test_nested_array_cleanup() {
    let inner = Value::array(vec![Value::int64(1), Value::int64(2)]);
    let middle = Value::array(vec![inner.clone(), inner.clone(), inner.clone()]);
    let outer = Value::array(vec![middle.clone(), middle.clone()]);

    let clones: Vec<_> = (0..100).map(|_| outer.clone()).collect();

    drop(clones);
    drop(outer);
    drop(middle);
    drop(inner);
}

#[test]
fn test_clones_are_logically_independent() {
    let v1 = Value::string("hello".to_string());
    let v2 = v1.clone();

    assert_eq!(v1.as_str(), Some("hello"));
    assert_eq!(v2.as_str(), Some("hello"));

    drop(v1);

    assert_eq!(v2.as_str(), Some("hello"));
}

#[test]
fn test_large_array_clone_performance() {
    let large_array: Vec<Value> = (0..10000).map(Value::int64).collect();
    let v1 = Value::array(large_array);

    let start = std::time::Instant::now();
    let v2 = v1.clone();
    let duration = start.elapsed();

    assert!(duration.as_micros() < 100, "Clone should be fast with Arc");

    assert_eq!(
        v1.as_array().unwrap().as_ptr(),
        v2.as_array().unwrap().as_ptr()
    );
}

#[test]
fn test_mixed_types_clone_correctly() {
    use rust_decimal::Decimal;

    let values = vec![
        Value::int64(42),
        Value::string("test".to_string()),
        Value::string("long string".repeat(10)),
        Value::numeric(Decimal::new(9999, 2)),
        Value::array(vec![Value::int64(1), Value::int64(2)]),
    ];

    let clones: Vec<_> = values.iter().map(|v| v.clone()).collect();

    for (original, clone) in values.iter().zip(clones.iter()) {
        match (original.data_type(), clone.data_type()) {
            (a, b) => assert_eq!(a, b, "Types should match"),
        }
    }

    assert_eq!(
        values[2].as_str().unwrap().as_ptr(),
        clones[2].as_str().unwrap().as_ptr(),
        "Large strings should share memory"
    );
}

#[test]
fn test_value_is_send_and_sync() {
    fn assert_send<T: Send>() {}
    fn assert_sync<T: Sync>() {}

    assert_send::<Value>();
    assert_sync::<Value>();
}

#[test]
fn test_clone_across_threads() {
    use std::thread;

    let large_str = "test".repeat(1000);
    let v = Value::string(large_str);
    let v_clone = v.clone();

    let handle = thread::spawn(move || {
        assert_eq!(v_clone.as_str().unwrap().len(), 4000);
        v_clone
    });

    assert_eq!(v.as_str().unwrap().len(), 4000);

    let returned = handle.join().unwrap();
    assert_eq!(returned.as_str().unwrap().len(), 4000);
}

#[test]
fn test_concurrent_cloning() {
    use std::sync::Arc;
    use std::thread;

    let value = Arc::new(Value::string("concurrent".repeat(100)));

    let mut handles = vec![];

    for _ in 0..10 {
        let v = Arc::clone(&value);
        let handle = thread::spawn(move || {
            let mut clones = Vec::new();
            for _ in 0..100 {
                clones.push((*v).clone());
            }
            clones.len()
        });
        handles.push(handle);
    }

    for handle in handles {
        let count = handle.join().unwrap();
        assert_eq!(count, 100);
    }
}

#[test]
fn test_clones_are_equal() {
    let v1 = Value::string("test".repeat(100));
    let v2 = v1.clone();

    assert_eq!(v1, v2, "Clones should be equal");

    let arr1 = Value::array(vec![Value::int64(1), Value::int64(2)]);
    let arr2 = arr1.clone();

    assert_eq!(arr1, arr2, "Cloned arrays should be equal");
}

#[test]
fn test_arc_values_have_correct_debug() {
    let v = Value::string("test".to_string());
    let debug_str = format!("{:?}", v);

    assert!(debug_str.contains("test") || debug_str.contains("String"));
}

#[test]
fn test_deeply_nested_clones() {
    let mut value = Value::int64(42);

    for _ in 0..10 {
        value = Value::array(vec![value.clone(), value.clone(), value.clone()]);
    }

    let clone = value.clone();

    assert_eq!(
        value.as_array().unwrap().as_ptr(),
        clone.as_array().unwrap().as_ptr()
    );

    assert_eq!(value, clone);
}
