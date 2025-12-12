use debug_print::debug_eprintln;
use yachtsql_common::types::Value;
use yachtsql_functions::FunctionRegistry;

#[test]
fn test_safe_subtract_overflow_direct() {
    let registry = FunctionRegistry::new();
    let func = registry
        .get_scalar("SAFE_SUBTRACT")
        .expect("SAFE_SUBTRACT not found");

    let int64_min: i64 = -9223372036854775808;
    let args = vec![Value::int64(int64_min), Value::int64(1)];

    let result = func.evaluate(&args).expect("Evaluation failed");
    debug_eprintln!("[test::safe_direct] Result: {:?}", result);
    debug_eprintln!("[test::safe_direct] Is NULL: {}", result.is_null());

    assert!(
        result.is_null(),
        "Expected NULL on overflow, got {:?}",
        result
    );
}

#[test]
fn test_safe_add_overflow_direct() {
    let registry = FunctionRegistry::new();
    let func = registry.get_scalar("SAFE_ADD").expect("SAFE_ADD not found");

    let int64_max: i64 = 9223372036854775807;
    let args = vec![Value::int64(int64_max), Value::int64(1)];

    let result = func.evaluate(&args).expect("Evaluation failed");
    debug_eprintln!("[test::safe_direct] Result: {:?}", result);
    debug_eprintln!("[test::safe_direct] Is NULL: {}", result.is_null());

    assert!(
        result.is_null(),
        "Expected NULL on overflow, got {:?}",
        result
    );
}

#[test]
fn test_safe_subtract_overflow_float64() {
    let registry = FunctionRegistry::new();
    let func = registry
        .get_scalar("SAFE_SUBTRACT")
        .expect("SAFE_SUBTRACT not found");

    let int64_min_f64: f64 = -9.223372036854776e18;
    let args = vec![Value::float64(int64_min_f64), Value::float64(1.0)];

    let result = func.evaluate(&args).expect("Evaluation failed");
    debug_eprintln!(
        "[test::safe_direct] Input: Float64({}) - Float64(1.0)",
        int64_min_f64
    );
    debug_eprintln!("[test::safe_direct] Result: {:?}", result);
    debug_eprintln!("[test::safe_direct] Is NULL: {}", result.is_null());

    assert!(
        result.is_null(),
        "Expected NULL on overflow with Float64, got {:?}",
        result
    );
}
