use crate::common::create_executor;

#[test]
fn test_rand() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT rand()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand64() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT rand64()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_constant() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randConstant()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_uniform() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randUniform(0, 10)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_normal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randNormal(0, 1)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_log_normal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randLogNormal(0, 1)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_exponential() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randExponential(1)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_chi_squared() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randChiSquared(5)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_student_t() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randStudentT(10)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_fisher_f() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randFisherF(5, 10)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_bernoulli() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randBernoulli(0.5)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_binomial() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT randBinomial(10, 0.5)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_negative_binomial() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT randNegativeBinomial(10, 0.5)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_rand_poisson() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randPoisson(5)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_generate_uuid_v4() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT generateUUIDv4()").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_random_string() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randomString(10)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_random_fixed_string() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT randomFixedString(10)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_random_printable_ascii() {
    let mut executor = create_executor();
    let result = executor
        .execute_sql("SELECT randomPrintableASCII(10)")
        .unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_random_string_utf8() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT randomStringUTF8(10)").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_faker_name() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT fakeData('name')").unwrap();
    assert!(result.num_rows() == 1); // TODO: use table![[expected_values]]
}

#[test]
fn test_random_in_select() {
    let mut executor = create_executor();
    executor
        .execute_sql("CREATE TABLE random_test (id INT64)")
        .unwrap();
    executor
        .execute_sql("INSERT INTO random_test VALUES (1), (2), (3)")
        .unwrap();

    let result = executor
        .execute_sql("SELECT id, rand() AS r FROM random_test ORDER BY id")
        .unwrap();
    assert!(result.num_rows() == 3); // TODO: use table![[expected_values]]
}
