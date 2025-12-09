use std::rc::Rc;

use rand::Rng;
use rand_distr::{
    Bernoulli, Binomial, ChiSquared, Distribution, Exp, FisherF, LogNormal, Normal, Poisson,
    StudentT,
};
use uuid::Uuid;
use yachtsql_core::error::Error;
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    register_basic_random(registry);
    register_distribution_random(registry);
    register_string_random(registry);
    register_uuid_random(registry);
}

fn register_basic_random(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "RAND".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RAND".to_string(),
            arg_types: vec![],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_| {
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<u32>() as i64))
            },
        }),
    );

    registry.register_scalar(
        "RAND32".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RAND32".to_string(),
            arg_types: vec![],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_| {
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<u32>() as i64))
            },
        }),
    );

    registry.register_scalar(
        "RAND64".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RAND64".to_string(),
            arg_types: vec![],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_| {
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<i64>()))
            },
        }),
    );

    registry.register_scalar(
        "RANDCONSTANT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDCONSTANT".to_string(),
            arg_types: vec![],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |_| {
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<u32>() as i64))
            },
        }),
    );

    registry.register_scalar(
        "RANDUNIFORM".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDUNIFORM".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("randUniform requires 2 arguments"));
                }
                let min = extract_f64(&args[0])?;
                let max = extract_f64(&args[1])?;
                if min >= max {
                    return Err(Error::invalid_query(
                        "randUniform: min must be less than max",
                    ));
                }
                let mut rng = rand::thread_rng();
                let value = rng.gen_range(min..max);
                Ok(Value::float64(value))
            },
        }),
    );
}

fn register_distribution_random(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "RANDNORMAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDNORMAL".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("randNormal requires 2 arguments"));
                }
                let mean = extract_f64(&args[0])?;
                let stddev = extract_f64(&args[1])?;
                if stddev <= 0.0 {
                    return Err(Error::invalid_query("randNormal: stddev must be positive"));
                }
                let normal =
                    Normal::new(mean, stddev).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(normal.sample(&mut rng)))
            },
        }),
    );

    registry.register_scalar(
        "RANDLOGNORMAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDLOGNORMAL".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("randLogNormal requires 2 arguments"));
                }
                let mean = extract_f64(&args[0])?;
                let stddev = extract_f64(&args[1])?;
                if stddev <= 0.0 {
                    return Err(Error::invalid_query(
                        "randLogNormal: stddev must be positive",
                    ));
                }
                let log_normal = LogNormal::new(mean, stddev)
                    .map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(log_normal.sample(&mut rng)))
            },
        }),
    );

    registry.register_scalar(
        "RANDEXPONENTIAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDEXPONENTIAL".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randExponential requires 1 argument"));
                }
                let lambda = extract_f64(&args[0])?;
                if lambda <= 0.0 {
                    return Err(Error::invalid_query(
                        "randExponential: lambda must be positive",
                    ));
                }
                let exp = Exp::new(lambda).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(exp.sample(&mut rng)))
            },
        }),
    );

    registry.register_scalar(
        "RANDCHISQUARED".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDCHISQUARED".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randChiSquared requires 1 argument"));
                }
                let k = extract_f64(&args[0])?;
                if k <= 0.0 {
                    return Err(Error::invalid_query(
                        "randChiSquared: degrees of freedom must be positive",
                    ));
                }
                let chi_squared =
                    ChiSquared::new(k).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(chi_squared.sample(&mut rng)))
            },
        }),
    );

    registry.register_scalar(
        "RANDSTUDENTT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDSTUDENTT".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randStudentT requires 1 argument"));
                }
                let df = extract_f64(&args[0])?;
                if df <= 0.0 {
                    return Err(Error::invalid_query(
                        "randStudentT: degrees of freedom must be positive",
                    ));
                }
                let student_t =
                    StudentT::new(df).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(student_t.sample(&mut rng)))
            },
        }),
    );

    registry.register_scalar(
        "RANDFISHERF".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDFISHERF".to_string(),
            arg_types: vec![DataType::Float64, DataType::Float64],
            return_type: DataType::Float64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("randFisherF requires 2 arguments"));
                }
                let d1 = extract_f64(&args[0])?;
                let d2 = extract_f64(&args[1])?;
                if d1 <= 0.0 || d2 <= 0.0 {
                    return Err(Error::invalid_query(
                        "randFisherF: degrees of freedom must be positive",
                    ));
                }
                let fisher_f =
                    FisherF::new(d1, d2).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(fisher_f.sample(&mut rng)))
            },
        }),
    );

    registry.register_scalar(
        "RANDBERNOULLI".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDBERNOULLI".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randBernoulli requires 1 argument"));
                }
                let p = extract_f64(&args[0])?;
                if !(0.0..=1.0).contains(&p) {
                    return Err(Error::invalid_query(
                        "randBernoulli: probability must be between 0 and 1",
                    ));
                }
                let bernoulli =
                    Bernoulli::new(p).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(if bernoulli.sample(&mut rng) { 1 } else { 0 }))
            },
        }),
    );

    registry.register_scalar(
        "RANDBINOMIAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDBINOMIAL".to_string(),
            arg_types: vec![DataType::Int64, DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query("randBinomial requires 2 arguments"));
                }
                let n = extract_i64(&args[0])?;
                let p = extract_f64(&args[1])?;
                if n < 0 {
                    return Err(Error::invalid_query("randBinomial: n must be non-negative"));
                }
                if !(0.0..=1.0).contains(&p) {
                    return Err(Error::invalid_query(
                        "randBinomial: probability must be between 0 and 1",
                    ));
                }
                let binomial =
                    Binomial::new(n as u64, p).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(binomial.sample(&mut rng) as i64))
            },
        }),
    );

    registry.register_scalar(
        "RANDNEGATIVEBINOMIAL".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDNEGATIVEBINOMIAL".to_string(),
            arg_types: vec![DataType::Int64, DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "randNegativeBinomial requires 2 arguments",
                    ));
                }
                let r = extract_i64(&args[0])?;
                let p = extract_f64(&args[1])?;
                if r <= 0 {
                    return Err(Error::invalid_query(
                        "randNegativeBinomial: r must be positive",
                    ));
                }
                if !(0.0..=1.0).contains(&p) {
                    return Err(Error::invalid_query(
                        "randNegativeBinomial: probability must be between 0 and 1",
                    ));
                }
                let mut rng = rand::thread_rng();
                let mut count = 0i64;
                let mut successes = 0;
                while successes < r {
                    if rng.gen_bool(p) {
                        successes += 1;
                    } else {
                        count += 1;
                    }
                }
                Ok(Value::int64(count))
            },
        }),
    );

    registry.register_scalar(
        "RANDPOISSON".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDPOISSON".to_string(),
            arg_types: vec![DataType::Float64],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randPoisson requires 1 argument"));
                }
                let lambda = extract_f64(&args[0])?;
                if lambda <= 0.0 {
                    return Err(Error::invalid_query("randPoisson: lambda must be positive"));
                }
                let poisson =
                    Poisson::new(lambda).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(poisson.sample(&mut rng) as i64))
            },
        }),
    );
}

fn register_string_random(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "RANDOMSTRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDOMSTRING".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randomString requires 1 argument"));
                }
                let length = extract_i64(&args[0])?;
                if length < 0 {
                    return Err(Error::invalid_query(
                        "randomString: length must be non-negative",
                    ));
                }
                let mut rng = rand::thread_rng();
                let s: String = (0..length)
                    .map(|_| rng.gen_range(0..=255) as u8 as char)
                    .collect();
                Ok(Value::string(s))
            },
        }),
    );

    registry.register_scalar(
        "RANDOMFIXEDSTRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDOMFIXEDSTRING".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "randomFixedString requires 1 argument",
                    ));
                }
                let length = extract_i64(&args[0])?;
                if length < 0 {
                    return Err(Error::invalid_query(
                        "randomFixedString: length must be non-negative",
                    ));
                }
                let mut rng = rand::thread_rng();
                let bytes: Vec<u8> = (0..length).map(|_| rng.r#gen::<u8>()).collect();
                Ok(Value::bytes(bytes))
            },
        }),
    );

    registry.register_scalar(
        "RANDOMPRINTABLEASCII".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDOMPRINTABLEASCII".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "randomPrintableASCII requires 1 argument",
                    ));
                }
                let length = extract_i64(&args[0])?;
                if length < 0 {
                    return Err(Error::invalid_query(
                        "randomPrintableASCII: length must be non-negative",
                    ));
                }
                let mut rng = rand::thread_rng();
                let s: String = (0..length)
                    .map(|_| rng.gen_range(32..=126) as u8 as char)
                    .collect();
                Ok(Value::string(s))
            },
        }),
    );

    registry.register_scalar(
        "RANDOMSTRINGUTF8".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "RANDOMSTRINGUTF8".to_string(),
            arg_types: vec![DataType::Int64],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("randomStringUTF8 requires 1 argument"));
                }
                let length = extract_i64(&args[0])?;
                if length < 0 {
                    return Err(Error::invalid_query(
                        "randomStringUTF8: length must be non-negative",
                    ));
                }
                let mut rng = rand::thread_rng();
                let s: String = (0..length)
                    .map(|_| {
                        let code_point = rng.gen_range(0x20..=0x7E);
                        char::from_u32(code_point).unwrap_or('?')
                    })
                    .collect();
                Ok(Value::string(s))
            },
        }),
    );

    registry.register_scalar(
        "FAKEDATA".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "FAKEDATA".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args.len() != 1 {
                    return Err(Error::invalid_query("fakeData requires 1 argument"));
                }
                let data_type = args[0]
                    .as_str()
                    .ok_or_else(|| Error::invalid_query("fakeData: argument must be a string"))?;
                let result = match data_type.to_lowercase().as_str() {
                    "name" => generate_fake_name(),
                    "email" => generate_fake_email(),
                    "phone" => generate_fake_phone(),
                    "address" => generate_fake_address(),
                    "city" => generate_fake_city(),
                    "country" => generate_fake_country(),
                    "company" => generate_fake_company(),
                    _ => format!("fake_{}", data_type),
                };
                Ok(Value::string(result))
            },
        }),
    );
}

fn register_uuid_random(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "GENERATEUUIDV4".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "GENERATEUUIDV4".to_string(),
            arg_types: vec![],
            return_type: DataType::String,
            variadic: false,
            evaluator: |_| {
                let uuid = Uuid::new_v4();
                Ok(Value::string(uuid.to_string()))
            },
        }),
    );
}

fn extract_f64(value: &Value) -> Result<f64, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected numeric value, got NULL"));
    }
    if let Some(f) = value.as_f64() {
        return Ok(f);
    }
    if let Some(i) = value.as_i64() {
        return Ok(i as f64);
    }
    if let Some(n) = value.as_numeric() {
        return n
            .to_string()
            .parse()
            .map_err(|_| Error::invalid_query("Failed to parse numeric"));
    }
    Err(Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn extract_i64(value: &Value) -> Result<i64, Error> {
    if value.is_null() {
        return Err(Error::invalid_query("Expected integer value, got NULL"));
    }
    if let Some(i) = value.as_i64() {
        return Ok(i);
    }
    if let Some(f) = value.as_f64() {
        return Ok(f as i64);
    }
    Err(Error::TypeMismatch {
        expected: "INT64".to_string(),
        actual: value.data_type().to_string(),
    })
}

fn generate_fake_name() -> String {
    let first_names = [
        "John", "Jane", "Alice", "Bob", "Charlie", "Diana", "Eve", "Frank",
    ];
    let last_names = [
        "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    ];
    let mut rng = rand::thread_rng();
    format!(
        "{} {}",
        first_names[rng.gen_range(0..first_names.len())],
        last_names[rng.gen_range(0..last_names.len())]
    )
}

fn generate_fake_email() -> String {
    let mut rng = rand::thread_rng();
    let domains = ["example.com", "test.org", "mail.net", "company.io"];
    let name: String = (0..8).map(|_| rng.gen_range(b'a'..=b'z') as char).collect();
    format!("{}@{}", name, domains[rng.gen_range(0..domains.len())])
}

fn generate_fake_phone() -> String {
    let mut rng = rand::thread_rng();
    format!(
        "+1-{:03}-{:03}-{:04}",
        rng.gen_range(100..999),
        rng.gen_range(100..999),
        rng.gen_range(1000..9999)
    )
}

fn generate_fake_address() -> String {
    let mut rng = rand::thread_rng();
    let streets = ["Main St", "Oak Ave", "Elm Dr", "Maple Blvd", "Pine Rd"];
    format!(
        "{} {}",
        rng.gen_range(1..9999),
        streets[rng.gen_range(0..streets.len())]
    )
}

fn generate_fake_city() -> String {
    let cities = [
        "New York",
        "Los Angeles",
        "Chicago",
        "Houston",
        "Phoenix",
        "Philadelphia",
        "San Antonio",
        "San Diego",
    ];
    let mut rng = rand::thread_rng();
    cities[rng.gen_range(0..cities.len())].to_string()
}

fn generate_fake_country() -> String {
    let countries = [
        "USA",
        "Canada",
        "UK",
        "Germany",
        "France",
        "Japan",
        "Australia",
        "Brazil",
    ];
    let mut rng = rand::thread_rng();
    countries[rng.gen_range(0..countries.len())].to_string()
}

fn generate_fake_company() -> String {
    let prefixes = [
        "Global",
        "Tech",
        "United",
        "Dynamic",
        "Premier",
        "Elite",
        "Innovative",
    ];
    let suffixes = [
        "Corp",
        "Inc",
        "LLC",
        "Systems",
        "Solutions",
        "Group",
        "Industries",
    ];
    let mut rng = rand::thread_rng();
    format!(
        "{} {}",
        prefixes[rng.gen_range(0..prefixes.len())],
        suffixes[rng.gen_range(0..suffixes.len())]
    )
}
