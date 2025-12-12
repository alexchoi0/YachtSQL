use rand::Rng;
use rand::distributions::{Alphanumeric, Bernoulli, Distribution, Standard, Uniform};
use rand_distr::{Binomial, ChiSquared, Exp, FisherF, LogNormal, Normal, Poisson, StudentT};
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand(
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        let mut rng = rand::thread_rng();
        let value: u32 = rng.r#gen();
        Ok(Value::int64(value as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand64(
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        let mut rng = rand::thread_rng();
        let value: u64 = rng.r#gen();
        Ok(Value::int64(value as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_constant(
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        let mut rng = rand::thread_rng();
        let value: u32 = rng.r#gen();
        Ok(Value::int64(value as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_uniform(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query("randUniform requires 2 arguments"));
        }
        let min = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
        let max = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let uniform = Uniform::new(min, max);
        let value = uniform.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_normal(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "randNormal requires 2 arguments: mean, stddev",
            ));
        }
        let mean = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
        let stddev = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let normal = Normal::new(mean, stddev).map_err(|e| {
            Error::invalid_query(format!("Invalid normal distribution parameters: {}", e))
        })?;
        let value = normal.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_log_normal(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "randLogNormal requires 2 arguments: mean, stddev",
            ));
        }
        let mean = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
        let stddev = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let log_normal = LogNormal::new(mean, stddev).map_err(|e| {
            Error::invalid_query(format!("Invalid log-normal distribution parameters: {}", e))
        })?;
        let value = log_normal.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_exponential(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randExponential requires 1 argument: lambda",
            ));
        }
        let lambda = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let exp = Exp::new(lambda).map_err(|e| {
            Error::invalid_query(format!("Invalid exponential distribution parameter: {}", e))
        })?;
        let value = exp.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_chi_squared(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randChiSquared requires 1 argument: degrees of freedom",
            ));
        }
        let df = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let chi = ChiSquared::new(df).map_err(|e| {
            Error::invalid_query(format!("Invalid chi-squared distribution parameter: {}", e))
        })?;
        let value = chi.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_student_t(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randStudentT requires 1 argument: degrees of freedom",
            ));
        }
        let df = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let t = StudentT::new(df).map_err(|e| {
            Error::invalid_query(format!("Invalid Student's t distribution parameter: {}", e))
        })?;
        let value = t.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_fisher_f(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "randFisherF requires 2 arguments: df1, df2",
            ));
        }
        let df1 = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
        let df2 = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let f = FisherF::new(df1, df2).map_err(|e| {
            Error::invalid_query(format!("Invalid Fisher F distribution parameters: {}", e))
        })?;
        let value = f.sample(&mut rng);
        Ok(Value::float64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_bernoulli(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randBernoulli requires 1 argument: probability",
            ));
        }
        let p = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let bernoulli = Bernoulli::new(p).map_err(|e| {
            Error::invalid_query(format!("Invalid Bernoulli distribution parameter: {}", e))
        })?;
        let value = if bernoulli.sample(&mut rng) { 1 } else { 0 };
        Ok(Value::int64(value))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_binomial(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "randBinomial requires 2 arguments: n, p",
            ));
        }
        let n = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as u64;
        let p = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let binomial = Binomial::new(n, p).map_err(|e| {
            Error::invalid_query(format!("Invalid binomial distribution parameters: {}", e))
        })?;
        let value = binomial.sample(&mut rng);
        Ok(Value::int64(value as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_negative_binomial(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "randNegativeBinomial requires 2 arguments: n, p",
            ));
        }
        let n = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as u64;
        let p = Self::evaluate_expr(&args[1], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let mut count = 0u64;
        let mut successes = 0u64;
        while successes < n {
            if rng.r#gen::<f64>() < p {
                successes += 1;
            } else {
                count += 1;
            }
        }
        Ok(Value::int64(count as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_rand_poisson(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randPoisson requires 1 argument: lambda",
            ));
        }
        let lambda = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_f64()
            .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;

        let mut rng = rand::thread_rng();
        let poisson = Poisson::new(lambda).map_err(|e| {
            Error::invalid_query(format!("Invalid Poisson distribution parameter: {}", e))
        })?;
        let value: f64 = poisson.sample(&mut rng);
        Ok(Value::int64(value as i64))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_generate_uuid_v4(
        _args: &[Expr],
        _batch: &Table,
        _row_idx: usize,
    ) -> Result<Value> {
        let uuid = uuid::Uuid::new_v4();
        Ok(Value::string(uuid.to_string()))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_random_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randomString requires 1 argument: length",
            ));
        }
        let len = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as usize;

        let mut rng = rand::thread_rng();
        let bytes: Vec<u8> = (0..len).map(|_| rng.r#gen::<u8>()).collect();
        Ok(Value::bytes(bytes))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_random_fixed_string(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        Self::eval_random_string(args, batch, row_idx)
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_random_printable_ascii(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randomPrintableASCII requires 1 argument: length",
            ));
        }
        let len = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as usize;

        let mut rng = rand::thread_rng();
        let chars: String = (0..len)
            .map(|_| {
                let c = rng.gen_range(32..127) as u8;
                c as char
            })
            .collect();
        Ok(Value::string(chars))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_random_string_utf8(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "randomStringUTF8 requires 1 argument: length",
            ));
        }
        let len = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", "other"))? as usize;

        let mut rng = rand::thread_rng();
        let chars: String = (0..len).map(|_| rng.sample(Alphanumeric) as char).collect();
        Ok(Value::string(chars))
    }

    pub(in crate::query_executor::evaluator::physical_plan) fn eval_fake_data(
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "fakeData requires 1 argument: data type",
            ));
        }
        let data_type = Self::evaluate_expr(&args[0], batch, row_idx)?
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
            .to_lowercase();

        let mut rng = rand::thread_rng();
        let result = match data_type.as_str() {
            "name" => {
                let first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana"];
                let last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Miller"];
                format!(
                    "{} {}",
                    first_names[rng.gen_range(0..first_names.len())],
                    last_names[rng.gen_range(0..last_names.len())]
                )
            }
            "email" => {
                let chars: String = (0..8).map(|_| rng.sample(Alphanumeric) as char).collect();
                format!("{}@example.com", chars.to_lowercase())
            }
            "phone" => {
                format!(
                    "{}-{}-{}",
                    rng.gen_range(100..999),
                    rng.gen_range(100..999),
                    rng.gen_range(1000..9999)
                )
            }
            "address" => {
                format!(
                    "{} {} St",
                    rng.gen_range(1..9999),
                    ["Main", "Oak", "Maple", "Pine", "Cedar"][rng.gen_range(0..5)]
                )
            }
            _ => format!("fake_{}_{}", data_type, rng.gen_range(1..10000)),
        };

        Ok(Value::string(result))
    }
}
