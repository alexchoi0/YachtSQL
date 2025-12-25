use std::ffi::CString;

use ordered_float::OrderedFloat;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};
use yachtsql_common::types::Value;

pub fn evaluate_py_function(
    py_code: &str,
    param_names: &[String],
    args: &[Value],
) -> Result<Value, String> {
    Python::attach(|py| {
        let globals = PyDict::new(py);

        for (name, value) in param_names.iter().zip(args.iter()) {
            let py_value = value_to_py(py, value)?;
            globals
                .set_item(name.as_str(), py_value)
                .map_err(|e| format!("Failed to set parameter {}: {}", name, e))?;
        }

        let c_code = CString::new(py_code).map_err(|e| format!("Invalid Python code: {}", e))?;
        py.run(&c_code, Some(&globals), None)
            .map_err(|e| format!("Python execution error: {}", e))?;

        let func_name =
            extract_function_name(py_code).ok_or("Cannot find function name in Python code")?;

        let func = globals
            .get_item(&func_name)
            .map_err(|e| format!("Failed to get function: {}", e))?
            .ok_or_else(|| format!("Function '{}' not found", func_name))?;

        let py_args: Vec<Bound<'_, PyAny>> = param_names
            .iter()
            .filter_map(|name| globals.get_item(name.as_str()).ok().flatten())
            .collect();

        let result = func
            .call1((py_args.as_slice(),))
            .or_else(|_| {
                let tuple = pyo3::types::PyTuple::new(py, &py_args)
                    .map_err(|e| format!("Failed to create tuple: {}", e))?;
                func.call1(tuple)
                    .map_err(|e| format!("Python call error: {}", e))
            })
            .map_err(|e| format!("Python call error: {}", e))?;

        py_to_value(py, &result)
    })
}

fn extract_function_name(py_code: &str) -> Option<String> {
    for line in py_code.lines() {
        let line = line.trim();
        if line.starts_with("def ") {
            let rest = line.strip_prefix("def ")?;
            let name_end = rest.find('(')?;
            return Some(rest[..name_end].trim().to_string());
        }
    }
    None
}

fn value_to_py<'py>(py: Python<'py>, value: &Value) -> Result<Bound<'py, PyAny>, String> {
    match value {
        Value::Null => Ok(py.None().into_bound(py)),
        Value::Bool(b) => Ok(b
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .to_owned()
            .into_any()),
        Value::Int64(n) => Ok(n.into_pyobject(py).map_err(|e| e.to_string())?.into_any()),
        Value::Float64(f) => Ok(f.0.into_pyobject(py).map_err(|e| e.to_string())?.into_any()),
        Value::String(s) => Ok(s
            .as_str()
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .into_any()),
        Value::Bytes(b) => Ok(b
            .as_slice()
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .into_any()),
        Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for elem in arr {
                let py_elem = value_to_py(py, elem)?;
                py_list.append(py_elem).map_err(|e| e.to_string())?;
            }
            Ok(py_list.into_any())
        }
        Value::Numeric(n) => {
            let s = n.to_string();
            let decimal_mod = py.import("decimal").map_err(|e| e.to_string())?;
            let decimal_class = decimal_mod.getattr("Decimal").map_err(|e| e.to_string())?;
            decimal_class.call1((s,)).map_err(|e| e.to_string())
        }
        _ => Ok(format!("{:?}", value)
            .into_pyobject(py)
            .map_err(|e| e.to_string())?
            .into_any()),
    }
}

fn py_to_value(py: Python<'_>, obj: &Bound<'_, PyAny>) -> Result<Value, String> {
    if obj.is_none() {
        return Ok(Value::Null);
    }

    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }

    if let Ok(n) = obj.extract::<i64>() {
        return Ok(Value::Int64(n));
    }

    if let Ok(f) = obj.extract::<f64>() {
        return Ok(Value::Float64(OrderedFloat(f)));
    }

    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }

    if let Ok(bytes) = obj.extract::<Vec<u8>>() {
        return Ok(Value::Bytes(bytes));
    }

    if let Ok(list) = obj.extract::<Bound<'_, PyList>>() {
        let mut arr = Vec::new();
        for item in list.iter() {
            arr.push(py_to_value(py, &item)?);
        }
        return Ok(Value::Array(arr));
    }

    let repr = obj.repr().map_err(|e| e.to_string())?;
    let repr_str = repr.to_string();
    Ok(Value::String(repr_str))
}
