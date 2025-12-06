use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::network::{CidrAddr, InetAddr};
use yachtsql_core::types::{DataType, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "HOST".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HOST".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    Ok(Value::string(inet.addr.to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "FAMILY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "FAMILY".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    Ok(Value::int64(inet.family() as i64))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "MASKLEN".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MASKLEN".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::Int64,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    match inet.prefix_len {
                        Some(len) => Ok(Value::int64(len as i64)),
                        None => Ok(Value::int64(inet.max_prefix_len() as i64)),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "NETMASK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NETMASK".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::Inet,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    match inet.netmask() {
                        Some(mask) => {
                            let inet_mask = InetAddr::new(mask);
                            Ok(Value::inet(inet_mask))
                        }
                        None => {
                            let max_mask = if inet.is_ipv4() {
                                std::net::IpAddr::V4(std::net::Ipv4Addr::new(255, 255, 255, 255))
                            } else {
                                std::net::IpAddr::V6(std::net::Ipv6Addr::new(
                                    0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                                ))
                            };
                            Ok(Value::inet(InetAddr::new(max_mask)))
                        }
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "NETWORK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "NETWORK".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::Cidr,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    match inet.network() {
                        Some(cidr) => Ok(Value::cidr(cidr)),
                        None => {
                            let prefix = inet.max_prefix_len();
                            let cidr = CidrAddr {
                                network: inet.addr,
                                prefix_len: prefix,
                            };
                            Ok(Value::cidr(cidr))
                        }
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "BROADCAST".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "BROADCAST".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::Inet,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    match inet.broadcast() {
                        Some(broadcast) => {
                            let inet_broadcast = InetAddr::new(broadcast);
                            Ok(Value::inet(inet_broadcast))
                        }
                        None => Ok(args[0].clone()),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "HOSTMASK".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "HOSTMASK".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::Inet,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    let prefix = inet.prefix_len.unwrap_or(inet.max_prefix_len());
                    let hostmask = if inet.is_ipv4() {
                        let mask = if prefix >= 32 { 0u32 } else { !0u32 >> prefix };
                        std::net::IpAddr::V4(std::net::Ipv4Addr::from(mask.to_be_bytes()))
                    } else {
                        let mask = if prefix >= 128 {
                            0u128
                        } else {
                            !0u128 >> prefix
                        };
                        std::net::IpAddr::V6(std::net::Ipv6Addr::from(mask.to_be_bytes()))
                    };
                    Ok(Value::inet(InetAddr::new(hostmask)))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "INET_SAME_FAMILY".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INET_SAME_FAMILY".to_string(),
            arg_types: vec![DataType::Inet, DataType::Inet],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_inet(), args[1].as_inet()) {
                    (Some(a), Some(b)) => Ok(Value::bool_val(a.is_ipv4() == b.is_ipv4())),
                    _ => Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "ABBREV".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ABBREV".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    match inet.prefix_len {
                        Some(prefix) if prefix == inet.max_prefix_len() => {
                            Ok(Value::string(inet.addr.to_string()))
                        }
                        Some(_) => Ok(Value::string(inet.to_string())),
                        None => Ok(Value::string(inet.addr.to_string())),
                    }
                } else if let Some(cidr) = args[0].as_cidr() {
                    Ok(Value::string(cidr.to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET or CIDR".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}
