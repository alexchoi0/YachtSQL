use std::rc::Rc;

use yachtsql_core::error::Error;
use yachtsql_core::types::network::{CidrAddr, InetAddr};
use yachtsql_core::types::{DataType, IPv4Addr, IPv6Addr, Value};

use super::FunctionRegistry;
use crate::scalar::ScalarFunctionImpl;

pub(super) fn register(registry: &mut FunctionRegistry) {
    registry.register_scalar(
        "TOIPV4".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOIPV4".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::IPv4,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    match IPv4Addr::parse(s) {
                        Some(ipv4) => Ok(Value::ipv4(ipv4)),
                        None => Err(Error::InvalidOperation(format!(
                            "Cannot parse '{}' as IPv4 address",
                            s
                        ))),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TOIPV6".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOIPV6".to_string(),
            arg_types: vec![DataType::String],
            return_type: DataType::IPv6,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    match IPv6Addr::parse(s) {
                        Some(ipv6) => Ok(Value::ipv6(ipv6)),
                        None => Err(Error::InvalidOperation(format!(
                            "Cannot parse '{}' as IPv6 address",
                            s
                        ))),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "IPV4TOIPV6".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "IPV4TOIPV6".to_string(),
            arg_types: vec![DataType::IPv4],
            return_type: DataType::IPv6,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ipv4) = args[0].as_ipv4() {
                    Ok(Value::ipv6(ipv4.to_ipv6()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "IPv4".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "ISIPADDRESSINRANGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "ISIPADDRESSINRANGE".to_string(),
            arg_types: vec![DataType::String, DataType::String],
            return_type: DataType::Bool,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let (Some(ip_str), Some(cidr_str)) = (args[0].as_str(), args[1].as_str()) else {
                    return Err(Error::TypeMismatch {
                        expected: "STRING, STRING".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    });
                };

                let parts: Vec<&str> = cidr_str.split('/').collect();
                if parts.len() != 2 {
                    return Err(Error::InvalidOperation(format!(
                        "Invalid CIDR notation: {}",
                        cidr_str
                    )));
                }
                let network_str = parts[0];
                let prefix_len: u8 = parts[1].parse().map_err(|_| {
                    Error::InvalidOperation(format!("Invalid prefix length in CIDR: {}", cidr_str))
                })?;

                if let Some(ipv4) = IPv4Addr::parse(ip_str) {
                    if let Some(network) = IPv4Addr::parse(network_str) {
                        if prefix_len > 32 {
                            return Err(Error::InvalidOperation(format!(
                                "Invalid IPv4 prefix length: {}",
                                prefix_len
                            )));
                        }
                        let mask = if prefix_len == 0 {
                            0u32
                        } else {
                            !0u32 << (32 - prefix_len)
                        };
                        let in_range = (ipv4.0 & mask) == (network.0 & mask);
                        return Ok(Value::bool_val(in_range));
                    }
                }

                if let Some(ipv6) = IPv6Addr::parse(ip_str) {
                    if let Some(network) = IPv6Addr::parse(network_str) {
                        if prefix_len > 128 {
                            return Err(Error::InvalidOperation(format!(
                                "Invalid IPv6 prefix length: {}",
                                prefix_len
                            )));
                        }
                        let mask = if prefix_len == 0 {
                            0u128
                        } else {
                            !0u128 << (128 - prefix_len)
                        };
                        let in_range = (ipv6.0 & mask) == (network.0 & mask);
                        return Ok(Value::bool_val(in_range));
                    }
                }

                Err(Error::InvalidOperation(format!(
                    "Cannot parse '{}' or '{}' as IP address",
                    ip_str, network_str
                )))
            },
        }),
    );

    registry.register_scalar(
        "TOSTRING".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TOSTRING".to_string(),
            arg_types: vec![DataType::IPv4],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ipv4) = args[0].as_ipv4() {
                    Ok(Value::string(ipv4.to_string()))
                } else if let Some(ipv6) = args[0].as_ipv6() {
                    Ok(Value::string(ipv6.to_string()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "IPv4 or IPv6".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

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
                            let inet_broadcast = InetAddr {
                                addr: broadcast,
                                prefix_len: inet.prefix_len,
                            };
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

    registry.register_scalar(
        "SET_MASKLEN".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "SET_MASKLEN".to_string(),
            arg_types: vec![DataType::Inet, DataType::Int64],
            return_type: DataType::Inet,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                if let (Some(inet), Some(new_len)) = (args[0].as_inet(), args[1].as_i64()) {
                    let new_prefix = new_len as u8;
                    let new_inet = InetAddr {
                        addr: inet.addr,
                        prefix_len: Some(new_prefix),
                    };
                    Ok(Value::inet(new_inet))
                } else if let (Some(cidr), Some(new_len)) = (args[0].as_cidr(), args[1].as_i64()) {
                    let new_prefix = new_len as u8;
                    let truncated_network = match cidr.network {
                        std::net::IpAddr::V4(ip) => {
                            let bits = u32::from_be_bytes(ip.octets());
                            let mask = if new_prefix == 0 {
                                0u32
                            } else if new_prefix >= 32 {
                                !0u32
                            } else {
                                !0u32 << (32 - new_prefix)
                            };
                            std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                                (bits & mask).to_be_bytes(),
                            ))
                        }
                        std::net::IpAddr::V6(ip) => {
                            let bits = u128::from_be_bytes(ip.octets());
                            let mask = if new_prefix == 0 {
                                0u128
                            } else if new_prefix >= 128 {
                                !0u128
                            } else {
                                !0u128 << (128 - new_prefix)
                            };
                            std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                                (bits & mask).to_be_bytes(),
                            ))
                        }
                    };
                    match CidrAddr::new(truncated_network, new_prefix) {
                        Ok(new_cidr) => Ok(Value::cidr(new_cidr)),
                        Err(e) => Err(Error::InvalidOperation(format!(
                            "Invalid prefix length: {}",
                            e
                        ))),
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "INET/CIDR and INT64".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "TEXT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TEXT".to_string(),
            arg_types: vec![DataType::Inet],
            return_type: DataType::String,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = args[0].as_inet() {
                    Ok(Value::string(inet.to_string()))
                } else if let Some(cidr) = args[0].as_cidr() {
                    Ok(Value::string(cidr.to_string()))
                } else {
                    Ok(Value::string(format!("{:?}", args[0])))
                }
            },
        }),
    );

    registry.register_scalar(
        "INET_MERGE".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "INET_MERGE".to_string(),
            arg_types: vec![DataType::Inet, DataType::Inet],
            return_type: DataType::Cidr,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                match (args[0].as_inet(), args[1].as_inet()) {
                    (Some(a), Some(b)) => {
                        if a.is_ipv4() != b.is_ipv4() {
                            return Err(Error::InvalidOperation(
                                "Cannot merge IPv4 and IPv6 addresses".to_string(),
                            ));
                        }

                        if a.is_ipv4() {
                            let a_bytes: [u8; 4] = match a.addr {
                                std::net::IpAddr::V4(ip) => ip.octets(),
                                _ => unreachable!(),
                            };
                            let b_bytes: [u8; 4] = match b.addr {
                                std::net::IpAddr::V4(ip) => ip.octets(),
                                _ => unreachable!(),
                            };

                            let a_u32 = u32::from_be_bytes(a_bytes);
                            let b_u32 = u32::from_be_bytes(b_bytes);
                            let xor = a_u32 ^ b_u32;
                            let common_bits = if xor == 0 {
                                32
                            } else {
                                xor.leading_zeros() as u8
                            };

                            let mask = if common_bits == 0 {
                                0u32
                            } else {
                                !(!0u32 >> common_bits)
                            };
                            let network = a_u32 & mask;
                            let network_ip = std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                                network.to_be_bytes(),
                            ));

                            Ok(Value::cidr(CidrAddr::new(network_ip, common_bits).unwrap()))
                        } else {
                            let a_bytes: [u8; 16] = match a.addr {
                                std::net::IpAddr::V6(ip) => ip.octets(),
                                _ => unreachable!(),
                            };
                            let b_bytes: [u8; 16] = match b.addr {
                                std::net::IpAddr::V6(ip) => ip.octets(),
                                _ => unreachable!(),
                            };

                            let a_u128 = u128::from_be_bytes(a_bytes);
                            let b_u128 = u128::from_be_bytes(b_bytes);
                            let xor = a_u128 ^ b_u128;
                            let common_bits = if xor == 0 {
                                128
                            } else {
                                xor.leading_zeros() as u8
                            };

                            let mask = if common_bits == 0 {
                                0u128
                            } else {
                                !(!0u128 >> common_bits)
                            };
                            let network = a_u128 & mask;
                            let network_ip = std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                                network.to_be_bytes(),
                            ));

                            Ok(Value::cidr(CidrAddr::new(network_ip, common_bits).unwrap()))
                        }
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "INET".to_string(),
                        actual: format!("{:?}, {:?}", args[0].data_type(), args[1].data_type()),
                    }),
                }
            },
        }),
    );

    registry.register_scalar(
        "TRUNC".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "TRUNC".to_string(),
            arg_types: vec![DataType::MacAddr],
            return_type: DataType::MacAddr,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(mac) = args[0].as_macaddr() {
                    Ok(Value::macaddr(mac.trunc()))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "MACADDR".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );

    registry.register_scalar(
        "MACADDR8_SET7BIT".to_string(),
        Rc::new(ScalarFunctionImpl {
            name: "MACADDR8_SET7BIT".to_string(),
            arg_types: vec![DataType::MacAddr8],
            return_type: DataType::MacAddr8,
            variadic: false,
            evaluator: |args| {
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(mac) = args[0].as_macaddr8() {
                    let mut octets = mac.octets;
                    octets[0] |= 0x02;
                    let new_mac = yachtsql_core::types::MacAddress {
                        octets,
                        is_eui64: mac.is_eui64,
                    };
                    Ok(Value::macaddr8(new_mac))
                } else {
                    Err(Error::TypeMismatch {
                        expected: "MACADDR8".to_string(),
                        actual: args[0].data_type().to_string(),
                    })
                }
            },
        }),
    );
}
