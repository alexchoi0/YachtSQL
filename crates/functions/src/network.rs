use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::LazyLock;

use publicsuffix::{List, Psl};
use url::Url;
use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

const IPV4_FAMILY: i64 = 4;
const IPV6_FAMILY: i64 = 6;
const IPV4_BYTES: usize = 4;
const IPV6_BYTES: usize = 16;
const IPV4_MAX_PREFIX: i64 = 32;
const IPV6_MAX_PREFIX: i64 = 128;
const IPV4_MAX_VALUE: i64 = 4_294_967_295;

static PSL: LazyLock<List> = LazyLock::new(List::new);

pub fn ip_from_string(addr_str: &str) -> Result<Value> {
    let ip = addr_str
        .parse::<IpAddr>()
        .map_err(|_| Error::invalid_query(format!("Invalid IP address: {}", addr_str)))?;

    let bytes = match ip {
        IpAddr::V4(ipv4) => ipv4.octets().to_vec(),
        IpAddr::V6(ipv6) => ipv6.octets().to_vec(),
    };

    Ok(Value::bytes(bytes))
}

pub fn safe_ip_from_string(addr_str: &str) -> Result<Value> {
    match addr_str.parse::<IpAddr>() {
        Ok(ip) => {
            let bytes = match ip {
                IpAddr::V4(ipv4) => ipv4.octets().to_vec(),
                IpAddr::V6(ipv6) => ipv6.octets().to_vec(),
            };
            Ok(Value::bytes(bytes))
        }
        Err(_) => Ok(Value::null()),
    }
}

pub fn ip_to_string(addr_bin: &[u8]) -> Result<Value> {
    match addr_bin.len() {
        IPV4_BYTES => {
            let ipv4 = Ipv4Addr::new(addr_bin[0], addr_bin[1], addr_bin[2], addr_bin[3]);
            Ok(Value::string(ipv4.to_string()))
        }
        IPV6_BYTES => {
            let mut octets = [0u8; IPV6_BYTES];
            octets.copy_from_slice(addr_bin);
            let ipv6 = Ipv6Addr::from(octets);
            Ok(Value::string(ipv6.to_string()))
        }
        n => Err(Error::invalid_query(format!(
            "Expected {} or {} bytes for IP address, got {}",
            IPV4_BYTES, IPV6_BYTES, n
        ))),
    }
}

pub fn ipv4_to_int64(addr_bin: &[u8]) -> Result<Value> {
    if addr_bin.len() != IPV4_BYTES {
        return Err(Error::invalid_query(format!(
            "Expected {}-byte IPv4 address, got {} bytes",
            IPV4_BYTES,
            addr_bin.len()
        )));
    }

    let ip_int = ((addr_bin[0] as i64) << 24)
        | ((addr_bin[1] as i64) << 16)
        | ((addr_bin[2] as i64) << 8)
        | (addr_bin[3] as i64);

    Ok(Value::int64(ip_int))
}

pub fn ipv4_from_int64(addr_int64: i64) -> Result<Value> {
    if !(0..=IPV4_MAX_VALUE).contains(&addr_int64) {
        return Err(Error::invalid_query(format!(
            "IPv4 address must be in range 0-{}, got {}",
            IPV4_MAX_VALUE, addr_int64
        )));
    }

    let octets = [
        ((addr_int64 >> 24) & 0xFF) as u8,
        ((addr_int64 >> 16) & 0xFF) as u8,
        ((addr_int64 >> 8) & 0xFF) as u8,
        (addr_int64 & 0xFF) as u8,
    ];

    Ok(Value::bytes(octets.to_vec()))
}

pub fn ip_net_mask(num_prefix_bits: i64, addr_family: i64) -> Result<Value> {
    match addr_family {
        IPV4_FAMILY => {
            if !(0..=IPV4_MAX_PREFIX).contains(&num_prefix_bits) {
                return Err(Error::invalid_query(format!(
                    "IPv4 prefix bits must be 0-{}, got {}",
                    IPV4_MAX_PREFIX, num_prefix_bits
                )));
            }
            let mask = if num_prefix_bits == 0 {
                0u32
            } else {
                !0u32 << (32 - num_prefix_bits)
            };
            let octets = mask.to_be_bytes();
            Ok(Value::bytes(octets.to_vec()))
        }
        IPV6_FAMILY => {
            if !(0..=IPV6_MAX_PREFIX).contains(&num_prefix_bits) {
                return Err(Error::invalid_query(format!(
                    "IPv6 prefix bits must be 0-{}, got {}",
                    IPV6_MAX_PREFIX, num_prefix_bits
                )));
            }
            let mut mask = [0u8; IPV6_BYTES];
            let full_bytes = (num_prefix_bits / 8) as usize;
            let remaining_bits = (num_prefix_bits % 8) as u8;

            for item in mask.iter_mut().take(full_bytes) {
                *item = 0xFF;
            }
            if full_bytes < IPV6_BYTES && remaining_bits > 0 {
                mask[full_bytes] = !0u8 << (8 - remaining_bits);
            }
            Ok(Value::bytes(mask.to_vec()))
        }
        _ => Err(Error::invalid_query(format!(
            "Address family must be {} (IPv4) or {} (IPv6), got {}",
            IPV4_FAMILY, IPV6_FAMILY, addr_family
        ))),
    }
}

pub fn ip_trunc(addr_bin: &[u8], num_prefix_bits: i64) -> Result<Value> {
    let addr_family = match addr_bin.len() {
        IPV4_BYTES => IPV4_FAMILY,
        IPV6_BYTES => IPV6_FAMILY,
        n => {
            return Err(Error::invalid_query(format!(
                "Expected {} or {} bytes for IP address, got {}",
                IPV4_BYTES, IPV6_BYTES, n
            )));
        }
    };

    let mask_value = ip_net_mask(num_prefix_bits, addr_family)?;
    let mask = mask_value
        .as_bytes()
        .ok_or_else(|| Error::invalid_query("Expected bytes from ip_net_mask".to_string()))?;

    let truncated: Vec<u8> = addr_bin
        .iter()
        .zip(mask.iter())
        .map(|(a, m)| a & m)
        .collect();

    Ok(Value::bytes(truncated))
}

pub fn host(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => Ok(Value::string(host.to_string())),
            None => Ok(Value::null()),
        },
        Err(_) => Ok(Value::null()),
    }
}

pub fn public_suffix(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => {
                let domain = host.trim_start_matches("www.");
                match PSL.suffix(domain.as_bytes()) {
                    Some(suffix) => {
                        let suffix_str = std::str::from_utf8(suffix.as_bytes())
                            .unwrap_or("")
                            .to_string();
                        Ok(Value::string(suffix_str))
                    }
                    None => Ok(Value::null()),
                }
            }
            None => Ok(Value::null()),
        },
        Err(_) => Ok(Value::null()),
    }
}

pub fn reg_domain(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => {
                let domain = host.trim_start_matches("www.");
                match PSL.domain(domain.as_bytes()) {
                    Some(reg_domain) => {
                        let reg_domain_str = std::str::from_utf8(reg_domain.as_bytes())
                            .unwrap_or("")
                            .to_string();
                        Ok(Value::string(reg_domain_str))
                    }
                    None => Ok(Value::null()),
                }
            }
            None => Ok(Value::null()),
        },
        Err(_) => Ok(Value::null()),
    }
}

pub fn protocol(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => Ok(Value::string(url.scheme().to_string())),
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn domain(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => Ok(Value::string(host.to_string())),
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn domain_without_www(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => {
                let domain = host.strip_prefix("www.").unwrap_or(host);
                Ok(Value::string(domain.to_string()))
            }
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn top_level_domain(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => {
                let parts: Vec<&str> = host.split('.').collect();
                if parts.is_empty() {
                    Ok(Value::string("".to_string()))
                } else {
                    Ok(Value::string(parts[parts.len() - 1].to_string()))
                }
            }
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn first_significant_subdomain(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => {
                let domain = host.strip_prefix("www.").unwrap_or(host);
                match PSL.domain(domain.as_bytes()) {
                    Some(reg_domain) => {
                        let reg_domain_str =
                            std::str::from_utf8(reg_domain.as_bytes()).unwrap_or("");
                        let parts: Vec<&str> = reg_domain_str.split('.').collect();
                        if parts.is_empty() {
                            Ok(Value::string("".to_string()))
                        } else {
                            Ok(Value::string(parts[0].to_string()))
                        }
                    }
                    None => {
                        let parts: Vec<&str> = domain.split('.').collect();
                        if parts.len() >= 2 {
                            Ok(Value::string(parts[parts.len() - 2].to_string()))
                        } else {
                            Ok(Value::string("".to_string()))
                        }
                    }
                }
            }
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn port(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.port() {
            Some(p) => Ok(Value::int64(p as i64)),
            None => Ok(Value::int64(0)),
        },
        Err(_) => Ok(Value::int64(0)),
    }
}

pub fn path(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => Ok(Value::string(url.path().to_string())),
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn path_full(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let mut result = url.path().to_string();
            if let Some(query) = url.query() {
                result.push('?');
                result.push_str(query);
            }
            Ok(Value::string(result))
        }
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn query_string(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.query() {
            Some(q) => Ok(Value::string(q.to_string())),
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn fragment(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.fragment() {
            Some(f) => Ok(Value::string(f.to_string())),
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn query_string_and_fragment(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let mut result = String::new();
            if let Some(query) = url.query() {
                result.push_str(query);
            }
            if let Some(frag) = url.fragment() {
                result.push('#');
                result.push_str(frag);
            }
            Ok(Value::string(result))
        }
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn extract_url_parameter(url_str: &str, param_name: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            for (key, value) in url.query_pairs() {
                if key == param_name {
                    return Ok(Value::string(value.to_string()));
                }
            }
            Ok(Value::string("".to_string()))
        }
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn extract_url_parameters(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let params: Vec<Value> = url
                .query_pairs()
                .map(|(k, v)| Value::string(format!("{}={}", k, v)))
                .collect();
            Ok(Value::array(params))
        }
        Err(_) => Ok(Value::array(vec![])),
    }
}

pub fn extract_url_parameter_names(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let names: Vec<Value> = url
                .query_pairs()
                .map(|(k, _)| Value::string(k.to_string()))
                .collect();
            Ok(Value::array(names))
        }
        Err(_) => Ok(Value::array(vec![])),
    }
}

pub fn url_hierarchy(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let scheme = url.scheme();
            let host = url.host_str().unwrap_or("");
            let port_str = url.port().map(|p| format!(":{}", p)).unwrap_or_default();
            let base = format!("{}://{}{}", scheme, host, port_str);

            let path = url.path();
            let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

            let mut hierarchy = Vec::new();
            hierarchy.push(Value::string(format!("{}/", base)));

            let mut current_path = String::new();
            for (i, segment) in segments.iter().enumerate() {
                current_path.push('/');
                current_path.push_str(segment);
                if i < segments.len() - 1 {
                    hierarchy.push(Value::string(format!("{}{}/", base, current_path)));
                } else {
                    hierarchy.push(Value::string(format!("{}{}", base, current_path)));
                }
            }

            Ok(Value::array(hierarchy))
        }
        Err(_) => Ok(Value::array(vec![])),
    }
}

pub fn url_path_hierarchy(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let path = url.path();
            let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

            let mut hierarchy = Vec::new();
            let mut current_path = String::new();
            for (i, segment) in segments.iter().enumerate() {
                current_path.push('/');
                current_path.push_str(segment);
                if i < segments.len() - 1 {
                    hierarchy.push(Value::string(format!("{}/", current_path)));
                } else {
                    hierarchy.push(Value::string(current_path.clone()));
                }
            }

            Ok(Value::array(hierarchy))
        }
        Err(_) => Ok(Value::array(vec![])),
    }
}

pub fn decode_url_component(encoded: &str) -> Result<Value> {
    match urlencoding::decode(encoded) {
        Ok(decoded) => Ok(Value::string(decoded.to_string())),
        Err(_) => Ok(Value::string(encoded.to_string())),
    }
}

pub fn encode_url_component(s: &str) -> Result<Value> {
    Ok(Value::string(urlencoding::encode(s).to_string()))
}

pub fn encode_url_form_component(s: &str) -> Result<Value> {
    let encoded = urlencoding::encode(s).to_string().replace("%20", "+");
    Ok(Value::string(encoded))
}

pub fn decode_url_form_component(encoded: &str) -> Result<Value> {
    let normalized = encoded.replace('+', "%20");
    match urlencoding::decode(&normalized) {
        Ok(decoded) => Ok(Value::string(decoded.to_string())),
        Err(_) => Ok(Value::string(encoded.to_string())),
    }
}

pub fn netloc(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => {
            let mut result = String::new();
            let username = url.username();
            if !username.is_empty() {
                result.push_str(username);
                if let Some(password) = url.password() {
                    result.push(':');
                    result.push_str(password);
                }
                result.push('@');
            }
            if let Some(host) = url.host_str() {
                result.push_str(host);
            }
            if let Some(port) = url.port() {
                result.push(':');
                result.push_str(&port.to_string());
            }
            Ok(Value::string(result))
        }
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn cut_to_first_significant_subdomain(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(url) => match url.host_str() {
            Some(host) => {
                let domain = host.strip_prefix("www.").unwrap_or(host);
                match PSL.domain(domain.as_bytes()) {
                    Some(reg_domain) => {
                        let reg_domain_str =
                            std::str::from_utf8(reg_domain.as_bytes()).unwrap_or("");
                        Ok(Value::string(reg_domain_str.to_string()))
                    }
                    None => Ok(Value::string(domain.to_string())),
                }
            }
            None => Ok(Value::string("".to_string())),
        },
        Err(_) => Ok(Value::string("".to_string())),
    }
}

pub fn cut_www(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(mut url) => {
            let new_host = url
                .host_str()
                .and_then(|h| h.strip_prefix("www."))
                .map(|s| s.to_string());
            if let Some(h) = new_host {
                let _ = url.set_host(Some(&h));
            }
            Ok(Value::string(url.to_string()))
        }
        Err(_) => Ok(Value::string(url_str.to_string())),
    }
}

pub fn cut_query_string(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(mut url) => {
            url.set_query(None);
            Ok(Value::string(url.to_string()))
        }
        Err(_) => Ok(Value::string(url_str.to_string())),
    }
}

pub fn cut_fragment(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(mut url) => {
            url.set_fragment(None);
            Ok(Value::string(url.to_string()))
        }
        Err(_) => Ok(Value::string(url_str.to_string())),
    }
}

pub fn cut_query_string_and_fragment(url_str: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(mut url) => {
            url.set_query(None);
            url.set_fragment(None);
            Ok(Value::string(url.to_string()))
        }
        Err(_) => Ok(Value::string(url_str.to_string())),
    }
}

pub fn cut_url_parameter(url_str: &str, param_name: &str) -> Result<Value> {
    match Url::parse(url_str) {
        Ok(mut url) => {
            let pairs: Vec<(String, String)> = url
                .query_pairs()
                .filter(|(k, _)| k != param_name)
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect();

            if pairs.is_empty() {
                url.set_query(None);
            } else {
                let query: String = pairs
                    .iter()
                    .map(|(k, v)| format!("{}={}", k, v))
                    .collect::<Vec<_>>()
                    .join("&");
                url.set_query(Some(&query));
            }
            Ok(Value::string(url.to_string()))
        }
        Err(_) => Ok(Value::string(url_str.to_string())),
    }
}

pub fn ipv4_num_to_string(num: i64) -> Result<Value> {
    if num < 0 || num > IPV4_MAX_VALUE {
        return Err(Error::invalid_query(format!(
            "IPv4 number must be 0-{}, got {}",
            IPV4_MAX_VALUE, num
        )));
    }
    let ip = Ipv4Addr::from((num as u32).to_be_bytes());
    Ok(Value::string(ip.to_string()))
}

pub fn ipv4_string_to_num(addr_str: &str) -> Result<Value> {
    let ip: Ipv4Addr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IPv4 address: {}", addr_str)))?;
    let octets = ip.octets();
    let num = ((octets[0] as i64) << 24)
        | ((octets[1] as i64) << 16)
        | ((octets[2] as i64) << 8)
        | (octets[3] as i64);
    Ok(Value::int64(num))
}

pub fn ipv4_num_to_string_class_c(num: i64) -> Result<Value> {
    if num < 0 || num > IPV4_MAX_VALUE {
        return Err(Error::invalid_query(format!(
            "IPv4 number must be 0-{}, got {}",
            IPV4_MAX_VALUE, num
        )));
    }
    let ip = Ipv4Addr::from((num as u32).to_be_bytes());
    let octets = ip.octets();
    Ok(Value::string(format!(
        "{}.{}.{}.xxx",
        octets[0], octets[1], octets[2]
    )))
}

pub fn ipv4_to_ipv6(num: i64) -> Result<Value> {
    if num < 0 || num > IPV4_MAX_VALUE {
        return Err(Error::invalid_query(format!(
            "IPv4 number must be 0-{}, got {}",
            IPV4_MAX_VALUE, num
        )));
    }
    let ipv4 = Ipv4Addr::from((num as u32).to_be_bytes());
    let ipv6 = ipv4.to_ipv6_mapped();
    Ok(Value::string(ipv6.to_string()))
}

pub fn ipv6_num_to_string(bytes: &[u8]) -> Result<Value> {
    if bytes.len() != 16 {
        return Err(Error::invalid_query(format!(
            "IPv6 address must be 16 bytes, got {}",
            bytes.len()
        )));
    }
    let mut octets = [0u8; 16];
    octets.copy_from_slice(bytes);
    let ip = Ipv6Addr::from(octets);
    Ok(Value::string(ip.to_string()))
}

pub fn ipv6_string_to_num(addr_str: &str) -> Result<Value> {
    let ip: Ipv6Addr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IPv6 address: {}", addr_str)))?;
    Ok(Value::bytes(ip.octets().to_vec()))
}

pub fn to_ipv4(addr_str: &str) -> Result<Value> {
    let ip: Ipv4Addr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IPv4 address: {}", addr_str)))?;
    Ok(Value::string(ip.to_string()))
}

pub fn to_ipv6(addr_str: &str) -> Result<Value> {
    let ip: Ipv6Addr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IPv6 address: {}", addr_str)))?;
    Ok(Value::string(ip.to_string()))
}

pub fn to_ipv4_or_null(addr_str: &str) -> Result<Value> {
    match addr_str.parse::<Ipv4Addr>() {
        Ok(ip) => Ok(Value::string(ip.to_string())),
        Err(_) => Ok(Value::null()),
    }
}

pub fn to_ipv6_or_null(addr_str: &str) -> Result<Value> {
    match addr_str.parse::<Ipv6Addr>() {
        Ok(ip) => Ok(Value::string(ip.to_string())),
        Err(_) => Ok(Value::null()),
    }
}

pub fn is_ipv4_string(addr_str: &str) -> Result<Value> {
    Ok(Value::bool_val(addr_str.parse::<Ipv4Addr>().is_ok()))
}

pub fn is_ipv6_string(addr_str: &str) -> Result<Value> {
    Ok(Value::bool_val(addr_str.parse::<Ipv6Addr>().is_ok()))
}

pub fn is_ip_address_in_range(addr_str: &str, cidr: &str) -> Result<Value> {
    use ipnetwork::IpNetwork;
    let network: IpNetwork = cidr
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid CIDR: {}", cidr)))?;
    let ip: IpAddr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IP address: {}", addr_str)))?;
    Ok(Value::bool_val(network.contains(ip)))
}

pub fn ipv4_cidr_to_range(addr_str: &str, prefix: i64) -> Result<Value> {
    use ipnetwork::Ipv4Network;
    let ip: Ipv4Addr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IPv4 address: {}", addr_str)))?;
    let network = Ipv4Network::new(ip, prefix as u8)
        .map_err(|_| Error::invalid_query(format!("Invalid prefix: {}", prefix)))?;
    let start = network.network().to_string();
    let end = network.broadcast().to_string();
    Ok(Value::string(format!("({}, {})", start, end)))
}

pub fn ipv6_cidr_to_range(addr_str: &str, prefix: i64) -> Result<Value> {
    use ipnetwork::Ipv6Network;
    let ip: Ipv6Addr = addr_str
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid IPv6 address: {}", addr_str)))?;
    let network = Ipv6Network::new(ip, prefix as u8)
        .map_err(|_| Error::invalid_query(format!("Invalid prefix: {}", prefix)))?;
    let start = network.network().to_string();
    let end = network.broadcast().to_string();
    Ok(Value::string(format!("({}, {})", start, end)))
}

pub fn mac_num_to_string(num: i64) -> Result<Value> {
    let bytes = [
        ((num >> 40) & 0xFF) as u8,
        ((num >> 32) & 0xFF) as u8,
        ((num >> 24) & 0xFF) as u8,
        ((num >> 16) & 0xFF) as u8,
        ((num >> 8) & 0xFF) as u8,
        (num & 0xFF) as u8,
    ];
    Ok(Value::string(format!(
        "{:02X}:{:02X}:{:02X}:{:02X}:{:02X}:{:02X}",
        bytes[0], bytes[1], bytes[2], bytes[3], bytes[4], bytes[5]
    )))
}

pub fn mac_string_to_num(addr_str: &str) -> Result<Value> {
    let parts: Vec<&str> = addr_str.split(':').collect();
    if parts.len() != 6 {
        return Err(Error::invalid_query(format!(
            "Invalid MAC address: {}",
            addr_str
        )));
    }
    let mut num: i64 = 0;
    for (i, part) in parts.iter().enumerate() {
        let byte = u8::from_str_radix(part, 16)
            .map_err(|_| Error::invalid_query(format!("Invalid MAC address: {}", addr_str)))?;
        num |= (byte as i64) << (40 - i * 8);
    }
    Ok(Value::int64(num))
}

pub fn mac_string_to_oui(addr_str: &str) -> Result<Value> {
    let parts: Vec<&str> = addr_str.split(':').collect();
    if parts.len() != 6 {
        return Err(Error::invalid_query(format!(
            "Invalid MAC address: {}",
            addr_str
        )));
    }
    let mut num: i64 = 0;
    for (i, part) in parts.iter().take(3).enumerate() {
        let byte = u8::from_str_radix(part, 16)
            .map_err(|_| Error::invalid_query(format!("Invalid MAC address: {}", addr_str)))?;
        num |= (byte as i64) << (16 - i * 8);
    }
    Ok(Value::int64(num))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ip_from_string_ipv4() {
        let result = ip_from_string("192.168.1.1").unwrap();
        let bytes = result.as_bytes().expect("Expected bytes");
        assert_eq!(bytes, &[192, 168, 1, 1]);
    }

    #[test]
    fn test_ip_from_string_ipv6() {
        let result = ip_from_string("::1").unwrap();
        let bytes = result.as_bytes().expect("Expected bytes");
        assert_eq!(bytes.len(), 16);
        assert_eq!(bytes[15], 1);
    }

    #[test]
    fn test_safe_ip_from_string_invalid() {
        let result = safe_ip_from_string("not-an-ip").unwrap();
        assert_eq!(result, Value::null());
    }

    #[test]
    fn test_ipv4_to_int64() {
        let result = ipv4_to_int64(&[192, 168, 1, 1]).unwrap();
        assert_eq!(result, Value::int64(3232235777));
    }

    #[test]
    fn test_ipv4_from_int64() {
        let result = ipv4_from_int64(3232235777).unwrap();
        let bytes = result.as_bytes().expect("Expected bytes");
        assert_eq!(bytes, &[192, 168, 1, 1]);
    }

    #[test]
    fn test_ip_net_mask_ipv4() {
        let result = ip_net_mask(24, 4).unwrap();
        let bytes = result.as_bytes().expect("Expected bytes");
        assert_eq!(bytes, &[255, 255, 255, 0]);
    }

    #[test]
    fn test_ip_trunc() {
        let result = ip_trunc(&[192, 168, 1, 100], 24).unwrap();
        let bytes = result.as_bytes().expect("Expected bytes");
        assert_eq!(bytes, &[192, 168, 1, 0]);
    }

    #[test]
    fn test_host() {
        let result = host("https://www.google.com/search?q=bigquery").unwrap();
        let s = result.as_str().expect("Expected string");
        assert_eq!(s, "www.google.com");
    }
}
