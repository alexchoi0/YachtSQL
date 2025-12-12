use std::fmt;
use std::net::IpAddr;
use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct InetAddr {
    pub addr: IpAddr,

    pub prefix_len: Option<u8>,
}

impl InetAddr {
    pub fn new(addr: IpAddr) -> Self {
        Self {
            addr,
            prefix_len: None,
        }
    }

    pub fn with_prefix(addr: IpAddr, prefix_len: u8) -> Result<Self, String> {
        let max_prefix = if addr.is_ipv4() { 32 } else { 128 };
        if prefix_len > max_prefix {
            return Err(format!(
                "Invalid prefix length {} for {} address (max {})",
                prefix_len,
                if addr.is_ipv4() { "IPv4" } else { "IPv6" },
                max_prefix
            ));
        }
        Ok(Self {
            addr,
            prefix_len: Some(prefix_len),
        })
    }

    pub fn is_ipv4(&self) -> bool {
        self.addr.is_ipv4()
    }

    pub fn is_ipv6(&self) -> bool {
        self.addr.is_ipv6()
    }

    pub fn max_prefix_len(&self) -> u8 {
        if self.addr.is_ipv4() { 32 } else { 128 }
    }

    pub fn family(&self) -> i32 {
        if self.addr.is_ipv4() { 4 } else { 6 }
    }

    pub fn netmask(&self) -> Option<IpAddr> {
        self.prefix_len.map(|prefix| match self.addr {
            IpAddr::V4(_) => {
                let mask = if prefix == 0 {
                    0u32
                } else {
                    !0u32 << (32 - prefix)
                };
                IpAddr::V4(std::net::Ipv4Addr::from(mask.to_be_bytes()))
            }
            IpAddr::V6(_) => {
                let mask = if prefix == 0 {
                    0u128
                } else {
                    !0u128 << (128 - prefix)
                };
                IpAddr::V6(std::net::Ipv6Addr::from(mask.to_be_bytes()))
            }
        })
    }

    pub fn broadcast(&self) -> Option<IpAddr> {
        match (self.addr, self.prefix_len) {
            (IpAddr::V4(ipv4), Some(prefix)) => {
                if prefix >= 32 {
                    return Some(IpAddr::V4(ipv4));
                }
                let mask = !0u32 >> prefix;
                let octets = ipv4.octets();
                let addr_bits = u32::from_be_bytes(octets);
                let broadcast_bits = addr_bits | mask;
                Some(IpAddr::V4(std::net::Ipv4Addr::from(
                    broadcast_bits.to_be_bytes(),
                )))
            }
            _ => None,
        }
    }

    pub fn network(&self) -> Option<CidrAddr> {
        self.prefix_len.map(|prefix| {
            let network_addr = match self.addr {
                IpAddr::V4(ipv4) => {
                    let mask = if prefix == 0 {
                        0u32
                    } else {
                        !0u32 << (32 - prefix)
                    };
                    let octets = ipv4.octets();
                    let addr_bits = u32::from_be_bytes(octets);
                    let network_bits = addr_bits & mask;
                    IpAddr::V4(std::net::Ipv4Addr::from(network_bits.to_be_bytes()))
                }
                IpAddr::V6(ipv6) => {
                    let mask = if prefix == 0 {
                        0u128
                    } else {
                        !0u128 << (128 - prefix)
                    };
                    let octets = ipv6.octets();
                    let addr_bits = u128::from_be_bytes(octets);
                    let network_bits = addr_bits & mask;
                    IpAddr::V6(std::net::Ipv6Addr::from(network_bits.to_be_bytes()))
                }
            };

            CidrAddr {
                network: network_addr,
                prefix_len: prefix,
            }
        })
    }
}

impl FromStr for InetAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        if let Some(slash_pos) = s.rfind('/') {
            let addr_part = &s[..slash_pos];
            let prefix_part = &s[slash_pos + 1..];

            let addr: IpAddr = addr_part
                .parse()
                .map_err(|e| format!("Invalid IP address '{}': {}", addr_part, e))?;

            let prefix_len: u8 = prefix_part
                .parse()
                .map_err(|e| format!("Invalid prefix length '{}': {}", prefix_part, e))?;

            Self::with_prefix(addr, prefix_len)
        } else {
            let addr: IpAddr = s
                .parse()
                .map_err(|e| format!("Invalid IP address '{}': {}", s, e))?;
            Ok(Self::new(addr))
        }
    }
}

impl fmt::Display for InetAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.prefix_len {
            Some(prefix) => write!(f, "{}/{}", self.addr, prefix),
            None => write!(f, "{}", self.addr),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CidrAddr {
    pub network: IpAddr,

    pub prefix_len: u8,
}

impl CidrAddr {
    pub fn new(network: IpAddr, prefix_len: u8) -> Result<Self, String> {
        let max_prefix = if network.is_ipv4() { 32 } else { 128 };
        if prefix_len > max_prefix {
            return Err(format!(
                "Invalid prefix length {} for {} address (max {})",
                prefix_len,
                if network.is_ipv4() { "IPv4" } else { "IPv6" },
                max_prefix
            ));
        }

        if !Self::has_zero_host_bits(&network, prefix_len) {
            return Err(format!(
                "Invalid CIDR: host bits are set in {}/{}",
                network, prefix_len
            ));
        }

        Ok(Self {
            network,
            prefix_len,
        })
    }

    fn has_zero_host_bits(addr: &IpAddr, prefix_len: u8) -> bool {
        match addr {
            IpAddr::V4(ipv4) => {
                if prefix_len >= 32 {
                    return true;
                }
                let mask = !0u32 >> prefix_len;
                let octets = ipv4.octets();
                let addr_bits = u32::from_be_bytes(octets);
                (addr_bits & mask) == 0
            }
            IpAddr::V6(ipv6) => {
                if prefix_len >= 128 {
                    return true;
                }
                let mask = !0u128 >> prefix_len;
                let octets = ipv6.octets();
                let addr_bits = u128::from_be_bytes(octets);
                (addr_bits & mask) == 0
            }
        }
    }

    pub fn is_ipv4(&self) -> bool {
        self.network.is_ipv4()
    }

    pub fn is_ipv6(&self) -> bool {
        self.network.is_ipv6()
    }

    pub fn broadcast(&self) -> Option<IpAddr> {
        match self.network {
            IpAddr::V4(ipv4) => {
                if self.prefix_len >= 32 {
                    return Some(IpAddr::V4(ipv4));
                }
                let mask = !0u32 >> self.prefix_len;
                let octets = ipv4.octets();
                let addr_bits = u32::from_be_bytes(octets);
                let broadcast_bits = addr_bits | mask;
                Some(IpAddr::V4(std::net::Ipv4Addr::from(
                    broadcast_bits.to_be_bytes(),
                )))
            }
            IpAddr::V6(_) => None,
        }
    }

    pub fn netmask(&self) -> IpAddr {
        match self.network {
            IpAddr::V4(_) => {
                let mask = if self.prefix_len == 0 {
                    0u32
                } else {
                    !0u32 << (32 - self.prefix_len)
                };
                IpAddr::V4(std::net::Ipv4Addr::from(mask.to_be_bytes()))
            }
            IpAddr::V6(_) => {
                let mask = if self.prefix_len == 0 {
                    0u128
                } else {
                    !0u128 << (128 - self.prefix_len)
                };
                IpAddr::V6(std::net::Ipv6Addr::from(mask.to_be_bytes()))
            }
        }
    }

    pub fn contains(&self, addr: &IpAddr) -> bool {
        match (&self.network, addr) {
            (IpAddr::V4(net), IpAddr::V4(ip)) => {
                let net_bits = u32::from_be_bytes(net.octets());
                let ip_bits = u32::from_be_bytes(ip.octets());
                let mask = if self.prefix_len == 0 {
                    0u32
                } else {
                    !0u32 << (32 - self.prefix_len)
                };
                (net_bits & mask) == (ip_bits & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(ip)) => {
                let net_bits = u128::from_be_bytes(net.octets());
                let ip_bits = u128::from_be_bytes(ip.octets());
                let mask = if self.prefix_len == 0 {
                    0u128
                } else {
                    !0u128 << (128 - self.prefix_len)
                };
                (net_bits & mask) == (ip_bits & mask)
            }
            _ => false,
        }
    }
}

impl FromStr for CidrAddr {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();

        let slash_pos = s.rfind('/').ok_or_else(|| {
            format!(
                "Invalid CIDR '{}': missing prefix length (e.g., '192.168.0.0/24')",
                s
            )
        })?;

        let addr_part = &s[..slash_pos];
        let prefix_part = &s[slash_pos + 1..];

        let addr: IpAddr = addr_part
            .parse()
            .map_err(|e| format!("Invalid IP address '{}': {}", addr_part, e))?;

        let prefix_len: u8 = prefix_part
            .parse()
            .map_err(|e| format!("Invalid prefix length '{}': {}", prefix_part, e))?;

        Self::new(addr, prefix_len)
    }
}

impl fmt::Display for CidrAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.network, self.prefix_len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_inet_ipv4_without_prefix() {
        let inet = InetAddr::from_str("192.168.1.1").unwrap();
        assert!(inet.is_ipv4());
        assert!(!inet.is_ipv6());
        assert_eq!(inet.prefix_len, None);
        assert_eq!(inet.to_string(), "192.168.1.1");
    }

    #[test]
    fn test_inet_ipv4_with_prefix() {
        let inet = InetAddr::from_str("192.168.1.1/24").unwrap();
        assert!(inet.is_ipv4());
        assert_eq!(inet.prefix_len, Some(24));
        assert_eq!(inet.to_string(), "192.168.1.1/24");
    }

    #[test]
    fn test_inet_ipv6() {
        let inet = InetAddr::from_str("::1").unwrap();
        assert!(inet.is_ipv6());
        assert_eq!(inet.prefix_len, None);

        let inet = InetAddr::from_str("2001:db8::1/64").unwrap();
        assert!(inet.is_ipv6());
        assert_eq!(inet.prefix_len, Some(64));
    }

    #[test]
    fn test_inet_invalid_prefix() {
        assert!(InetAddr::from_str("192.168.1.1/33").is_err());
        assert!(InetAddr::from_str("::1/129").is_err());
    }

    #[test]
    fn test_cidr_valid() {
        let cidr = CidrAddr::from_str("192.168.0.0/16").unwrap();
        assert!(cidr.is_ipv4());
        assert_eq!(cidr.prefix_len, 16);
        assert_eq!(cidr.to_string(), "192.168.0.0/16");

        let cidr = CidrAddr::from_str("10.0.0.0/8").unwrap();
        assert_eq!(cidr.to_string(), "10.0.0.0/8");
    }

    #[test]
    fn test_cidr_rejects_nonzero_host_bits() {
        let result = CidrAddr::from_str("192.168.1.5/24");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("host bits"));

        let result = CidrAddr::from_str("192.168.1.0/24");
        assert!(result.is_ok());
    }

    #[test]
    fn test_cidr_ipv6() {
        let cidr = CidrAddr::from_str("2001:db8::/32").unwrap();
        assert!(cidr.is_ipv6());
        assert_eq!(cidr.prefix_len, 32);
    }

    #[test]
    fn test_cidr_contains() {
        let cidr = CidrAddr::from_str("192.168.0.0/24").unwrap();

        assert!(cidr.contains(&"192.168.0.1".parse().unwrap()));
        assert!(cidr.contains(&"192.168.0.255".parse().unwrap()));

        assert!(!cidr.contains(&"192.168.1.1".parse().unwrap()));
        assert!(!cidr.contains(&"10.0.0.1".parse().unwrap()));
    }

    #[test]
    fn test_cidr_netmask() {
        let cidr = CidrAddr::from_str("192.168.0.0/24").unwrap();
        assert_eq!(cidr.netmask().to_string(), "255.255.255.0");

        let cidr = CidrAddr::from_str("10.0.0.0/8").unwrap();
        assert_eq!(cidr.netmask().to_string(), "255.0.0.0");

        let cidr = CidrAddr::from_str("0.0.0.0/0").unwrap();
        assert_eq!(cidr.netmask().to_string(), "0.0.0.0");
    }

    #[test]
    fn test_cidr_broadcast() {
        let cidr = CidrAddr::from_str("192.168.0.0/24").unwrap();
        assert_eq!(cidr.broadcast().unwrap().to_string(), "192.168.0.255");

        let cidr = CidrAddr::from_str("10.0.0.0/8").unwrap();
        assert_eq!(cidr.broadcast().unwrap().to_string(), "10.255.255.255");
    }
}
