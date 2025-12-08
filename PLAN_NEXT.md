# PLAN_10: PostgreSQL Network Types & Functions (35 tests)

## Overview
Implement PostgreSQL network data types (INET, CIDR, MACADDR) and related functions.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/postgresql/functions/network.rs`

---

## Data Types

### INET
- Stores IPv4 or IPv6 host address with optional subnet mask
- Format: `192.168.1.1` or `192.168.1.0/24`
- Can store single hosts or network addresses

### CIDR
- Stores IPv4 or IPv6 network address
- Format: `192.168.1.0/24`
- Must have host bits set to zero

### MACADDR
- Stores MAC address (6 bytes)
- Format: `08:00:2b:01:02:03` or `08-00-2b-01-02-03`

### MACADDR8
- Stores EUI-64 MAC address (8 bytes)
- Format: `08:00:2b:01:02:03:04:05`

---

## Current Implementation Status

**File:** `crates/core/src/types/mod.rs`

Tags already exist:
- `TAG_INET` (140)
- `TAG_CIDR` (141)
- `TAG_MACADDR` (146)
- `TAG_MACADDR8` (147)

DataType variants exist:
- `DataType::Inet`
- `DataType::Cidr`
- `DataType::MacAddr`
- `DataType::MacAddr8`

---

## Network Operators

### Containment Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `<<` | Is contained by | `'192.168.1.5'::inet << '192.168.1.0/24'::inet` |
| `<<=` | Is contained by or equals | `'192.168.1.0/24'::inet <<= '192.168.1.0/24'::inet` |
| `>>` | Contains | `'192.168.1.0/24'::inet >> '192.168.1.5'::inet` |
| `>>=` | Contains or equals | `'192.168.1.0/24'::inet >>= '192.168.1.0/24'::inet` |
| `&&` | Contains or is contained by | `'192.168.1.0/24'::inet && '192.168.1.128/25'::inet` |

### Comparison Operators

| Operator | Description |
|----------|-------------|
| `<`, `<=`, `=`, `>=`, `>`, `<>` | Standard comparisons |

### Bitwise Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `~` | Bitwise NOT | `~ '192.168.1.5'::inet` |
| `&` | Bitwise AND | `'192.168.1.5'::inet & '255.255.255.0'::inet` |
| `\|` | Bitwise OR | `'192.168.1.5'::inet \| '0.0.0.255'::inet` |

### Arithmetic Operators

| Operator | Description | Example |
|----------|-------------|---------|
| `+` | Add integer | `'192.168.1.5'::inet + 10` |
| `-` | Subtract integer | `'192.168.1.15'::inet - 10` |
| `-` | Difference | `'192.168.1.15'::inet - '192.168.1.5'::inet` |

---

## Network Functions

### Extraction Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `abbrev(inet)` | Abbreviated display | TEXT |
| `abbrev(cidr)` | Abbreviated display | TEXT |
| `broadcast(inet)` | Broadcast address | INET |
| `family(inet)` | Address family (4 or 6) | INT |
| `host(inet)` | Host part as text | TEXT |
| `hostmask(inet)` | Host mask | INET |
| `masklen(inet)` | Netmask length | INT |
| `netmask(inet)` | Netmask | INET |
| `network(inet)` | Network part | CIDR |
| `text(inet)` | Address as text | TEXT |

### Construction Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `inet_same_family(inet, inet)` | Same address family? | BOOLEAN |
| `inet_merge(inet, inet)` | Smallest network containing both | CIDR |
| `set_masklen(inet, int)` | Set netmask length | INET |
| `set_masklen(cidr, int)` | Set netmask length | CIDR |

### MACADDR Functions

| Function | Description | Return Type |
|----------|-------------|-------------|
| `trunc(macaddr)` | Set last 3 bytes to zero | MACADDR |
| `macaddr8_set7bit(macaddr8)` | Set 7th bit to 1 | MACADDR8 |

---

## Implementation Details

### Network Address Storage

```rust
// In crates/core/src/types/network.rs

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

#[derive(Debug, Clone, PartialEq)]
pub struct InetAddr {
    pub addr: IpAddr,
    pub netmask: u8,
}

impl InetAddr {
    pub fn new(addr: IpAddr, netmask: u8) -> Self {
        Self { addr, netmask }
    }

    pub fn from_str(s: &str) -> Result<Self> {
        if let Some((addr_str, mask_str)) = s.split_once('/') {
            let addr: IpAddr = addr_str.parse()?;
            let netmask: u8 = mask_str.parse()?;
            Ok(Self { addr, netmask })
        } else {
            let addr: IpAddr = s.parse()?;
            let netmask = match addr {
                IpAddr::V4(_) => 32,
                IpAddr::V6(_) => 128,
            };
            Ok(Self { addr, netmask })
        }
    }

    pub fn family(&self) -> i32 {
        match self.addr {
            IpAddr::V4(_) => 4,
            IpAddr::V6(_) => 6,
        }
    }

    pub fn contains(&self, other: &InetAddr) -> bool {
        if self.family() != other.family() {
            return false;
        }
        if self.netmask > other.netmask {
            return false;
        }
        self.network_bits() == other.masked_to(self.netmask).network_bits()
    }

    pub fn broadcast(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(v4) => {
                let bits = u32::from(v4);
                let host_bits = 32 - self.netmask;
                let broadcast = bits | ((1u32 << host_bits) - 1);
                IpAddr::V4(Ipv4Addr::from(broadcast))
            }
            IpAddr::V6(v6) => {
                let bits = u128::from(v6);
                let host_bits = 128 - self.netmask;
                let broadcast = bits | ((1u128 << host_bits) - 1);
                IpAddr::V6(Ipv6Addr::from(broadcast))
            }
        }
    }

    pub fn netmask(&self) -> IpAddr {
        match self.addr {
            IpAddr::V4(_) => {
                let mask = !((1u32 << (32 - self.netmask)) - 1);
                IpAddr::V4(Ipv4Addr::from(mask))
            }
            IpAddr::V6(_) => {
                let mask = !((1u128 << (128 - self.netmask)) - 1);
                IpAddr::V6(Ipv6Addr::from(mask))
            }
        }
    }
}
```

### MACADDR Storage

```rust
#[derive(Debug, Clone, PartialEq)]
pub struct MacAddr {
    pub bytes: [u8; 6],
}

impl MacAddr {
    pub fn from_str(s: &str) -> Result<Self> {
        let s = s.replace(['-', '.'], ":");
        let parts: Vec<&str> = s.split(':').collect();
        if parts.len() != 6 {
            return Err(Error::invalid_mac_address(s));
        }
        let mut bytes = [0u8; 6];
        for (i, part) in parts.iter().enumerate() {
            bytes[i] = u8::from_str_radix(part, 16)?;
        }
        Ok(Self { bytes })
    }

    pub fn trunc(&self) -> Self {
        Self {
            bytes: [self.bytes[0], self.bytes[1], self.bytes[2], 0, 0, 0],
        }
    }
}

impl std::fmt::Display for MacAddr {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{:02x}:{:02x}:{:02x}:{:02x}:{:02x}:{:02x}",
            self.bytes[0], self.bytes[1], self.bytes[2],
            self.bytes[3], self.bytes[4], self.bytes[5]
        )
    }
}
```

### Network Functions Implementation

```rust
pub fn inet_broadcast(inet: &InetAddr) -> Result<Value> {
    let broadcast = inet.broadcast();
    Ok(Value::inet(InetAddr::new(broadcast, inet.netmask)))
}

pub fn inet_family(inet: &InetAddr) -> Result<Value> {
    Ok(Value::int64(inet.family() as i64))
}

pub fn inet_host(inet: &InetAddr) -> Result<Value> {
    Ok(Value::string(inet.addr.to_string()))
}

pub fn inet_masklen(inet: &InetAddr) -> Result<Value> {
    Ok(Value::int64(inet.netmask as i64))
}

pub fn inet_netmask(inet: &InetAddr) -> Result<Value> {
    let mask = inet.netmask();
    Ok(Value::inet(InetAddr::new(mask, match mask {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    })))
}

pub fn inet_network(inet: &InetAddr) -> Result<Value> {
    let network = inet.network_addr();
    Ok(Value::cidr(InetAddr::new(network, inet.netmask)))
}

pub fn inet_same_family(a: &InetAddr, b: &InetAddr) -> Result<Value> {
    Ok(Value::bool(a.family() == b.family()))
}

pub fn set_masklen(inet: &InetAddr, len: i64) -> Result<Value> {
    let max_len = match inet.addr {
        IpAddr::V4(_) => 32,
        IpAddr::V6(_) => 128,
    };
    if len < 0 || len > max_len as i64 {
        return Err(Error::invalid_netmask_length(len));
    }
    Ok(Value::inet(InetAddr::new(inet.addr, len as u8)))
}
```

---

## Key Files to Modify

1. **New File:** `crates/core/src/types/network.rs`
   - `InetAddr` struct
   - `CidrAddr` struct
   - `MacAddr` struct

2. **Type System:** `crates/core/src/types/mod.rs`
   - Value constructors/accessors for network types

3. **Functions:** `crates/functions/src/network.rs`
   - All network functions

4. **Registry:** `crates/functions/src/registry/network_funcs.rs`
   - Register network functions

5. **Parser:** Parse network literals and casts

---

## Implementation Order

### Phase 1: INET Type
1. `InetAddr` struct with parsing
2. Value constructors/accessors
3. Basic comparison operators

### Phase 2: INET Functions
1. `family()`, `host()`, `masklen()`
2. `broadcast()`, `netmask()`, `network()`
3. `set_masklen()`

### Phase 3: Containment Operators
1. `<<` (is contained by)
2. `>>` (contains)
3. `<<=`, `>>=`

### Phase 4: CIDR Type
1. CIDR validation (host bits must be zero)
2. CIDR-specific functions

### Phase 5: MACADDR Types
1. `MacAddr` struct
2. `MacAddr8` struct
3. `trunc()` function

---

## Testing Pattern

```rust
#[test]
fn test_inet_literal() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '192.168.1.5'::inet").unwrap();
    assert_batch_eq!(result, [["192.168.1.5"]]);
}

#[test]
fn test_inet_with_mask() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '192.168.1.0/24'::inet").unwrap();
    assert_batch_eq!(result, [["192.168.1.0/24"]]);
}

#[test]
fn test_inet_containment() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT '192.168.1.5'::inet << '192.168.1.0/24'::inet"
    ).unwrap();
    assert_batch_eq!(result, [[true]]);
}

#[test]
fn test_inet_family() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT family('192.168.1.1'::inet)").unwrap();
    assert_batch_eq!(result, [[4]]);

    let result = executor.execute_sql("SELECT family('::1'::inet)").unwrap();
    assert_batch_eq!(result, [[6]]);
}

#[test]
fn test_inet_broadcast() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT broadcast('192.168.1.0/24'::inet)").unwrap();
    assert_batch_eq!(result, [["192.168.1.255/24"]]);
}

#[test]
fn test_macaddr() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT '08:00:2b:01:02:03'::macaddr").unwrap();
    assert_batch_eq!(result, [["08:00:2b:01:02:03"]]);
}

#[test]
fn test_macaddr_trunc() {
    let mut executor = create_executor();
    let result = executor.execute_sql("SELECT trunc('08:00:2b:01:02:03'::macaddr)").unwrap();
    assert_batch_eq!(result, [["08:00:2b:00:00:00"]]);
}
```

---

## Verification Steps

1. Run: `cargo test --test postgresql -- functions::network --ignored`
2. Implement INET type and basic functions
3. Add containment operators
4. Add CIDR and MACADDR types
5. Remove `#[ignore = "Implement me!"]` as tests pass
