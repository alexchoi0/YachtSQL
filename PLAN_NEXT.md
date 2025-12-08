# PLAN_16: ClickHouse Geospatial Functions (29 tests)

## Overview
Implement ClickHouse geographic and geospatial functions.

## Test File Location
`/Users/alex/Desktop/git/yachtsql-public/tests/clickhouse/functions/geo.rs`

---

## Functions to Implement

### Distance Functions

| Function | Description |
|----------|-------------|
| `greatCircleDistance(lon1, lat1, lon2, lat2)` | Great circle distance in meters |
| `geoDistance(lon1, lat1, lon2, lat2)` | Geographic distance (alias) |
| `greatCircleAngle(lon1, lat1, lon2, lat2)` | Central angle in degrees |

### Point-in-Polygon Functions

| Function | Description |
|----------|-------------|
| `pointInEllipses(x, y, ellipse1, ...)` | Check if point in any ellipse |
| `pointInPolygon(point, polygon)` | Check if point in polygon |

### Geohash Functions

| Function | Description |
|----------|-------------|
| `geohashEncode(lon, lat, precision)` | Encode to geohash string |
| `geohashDecode(geohash)` | Decode geohash to (lon, lat) |
| `geohashesInBox(lon_min, lat_min, lon_max, lat_max, precision)` | Geohashes in bounding box |

### H3 Index Functions

| Function | Description |
|----------|-------------|
| `geoToH3(lon, lat, resolution)` | Convert to H3 index |
| `h3ToGeo(h3index)` | Convert H3 to (lon, lat) |
| `h3ToGeoBoundary(h3index)` | Get H3 cell boundary |
| `h3GetResolution(h3index)` | Get H3 resolution |
| `h3IsValid(h3index)` | Check if valid H3 index |
| `h3kRing(h3index, k)` | Get k-ring neighbors |
| `h3GetBaseCell(h3index)` | Get base cell |
| `h3HexAreaM2(resolution)` | Hex area in m² |
| `h3IndexesAreNeighbors(h3index1, h3index2)` | Check if neighbors |
| `h3ToChildren(h3index, childRes)` | Get child cells |
| `h3ToParent(h3index, parentRes)` | Get parent cell |
| `h3ToString(h3index)` | Convert to string |
| `stringToH3(string)` | Parse H3 from string |

### S2 Index Functions

| Function | Description |
|----------|-------------|
| `geoToS2(lon, lat)` | Convert to S2 cell ID |
| `s2ToGeo(s2index)` | Convert S2 to (lon, lat) |
| `s2GetNeighbors(s2index)` | Get neighbor cells |
| `s2CellsIntersect(s2index1, s2index2)` | Check intersection |
| `s2CapContains(center_lon, center_lat, radius, s2index)` | Point in cap |
| `s2RectAdd(rect, s2index)` | Add cell to rect |
| `s2RectContains(rect, s2index)` | Check containment |
| `s2RectUnion(rect1, rect2)` | Union of rects |
| `s2RectIntersection(rect1, rect2)` | Intersection of rects |

---

## Implementation Details

### Great Circle Distance (Haversine)

```rust
pub fn great_circle_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6_371_000.0;

    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);

    let c = 2.0 * a.sqrt().asin();

    EARTH_RADIUS_M * c
}

pub fn great_circle_angle(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    // Returns central angle in degrees
    let distance = great_circle_distance(lon1, lat1, lon2, lat2);
    const EARTH_RADIUS_M: f64 = 6_371_000.0;
    (distance / EARTH_RADIUS_M).to_degrees()
}
```

### Point in Ellipse

```rust
pub fn point_in_ellipses(x: f64, y: f64, ellipses: &[(f64, f64, f64, f64)]) -> bool {
    // Each ellipse is (center_x, center_y, semi_axis_x, semi_axis_y)
    for (cx, cy, ax, ay) in ellipses {
        let dx = x - cx;
        let dy = y - cy;
        if (dx * dx) / (ax * ax) + (dy * dy) / (ay * ay) <= 1.0 {
            return true;
        }
    }
    false
}
```

### Point in Polygon

```rust
pub fn point_in_polygon(x: f64, y: f64, polygon: &[(f64, f64)]) -> bool {
    // Ray casting algorithm
    let mut inside = false;
    let n = polygon.len();

    if n < 3 {
        return false;
    }

    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = polygon[i];
        let (xj, yj) = polygon[j];

        if ((yi > y) != (yj > y)) && (x < (xj - xi) * (y - yi) / (yj - yi) + xi) {
            inside = !inside;
        }
        j = i;
    }

    inside
}
```

### Geohash Encoding

```rust
const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

pub fn geohash_encode(lon: f64, lat: f64, precision: usize) -> String {
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut geohash = String::with_capacity(precision);
    let mut bits = 0u8;
    let mut bit_count = 0;
    let mut is_lon = true;

    while geohash.len() < precision {
        if is_lon {
            let mid = (lon_range.0 + lon_range.1) / 2.0;
            if lon >= mid {
                bits = (bits << 1) | 1;
                lon_range.0 = mid;
            } else {
                bits <<= 1;
                lon_range.1 = mid;
            }
        } else {
            let mid = (lat_range.0 + lat_range.1) / 2.0;
            if lat >= mid {
                bits = (bits << 1) | 1;
                lat_range.0 = mid;
            } else {
                bits <<= 1;
                lat_range.1 = mid;
            }
        }

        is_lon = !is_lon;
        bit_count += 1;

        if bit_count == 5 {
            geohash.push(BASE32[bits as usize] as char);
            bits = 0;
            bit_count = 0;
        }
    }

    geohash
}

pub fn geohash_decode(geohash: &str) -> Result<(f64, f64)> {
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut is_lon = true;

    for c in geohash.chars() {
        let idx = BASE32.iter().position(|&b| b as char == c)
            .ok_or_else(|| Error::invalid_geohash(geohash))?;

        for i in (0..5).rev() {
            let bit = (idx >> i) & 1;
            if is_lon {
                let mid = (lon_range.0 + lon_range.1) / 2.0;
                if bit == 1 {
                    lon_range.0 = mid;
                } else {
                    lon_range.1 = mid;
                }
            } else {
                let mid = (lat_range.0 + lat_range.1) / 2.0;
                if bit == 1 {
                    lat_range.0 = mid;
                } else {
                    lat_range.1 = mid;
                }
            }
            is_lon = !is_lon;
        }
    }

    let lon = (lon_range.0 + lon_range.1) / 2.0;
    let lat = (lat_range.0 + lat_range.1) / 2.0;

    Ok((lon, lat))
}
```

### Geohashes in Box

```rust
pub fn geohashes_in_box(
    lon_min: f64,
    lat_min: f64,
    lon_max: f64,
    lat_max: f64,
    precision: usize,
) -> Vec<String> {
    let mut result = Vec::new();

    // Calculate step size based on precision
    let (lon_step, lat_step) = geohash_cell_size(precision);

    let mut lon = lon_min;
    while lon <= lon_max {
        let mut lat = lat_min;
        while lat <= lat_max {
            let hash = geohash_encode(lon, lat, precision);
            if !result.contains(&hash) {
                result.push(hash);
            }
            lat += lat_step;
        }
        lon += lon_step;
    }

    result
}

fn geohash_cell_size(precision: usize) -> (f64, f64) {
    // Approximate cell dimensions for given precision
    let lon_bits = (precision * 5 + 1) / 2;
    let lat_bits = precision * 5 / 2;
    let lon_size = 360.0 / (1u64 << lon_bits) as f64;
    let lat_size = 180.0 / (1u64 << lat_bits) as f64;
    (lon_size, lat_size)
}
```

### H3 Functions (Simplified)

```rust
// Note: Full H3 implementation would use the h3 crate
// This is a simplified version for demonstration

pub fn geo_to_h3(lon: f64, lat: f64, resolution: u8) -> u64 {
    // Placeholder - in real impl, use h3 crate
    // h3::lat_lng_to_cell(LatLng::new(lat, lon), resolution)
    let lat_idx = ((lat + 90.0) / 180.0 * (1u64 << 20) as f64) as u64;
    let lon_idx = ((lon + 180.0) / 360.0 * (1u64 << 20) as f64) as u64;
    (resolution as u64) << 56 | lat_idx << 28 | lon_idx
}

pub fn h3_to_geo(h3index: u64) -> (f64, f64) {
    // Placeholder - extract lat/lon from simplified index
    let lat_idx = (h3index >> 28) & 0xFFFFF;
    let lon_idx = h3index & 0xFFFFFFF;
    let lat = lat_idx as f64 / (1u64 << 20) as f64 * 180.0 - 90.0;
    let lon = lon_idx as f64 / (1u64 << 20) as f64 * 360.0 - 180.0;
    (lon, lat)
}

pub fn h3_get_resolution(h3index: u64) -> u8 {
    (h3index >> 56) as u8
}

pub fn h3_is_valid(h3index: u64) -> bool {
    let res = h3_get_resolution(h3index);
    res <= 15  // H3 resolutions are 0-15
}
```

---

## Key Files to Modify

1. **Functions:** `crates/functions/src/clickhouse/geo.rs` (new)
   - All geo functions

2. **Registry:** `crates/functions/src/registry/clickhouse_geo.rs` (new)
   - Register geo functions

3. **Dependencies:** Consider adding `h3` crate for full H3 support

---

## Implementation Order

### Phase 1: Distance Functions
1. `greatCircleDistance`
2. `geoDistance`
3. `greatCircleAngle`

### Phase 2: Point-in-Shape
1. `pointInEllipses`
2. `pointInPolygon`

### Phase 3: Geohash
1. `geohashEncode`
2. `geohashDecode`
3. `geohashesInBox`

### Phase 4: H3 (if needed)
1. `geoToH3`, `h3ToGeo`
2. `h3GetResolution`, `h3IsValid`
3. Additional H3 functions

### Phase 5: S2 (if needed)
1. `geoToS2`, `s2ToGeo`
2. S2 spatial operations

---

## Testing Pattern

```rust
#[test]
fn test_great_circle_distance() {
    let mut executor = create_executor();
    // New York to London
    let result = executor.execute_sql(
        "SELECT greatCircleDistance(-74.006, 40.7128, -0.1276, 51.5074)"
    ).unwrap();
    // Distance should be approximately 5,570 km
    let dist = result.get_value(0, 0).as_f64().unwrap();
    assert!((dist - 5_570_000.0).abs() < 50_000.0);  // Within 50km
}

#[test]
fn test_point_in_ellipses() {
    let mut executor = create_executor();
    // Point at origin, ellipse centered at origin with semi-axes 10, 10
    let result = executor.execute_sql(
        "SELECT pointInEllipses(0, 0, 0, 0, 10, 10)"
    ).unwrap();
    assert_batch_eq!(result, [[1]]);
}

#[test]
fn test_point_in_polygon() {
    let mut executor = create_executor();
    // Square polygon, test point inside
    let result = executor.execute_sql(
        "SELECT pointInPolygon((5, 5), [(0, 0), (10, 0), (10, 10), (0, 10)])"
    ).unwrap();
    assert_batch_eq!(result, [[1]]);
}

#[test]
fn test_geohash_encode() {
    let mut executor = create_executor();
    // Encode San Francisco coordinates
    let result = executor.execute_sql(
        "SELECT geohashEncode(-122.4194, 37.7749, 6)"
    ).unwrap();
    // Should start with '9q8y' for SF area
    let hash = result.get_value(0, 0).as_str().unwrap();
    assert!(hash.starts_with("9q8y"));
}

#[test]
fn test_geohash_decode() {
    let mut executor = create_executor();
    let result = executor.execute_sql(
        "SELECT geohashDecode('9q8yy')"
    ).unwrap();
    // Should return (lon, lat) tuple near SF
    let arr = result.get_value(0, 0).as_array().unwrap();
    let lon = arr[0].as_f64().unwrap();
    let lat = arr[1].as_f64().unwrap();
    assert!((lon - (-122.4)).abs() < 0.5);
    assert!((lat - 37.8).abs() < 0.5);
}
```

---

## Verification Steps

1. Run: `cargo test --test clickhouse -- functions::geo --ignored`
2. Implement distance functions first
3. Add point-in-shape functions
4. Add geohash functions
5. Add H3/S2 if tests require them
6. Remove `#[ignore = "Implement me!"]` as tests pass
