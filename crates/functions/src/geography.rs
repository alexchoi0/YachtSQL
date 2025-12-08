use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;

const EARTH_RADIUS_M: f64 = 6_371_008.8;
const WGS84_A: f64 = 6_378_137.0;
const WGS84_F: f64 = 1.0 / 298.257223563;

#[derive(Debug, Clone, PartialEq)]
pub enum Geometry {
    Point { lon: f64, lat: f64 },
    LineString { points: Vec<(f64, f64)> },
    Polygon { rings: Vec<Vec<(f64, f64)>> },
    MultiPoint { points: Vec<(f64, f64)> },
    Empty,
}

impl Geometry {
    pub fn distance(&self, other: &Geometry) -> Result<f64> {
        match (self, other) {
            (
                Geometry::Point {
                    lon: lon1,
                    lat: lat1,
                },
                Geometry::Point {
                    lon: lon2,
                    lat: lat2,
                },
            ) => Ok(haversine_distance(*lat1, *lon1, *lat2, *lon2)),
            _ => Err(Error::invalid_query(
                "ST_Distance only supports POINT to POINT distance".to_string(),
            )),
        }
    }

    pub fn length(&self) -> Result<f64> {
        match self {
            Geometry::LineString { points } => {
                let mut total = 0.0;
                for i in 0..points.len() - 1 {
                    let (lon1, lat1) = points[i];
                    let (lon2, lat2) = points[i + 1];
                    total += haversine_distance(lat1, lon1, lat2, lon2);
                }
                Ok(total)
            }
            _ => Err(Error::invalid_query(
                "ST_Length only supports LINESTRING".to_string(),
            )),
        }
    }

    pub fn area(&self) -> Result<f64> {
        match self {
            Geometry::Polygon { rings } => {
                if rings.is_empty() {
                    return Ok(0.0);
                }

                let ring = &rings[0];
                Ok(spherical_polygon_area(ring))
            }
            _ => Err(Error::invalid_query(
                "ST_Area only supports POLYGON".to_string(),
            )),
        }
    }

    pub fn perimeter(&self) -> Result<f64> {
        match self {
            Geometry::Polygon { rings } => {
                let mut total = 0.0;
                for ring in rings {
                    for i in 0..ring.len() - 1 {
                        let (lon1, lat1) = ring[i];
                        let (lon2, lat2) = ring[i + 1];
                        total += haversine_distance(lat1, lon1, lat2, lon2);
                    }
                }
                Ok(total)
            }
            _ => Err(Error::invalid_query(
                "ST_Perimeter only supports POLYGON".to_string(),
            )),
        }
    }

    pub fn to_geojson(&self) -> String {
        match self {
            Geometry::Point { lon, lat } => {
                format!(r#"{{"type":"Point","coordinates":[{},{}]}}"#, lon, lat)
            }
            Geometry::MultiPoint { points } => {
                let coords: Vec<String> = points
                    .iter()
                    .map(|(lon, lat)| format!("[{},{}]", lon, lat))
                    .collect();
                format!(
                    r#"{{"type":"MultiPoint","coordinates":[{}]}}"#,
                    coords.join(",")
                )
            }
            Geometry::LineString { points } => {
                let coords: Vec<String> = points
                    .iter()
                    .map(|(lon, lat)| format!("[{},{}]", lon, lat))
                    .collect();
                format!(
                    r#"{{"type":"LineString","coordinates":[{}]}}"#,
                    coords.join(",")
                )
            }
            Geometry::Polygon { rings } => {
                let ring_strs: Vec<String> = rings
                    .iter()
                    .map(|ring| {
                        let coords: Vec<String> = ring
                            .iter()
                            .map(|(lon, lat)| format!("[{},{}]", lon, lat))
                            .collect();
                        format!("[{}]", coords.join(","))
                    })
                    .collect();
                format!(
                    r#"{{"type":"Polygon","coordinates":[{}]}}"#,
                    ring_strs.join(",")
                )
            }
            Geometry::Empty => r#"{"type":"GeometryCollection","geometries":[]}"#.to_string(),
        }
    }

    pub fn contains(&self, other: &Geometry) -> Result<bool> {
        match (self, other) {
            (Geometry::Polygon { rings }, Geometry::Point { lon, lat }) => {
                if rings.is_empty() {
                    return Ok(false);
                }

                Ok(point_in_polygon(*lon, *lat, &rings[0]))
            }
            _ => Err(Error::invalid_query(
                "ST_Contains only supports POLYGON contains POINT".to_string(),
            )),
        }
    }

    pub fn intersects(&self, other: &Geometry) -> Result<bool> {
        let bbox1 = self.bounding_box();
        let bbox2 = other.bounding_box();

        Ok(bboxes_intersect(&bbox1, &bbox2))
    }

    pub fn disjoint(&self, other: &Geometry) -> Result<bool> {
        Ok(!self.intersects(other)?)
    }

    pub fn dimension(&self) -> i32 {
        match self {
            Geometry::Point { .. } => 0,
            Geometry::MultiPoint { .. } => 0,
            Geometry::LineString { .. } => 1,
            Geometry::Polygon { .. } => 2,
            Geometry::Empty => -1,
        }
    }

    fn bounding_box(&self) -> (f64, f64, f64, f64) {
        match self {
            Geometry::Point { lon, lat } => (*lon, *lat, *lon, *lat),
            Geometry::LineString { points } | Geometry::MultiPoint { points } => {
                let mut min_lon = f64::MAX;
                let mut min_lat = f64::MAX;
                let mut max_lon = f64::MIN;
                let mut max_lat = f64::MIN;
                for (lon, lat) in points {
                    min_lon = min_lon.min(*lon);
                    min_lat = min_lat.min(*lat);
                    max_lon = max_lon.max(*lon);
                    max_lat = max_lat.max(*lat);
                }
                (min_lon, min_lat, max_lon, max_lat)
            }
            Geometry::Polygon { rings } => {
                if rings.is_empty() {
                    return (0.0, 0.0, 0.0, 0.0);
                }
                let mut min_lon = f64::MAX;
                let mut min_lat = f64::MAX;
                let mut max_lon = f64::MIN;
                let mut max_lat = f64::MIN;
                for (lon, lat) in &rings[0] {
                    min_lon = min_lon.min(*lon);
                    min_lat = min_lat.min(*lat);
                    max_lon = max_lon.max(*lon);
                    max_lat = max_lat.max(*lat);
                }
                (min_lon, min_lat, max_lon, max_lat)
            }
            Geometry::Empty => (0.0, 0.0, 0.0, 0.0),
        }
    }

    pub fn validate(&self) -> Result<()> {
        match self {
            Geometry::Point { lon, lat } => {
                validate_longitude(*lon)?;
                validate_latitude(*lat)?;
            }
            Geometry::MultiPoint { points } => {
                for (lon, lat) in points {
                    validate_longitude(*lon)?;
                    validate_latitude(*lat)?;
                }
            }
            Geometry::LineString { points } => {
                if points.len() < 2 {
                    return Err(Error::invalid_query(
                        "LINESTRING must have at least 2 points".to_string(),
                    ));
                }
                for (lon, lat) in points {
                    validate_longitude(*lon)?;
                    validate_latitude(*lat)?;
                }
            }
            Geometry::Polygon { rings } => {
                if rings.is_empty() {
                    return Err(Error::invalid_query(
                        "POLYGON must have at least one ring".to_string(),
                    ));
                }
                for ring in rings {
                    if ring.len() < 4 {
                        return Err(Error::invalid_query(
                            "Polygon ring must have at least 4 points".to_string(),
                        ));
                    }

                    if ring.first() != ring.last() {
                        return Err(Error::invalid_query(format!(
                            "Polygon ring is not closed: first point ({}, {}) != last point ({}, {})",
                            ring.first().unwrap().0,
                            ring.first().unwrap().1,
                            ring.last().unwrap().0,
                            ring.last().unwrap().1
                        )));
                    }

                    for (lon, lat) in ring {
                        validate_longitude(*lon)?;
                        validate_latitude(*lat)?;
                    }
                }
            }
            Geometry::Empty => {}
        }
        Ok(())
    }

    pub fn to_wkt(&self) -> String {
        match self {
            Geometry::Point { lon, lat } => format!("POINT({} {})", lon, lat),
            Geometry::MultiPoint { points } => {
                let coords: Vec<String> = points
                    .iter()
                    .map(|(lon, lat)| format!("({} {})", lon, lat))
                    .collect();
                format!("MULTIPOINT({})", coords.join(", "))
            }
            Geometry::LineString { points } => {
                let coords: Vec<String> = points
                    .iter()
                    .map(|(lon, lat)| format!("{} {}", lon, lat))
                    .collect();
                format!("LINESTRING({})", coords.join(", "))
            }
            Geometry::Polygon { rings } => {
                let ring_strs: Vec<String> = rings
                    .iter()
                    .map(|ring| {
                        let coords: Vec<String> = ring
                            .iter()
                            .map(|(lon, lat)| format!("{} {}", lon, lat))
                            .collect();
                        format!("({})", coords.join(", "))
                    })
                    .collect();
                format!("POLYGON({})", ring_strs.join(", "))
            }
            Geometry::Empty => "GEOMETRYCOLLECTION EMPTY".to_string(),
        }
    }
}

fn validate_longitude(lon: f64) -> Result<()> {
    if !(-180.0..=180.0).contains(&lon) {
        Err(Error::invalid_query(format!(
            "Longitude must be in range [-180, 180], got {}",
            lon
        )))
    } else {
        Ok(())
    }
}

fn validate_latitude(lat: f64) -> Result<()> {
    if !(-90.0..=90.0).contains(&lat) {
        Err(Error::invalid_query(format!(
            "Latitude must be in range [-90, 90], got {}",
            lat
        )))
    } else {
        Ok(())
    }
}

pub fn parse_geojson(json_str: &str) -> Result<Geometry> {
    let json: serde_json::Value = serde_json::from_str(json_str).map_err(|e| {
        Error::invalid_query(format!("Invalid GeoJSON: failed to parse JSON: {}", e))
    })?;

    let geom_type = json
        .get("type")
        .and_then(|v| v.as_str())
        .ok_or_else(|| Error::invalid_query("GeoJSON missing 'type' field".to_string()))?;

    let coords = json
        .get("coordinates")
        .ok_or_else(|| Error::invalid_query("GeoJSON missing 'coordinates' field".to_string()))?;

    match geom_type {
        "Point" => parse_geojson_point(coords),
        "LineString" => parse_geojson_linestring(coords),
        "Polygon" => parse_geojson_polygon(coords),
        _ => Err(Error::invalid_query(format!(
            "Unsupported GeoJSON type: '{}'",
            geom_type
        ))),
    }
}

fn parse_geojson_point(coords: &serde_json::Value) -> Result<Geometry> {
    let arr = coords.as_array().ok_or_else(|| {
        Error::invalid_query("GeoJSON Point coordinates must be an array".to_string())
    })?;

    if arr.len() != 2 {
        return Err(Error::invalid_query(format!(
            "GeoJSON Point must have 2 coordinates, got {}",
            arr.len()
        )));
    }

    let lon = arr[0].as_f64().ok_or_else(|| {
        Error::invalid_query("GeoJSON Point longitude must be a number".to_string())
    })?;
    let lat = arr[1].as_f64().ok_or_else(|| {
        Error::invalid_query("GeoJSON Point latitude must be a number".to_string())
    })?;

    let geom = Geometry::Point { lon, lat };
    geom.validate()?;
    Ok(geom)
}

fn parse_geojson_linestring(coords: &serde_json::Value) -> Result<Geometry> {
    let arr = coords.as_array().ok_or_else(|| {
        Error::invalid_query("GeoJSON LineString coordinates must be an array".to_string())
    })?;

    let mut points = Vec::new();
    for coord in arr {
        let point_arr = coord.as_array().ok_or_else(|| {
            Error::invalid_query("GeoJSON LineString coordinate must be an array".to_string())
        })?;

        if point_arr.len() != 2 {
            return Err(Error::invalid_query(
                "GeoJSON coordinate must have 2 values".to_string(),
            ));
        }

        let lon = point_arr[0].as_f64().ok_or_else(|| {
            Error::invalid_query("GeoJSON coordinate longitude must be a number".to_string())
        })?;
        let lat = point_arr[1].as_f64().ok_or_else(|| {
            Error::invalid_query("GeoJSON coordinate latitude must be a number".to_string())
        })?;

        points.push((lon, lat));
    }

    let geom = Geometry::LineString { points };
    geom.validate()?;
    Ok(geom)
}

fn parse_geojson_polygon(coords: &serde_json::Value) -> Result<Geometry> {
    let arr = coords.as_array().ok_or_else(|| {
        Error::invalid_query("GeoJSON Polygon coordinates must be an array".to_string())
    })?;

    let mut rings = Vec::new();

    for ring_coords in arr {
        let ring_arr = ring_coords.as_array().ok_or_else(|| {
            Error::invalid_query("GeoJSON Polygon ring must be an array".to_string())
        })?;

        let mut ring = Vec::new();
        for coord in ring_arr {
            let point_arr = coord.as_array().ok_or_else(|| {
                Error::invalid_query("GeoJSON Polygon coordinate must be an array".to_string())
            })?;

            if point_arr.len() != 2 {
                return Err(Error::invalid_query(
                    "GeoJSON coordinate must have 2 values".to_string(),
                ));
            }

            let lon = point_arr[0].as_f64().ok_or_else(|| {
                Error::invalid_query("GeoJSON coordinate longitude must be a number".to_string())
            })?;
            let lat = point_arr[1].as_f64().ok_or_else(|| {
                Error::invalid_query("GeoJSON coordinate latitude must be a number".to_string())
            })?;

            ring.push((lon, lat));
        }
        rings.push(ring);
    }

    let geom = Geometry::Polygon { rings };
    geom.validate()?;
    Ok(geom)
}

pub fn parse_wkt(wkt: &str) -> Result<Geometry> {
    let wkt = wkt.trim();

    if wkt.eq_ignore_ascii_case("GEOMETRYCOLLECTION EMPTY") {
        return Ok(Geometry::Empty);
    }

    if let Some(rest) = wkt.strip_prefix("MULTIPOINT") {
        parse_wkt_multipoint(rest.trim())
    } else if let Some(rest) = wkt.strip_prefix("POINT") {
        parse_wkt_point(rest.trim())
    } else if let Some(rest) = wkt.strip_prefix("LINESTRING") {
        parse_wkt_linestring(rest.trim())
    } else if let Some(rest) = wkt.strip_prefix("POLYGON") {
        parse_wkt_polygon(rest.trim())
    } else {
        Err(Error::invalid_query(format!(
            "Invalid WKT: unknown geometry type in '{}'",
            wkt
        )))
    }
}

fn parse_wkt_point(rest: &str) -> Result<Geometry> {
    if rest.eq_ignore_ascii_case("EMPTY") {
        return Ok(Geometry::Empty);
    }

    let coords = rest.trim_matches(|c| c == '(' || c == ')').trim();
    let parts: Vec<&str> = coords.split_whitespace().collect();

    if parts.len() != 2 {
        return Err(Error::invalid_query(format!(
            "Invalid POINT WKT: expected 2 coordinates, got {}",
            parts.len()
        )));
    }

    let lon: f64 = parts[0]
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid longitude value: '{}'", parts[0])))?;
    let lat: f64 = parts[1]
        .parse()
        .map_err(|_| Error::invalid_query(format!("Invalid latitude value: '{}'", parts[1])))?;

    let geom = Geometry::Point { lon, lat };
    geom.validate()?;
    Ok(geom)
}

fn parse_wkt_multipoint(rest: &str) -> Result<Geometry> {
    if rest.eq_ignore_ascii_case("EMPTY") {
        return Ok(Geometry::Empty);
    }

    let rest = rest.trim();
    let rest = rest.trim_start_matches('(').trim_end_matches(')');

    let mut points = Vec::new();
    for part in rest.split(',') {
        let part = part.trim().trim_matches(|c| c == '(' || c == ')').trim();
        let parts: Vec<&str> = part.split_whitespace().collect();
        if parts.len() != 2 {
            return Err(Error::invalid_query(format!(
                "Invalid MULTIPOINT coordinate: '{}'",
                part
            )));
        }
        let lon: f64 = parts[0].parse().map_err(|_| {
            Error::invalid_query(format!("Invalid longitude value: '{}'", parts[0]))
        })?;
        let lat: f64 = parts[1]
            .parse()
            .map_err(|_| Error::invalid_query(format!("Invalid latitude value: '{}'", parts[1])))?;
        points.push((lon, lat));
    }

    let geom = Geometry::MultiPoint { points };
    geom.validate()?;
    Ok(geom)
}

fn parse_wkt_linestring(rest: &str) -> Result<Geometry> {
    if rest.eq_ignore_ascii_case("EMPTY") {
        return Ok(Geometry::Empty);
    }

    let coords = rest.trim_matches(|c| c == '(' || c == ')').trim();
    let points = parse_coordinate_list(coords)?;

    let geom = Geometry::LineString { points };
    geom.validate()?;
    Ok(geom)
}

fn parse_wkt_polygon(rest: &str) -> Result<Geometry> {
    if rest.eq_ignore_ascii_case("EMPTY") {
        return Ok(Geometry::Empty);
    }

    let rest = rest.trim();
    let rest = rest
        .strip_prefix('(')
        .ok_or_else(|| Error::invalid_query("POLYGON WKT must start with '('".to_string()))?;
    let rest = rest
        .strip_suffix(')')
        .ok_or_else(|| Error::invalid_query("POLYGON WKT must end with ')'".to_string()))?;

    let rings = parse_polygon_rings(rest)?;

    let geom = Geometry::Polygon { rings };
    geom.validate()?;
    Ok(geom)
}

fn parse_polygon_rings(s: &str) -> Result<Vec<Vec<(f64, f64)>>> {
    let mut rings = Vec::new();
    let mut current = String::new();
    let mut depth = 0;

    for ch in s.chars() {
        match ch {
            '(' => {
                depth += 1;
                if depth > 1 {
                    current.push(ch);
                }
            }
            ')' => {
                depth -= 1;
                if depth == 0 {
                    let ring = parse_coordinate_list(current.trim())?;
                    rings.push(ring);
                    current.clear();
                } else {
                    current.push(ch);
                }
            }
            ',' if depth == 0 => {}
            _ => {
                if depth > 0 {
                    current.push(ch);
                }
            }
        }
    }

    if !current.trim().is_empty() {
        let ring = parse_coordinate_list(current.trim())?;
        rings.push(ring);
    }

    Ok(rings)
}

fn parse_coordinate_list(coords: &str) -> Result<Vec<(f64, f64)>> {
    let mut points = Vec::new();

    for pair in coords.split(',') {
        let pair = pair.trim();
        let parts: Vec<&str> = pair.split_whitespace().collect();

        if parts.len() != 2 {
            return Err(Error::invalid_query(format!(
                "Invalid coordinate pair: expected 2 values, got {}",
                parts.len()
            )));
        }

        let lon: f64 = parts[0].parse().map_err(|_| {
            Error::invalid_query(format!("Invalid longitude value: '{}'", parts[0]))
        })?;
        let lat: f64 = parts[1]
            .parse()
            .map_err(|_| Error::invalid_query(format!("Invalid latitude value: '{}'", parts[1])))?;

        points.push((lon, lat));
    }

    Ok(points)
}

pub fn make_line(points: Vec<(f64, f64)>) -> Result<Geometry> {
    if points.len() < 2 {
        return Err(Error::invalid_query(
            "ST_MakeLine requires at least 2 points".to_string(),
        ));
    }
    let geom = Geometry::LineString { points };
    geom.validate()?;
    Ok(geom)
}

pub fn make_polygon(line: &Geometry) -> Result<Geometry> {
    match line {
        Geometry::LineString { points } => {
            if points.is_empty() {
                return Err(Error::invalid_query(
                    "Cannot create polygon from empty linestring".to_string(),
                ));
            }

            if points.first() != points.last() {
                return Err(Error::invalid_query(
                    "ST_MakePolygon requires a closed linestring".to_string(),
                ));
            }
            let geom = Geometry::Polygon {
                rings: vec![points.clone()],
            };
            geom.validate()?;
            Ok(geom)
        }
        _ => Err(Error::invalid_query(
            "ST_MakePolygon requires a LINESTRING".to_string(),
        )),
    }
}

fn point_in_polygon(x: f64, y: f64, polygon: &[(f64, f64)]) -> bool {
    let mut inside = false;
    let n = polygon.len();

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

fn bboxes_intersect(bbox1: &(f64, f64, f64, f64), bbox2: &(f64, f64, f64, f64)) -> bool {
    let (min_lon1, min_lat1, max_lon1, max_lat1) = bbox1;
    let (min_lon2, min_lat2, max_lon2, max_lat2) = bbox2;

    !(*max_lon1 < *min_lon2
        || *min_lon1 > *max_lon2
        || *max_lat1 < *min_lat2
        || *min_lat1 > *max_lat2)
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let a = (delta_lat / 2.0).sin().powi(2)
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    EARTH_RADIUS_M * c
}

fn spherical_polygon_area(ring: &[(f64, f64)]) -> f64 {
    if ring.len() < 4 {
        return 0.0;
    }

    let mut sum = 0.0;
    for i in 0..ring.len() - 1 {
        let (lon1, lat1) = ring[i];
        let (lon2, lat2) = ring[i + 1];

        let lon1_rad = lon1.to_radians();
        let lat1_rad = lat1.to_radians();
        let lon2_rad = lon2.to_radians();
        let lat2_rad = lat2.to_radians();

        sum += (lon2_rad - lon1_rad) * (2.0 + lat1_rad.sin() + lat2_rad.sin());
    }

    sum.abs() * EARTH_RADIUS_M * EARTH_RADIUS_M / 2.0
}

pub fn great_circle_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> Result<Value> {
    let distance = haversine_distance(lat1, lon1, lat2, lon2);
    Ok(Value::float64(distance))
}

pub fn geo_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> Result<Value> {
    let distance = vincenty_distance(lat1, lon1, lat2, lon2);
    Ok(Value::float64(distance))
}

fn vincenty_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lon = (lon2 - lon1).to_radians();

    let b = WGS84_A * (1.0 - WGS84_F);
    let u1 = ((1.0 - WGS84_F) * lat1_rad.tan()).atan();
    let u2 = ((1.0 - WGS84_F) * lat2_rad.tan()).atan();

    let sin_u1 = u1.sin();
    let cos_u1 = u1.cos();
    let sin_u2 = u2.sin();
    let cos_u2 = u2.cos();

    let mut lambda = delta_lon;
    let mut lambda_prev;
    let mut sin_sigma;
    let mut cos_sigma;
    let mut sigma;
    let mut sin_alpha;
    let mut cos2_alpha;
    let mut cos_2sigma_m;

    for _ in 0..100 {
        let sin_lambda = lambda.sin();
        let cos_lambda = lambda.cos();

        sin_sigma = ((cos_u2 * sin_lambda).powi(2)
            + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda).powi(2))
        .sqrt();

        if sin_sigma == 0.0 {
            return 0.0;
        }

        cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
        sigma = sin_sigma.atan2(cos_sigma);

        sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin_sigma;
        cos2_alpha = 1.0 - sin_alpha.powi(2);

        cos_2sigma_m = if cos2_alpha == 0.0 {
            0.0
        } else {
            cos_sigma - 2.0 * sin_u1 * sin_u2 / cos2_alpha
        };

        let c = WGS84_F / 16.0 * cos2_alpha * (4.0 + WGS84_F * (4.0 - 3.0 * cos2_alpha));

        lambda_prev = lambda;
        lambda = delta_lon
            + (1.0 - c)
                * WGS84_F
                * sin_alpha
                * (sigma
                    + c * sin_sigma
                        * (cos_2sigma_m + c * cos_sigma * (-1.0 + 2.0 * cos_2sigma_m.powi(2))));

        if (lambda - lambda_prev).abs() < 1e-12 {
            break;
        }
    }

    let sin_lambda = lambda.sin();
    let cos_lambda = lambda.cos();

    sin_sigma = ((cos_u2 * sin_lambda).powi(2)
        + (cos_u1 * sin_u2 - sin_u1 * cos_u2 * cos_lambda).powi(2))
    .sqrt();
    cos_sigma = sin_u1 * sin_u2 + cos_u1 * cos_u2 * cos_lambda;
    sigma = sin_sigma.atan2(cos_sigma);

    sin_alpha = cos_u1 * cos_u2 * sin_lambda / sin_sigma;
    cos2_alpha = 1.0 - sin_alpha.powi(2);

    cos_2sigma_m = if cos2_alpha == 0.0 {
        0.0
    } else {
        cos_sigma - 2.0 * sin_u1 * sin_u2 / cos2_alpha
    };

    let u_sq = cos2_alpha * (WGS84_A.powi(2) - b.powi(2)) / b.powi(2);
    let a_coef = 1.0 + u_sq / 16384.0 * (4096.0 + u_sq * (-768.0 + u_sq * (320.0 - 175.0 * u_sq)));
    let b_coef = u_sq / 1024.0 * (256.0 + u_sq * (-128.0 + u_sq * (74.0 - 47.0 * u_sq)));

    let delta_sigma = b_coef
        * sin_sigma
        * (cos_2sigma_m
            + b_coef / 4.0
                * (cos_sigma * (-1.0 + 2.0 * cos_2sigma_m.powi(2))
                    - b_coef / 6.0
                        * cos_2sigma_m
                        * (-3.0 + 4.0 * sin_sigma.powi(2))
                        * (-3.0 + 4.0 * cos_2sigma_m.powi(2))));

    b * a_coef * (sigma - delta_sigma)
}

pub fn point_in_ellipses_fn(x: f64, y: f64, ellipses: &[f64]) -> Result<Value> {
    if !ellipses.len().is_multiple_of(4) {
        return Err(Error::invalid_query(
            "pointInEllipses requires ellipse parameters in groups of 4: (x_center, y_center, a, b)".to_string(),
        ));
    }

    for chunk in ellipses.chunks(4) {
        let cx = chunk[0];
        let cy = chunk[1];
        let a = chunk[2];
        let b = chunk[3];

        let dx = x - cx;
        let dy = y - cy;

        if (dx * dx) / (a * a) + (dy * dy) / (b * b) <= 1.0 {
            return Ok(Value::bool_val(true));
        }
    }

    Ok(Value::bool_val(false))
}

pub fn point_in_polygon_fn(x: f64, y: f64, polygon: &[(f64, f64)]) -> Result<Value> {
    let inside = point_in_polygon(x, y, polygon);
    Ok(Value::bool_val(inside))
}

pub fn geohash_encode(lat: f64, lon: f64, precision: u8) -> Result<Value> {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

    let precision = precision.min(12) as usize;
    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;
    let mut min_lon = -180.0_f64;
    let mut max_lon = 180.0_f64;

    let mut hash = String::with_capacity(precision);
    let mut bit = 0;
    let mut ch = 0u8;
    let mut is_lon = true;

    while hash.len() < precision {
        if is_lon {
            let mid = (min_lon + max_lon) / 2.0;
            if lon >= mid {
                ch |= 1 << (4 - bit);
                min_lon = mid;
            } else {
                max_lon = mid;
            }
        } else {
            let mid = (min_lat + max_lat) / 2.0;
            if lat >= mid {
                ch |= 1 << (4 - bit);
                min_lat = mid;
            } else {
                max_lat = mid;
            }
        }

        is_lon = !is_lon;
        bit += 1;

        if bit == 5 {
            hash.push(BASE32[ch as usize] as char);
            bit = 0;
            ch = 0;
        }
    }

    Ok(Value::string(hash))
}

pub fn geohash_decode(hash: &str) -> Result<Value> {
    const BASE32: &str = "0123456789bcdefghjkmnpqrstuvwxyz";

    let mut min_lat = -90.0_f64;
    let mut max_lat = 90.0_f64;
    let mut min_lon = -180.0_f64;
    let mut max_lon = 180.0_f64;

    let mut is_lon = true;

    for c in hash.chars() {
        let idx = BASE32
            .find(c.to_ascii_lowercase())
            .ok_or_else(|| Error::invalid_query(format!("Invalid geohash character: '{}'", c)))?;

        for i in (0..5).rev() {
            let bit = (idx >> i) & 1;

            if is_lon {
                let mid = (min_lon + max_lon) / 2.0;
                if bit == 1 {
                    min_lon = mid;
                } else {
                    max_lon = mid;
                }
            } else {
                let mid = (min_lat + max_lat) / 2.0;
                if bit == 1 {
                    min_lat = mid;
                } else {
                    max_lat = mid;
                }
            }
            is_lon = !is_lon;
        }
    }

    let lat = (min_lat + max_lat) / 2.0;
    let lon = (min_lon + max_lon) / 2.0;

    let mut result = indexmap::IndexMap::new();
    result.insert("latitude".to_string(), Value::float64(lat));
    result.insert("longitude".to_string(), Value::float64(lon));
    Ok(Value::struct_val(result))
}

pub fn geohashes_in_box(
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
    precision: u8,
) -> Result<Value> {
    let precision = precision.min(6) as usize;

    let mut hashes = Vec::new();
    let step_lat = 180.0 / (1 << (precision * 5 / 2)) as f64;
    let step_lon = 360.0 / (1 << (precision * 5).div_ceil(2)) as f64;

    let mut lat = min_lat;
    while lat <= max_lat {
        let mut lon = min_lon;
        while lon <= max_lon {
            if let Ok(hash) = geohash_encode(lat, lon, precision as u8) {
                if let Some(s) = hash.as_str() {
                    if !hashes.contains(&s.to_string()) {
                        hashes.push(s.to_string());
                    }
                }
            }
            lon += step_lon;
        }
        lat += step_lat;
    }

    let values: Vec<Value> = hashes.into_iter().map(Value::string).collect();
    Ok(Value::array(values))
}

pub fn h3_is_valid(h3_index: u64) -> Result<Value> {
    let mode = (h3_index >> 59) & 0xF;
    let resolution = (h3_index >> 52) & 0xF;

    let is_valid = mode == 1 && resolution <= 15;
    Ok(Value::bool_val(is_valid))
}

pub fn h3_get_resolution(h3_index: u64) -> Result<Value> {
    let resolution = (h3_index >> 52) & 0xF;
    Ok(Value::int64(resolution as i64))
}

pub fn h3_edge_length_m(resolution: i64) -> Result<Value> {
    let lengths = [
        1107712.591,
        418676.0055,
        158244.6558,
        59810.8572,
        22606.3794,
        8544.4089,
        3229.4827,
        1220.6299,
        461.3541,
        174.3752,
        65.9077,
        24.9106,
        9.4153,
        3.5586,
        1.3448,
        0.5082,
    ];

    if resolution < 0 || resolution > 15 {
        return Err(Error::invalid_query(format!(
            "H3 resolution must be between 0 and 15, got {}",
            resolution
        )));
    }

    Ok(Value::float64(lengths[resolution as usize]))
}

pub fn h3_edge_angle(resolution: i64) -> Result<Value> {
    let angles = [
        0.0005272381,
        0.0001400989,
        0.0000373268,
        0.0000099458,
        0.0000026497,
        0.0000007059,
        0.0000001880,
        0.0000000501,
        0.0000000133,
        0.0000000035,
        0.0000000009,
        0.0000000002,
        0.0000000001,
        0.0000000000,
        0.0000000000,
        0.0000000000,
    ];

    if resolution < 0 || resolution > 15 {
        return Err(Error::invalid_query(format!(
            "H3 resolution must be between 0 and 15, got {}",
            resolution
        )));
    }

    Ok(Value::float64(angles[resolution as usize]))
}

pub fn h3_hex_area_km2(resolution: i64) -> Result<Value> {
    let areas = [
        4250546.8477,
        607220.9782,
        86745.8540,
        12392.2648,
        1770.3236,
        252.9033,
        36.1290,
        5.1612,
        0.7373,
        0.1053,
        0.0150,
        0.0021,
        0.0003,
        0.00004,
        0.000006,
        0.0000009,
    ];

    if resolution < 0 || resolution > 15 {
        return Err(Error::invalid_query(format!(
            "H3 resolution must be between 0 and 15, got {}",
            resolution
        )));
    }

    Ok(Value::float64(areas[resolution as usize]))
}

pub fn h3_to_geo(h3_index: u64) -> Result<Value> {
    let base_cell = (h3_index >> 45) & 0x7F;
    let resolution = ((h3_index >> 52) & 0xF) as usize;

    let base_lat = ((base_cell as f64) / 122.0 * 180.0 - 90.0).to_radians();
    let base_lon = ((base_cell as f64) % 122.0 / 122.0 * 360.0 - 180.0).to_radians();

    let lat = base_lat + (h3_index & 0xFFF) as f64 / (7_i64.pow(resolution as u32) as f64) * 0.001;
    let lon = base_lon
        + ((h3_index >> 12) & 0xFFF) as f64 / (7_i64.pow(resolution as u32) as f64) * 0.001;

    let mut result = indexmap::IndexMap::new();
    result.insert("latitude".to_string(), Value::float64(lat.to_degrees()));
    result.insert("longitude".to_string(), Value::float64(lon.to_degrees()));
    Ok(Value::struct_val(result))
}

pub fn h3_to_geo_boundary(h3_index: u64) -> Result<Value> {
    let center = h3_to_geo(h3_index)?;
    let (lat, lon) = if let Some(s) = center.as_struct() {
        let lat = s.get("latitude").and_then(|v| v.as_f64()).unwrap_or(0.0);
        let lon = s.get("longitude").and_then(|v| v.as_f64()).unwrap_or(0.0);
        (lat, lon)
    } else {
        (0.0, 0.0)
    };

    let resolution = ((h3_index >> 52) & 0xF) as usize;
    let edge_len = match h3_edge_length_m(resolution as i64) {
        Ok(v) => v.as_f64().unwrap_or(1000.0) / 111320.0,
        Err(_) => 0.01,
    };

    let mut boundary = Vec::new();
    for i in 0..6 {
        let angle = std::f64::consts::PI / 3.0 * i as f64;
        let pt_lat = lat + edge_len * angle.cos();
        let pt_lon = lon + edge_len * angle.sin() / lat.to_radians().cos().max(0.01);

        let mut point = indexmap::IndexMap::new();
        point.insert("latitude".to_string(), Value::float64(pt_lat));
        point.insert("longitude".to_string(), Value::float64(pt_lon));
        boundary.push(Value::struct_val(point));
    }

    Ok(Value::array(boundary))
}

pub fn geo_to_h3(lat: f64, lon: f64, resolution: i64) -> Result<Value> {
    if resolution < 0 || resolution > 15 {
        return Err(Error::invalid_query(format!(
            "H3 resolution must be between 0 and 15, got {}",
            resolution
        )));
    }

    let lat_rad = lat.to_radians();
    let lon_rad = lon.to_radians();

    let base_cell = (((lat + 90.0) / 180.0 * 122.0) as u64).min(121);
    let mode: u64 = 1;
    let res = resolution as u64;

    let lat_idx = ((lat_rad.abs() * 7_f64.powi(resolution as i32) * 1000.0) as u64) & 0xFFF;
    let lon_idx = ((lon_rad.abs() * 7_f64.powi(resolution as i32) * 1000.0) as u64) & 0xFFF;

    let h3_index = (mode << 59) | (res << 52) | (base_cell << 45) | (lon_idx << 12) | lat_idx;

    Ok(Value::int64(h3_index as i64))
}

pub fn h3_k_ring(h3_index: u64, k: i64) -> Result<Value> {
    if k < 0 {
        return Err(Error::invalid_query(format!(
            "k must be non-negative, got {}",
            k
        )));
    }

    let mut ring = vec![Value::int64(h3_index as i64)];

    for i in 1..=k {
        for j in 0..6 {
            let neighbor = h3_index.wrapping_add((i as u64) * 7 + j);
            ring.push(Value::int64(neighbor as i64));
        }
    }

    Ok(Value::array(ring))
}

pub fn h3_get_base_cell(h3_index: u64) -> Result<Value> {
    let base_cell = (h3_index >> 45) & 0x7F;
    Ok(Value::int64(base_cell as i64))
}

pub fn h3_is_pentagon(h3_index: u64) -> Result<Value> {
    let base_cell = (h3_index >> 45) & 0x7F;
    let pentagon_cells = [4, 14, 24, 38, 49, 58, 63, 72, 83, 97, 107, 117];
    Ok(Value::bool_val(
        pentagon_cells.contains(&(base_cell as i32)),
    ))
}

pub fn h3_is_res_class_iii(h3_index: u64) -> Result<Value> {
    let resolution = (h3_index >> 52) & 0xF;
    Ok(Value::bool_val(resolution % 2 == 1))
}

pub fn h3_get_faces(h3_index: u64) -> Result<Value> {
    let base_cell = ((h3_index >> 45) & 0x7F) as i64;
    let face = base_cell % 20;
    Ok(Value::array(vec![Value::int64(face)]))
}

pub fn h3_cell_area_m2(h3_index: u64) -> Result<Value> {
    let resolution = (h3_index >> 52) & 0xF;
    let areas_m2 = [
        4.250_546_847_7e12,
        6.072_209_782e11,
        8.674_585_4e10,
        1.239_226_48e10,
        1.770_323_6e9,
        2.529_033e8,
        3.612_9e7,
        5.161_2e6,
        7.373e5,
        1.053e5,
        1.5e4,
        2.1e3,
        3e2,
        4e1,
        6.0,
        9e-1,
    ];

    if resolution > 15 {
        return Err(Error::invalid_query(format!(
            "Invalid H3 resolution: {}",
            resolution
        )));
    }

    Ok(Value::float64(areas_m2[resolution as usize]))
}

pub fn h3_cell_area_rads2(h3_index: u64) -> Result<Value> {
    let area_m2 = h3_cell_area_m2(h3_index)?;
    let area = area_m2.as_f64().unwrap_or(0.0);
    let area_rads2 = area / (EARTH_RADIUS_M * EARTH_RADIUS_M);
    Ok(Value::float64(area_rads2))
}

pub fn h3_to_parent(h3_index: u64, parent_resolution: i64) -> Result<Value> {
    let current_resolution = ((h3_index >> 52) & 0xF) as i64;

    if parent_resolution < 0 || parent_resolution > 15 {
        return Err(Error::invalid_query(format!(
            "Parent resolution must be between 0 and 15, got {}",
            parent_resolution
        )));
    }

    if parent_resolution > current_resolution {
        return Err(Error::invalid_query(format!(
            "Parent resolution {} must be <= current resolution {}",
            parent_resolution, current_resolution
        )));
    }

    let mask = !((1u64 << (3 * (15 - parent_resolution as u64))) - 1);
    let parent = (h3_index & mask) | ((parent_resolution as u64) << 52);

    Ok(Value::int64(parent as i64))
}

pub fn h3_to_children(h3_index: u64, child_resolution: i64) -> Result<Value> {
    let current_resolution = ((h3_index >> 52) & 0xF) as i64;

    if child_resolution < 0 || child_resolution > 15 {
        return Err(Error::invalid_query(format!(
            "Child resolution must be between 0 and 15, got {}",
            child_resolution
        )));
    }

    if child_resolution < current_resolution {
        return Err(Error::invalid_query(format!(
            "Child resolution {} must be >= current resolution {}",
            child_resolution, current_resolution
        )));
    }

    let diff = child_resolution - current_resolution;
    let num_children = 7_i64.pow(diff as u32);

    let mut children = Vec::new();
    for i in 0..num_children {
        let child = (h3_index & !(0xFu64 << 52)) | ((child_resolution as u64) << 52) | (i as u64);
        children.push(Value::int64(child as i64));
    }

    Ok(Value::array(children))
}

pub fn h3_distance(h3_index1: u64, h3_index2: u64) -> Result<Value> {
    let res1 = (h3_index1 >> 52) & 0xF;
    let res2 = (h3_index2 >> 52) & 0xF;

    if res1 != res2 {
        return Err(Error::invalid_query(
            "H3 indices must have the same resolution for distance calculation".to_string(),
        ));
    }

    let base1 = (h3_index1 >> 45) & 0x7F;
    let base2 = (h3_index2 >> 45) & 0x7F;

    let distance = (base1 as i64 - base2 as i64).abs()
        + ((h3_index1 & 0xFFF) as i64 - (h3_index2 & 0xFFF) as i64).abs();

    Ok(Value::int64(distance))
}

pub fn h3_line(h3_index1: u64, h3_index2: u64) -> Result<Value> {
    let res1 = (h3_index1 >> 52) & 0xF;
    let res2 = (h3_index2 >> 52) & 0xF;

    if res1 != res2 {
        return Err(Error::invalid_query(
            "H3 indices must have the same resolution for line calculation".to_string(),
        ));
    }

    let dist = h3_distance(h3_index1, h3_index2)?;
    let distance = dist.as_i64().unwrap_or(0);

    let mut line = vec![Value::int64(h3_index1 as i64)];

    if distance > 0 {
        for i in 1..distance {
            let interp = h3_index1 + ((h3_index2 - h3_index1) / distance as u64) * i as u64;
            line.push(Value::int64(interp as i64));
        }
        line.push(Value::int64(h3_index2 as i64));
    }

    Ok(Value::array(line))
}

pub fn s2_cell_id_to_long_lat(s2_cell_id: u64) -> Result<Value> {
    let face = (s2_cell_id >> 61) & 0x7;
    let pos = s2_cell_id & 0x1FFFFFFFFFFFFFFF;

    let u = (pos as f64 / (1u64 << 61) as f64) * 2.0 - 1.0;
    let v = ((pos >> 30) as f64 / (1u64 << 31) as f64) * 2.0 - 1.0;

    let (x, y, z) = match face {
        0 => (1.0, u, v),
        1 => (-u, 1.0, v),
        2 => (-u, -v, 1.0),
        3 => (-1.0, -v, -u),
        4 => (v, -1.0, -u),
        5 => (v, u, -1.0),
        _ => (1.0, 0.0, 0.0),
    };

    let lon = y.atan2(x).to_degrees();
    let lat = z.atan2((x * x + y * y).sqrt()).to_degrees();

    let mut result = indexmap::IndexMap::new();
    result.insert("longitude".to_string(), Value::float64(lon));
    result.insert("latitude".to_string(), Value::float64(lat));
    Ok(Value::struct_val(result))
}

pub fn long_lat_to_s2_cell_id(lon: f64, lat: f64, level: i64) -> Result<Value> {
    if level < 0 || level > 30 {
        return Err(Error::invalid_query(format!(
            "S2 level must be between 0 and 30, got {}",
            level
        )));
    }

    let lat_rad = lat.to_radians();
    let lon_rad = lon.to_radians();

    let x = lat_rad.cos() * lon_rad.cos();
    let y = lat_rad.cos() * lon_rad.sin();
    let z = lat_rad.sin();

    let face = if x.abs() >= y.abs() && x.abs() >= z.abs() {
        if x >= 0.0 { 0 } else { 3 }
    } else if y.abs() >= z.abs() {
        if y >= 0.0 { 1 } else { 4 }
    } else if z >= 0.0 {
        2
    } else {
        5
    };

    let (u, v) = match face {
        0 => (y / x, z / x),
        1 => (-x / y, z / y),
        2 => (-x / z, -y / z),
        3 => (z / x, y / x),
        4 => (z / y, -x / y),
        5 => (-y / z, -x / z),
        _ => (0.0, 0.0),
    };

    let s = ((u + 1.0) / 2.0 * (1u64 << 30) as f64) as u64;
    let t = ((v + 1.0) / 2.0 * (1u64 << 30) as f64) as u64;

    let cell_id = ((face as u64) << 61) | (s << 31) | t;

    Ok(Value::int64(cell_id as i64))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_wkt_point() {
        let geom = parse_wkt("POINT(1 2)").unwrap();
        assert_eq!(geom, Geometry::Point { lon: 1.0, lat: 2.0 });
    }

    #[test]
    fn test_parse_wkt_point_with_space() {
        let geom = parse_wkt("POINT (1 2)").unwrap();
        assert_eq!(geom, Geometry::Point { lon: 1.0, lat: 2.0 });
    }

    #[test]
    fn test_parse_wkt_linestring() {
        let geom = parse_wkt("LINESTRING(0 0, 1 1, 2 2)").unwrap();
        assert_eq!(
            geom,
            Geometry::LineString {
                points: vec![(0.0, 0.0), (1.0, 1.0), (2.0, 2.0)]
            }
        );
    }

    #[test]
    fn test_parse_wkt_polygon_closed() {
        let geom = parse_wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))").unwrap();
        assert_eq!(
            geom,
            Geometry::Polygon {
                rings: vec![vec![
                    (0.0, 0.0),
                    (10.0, 0.0),
                    (10.0, 10.0),
                    (0.0, 10.0),
                    (0.0, 0.0)
                ]]
            }
        );
    }

    #[test]
    fn test_parse_wkt_polygon_unclosed() {
        let result = parse_wkt("POLYGON((0 0, 10 0, 10 10, 0 10, 1 1))");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("not closed"));
    }

    #[test]
    fn test_validate_longitude_out_of_range() {
        let result = parse_wkt("POINT(200 0)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Longitude"));
    }

    #[test]
    fn test_validate_latitude_out_of_range() {
        let result = parse_wkt("POINT(0 100)");
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Latitude"));
    }

    #[test]
    fn test_geometry_to_wkt() {
        let geom = Geometry::Point { lon: 1.0, lat: 2.0 };
        assert_eq!(geom.to_wkt(), "POINT(1 2)");

        let geom = Geometry::LineString {
            points: vec![(0.0, 0.0), (1.0, 1.0)],
        };
        assert_eq!(geom.to_wkt(), "LINESTRING(0 0, 1 1)");

        let geom = Geometry::Polygon {
            rings: vec![vec![
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]],
        };
        assert_eq!(geom.to_wkt(), "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))");
    }
}
