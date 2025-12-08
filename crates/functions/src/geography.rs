use yachtsql_core::error::{Error, Result};

const EARTH_RADIUS_M: f64 = 6_371_008.8;

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
