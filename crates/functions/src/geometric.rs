use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{PgCircle, PgLseg, PgPoint, PgPolygon, Value};

pub fn point_constructor(x: &Value, y: &Value) -> Result<Value> {
    if x.is_null() || y.is_null() {
        return Ok(Value::null());
    }

    let x_val = parse_float_arg(x)?;
    let y_val = parse_float_arg(y)?;

    Ok(Value::point(PgPoint::new(x_val, y_val)))
}

fn parse_float_arg(val: &Value) -> Result<f64> {
    if let Some(f) = val.as_f64() {
        return Ok(f);
    }
    if let Some(i) = val.as_i64() {
        return Ok(i as f64);
    }
    if let Some(s) = val.as_str() {
        let s_lower = s.to_lowercase();
        if s_lower == "infinity" || s_lower == "inf" {
            return Ok(f64::INFINITY);
        }
        if s_lower == "-infinity" || s_lower == "-inf" {
            return Ok(f64::NEG_INFINITY);
        }
        if s_lower == "nan" {
            return Ok(f64::NAN);
        }

        if let Ok(f) = s.parse::<f64>() {
            return Ok(f);
        }
    }
    Err(Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: val.data_type().to_string(),
    })
}

pub fn circle_constructor(center: &Value, radius: &Value) -> Result<Value> {
    if center.is_null() || radius.is_null() {
        return Ok(Value::null());
    }

    let center_point = center.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: center.data_type().to_string(),
    })?;

    let radius_val = radius.as_f64().ok_or_else(|| Error::TypeMismatch {
        expected: "FLOAT64".to_string(),
        actual: radius.data_type().to_string(),
    })?;

    Ok(Value::circle(PgCircle::new(
        center_point.clone(),
        radius_val,
    )))
}

pub fn area(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    if let Some(c) = geom.as_circle() {
        return Ok(Value::float64(c.area()));
    }

    if let Some(p) = geom.as_polygon() {
        return Ok(Value::float64(p.area()));
    }

    Err(Error::TypeMismatch {
        expected: "CIRCLE or POLYGON".to_string(),
        actual: geom.data_type().to_string(),
    })
}

pub fn center(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    if let Some(c) = geom.as_circle() {
        return Ok(Value::point(c.center.clone()));
    }

    Err(Error::TypeMismatch {
        expected: "CIRCLE".to_string(),
        actual: geom.data_type().to_string(),
    })
}

pub fn diameter(circle: &Value) -> Result<Value> {
    if circle.is_null() {
        return Ok(Value::null());
    }

    let c = circle.as_circle().ok_or_else(|| Error::TypeMismatch {
        expected: "CIRCLE".to_string(),
        actual: circle.data_type().to_string(),
    })?;

    Ok(Value::float64(c.diameter()))
}

pub fn radius(circle: &Value) -> Result<Value> {
    if circle.is_null() {
        return Ok(Value::null());
    }

    let c = circle.as_circle().ok_or_else(|| Error::TypeMismatch {
        expected: "CIRCLE".to_string(),
        actual: circle.data_type().to_string(),
    })?;

    Ok(Value::float64(c.radius))
}

pub fn distance(geom1: &Value, geom2: &Value) -> Result<Value> {
    if geom1.is_null() || geom2.is_null() {
        return Ok(Value::null());
    }

    if let (Some(p1), Some(p2)) = (geom1.as_point(), geom2.as_point()) {
        return Ok(Value::float64(p1.distance(p2)));
    }

    if let (Some(p), Some(c)) = (geom1.as_point(), geom2.as_circle()) {
        return Ok(Value::float64(c.distance_to_point(p)));
    }

    if let (Some(c), Some(p)) = (geom1.as_circle(), geom2.as_point()) {
        return Ok(Value::float64(c.distance_to_point(p)));
    }

    if let (Some(c1), Some(c2)) = (geom1.as_circle(), geom2.as_circle()) {
        return Ok(Value::float64(c1.distance_to_circle(c2)));
    }

    Err(Error::invalid_query(format!(
        "Cannot compute distance between {} and {}",
        geom1.data_type(),
        geom2.data_type()
    )))
}

pub fn contains(container: &Value, contained: &Value) -> Result<Value> {
    if container.is_null() || contained.is_null() {
        return Ok(Value::null());
    }

    if let (Some(c), Some(p)) = (container.as_circle(), contained.as_point()) {
        return Ok(Value::bool_val(c.contains_point(p)));
    }

    if let (Some(poly), Some(p)) = (container.as_polygon(), contained.as_point()) {
        return Ok(Value::bool_val(poly.contains_point(p)));
    }

    Err(Error::invalid_query(format!(
        "Cannot check containment for {} @> {}",
        container.data_type(),
        contained.data_type()
    )))
}

pub fn contained_by(contained: &Value, container: &Value) -> Result<Value> {
    if container.is_null() || contained.is_null() {
        return Ok(Value::null());
    }

    if let (Some(p), Some(c)) = (contained.as_point(), container.as_circle()) {
        return Ok(Value::bool_val(c.contains_point(p)));
    }

    if let (Some(p), Some(poly)) = (contained.as_point(), container.as_polygon()) {
        return Ok(Value::bool_val(poly.contains_point(p)));
    }

    Err(Error::invalid_query(format!(
        "Cannot check containment for {} <@ {}",
        contained.data_type(),
        container.data_type()
    )))
}

pub fn overlaps(geom1: &Value, geom2: &Value) -> Result<Value> {
    if geom1.is_null() || geom2.is_null() {
        return Ok(Value::null());
    }

    if let (Some(c1), Some(c2)) = (geom1.as_circle(), geom2.as_circle()) {
        return Ok(Value::bool_val(c1.overlaps(c2)));
    }

    Err(Error::invalid_query(format!(
        "Cannot check overlap for {} && {}",
        geom1.data_type(),
        geom2.data_type()
    )))
}

pub fn point_add(p1: &Value, p2: &Value) -> Result<Value> {
    if p1.is_null() || p2.is_null() {
        return Ok(Value::null());
    }

    let point1 = p1.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p1.data_type().to_string(),
    })?;

    let point2 = p2.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p2.data_type().to_string(),
    })?;

    Ok(Value::point(PgPoint::new(
        point1.x + point2.x,
        point1.y + point2.y,
    )))
}

pub fn point_subtract(p1: &Value, p2: &Value) -> Result<Value> {
    if p1.is_null() || p2.is_null() {
        return Ok(Value::null());
    }

    let point1 = p1.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p1.data_type().to_string(),
    })?;

    let point2 = p2.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p2.data_type().to_string(),
    })?;

    Ok(Value::point(PgPoint::new(
        point1.x - point2.x,
        point1.y - point2.y,
    )))
}

pub fn point_multiply(p1: &Value, p2: &Value) -> Result<Value> {
    if p1.is_null() || p2.is_null() {
        return Ok(Value::null());
    }

    let point1 = p1.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p1.data_type().to_string(),
    })?;

    let point2 = p2.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p2.data_type().to_string(),
    })?;

    let x = point1.x * point2.x - point1.y * point2.y;
    let y = point1.x * point2.y + point1.y * point2.x;

    Ok(Value::point(PgPoint::new(x, y)))
}

pub fn point_divide(p1: &Value, p2: &Value) -> Result<Value> {
    if p1.is_null() || p2.is_null() {
        return Ok(Value::null());
    }

    let point1 = p1.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p1.data_type().to_string(),
    })?;

    let point2 = p2.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p2.data_type().to_string(),
    })?;

    let denom = point2.x * point2.x + point2.y * point2.y;
    if denom == 0.0 {
        return Err(Error::ExecutionError("Division by zero point".to_string()));
    }

    let x = (point1.x * point2.x + point1.y * point2.y) / denom;
    let y = (point1.y * point2.x - point1.x * point2.y) / denom;

    Ok(Value::point(PgPoint::new(x, y)))
}

pub fn lseg_constructor(p1: &Value, p2: &Value) -> Result<Value> {
    if p1.is_null() || p2.is_null() {
        return Ok(Value::null());
    }

    let point1 = p1.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p1.data_type().to_string(),
    })?;

    let point2 = p2.as_point().ok_or_else(|| Error::TypeMismatch {
        expected: "POINT".to_string(),
        actual: p2.data_type().to_string(),
    })?;

    Ok(Value::lseg(PgLseg::new(point1.clone(), point2.clone())))
}

pub fn length(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    if let Some(lseg) = geom.as_lseg() {
        return Ok(Value::float64(lseg.length()));
    }

    if let Some(path) = geom.as_path() {
        return Ok(Value::float64(path.length()));
    }

    Err(Error::TypeMismatch {
        expected: "LSEG or PATH".to_string(),
        actual: geom.data_type().to_string(),
    })
}

pub fn npoints(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    if let Some(path) = geom.as_path() {
        return Ok(Value::int64(path.npoints()));
    }

    if let Some(polygon) = geom.as_polygon() {
        return Ok(Value::int64(polygon.npoints()));
    }

    Err(Error::TypeMismatch {
        expected: "PATH or POLYGON".to_string(),
        actual: geom.data_type().to_string(),
    })
}

pub fn isclosed(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let path = geom.as_path().ok_or_else(|| Error::TypeMismatch {
        expected: "PATH".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    Ok(Value::bool_val(path.is_closed()))
}

pub fn isopen(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let path = geom.as_path().ok_or_else(|| Error::TypeMismatch {
        expected: "PATH".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    Ok(Value::bool_val(path.is_open()))
}

pub fn popen(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let path = geom.as_path().ok_or_else(|| Error::TypeMismatch {
        expected: "PATH".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    Ok(Value::path(path.popen()))
}

pub fn pclose(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let path = geom.as_path().ok_or_else(|| Error::TypeMismatch {
        expected: "PATH".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    Ok(Value::path(path.pclose()))
}

pub fn is_parallel(lseg1: &Value, lseg2: &Value) -> Result<Value> {
    if lseg1.is_null() || lseg2.is_null() {
        return Ok(Value::null());
    }

    let l1 = lseg1.as_lseg().ok_or_else(|| Error::TypeMismatch {
        expected: "LSEG".to_string(),
        actual: lseg1.data_type().to_string(),
    })?;

    let l2 = lseg2.as_lseg().ok_or_else(|| Error::TypeMismatch {
        expected: "LSEG".to_string(),
        actual: lseg2.data_type().to_string(),
    })?;

    Ok(Value::bool_val(l1.is_parallel(l2)))
}

pub fn is_perpendicular(lseg1: &Value, lseg2: &Value) -> Result<Value> {
    if lseg1.is_null() || lseg2.is_null() {
        return Ok(Value::null());
    }

    let l1 = lseg1.as_lseg().ok_or_else(|| Error::TypeMismatch {
        expected: "LSEG".to_string(),
        actual: lseg1.data_type().to_string(),
    })?;

    let l2 = lseg2.as_lseg().ok_or_else(|| Error::TypeMismatch {
        expected: "LSEG".to_string(),
        actual: lseg2.data_type().to_string(),
    })?;

    Ok(Value::bool_val(l1.is_perpendicular(l2)))
}

pub fn intersects(lseg1: &Value, lseg2: &Value) -> Result<Value> {
    if lseg1.is_null() || lseg2.is_null() {
        return Ok(Value::null());
    }

    let l1 = lseg1.as_lseg().ok_or_else(|| Error::TypeMismatch {
        expected: "LSEG".to_string(),
        actual: lseg1.data_type().to_string(),
    })?;

    let l2 = lseg2.as_lseg().ok_or_else(|| Error::TypeMismatch {
        expected: "LSEG".to_string(),
        actual: lseg2.data_type().to_string(),
    })?;

    Ok(Value::bool_val(l1.intersects(l2)))
}

pub fn circle_to_polygon(npts: &Value, geom: &Value) -> Result<Value> {
    if geom.is_null() || npts.is_null() {
        return Ok(Value::null());
    }

    let n = npts.as_i64().ok_or_else(|| Error::TypeMismatch {
        expected: "INTEGER".to_string(),
        actual: npts.data_type().to_string(),
    })? as usize;

    if n < 3 {
        return Err(Error::InvalidQuery(
            "POLYGON requires at least 3 points".to_string(),
        ));
    }

    let c = geom.as_circle().ok_or_else(|| Error::TypeMismatch {
        expected: "CIRCLE".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    let mut points = Vec::with_capacity(n);
    for i in 0..n {
        let angle = 2.0 * std::f64::consts::PI * (i as f64) / (n as f64);
        let x = c.center.x + c.radius * angle.cos();
        let y = c.center.y + c.radius * angle.sin();
        points.push(PgPoint::new(x, y));
    }
    Ok(Value::polygon(PgPolygon::new(points)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point_constructor() {
        let x = Value::float64(3.0);
        let y = Value::float64(4.0);
        let point = point_constructor(&x, &y).unwrap();
        let p = point.as_point().unwrap();
        assert_eq!(p.x, 3.0);
        assert_eq!(p.y, 4.0);
    }

    #[test]
    fn test_point_constructor_null() {
        let x = Value::null();
        let y = Value::float64(4.0);
        let point = point_constructor(&x, &y).unwrap();
        assert!(point.is_null());
    }

    #[test]
    fn test_circle_constructor() {
        let center = Value::point(PgPoint::new(0.0, 0.0));
        let radius = Value::float64(5.0);
        let c = circle_constructor(&center, &radius).unwrap();
        let circle = c.as_circle().unwrap();
        assert_eq!(circle.radius, 5.0);
    }

    #[test]
    fn test_area_circle() {
        let c = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 1.0));
        let a = area(&c).unwrap();
        assert!((a.as_f64().unwrap() - std::f64::consts::PI).abs() < 0.0001);
    }

    #[test]
    fn test_center_circle() {
        let c = Value::circle(PgCircle::new(PgPoint::new(3.0, 4.0), 5.0));
        let center_point = center(&c).unwrap();
        let point = center_point.as_point().unwrap();
        assert_eq!(point.x, 3.0);
        assert_eq!(point.y, 4.0);
    }

    #[test]
    fn test_diameter() {
        let c = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 7.0));
        let d = diameter(&c).unwrap();
        assert_eq!(d.as_f64().unwrap(), 14.0);
    }

    #[test]
    fn test_radius() {
        let c = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 3.5));
        let r = radius(&c).unwrap();
        assert_eq!(r.as_f64().unwrap(), 3.5);
    }

    #[test]
    fn test_distance_point_to_point() {
        let p1 = Value::point(PgPoint::new(0.0, 0.0));
        let p2 = Value::point(PgPoint::new(3.0, 4.0));
        let d = distance(&p1, &p2).unwrap();
        assert_eq!(d.as_f64().unwrap(), 5.0);
    }

    #[test]
    fn test_distance_point_to_circle() {
        let p = Value::point(PgPoint::new(10.0, 0.0));
        let c = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 3.0));
        let d = distance(&p, &c).unwrap();
        assert_eq!(d.as_f64().unwrap(), 7.0);
    }

    #[test]
    fn test_distance_circle_to_circle() {
        let c1 = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 2.0));
        let c2 = Value::circle(PgCircle::new(PgPoint::new(10.0, 0.0), 3.0));
        let d = distance(&c1, &c2).unwrap();
        assert_eq!(d.as_f64().unwrap(), 5.0);
    }

    #[test]
    fn test_contains_circle_point() {
        let c = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 10.0));
        let p_inside = Value::point(PgPoint::new(3.0, 4.0));
        let p_outside = Value::point(PgPoint::new(8.0, 8.0));

        assert!(contains(&c, &p_inside).unwrap().as_bool().unwrap());
        assert!(!contains(&c, &p_outside).unwrap().as_bool().unwrap());
    }

    #[test]
    fn test_overlaps_circles() {
        let c1 = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 5.0));
        let c2 = Value::circle(PgCircle::new(PgPoint::new(8.0, 0.0), 5.0));
        let c3 = Value::circle(PgCircle::new(PgPoint::new(20.0, 0.0), 5.0));

        assert!(overlaps(&c1, &c2).unwrap().as_bool().unwrap());
        assert!(!overlaps(&c1, &c3).unwrap().as_bool().unwrap());
    }
}
