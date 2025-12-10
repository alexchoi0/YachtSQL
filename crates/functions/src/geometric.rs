use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::{PgBox, PgCircle, PgLseg, PgPoint, PgPolygon, Value};

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

pub fn box_constructor(p1: &Value, p2: &Value) -> Result<Value> {
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

    Ok(Value::pgbox(PgBox::new(point1.clone(), point2.clone())))
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

    if let Some(b) = geom.as_pgbox() {
        return Ok(Value::float64(b.area()));
    }

    if let Some(c) = geom.as_circle() {
        return Ok(Value::float64(c.area()));
    }

    if let Some(p) = geom.as_polygon() {
        return Ok(Value::float64(p.area()));
    }

    Err(Error::TypeMismatch {
        expected: "BOX, CIRCLE, or POLYGON".to_string(),
        actual: geom.data_type().to_string(),
    })
}

pub fn center(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    if let Some(b) = geom.as_pgbox() {
        return Ok(Value::point(b.center()));
    }

    if let Some(c) = geom.as_circle() {
        return Ok(Value::point(c.center.clone()));
    }

    Err(Error::TypeMismatch {
        expected: "BOX or CIRCLE".to_string(),
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

pub fn width(box_val: &Value) -> Result<Value> {
    if box_val.is_null() {
        return Ok(Value::null());
    }

    let b = box_val.as_pgbox().ok_or_else(|| Error::TypeMismatch {
        expected: "BOX".to_string(),
        actual: box_val.data_type().to_string(),
    })?;

    Ok(Value::float64(b.width()))
}

pub fn height(box_val: &Value) -> Result<Value> {
    if box_val.is_null() {
        return Ok(Value::null());
    }

    let b = box_val.as_pgbox().ok_or_else(|| Error::TypeMismatch {
        expected: "BOX".to_string(),
        actual: box_val.data_type().to_string(),
    })?;

    Ok(Value::float64(b.height()))
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

    if let (Some(b), Some(p)) = (container.as_pgbox(), contained.as_point()) {
        return Ok(Value::bool_val(b.contains_point(p)));
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

    if let (Some(p), Some(b)) = (contained.as_point(), container.as_pgbox()) {
        return Ok(Value::bool_val(b.contains_point(p)));
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

    if let (Some(b1), Some(b2)) = (geom1.as_pgbox(), geom2.as_pgbox()) {
        return Ok(Value::bool_val(b1.overlaps(b2)));
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

pub fn box_to_circle(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let b = geom.as_pgbox().ok_or_else(|| Error::TypeMismatch {
        expected: "BOX".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    let center = b.center();
    let radius = b.diagonal() / 2.0;
    Ok(Value::circle(PgCircle::new(center, radius)))
}

pub fn circle_to_box(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let c = geom.as_circle().ok_or_else(|| Error::TypeMismatch {
        expected: "CIRCLE".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    let half_side = c.radius / std::f64::consts::SQRT_2;
    let low = PgPoint::new(c.center.x - half_side, c.center.y - half_side);
    let high = PgPoint::new(c.center.x + half_side, c.center.y + half_side);
    Ok(Value::pgbox(PgBox::new(low, high)))
}

pub fn box_to_polygon(geom: &Value) -> Result<Value> {
    if geom.is_null() {
        return Ok(Value::null());
    }

    let b = geom.as_pgbox().ok_or_else(|| Error::TypeMismatch {
        expected: "BOX".to_string(),
        actual: geom.data_type().to_string(),
    })?;

    let points = vec![
        PgPoint::new(b.low.x, b.low.y),
        PgPoint::new(b.high.x, b.low.y),
        PgPoint::new(b.high.x, b.high.y),
        PgPoint::new(b.low.x, b.high.y),
    ];
    Ok(Value::polygon(PgPolygon::new(points)))
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

pub fn bound_box(box1: &Value, box2: &Value) -> Result<Value> {
    if box1.is_null() || box2.is_null() {
        return Ok(Value::null());
    }

    let b1 = box1.as_pgbox().ok_or_else(|| Error::TypeMismatch {
        expected: "BOX".to_string(),
        actual: box1.data_type().to_string(),
    })?;

    let b2 = box2.as_pgbox().ok_or_else(|| Error::TypeMismatch {
        expected: "BOX".to_string(),
        actual: box2.data_type().to_string(),
    })?;

    let low = PgPoint::new(b1.low.x.min(b2.low.x), b1.low.y.min(b2.low.y));
    let high = PgPoint::new(b1.high.x.max(b2.high.x), b1.high.y.max(b2.high.y));
    Ok(Value::pgbox(PgBox::new(low, high)))
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
    fn test_box_constructor() {
        let p1 = Value::point(PgPoint::new(0.0, 0.0));
        let p2 = Value::point(PgPoint::new(3.0, 4.0));
        let b = box_constructor(&p1, &p2).unwrap();
        let boxval = b.as_pgbox().unwrap();
        assert_eq!(boxval.width(), 3.0);
        assert_eq!(boxval.height(), 4.0);
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
    fn test_area_box() {
        let b = Value::pgbox(PgBox::new(PgPoint::new(0.0, 0.0), PgPoint::new(3.0, 4.0)));
        let a = area(&b).unwrap();
        assert_eq!(a.as_f64().unwrap(), 12.0);
    }

    #[test]
    fn test_area_circle() {
        let c = Value::circle(PgCircle::new(PgPoint::new(0.0, 0.0), 1.0));
        let a = area(&c).unwrap();
        assert!((a.as_f64().unwrap() - std::f64::consts::PI).abs() < 0.0001);
    }

    #[test]
    fn test_center_box() {
        let b = Value::pgbox(PgBox::new(PgPoint::new(0.0, 0.0), PgPoint::new(4.0, 4.0)));
        let c = center(&b).unwrap();
        let point = c.as_point().unwrap();
        assert_eq!(point.x, 2.0);
        assert_eq!(point.y, 2.0);
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
    fn test_width_height() {
        let b = Value::pgbox(PgBox::new(PgPoint::new(1.0, 2.0), PgPoint::new(5.0, 7.0)));
        let w = width(&b).unwrap();
        let h = height(&b).unwrap();
        assert_eq!(w.as_f64().unwrap(), 4.0);
        assert_eq!(h.as_f64().unwrap(), 5.0);
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
    fn test_contains_box_point() {
        let b = Value::pgbox(PgBox::new(PgPoint::new(0.0, 0.0), PgPoint::new(10.0, 10.0)));
        let p_inside = Value::point(PgPoint::new(5.0, 5.0));
        let p_outside = Value::point(PgPoint::new(15.0, 15.0));

        assert!(contains(&b, &p_inside).unwrap().as_bool().unwrap());
        assert!(!contains(&b, &p_outside).unwrap().as_bool().unwrap());
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
    fn test_overlaps_boxes() {
        let b1 = Value::pgbox(PgBox::new(PgPoint::new(0.0, 0.0), PgPoint::new(5.0, 5.0)));
        let b2 = Value::pgbox(PgBox::new(PgPoint::new(3.0, 3.0), PgPoint::new(8.0, 8.0)));
        let b3 = Value::pgbox(PgBox::new(
            PgPoint::new(10.0, 10.0),
            PgPoint::new(15.0, 15.0),
        ));

        assert!(overlaps(&b1, &b2).unwrap().as_bool().unwrap());
        assert!(!overlaps(&b1, &b3).unwrap().as_bool().unwrap());
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
