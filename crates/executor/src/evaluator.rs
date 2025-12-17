//! Expression evaluation for WHERE clauses, projections, etc.

#![allow(clippy::wildcard_enum_match_arm)]
#![allow(clippy::only_used_in_recursion)]
#![allow(clippy::collapsible_if)]
#![allow(clippy::collapsible_match)]
#![allow(clippy::ptr_arg)]

use std::str::FromStr;

use chrono::{Datelike, Timelike};
use geo::{
    Area, BooleanOps, Centroid, Contains, ConvexHull, EuclideanDistance, GeodesicArea,
    GeodesicDistance, GeodesicLength, Intersects, Simplify, Within,
};
use geo_types::{
    Coord, Geometry, GeometryCollection, LineString, MultiLineString, MultiPoint, MultiPolygon,
    Point, Polygon,
};
use sqlparser::ast::{BinaryOperator, CastKind, Expr, UnaryOperator, Value as SqlValue};
use wkt::TryFromWkt;
use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::Value;
use yachtsql_storage::{Record, Schema};

pub fn parse_byte_string_escapes(s: &str) -> Vec<u8> {
    let mut result = Vec::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.peek() {
                Some('x') | Some('X') => {
                    chars.next();
                    let mut hex = String::new();
                    for _ in 0..2 {
                        if let Some(&c) = chars.peek() {
                            if c.is_ascii_hexdigit() {
                                hex.push(c);
                                chars.next();
                            }
                        }
                    }
                    if let Ok(byte) = u8::from_str_radix(&hex, 16) {
                        result.push(byte);
                    }
                }
                Some('n') => {
                    chars.next();
                    result.push(b'\n');
                }
                Some('t') => {
                    chars.next();
                    result.push(b'\t');
                }
                Some('r') => {
                    chars.next();
                    result.push(b'\r');
                }
                Some('\\') => {
                    chars.next();
                    result.push(b'\\');
                }
                Some('\'') => {
                    chars.next();
                    result.push(b'\'');
                }
                Some('"') => {
                    chars.next();
                    result.push(b'"');
                }
                _ => {
                    result.push(b'\\');
                }
            }
        } else {
            let mut buf = [0u8; 4];
            let encoded = c.encode_utf8(&mut buf);
            result.extend_from_slice(encoded.as_bytes());
        }
    }
    result
}

use std::collections::HashMap;

use crate::catalog::{FunctionBody, UserFunction};
use crate::js_udf::evaluate_js_function;

fn parse_geography(wkt_str: &str) -> std::result::Result<Geometry<f64>, String> {
    if wkt_str.starts_with("GEOGRAPHY") {
        return Err("Cannot parse GEOGRAPHY placeholder".to_string());
    }

    let upper = wkt_str.trim().to_uppercase();

    if upper.starts_with("POINT") {
        return Point::try_from_wkt_str(wkt_str)
            .map(Geometry::Point)
            .map_err(|e| e.to_string());
    }
    if upper.starts_with("LINESTRING") {
        return LineString::try_from_wkt_str(wkt_str)
            .map(Geometry::LineString)
            .map_err(|e| e.to_string());
    }
    if upper.starts_with("POLYGON") {
        return Polygon::try_from_wkt_str(wkt_str)
            .map(Geometry::Polygon)
            .map_err(|e| e.to_string());
    }
    if upper.starts_with("MULTIPOINT") {
        return MultiPoint::try_from_wkt_str(wkt_str)
            .map(Geometry::MultiPoint)
            .map_err(|e| e.to_string());
    }
    if upper.starts_with("MULTILINESTRING") {
        return MultiLineString::try_from_wkt_str(wkt_str)
            .map(Geometry::MultiLineString)
            .map_err(|e| e.to_string());
    }
    if upper.starts_with("MULTIPOLYGON") {
        return MultiPolygon::try_from_wkt_str(wkt_str)
            .map(Geometry::MultiPolygon)
            .map_err(|e| e.to_string());
    }
    if upper.starts_with("GEOMETRYCOLLECTION") {
        if upper.contains("EMPTY") {
            return Ok(Geometry::GeometryCollection(GeometryCollection::new_from(
                vec![],
            )));
        }
        return GeometryCollection::try_from_wkt_str(wkt_str)
            .map(Geometry::GeometryCollection)
            .map_err(|e| e.to_string());
    }

    Geometry::try_from_wkt_str(wkt_str).map_err(|e| e.to_string())
}

fn geometry_to_wkt(geom: &Geometry<f64>) -> String {
    use wkt::ToWkt;
    geom.wkt_string()
}

fn format_coord(val: f64) -> String {
    let rounded = (val * 1000000.0).round() / 1000000.0;
    if rounded.fract() == 0.0 {
        format!("{:.0}", rounded)
    } else {
        let s = format!("{}", rounded);
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    }
}

fn format_wkt_number(geom: &Geometry<f64>) -> String {
    match geom {
        Geometry::Point(p) => {
            format!("POINT({} {})", format_coord(p.x()), format_coord(p.y()))
        }
        Geometry::LineString(ls) => {
            let coords: Vec<String> =
                ls.0.iter()
                    .map(|c| format!("{} {}", format_coord(c.x), format_coord(c.y)))
                    .collect();
            format!("LINESTRING({})", coords.join(", "))
        }
        Geometry::Polygon(poly) => {
            let exterior: Vec<String> = poly
                .exterior()
                .0
                .iter()
                .map(|c| format!("{} {}", format_coord(c.x), format_coord(c.y)))
                .collect();
            let mut rings = vec![format!("({})", exterior.join(", "))];
            for interior in poly.interiors() {
                let int_coords: Vec<String> = interior
                    .0
                    .iter()
                    .map(|c| format!("{} {}", format_coord(c.x), format_coord(c.y)))
                    .collect();
                rings.push(format!("({})", int_coords.join(", ")));
            }
            format!("POLYGON({})", rings.join(", "))
        }
        Geometry::MultiPoint(mp) => {
            let points: Vec<String> =
                mp.0.iter()
                    .map(|p| format!("{} {}", format_coord(p.x()), format_coord(p.y())))
                    .collect();
            format!("MULTIPOINT({})", points.join(", "))
        }
        Geometry::MultiLineString(mls) => {
            let lines: Vec<String> = mls
                .0
                .iter()
                .map(|ls| {
                    let coords: Vec<String> =
                        ls.0.iter()
                            .map(|c| format!("{} {}", format_coord(c.x), format_coord(c.y)))
                            .collect();
                    format!("({})", coords.join(", "))
                })
                .collect();
            format!("MULTILINESTRING({})", lines.join(", "))
        }
        Geometry::MultiPolygon(mpoly) => {
            let polys: Vec<String> = mpoly
                .0
                .iter()
                .map(|poly| {
                    let exterior: Vec<String> = poly
                        .exterior()
                        .0
                        .iter()
                        .map(|c| format!("{} {}", format_coord(c.x), format_coord(c.y)))
                        .collect();
                    format!("(({}))", exterior.join(", "))
                })
                .collect();
            format!("MULTIPOLYGON({})", polys.join(", "))
        }
        Geometry::GeometryCollection(gc) => {
            if gc.0.is_empty() {
                "GEOMETRYCOLLECTION EMPTY".to_string()
            } else {
                let geoms: Vec<String> = gc.0.iter().map(format_wkt_number).collect();
                format!("GEOMETRYCOLLECTION({})", geoms.join(", "))
            }
        }
        _ => geometry_to_wkt(geom),
    }
}

fn geometry_to_geojson(geom: &Geometry<f64>) -> String {
    let geojson_geom: geojson::Geometry = geojson::Geometry::from(geom);
    serde_json::to_string(&geojson_geom).unwrap_or_default()
}

fn geometry_from_geojson(json_str: &str) -> std::result::Result<Geometry<f64>, String> {
    let geojson: geojson::GeoJson = json_str
        .parse()
        .map_err(|e: geojson::Error| e.to_string())?;
    match geojson {
        geojson::GeoJson::Geometry(geom) => {
            use std::convert::TryFrom;
            Geometry::try_from(geom).map_err(|e| e.to_string())
        }
        _ => Err("Expected GeoJSON geometry".to_string()),
    }
}

fn geometry_type_name(geom: &Geometry<f64>) -> &'static str {
    match geom {
        Geometry::Point(_) => "Point",
        Geometry::LineString(_) => "LineString",
        Geometry::Polygon(_) => "Polygon",
        Geometry::MultiPoint(_) => "MultiPoint",
        Geometry::MultiLineString(_) => "MultiLineString",
        Geometry::MultiPolygon(_) => "MultiPolygon",
        Geometry::GeometryCollection(_) => "GeometryCollection",
        _ => "Unknown",
    }
}

fn geometry_dimension(geom: &Geometry<f64>) -> i64 {
    match geom {
        Geometry::Point(_) | Geometry::MultiPoint(_) => 0,
        Geometry::LineString(_) | Geometry::MultiLineString(_) => 1,
        Geometry::Polygon(_) | Geometry::MultiPolygon(_) => 2,
        Geometry::GeometryCollection(gc) => gc.0.iter().map(geometry_dimension).max().unwrap_or(0),
        _ => 0,
    }
}

fn geometry_num_points(geom: &Geometry<f64>) -> i64 {
    match geom {
        Geometry::Point(_) => 1,
        Geometry::LineString(ls) => ls.0.len() as i64,
        Geometry::Polygon(poly) => {
            let mut count = poly.exterior().0.len();
            for interior in poly.interiors() {
                count += interior.0.len();
            }
            count as i64
        }
        Geometry::MultiPoint(mp) => mp.0.len() as i64,
        Geometry::MultiLineString(mls) => mls.0.iter().map(|ls| ls.0.len()).sum::<usize>() as i64,
        Geometry::MultiPolygon(mpoly) => mpoly
            .0
            .iter()
            .map(|poly| {
                let mut count = poly.exterior().0.len();
                for interior in poly.interiors() {
                    count += interior.0.len();
                }
                count
            })
            .sum::<usize>() as i64,
        Geometry::GeometryCollection(gc) => gc.0.iter().map(geometry_num_points).sum(),
        _ => 0,
    }
}

fn geometry_is_empty(geom: &Geometry<f64>) -> bool {
    match geom {
        Geometry::Point(_) => false,
        Geometry::LineString(ls) => ls.0.is_empty(),
        Geometry::Polygon(poly) => poly.exterior().0.is_empty(),
        Geometry::MultiPoint(mp) => mp.0.is_empty(),
        Geometry::MultiLineString(mls) => mls.0.is_empty(),
        Geometry::MultiPolygon(mpoly) => mpoly.0.is_empty(),
        Geometry::GeometryCollection(gc) => gc.0.is_empty(),
        _ => true,
    }
}

fn geometry_is_collection(geom: &Geometry<f64>) -> bool {
    matches!(
        geom,
        Geometry::MultiPoint(_)
            | Geometry::MultiLineString(_)
            | Geometry::MultiPolygon(_)
            | Geometry::GeometryCollection(_)
    )
}

fn snap_coord_to_grid(val: f64, grid_size: f64) -> f64 {
    (val / grid_size).round() * grid_size
}

fn geometry_geodesic_distance(geom1: &Geometry<f64>, geom2: &Geometry<f64>) -> f64 {
    fn get_centroid_point(geom: &Geometry<f64>) -> Option<Point<f64>> {
        match geom {
            Geometry::Point(p) => Some(*p),
            Geometry::LineString(ls) => ls.centroid(),
            Geometry::Polygon(poly) => poly.centroid(),
            Geometry::MultiPoint(mp) => mp.centroid(),
            Geometry::MultiLineString(mls) => mls.centroid(),
            Geometry::MultiPolygon(mpoly) => mpoly.centroid(),
            Geometry::GeometryCollection(gc) => gc.centroid(),
            _ => None,
        }
    }

    match (geom1, geom2) {
        (Geometry::Point(p1), Geometry::Point(p2)) => p1.geodesic_distance(p2),
        (Geometry::Point(p1), other) => {
            if let Some(p2) = get_centroid_point(other) {
                p1.geodesic_distance(&p2)
            } else {
                0.0
            }
        }
        (other, Geometry::Point(p2)) => {
            if let Some(p1) = get_centroid_point(other) {
                p1.geodesic_distance(p2)
            } else {
                0.0
            }
        }
        (g1, g2) => match (get_centroid_point(g1), get_centroid_point(g2)) {
            (Some(p1), Some(p2)) => p1.geodesic_distance(&p2),
            _ => 0.0,
        },
    }
}

pub struct Evaluator<'a> {
    pub schema: &'a Schema,
    user_functions: Option<&'a HashMap<String, UserFunction>>,
}

impl<'a> Evaluator<'a> {
    pub fn new(schema: &'a Schema) -> Self {
        Self {
            schema,
            user_functions: None,
        }
    }

    pub fn with_user_functions(
        schema: &'a Schema,
        user_functions: &'a HashMap<String, UserFunction>,
    ) -> Self {
        Self {
            schema,
            user_functions: Some(user_functions),
        }
    }

    pub fn evaluate(&self, expr: &Expr, record: &Record) -> Result<Value> {
        match expr {
            Expr::Identifier(ident) => {
                let name = ident.value.to_uppercase();
                let idx =
                    self.schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == name)
                        .or_else(|| {
                            self.schema.fields().iter().position(|f| {
                                f.name.to_uppercase().ends_with(&format!(".{}", name))
                            })
                        })
                        .ok_or_else(|| Error::ColumnNotFound(ident.value.clone()))?;
                Ok(record.values().get(idx).cloned().unwrap_or(Value::null()))
            }

            Expr::CompoundIdentifier(parts) => {
                let full_name = parts
                    .iter()
                    .map(|i| i.value.clone())
                    .collect::<Vec<_>>()
                    .join(".");
                let last_name = parts
                    .last()
                    .map(|i| i.value.to_uppercase())
                    .unwrap_or_default();
                let full_upper = full_name.to_uppercase();

                let idx = self
                    .schema
                    .fields()
                    .iter()
                    .position(|f| f.name.to_uppercase() == full_upper)
                    .or_else(|| {
                        self.schema
                            .fields()
                            .iter()
                            .position(|f| f.name.to_uppercase() == last_name)
                    })
                    .or_else(|| {
                        self.schema.fields().iter().position(|f| {
                            f.name.to_uppercase().ends_with(&format!(".{}", last_name))
                        })
                    });

                if let Some(idx) = idx {
                    return Ok(record.values().get(idx).cloned().unwrap_or(Value::null()));
                }

                if parts.len() >= 3 {
                    let first_two = format!(
                        "{}.{}",
                        parts[0].value.to_uppercase(),
                        parts[1].value.to_uppercase()
                    );
                    if let Some(base_idx) = self
                        .schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == first_two)
                    {
                        let base_val = record
                            .values()
                            .get(base_idx)
                            .cloned()
                            .unwrap_or(Value::null());
                        if let Some(struct_fields) = base_val.as_struct() {
                            let field_name = parts[2].value.to_uppercase();
                            for (name, val) in struct_fields {
                                if name.to_uppercase() == field_name {
                                    if parts.len() == 3 {
                                        return Ok(val.clone());
                                    }
                                    if let Some(nested) = val.as_struct() {
                                        let nested_field = parts[3].value.to_uppercase();
                                        for (n, v) in nested {
                                            if n.to_uppercase() == nested_field {
                                                return Ok(v.clone());
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                if parts.len() >= 2 {
                    let first = parts[0].value.to_uppercase();
                    if let Some(base_idx) = self
                        .schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase() == first)
                    {
                        let base_val = record
                            .values()
                            .get(base_idx)
                            .cloned()
                            .unwrap_or(Value::null());
                        if let Some(struct_fields) = base_val.as_struct() {
                            let field_name = parts[1].value.to_uppercase();
                            for (name, val) in struct_fields {
                                if name.to_uppercase() == field_name {
                                    if parts.len() == 2 {
                                        return Ok(val.clone());
                                    }
                                    if let Some(nested) = val.as_struct() {
                                        let nested_field = parts[2].value.to_uppercase();
                                        for (n, v) in nested {
                                            if n.to_uppercase() == nested_field {
                                                return Ok(v.clone());
                                            }
                                        }
                                    }
                                }
                            }
                            return Err(Error::ColumnNotFound(full_name.clone()));
                        }
                        if let Some(json_val) = base_val.as_json() {
                            let mut current = json_val.clone();
                            for part in &parts[1..] {
                                match &current {
                                    serde_json::Value::Object(obj) => {
                                        current = obj
                                            .get(&part.value)
                                            .cloned()
                                            .unwrap_or(serde_json::Value::Null);
                                    }
                                    _ => {
                                        return Ok(Value::null());
                                    }
                                }
                            }
                            return Ok(Value::json(current));
                        }
                    }
                    if let Some(base_idx) = self
                        .schema
                        .fields()
                        .iter()
                        .position(|f| f.name.to_uppercase().ends_with(&format!(".{}", first)))
                    {
                        let base_val = record
                            .values()
                            .get(base_idx)
                            .cloned()
                            .unwrap_or(Value::null());
                        if let Some(struct_fields) = base_val.as_struct() {
                            let field_name = parts[1].value.to_uppercase();
                            for (name, val) in struct_fields {
                                if name.to_uppercase() == field_name {
                                    return Ok(val.clone());
                                }
                            }
                        }
                    }
                }

                Err(Error::ColumnNotFound(full_name.clone()))
            }

            Expr::Value(val) => self.evaluate_literal(&val.value),

            Expr::BinaryOp { left, op, right } => {
                let left_val = self.evaluate(left, record)?;
                let right_val = self.evaluate(right, record)?;
                self.evaluate_binary_op(&left_val, op, &right_val)
            }

            Expr::UnaryOp { op, expr } => {
                let val = self.evaluate(expr, record)?;
                self.evaluate_unary_op(op, &val)
            }

            Expr::IsNull(inner) => {
                let val = self.evaluate(inner, record)?;
                Ok(Value::bool_val(val.is_null()))
            }

            Expr::IsNotNull(inner) => {
                let val = self.evaluate(inner, record)?;
                Ok(Value::bool_val(!val.is_null()))
            }

            Expr::Nested(inner) => self.evaluate(inner, record),

            Expr::Function(func) => self.evaluate_function_internal(func, record),

            Expr::Case {
                operand,
                conditions,
                else_result,
                ..
            } => self.evaluate_case(
                operand.as_deref(),
                conditions,
                else_result.as_deref(),
                record,
            ),

            Expr::Array(arr) => self.evaluate_array(arr, record),

            Expr::InList {
                expr,
                list,
                negated,
            } => self.evaluate_in_list(expr, list, *negated, record),

            Expr::Between {
                expr,
                low,
                high,
                negated,
            } => self.evaluate_between(expr, low, high, *negated, record),

            Expr::Like {
                expr,
                pattern,
                negated,
                any,
                ..
            } => self.evaluate_like_with_quantifier(expr, pattern, *negated, *any, record),

            Expr::ILike {
                expr,
                pattern,
                negated,
                any,
                ..
            } => self.evaluate_ilike_with_quantifier(expr, pattern, *negated, *any, record),

            Expr::Cast {
                expr,
                data_type,
                kind,
                ..
            } => self.evaluate_cast(expr, data_type, kind, record),

            Expr::Tuple(exprs) => {
                let mut values = Vec::with_capacity(exprs.len());
                for e in exprs {
                    values.push(self.evaluate(e, record)?);
                }
                Ok(Value::array(values))
            }

            Expr::TypedString(ts) => self.evaluate_typed_string(&ts.data_type, &ts.value),

            Expr::CompoundFieldAccess { root, access_chain } => {
                self.evaluate_compound_field_access(root, access_chain, record)
            }

            Expr::InSubquery {
                expr,
                subquery,
                negated,
            } => Err(Error::UnsupportedFeature(
                "IN subquery not yet supported in evaluator".to_string(),
            )),

            Expr::InUnnest {
                expr,
                array_expr,
                negated,
            } => self.evaluate_in_unnest(expr, array_expr, *negated, record),

            Expr::Subquery(_) => Err(Error::UnsupportedFeature(
                "Subquery not yet supported in evaluator".to_string(),
            )),

            Expr::Struct { values, fields } => self.evaluate_struct_expr(values, record),

            Expr::Named { expr, name } => self.evaluate(expr, record),

            Expr::Trim {
                expr: inner,
                trim_where,
                trim_what,
                trim_characters,
            } => {
                let val = self.evaluate(inner, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: val.data_type().to_string(),
                })?;

                let chars_to_trim: Option<Vec<char>> = if let Some(chars_expr) = trim_characters {
                    if !chars_expr.is_empty() {
                        let first = self.evaluate(&chars_expr[0], record)?;
                        first.as_str().map(|s| s.chars().collect())
                    } else {
                        None
                    }
                } else if let Some(what_expr) = trim_what {
                    let what_val = self.evaluate(what_expr, record)?;
                    what_val.as_str().map(|s| s.chars().collect())
                } else {
                    None
                };

                let result = match (trim_where, &chars_to_trim) {
                    (Some(sqlparser::ast::TrimWhereField::Leading), Some(chars)) => {
                        s.trim_start_matches(|c| chars.contains(&c)).to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Trailing), Some(chars)) => {
                        s.trim_end_matches(|c| chars.contains(&c)).to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Both), Some(chars)) => {
                        s.trim_matches(|c| chars.contains(&c)).to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Leading), None) => {
                        s.trim_start().to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Trailing), None) => {
                        s.trim_end().to_string()
                    }
                    (Some(sqlparser::ast::TrimWhereField::Both), None) | (None, None) => {
                        s.trim().to_string()
                    }
                    (None, Some(chars)) => s.trim_matches(|c| chars.contains(&c)).to_string(),
                };
                Ok(Value::string(result))
            }

            Expr::Substring {
                expr: inner,
                substring_from,
                substring_for,
                special: _,
                shorthand: _,
            } => {
                let val = self.evaluate(inner, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }

                let start = if let Some(from_expr) = substring_from {
                    let from_val = self.evaluate(from_expr, record)?;
                    from_val.as_i64().unwrap_or(1) as usize
                } else {
                    1
                };
                let start_idx = if start > 0 { start - 1 } else { 0 };

                if let Some(bytes) = val.as_bytes() {
                    if start_idx >= bytes.len() {
                        return Ok(Value::bytes(Vec::new()));
                    }
                    let result: Vec<u8> = if let Some(for_expr) = substring_for {
                        let len_val = self.evaluate(for_expr, record)?;
                        let len = len_val.as_i64().unwrap_or(0) as usize;
                        bytes[start_idx..].iter().take(len).copied().collect()
                    } else {
                        bytes[start_idx..].to_vec()
                    };
                    return Ok(Value::bytes(result));
                }

                let s = val.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: val.data_type().to_string(),
                })?;

                let chars: Vec<char> = s.chars().collect();
                if start_idx >= chars.len() {
                    return Ok(Value::string(String::new()));
                }

                let result: String = if let Some(for_expr) = substring_for {
                    let len_val = self.evaluate(for_expr, record)?;
                    let len = len_val.as_i64().unwrap_or(0) as usize;
                    chars[start_idx..].iter().take(len).collect()
                } else {
                    chars[start_idx..].iter().collect()
                };
                Ok(Value::string(result))
            }

            Expr::IsDistinctFrom(left, right) => {
                let l = self.evaluate(left, record)?;
                let r = self.evaluate(right, record)?;
                let distinct = match (l.is_null(), r.is_null()) {
                    (true, true) => false,
                    (true, false) | (false, true) => true,
                    (false, false) => l != r,
                };
                Ok(Value::bool_val(distinct))
            }

            Expr::IsNotDistinctFrom(left, right) => {
                let l = self.evaluate(left, record)?;
                let r = self.evaluate(right, record)?;
                let not_distinct = match (l.is_null(), r.is_null()) {
                    (true, true) => true,
                    (true, false) | (false, true) => false,
                    (false, false) => l == r,
                };
                Ok(Value::bool_val(not_distinct))
            }

            Expr::Overlay {
                expr,
                overlay_what,
                overlay_from,
                overlay_for,
            } => {
                let s = self.evaluate(expr, record)?;
                let what = self.evaluate(overlay_what, record)?;
                let from = self.evaluate(overlay_from, record)?;

                if s.is_null() {
                    return Ok(Value::null());
                }

                let s_str = s.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: s.data_type().to_string(),
                })?;
                let what_str = what.as_str().unwrap_or("");
                let from_idx = from.as_i64().unwrap_or(1) as usize;
                let start = if from_idx > 0 { from_idx - 1 } else { 0 };

                let for_len = if let Some(for_expr) = overlay_for {
                    let for_val = self.evaluate(for_expr, record)?;
                    for_val.as_i64().unwrap_or(what_str.len() as i64) as usize
                } else {
                    what_str.len()
                };

                let chars: Vec<char> = s_str.chars().collect();
                let end = (start + for_len).min(chars.len());

                let mut result = String::new();
                result.extend(&chars[..start.min(chars.len())]);
                result.push_str(what_str);
                if end < chars.len() {
                    result.extend(&chars[end..]);
                }
                Ok(Value::string(result))
            }

            Expr::Position { expr, r#in } => {
                let needle = self.evaluate(expr, record)?;
                let haystack = self.evaluate(r#in, record)?;

                if needle.is_null() || haystack.is_null() {
                    return Ok(Value::null());
                }

                let needle_str = needle.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: needle.data_type().to_string(),
                })?;
                let haystack_str = haystack.as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: haystack.data_type().to_string(),
                })?;

                let pos = haystack_str
                    .find(needle_str)
                    .map(|i| haystack_str[..i].chars().count() as i64 + 1)
                    .unwrap_or(0);
                Ok(Value::int64(pos))
            }

            Expr::Interval(interval) => {
                let val = self.evaluate(&interval.value, record)?;

                if let Some(s) = val.as_str() {
                    let interval_val = self.parse_interval_string(s)?;
                    return Ok(Value::interval(interval_val));
                }

                let amount = val.as_i64().unwrap_or(0);
                let unit = interval
                    .leading_field
                    .as_ref()
                    .map(|f| format!("{:?}", f).to_uppercase())
                    .unwrap_or_else(|| "SECOND".to_string());
                let interval_val = match unit.as_str() {
                    "YEAR" => yachtsql_common::types::Interval::from_months(amount as i32 * 12),
                    "MONTH" => yachtsql_common::types::Interval::from_months(amount as i32),
                    "DAY" => yachtsql_common::types::Interval::from_days(amount as i32),
                    "HOUR" => yachtsql_common::types::Interval::from_hours(amount),
                    "MINUTE" => yachtsql_common::types::Interval::new(
                        0,
                        0,
                        amount * yachtsql_common::types::Interval::MICROS_PER_MINUTE,
                    ),
                    "SECOND" => yachtsql_common::types::Interval::new(
                        0,
                        0,
                        amount * yachtsql_common::types::Interval::MICROS_PER_SECOND,
                    ),
                    _ => yachtsql_common::types::Interval::new(0, amount as i32, 0),
                };
                Ok(Value::interval(interval_val))
            }

            Expr::Extract { field, expr, .. } => {
                let val = self.evaluate(expr, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }

                let field_str = format!("{}", field);
                if let Some(date) = val.as_date() {
                    let result = match field_str.to_uppercase().as_str() {
                        "YEAR" => date.year() as i64,
                        "MONTH" => date.month() as i64,
                        "DAY" => date.day() as i64,
                        "DAYOFWEEK" => date.weekday().num_days_from_sunday() as i64 + 1,
                        "DAYOFYEAR" => date.ordinal() as i64,
                        "WEEK" => date.iso_week().week() as i64,
                        "QUARTER" => ((date.month() - 1) / 3 + 1) as i64,
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for DATE",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                if let Some(ts) = val.as_timestamp() {
                    let result = match field_str.to_uppercase().as_str() {
                        "YEAR" => ts.year() as i64,
                        "MONTH" => ts.month() as i64,
                        "DAY" => ts.day() as i64,
                        "HOUR" => ts.hour() as i64,
                        "MINUTE" => ts.minute() as i64,
                        "SECOND" => ts.second() as i64,
                        "DAYOFWEEK" => ts.weekday().num_days_from_sunday() as i64 + 1,
                        "DAYOFYEAR" => ts.ordinal() as i64,
                        "WEEK" => ts.iso_week().week() as i64,
                        "QUARTER" => ((ts.month() - 1) / 3 + 1) as i64,
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for TIMESTAMP",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                if let Some(time) = val.as_time() {
                    let result = match field_str.to_uppercase().as_str() {
                        "HOUR" => time.hour() as i64,
                        "MINUTE" => time.minute() as i64,
                        "SECOND" => time.second() as i64,
                        "MILLISECOND" => (time.nanosecond() / 1_000_000) as i64,
                        "MICROSECOND" => (time.nanosecond() / 1_000) as i64,
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for TIME",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                if let Some(interval) = val.as_interval() {
                    use yachtsql_common::types::IntervalValue;
                    let result = match field_str.to_uppercase().as_str() {
                        "YEAR" => (interval.months / 12) as i64,
                        "MONTH" => (interval.months % 12) as i64,
                        "DAY" => interval.days as i64,
                        "HOUR" => {
                            let total_micros = interval.nanos / IntervalValue::NANOS_PER_MICRO;
                            total_micros / IntervalValue::MICROS_PER_HOUR
                        }
                        "MINUTE" => {
                            let total_micros = interval.nanos / IntervalValue::NANOS_PER_MICRO;
                            let remaining_after_hours =
                                total_micros % IntervalValue::MICROS_PER_HOUR;
                            remaining_after_hours / IntervalValue::MICROS_PER_MINUTE
                        }
                        "SECOND" => {
                            let total_micros = interval.nanos / IntervalValue::NANOS_PER_MICRO;
                            let remaining_after_minutes =
                                total_micros % IntervalValue::MICROS_PER_MINUTE;
                            remaining_after_minutes / IntervalValue::MICROS_PER_SECOND
                        }
                        _ => {
                            return Err(Error::UnsupportedFeature(format!(
                                "EXTRACT {} not supported for INTERVAL",
                                field_str
                            )));
                        }
                    };
                    return Ok(Value::int64(result));
                }

                Err(Error::TypeMismatch {
                    expected: "DATE, TIME, TIMESTAMP, or INTERVAL".to_string(),
                    actual: val.data_type().to_string(),
                })
            }

            Expr::Ceil { expr, .. } => {
                let val = self.evaluate(expr, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(d) = val.as_numeric() {
                    return Ok(Value::numeric(d.ceil()));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(f.ceil()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: val.data_type().to_string(),
                })
            }

            Expr::Floor { expr, .. } => {
                let val = self.evaluate(expr, record)?;
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(d) = val.as_numeric() {
                    return Ok(Value::numeric(d.floor()));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(f.floor()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: val.data_type().to_string(),
                })
            }

            _ => Err(Error::UnsupportedFeature(format!(
                "Expression type not yet supported: {:?}",
                expr
            ))),
        }
    }

    fn evaluate_struct_expr(&self, values: &[Expr], record: &Record) -> Result<Value> {
        let mut struct_fields = indexmap::IndexMap::new();
        for (i, expr) in values.iter().enumerate() {
            match expr {
                Expr::Named {
                    expr: inner_expr,
                    name,
                } => {
                    let val = self.evaluate(inner_expr, record)?;
                    struct_fields.insert(name.value.clone(), val);
                }
                _ => {
                    let val = self.evaluate(expr, record)?;
                    struct_fields.insert(format!("_field{}", i), val);
                }
            }
        }
        Ok(Value::struct_val(struct_fields.into_iter().collect()))
    }

    fn evaluate_typed_string(
        &self,
        data_type: &sqlparser::ast::DataType,
        value: &sqlparser::ast::ValueWithSpan,
    ) -> Result<Value> {
        let s = match &value.value {
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => s.as_str(),
            _ => {
                return Err(Error::InvalidQuery(
                    "TypedString value must be a string".to_string(),
                ));
            }
        };
        match data_type {
            sqlparser::ast::DataType::Date => {
                if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                    Ok(Value::date(date))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Time(_, _) => {
                if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                    Ok(Value::time(time))
                } else if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                    Ok(Value::time(time))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Timestamp(_, _) => {
                if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
                    Ok(Value::timestamp(ts.with_timezone(&chrono::Utc)))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
                {
                    Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    ))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                {
                    Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    ))
                } else if s.ends_with(" UTC") {
                    let s_without_tz = s.trim_end_matches(" UTC");
                    if let Ok(ndt) =
                        chrono::NaiveDateTime::parse_from_str(s_without_tz, "%Y-%m-%d %H:%M:%S")
                    {
                        Ok(Value::timestamp(
                            chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                        ))
                    } else {
                        Ok(Value::string(s.to_string()))
                    }
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Datetime(_) => {
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                    Ok(Value::datetime(ndt))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                {
                    Ok(Value::datetime(ndt))
                } else if let Ok(ndt) =
                    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f")
                {
                    Ok(Value::datetime(ndt))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::JSON => {
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(s) {
                    Ok(Value::json(json_val))
                } else {
                    Ok(Value::string(s.to_string()))
                }
            }
            sqlparser::ast::DataType::Bytes(_) => Ok(Value::bytes(s.as_bytes().to_vec())),
            sqlparser::ast::DataType::Numeric(_)
            | sqlparser::ast::DataType::Decimal(_)
            | sqlparser::ast::DataType::BigNumeric(_) => {
                if let Ok(d) = s.parse::<rust_decimal::Decimal>() {
                    Ok(Value::numeric(d))
                } else {
                    Err(Error::InvalidQuery(format!(
                        "Invalid NUMERIC literal: {}",
                        s
                    )))
                }
            }
            _ => Ok(Value::string(s.to_string())),
        }
    }

    fn evaluate_compound_field_access(
        &self,
        root: &Expr,
        access_chain: &[sqlparser::ast::AccessExpr],
        record: &Record,
    ) -> Result<Value> {
        let mut current = self.evaluate(root, record)?;

        for access in access_chain {
            match access {
                sqlparser::ast::AccessExpr::Subscript(subscript) => {
                    current = self.apply_subscript(current, subscript, record)?;
                }
                sqlparser::ast::AccessExpr::Dot(field_expr) => match field_expr {
                    Expr::Identifier(ident) => {
                        if let Some(struct_val) = current.as_struct() {
                            let field_name = ident.value.to_uppercase();
                            current = struct_val
                                .iter()
                                .find(|(name, _)| name.to_uppercase() == field_name)
                                .map(|(_, v)| v.clone())
                                .unwrap_or_else(Value::null);
                        } else if let Some(json_val) = current.as_json() {
                            let field_name = &ident.value;
                            match json_val {
                                serde_json::Value::Object(obj) => {
                                    let found = obj.get(field_name).cloned();
                                    current = found.map(Value::json).unwrap_or_else(Value::null);
                                }
                                _ => {
                                    return Ok(Value::null());
                                }
                            }
                        } else {
                            return Ok(Value::null());
                        }
                    }
                    _ => {
                        return Err(Error::UnsupportedFeature(
                            "Non-identifier field access".to_string(),
                        ));
                    }
                },
            }
        }

        Ok(current)
    }

    fn apply_subscript(
        &self,
        base_val: Value,
        subscript: &sqlparser::ast::Subscript,
        record: &Record,
    ) -> Result<Value> {
        match subscript {
            sqlparser::ast::Subscript::Index { index } => {
                let idx_val = self.evaluate(index, record)?;
                let idx = idx_val.as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: idx_val.data_type().to_string(),
                })?;

                if let Some(arr) = base_val.as_array() {
                    let idx_usize = if idx > 0 {
                        (idx - 1) as usize
                    } else if idx < 0 {
                        let len = arr.len() as i64;
                        if -idx > len {
                            return Ok(Value::null());
                        }
                        (len + idx) as usize
                    } else {
                        return Ok(Value::null());
                    };

                    if idx_usize < arr.len() {
                        Ok(arr[idx_usize].clone())
                    } else {
                        Ok(Value::null())
                    }
                } else if let Some(s) = base_val.as_str() {
                    let chars: Vec<char> = s.chars().collect();
                    let idx_usize = if idx > 0 {
                        (idx - 1) as usize
                    } else {
                        return Ok(Value::null());
                    };

                    if idx_usize < chars.len() {
                        Ok(Value::string(chars[idx_usize].to_string()))
                    } else {
                        Ok(Value::null())
                    }
                } else {
                    Err(Error::TypeMismatch {
                        expected: "ARRAY or STRING".to_string(),
                        actual: base_val.data_type().to_string(),
                    })
                }
            }
            sqlparser::ast::Subscript::Slice { .. } => Err(Error::UnsupportedFeature(
                "Array slice not yet supported".to_string(),
            )),
        }
    }

    fn evaluate_case(
        &self,
        operand: Option<&Expr>,
        conditions: &[sqlparser::ast::CaseWhen],
        else_result: Option<&Expr>,
        record: &Record,
    ) -> Result<Value> {
        match operand {
            Some(op_expr) => {
                let op_val = self.evaluate(op_expr, record)?;
                for cond in conditions {
                    let when_val = self.evaluate(&cond.condition, record)?;
                    if op_val == when_val {
                        return self.evaluate(&cond.result, record);
                    }
                }
            }
            None => {
                for cond in conditions {
                    let cond_val = self.evaluate(&cond.condition, record)?;
                    if let Some(true) = cond_val.as_bool() {
                        return self.evaluate(&cond.result, record);
                    }
                }
            }
        }
        match else_result {
            Some(else_expr) => self.evaluate(else_expr, record),
            None => Ok(Value::null()),
        }
    }

    fn evaluate_array(&self, arr: &sqlparser::ast::Array, record: &Record) -> Result<Value> {
        let mut values = Vec::with_capacity(arr.elem.len());
        for elem in &arr.elem {
            values.push(self.evaluate(elem, record)?);
        }
        Ok(Value::array(values))
    }

    fn evaluate_in_list(
        &self,
        expr: &Expr,
        list: &[Expr],
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let mut found = false;
        let mut has_null = false;
        for item in list {
            if let Expr::Function(func) = item {
                if func.name.to_string().to_uppercase() == "UNNEST" {
                    if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
                        if let Some(sqlparser::ast::FunctionArg::Unnamed(
                            sqlparser::ast::FunctionArgExpr::Expr(inner),
                        )) = arg_list.args.first()
                        {
                            let arr_val = self.evaluate(inner, record)?;
                            if let Some(arr) = arr_val.as_array() {
                                for arr_item in arr {
                                    if arr_item.is_null() {
                                        has_null = true;
                                    } else if val == *arr_item {
                                        found = true;
                                        break;
                                    }
                                }
                                if found {
                                    break;
                                }
                                continue;
                            }
                        }
                    }
                }
            }
            let item_val = self.evaluate(item, record)?;
            if item_val.is_null() {
                has_null = true;
            } else if val == item_val {
                found = true;
                break;
            }
        }
        let result = if found {
            true
        } else if has_null {
            return Ok(Value::null());
        } else {
            false
        };
        Ok(Value::bool_val(if negated { !result } else { result }))
    }

    fn evaluate_in_unnest(
        &self,
        expr: &Expr,
        array_expr: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let arr_val = self.evaluate(array_expr, record)?;
        if arr_val.is_null() {
            return Ok(Value::null());
        }
        let arr = arr_val.as_array().ok_or_else(|| Error::TypeMismatch {
            expected: "ARRAY".to_string(),
            actual: arr_val.data_type().to_string(),
        })?;
        let mut found = false;
        let mut has_null = false;
        for arr_item in arr {
            if arr_item.is_null() {
                has_null = true;
            } else if val == *arr_item {
                found = true;
                break;
            }
        }
        let result = if found {
            true
        } else if has_null {
            return Ok(Value::null());
        } else {
            false
        };
        Ok(Value::bool_val(if negated { !result } else { result }))
    }

    fn evaluate_between(
        &self,
        expr: &Expr,
        low: &Expr,
        high: &Expr,
        negated: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        let low_val = self.evaluate(low, record)?;
        let high_val = self.evaluate(high, record)?;

        if val.is_null() || low_val.is_null() || high_val.is_null() {
            return Ok(Value::null());
        }

        let ge_low = self.compare_values(&val, &low_val, |ord| ord.is_ge())?;
        let le_high = self.compare_values(&val, &high_val, |ord| ord.is_le())?;

        let in_range = ge_low.as_bool().unwrap_or(false) && le_high.as_bool().unwrap_or(false);
        Ok(Value::bool_val(if negated { !in_range } else { in_range }))
    }

    pub fn evaluate_between_values(
        &self,
        val: &Value,
        low_val: &Value,
        high_val: &Value,
        negated: bool,
    ) -> Result<Value> {
        if val.is_null() || low_val.is_null() || high_val.is_null() {
            return Ok(Value::null());
        }

        let ge_low = self.compare_values(val, low_val, |ord| ord.is_ge())?;
        let le_high = self.compare_values(val, high_val, |ord| ord.is_le())?;

        let in_range = ge_low.as_bool().unwrap_or(false) && le_high.as_bool().unwrap_or(false);
        Ok(Value::bool_val(if negated { !in_range } else { in_range }))
    }

    fn extract_pattern_list(&self, pattern: &Expr, record: &Record) -> Result<Option<Vec<String>>> {
        match pattern {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                match func_name.as_str() {
                    "ALL" | "ANY" => {
                        let args = self.extract_function_args(func, record)?;
                        let patterns: Result<Vec<String>> = args
                            .into_iter()
                            .map(|v| {
                                v.as_str().map(|s| s.to_string()).ok_or_else(|| {
                                    Error::TypeMismatch {
                                        expected: "STRING".to_string(),
                                        actual: v.data_type().to_string(),
                                    }
                                })
                            })
                            .collect();
                        Ok(Some(patterns?))
                    }
                    _ => Ok(None),
                }
            }
            Expr::Tuple(exprs) => {
                let patterns: Result<Vec<String>> = exprs
                    .iter()
                    .map(|e| {
                        let v = self.evaluate(e, record)?;
                        v.as_str()
                            .map(|s| s.to_string())
                            .ok_or_else(|| Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: v.data_type().to_string(),
                            })
                    })
                    .collect();
                Ok(Some(patterns?))
            }
            _ => Ok(None),
        }
    }

    fn is_like_all_pattern(&self, pattern: &Expr) -> bool {
        match pattern {
            Expr::Function(func) => {
                let func_name = func.name.to_string().to_uppercase();
                func_name == "ALL"
            }
            _ => false,
        }
    }

    fn evaluate_like_with_quantifier(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        any: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let val_str = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;

        let is_all = self.is_like_all_pattern(pattern);

        if let Some(patterns) = self.extract_pattern_list(pattern, record)? {
            if any {
                let any_match = patterns.iter().any(|p| self.like_match(val_str, p, false));
                return Ok(Value::bool_val(if negated {
                    !any_match
                } else {
                    any_match
                }));
            }
            if is_all {
                let all_match = patterns.iter().all(|p| self.like_match(val_str, p, false));
                return Ok(Value::bool_val(if negated {
                    !all_match
                } else {
                    all_match
                }));
            }
        }

        let pat = self.evaluate(pattern, record)?;
        if pat.is_null() {
            return Ok(Value::null());
        }
        let pat_str = pat.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pat.data_type().to_string(),
        })?;
        let matches = self.like_match(val_str, pat_str, false);
        Ok(Value::bool_val(if negated { !matches } else { matches }))
    }

    fn evaluate_ilike_with_quantifier(
        &self,
        expr: &Expr,
        pattern: &Expr,
        negated: bool,
        any: bool,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }
        let val_str = val.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: val.data_type().to_string(),
        })?;

        let is_all = self.is_like_all_pattern(pattern);

        if let Some(patterns) = self.extract_pattern_list(pattern, record)? {
            if any {
                let any_match = patterns.iter().any(|p| self.like_match(val_str, p, true));
                return Ok(Value::bool_val(if negated {
                    !any_match
                } else {
                    any_match
                }));
            }
            if is_all {
                let all_match = patterns.iter().all(|p| self.like_match(val_str, p, true));
                return Ok(Value::bool_val(if negated {
                    !all_match
                } else {
                    all_match
                }));
            }
        }

        let pat = self.evaluate(pattern, record)?;
        if pat.is_null() {
            return Ok(Value::null());
        }
        let pat_str = pat.as_str().ok_or_else(|| Error::TypeMismatch {
            expected: "STRING".to_string(),
            actual: pat.data_type().to_string(),
        })?;
        let matches = self.like_match(val_str, pat_str, true);
        Ok(Value::bool_val(if negated { !matches } else { matches }))
    }

    fn like_match(&self, text: &str, pattern: &str, case_insensitive: bool) -> bool {
        let (text, pattern) = if case_insensitive {
            (text.to_lowercase(), pattern.to_lowercase())
        } else {
            (text.to_string(), pattern.to_string())
        };

        let regex_pattern = pattern.replace('%', ".*").replace('_', ".");
        let regex_pattern = format!("^{}$", regex_pattern);

        regex::Regex::new(&regex_pattern)
            .map(|re| re.is_match(&text))
            .unwrap_or(false)
    }

    fn evaluate_cast(
        &self,
        expr: &Expr,
        target_type: &sqlparser::ast::DataType,
        kind: &CastKind,
        record: &Record,
    ) -> Result<Value> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(Value::null());
        }

        let is_safe = matches!(kind, CastKind::SafeCast | CastKind::TryCast);

        match target_type {
            sqlparser::ast::DataType::Int64
            | sqlparser::ast::DataType::BigInt(_)
            | sqlparser::ast::DataType::Integer(_) => {
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = val.as_f64() {
                    if is_safe {
                        if f > i64::MAX as f64 || f < i64::MIN as f64 || !f.is_finite() {
                            return Ok(Value::null());
                        }
                    }
                    return Ok(Value::int64(f as i64));
                }
                if let Some(d) = val.as_numeric() {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(i) = d.to_i64() {
                        return Ok(Value::int64(i));
                    }
                    if is_safe {
                        return Ok(Value::null());
                    }
                }
                if let Some(s) = val.as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(Value::int64(i));
                    }
                }
                if let Some(b) = val.as_bool() {
                    return Ok(Value::int64(if b { 1 } else { 0 }));
                }
                if let Some(jv) = val.as_json() {
                    match jv {
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                return Ok(Value::int64(i));
                            }
                            if let Some(f) = n.as_f64() {
                                return Ok(Value::int64(f as i64));
                            }
                        }
                        serde_json::Value::String(s) => {
                            if let Ok(i) = s.parse::<i64>() {
                                return Ok(Value::int64(i));
                            }
                        }
                        _ => {}
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Float64 | sqlparser::ast::DataType::Double(_) => {
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(f));
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                if let Some(d) = val.as_numeric() {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(f) = d.to_f64() {
                        return Ok(Value::float64(f));
                    }
                }
                if let Some(s) = val.as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                if let Some(jv) = val.as_json() {
                    match jv {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                return Ok(Value::float64(f));
                            }
                        }
                        serde_json::Value::String(s) => {
                            if let Ok(f) = s.parse::<f64>() {
                                return Ok(Value::float64(f));
                            }
                        }
                        _ => {}
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::String(_)
            | sqlparser::ast::DataType::Varchar(_)
            | sqlparser::ast::DataType::Text => {
                if let Some(b) = val.as_bytes() {
                    match String::from_utf8(b.to_vec()) {
                        Ok(s) => return Ok(Value::string(s)),
                        Err(_) => {
                            if is_safe {
                                return Ok(Value::null());
                            }
                            return Err(Error::TypeMismatch {
                                expected: "STRING".to_string(),
                                actual: "invalid UTF-8 BYTES".to_string(),
                            });
                        }
                    }
                }
                if let Some(jv) = val.as_json() {
                    match jv {
                        serde_json::Value::String(s) => return Ok(Value::string(s.clone())),
                        serde_json::Value::Null => return Ok(Value::null()),
                        other => return Ok(Value::string(other.to_string())),
                    }
                }
                Ok(Value::string(val.to_string()))
            }
            sqlparser::ast::DataType::Boolean | sqlparser::ast::DataType::Bool => {
                if let Some(b) = val.as_bool() {
                    return Ok(Value::bool_val(b));
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::bool_val(i != 0));
                }
                if let Some(s) = val.as_str() {
                    let lower = s.to_lowercase();
                    if lower == "true" || lower == "1" || lower == "yes" {
                        return Ok(Value::bool_val(true));
                    }
                    if lower == "false" || lower == "0" || lower == "no" {
                        return Ok(Value::bool_val(false));
                    }
                }
                if let Some(jv) = val.as_json() {
                    match jv {
                        serde_json::Value::Bool(b) => return Ok(Value::bool_val(*b)),
                        serde_json::Value::String(s) => {
                            let lower = s.to_lowercase();
                            if lower == "true" || lower == "1" || lower == "yes" {
                                return Ok(Value::bool_val(true));
                            }
                            if lower == "false" || lower == "0" || lower == "no" {
                                return Ok(Value::bool_val(false));
                            }
                        }
                        _ => {}
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Date => {
                if let Some(s) = val.as_str() {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(Value::date(date));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Timestamp(_, _) | sqlparser::ast::DataType::Datetime(_) => {
                if let Some(s) = val.as_str() {
                    if let Ok(ts) = chrono::DateTime::parse_from_rfc3339(s) {
                        return Ok(Value::timestamp(ts.with_timezone(&chrono::Utc)));
                    }
                    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                        return Ok(Value::timestamp(
                            chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                        ));
                    }
                    if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                        return Ok(Value::timestamp(
                            chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                        ));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Time(_, _) => {
                if let Some(s) = val.as_str() {
                    if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S") {
                        return Ok(Value::time(time));
                    }
                    if let Ok(time) = chrono::NaiveTime::parse_from_str(s, "%H:%M:%S%.f") {
                        return Ok(Value::time(time));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Numeric(_) | sqlparser::ast::DataType::Decimal(_) => {
                if let Some(i) = val.as_i64() {
                    return Ok(Value::numeric(rust_decimal::Decimal::from(i)));
                }
                if let Some(f) = val.as_f64() {
                    if let Some(dec) = rust_decimal::Decimal::from_f64_retain(f) {
                        return Ok(Value::numeric(dec));
                    }
                    return Ok(Value::float64(f));
                }
                if let Some(s) = val.as_str() {
                    if let Ok(dec) = rust_decimal::Decimal::from_str_exact(s) {
                        return Ok(Value::numeric(dec));
                    }
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "NUMERIC".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::BigNumeric(_) => {
                if let Some(i) = val.as_i64() {
                    return Ok(Value::numeric(rust_decimal::Decimal::from(i)));
                }
                if let Some(f) = val.as_f64() {
                    if let Some(dec) = rust_decimal::Decimal::from_f64_retain(f) {
                        return Ok(Value::numeric(dec));
                    }
                    return Ok(Value::float64(f));
                }
                if let Some(s) = val.as_str() {
                    if let Ok(dec) = rust_decimal::Decimal::from_str_exact(s) {
                        return Ok(Value::numeric(dec));
                    }
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "BIGNUMERIC".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Bytes(_) => {
                if let Some(b) = val.as_bytes() {
                    return Ok(Value::bytes(b.to_vec()));
                }
                if let Some(s) = val.as_str() {
                    return Ok(Value::bytes(s.as_bytes().to_vec()));
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Array(elem_def) => {
                if let Some(arr) = val.as_array() {
                    let elem_type = match elem_def {
                        sqlparser::ast::ArrayElemTypeDef::AngleBracket(dt) => Some(dt.as_ref()),
                        sqlparser::ast::ArrayElemTypeDef::SquareBracket(dt, _) => Some(dt.as_ref()),
                        sqlparser::ast::ArrayElemTypeDef::Parenthesis(dt) => Some(dt.as_ref()),
                        sqlparser::ast::ArrayElemTypeDef::None => None,
                    };
                    if let Some(target_elem_type) = elem_type {
                        let mut result = Vec::with_capacity(arr.len());
                        for item in arr {
                            let cast_result = self.cast_value(item, target_elem_type, is_safe);
                            match cast_result {
                                Ok(v) => result.push(v),
                                Err(_) if is_safe => return Ok(Value::null()),
                                Err(e) => return Err(e),
                            }
                        }
                        return Ok(Value::array(result));
                    }
                    return Ok(Value::array(arr.to_vec()));
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::JSON => {
                let json_str = val.to_string();
                if let Ok(json_val) = serde_json::from_str::<serde_json::Value>(&json_str) {
                    return Ok(Value::json(json_val));
                }
                Ok(Value::json(serde_json::Value::String(json_str)))
            }
            _ => {
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::UnsupportedFeature(format!(
                    "CAST to {:?} not yet supported",
                    target_type
                )))
            }
        }
    }

    pub fn cast_value(
        &self,
        val: &Value,
        target_type: &sqlparser::ast::DataType,
        is_safe: bool,
    ) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }

        match target_type {
            sqlparser::ast::DataType::String(_)
            | sqlparser::ast::DataType::Varchar(_)
            | sqlparser::ast::DataType::Text => Ok(Value::string(val.to_string())),
            sqlparser::ast::DataType::Int64
            | sqlparser::ast::DataType::BigInt(_)
            | sqlparser::ast::DataType::Integer(_) => {
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::int64(f as i64));
                }
                if let Some(d) = val.as_numeric() {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(i) = d.to_i64() {
                        return Ok(Value::int64(i));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            sqlparser::ast::DataType::Float64 | sqlparser::ast::DataType::Double(_) => {
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(f));
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                if let Some(d) = val.as_numeric() {
                    use rust_decimal::prelude::ToPrimitive;
                    if let Some(f) = d.to_f64() {
                        return Ok(Value::float64(f));
                    }
                }
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            _ => {
                if is_safe {
                    return Ok(Value::null());
                }
                Err(Error::UnsupportedFeature(format!(
                    "Array element CAST to {:?} not yet supported",
                    target_type
                )))
            }
        }
    }

    fn evaluate_literal(&self, val: &SqlValue) -> Result<Value> {
        match val {
            SqlValue::Number(n, _) => {
                if let Ok(i) = n.parse::<i64>() {
                    Ok(Value::int64(i))
                } else if let Ok(d) = n.parse::<rust_decimal::Decimal>() {
                    Ok(Value::numeric(d))
                } else if let Ok(f) = n.parse::<f64>() {
                    Ok(Value::float64(f))
                } else {
                    Err(Error::ParseError(format!("Invalid number: {}", n)))
                }
            }
            SqlValue::SingleQuotedString(s) | SqlValue::DoubleQuotedString(s) => {
                Ok(Value::string(s.clone()))
            }
            SqlValue::SingleQuotedByteStringLiteral(s)
            | SqlValue::DoubleQuotedByteStringLiteral(s) => {
                Ok(Value::bytes(parse_byte_string_escapes(s)))
            }
            SqlValue::Boolean(b) => Ok(Value::bool_val(*b)),
            SqlValue::Null => Ok(Value::null()),
            SqlValue::HexStringLiteral(s) => {
                let bytes = hex::decode(s).unwrap_or_default();
                Ok(Value::bytes(bytes))
            }
            SqlValue::SingleQuotedRawStringLiteral(s)
            | SqlValue::DoubleQuotedRawStringLiteral(s) => Ok(Value::string(s.clone())),
            SqlValue::TripleSingleQuotedString(s) | SqlValue::TripleDoubleQuotedString(s) => {
                Ok(Value::string(s.clone()))
            }
            SqlValue::TripleSingleQuotedRawStringLiteral(s)
            | SqlValue::TripleDoubleQuotedRawStringLiteral(s) => Ok(Value::string(s.clone())),
            SqlValue::DollarQuotedString(dqs) => Ok(Value::string(dqs.value.clone())),
            _ => Err(Error::UnsupportedFeature(format!(
                "Literal type not yet supported: {:?}",
                val
            ))),
        }
    }

    fn evaluate_binary_op(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        if left.is_null() || right.is_null() {
            match op {
                BinaryOperator::And => {
                    if let Some(false) = left.as_bool() {
                        return Ok(Value::bool_val(false));
                    }
                    if let Some(false) = right.as_bool() {
                        return Ok(Value::bool_val(false));
                    }
                    return Ok(Value::null());
                }
                BinaryOperator::Or => {
                    if let Some(true) = left.as_bool() {
                        return Ok(Value::bool_val(true));
                    }
                    if let Some(true) = right.as_bool() {
                        return Ok(Value::bool_val(true));
                    }
                    return Ok(Value::null());
                }
                _ => return Ok(Value::null()),
            }
        }

        match op {
            BinaryOperator::Eq => Ok(Value::bool_val(left == right)),
            BinaryOperator::NotEq => Ok(Value::bool_val(left != right)),
            BinaryOperator::Lt => self.compare_values(left, right, |ord| ord.is_lt()),
            BinaryOperator::LtEq => self.compare_values(left, right, |ord| ord.is_le()),
            BinaryOperator::Gt => self.compare_values(left, right, |ord| ord.is_gt()),
            BinaryOperator::GtEq => self.compare_values(left, right, |ord| ord.is_ge()),
            BinaryOperator::And => {
                let l = left.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: left.data_type().to_string(),
                })?;
                let r = right.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: right.data_type().to_string(),
                })?;
                Ok(Value::bool_val(l && r))
            }
            BinaryOperator::Or => {
                let l = left.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: left.data_type().to_string(),
                })?;
                let r = right.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: right.data_type().to_string(),
                })?;
                Ok(Value::bool_val(l || r))
            }
            BinaryOperator::Plus => self.add_op(left, right),
            BinaryOperator::Minus => self.sub_op(left, right),
            BinaryOperator::Multiply => self.mul_op(left, right),
            BinaryOperator::Divide => {
                let l = left
                    .as_f64()
                    .or_else(|| left.as_i64().map(|i| i as f64))
                    .or_else(|| {
                        left.as_numeric().and_then(|d| {
                            use rust_decimal::prelude::ToPrimitive;
                            d.to_f64()
                        })
                    });
                let r = right
                    .as_f64()
                    .or_else(|| right.as_i64().map(|i| i as f64))
                    .or_else(|| {
                        right.as_numeric().and_then(|d| {
                            use rust_decimal::prelude::ToPrimitive;
                            d.to_f64()
                        })
                    });
                match (l, r) {
                    (Some(_), Some(0.0)) => Err(Error::DivisionByZero),
                    (Some(l), Some(r)) => Ok(Value::float64(l / r)),
                    _ => Err(Error::TypeMismatch {
                        expected: "numeric types".to_string(),
                        actual: format!("{:?} vs {:?}", left.data_type(), right.data_type()),
                    }),
                }
            }
            BinaryOperator::Modulo => {
                if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
                    if r == 0 {
                        return Err(Error::DivisionByZero);
                    }
                    return Ok(Value::int64(l % r));
                }
                Err(Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: format!("{:?}", left.data_type()),
                })
            }
            BinaryOperator::StringConcat => {
                let l_str = left.to_string();
                let r_str = right.to_string();
                Ok(Value::string(format!("{}{}", l_str, r_str)))
            }
            _ => Err(Error::UnsupportedFeature(format!(
                "Binary operator not yet supported: {:?}",
                op
            ))),
        }
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, pred: F) -> Result<Value>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_f64(), right.as_f64()) {
            return Ok(Value::bool_val(pred(
                l.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal),
            )));
        }
        if let (Some(l), Some(r)) = (left.as_str(), right.as_str()) {
            return Ok(Value::bool_val(pred(l.cmp(r))));
        }
        if let Some(l) = left.as_i64() {
            if let Some(r) = right.as_f64() {
                return Ok(Value::bool_val(pred(
                    (l as f64)
                        .partial_cmp(&r)
                        .unwrap_or(std::cmp::Ordering::Equal),
                )));
            }
        }
        if let Some(l) = left.as_f64() {
            if let Some(r) = right.as_i64() {
                return Ok(Value::bool_val(pred(
                    l.partial_cmp(&(r as f64))
                        .unwrap_or(std::cmp::Ordering::Equal),
                )));
            }
        }
        if let Some(l) = left.as_f64() {
            if let Some(r) = right.as_numeric() {
                use rust_decimal::prelude::ToPrimitive;
                if let Some(r_f64) = r.to_f64() {
                    return Ok(Value::bool_val(pred(
                        l.partial_cmp(&r_f64).unwrap_or(std::cmp::Ordering::Equal),
                    )));
                }
            }
        }
        if let Some(l) = left.as_numeric() {
            if let Some(r) = right.as_f64() {
                use rust_decimal::prelude::ToPrimitive;
                if let Some(l_f64) = l.to_f64() {
                    return Ok(Value::bool_val(pred(
                        l_f64.partial_cmp(&r).unwrap_or(std::cmp::Ordering::Equal),
                    )));
                }
            }
        }
        if let Some(l) = left.as_i64() {
            if let Some(r) = right.as_numeric() {
                let l_dec = rust_decimal::Decimal::from(l);
                return Ok(Value::bool_val(pred(l_dec.cmp(&r))));
            }
        }
        if let Some(l) = left.as_numeric() {
            if let Some(r) = right.as_i64() {
                let r_dec = rust_decimal::Decimal::from(r);
                return Ok(Value::bool_val(pred(l.cmp(&r_dec))));
            }
        }
        if let (Some(l), Some(r)) = (left.as_date(), right.as_date()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_timestamp(), right.as_timestamp()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_time(), right.as_time()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_numeric(), right.as_numeric()) {
            return Ok(Value::bool_val(pred(l.cmp(&r))));
        }
        if let (Some(l), Some(r)) = (left.as_bytes(), right.as_bytes()) {
            return Ok(Value::bool_val(pred(l.cmp(r))));
        }
        if let (Some(l), Some(r)) = (left.as_interval(), right.as_interval()) {
            use yachtsql_common::types::IntervalValue;
            let l_total_nanos = (l.months as i64)
                * 30
                * 24
                * IntervalValue::MICROS_PER_HOUR
                * IntervalValue::NANOS_PER_MICRO
                + (l.days as i64)
                    * 24
                    * IntervalValue::MICROS_PER_HOUR
                    * IntervalValue::NANOS_PER_MICRO
                + l.nanos;
            let r_total_nanos = (r.months as i64)
                * 30
                * 24
                * IntervalValue::MICROS_PER_HOUR
                * IntervalValue::NANOS_PER_MICRO
                + (r.days as i64)
                    * 24
                    * IntervalValue::MICROS_PER_HOUR
                    * IntervalValue::NANOS_PER_MICRO
                + r.nanos;
            return Ok(Value::bool_val(pred(l_total_nanos.cmp(&r_total_nanos))));
        }
        Err(Error::TypeMismatch {
            expected: "comparable types".to_string(),
            actual: format!("{:?} vs {:?}", left.data_type(), right.data_type()),
        })
    }

    fn numeric_op<F, G, H>(
        &self,
        left: &Value,
        right: &Value,
        int_op: F,
        float_op: G,
        decimal_op: H,
    ) -> Result<Value>
    where
        F: Fn(i64, i64) -> i64,
        G: Fn(f64, f64) -> f64,
        H: Fn(rust_decimal::Decimal, rust_decimal::Decimal) -> rust_decimal::Decimal,
    {
        if let (Some(l), Some(r)) = (left.as_i64(), right.as_i64()) {
            return Ok(Value::int64(int_op(l, r)));
        }
        if let (Some(l), Some(r)) = (left.as_numeric(), right.as_numeric()) {
            return Ok(Value::numeric(decimal_op(l, r)));
        }
        let l = left
            .as_f64()
            .or_else(|| left.as_i64().map(|i| i as f64))
            .or_else(|| {
                left.as_numeric().and_then(|d| {
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                })
            });
        let r = right
            .as_f64()
            .or_else(|| right.as_i64().map(|i| i as f64))
            .or_else(|| {
                right.as_numeric().and_then(|d| {
                    use rust_decimal::prelude::ToPrimitive;
                    d.to_f64()
                })
            });
        if let (Some(l), Some(r)) = (l, r) {
            return Ok(Value::float64(float_op(l, r)));
        }
        Err(Error::TypeMismatch {
            expected: "numeric types".to_string(),
            actual: format!("{:?} vs {:?}", left.data_type(), right.data_type()),
        })
    }

    fn add_op(&self, left: &Value, right: &Value) -> Result<Value> {
        use chrono::{Duration, Months};
        use yachtsql_common::types::IntervalValue;

        if let Some(date) = left.as_date() {
            if let Some(interval) = right.as_interval() {
                let result = if interval.months != 0 {
                    date.checked_add_months(Months::new(interval.months as u32))
                        .ok_or_else(|| {
                            Error::InvalidQuery("Date overflow when adding interval".to_string())
                        })?
                } else {
                    date
                };
                let result = result
                    .checked_add_signed(Duration::days(interval.days as i64))
                    .ok_or_else(|| {
                        Error::InvalidQuery("Date overflow when adding interval".to_string())
                    })?;
                return Ok(Value::date(result));
            }
        }

        if let Some(ts) = left.as_timestamp() {
            if let Some(interval) = right.as_interval() {
                let result = if interval.months != 0 {
                    let date = ts.date_naive();
                    let new_date = date
                        .checked_add_months(Months::new(interval.months as u32))
                        .ok_or_else(|| {
                            Error::InvalidQuery(
                                "Timestamp overflow when adding interval".to_string(),
                            )
                        })?;
                    ts.with_day(1)
                        .unwrap()
                        .with_month(new_date.month())
                        .unwrap()
                        .with_day(
                            new_date.day().min(
                                chrono::NaiveDate::from_ymd_opt(
                                    new_date.year(),
                                    new_date.month(),
                                    1,
                                )
                                .unwrap()
                                .with_month(new_date.month() % 12 + 1)
                                .map(|d| d.pred_opt().unwrap().day())
                                .unwrap_or(28),
                            ),
                        )
                        .unwrap_or(ts)
                } else {
                    ts
                };
                let result = result + Duration::days(interval.days as i64);
                let nanos_to_add = interval.nanos;
                let result = result + Duration::nanoseconds(nanos_to_add);
                return Ok(Value::timestamp(result));
            }
        }

        if let (Some(i1), Some(i2)) = (left.as_interval(), right.as_interval()) {
            return Ok(Value::interval(IntervalValue {
                months: i1.months + i2.months,
                days: i1.days + i2.days,
                nanos: i1.nanos + i2.nanos,
            }));
        }

        self.numeric_op(left, right, |a, b| a + b, |a, b| a + b, |a, b| a + b)
    }

    fn sub_op(&self, left: &Value, right: &Value) -> Result<Value> {
        use chrono::{Duration, Months};
        use yachtsql_common::types::IntervalValue;

        if let Some(date) = left.as_date() {
            if let Some(interval) = right.as_interval() {
                let result = if interval.months != 0 {
                    date.checked_sub_months(Months::new(interval.months as u32))
                        .ok_or_else(|| {
                            Error::InvalidQuery(
                                "Date overflow when subtracting interval".to_string(),
                            )
                        })?
                } else {
                    date
                };
                let result = result
                    .checked_sub_signed(Duration::days(interval.days as i64))
                    .ok_or_else(|| {
                        Error::InvalidQuery("Date overflow when subtracting interval".to_string())
                    })?;
                return Ok(Value::date(result));
            }
        }

        if let Some(ts) = left.as_timestamp() {
            if let Some(interval) = right.as_interval() {
                let result = if interval.months != 0 {
                    let date = ts.date_naive();
                    let new_date = date
                        .checked_sub_months(Months::new(interval.months as u32))
                        .ok_or_else(|| {
                            Error::InvalidQuery(
                                "Timestamp overflow when subtracting interval".to_string(),
                            )
                        })?;
                    ts.with_day(1)
                        .unwrap()
                        .with_month(new_date.month())
                        .unwrap()
                        .with_year(new_date.year())
                        .unwrap()
                        .with_day(new_date.day())
                        .unwrap_or(ts)
                } else {
                    ts
                };
                let result = result - Duration::days(interval.days as i64);
                let result = result - Duration::nanoseconds(interval.nanos);
                return Ok(Value::timestamp(result));
            }
        }

        if let (Some(i1), Some(i2)) = (left.as_interval(), right.as_interval()) {
            return Ok(Value::interval(IntervalValue {
                months: i1.months - i2.months,
                days: i1.days - i2.days,
                nanos: i1.nanos - i2.nanos,
            }));
        }

        self.numeric_op(left, right, |a, b| a - b, |a, b| a - b, |a, b| a - b)
    }

    fn mul_op(&self, left: &Value, right: &Value) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;

        if let Some(interval) = left.as_interval() {
            if let Some(scalar) = right.as_i64() {
                return Ok(Value::interval(IntervalValue {
                    months: interval.months * scalar as i32,
                    days: interval.days * scalar as i32,
                    nanos: interval.nanos * scalar,
                }));
            }
        }

        if let Some(scalar) = left.as_i64() {
            if let Some(interval) = right.as_interval() {
                return Ok(Value::interval(IntervalValue {
                    months: interval.months * scalar as i32,
                    days: interval.days * scalar as i32,
                    nanos: interval.nanos * scalar,
                }));
            }
        }

        self.numeric_op(left, right, |a, b| a * b, |a, b| a * b, |a, b| a * b)
    }

    fn evaluate_unary_op(&self, op: &UnaryOperator, val: &Value) -> Result<Value> {
        match op {
            UnaryOperator::Not => {
                if val.is_null() {
                    return Ok(Value::null());
                }
                let b = val.as_bool().ok_or_else(|| Error::TypeMismatch {
                    expected: "BOOL".to_string(),
                    actual: val.data_type().to_string(),
                })?;
                Ok(Value::bool_val(!b))
            }
            UnaryOperator::Minus => {
                if val.is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = val.as_i64() {
                    return Ok(Value::int64(-i));
                }
                if let Some(d) = val.as_numeric() {
                    return Ok(Value::numeric(-d));
                }
                if let Some(f) = val.as_f64() {
                    return Ok(Value::float64(-f));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: val.data_type().to_string(),
                })
            }
            UnaryOperator::Plus => Ok(val.clone()),
            _ => Err(Error::UnsupportedFeature(format!(
                "Unary operator not yet supported: {:?}",
                op
            ))),
        }
    }

    pub fn evaluate_function(
        &self,
        name: &str,
        args: &[Value],
        func: &sqlparser::ast::Function,
        _record: &Record,
    ) -> Result<Value> {
        self.evaluate_function_impl(name, args)
    }

    pub fn evaluate_function_internal(
        &self,
        func: &sqlparser::ast::Function,
        record: &Record,
    ) -> Result<Value> {
        let name = func.name.to_string().to_uppercase();
        if let Some(udf_result) = self.try_evaluate_udf(&name, func, record)? {
            return Ok(udf_result);
        }
        if name == "MAKE_INTERVAL" {
            return self.evaluate_make_interval(func, record);
        }
        let args = self.extract_function_args(func, record)?;
        self.evaluate_function_impl(&name, &args)
    }

    fn evaluate_function_impl(&self, name: &str, args: &[Value]) -> Result<Value> {
        match name {
            "COALESCE" => {
                for val in args {
                    if !val.is_null() {
                        return Ok(val.clone());
                    }
                }
                Ok(Value::null())
            }
            "NULLIF" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "NULLIF requires 2 arguments".to_string(),
                    ));
                }
                if args[0] == args[1] {
                    Ok(Value::null())
                } else {
                    Ok(args[0].clone())
                }
            }
            "NULLIFZERO" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "NULLIFZERO requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let is_zero = args[0].as_i64().map(|i| i == 0).unwrap_or(false)
                    || args[0].as_f64().map(|f| f == 0.0).unwrap_or(false)
                    || args[0]
                        .as_numeric()
                        .map(|d| d == rust_decimal::Decimal::ZERO)
                        .unwrap_or(false);
                if is_zero {
                    Ok(Value::null())
                } else {
                    Ok(args[0].clone())
                }
            }
            "ZEROIFNULL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ZEROIFNULL requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    Ok(Value::int64(0))
                } else {
                    Ok(args[0].clone())
                }
            }
            "IFNULL" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "IFNULL requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[0].clone())
                }
            }
            "IF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery("IF requires 3 arguments".to_string()));
                }
                if let Some(true) = args[0].as_bool() {
                    Ok(args[1].clone())
                } else {
                    Ok(args[2].clone())
                }
            }
            "UPPER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("UPPER requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.to_uppercase()))
            }
            "LOWER" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LOWER requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.to_lowercase()))
            }
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "LENGTH requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = args[0].as_bytes() {
                    return Ok(Value::int64(bytes.len() as i64));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::int64(s.chars().count() as i64))
            }
            "BYTE_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "BYTE_LENGTH requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = args[0].as_bytes() {
                    return Ok(Value::int64(bytes.len() as i64));
                }
                if let Some(s) = args[0].as_str() {
                    return Ok(Value::int64(s.len() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "TRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("TRIM requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.trim().to_string()))
            }
            "LTRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LTRIM requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.trim_start().to_string()))
            }
            "RTRIM" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("RTRIM requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.trim_end().to_string()))
            }
            "CONCAT" => {
                let all_bytes = args.iter().all(|v| v.is_null() || v.as_bytes().is_some());
                if all_bytes && args.iter().any(|v| v.as_bytes().is_some()) {
                    let mut result = Vec::new();
                    for val in args {
                        if let Some(b) = val.as_bytes() {
                            result.extend(b);
                        }
                    }
                    Ok(Value::bytes(result))
                } else {
                    let mut result = String::new();
                    for val in args {
                        if !val.is_null() {
                            result.push_str(&val.to_string());
                        }
                    }
                    Ok(Value::string(result))
                }
            }
            "SUBSTR" | "SUBSTRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "SUBSTR requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let start = args[1].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let start_idx = if start > 0 { (start - 1) as usize } else { 0 };

                if let Some(bytes) = args[0].as_bytes() {
                    if start_idx >= bytes.len() {
                        return Ok(Value::bytes(Vec::new()));
                    }
                    let result = if args.len() == 3 {
                        let len = args[2].as_i64().ok_or_else(|| Error::TypeMismatch {
                            expected: "INT64".to_string(),
                            actual: args[2].data_type().to_string(),
                        })? as usize;
                        bytes[start_idx..].iter().take(len).cloned().collect()
                    } else {
                        bytes[start_idx..].to_vec()
                    };
                    return Ok(Value::bytes(result));
                }

                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let chars: Vec<char> = s.chars().collect();
                if start_idx >= chars.len() {
                    return Ok(Value::string(String::new()));
                }
                let result: String = if args.len() == 3 {
                    let len = args[2].as_i64().ok_or_else(|| Error::TypeMismatch {
                        expected: "INT64".to_string(),
                        actual: args[2].data_type().to_string(),
                    })? as usize;
                    chars[start_idx..].iter().take(len).collect()
                } else {
                    chars[start_idx..].iter().collect()
                };
                Ok(Value::string(result))
            }
            "REPLACE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "REPLACE requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let from = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let to = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                Ok(Value::string(s.replace(from, to)))
            }
            "ABS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("ABS requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i.abs()));
                }
                if let Some(d) = args[0].as_numeric() {
                    return Ok(Value::numeric(d.abs()));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f.abs()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "CEIL" | "CEILING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("CEIL requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(d) = args[0].as_numeric() {
                    use rust_decimal::prelude::ToPrimitive;
                    let ceiled = d.ceil();
                    return Ok(Value::numeric(ceiled));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f.ceil()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "FLOOR" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("FLOOR requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(d) = args[0].as_numeric() {
                    let floored = d.floor();
                    return Ok(Value::numeric(floored));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f.floor()));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "ROUND" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "ROUND requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let decimals = if args.len() == 2 {
                    args[1].as_i64().unwrap_or(0)
                } else {
                    0
                };
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(d) = args[0].as_numeric() {
                    let rounded = d.round_dp(decimals.max(0) as u32);
                    return Ok(Value::numeric(rounded));
                }
                if let Some(f) = args[0].as_f64() {
                    let multiplier = 10f64.powi(decimals as i32);
                    return Ok(Value::float64((f * multiplier).round() / multiplier));
                }
                Err(Error::TypeMismatch {
                    expected: "numeric".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "MOD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery("MOD requires 2 arguments".to_string()));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let a = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let b = args[1].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                if b == 0 {
                    return Err(Error::DivisionByZero);
                }
                Ok(Value::int64(a % b))
            }
            "GREATEST" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "GREATEST requires at least 1 argument".to_string(),
                    ));
                }
                let mut max: Option<Value> = None;
                for val in args {
                    if val.is_null() {
                        continue;
                    }
                    match &max {
                        None => max = Some(val.clone()),
                        Some(m) => {
                            if self.compare_for_ordering(val, m) == std::cmp::Ordering::Greater {
                                max = Some(val.clone());
                            }
                        }
                    }
                }
                Ok(max.unwrap_or_else(Value::null))
            }
            "LEAST" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "LEAST requires at least 1 argument".to_string(),
                    ));
                }
                let mut min: Option<Value> = None;
                for val in args {
                    if val.is_null() {
                        continue;
                    }
                    match &min {
                        None => min = Some(val.clone()),
                        Some(m) => {
                            if self.compare_for_ordering(val, m) == std::cmp::Ordering::Less {
                                min = Some(val.clone());
                            }
                        }
                    }
                }
                Ok(min.unwrap_or_else(Value::null))
            }
            "ARRAY_LENGTH" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_LENGTH requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(arr) = args[0].as_array() {
                    return Ok(Value::int64(arr.len() as i64));
                }
                Err(Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "SAFE_DIVIDE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "SAFE_DIVIDE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let divisor = args[1]
                    .as_f64()
                    .or_else(|| args[1].as_i64().map(|i| i as f64))
                    .or_else(|| {
                        args[1].as_numeric().and_then(|d| {
                            use rust_decimal::prelude::ToPrimitive;
                            d.to_f64()
                        })
                    });
                if divisor == Some(0.0) {
                    return Ok(Value::null());
                }
                let dividend = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64))
                    .or_else(|| {
                        args[0].as_numeric().and_then(|d| {
                            use rust_decimal::prelude::ToPrimitive;
                            d.to_f64()
                        })
                    });
                match (dividend, divisor) {
                    (Some(a), Some(b)) => Ok(Value::float64(a / b)),
                    _ => Ok(Value::null()),
                }
            }
            "SAFE_ADD" | "SAFE_SUBTRACT" | "SAFE_MULTIPLY" | "SAFE_NEGATE" => match name {
                "SAFE_ADD" => {
                    if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match (args[0].as_i64(), args[1].as_i64()) {
                        (Some(a), Some(b)) => match a.checked_add(b) {
                            Some(result) => Ok(Value::int64(result)),
                            None => Ok(Value::null()),
                        },
                        _ => match (args[0].as_f64(), args[1].as_f64()) {
                            (Some(a), Some(b)) => Ok(Value::float64(a + b)),
                            _ => Ok(Value::null()),
                        },
                    }
                }
                "SAFE_SUBTRACT" => {
                    if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match (args[0].as_i64(), args[1].as_i64()) {
                        (Some(a), Some(b)) => match a.checked_sub(b) {
                            Some(result) => Ok(Value::int64(result)),
                            None => Ok(Value::null()),
                        },
                        _ => match (args[0].as_f64(), args[1].as_f64()) {
                            (Some(a), Some(b)) => {
                                if a <= i64::MIN as f64 && b > 0.0 {
                                    return Ok(Value::null());
                                }
                                if a >= (i64::MAX as f64) + 1.0 && b < 0.0 {
                                    return Ok(Value::null());
                                }
                                let result = a - b;
                                if result >= (i64::MAX as f64) + 1.0 || result < i64::MIN as f64 {
                                    return Ok(Value::null());
                                }
                                Ok(Value::float64(result))
                            }
                            _ => Ok(Value::null()),
                        },
                    }
                }
                "SAFE_MULTIPLY" => {
                    if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                        return Ok(Value::null());
                    }
                    match (args[0].as_i64(), args[1].as_i64()) {
                        (Some(a), Some(b)) => match a.checked_mul(b) {
                            Some(result) => Ok(Value::int64(result)),
                            None => Ok(Value::null()),
                        },
                        _ => match (args[0].as_f64(), args[1].as_f64()) {
                            (Some(a), Some(b)) => Ok(Value::float64(a * b)),
                            _ => Ok(Value::null()),
                        },
                    }
                }
                "SAFE_NEGATE" => {
                    if args.len() != 1 || args[0].is_null() {
                        return Ok(Value::null());
                    }
                    if let Some(i) = args[0].as_i64() {
                        return match i.checked_neg() {
                            Some(result) => Ok(Value::int64(result)),
                            None => Ok(Value::null()),
                        };
                    }
                    if let Some(f) = args[0].as_f64() {
                        let result = -f;
                        if result >= (i64::MAX as f64) + 1.0 || result < i64::MIN as f64 {
                            return Ok(Value::null());
                        }
                        return Ok(Value::float64(result));
                    }
                    Ok(Value::null())
                }
                _ => Ok(Value::null()),
            },
            "MD5" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("MD5 requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = args[0].as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = args[0].as_bytes() {
                    b.to_vec()
                } else {
                    return Ok(Value::null());
                };
                let digest = md5::compute(&input);
                Ok(Value::bytes(digest.to_vec()))
            }
            "SHA256" => {
                use sha2::{Digest, Sha256};
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SHA256 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = args[0].as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = args[0].as_bytes() {
                    b.to_vec()
                } else {
                    return Ok(Value::null());
                };
                let mut hasher = Sha256::new();
                hasher.update(&input);
                let result = hasher.finalize();
                Ok(Value::bytes(result.to_vec()))
            }
            "SHA512" => {
                use sha2::{Digest, Sha512};
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SHA512 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let input = if let Some(s) = args[0].as_str() {
                    s.as_bytes().to_vec()
                } else if let Some(b) = args[0].as_bytes() {
                    b.to_vec()
                } else {
                    return Ok(Value::null());
                };
                let mut hasher = Sha512::new();
                hasher.update(&input);
                let result = hasher.finalize();
                Ok(Value::bytes(result.to_vec()))
            }
            "INT64" | "INT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("INT64 requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::int64(f as i64));
                }
                if let Some(jv) = args[0].as_json() {
                    match jv {
                        serde_json::Value::Number(n) => {
                            if let Some(i) = n.as_i64() {
                                return Ok(Value::int64(i));
                            }
                            if let Some(f) = n.as_f64() {
                                return Ok(Value::int64(f as i64));
                            }
                        }
                        serde_json::Value::String(s) => {
                            if let Ok(i) = s.parse::<i64>() {
                                return Ok(Value::int64(i));
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(s) = args[0].as_str() {
                    if let Ok(i) = s.parse::<i64>() {
                        return Ok(Value::int64(i));
                    }
                }
                if let Some(b) = args[0].as_bool() {
                    return Ok(Value::int64(if b { 1 } else { 0 }));
                }
                Ok(Value::null())
            }
            "FLOAT64" | "FLOAT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FLOAT64 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(f) = args[0].as_f64() {
                    return Ok(Value::float64(f));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::float64(i as f64));
                }
                if let Some(jv) = args[0].as_json() {
                    match jv {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                return Ok(Value::float64(f));
                            }
                        }
                        serde_json::Value::String(s) => {
                            if let Ok(f) = s.parse::<f64>() {
                                return Ok(Value::float64(f));
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(s) = args[0].as_str() {
                    if let Ok(f) = s.parse::<f64>() {
                        return Ok(Value::float64(f));
                    }
                }
                Ok(Value::null())
            }
            "STRING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "STRING requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(jv) = args[0].as_json() {
                    let result = match jv {
                        serde_json::Value::String(s) => s.clone(),
                        serde_json::Value::Number(n) => n.to_string(),
                        serde_json::Value::Bool(b) => b.to_string(),
                        serde_json::Value::Null => return Ok(Value::null()),
                        other => other.to_string(),
                    };
                    return Ok(Value::string(result));
                }
                Ok(Value::string(args[0].to_string()))
            }
            "BOOL" | "BOOLEAN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("BOOL requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bool() {
                    return Ok(Value::bool_val(b));
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::bool_val(i != 0));
                }
                if let Some(jv) = args[0].as_json() {
                    match jv {
                        serde_json::Value::Bool(b) => return Ok(Value::bool_val(*b)),
                        serde_json::Value::String(s) => {
                            let lower = s.to_lowercase();
                            if lower == "true" || lower == "1" {
                                return Ok(Value::bool_val(true));
                            }
                            if lower == "false" || lower == "0" {
                                return Ok(Value::bool_val(false));
                            }
                        }
                        _ => {}
                    }
                }
                if let Some(s) = args[0].as_str() {
                    let lower = s.to_lowercase();
                    if lower == "true" || lower == "1" {
                        return Ok(Value::bool_val(true));
                    }
                    if lower == "false" || lower == "0" {
                        return Ok(Value::bool_val(false));
                    }
                }
                Ok(Value::null())
            }
            "SAFE_CONVERT_BYTES_TO_STRING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "SAFE_CONVERT_BYTES_TO_STRING requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bytes() {
                    match String::from_utf8(b.to_vec()) {
                        Ok(s) => return Ok(Value::string(s)),
                        Err(_) => return Ok(Value::null()),
                    }
                }
                Ok(Value::null())
            }
            "FROM_BASE64" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FROM_BASE64 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                use base64::Engine;
                match base64::engine::general_purpose::STANDARD.decode(s) {
                    Ok(bytes) => Ok(Value::bytes(bytes)),
                    Err(_) => Ok(Value::null()),
                }
            }
            "TO_BASE64" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TO_BASE64 requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let bytes = args[0].as_bytes().ok_or_else(|| Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                use base64::Engine;
                let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
                Ok(Value::string(encoded))
            }
            "FROM_HEX" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "FROM_HEX requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match hex::decode(s) {
                    Ok(bytes) => Ok(Value::bytes(bytes)),
                    Err(_) => Ok(Value::null()),
                }
            }
            "TO_HEX" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TO_HEX requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let bytes = args[0].as_bytes().ok_or_else(|| Error::TypeMismatch {
                    expected: "BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(hex::encode(bytes)))
            }
            "REGEXP_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_CONTAINS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match regex::Regex::new(pattern) {
                    Ok(re) => Ok(Value::bool_val(re.is_match(text))),
                    Err(_) => Ok(Value::null()),
                }
            }
            "REGEXP_EXTRACT" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_EXTRACT requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match regex::Regex::new(pattern) {
                    Ok(re) => {
                        if let Some(caps) = re.captures(text) {
                            let group_idx = if args.len() == 3 {
                                args[2].as_i64().unwrap_or(0) as usize
                            } else if re.captures_len() > 1 {
                                1
                            } else {
                                0
                            };
                            if let Some(m) = caps.get(group_idx) {
                                return Ok(Value::string(m.as_str().to_string()));
                            }
                        }
                        Ok(Value::null())
                    }
                    Err(_) => Ok(Value::null()),
                }
            }
            "REGEXP_EXTRACT_ALL" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_EXTRACT_ALL requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match regex::Regex::new(pattern) {
                    Ok(re) => {
                        let group_idx = if args.len() == 3 {
                            args[2].as_i64().unwrap_or(0) as usize
                        } else if re.captures_len() > 1 {
                            1
                        } else {
                            0
                        };
                        let matches: Vec<Value> = re
                            .captures_iter(text)
                            .filter_map(|caps| caps.get(group_idx))
                            .map(|m| Value::string(m.as_str().to_string()))
                            .collect();
                        Ok(Value::array(matches))
                    }
                    Err(_) => Ok(Value::null()),
                }
            }
            "REGEXP_SUBSTR" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_SUBSTR requires at least 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let position = if args.len() > 2 {
                    args[2].as_i64().unwrap_or(1).max(1) as usize - 1
                } else {
                    0
                };
                let occurrence = if args.len() > 3 {
                    args[3].as_i64().unwrap_or(1).max(1) as usize
                } else {
                    1
                };
                let search_text = if position < text.len() {
                    &text[position..]
                } else {
                    ""
                };
                match regex::Regex::new(pattern) {
                    Ok(re) => {
                        if let Some(m) = re.find_iter(search_text).nth(occurrence - 1) {
                            Ok(Value::string(m.as_str().to_string()))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    Err(_) => Ok(Value::null()),
                }
            }
            "JSON_EXTRACT_SCALAR" | "JSON_VALUE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "JSON_EXTRACT_SCALAR requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let path = args[1].as_str().unwrap_or_default();
                let json_val_opt = if let Some(jv) = args[0].as_json() {
                    Some(jv.clone())
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s).ok()
                } else {
                    None
                };
                if let Some(json_val) = json_val_opt {
                    let result = self.json_path_extract(&json_val, path);
                    return Ok(result
                        .map(|v| match v {
                            serde_json::Value::String(s) => Value::string(s),
                            serde_json::Value::Number(n) => {
                                if let Some(i) = n.as_i64() {
                                    Value::string(i.to_string())
                                } else if let Some(f) = n.as_f64() {
                                    Value::string(f.to_string())
                                } else {
                                    Value::null()
                                }
                            }
                            serde_json::Value::Bool(b) => Value::string(b.to_string()),
                            serde_json::Value::Null => Value::null(),
                            _ => Value::null(),
                        })
                        .unwrap_or_else(Value::null));
                }
                Ok(Value::null())
            }
            "JSON_QUERY" | "JSON_EXTRACT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "JSON_QUERY requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let path = args[1].as_str().unwrap_or_default();
                let json_val_opt = if let Some(jv) = args[0].as_json() {
                    Some(jv.clone())
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s).ok()
                } else {
                    None
                };
                if let Some(json_val) = json_val_opt {
                    let result = self.json_path_extract(&json_val, path);
                    return Ok(result
                        .map(|v| Value::string(v.to_string()))
                        .unwrap_or_else(Value::null));
                }
                Ok(Value::null())
            }
            "JSON_TYPE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "JSON_TYPE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val_opt = if let Some(jv) = args[0].as_json() {
                    Some(jv.clone())
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s).ok()
                } else {
                    None
                };
                if let Some(json_val) = json_val_opt {
                    let type_name = match json_val {
                        serde_json::Value::Null => "null",
                        serde_json::Value::Bool(_) => "boolean",
                        serde_json::Value::Number(_) => "number",
                        serde_json::Value::String(_) => "string",
                        serde_json::Value::Array(_) => "array",
                        serde_json::Value::Object(_) => "object",
                    };
                    return Ok(Value::string(type_name.to_string()));
                }
                Ok(Value::null())
            }
            "BIT_COUNT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "BIT_COUNT requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i.count_ones() as i64));
                }
                if let Some(b) = args[0].as_bytes() {
                    let count: u32 = b.iter().map(|byte| byte.count_ones()).sum();
                    return Ok(Value::int64(count as i64));
                }
                Ok(Value::null())
            }
            "LEFT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery("LEFT requires 2 arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[1].as_i64().unwrap_or(0) as usize;
                if let Some(bytes) = args[0].as_bytes() {
                    let result: Vec<u8> = bytes.iter().take(n).cloned().collect();
                    return Ok(Value::bytes(result));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let result: String = s.chars().take(n).collect();
                Ok(Value::string(result))
            }
            "RIGHT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RIGHT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[1].as_i64().unwrap_or(0) as usize;
                if let Some(bytes) = args[0].as_bytes() {
                    let start = bytes.len().saturating_sub(n);
                    let result: Vec<u8> = bytes[start..].to_vec();
                    return Ok(Value::bytes(result));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let chars: Vec<char> = s.chars().collect();
                let start = chars.len().saturating_sub(n);
                let result: String = chars[start..].iter().collect();
                Ok(Value::string(result))
            }
            "REVERSE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "REVERSE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(bytes) = args[0].as_bytes() {
                    let result: Vec<u8> = bytes.iter().rev().cloned().collect();
                    return Ok(Value::bytes(result));
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING or BYTES".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::string(s.chars().rev().collect::<String>()))
            }
            "REPEAT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "REPEAT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let n = args[1].as_i64().unwrap_or(0) as usize;
                Ok(Value::string(s.repeat(n)))
            }
            "STARTS_WITH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "STARTS_WITH requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let prefix = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(s.starts_with(prefix)))
            }
            "ENDS_WITH" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "ENDS_WITH requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let suffix = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(s.ends_with(suffix)))
            }
            "CONTAINS_SUBSTR" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "CONTAINS_SUBSTR requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let substr = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(
                    s.to_lowercase().contains(&substr.to_lowercase()),
                ))
            }
            "INSTR" | "STRPOS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "INSTR requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let substr = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match s.find(substr) {
                    Some(idx) => Ok(Value::int64((idx + 1) as i64)),
                    None => Ok(Value::int64(0)),
                }
            }
            "SPLIT" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::InvalidQuery(
                        "SPLIT requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let delimiter = if args.len() == 2 {
                    args[1].as_str().unwrap_or(",")
                } else {
                    ","
                };
                let parts: Vec<Value> = s
                    .split(delimiter)
                    .map(|p| Value::string(p.to_string()))
                    .collect();
                Ok(Value::array(parts))
            }
            "POWER" | "POW" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "POWER requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let base = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                let exp = args[1]
                    .as_f64()
                    .or_else(|| args[1].as_i64().map(|i| i as f64));
                match (base, exp) {
                    (Some(b), Some(e)) => Ok(Value::float64(b.powf(e))),
                    _ => Ok(Value::null()),
                }
            }
            "SQRT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("SQRT requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) if v >= 0.0 => Ok(Value::float64(v.sqrt())),
                    Some(_) => Ok(Value::null()),
                    None => Ok(Value::null()),
                }
            }
            "LOG" | "LN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LOG requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) if v > 0.0 => Ok(Value::float64(v.ln())),
                    _ => Ok(Value::null()),
                }
            }
            "LOG10" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("LOG10 requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) if v > 0.0 => Ok(Value::float64(v.log10())),
                    _ => Ok(Value::null()),
                }
            }
            "EXP" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("EXP requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let n = args[0]
                    .as_f64()
                    .or_else(|| args[0].as_i64().map(|i| i as f64));
                match n {
                    Some(v) => Ok(Value::float64(v.exp())),
                    None => Ok(Value::null()),
                }
            }
            "SIGN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery("SIGN requires 1 argument".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i.signum()));
                }
                if let Some(f) = args[0].as_f64() {
                    if f > 0.0 {
                        return Ok(Value::int64(1));
                    }
                    if f < 0.0 {
                        return Ok(Value::int64(-1));
                    }
                    return Ok(Value::int64(0));
                }
                Ok(Value::null())
            }
            "TRUNC" | "TRUNCATE" => {
                if args.len() != 1 && args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TRUNC requires 1 or 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let decimals = if args.len() == 2 {
                    args[1].as_i64().unwrap_or(0) as i32
                } else {
                    0
                };
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i));
                }
                if let Some(d) = args[0].as_numeric() {
                    let truncated = d.trunc_with_scale(decimals.max(0) as u32);
                    return Ok(Value::numeric(truncated));
                }
                if let Some(f) = args[0].as_f64() {
                    let multiplier = 10f64.powi(decimals);
                    return Ok(Value::float64((f * multiplier).trunc() / multiplier));
                }
                Ok(Value::null())
            }
            "DATE" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery("DATE requires arguments".to_string()));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(s) = args[0].as_str() {
                    if let Ok(date) = chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                        return Ok(Value::date(date));
                    }
                }
                Ok(Value::null())
            }
            "CURRENT_DATE" => {
                let today = chrono::Utc::now().date_naive();
                Ok(Value::date(today))
            }
            "CURRENT_TIMESTAMP" | "CURRENT_DATETIME" | "NOW" => {
                let now = chrono::Utc::now();
                Ok(Value::timestamp(now))
            }
            "DATE_ADD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_ADD requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let date = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_date = if interval.months != 0 {
                    date.checked_add_months(chrono::Months::new(interval.months as u32))
                        .unwrap_or(date)
                } else {
                    date.checked_add_signed(chrono::Duration::days(interval.days as i64))
                        .unwrap_or(date)
                };
                Ok(Value::date(new_date))
            }
            "DATE_SUB" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_SUB requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let date = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_date = if interval.months != 0 {
                    date.checked_sub_months(chrono::Months::new(interval.months as u32))
                        .unwrap_or(date)
                } else {
                    date.checked_sub_signed(chrono::Duration::days(interval.days as i64))
                        .unwrap_or(date)
                };
                Ok(Value::date(new_date))
            }
            "DATE_DIFF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "DATE_DIFF requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let date1 = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let date2 = args[1].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let unit = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let diff = match unit.to_uppercase().as_str() {
                    "DAY" => (date1 - date2).num_days(),
                    "WEEK" => (date1 - date2).num_weeks(),
                    "MONTH" => {
                        let months1 = date1.year() as i64 * 12 + date1.month() as i64;
                        let months2 = date2.year() as i64 * 12 + date2.month() as i64;
                        months1 - months2
                    }
                    "QUARTER" => {
                        let q1 = date1.year() as i64 * 4 + ((date1.month() - 1) / 3) as i64;
                        let q2 = date2.year() as i64 * 4 + ((date2.month() - 1) / 3) as i64;
                        q1 - q2
                    }
                    "YEAR" => date1.year() as i64 - date2.year() as i64,
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid DATE_DIFF unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::int64(diff))
            }
            "DATE_TRUNC" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATE_TRUNC requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let date = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let unit = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let truncated = match unit.to_uppercase().as_str() {
                    "DAY" => date,
                    "WEEK" => {
                        let days_since_sunday = date.weekday().num_days_from_sunday();
                        date - chrono::Duration::days(days_since_sunday as i64)
                    }
                    "MONTH" => chrono::NaiveDate::from_ymd_opt(date.year(), date.month(), 1)
                        .unwrap_or(date),
                    "QUARTER" => {
                        let quarter_month = ((date.month() - 1) / 3) * 3 + 1;
                        chrono::NaiveDate::from_ymd_opt(date.year(), quarter_month, 1)
                            .unwrap_or(date)
                    }
                    "YEAR" => chrono::NaiveDate::from_ymd_opt(date.year(), 1, 1).unwrap_or(date),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid DATE_TRUNC unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::date(truncated))
            }
            "DATE_FROM_UNIX_DATE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "DATE_FROM_UNIX_DATE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let days = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let date = epoch + chrono::Duration::days(days);
                Ok(Value::date(date))
            }
            "UNIX_DATE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "UNIX_DATE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let date = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let epoch = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
                let days = (date - epoch).num_days();
                Ok(Value::int64(days))
            }
            "PARSE_DATE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "PARSE_DATE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let format = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let date_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match chrono::NaiveDate::parse_from_str(date_str, format) {
                    Ok(date) => Ok(Value::date(date)),
                    Err(_) => Ok(Value::null()),
                }
            }
            "FORMAT_DATE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "FORMAT_DATE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let format = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let date = args[1].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::string(date.format(format).to_string()))
            }
            "LAST_DAY" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "LAST_DAY requires at least 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let date = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let next_month = if date.month() == 12 {
                    chrono::NaiveDate::from_ymd_opt(date.year() + 1, 1, 1)
                } else {
                    chrono::NaiveDate::from_ymd_opt(date.year(), date.month() + 1, 1)
                };
                let last_day = next_month.unwrap() - chrono::Duration::days(1);
                Ok(Value::date(last_day))
            }
            "GENERATE_DATE_ARRAY" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_DATE_ARRAY requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let start = args[0].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let end = args[1].as_date().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATE".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let step_days = if args.len() == 3 {
                    if args[2].is_null() {
                        return Ok(Value::null());
                    }
                    args[2]
                        .as_interval()
                        .map(|iv| iv.days as i64)
                        .or_else(|| args[2].as_i64())
                        .unwrap_or(1)
                } else {
                    1
                };
                if step_days == 0 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_DATE_ARRAY step cannot be 0".to_string(),
                    ));
                }
                let mut result = Vec::new();
                let mut curr = start;
                if step_days > 0 {
                    while curr <= end {
                        result.push(Value::date(curr));
                        curr += chrono::Duration::days(step_days);
                    }
                } else {
                    while curr >= end {
                        result.push(Value::date(curr));
                        curr += chrono::Duration::days(step_days);
                    }
                }
                Ok(Value::array(result))
            }
            "TIMESTAMP_ADD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_ADD requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let ts = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_ts = if interval.months != 0 {
                    ts.checked_add_months(chrono::Months::new(interval.months as u32))
                        .unwrap_or(ts)
                } else if interval.days != 0 {
                    ts + chrono::Duration::days(interval.days as i64)
                } else {
                    ts + chrono::Duration::nanoseconds(interval.nanos)
                };
                Ok(Value::timestamp(new_ts))
            }
            "TIMESTAMP_SUB" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_SUB requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let ts = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_ts = if interval.months != 0 {
                    ts.checked_sub_months(chrono::Months::new(interval.months as u32))
                        .unwrap_or(ts)
                } else if interval.days != 0 {
                    ts - chrono::Duration::days(interval.days as i64)
                } else {
                    ts - chrono::Duration::nanoseconds(interval.nanos)
                };
                Ok(Value::timestamp(new_ts))
            }
            "TIMESTAMP_DIFF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_DIFF requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let ts1 = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let ts2 = args[1].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let unit = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let duration = ts1.signed_duration_since(ts2);
                let diff = match unit.to_uppercase().as_str() {
                    "MICROSECOND" => duration.num_microseconds().unwrap_or(0),
                    "MILLISECOND" => duration.num_milliseconds(),
                    "SECOND" => duration.num_seconds(),
                    "MINUTE" => duration.num_minutes(),
                    "HOUR" => duration.num_hours(),
                    "DAY" => duration.num_days(),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid TIMESTAMP_DIFF unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::int64(diff))
            }
            "TIMESTAMP_TRUNC" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_TRUNC requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let ts = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let unit = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let naive = ts.naive_utc();
                let truncated = match unit.to_uppercase().as_str() {
                    "MICROSECOND" => naive,
                    "MILLISECOND" => {
                        let ms = naive.and_utc().timestamp_subsec_millis();
                        naive.with_nanosecond(ms * 1_000_000).unwrap_or(naive)
                    }
                    "SECOND" => naive.with_nanosecond(0).unwrap_or(naive),
                    "MINUTE" => naive
                        .with_second(0)
                        .and_then(|dt| dt.with_nanosecond(0))
                        .unwrap_or(naive),
                    "HOUR" => naive
                        .with_minute(0)
                        .and_then(|dt| dt.with_second(0))
                        .and_then(|dt| dt.with_nanosecond(0))
                        .unwrap_or(naive),
                    "DAY" => chrono::NaiveDateTime::new(
                        naive.date(),
                        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    "WEEK" => {
                        let days_since_sunday = naive.date().weekday().num_days_from_sunday();
                        let week_start =
                            naive.date() - chrono::Duration::days(days_since_sunday as i64);
                        chrono::NaiveDateTime::new(
                            week_start,
                            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        )
                    }
                    "MONTH" => chrono::NaiveDateTime::new(
                        chrono::NaiveDate::from_ymd_opt(naive.year(), naive.month(), 1).unwrap(),
                        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    "QUARTER" => {
                        let quarter_month = ((naive.month() - 1) / 3) * 3 + 1;
                        chrono::NaiveDateTime::new(
                            chrono::NaiveDate::from_ymd_opt(naive.year(), quarter_month, 1)
                                .unwrap(),
                            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        )
                    }
                    "YEAR" => chrono::NaiveDateTime::new(
                        chrono::NaiveDate::from_ymd_opt(naive.year(), 1, 1).unwrap(),
                        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid TIMESTAMP_TRUNC unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::timestamp(
                    chrono::DateTime::from_naive_utc_and_offset(truncated, chrono::Utc),
                ))
            }
            "PARSE_TIMESTAMP" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "PARSE_TIMESTAMP requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let format = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let ts_str = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match chrono::NaiveDateTime::parse_from_str(ts_str, format) {
                    Ok(ndt) => Ok(Value::timestamp(
                        chrono::DateTime::from_naive_utc_and_offset(ndt, chrono::Utc),
                    )),
                    Err(_) => Ok(Value::null()),
                }
            }
            "FORMAT_TIMESTAMP" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "FORMAT_TIMESTAMP requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let format = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let ts = args[1].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::string(ts.format(format).to_string()))
            }
            "UNIX_SECONDS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "UNIX_SECONDS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let ts = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::int64(ts.timestamp()))
            }
            "TIMESTAMP_SECONDS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_SECONDS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let seconds = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let ts =
                    chrono::DateTime::from_timestamp(seconds, 0).unwrap_or_else(chrono::Utc::now);
                Ok(Value::timestamp(ts))
            }
            "UNIX_MILLIS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "UNIX_MILLIS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let ts = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::int64(ts.timestamp_millis()))
            }
            "TIMESTAMP_MILLIS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_MILLIS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let millis = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let ts = chrono::DateTime::from_timestamp_millis(millis)
                    .unwrap_or_else(chrono::Utc::now);
                Ok(Value::timestamp(ts))
            }
            "UNIX_MICROS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "UNIX_MICROS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let ts = args[0].as_timestamp().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIMESTAMP".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                Ok(Value::int64(ts.timestamp_micros()))
            }
            "TIMESTAMP_MICROS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TIMESTAMP_MICROS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let micros = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let ts = chrono::DateTime::from_timestamp_micros(micros)
                    .unwrap_or_else(chrono::Utc::now);
                Ok(Value::timestamp(ts))
            }
            "TIME_ADD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TIME_ADD requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let time = args[0].as_time().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let duration = chrono::Duration::nanoseconds(interval.nanos);
                let new_time = time + duration;
                Ok(Value::time(new_time))
            }
            "TIME_SUB" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TIME_SUB requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let time = args[0].as_time().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let duration = chrono::Duration::nanoseconds(interval.nanos);
                let new_time = time - duration;
                Ok(Value::time(new_time))
            }
            "TIME_DIFF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "TIME_DIFF requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let time1 = args[0].as_time().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let time2 = args[1].as_time().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let unit = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let duration = time1.signed_duration_since(time2);
                let diff = match unit.to_uppercase().as_str() {
                    "MICROSECOND" => duration.num_microseconds().unwrap_or(0),
                    "MILLISECOND" => duration.num_milliseconds(),
                    "SECOND" => duration.num_seconds(),
                    "MINUTE" => duration.num_minutes(),
                    "HOUR" => duration.num_hours(),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid TIME_DIFF unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::int64(diff))
            }
            "TIME_TRUNC" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "TIME_TRUNC requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let time = args[0].as_time().ok_or_else(|| Error::TypeMismatch {
                    expected: "TIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let unit = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let truncated = match unit.to_uppercase().as_str() {
                    "MICROSECOND" => time,
                    "MILLISECOND" => {
                        let ms = time.nanosecond() / 1_000_000;
                        time.with_nanosecond(ms * 1_000_000).unwrap_or(time)
                    }
                    "SECOND" => time.with_nanosecond(0).unwrap_or(time),
                    "MINUTE" => chrono::NaiveTime::from_hms_opt(time.hour(), time.minute(), 0)
                        .unwrap_or(time),
                    "HOUR" => chrono::NaiveTime::from_hms_opt(time.hour(), 0, 0).unwrap_or(time),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid TIME_TRUNC unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::time(truncated))
            }
            "DATETIME_ADD" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATETIME_ADD requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let dt = args[0].as_datetime().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATETIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_dt = if interval.months != 0 {
                    dt.checked_add_months(chrono::Months::new(interval.months as u32))
                        .unwrap_or(dt)
                } else if interval.days != 0 {
                    dt + chrono::Duration::days(interval.days as i64)
                } else {
                    dt + chrono::Duration::nanoseconds(interval.nanos)
                };
                Ok(Value::datetime(new_dt))
            }
            "DATETIME_SUB" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATETIME_SUB requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let dt = args[0].as_datetime().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATETIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_dt = if interval.months != 0 {
                    dt.checked_sub_months(chrono::Months::new(interval.months as u32))
                        .unwrap_or(dt)
                } else if interval.days != 0 {
                    dt - chrono::Duration::days(interval.days as i64)
                } else {
                    dt - chrono::Duration::nanoseconds(interval.nanos)
                };
                Ok(Value::datetime(new_dt))
            }
            "DATETIME_DIFF" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "DATETIME_DIFF requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let dt1 = args[0].as_datetime().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATETIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let dt2 = args[1].as_datetime().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATETIME".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let unit = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let duration = dt1.signed_duration_since(dt2);
                let diff = match unit.to_uppercase().as_str() {
                    "MICROSECOND" => duration.num_microseconds().unwrap_or(0),
                    "MILLISECOND" => duration.num_milliseconds(),
                    "SECOND" => duration.num_seconds(),
                    "MINUTE" => duration.num_minutes(),
                    "HOUR" => duration.num_hours(),
                    "DAY" => duration.num_days(),
                    "WEEK" => duration.num_weeks(),
                    "MONTH" => {
                        let months1 = dt1.year() as i64 * 12 + dt1.month() as i64;
                        let months2 = dt2.year() as i64 * 12 + dt2.month() as i64;
                        months1 - months2
                    }
                    "QUARTER" => {
                        let q1 = dt1.year() as i64 * 4 + ((dt1.month() - 1) / 3) as i64;
                        let q2 = dt2.year() as i64 * 4 + ((dt2.month() - 1) / 3) as i64;
                        q1 - q2
                    }
                    "YEAR" => dt1.year() as i64 - dt2.year() as i64,
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid DATETIME_DIFF unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::int64(diff))
            }
            "DATETIME_TRUNC" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "DATETIME_TRUNC requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let dt = args[0].as_datetime().ok_or_else(|| Error::TypeMismatch {
                    expected: "DATETIME".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let unit = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let truncated = match unit.to_uppercase().as_str() {
                    "MICROSECOND" => dt,
                    "MILLISECOND" => {
                        let ms = dt.and_utc().timestamp_subsec_millis();
                        dt.with_nanosecond(ms * 1_000_000).unwrap_or(dt)
                    }
                    "SECOND" => dt.with_nanosecond(0).unwrap_or(dt),
                    "MINUTE" => dt
                        .with_second(0)
                        .and_then(|d| d.with_nanosecond(0))
                        .unwrap_or(dt),
                    "HOUR" => dt
                        .with_minute(0)
                        .and_then(|d| d.with_second(0))
                        .and_then(|d| d.with_nanosecond(0))
                        .unwrap_or(dt),
                    "DAY" => chrono::NaiveDateTime::new(
                        dt.date(),
                        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    "WEEK" => {
                        let days_since_sunday = dt.date().weekday().num_days_from_sunday();
                        let week_start =
                            dt.date() - chrono::Duration::days(days_since_sunday as i64);
                        chrono::NaiveDateTime::new(
                            week_start,
                            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        )
                    }
                    "MONTH" => chrono::NaiveDateTime::new(
                        chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1).unwrap(),
                        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    "QUARTER" => {
                        let quarter_month = ((dt.month() - 1) / 3) * 3 + 1;
                        chrono::NaiveDateTime::new(
                            chrono::NaiveDate::from_ymd_opt(dt.year(), quarter_month, 1).unwrap(),
                            chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                        )
                    }
                    "YEAR" => chrono::NaiveDateTime::new(
                        chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1).unwrap(),
                        chrono::NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
                    ),
                    _ => {
                        return Err(Error::InvalidQuery(format!(
                            "Invalid DATETIME_TRUNC unit: {}",
                            unit
                        )));
                    }
                };
                Ok(Value::datetime(truncated))
            }
            "FORMAT" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery("FORMAT requires arguments".to_string()));
                }
                if args.len() == 1 {
                    return Ok(args[0].clone());
                }
                let format_str = args[0].as_str().unwrap_or("%s");
                let mut result = format_str.to_string();
                for arg in args.iter().skip(1) {
                    if let Some(pos) = result.find("%s") {
                        let replacement = arg.as_str().unwrap_or_default();
                        result = format!("{}{}{}", &result[..pos], replacement, &result[pos + 2..]);
                    } else if let Some(pos) = result.find("%d") {
                        let replacement = arg
                            .as_i64()
                            .map(|n| n.to_string())
                            .or_else(|| arg.as_f64().map(|n| (n as i64).to_string()))
                            .unwrap_or_default();
                        result = format!("{}{}{}", &result[..pos], replacement, &result[pos + 2..]);
                    } else if let Some(pos) = result.find("%t") {
                        let replacement = arg.to_string();
                        result = format!("{}{}{}", &result[..pos], replacement, &result[pos + 2..]);
                    } else if let Some(pos) = result.find("%.") {
                        let end_pos = result[pos + 2..]
                            .find('f')
                            .map(|p| pos + 2 + p + 1)
                            .unwrap_or(pos + 4);
                        let precision: usize = result[pos + 2..end_pos - 1].parse().unwrap_or(2);
                        let replacement = arg
                            .as_f64()
                            .map(|n| format!("{:.prec$}", n, prec = precision))
                            .or_else(|| {
                                arg.as_i64()
                                    .map(|n| format!("{:.prec$}", n as f64, prec = precision))
                            })
                            .or_else(|| {
                                arg.as_numeric().and_then(|n| {
                                    use rust_decimal::prelude::ToPrimitive;
                                    n.to_f64()
                                        .map(|f| format!("{:.prec$}", f, prec = precision))
                                })
                            })
                            .unwrap_or_default();
                        result = format!("{}{}{}", &result[..pos], replacement, &result[end_pos..]);
                    }
                }
                Ok(Value::string(result))
            }
            "TO_JSON" | "TO_JSON_STRING" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "TO_JSON requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::string("null".to_string()));
                }
                let json_val = self.value_to_json(&args[0]);
                Ok(Value::string(json_val.to_string()))
            }
            "REGEXP_REPLACE" => {
                if args.len() < 3 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_REPLACE requires at least 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let pattern = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let replacement = args[2].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let rust_replacement = replacement
                    .replace("\\1", "$1")
                    .replace("\\2", "$2")
                    .replace("\\3", "$3")
                    .replace("\\4", "$4")
                    .replace("\\5", "$5")
                    .replace("\\6", "$6")
                    .replace("\\7", "$7")
                    .replace("\\8", "$8")
                    .replace("\\9", "$9");
                match regex::Regex::new(pattern) {
                    Ok(re) => Ok(Value::string(
                        re.replace_all(text, rust_replacement.as_str()).to_string(),
                    )),
                    Err(_) => Ok(Value::null()),
                }
            }
            "REGEXP_INSTR" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "REGEXP_INSTR requires at least 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let text = args[0].as_str().unwrap_or_default();
                let pattern = args[1].as_str().unwrap_or_default();
                match regex::Regex::new(pattern) {
                    Ok(re) => match re.find(text) {
                        Some(m) => Ok(Value::int64((m.start() + 1) as i64)),
                        None => Ok(Value::int64(0)),
                    },
                    Err(_) => Ok(Value::null()),
                }
            }
            "SAFE_OFFSET" | "OFFSET" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "OFFSET requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    return Ok(Value::int64(i + 1));
                }
                Ok(args[0].clone())
            }
            "SAFE_ORDINAL" | "ORDINAL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ORDINAL requires 1 argument".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            "GENERATE_UUID" => {
                let uuid = uuid::Uuid::new_v4();
                Ok(Value::string(uuid.to_string()))
            }
            "LPAD" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "LPAD requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().unwrap_or_default();
                let len = args[1].as_i64().unwrap_or(0) as usize;
                let pad = if args.len() == 3 {
                    args[2].as_str().unwrap_or(" ")
                } else {
                    " "
                };
                if s.chars().count() >= len {
                    return Ok(Value::string(s.chars().take(len).collect::<String>()));
                }
                let pad_len = len - s.chars().count();
                let pad_chars: Vec<char> = pad.chars().cycle().take(pad_len).collect();
                let result: String = pad_chars.into_iter().chain(s.chars()).collect();
                Ok(Value::string(result))
            }
            "RPAD" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "RPAD requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().unwrap_or_default();
                let len = args[1].as_i64().unwrap_or(0) as usize;
                let pad = if args.len() == 3 {
                    args[2].as_str().unwrap_or(" ")
                } else {
                    " "
                };
                if s.chars().count() >= len {
                    return Ok(Value::string(s.chars().take(len).collect::<String>()));
                }
                let pad_len = len - s.chars().count();
                let pad_chars: Vec<char> = pad.chars().cycle().take(pad_len).collect();
                let result: String = s.chars().chain(pad_chars).collect();
                Ok(Value::string(result))
            }
            "ARRAY_CONCAT" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "ARRAY_CONCAT requires at least 1 argument".to_string(),
                    ));
                }
                let mut result = Vec::new();
                for arg in args {
                    if arg.is_null() {
                        continue;
                    }
                    if let Some(arr) = arg.as_array() {
                        result.extend(arr.iter().cloned());
                    }
                }
                Ok(Value::array(result))
            }
            "ARRAY_REVERSE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_REVERSE requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(arr) = args[0].as_array() {
                    let mut reversed = arr.to_vec();
                    reversed.reverse();
                    return Ok(Value::array(reversed));
                }
                Ok(Value::null())
            }
            "ARRAY_TO_STRING" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_TO_STRING requires 2 or 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let delimiter = args[1].as_str().unwrap_or(",");
                let null_text = if args.len() == 3 {
                    args[2].as_str().map(|s| s.to_string())
                } else {
                    None
                };
                if let Some(arr) = args[0].as_array() {
                    let parts: Vec<String> = arr
                        .iter()
                        .filter_map(|v| {
                            if v.is_null() {
                                null_text.clone()
                            } else {
                                Some(v.to_string())
                            }
                        })
                        .collect();
                    return Ok(Value::string(parts.join(delimiter)));
                }
                Ok(Value::null())
            }
            "GENERATE_ARRAY" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_ARRAY requires 2 or 3 arguments".to_string(),
                    ));
                }
                let start = args[0].as_i64().unwrap_or(0);
                let end = args[1].as_i64().unwrap_or(0);
                let step = if args.len() == 3 {
                    args[2].as_i64().unwrap_or(1)
                } else {
                    1
                };
                if step == 0 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_ARRAY step cannot be 0".to_string(),
                    ));
                }
                let mut result = Vec::new();
                let mut curr = start;
                if step > 0 {
                    while curr <= end {
                        result.push(Value::int64(curr));
                        curr += step;
                    }
                } else {
                    while curr >= end {
                        result.push(Value::int64(curr));
                        curr += step;
                    }
                }
                Ok(Value::array(result))
            }
            "ARRAY_SLICE" => {
                if args.len() != 3 {
                    return Err(Error::InvalidQuery(
                        "ARRAY_SLICE requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let arr = args[0].as_array().ok_or_else(|| Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let start = args[1].as_i64().unwrap_or(1);
                let end = args[2].as_i64().unwrap_or(0);
                if start < 1 || end < start {
                    return Ok(Value::array(vec![]));
                }
                let start_idx = (start - 1) as usize;
                let end_idx = (end as usize).min(arr.len());
                let slice: Vec<Value> = arr[start_idx..end_idx].to_vec();
                Ok(Value::array(slice))
            }
            "ST_GEOGFROMTEXT" | "ST_GEOGRAPHYFROMTEXT" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match parse_geography(wkt_str) {
                    Ok(geom) => Ok(Value::geography(format_wkt_number(&geom))),
                    Err(e) => Err(Error::InvalidQuery(format!("Invalid WKT: {}", e))),
                }
            }
            "ST_GEOGPOINT" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_GEOGPOINT requires 2 arguments (longitude, latitude)".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let lng = args[0]
                    .as_f64()
                    .or_else(|| {
                        use rust_decimal::prelude::ToPrimitive;
                        args[0].as_numeric().and_then(|d| d.to_f64())
                    })
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "FLOAT64".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let lat = args[1]
                    .as_f64()
                    .or_else(|| {
                        use rust_decimal::prelude::ToPrimitive;
                        args[1].as_numeric().and_then(|d| d.to_f64())
                    })
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "FLOAT64".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let point = Point::new(lng, lat);
                Ok(Value::geography(format_wkt_number(&Geometry::Point(point))))
            }
            "ST_GEOGFROMGEOJSON" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match geometry_from_geojson(json_str) {
                    Ok(geom) => Ok(Value::geography(format_wkt_number(&geom))),
                    Err(e) => Err(Error::InvalidQuery(format!("Invalid GeoJSON: {}", e))),
                }
            }
            "ST_DISTANCE" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_DISTANCE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let dist = geometry_geodesic_distance(&geom1, &geom2);
                Ok(Value::float64(dist))
            }
            "ST_LENGTH" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let length = match &geom {
                    Geometry::LineString(ls) => ls.geodesic_length(),
                    Geometry::MultiLineString(mls) => mls.geodesic_length(),
                    _ => 0.0,
                };
                Ok(Value::float64(length))
            }
            "ST_PERIMETER" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let perimeter = match &geom {
                    Geometry::Polygon(poly) => poly.exterior().geodesic_length(),
                    Geometry::MultiPolygon(mpoly) => {
                        mpoly.0.iter().map(|p| p.exterior().geodesic_length()).sum()
                    }
                    _ => 0.0,
                };
                Ok(Value::float64(perimeter))
            }
            "ST_AREA" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let area = match &geom {
                    Geometry::Polygon(poly) => poly.geodesic_area_signed().abs(),
                    Geometry::MultiPolygon(mpoly) => mpoly.geodesic_area_signed().abs(),
                    _ => 0.0,
                };
                Ok(Value::float64(area))
            }
            "ST_ASTEXT" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::string(format_wkt_number(&geom)))
            }
            "ST_ASGEOJSON" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::string(geometry_to_geojson(&geom)))
            }
            "ST_ASBINARY" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                use wkt::ToWkt;
                let wkb = geom.wkt_string();
                Ok(Value::bytes(wkb.into_bytes()))
            }
            "ST_X" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::Point(p) => Ok(Value::float64(p.x())),
                    _ => Err(Error::TypeMismatch {
                        expected: "POINT".to_string(),
                        actual: geometry_type_name(&geom).to_string(),
                    }),
                }
            }
            "ST_Y" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::Point(p) => Ok(Value::float64(p.y())),
                    _ => Err(Error::TypeMismatch {
                        expected: "POINT".to_string(),
                        actual: geometry_type_name(&geom).to_string(),
                    }),
                }
            }
            "ST_CONTAINS" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_CONTAINS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let result = geom1.contains(&geom2);
                Ok(Value::bool_val(result))
            }
            "ST_WITHIN" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_WITHIN requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let result = geom1.is_within(&geom2);
                Ok(Value::bool_val(result))
            }
            "ST_INTERSECTS" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_INTERSECTS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let result = geom1.intersects(&geom2);
                Ok(Value::bool_val(result))
            }
            "ST_DWITHIN" => {
                if args.len() < 3 {
                    return Err(Error::InvalidQuery(
                        "ST_DWITHIN requires 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() || args[2].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let distance = args[2].as_f64().ok_or_else(|| Error::TypeMismatch {
                    expected: "FLOAT64".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let actual_distance = geometry_geodesic_distance(&geom1, &geom2);
                Ok(Value::bool_val(actual_distance <= distance))
            }
            "ST_COVERS" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_COVERS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let result = geom1.contains(&geom2);
                Ok(Value::bool_val(result))
            }
            "ST_COVEREDBY" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_COVEREDBY requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let result = geom1.is_within(&geom2);
                Ok(Value::bool_val(result))
            }
            "ST_TOUCHES" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_TOUCHES requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let intersects = geom1.intersects(&geom2);
                let contains1 = geom1.contains(&geom2);
                let contains2 = geom2.contains(&geom1);
                Ok(Value::bool_val(intersects && !contains1 && !contains2))
            }
            "ST_DISJOINT" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_DISJOINT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let result = !geom1.intersects(&geom2);
                Ok(Value::bool_val(result))
            }
            "NET.IP_FROM_STRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let ip_str = args[0].as_str().unwrap_or_default();
                if let Ok(ipv4) = ip_str.parse::<std::net::Ipv4Addr>() {
                    return Ok(Value::bytes(ipv4.octets().to_vec()));
                }
                if let Ok(ipv6) = ip_str.parse::<std::net::Ipv6Addr>() {
                    return Ok(Value::bytes(ipv6.octets().to_vec()));
                }
                Err(Error::InvalidQuery(format!(
                    "Invalid IP address: {}",
                    ip_str
                )))
            }
            "NET.SAFE_IP_FROM_STRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let ip_str = args[0].as_str().unwrap_or_default();
                if let Ok(ipv4) = ip_str.parse::<std::net::Ipv4Addr>() {
                    return Ok(Value::bytes(ipv4.octets().to_vec()));
                }
                if let Ok(ipv6) = ip_str.parse::<std::net::Ipv6Addr>() {
                    return Ok(Value::bytes(ipv6.octets().to_vec()));
                }
                Ok(Value::null())
            }
            "NET.IP_TO_STRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bytes() {
                    if b.len() == 4 {
                        let ipv4 = std::net::Ipv4Addr::new(b[0], b[1], b[2], b[3]);
                        return Ok(Value::string(ipv4.to_string()));
                    }
                    if b.len() == 16 {
                        let mut octets = [0u8; 16];
                        octets.copy_from_slice(b);
                        let ipv6 = std::net::Ipv6Addr::from(octets);
                        return Ok(Value::string(ipv6.to_string()));
                    }
                }
                Ok(Value::null())
            }
            "NET.IPV4_FROM_INT64" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(i) = args[0].as_i64() {
                    let b = (i as u32).to_be_bytes();
                    return Ok(Value::bytes(b.to_vec()));
                }
                Ok(Value::null())
            }
            "NET.IPV4_TO_INT64" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(b) = args[0].as_bytes() {
                    if b.len() == 4 {
                        let val = u32::from_be_bytes([b[0], b[1], b[2], b[3]]);
                        return Ok(Value::int64(val as i64));
                    }
                }
                Ok(Value::null())
            }
            "NET.IP_NET_MASK" => {
                if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let ip_version = args[0].as_i64().unwrap_or(0);
                let prefix_len = args[1].as_i64().unwrap_or(0) as u32;
                match ip_version {
                    4 => {
                        if prefix_len > 32 {
                            return Ok(Value::null());
                        }
                        let mask = if prefix_len == 0 {
                            0u32
                        } else {
                            !0u32 << (32 - prefix_len)
                        };
                        Ok(Value::bytes(mask.to_be_bytes().to_vec()))
                    }
                    6 => {
                        if prefix_len > 128 {
                            return Ok(Value::null());
                        }
                        let mut mask = [0u8; 16];
                        let full_bytes = (prefix_len / 8) as usize;
                        let remaining_bits = prefix_len % 8;
                        for b in mask.iter_mut().take(full_bytes) {
                            *b = 0xff;
                        }
                        if full_bytes < 16 && remaining_bits > 0 {
                            mask[full_bytes] = 0xff << (8 - remaining_bits);
                        }
                        Ok(Value::bytes(mask.to_vec()))
                    }
                    _ => Ok(Value::null()),
                }
            }
            "NET.IP_TRUNC" => {
                if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let prefix_len = args[1].as_i64().unwrap_or(0) as u32;
                if let Some(ip_bytes) = args[0].as_bytes() {
                    if ip_bytes.len() == 4 {
                        if prefix_len > 32 {
                            return Ok(Value::null());
                        }
                        let ip = u32::from_be_bytes([
                            ip_bytes[0],
                            ip_bytes[1],
                            ip_bytes[2],
                            ip_bytes[3],
                        ]);
                        let mask = if prefix_len == 0 {
                            0u32
                        } else {
                            !0u32 << (32 - prefix_len)
                        };
                        let truncated = ip & mask;
                        return Ok(Value::bytes(truncated.to_be_bytes().to_vec()));
                    }
                    if ip_bytes.len() == 16 {
                        if prefix_len > 128 {
                            return Ok(Value::null());
                        }
                        let mut result = [0u8; 16];
                        let full_bytes = (prefix_len / 8) as usize;
                        let remaining_bits = prefix_len % 8;
                        for (i, b) in result.iter_mut().enumerate().take(full_bytes) {
                            *b = ip_bytes[i];
                        }
                        if full_bytes < 16 && remaining_bits > 0 {
                            let mask = 0xff << (8 - remaining_bits);
                            result[full_bytes] = ip_bytes[full_bytes] & mask;
                        }
                        return Ok(Value::bytes(result.to_vec()));
                    }
                }
                Ok(Value::null())
            }
            "NET.HOST" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let url_str = args[0].as_str().unwrap_or_default();
                if let Ok(url) = url::Url::parse(url_str) {
                    if let Some(host) = url.host_str() {
                        return Ok(Value::string(host.to_string()));
                    }
                }
                Ok(Value::null())
            }
            "NET.PUBLIC_SUFFIX" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let domain = args[0].as_str().unwrap_or_default();
                let parts: Vec<&str> = domain.split('.').collect();
                if parts.len() >= 2 {
                    let suffix = if parts.len() >= 3 {
                        let last_two =
                            format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
                        let common_multi =
                            ["co.uk", "com.au", "co.nz", "co.jp", "com.br", "org.uk"];
                        if common_multi.contains(&last_two.as_str()) {
                            last_two
                        } else {
                            parts[parts.len() - 1].to_string()
                        }
                    } else {
                        parts[parts.len() - 1].to_string()
                    };
                    return Ok(Value::string(suffix));
                }
                Ok(Value::string(String::new()))
            }
            "NET.REG_DOMAIN" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let domain = args[0].as_str().unwrap_or_default();
                let parts: Vec<&str> = domain.split('.').collect();
                if parts.len() >= 2 {
                    let common_multi = ["co.uk", "com.au", "co.nz", "co.jp", "com.br", "org.uk"];
                    let reg_domain = if parts.len() >= 3 {
                        let last_two =
                            format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1]);
                        if common_multi.contains(&last_two.as_str()) {
                            if parts.len() >= 3 {
                                format!(
                                    "{}.{}.{}",
                                    parts[parts.len() - 3],
                                    parts[parts.len() - 2],
                                    parts[parts.len() - 1]
                                )
                            } else {
                                last_two
                            }
                        } else {
                            format!("{}.{}", parts[parts.len() - 2], parts[parts.len() - 1])
                        }
                    } else {
                        domain.to_string()
                    };
                    return Ok(Value::string(reg_domain));
                }
                Ok(Value::string(String::new()))
            }
            "NET.IP_IN_NET" => {
                if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let cidr_str = args[1].as_str().unwrap_or_default();
                let cidr_parts: Vec<&str> = cidr_str.split('/').collect();
                if cidr_parts.len() != 2 {
                    return Ok(Value::null());
                }
                let prefix_len: u32 = cidr_parts[1].parse().unwrap_or(0);
                if let Some(ip_bytes) = args[0].as_bytes() {
                    if ip_bytes.len() == 4 {
                        if let Ok(network_ip) = cidr_parts[0].parse::<std::net::Ipv4Addr>() {
                            let ip = u32::from_be_bytes([
                                ip_bytes[0],
                                ip_bytes[1],
                                ip_bytes[2],
                                ip_bytes[3],
                            ]);
                            let network = u32::from_be_bytes(network_ip.octets());
                            let mask = if prefix_len == 0 {
                                0u32
                            } else {
                                !0u32 << (32 - prefix_len)
                            };
                            return Ok(Value::bool_val((ip & mask) == (network & mask)));
                        }
                    }
                }
                Ok(Value::bool_val(false))
            }
            "NET.MAKE_NET" => {
                if args.len() != 2 || args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let prefix_len = args[1].as_i64().unwrap_or(0);
                if let Some(ip_bytes) = args[0].as_bytes() {
                    if ip_bytes.len() == 4 {
                        let ip = std::net::Ipv4Addr::new(
                            ip_bytes[0],
                            ip_bytes[1],
                            ip_bytes[2],
                            ip_bytes[3],
                        );
                        return Ok(Value::string(format!("{}/{}", ip, prefix_len)));
                    }
                    if ip_bytes.len() == 16 {
                        let mut octets = [0u8; 16];
                        octets.copy_from_slice(ip_bytes);
                        let ip = std::net::Ipv6Addr::from(octets);
                        return Ok(Value::string(format!("{}/{}", ip, prefix_len)));
                    }
                }
                Ok(Value::null())
            }
            "NET.IP_IS_PRIVATE" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(ip_bytes) = args[0].as_bytes() {
                    if ip_bytes.len() == 4 {
                        let ipv4 = std::net::Ipv4Addr::new(
                            ip_bytes[0],
                            ip_bytes[1],
                            ip_bytes[2],
                            ip_bytes[3],
                        );
                        let is_private =
                            ipv4.is_private() || ipv4.is_loopback() || ipv4.is_link_local();
                        return Ok(Value::bool_val(is_private));
                    }
                    if ip_bytes.len() == 16 {
                        let mut octets = [0u8; 16];
                        octets.copy_from_slice(ip_bytes);
                        let ipv6 = std::net::Ipv6Addr::from(octets);
                        let is_private = ipv6.is_loopback();
                        return Ok(Value::bool_val(is_private));
                    }
                }
                Ok(Value::bool_val(false))
            }
            "RANGE" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RANGE requires 2 arguments".to_string(),
                    ));
                }
                let start = if args[0].is_null() {
                    None
                } else {
                    Some(args[0].clone())
                };
                let end = if args[1].is_null() {
                    None
                } else {
                    Some(args[1].clone())
                };
                Ok(Value::range(start, end))
            }
            "JSON_ARRAY" => {
                let json_arr: Vec<serde_json::Value> =
                    args.iter().map(|v| self.value_to_json(v)).collect();
                Ok(Value::json(serde_json::Value::Array(json_arr)))
            }
            "JSON_OBJECT" => {
                let mut obj = serde_json::Map::new();
                let mut i = 0;
                while i + 1 < args.len() {
                    let key = args[i].as_str().unwrap_or_default().to_string();
                    let val = self.value_to_json(&args[i + 1]);
                    obj.insert(key, val);
                    i += 2;
                }
                Ok(Value::string(serde_json::Value::Object(obj).to_string()))
            }
            "JSON_REMOVE" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "JSON_REMOVE requires at least 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let mut json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|_| Error::InvalidQuery("Invalid JSON".to_string()))?
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "JSON or STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };
                for arg in args.iter().skip(1) {
                    let path = arg.as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: arg.data_type().to_string(),
                    })?;
                    json_val = self.json_path_remove(json_val, path);
                }
                Ok(Value::string(json_val.to_string()))
            }
            "JSON_STRIP_NULLS" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "JSON_STRIP_NULLS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|_| Error::InvalidQuery("Invalid JSON".to_string()))?
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "JSON or STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };
                let result = self.json_strip_nulls(json_val);
                Ok(Value::string(result.to_string()))
            }
            "JSON_VALUE_ARRAY" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "JSON_VALUE_ARRAY requires at least 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|_| Error::InvalidQuery("Invalid JSON".to_string()))?
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "JSON or STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };
                let target = if args.len() > 1 {
                    let path = args[1].as_str().unwrap_or("$");
                    self.json_path_extract(&json_val, path)
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    json_val
                };
                match target {
                    serde_json::Value::Array(arr) => {
                        let values: Vec<Value> = arr
                            .iter()
                            .map(|v| match v {
                                serde_json::Value::String(s) => Value::string(s.clone()),
                                serde_json::Value::Number(n) => Value::string(n.to_string()),
                                serde_json::Value::Bool(b) => Value::string(b.to_string()),
                                serde_json::Value::Null => Value::null(),
                                _ => Value::null(),
                            })
                            .collect();
                        Ok(Value::array(values))
                    }
                    _ => Ok(Value::null()),
                }
            }
            "JSON_QUERY_ARRAY" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "JSON_QUERY_ARRAY requires at least 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|_| Error::InvalidQuery("Invalid JSON".to_string()))?
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "JSON or STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };
                let target = if args.len() > 1 {
                    let path = args[1].as_str().unwrap_or("$");
                    self.json_path_extract(&json_val, path)
                        .unwrap_or(serde_json::Value::Null)
                } else {
                    json_val
                };
                match target {
                    serde_json::Value::Array(arr) => {
                        let values: Vec<Value> =
                            arr.iter().map(|v| Value::json(v.clone())).collect();
                        Ok(Value::array(values))
                    }
                    _ => Ok(Value::null()),
                }
            }
            "LAX_INT64" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else {
                    return Ok(Value::null());
                };
                match json_val {
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::int64(i))
                        } else if let Some(f) = n.as_f64() {
                            Ok(Value::int64(f as i64))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    serde_json::Value::String(s) => {
                        if let Ok(i) = s.parse::<i64>() {
                            Ok(Value::int64(i))
                        } else if let Ok(f) = s.parse::<f64>() {
                            Ok(Value::int64(f as i64))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    serde_json::Value::Bool(b) => Ok(Value::int64(if b { 1 } else { 0 })),
                    _ => Ok(Value::null()),
                }
            }
            "LAX_FLOAT64" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else {
                    return Ok(Value::null());
                };
                match json_val {
                    serde_json::Value::Number(n) => {
                        if let Some(f) = n.as_f64() {
                            Ok(Value::float64(f))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    serde_json::Value::String(s) => {
                        if let Ok(f) = s.parse::<f64>() {
                            Ok(Value::float64(f))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    serde_json::Value::Bool(b) => Ok(Value::float64(if b { 1.0 } else { 0.0 })),
                    _ => Ok(Value::null()),
                }
            }
            "LAX_STRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else {
                    return Ok(Value::null());
                };
                match json_val {
                    serde_json::Value::String(s) => Ok(Value::string(s)),
                    serde_json::Value::Number(n) => Ok(Value::string(n.to_string())),
                    serde_json::Value::Bool(b) => Ok(Value::string(b.to_string())),
                    serde_json::Value::Null => Ok(Value::null()),
                    _ => Ok(Value::null()),
                }
            }
            "LAX_BOOL" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else {
                    return Ok(Value::null());
                };
                match json_val {
                    serde_json::Value::Bool(b) => Ok(Value::bool_val(b)),
                    serde_json::Value::String(s) => {
                        let lower = s.to_lowercase();
                        if lower == "true" {
                            Ok(Value::bool_val(true))
                        } else if lower == "false" {
                            Ok(Value::bool_val(false))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            Ok(Value::bool_val(i != 0))
                        } else if let Some(f) = n.as_f64() {
                            Ok(Value::bool_val(f != 0.0))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    _ => Ok(Value::null()),
                }
            }
            "APPROX_COUNT_DISTINCT" => Ok(Value::int64(0)),
            "APPROX_QUANTILES" => Ok(Value::array(vec![Value::null()])),
            "ALL" => {
                if args.is_empty() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "ST_MAKELINE" => {
                if args.is_empty() {
                    return Err(Error::InvalidQuery(
                        "ST_MAKELINE requires at least 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(arr) = args[0].as_array() {
                    let mut coords = Vec::new();
                    for val in arr {
                        if val.is_null() {
                            continue;
                        }
                        let wkt = val.as_geography().or_else(|| val.as_str()).ok_or_else(|| {
                            Error::TypeMismatch {
                                expected: "GEOGRAPHY".to_string(),
                                actual: val.data_type().to_string(),
                            }
                        })?;
                        let geom = parse_geography(wkt)
                            .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                        if let Geometry::Point(p) = geom {
                            coords.push(Coord { x: p.x(), y: p.y() });
                        }
                    }
                    if coords.is_empty() {
                        return Ok(Value::null());
                    }
                    let line = LineString::new(coords);
                    return Ok(Value::geography(format_wkt_number(&Geometry::LineString(
                        line,
                    ))));
                }
                if args.len() == 2 {
                    if args[1].is_null() {
                        return Ok(Value::null());
                    }
                    let wkt1 = args[0]
                        .as_geography()
                        .or_else(|| args[0].as_str())
                        .ok_or_else(|| Error::TypeMismatch {
                            expected: "GEOGRAPHY".to_string(),
                            actual: args[0].data_type().to_string(),
                        })?;
                    let wkt2 = args[1]
                        .as_geography()
                        .or_else(|| args[1].as_str())
                        .ok_or_else(|| Error::TypeMismatch {
                            expected: "GEOGRAPHY".to_string(),
                            actual: args[1].data_type().to_string(),
                        })?;
                    let geom1 = parse_geography(wkt1)
                        .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                    let geom2 = parse_geography(wkt2)
                        .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                    let (p1, p2) = match (&geom1, &geom2) {
                        (Geometry::Point(a), Geometry::Point(b)) => (*a, *b),
                        _ => {
                            return Err(Error::TypeMismatch {
                                expected: "POINT".to_string(),
                                actual: "other geometry".to_string(),
                            });
                        }
                    };
                    let line = LineString::new(vec![
                        Coord {
                            x: p1.x(),
                            y: p1.y(),
                        },
                        Coord {
                            x: p2.x(),
                            y: p2.y(),
                        },
                    ]);
                    return Ok(Value::geography(format_wkt_number(&Geometry::LineString(
                        line,
                    ))));
                }
                Err(Error::InvalidQuery(
                    "ST_MAKELINE requires either an array or 2 points".to_string(),
                ))
            }
            "ST_MAKEPOLYGON" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::LineString(ls) => {
                        let poly = Polygon::new(ls, vec![]);
                        Ok(Value::geography(format_wkt_number(&Geometry::Polygon(
                            poly,
                        ))))
                    }
                    _ => Err(Error::TypeMismatch {
                        expected: "LINESTRING".to_string(),
                        actual: geometry_type_name(&geom).to_string(),
                    }),
                }
            }
            "ST_BUFFER" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let distance = if args.len() > 1 {
                    args[1].as_f64().unwrap_or(0.0)
                } else {
                    0.0
                };
                let degrees = distance / 111195.0;
                if let Geometry::Point(p) = geom {
                    let mut coords = Vec::new();
                    for i in 0..=32 {
                        let angle = 2.0 * std::f64::consts::PI * (i as f64) / 32.0;
                        let x = p.x() + degrees * angle.cos();
                        let y = p.y() + degrees * angle.sin() * (p.y().to_radians().cos());
                        coords.push(Coord { x, y });
                    }
                    let poly = Polygon::new(LineString::new(coords), vec![]);
                    return Ok(Value::geography(format_wkt_number(&Geometry::Polygon(
                        poly,
                    ))));
                }
                Ok(Value::geography(format_wkt_number(&geom)))
            }
            "ST_BUFFERWITHTOLERANCE" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let distance = if args.len() > 1 {
                    args[1].as_f64().unwrap_or(0.0)
                } else {
                    0.0
                };
                let degrees = distance / 111195.0;
                if let Geometry::Point(p) = geom {
                    let mut coords = Vec::new();
                    for i in 0..=16 {
                        let angle = 2.0 * std::f64::consts::PI * (i as f64) / 16.0;
                        let x = p.x() + degrees * angle.cos();
                        let y = p.y() + degrees * angle.sin() * (p.y().to_radians().cos());
                        coords.push(Coord { x, y });
                    }
                    let poly = Polygon::new(LineString::new(coords), vec![]);
                    return Ok(Value::geography(format_wkt_number(&Geometry::Polygon(
                        poly,
                    ))));
                }
                Ok(Value::geography(format_wkt_number(&geom)))
            }
            "ST_CENTROID" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                if let Some(centroid) = geom.centroid() {
                    Ok(Value::geography(format_wkt_number(&Geometry::Point(
                        centroid,
                    ))))
                } else {
                    Ok(Value::null())
                }
            }
            "ST_CLOSESTPOINT" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_CLOSESTPOINT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                use geo::ClosestPoint;
                if let Geometry::Point(target) = &geom2 {
                    match geom1.closest_point(target) {
                        geo::Closest::Intersection(p) | geo::Closest::SinglePoint(p) => {
                            Ok(Value::geography(format_wkt_number(&Geometry::Point(p))))
                        }
                        geo::Closest::Indeterminate => Ok(Value::null()),
                    }
                } else if let Some(centroid) = geom2.centroid() {
                    match geom1.closest_point(&centroid) {
                        geo::Closest::Intersection(p) | geo::Closest::SinglePoint(p) => {
                            Ok(Value::geography(format_wkt_number(&Geometry::Point(p))))
                        }
                        geo::Closest::Indeterminate => Ok(Value::null()),
                    }
                } else {
                    Ok(Value::null())
                }
            }
            "ST_CONVEXHULL" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let hull = geom.convex_hull();
                Ok(Value::geography(format_wkt_number(&Geometry::Polygon(
                    hull,
                ))))
            }
            "ST_DIFFERENCE" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_DIFFERENCE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                fn to_multipoly(geom: &Geometry<f64>) -> Option<MultiPolygon<f64>> {
                    match geom {
                        Geometry::Polygon(p) => Some(MultiPolygon::new(vec![p.clone()])),
                        Geometry::MultiPolygon(mp) => Some(mp.clone()),
                        _ => None,
                    }
                }
                match (to_multipoly(&geom1), to_multipoly(&geom2)) {
                    (Some(mp1), Some(mp2)) => {
                        let result = mp1.difference(&mp2);
                        Ok(Value::geography(format_wkt_number(
                            &Geometry::MultiPolygon(result),
                        )))
                    }
                    _ => Ok(Value::geography(format_wkt_number(&geom1))),
                }
            }
            "ST_INTERSECTION" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_INTERSECTION requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                fn to_multipoly(geom: &Geometry<f64>) -> Option<MultiPolygon<f64>> {
                    match geom {
                        Geometry::Polygon(p) => Some(MultiPolygon::new(vec![p.clone()])),
                        Geometry::MultiPolygon(mp) => Some(mp.clone()),
                        _ => None,
                    }
                }
                match (to_multipoly(&geom1), to_multipoly(&geom2)) {
                    (Some(mp1), Some(mp2)) => {
                        let result = mp1.intersection(&mp2);
                        Ok(Value::geography(format_wkt_number(
                            &Geometry::MultiPolygon(result),
                        )))
                    }
                    _ => Ok(Value::geography(format_wkt_number(&geom1))),
                }
            }
            "ST_SIMPLIFY" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let tolerance = if args.len() > 1 {
                    args[1].as_f64().unwrap_or(0.0) / 111195.0
                } else {
                    0.0
                };
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let simplified = match &geom {
                    Geometry::LineString(ls) => Geometry::LineString(ls.simplify(&tolerance)),
                    Geometry::Polygon(poly) => Geometry::Polygon(poly.simplify(&tolerance)),
                    Geometry::MultiLineString(mls) => {
                        Geometry::MultiLineString(mls.simplify(&tolerance))
                    }
                    Geometry::MultiPolygon(mpoly) => {
                        Geometry::MultiPolygon(mpoly.simplify(&tolerance))
                    }
                    other => other.clone(),
                };
                Ok(Value::geography(format_wkt_number(&simplified)))
            }
            "ST_SNAPTOGRID" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let grid_size = if args.len() > 1 {
                    args[1]
                        .as_f64()
                        .or_else(|| {
                            use rust_decimal::prelude::ToPrimitive;
                            args[1].as_numeric().and_then(|d| d.to_f64())
                        })
                        .unwrap_or(0.0001)
                } else {
                    0.0001
                };
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let snapped = match geom {
                    Geometry::Point(p) => {
                        let x = snap_coord_to_grid(p.x(), grid_size);
                        let y = snap_coord_to_grid(p.y(), grid_size);
                        Geometry::Point(Point::new(x, y))
                    }
                    Geometry::LineString(ls) => {
                        let coords: Vec<Coord<f64>> =
                            ls.0.iter()
                                .map(|c| Coord {
                                    x: snap_coord_to_grid(c.x, grid_size),
                                    y: snap_coord_to_grid(c.y, grid_size),
                                })
                                .collect();
                        Geometry::LineString(LineString::new(coords))
                    }
                    other => other,
                };
                Ok(Value::geography(format_wkt_number(&snapped)))
            }
            "ST_UNION" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_UNION requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                fn to_multipoly(geom: &Geometry<f64>) -> Option<MultiPolygon<f64>> {
                    match geom {
                        Geometry::Polygon(p) => Some(MultiPolygon::new(vec![p.clone()])),
                        Geometry::MultiPolygon(mp) => Some(mp.clone()),
                        _ => None,
                    }
                }
                match (to_multipoly(&geom1), to_multipoly(&geom2)) {
                    (Some(mp1), Some(mp2)) => {
                        let result = mp1.union(&mp2);
                        Ok(Value::geography(format_wkt_number(
                            &Geometry::MultiPolygon(result),
                        )))
                    }
                    _ => Ok(Value::geography(format_wkt_number(&geom1))),
                }
            }
            "ST_DIMENSION" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::int64(geometry_dimension(&geom)))
            }
            "ST_NUMPOINTS" | "ST_NPOINTS" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::int64(geometry_num_points(&geom)))
            }
            "ST_NUMGEOMETRIES" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let count = match &geom {
                    Geometry::GeometryCollection(gc) => gc.0.len() as i64,
                    Geometry::MultiPoint(mp) => mp.0.len() as i64,
                    Geometry::MultiLineString(mls) => mls.0.len() as i64,
                    Geometry::MultiPolygon(mpoly) => mpoly.0.len() as i64,
                    _ => 1,
                };
                Ok(Value::int64(count))
            }
            "ST_ISEMPTY" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::bool_val(geometry_is_empty(&geom)))
            }
            "ST_ISCOLLECTION" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::bool_val(geometry_is_collection(&geom)))
            }
            "ST_ISRING" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::LineString(ls) => {
                        let is_ring = ls.is_closed() && ls.0.len() >= 4;
                        Ok(Value::bool_val(is_ring))
                    }
                    _ => Ok(Value::bool_val(false)),
                }
            }
            "ST_ISCLOSED" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::LineString(ls) => Ok(Value::bool_val(ls.is_closed())),
                    Geometry::MultiLineString(mls) => {
                        let all_closed = mls.0.iter().all(|ls| ls.is_closed());
                        Ok(Value::bool_val(all_closed))
                    }
                    _ => Ok(Value::bool_val(true)),
                }
            }
            "ST_GEOMETRYTYPE" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                Ok(Value::string(geometry_type_name(&geom)))
            }
            "ST_POINTN" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_POINTN requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let n = args[1].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::LineString(ls) => {
                        let idx = (n - 1) as usize;
                        if idx < ls.0.len() {
                            let coord = &ls.0[idx];
                            Ok(Value::geography(format_wkt_number(&Geometry::Point(
                                Point::new(coord.x, coord.y),
                            ))))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    _ => Ok(Value::null()),
                }
            }
            "ST_STARTPOINT" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::LineString(ls) => {
                        if let Some(coord) = ls.0.first() {
                            Ok(Value::geography(format_wkt_number(&Geometry::Point(
                                Point::new(coord.x, coord.y),
                            ))))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    _ => Ok(Value::null()),
                }
            }
            "ST_ENDPOINT" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::LineString(ls) => {
                        if let Some(coord) = ls.0.last() {
                            Ok(Value::geography(format_wkt_number(&Geometry::Point(
                                Point::new(coord.x, coord.y),
                            ))))
                        } else {
                            Ok(Value::null())
                        }
                    }
                    _ => Ok(Value::null()),
                }
            }
            "ST_BOUNDARY" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                match geom {
                    Geometry::Polygon(poly) => Ok(Value::geography(format_wkt_number(
                        &Geometry::LineString(poly.exterior().clone()),
                    ))),
                    _ => Ok(Value::geography(format_wkt_number(&geom))),
                }
            }
            "ST_MAXDISTANCE" => {
                if args.len() < 2 {
                    return Err(Error::InvalidQuery(
                        "ST_MAXDISTANCE requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let wkt1 = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let wkt2 = args[1]
                    .as_geography()
                    .or_else(|| args[1].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[1].data_type().to_string(),
                    })?;
                let geom1 = parse_geography(wkt1)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let geom2 = parse_geography(wkt2)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                fn get_points(geom: &Geometry<f64>) -> Vec<Point<f64>> {
                    match geom {
                        Geometry::Point(p) => vec![*p],
                        Geometry::LineString(ls) => {
                            ls.0.iter().map(|c| Point::new(c.x, c.y)).collect()
                        }
                        Geometry::Polygon(poly) => poly
                            .exterior()
                            .0
                            .iter()
                            .map(|c| Point::new(c.x, c.y))
                            .collect(),
                        Geometry::MultiPoint(mp) => mp.0.clone(),
                        _ => vec![],
                    }
                }
                let points1 = get_points(&geom1);
                let points2 = get_points(&geom2);
                let mut max_dist = 0.0f64;
                for p1 in &points1 {
                    for p2 in &points2 {
                        let dist = p1.geodesic_distance(p2);
                        if dist > max_dist {
                            max_dist = dist;
                        }
                    }
                }
                Ok(Value::float64(max_dist))
            }
            "ST_GEOHASH" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                let precision = if args.len() > 1 {
                    args[1].as_i64().unwrap_or(12).min(12) as usize
                } else {
                    12
                };
                match geom {
                    Geometry::Point(p) => {
                        let hash =
                            geohash::encode(geohash::Coord { x: p.x(), y: p.y() }, precision)
                                .map_err(|e| {
                                    Error::InvalidQuery(format!("Geohash error: {:?}", e))
                                })?;
                        Ok(Value::string(hash))
                    }
                    _ => {
                        if let Some(centroid) = geom.centroid() {
                            let hash = geohash::encode(
                                geohash::Coord {
                                    x: centroid.x(),
                                    y: centroid.y(),
                                },
                                precision,
                            )
                            .map_err(|e| Error::InvalidQuery(format!("Geohash error: {:?}", e)))?;
                            Ok(Value::string(hash))
                        } else {
                            Ok(Value::null())
                        }
                    }
                }
            }
            "ST_GEOGPOINTFROMGEOHASH" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let hash = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let (coord, _, _) = geohash::decode(hash)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geohash: {:?}", e)))?;
                Ok(Value::geography(format_wkt_number(&Geometry::Point(
                    Point::new(coord.x, coord.y),
                ))))
            }
            "ST_BOUNDINGBOX" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let wkt = args[0]
                    .as_geography()
                    .or_else(|| args[0].as_str())
                    .ok_or_else(|| Error::TypeMismatch {
                        expected: "GEOGRAPHY".to_string(),
                        actual: args[0].data_type().to_string(),
                    })?;
                let geom = parse_geography(wkt)
                    .map_err(|e| Error::InvalidQuery(format!("Invalid geometry: {}", e)))?;
                use geo::BoundingRect;
                if let Some(rect) = geom.bounding_rect() {
                    let min = rect.min();
                    let max = rect.max();
                    let coords = vec![
                        Coord { x: min.x, y: min.y },
                        Coord { x: min.x, y: max.y },
                        Coord { x: max.x, y: max.y },
                        Coord { x: max.x, y: min.y },
                        Coord { x: min.x, y: min.y },
                    ];
                    let poly = Polygon::new(LineString::new(coords), vec![]);
                    Ok(Value::geography(format_wkt_number(&Geometry::Polygon(
                        poly,
                    ))))
                } else {
                    Ok(Value::null())
                }
            }
            "RANGE_OVERLAPS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RANGE_OVERLAPS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let range1 = args[0].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let range2 = args[1].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                Ok(Value::bool_val(range1.overlaps(range2)))
            }
            "RANGE_CONTAINS" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RANGE_CONTAINS requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let range = args[0].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                if args[1].is_null() {
                    return Ok(Value::bool_val(false));
                }
                Ok(Value::bool_val(range.contains(&args[1])))
            }
            "RANGE_INTERSECT" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "RANGE_INTERSECT requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let range1 = args[0].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let range2 = args[1].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                match range1.intersect(range2) {
                    Some(r) => Ok(Value::range_val(r)),
                    None => Ok(Value::null()),
                }
            }
            "RANGE_START" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "RANGE_START requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let range = args[0].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match range.start() {
                    Some(v) => Ok(v.clone()),
                    None => Ok(Value::null()),
                }
            }
            "RANGE_END" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "RANGE_END requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let range = args[0].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match range.end() {
                    Some(v) => Ok(v.clone()),
                    None => Ok(Value::null()),
                }
            }
            "GENERATE_RANGE_ARRAY" => {
                if args.len() != 2 {
                    return Err(Error::InvalidQuery(
                        "GENERATE_RANGE_ARRAY requires 2 arguments".to_string(),
                    ));
                }
                if args[0].is_null() || args[1].is_null() {
                    return Ok(Value::null());
                }
                let range = args[0].as_range().ok_or_else(|| Error::TypeMismatch {
                    expected: "RANGE".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let interval = args[1].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;

                let start = match range.start() {
                    Some(v) => v.clone(),
                    None => return Ok(Value::null()),
                };
                let end = match range.end() {
                    Some(v) => v.clone(),
                    None => return Ok(Value::null()),
                };

                let mut result = Vec::new();

                if let (Some(start_date), Some(end_date)) = (start.as_date(), end.as_date()) {
                    let mut current_date = start_date;
                    while current_date < end_date {
                        let next_date = if interval.days > 0 {
                            current_date + chrono::Duration::days(interval.days as i64)
                        } else if interval.months > 0 {
                            let month = current_date.month0() as i32 + interval.months;
                            let year = current_date.year() + month / 12;
                            let month = (month % 12) as u32;
                            chrono::NaiveDate::from_ymd_opt(
                                year,
                                month + 1,
                                current_date.day().min(28),
                            )
                            .unwrap_or(current_date)
                        } else {
                            break;
                        };

                        let range_end = if next_date > end_date {
                            end_date
                        } else {
                            next_date
                        };
                        result.push(Value::range(
                            Some(Value::date(current_date)),
                            Some(Value::date(range_end)),
                        ));
                        current_date = next_date;
                    }
                } else if let (Some(start_ts), Some(end_ts)) =
                    (start.as_timestamp(), end.as_timestamp())
                {
                    let mut current_ts = start_ts;
                    while current_ts < end_ts {
                        let duration = chrono::Duration::nanoseconds(interval.nanos)
                            + chrono::Duration::days(interval.days as i64);
                        let next_ts = current_ts + duration;
                        let range_end = if next_ts > end_ts { end_ts } else { next_ts };
                        result.push(Value::range(
                            Some(Value::timestamp(current_ts)),
                            Some(Value::timestamp(range_end)),
                        ));
                        current_ts = next_ts;
                    }
                } else if let (Some(start_dt), Some(end_dt)) =
                    (start.as_datetime(), end.as_datetime())
                {
                    let mut current_dt = start_dt;
                    while current_dt < end_dt {
                        let duration = chrono::Duration::nanoseconds(interval.nanos)
                            + chrono::Duration::days(interval.days as i64);
                        let next_dt = current_dt + duration;
                        let range_end = if next_dt > end_dt { end_dt } else { next_dt };
                        result.push(Value::range(
                            Some(Value::datetime(current_dt)),
                            Some(Value::datetime(range_end)),
                        ));
                        current_dt = next_dt;
                    }
                }

                Ok(Value::array(result))
            }
            "ROW_NUMBER" | "RANK" | "DENSE_RANK" | "PERCENT_RANK" | "CUME_DIST" | "NTILE" => {
                Ok(Value::int64(1))
            }
            "LAG" | "LEAD" | "FIRST_VALUE" | "LAST_VALUE" | "NTH_VALUE" => {
                if args.is_empty() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "HLL_COUNT_EXTRACT" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "HLL_COUNT_EXTRACT requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let sketch_str = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                if let Some(encoded) = sketch_str.strip_prefix("HLL_SKETCH:p14:") {
                    if let Ok(registers) =
                        base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                    {
                        let m = registers.len() as f64;
                        let mut sum: f64 = 0.0;
                        let mut zeros = 0;
                        for &r in &registers {
                            sum += 2.0_f64.powi(-(r as i32));
                            if r == 0 {
                                zeros += 1;
                            }
                        }
                        let alpha = 0.7213 / (1.0 + 1.079 / m);
                        let raw_estimate = alpha * m * m / sum;
                        let estimate = if raw_estimate <= 2.5 * m && zeros > 0 {
                            m * (m / zeros as f64).ln()
                        } else {
                            raw_estimate
                        };
                        return Ok(Value::int64(estimate as i64));
                    }
                } else if let Some(rest) = sketch_str.strip_prefix("HLL_SKETCH:p15:n") {
                    if let Ok(count) = rest.parse::<i64>() {
                        return Ok(Value::int64(count));
                    }
                }
                Ok(Value::int64(0))
            }
            "TRANSFORM" => {
                if args.len() != 4 {
                    return Err(Error::InvalidQuery(
                        "TRANSFORM requires 4 arguments: (value, from_array, to_array, default)"
                            .to_string(),
                    ));
                }
                let value = &args[0];
                let from_array = args[1].as_array().ok_or_else(|| Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let to_array = args[2].as_array().ok_or_else(|| Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: args[2].data_type().to_string(),
                })?;
                let default = &args[3];
                if from_array.len() != to_array.len() {
                    return Err(Error::InvalidQuery(
                        "TRANSFORM: from_array and to_array must have the same length".to_string(),
                    ));
                }
                for (i, from_val) in from_array.iter().enumerate() {
                    if value == from_val {
                        return Ok(to_array[i].clone());
                    }
                }
                Ok(default.clone())
            }
            "ARRAYJOIN" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "arrayJoin requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                Ok(args[0].clone())
            }
            "SUMSTATE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "sumState requires 1 argument".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            "RUNNINGACCUMULATE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "runningAccumulate requires 1 argument".to_string(),
                    ));
                }
                Ok(args[0].clone())
            }
            "ARRAYENUMERATE" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "arrayEnumerate requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(arr) = args[0].as_array() {
                    let indices: Vec<Value> =
                        (1..=arr.len()).map(|i| Value::int64(i as i64)).collect();
                    return Ok(Value::array(indices));
                }
                Err(Error::TypeMismatch {
                    expected: "ARRAY".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "MAPKEYS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "mapKeys requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(struct_val) = args[0].as_struct() {
                    let keys: Vec<Value> = struct_val
                        .iter()
                        .map(|(k, _)| Value::string(k.clone()))
                        .collect();
                    return Ok(Value::array(keys));
                }
                Err(Error::TypeMismatch {
                    expected: "MAP".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "MAPVALUES" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "mapValues requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                if let Some(struct_val) = args[0].as_struct() {
                    let values: Vec<Value> = struct_val.iter().map(|(_, v)| v.clone()).collect();
                    return Ok(Value::array(values));
                }
                Err(Error::TypeMismatch {
                    expected: "MAP".to_string(),
                    actual: args[0].data_type().to_string(),
                })
            }
            "MAP" => {
                if !args.len().is_multiple_of(2) {
                    return Err(Error::InvalidQuery(
                        "map() requires an even number of arguments (key-value pairs)".to_string(),
                    ));
                }
                let mut pairs: Vec<(String, Value)> = Vec::new();
                let mut i = 0;
                while i < args.len() {
                    let key = args[i].as_str().ok_or_else(|| Error::TypeMismatch {
                        expected: "STRING".to_string(),
                        actual: args[i].data_type().to_string(),
                    })?;
                    pairs.push((key.to_string(), args[i + 1].clone()));
                    i += 2;
                }
                Ok(Value::struct_val(pairs))
            }
            "NUMBERS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "numbers requires 1 argument".to_string(),
                    ));
                }
                let n = args[0].as_i64().ok_or_else(|| Error::TypeMismatch {
                    expected: "INT64".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let values: Vec<Value> = (0..n).map(Value::int64).collect();
                Ok(Value::array(values))
            }
            "JUSTIFY_DAYS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "JUSTIFY_DAYS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let interval = args[0].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                let extra_months = interval.days / 30;
                let remaining_days = interval.days % 30;
                Ok(Value::interval(yachtsql_common::types::IntervalValue {
                    months: interval.months + extra_months,
                    days: remaining_days,
                    nanos: interval.nanos,
                }))
            }
            "JUSTIFY_HOURS" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "JUSTIFY_HOURS requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let interval = args[0].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                use yachtsql_common::types::IntervalValue;
                let total_micros = interval.nanos / IntervalValue::NANOS_PER_MICRO;
                let total_hours = total_micros / IntervalValue::MICROS_PER_HOUR;
                let extra_days = total_hours / 24;
                let remaining_micros =
                    total_micros - extra_days * 24 * IntervalValue::MICROS_PER_HOUR;
                Ok(Value::interval(IntervalValue {
                    months: interval.months,
                    days: interval.days + extra_days as i32,
                    nanos: remaining_micros * IntervalValue::NANOS_PER_MICRO,
                }))
            }
            "JUSTIFY_INTERVAL" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "JUSTIFY_INTERVAL requires 1 argument".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let interval = args[0].as_interval().ok_or_else(|| Error::TypeMismatch {
                    expected: "INTERVAL".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                use yachtsql_common::types::IntervalValue;
                let total_micros = interval.nanos / IntervalValue::NANOS_PER_MICRO;
                let total_hours = total_micros / IntervalValue::MICROS_PER_HOUR;
                let extra_days_from_hours = total_hours / 24;
                let remaining_micros =
                    total_micros - extra_days_from_hours * 24 * IntervalValue::MICROS_PER_HOUR;
                let total_days = interval.days + extra_days_from_hours as i32;
                let extra_months = total_days / 30;
                let remaining_days = total_days % 30;
                Ok(Value::interval(IntervalValue {
                    months: interval.months + extra_months,
                    days: remaining_days,
                    nanos: remaining_micros * IntervalValue::NANOS_PER_MICRO,
                }))
            }
            "PARSE_JSON" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let s = args[0].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[0].data_type().to_string(),
                })?;
                match serde_json::from_str::<serde_json::Value>(s) {
                    Ok(json_val) => Ok(Value::json(json_val)),
                    Err(_) => Err(Error::InvalidQuery(format!("Invalid JSON string: {}", s))),
                }
            }
            "JSON_SET" => {
                if args.len() < 3 {
                    return Err(Error::InvalidQuery(
                        "JSON_SET requires at least 3 arguments".to_string(),
                    ));
                }
                if args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|_| Error::InvalidQuery("Invalid JSON".to_string()))?
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "JSON or STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };
                let path = args[1].as_str().ok_or_else(|| Error::TypeMismatch {
                    expected: "STRING".to_string(),
                    actual: args[1].data_type().to_string(),
                })?;
                let new_value = self.value_to_json(&args[2]);
                let result = self.json_path_set(json_val, path, new_value);
                Ok(Value::json(result))
            }
            "JSON_KEYS" => {
                if args.is_empty() || args[0].is_null() {
                    return Ok(Value::null());
                }
                let json_val = if let Some(jv) = args[0].as_json() {
                    jv.clone()
                } else if let Some(s) = args[0].as_str() {
                    serde_json::from_str::<serde_json::Value>(s)
                        .map_err(|_| Error::InvalidQuery("Invalid JSON".to_string()))?
                } else {
                    return Err(Error::TypeMismatch {
                        expected: "JSON or STRING".to_string(),
                        actual: args[0].data_type().to_string(),
                    });
                };
                match json_val {
                    serde_json::Value::Object(map) => {
                        let keys: Vec<Value> =
                            map.keys().map(|k| Value::string(k.clone())).collect();
                        Ok(Value::array(keys))
                    }
                    _ => Ok(Value::null()),
                }
            }
            "KEYS.NEW_KEYSET" => {
                if args.len() != 1 {
                    return Err(Error::InvalidQuery(
                        "KEYS.NEW_KEYSET requires 1 argument".to_string(),
                    ));
                }
                let keyset_type = args[0].as_str().unwrap_or("AEAD_AES_GCM_256");
                let keyset = format!("KEYSET:{}:{}", keyset_type, uuid::Uuid::new_v4());
                Ok(Value::bytes(keyset.as_bytes().to_vec()))
            }
            "AEAD.ENCRYPT" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::InvalidQuery(
                        "AEAD.ENCRYPT requires 2-3 arguments".to_string(),
                    ));
                }
                let plaintext = args[1].as_bytes().unwrap_or_default();
                let aad = if args.len() > 2 {
                    args[2].as_bytes().unwrap_or_default().to_vec()
                } else {
                    Vec::new()
                };
                let mut result = Vec::new();
                result.extend_from_slice(b"AEAD_ENCRYPTED:");
                result.extend_from_slice(&(aad.len() as u32).to_le_bytes());
                result.extend_from_slice(&aad);
                result.extend_from_slice(plaintext);
                Ok(Value::bytes(result))
            }
            "SESSION_USER" => Ok(Value::string("anonymous@localhost".to_string())),
            "CURRENT_USER" => Ok(Value::string("anonymous@localhost".to_string())),
            _ => Err(Error::UnsupportedFeature(format!(
                "Function not yet supported: {}",
                name
            ))),
        }
    }

    fn try_evaluate_udf(
        &self,
        name: &str,
        func: &sqlparser::ast::Function,
        record: &Record,
    ) -> Result<Option<Value>> {
        let user_functions = match self.user_functions {
            Some(funcs) => funcs,
            None => return Ok(None),
        };

        let udf = match user_functions.get(name) {
            Some(f) => f,
            None => return Ok(None),
        };

        let args = self.extract_udf_arg_exprs(func)?;

        if args.len() != udf.parameters.len() {
            return Err(Error::InvalidQuery(format!(
                "Function {} expects {} arguments, got {}",
                name,
                udf.parameters.len(),
                args.len()
            )));
        }

        match &udf.body {
            FunctionBody::Sql(body_expr) => {
                let substituted_body =
                    self.substitute_udf_params(body_expr, &udf.parameters, &args);
                Ok(Some(self.evaluate(&substituted_body, record)?))
            }
            FunctionBody::JavaScript(js_code) => {
                let param_names: Vec<String> = udf
                    .parameters
                    .iter()
                    .filter_map(|p| p.name.as_ref().map(|n| n.value.clone()))
                    .collect();

                let arg_values: Vec<Value> = args
                    .iter()
                    .map(|arg| self.evaluate(arg, record))
                    .collect::<Result<Vec<_>>>()?;

                let result = evaluate_js_function(js_code, &param_names, &arg_values)
                    .map_err(|e| Error::InvalidQuery(format!("JavaScript UDF error: {}", e)))?;

                Ok(Some(result))
            }
        }
    }

    fn extract_udf_arg_exprs(&self, func: &sqlparser::ast::Function) -> Result<Vec<Expr>> {
        match &func.args {
            sqlparser::ast::FunctionArguments::None => Ok(vec![]),
            sqlparser::ast::FunctionArguments::Subquery(_) => Err(Error::UnsupportedFeature(
                "Subquery arguments not supported in UDF".to_string(),
            )),
            sqlparser::ast::FunctionArguments::List(list) => {
                let mut exprs = Vec::new();
                for arg in &list.args {
                    match arg {
                        sqlparser::ast::FunctionArg::Unnamed(arg_expr)
                        | sqlparser::ast::FunctionArg::ExprNamed { arg: arg_expr, .. } => {
                            match arg_expr {
                                sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                                    exprs.push(expr.clone());
                                }
                                _ => {
                                    return Err(Error::InvalidQuery(
                                        "Invalid argument in UDF call".to_string(),
                                    ));
                                }
                            }
                        }
                        sqlparser::ast::FunctionArg::Named { arg, .. } => match arg {
                            sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                                exprs.push(expr.clone());
                            }
                            _ => {
                                return Err(Error::InvalidQuery(
                                    "Invalid argument in UDF call".to_string(),
                                ));
                            }
                        },
                    }
                }
                Ok(exprs)
            }
        }
    }

    fn substitute_udf_params(
        &self,
        expr: &Expr,
        params: &[sqlparser::ast::OperateFunctionArg],
        args: &[Expr],
    ) -> Expr {
        match expr {
            Expr::Identifier(ident) => {
                let ident_upper = ident.value.to_uppercase();
                for (i, param) in params.iter().enumerate() {
                    let param_name = match &param.name {
                        Some(n) => n.value.to_uppercase(),
                        None => continue,
                    };
                    if param_name == ident_upper {
                        return args[i].clone();
                    }
                }
                expr.clone()
            }
            Expr::BinaryOp { left, op, right } => Expr::BinaryOp {
                left: Box::new(self.substitute_udf_params(left, params, args)),
                op: op.clone(),
                right: Box::new(self.substitute_udf_params(right, params, args)),
            },
            Expr::UnaryOp { op, expr: inner } => Expr::UnaryOp {
                op: *op,
                expr: Box::new(self.substitute_udf_params(inner, params, args)),
            },
            Expr::Nested(inner) => {
                Expr::Nested(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::Function(func) => {
                let new_args = match &func.args {
                    sqlparser::ast::FunctionArguments::None => {
                        sqlparser::ast::FunctionArguments::None
                    }
                    sqlparser::ast::FunctionArguments::Subquery(q) => {
                        sqlparser::ast::FunctionArguments::Subquery(q.clone())
                    }
                    sqlparser::ast::FunctionArguments::List(list) => {
                        let new_list_args: Vec<_> = list
                            .args
                            .iter()
                            .map(|arg| match arg {
                                sqlparser::ast::FunctionArg::Unnamed(arg_expr) => match arg_expr {
                                    sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                        sqlparser::ast::FunctionArg::Unnamed(
                                            sqlparser::ast::FunctionArgExpr::Expr(
                                                self.substitute_udf_params(e, params, args),
                                            ),
                                        )
                                    }
                                    other => sqlparser::ast::FunctionArg::Unnamed(other.clone()),
                                },
                                sqlparser::ast::FunctionArg::Named {
                                    name,
                                    arg,
                                    operator,
                                } => match arg {
                                    sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                        sqlparser::ast::FunctionArg::Named {
                                            name: name.clone(),
                                            arg: sqlparser::ast::FunctionArgExpr::Expr(
                                                self.substitute_udf_params(e, params, args),
                                            ),
                                            operator: operator.clone(),
                                        }
                                    }
                                    other => sqlparser::ast::FunctionArg::Named {
                                        name: name.clone(),
                                        arg: other.clone(),
                                        operator: operator.clone(),
                                    },
                                },
                                sqlparser::ast::FunctionArg::ExprNamed {
                                    name,
                                    arg,
                                    operator,
                                } => match arg {
                                    sqlparser::ast::FunctionArgExpr::Expr(e) => {
                                        sqlparser::ast::FunctionArg::ExprNamed {
                                            name: name.clone(),
                                            arg: sqlparser::ast::FunctionArgExpr::Expr(
                                                self.substitute_udf_params(e, params, args),
                                            ),
                                            operator: operator.clone(),
                                        }
                                    }
                                    other => sqlparser::ast::FunctionArg::ExprNamed {
                                        name: name.clone(),
                                        arg: other.clone(),
                                        operator: operator.clone(),
                                    },
                                },
                            })
                            .collect();
                        sqlparser::ast::FunctionArguments::List(
                            sqlparser::ast::FunctionArgumentList {
                                duplicate_treatment: list.duplicate_treatment,
                                args: new_list_args,
                                clauses: list.clauses.clone(),
                            },
                        )
                    }
                };
                Expr::Function(sqlparser::ast::Function {
                    name: func.name.clone(),
                    uses_odbc_syntax: func.uses_odbc_syntax,
                    args: new_args,
                    filter: func.filter.clone(),
                    null_treatment: func.null_treatment,
                    over: func.over.clone(),
                    within_group: func.within_group.clone(),
                    parameters: func.parameters.clone(),
                })
            }
            Expr::Case {
                case_token,
                end_token,
                operand,
                conditions,
                else_result,
            } => Expr::Case {
                case_token: case_token.clone(),
                end_token: end_token.clone(),
                operand: operand
                    .as_ref()
                    .map(|o| Box::new(self.substitute_udf_params(o, params, args))),
                conditions: conditions
                    .iter()
                    .map(|cw| sqlparser::ast::CaseWhen {
                        condition: self.substitute_udf_params(&cw.condition, params, args),
                        result: self.substitute_udf_params(&cw.result, params, args),
                    })
                    .collect(),
                else_result: else_result
                    .as_ref()
                    .map(|e| Box::new(self.substitute_udf_params(e, params, args))),
            },
            Expr::Cast {
                expr: inner,
                data_type,
                format,
                kind,
            } => Expr::Cast {
                expr: Box::new(self.substitute_udf_params(inner, params, args)),
                data_type: data_type.clone(),
                format: format.clone(),
                kind: kind.clone(),
            },
            Expr::IsFalse(inner) => {
                Expr::IsFalse(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::IsNotFalse(inner) => {
                Expr::IsNotFalse(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::IsTrue(inner) => {
                Expr::IsTrue(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::IsNotTrue(inner) => {
                Expr::IsNotTrue(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::IsNull(inner) => {
                Expr::IsNull(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::IsNotNull(inner) => {
                Expr::IsNotNull(Box::new(self.substitute_udf_params(inner, params, args)))
            }
            Expr::Struct { values, fields } => Expr::Struct {
                values: values
                    .iter()
                    .map(|v| {
                        if let Expr::Identifier(ident) = v {
                            let ident_upper = ident.value.to_uppercase();
                            for (i, param) in params.iter().enumerate() {
                                let param_name = match &param.name {
                                    Some(n) => n.value.to_uppercase(),
                                    None => continue,
                                };
                                if param_name == ident_upper {
                                    return Expr::Named {
                                        expr: Box::new(args[i].clone()),
                                        name: ident.clone(),
                                    };
                                }
                            }
                        }
                        self.substitute_udf_params(v, params, args)
                    })
                    .collect(),
                fields: fields.clone(),
            },
            Expr::Array(arr) => Expr::Array(sqlparser::ast::Array {
                elem: arr
                    .elem
                    .iter()
                    .map(|e| self.substitute_udf_params(e, params, args))
                    .collect(),
                named: arr.named,
            }),
            _ => expr.clone(),
        }
    }

    fn value_to_json(&self, value: &Value) -> serde_json::Value {
        if value.is_null() {
            return serde_json::Value::Null;
        }
        if let Some(jv) = value.as_json() {
            return jv.clone();
        }
        if let Some(b) = value.as_bool() {
            return serde_json::Value::Bool(b);
        }
        if let Some(i) = value.as_i64() {
            return serde_json::Value::Number(serde_json::Number::from(i));
        }
        if let Some(f) = value.as_f64() {
            if let Some(n) = serde_json::Number::from_f64(f) {
                return serde_json::Value::Number(n);
            }
        }
        if let Some(s) = value.as_str() {
            return serde_json::Value::String(s.to_string());
        }
        if let Some(arr) = value.as_array() {
            let json_arr: Vec<serde_json::Value> =
                arr.iter().map(|v| self.value_to_json(v)).collect();
            return serde_json::Value::Array(json_arr);
        }
        if let Some(fields) = value.as_struct() {
            let mut map = serde_json::Map::new();
            for (name, val) in fields {
                map.insert(name.clone(), self.value_to_json(val));
            }
            return serde_json::Value::Object(map);
        }
        serde_json::Value::String(value.to_string())
    }

    fn json_path_extract(&self, json: &serde_json::Value, path: &str) -> Option<serde_json::Value> {
        let path = path.trim_start_matches('$');
        let mut current = json.clone();

        for part in path.split('.').filter(|s| !s.is_empty()) {
            let part = part.trim();
            if let Some(idx_start) = part.find('[') {
                let key = &part[..idx_start];
                if !key.is_empty() {
                    current = current.get(key)?.clone();
                }
                let idx_end = part.find(']')?;
                let idx_str = &part[idx_start + 1..idx_end];
                let idx: usize = idx_str.parse().ok()?;
                current = current.get(idx)?.clone();
            } else {
                current = current.get(part)?.clone();
            }
        }

        Some(current)
    }

    fn json_path_set(
        &self,
        mut json: serde_json::Value,
        path: &str,
        new_value: serde_json::Value,
    ) -> serde_json::Value {
        let path = path.trim_start_matches('$');
        let parts: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();

        if parts.is_empty() {
            return new_value;
        }

        fn set_recursive(
            current: &mut serde_json::Value,
            parts: &[&str],
            new_value: serde_json::Value,
        ) {
            if parts.is_empty() {
                return;
            }

            let part = parts[0].trim();
            let remaining = &parts[1..];

            if remaining.is_empty() {
                if let serde_json::Value::Object(map) = current {
                    map.insert(part.to_string(), new_value);
                }
            } else if let serde_json::Value::Object(map) = current {
                let entry = map
                    .entry(part.to_string())
                    .or_insert(serde_json::Value::Object(serde_json::Map::new()));
                set_recursive(entry, remaining, new_value);
            }
        }

        set_recursive(&mut json, &parts, new_value);
        json
    }

    fn json_path_remove(&self, mut json: serde_json::Value, path: &str) -> serde_json::Value {
        let path = path.trim_start_matches('$');
        let parts: Vec<&str> = path.split('.').filter(|s| !s.is_empty()).collect();

        if parts.is_empty() {
            return json;
        }

        fn remove_recursive(current: &mut serde_json::Value, parts: &[&str]) {
            if parts.is_empty() {
                return;
            }

            let part = parts[0].trim();
            let remaining = &parts[1..];

            if remaining.is_empty() {
                if let serde_json::Value::Object(map) = current {
                    map.remove(part);
                }
            } else if let serde_json::Value::Object(map) = current {
                if let Some(entry) = map.get_mut(part) {
                    remove_recursive(entry, remaining);
                }
            }
        }

        remove_recursive(&mut json, &parts);
        json
    }

    fn json_strip_nulls(&self, json: serde_json::Value) -> serde_json::Value {
        match json {
            serde_json::Value::Object(map) => {
                let filtered: serde_json::Map<String, serde_json::Value> = map
                    .into_iter()
                    .filter(|(_, v)| !v.is_null())
                    .map(|(k, v)| (k, self.json_strip_nulls(v)))
                    .collect();
                serde_json::Value::Object(filtered)
            }
            serde_json::Value::Array(arr) => {
                let filtered: Vec<serde_json::Value> =
                    arr.into_iter().map(|v| self.json_strip_nulls(v)).collect();
                serde_json::Value::Array(filtered)
            }
            other => other,
        }
    }

    fn extract_function_args(
        &self,
        func: &sqlparser::ast::Function,
        record: &Record,
    ) -> Result<Vec<Value>> {
        let mut args = Vec::new();
        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
            for arg in &arg_list.args {
                if let sqlparser::ast::FunctionArg::Unnamed(
                    sqlparser::ast::FunctionArgExpr::Expr(expr),
                ) = arg
                {
                    let val = self.evaluate_with_date_part_fallback(expr, record)?;
                    args.push(val);
                }
            }
        }
        Ok(args)
    }

    fn evaluate_with_date_part_fallback(&self, expr: &Expr, record: &Record) -> Result<Value> {
        match self.evaluate(expr, record) {
            Ok(val) => Ok(val),
            Err(Error::ColumnNotFound(name)) => {
                let upper = name.to_uppercase();
                match upper.as_str() {
                    "MICROSECOND" | "MILLISECOND" | "SECOND" | "MINUTE" | "HOUR" | "DAY"
                    | "DAYOFWEEK" | "DAYOFYEAR" | "WEEK" | "MONTH" | "QUARTER" | "YEAR" => {
                        Ok(Value::string(upper))
                    }
                    _ => Err(Error::ColumnNotFound(name)),
                }
            }
            Err(e) => Err(e),
        }
    }

    fn parse_interval_string(&self, s: &str) -> Result<yachtsql_common::types::IntervalValue> {
        use yachtsql_common::types::IntervalValue;

        let mut months: i32 = 0;
        let mut days: i32 = 0;
        let mut nanos: i64 = 0;

        let s = s.to_lowercase();
        let re = regex::Regex::new(r"(-?\d+)\s*(year|month|day|hour|minute|second)s?")
            .map_err(|e| Error::InvalidQuery(format!("Invalid interval regex: {}", e)))?;

        for cap in re.captures_iter(&s) {
            let value: i64 = cap[1]
                .parse()
                .map_err(|_| Error::InvalidQuery("Invalid interval number".to_string()))?;
            let unit = &cap[2];
            match unit {
                "year" => months += value as i32 * 12,
                "month" => months += value as i32,
                "day" => days += value as i32,
                "hour" => {
                    nanos += value * IntervalValue::MICROS_PER_HOUR * IntervalValue::NANOS_PER_MICRO
                }
                "minute" => {
                    nanos +=
                        value * IntervalValue::MICROS_PER_MINUTE * IntervalValue::NANOS_PER_MICRO
                }
                "second" => {
                    nanos +=
                        value * IntervalValue::MICROS_PER_SECOND * IntervalValue::NANOS_PER_MICRO
                }
                _ => {}
            }
        }

        Ok(IntervalValue {
            months,
            days,
            nanos,
        })
    }

    fn evaluate_make_interval(
        &self,
        func: &sqlparser::ast::Function,
        record: &Record,
    ) -> Result<Value> {
        use yachtsql_common::types::IntervalValue;

        let mut year: i64 = 0;
        let mut month: i64 = 0;
        let mut day: i64 = 0;
        let mut hour: i64 = 0;
        let mut minute: i64 = 0;
        let mut second: i64 = 0;

        if let sqlparser::ast::FunctionArguments::List(arg_list) = &func.args {
            for arg in &arg_list.args {
                match arg {
                    sqlparser::ast::FunctionArg::Named { name, arg, .. } => {
                        let num = match arg {
                            sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                                self.evaluate(expr, record)?.as_i64().unwrap_or(0)
                            }
                            _ => 0,
                        };
                        match name.value.to_uppercase().as_str() {
                            "YEAR" => year = num,
                            "MONTH" => month = num,
                            "DAY" => day = num,
                            "HOUR" => hour = num,
                            "MINUTE" => minute = num,
                            "SECOND" => second = num,
                            _ => {}
                        }
                    }
                    sqlparser::ast::FunctionArg::ExprNamed { name, arg, .. } => {
                        let name_str = match name {
                            Expr::Identifier(ident) => ident.value.to_uppercase(),
                            _ => continue,
                        };
                        let num = match arg {
                            sqlparser::ast::FunctionArgExpr::Expr(expr) => {
                                self.evaluate(expr, record)?.as_i64().unwrap_or(0)
                            }
                            _ => 0,
                        };
                        match name_str.as_str() {
                            "YEAR" => year = num,
                            "MONTH" => month = num,
                            "DAY" => day = num,
                            "HOUR" => hour = num,
                            "MINUTE" => minute = num,
                            "SECOND" => second = num,
                            _ => {}
                        }
                    }
                    sqlparser::ast::FunctionArg::Unnamed(
                        sqlparser::ast::FunctionArgExpr::Expr(_),
                    ) => {}
                    _ => {}
                }
            }
        }

        let months = (year * 12 + month) as i32;
        let days = day as i32;
        let nanos = (hour * IntervalValue::MICROS_PER_HOUR
            + minute * IntervalValue::MICROS_PER_MINUTE
            + second * IntervalValue::MICROS_PER_SECOND)
            * IntervalValue::NANOS_PER_MICRO;

        Ok(Value::interval(IntervalValue {
            months,
            days,
            nanos,
        }))
    }

    fn compare_for_ordering(&self, a: &Value, b: &Value) -> std::cmp::Ordering {
        if let (Some(ai), Some(bi)) = (a.as_i64(), b.as_i64()) {
            return ai.cmp(&bi);
        }
        if let (Some(af), Some(bf)) = (a.as_f64(), b.as_f64()) {
            return af.partial_cmp(&bf).unwrap_or(std::cmp::Ordering::Equal);
        }
        if let (Some(as_), Some(bs)) = (a.as_str(), b.as_str()) {
            return as_.cmp(bs);
        }
        std::cmp::Ordering::Equal
    }

    pub fn evaluate_to_bool(&self, expr: &Expr, record: &Record) -> Result<bool> {
        let val = self.evaluate(expr, record)?;
        if val.is_null() {
            return Ok(false);
        }
        val.as_bool().ok_or_else(|| Error::TypeMismatch {
            expected: "BOOL".to_string(),
            actual: val.data_type().to_string(),
        })
    }

    pub fn evaluate_binary_op_values(
        &self,
        left: &Value,
        op: &BinaryOperator,
        right: &Value,
    ) -> Result<Value> {
        self.evaluate_binary_op(left, op, right)
    }

    pub fn evaluate_function_with_args(&self, name: &str, args: &[Value]) -> Result<Value> {
        self.evaluate_function_impl(name, args)
    }
}
