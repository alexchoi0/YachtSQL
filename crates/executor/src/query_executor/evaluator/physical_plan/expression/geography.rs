use yachtsql_core::error::{Error, Result};
use yachtsql_core::types::Value;
use yachtsql_functions::geography::{self, Geometry};
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_geography_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "ST_GEOGPOINT" => Self::eval_st_geogpoint(args, batch, row_idx),
            "ST_GEOGFROMTEXT" => Self::eval_st_geogfromtext(args, batch, row_idx),
            "ST_GEOGFROMGEOJSON" => Self::eval_st_geogfromgeojson(args, batch, row_idx),
            "ST_ASTEXT" => Self::eval_st_astext(args, batch, row_idx),
            "ST_ASGEOJSON" => Self::eval_st_asgeojson(args, batch, row_idx),
            "ST_ASBINARY" => Self::eval_st_asbinary(args, batch, row_idx),
            "ST_X" => Self::eval_st_x(args, batch, row_idx),
            "ST_Y" => Self::eval_st_y(args, batch, row_idx),
            "ST_GEOMETRYTYPE" => Self::eval_st_geometrytype(args, batch, row_idx),
            "ST_ISEMPTY" => Self::eval_st_isempty(args, batch, row_idx),
            "ST_ISCLOSED" => Self::eval_st_isclosed(args, batch, row_idx),
            "ST_ISCOLLECTION" => Self::eval_st_iscollection(args, batch, row_idx),
            "ST_DIMENSION" => Self::eval_st_dimension(args, batch, row_idx),
            "ST_NUMPOINTS" | "ST_NPOINTS" => Self::eval_st_numpoints(args, batch, row_idx),
            "ST_POINTN" => Self::eval_st_pointn(args, batch, row_idx),
            "ST_STARTPOINT" => Self::eval_st_startpoint(args, batch, row_idx),
            "ST_ENDPOINT" => Self::eval_st_endpoint(args, batch, row_idx),
            "ST_MAKELINE" => Self::eval_st_makeline(args, batch, row_idx),
            "ST_MAKEPOLYGON" => Self::eval_st_makepolygon(args, batch, row_idx),
            "ST_DISTANCE" => Self::eval_st_distance(args, batch, row_idx),
            "ST_LENGTH" => Self::eval_st_length(args, batch, row_idx),
            "ST_AREA" => Self::eval_st_area(args, batch, row_idx),
            "ST_PERIMETER" => Self::eval_st_perimeter(args, batch, row_idx),
            "ST_MAXDISTANCE" => Self::eval_st_maxdistance(args, batch, row_idx),
            "ST_AZIMUTH" => Self::eval_st_azimuth(args, batch, row_idx),
            "ST_CENTROID" => Self::eval_st_centroid(args, batch, row_idx),
            "ST_CONTAINS" => Self::eval_st_contains(args, batch, row_idx),
            "ST_COVERS" => Self::eval_st_covers(args, batch, row_idx),
            "ST_COVEREDBY" => Self::eval_st_coveredby(args, batch, row_idx),
            "ST_DISJOINT" => Self::eval_st_disjoint(args, batch, row_idx),
            "ST_DWITHIN" => Self::eval_st_dwithin(args, batch, row_idx),
            "ST_EQUALS" => Self::eval_st_equals(args, batch, row_idx),
            "ST_INTERSECTS" => Self::eval_st_intersects(args, batch, row_idx),
            "ST_TOUCHES" => Self::eval_st_touches(args, batch, row_idx),
            "ST_WITHIN" => Self::eval_st_within(args, batch, row_idx),
            "ST_BOUNDARY" => Self::eval_st_boundary(args, batch, row_idx),
            "ST_BUFFER" => Self::eval_st_buffer(args, batch, row_idx),
            "ST_BUFFERWITHTOLERANCE" => Self::eval_st_bufferwithtolerance(args, batch, row_idx),
            "ST_CLOSESTPOINT" => Self::eval_st_closestpoint(args, batch, row_idx),
            "ST_CONVEXHULL" => Self::eval_st_convexhull(args, batch, row_idx),
            "ST_DIFFERENCE" => Self::eval_st_difference(args, batch, row_idx),
            "ST_INTERSECTION" => Self::eval_st_intersection(args, batch, row_idx),
            "ST_SIMPLIFY" => Self::eval_st_simplify(args, batch, row_idx),
            "ST_SNAPTOGRID" => Self::eval_st_snaptogrid(args, batch, row_idx),
            "ST_UNION" => Self::eval_st_union(args, batch, row_idx),
            "ST_BOUNDINGBOX" => Self::eval_st_boundingbox(args, batch, row_idx),
            "ST_GEOHASH" => Self::eval_st_geohash(args, batch, row_idx),
            "ST_GEOGPOINTFROMGEOHASH" => Self::eval_st_geogpointfromgeohash(args, batch, row_idx),
            _ => Err(Error::invalid_query(format!(
                "Unknown geography function: {}",
                name
            ))),
        }
    }

    fn parse_geography(value: &Value) -> Result<Geometry> {
        if value.is_null() {
            return Ok(Geometry::Empty);
        }
        let wkt = value
            .as_geography()
            .ok_or_else(|| Error::type_mismatch("GEOGRAPHY", &value.data_type().to_string()))?;
        geography::parse_wkt(wkt)
    }

    fn eval_st_geogpoint(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_GEOGPOINT requires exactly 2 arguments (longitude, latitude)".to_string(),
            ));
        }
        let lon_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let lat_val = Self::evaluate_expr(&args[1], batch, row_idx)?;

        if lon_val.is_null() || lat_val.is_null() {
            return Ok(Value::null());
        }

        let lon = lon_val
            .as_f64()
            .or_else(|| lon_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &lon_val.data_type().to_string()))?;
        let lat = lat_val
            .as_f64()
            .or_else(|| lat_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &lat_val.data_type().to_string()))?;

        let geom = Geometry::Point { lon, lat };
        geom.validate()?;
        Ok(Value::geography(geom.to_wkt()))
    }

    fn eval_st_geogfromtext(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_GEOGFROMTEXT requires at least 1 argument".to_string(),
            ));
        }
        let wkt_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if wkt_val.is_null() {
            return Ok(Value::null());
        }
        let wkt = wkt_val
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", &wkt_val.data_type().to_string()))?;
        let geom = geography::parse_wkt(wkt)?;
        Ok(Value::geography(geom.to_wkt()))
    }

    fn eval_st_geogfromgeojson(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_GEOGFROMGEOJSON requires at least 1 argument".to_string(),
            ));
        }
        let json_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if json_val.is_null() {
            return Ok(Value::null());
        }
        let json = json_val
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", &json_val.data_type().to_string()))?;
        let geom = geography::parse_geojson(json)?;
        Ok(Value::geography(geom.to_wkt()))
    }

    fn eval_st_astext(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ASTEXT requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let wkt = geog_val
            .as_geography()
            .ok_or_else(|| Error::type_mismatch("GEOGRAPHY", &geog_val.data_type().to_string()))?;
        Ok(Value::string(wkt.to_string()))
    }

    fn eval_st_asgeojson(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ASGEOJSON requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        Ok(Value::string(geom.to_geojson()))
    }

    fn eval_st_asbinary(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ASBINARY requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let wkb = geom_to_wkb(&geom);
        Ok(Value::bytes(wkb))
    }

    fn eval_st_x(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_X requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        match geom {
            Geometry::Point { lon, .. } => Ok(Value::float64(lon)),
            _ => Err(Error::invalid_query(
                "ST_X requires a POINT geometry".to_string(),
            )),
        }
    }

    fn eval_st_y(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_Y requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        match geom {
            Geometry::Point { lat, .. } => Ok(Value::float64(lat)),
            _ => Err(Error::invalid_query(
                "ST_Y requires a POINT geometry".to_string(),
            )),
        }
    }

    fn eval_st_geometrytype(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_GEOMETRYTYPE requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let type_name = match geom {
            Geometry::Point { .. } => "Point",
            Geometry::MultiPoint { .. } => "MultiPoint",
            Geometry::LineString { .. } => "LineString",
            Geometry::Polygon { .. } => "Polygon",
            Geometry::Empty => "GeometryCollection",
        };
        Ok(Value::string(type_name.to_string()))
    }

    fn eval_st_isempty(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ISEMPTY requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        Ok(Value::bool_val(matches!(geom, Geometry::Empty)))
    }

    fn eval_st_isclosed(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ISCLOSED requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let is_closed = match &geom {
            Geometry::Point { .. } => true,
            Geometry::MultiPoint { .. } => true,
            Geometry::LineString { points } => points.len() >= 2 && points.first() == points.last(),
            Geometry::Polygon { .. } => true,
            Geometry::Empty => true,
        };
        Ok(Value::bool_val(is_closed))
    }

    fn eval_st_iscollection(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ISCOLLECTION requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        Ok(Value::bool_val(false))
    }

    fn eval_st_dimension(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_DIMENSION requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        Ok(Value::int64(geom.dimension() as i64))
    }

    fn eval_st_numpoints(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_NUMPOINTS requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let count = match &geom {
            Geometry::Point { .. } => 1,
            Geometry::MultiPoint { points } => points.len(),
            Geometry::LineString { points } => points.len(),
            Geometry::Polygon { rings } => rings.iter().map(|r| r.len()).sum(),
            Geometry::Empty => 0,
        };
        Ok(Value::int64(count as i64))
    }

    fn eval_st_pointn(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_POINTN requires exactly 2 arguments".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let n_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog_val.is_null() || n_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let n = n_val
            .as_i64()
            .ok_or_else(|| Error::type_mismatch("INT64", &n_val.data_type().to_string()))?
            as usize;

        match &geom {
            Geometry::LineString { points } => {
                if n == 0 || n > points.len() {
                    return Ok(Value::null());
                }
                let (lon, lat) = points[n - 1];
                let point = Geometry::Point { lon, lat };
                Ok(Value::geography(point.to_wkt()))
            }
            _ => Err(Error::invalid_query(
                "ST_POINTN requires a LINESTRING geometry".to_string(),
            )),
        }
    }

    fn eval_st_startpoint(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_STARTPOINT requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        match &geom {
            Geometry::LineString { points } => {
                if points.is_empty() {
                    return Ok(Value::null());
                }
                let (lon, lat) = points[0];
                let point = Geometry::Point { lon, lat };
                Ok(Value::geography(point.to_wkt()))
            }
            _ => Err(Error::invalid_query(
                "ST_STARTPOINT requires a LINESTRING geometry".to_string(),
            )),
        }
    }

    fn eval_st_endpoint(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_ENDPOINT requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        match &geom {
            Geometry::LineString { points } => {
                if points.is_empty() {
                    return Ok(Value::null());
                }
                let (lon, lat) = points[points.len() - 1];
                let point = Geometry::Point { lon, lat };
                Ok(Value::geography(point.to_wkt()))
            }
            _ => Err(Error::invalid_query(
                "ST_ENDPOINT requires a LINESTRING geometry".to_string(),
            )),
        }
    }

    fn eval_st_makeline(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_MAKELINE requires at least 1 argument".to_string(),
            ));
        }

        let mut points = Vec::new();

        for arg in args {
            let val = Self::evaluate_expr(arg, batch, row_idx)?;
            if val.is_null() {
                return Ok(Value::null());
            }

            if let Some(arr) = val.as_array() {
                for elem in arr {
                    let geom = Self::parse_geography(elem)?;
                    match geom {
                        Geometry::Point { lon, lat } => points.push((lon, lat)),
                        _ => {
                            return Err(Error::invalid_query(
                                "ST_MAKELINE array elements must be POINT geometries".to_string(),
                            ));
                        }
                    }
                }
            } else {
                let geom = Self::parse_geography(&val)?;
                match geom {
                    Geometry::Point { lon, lat } => points.push((lon, lat)),
                    _ => {
                        return Err(Error::invalid_query(
                            "ST_MAKELINE requires POINT geometries".to_string(),
                        ));
                    }
                }
            }
        }

        let line = geography::make_line(points)?;
        Ok(Value::geography(line.to_wkt()))
    }

    fn eval_st_makepolygon(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_MAKEPOLYGON requires at least 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let polygon = geography::make_polygon(&geom)?;
        Ok(Value::geography(polygon.to_wkt()))
    }

    fn eval_st_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_DISTANCE requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let distance = geom1.distance(&geom2)?;
        Ok(Value::float64(distance))
    }

    fn eval_st_length(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_LENGTH requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let length = geom.length()?;
        Ok(Value::float64(length))
    }

    fn eval_st_area(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_AREA requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let area = geom.area()?;
        Ok(Value::float64(area))
    }

    fn eval_st_perimeter(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_PERIMETER requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let perimeter = geom.perimeter()?;
        Ok(Value::float64(perimeter))
    }

    fn eval_st_maxdistance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_MAXDISTANCE requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let max_dist = max_distance(&geom1, &geom2);
        Ok(Value::float64(max_dist))
    }

    fn eval_st_azimuth(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_AZIMUTH requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;

        let (lon1, lat1) = match geom1 {
            Geometry::Point { lon, lat } => (lon, lat),
            _ => {
                return Err(Error::invalid_query(
                    "ST_AZIMUTH requires POINT geometries".to_string(),
                ));
            }
        };
        let (lon2, lat2) = match geom2 {
            Geometry::Point { lon, lat } => (lon, lat),
            _ => {
                return Err(Error::invalid_query(
                    "ST_AZIMUTH requires POINT geometries".to_string(),
                ));
            }
        };

        let azimuth = compute_azimuth(lon1, lat1, lon2, lat2);
        Ok(Value::float64(azimuth))
    }

    fn eval_st_centroid(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_CENTROID requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let centroid = compute_centroid(&geom)?;
        Ok(Value::geography(centroid.to_wkt()))
    }

    fn eval_st_contains(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_CONTAINS requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let contains = geom1.contains(&geom2)?;
        Ok(Value::bool_val(contains))
    }

    fn eval_st_covers(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_COVERS requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let covers = geom1.contains(&geom2)?;
        Ok(Value::bool_val(covers))
    }

    fn eval_st_coveredby(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_COVEREDBY requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let covered_by = geom2.contains(&geom1)?;
        Ok(Value::bool_val(covered_by))
    }

    fn eval_st_disjoint(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_DISJOINT requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let disjoint = geom1.disjoint(&geom2)?;
        Ok(Value::bool_val(disjoint))
    }

    fn eval_st_dwithin(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 3 {
            return Err(Error::invalid_query(
                "ST_DWITHIN requires exactly 3 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let dist_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() || dist_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let max_dist = dist_val
            .as_f64()
            .or_else(|| dist_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &dist_val.data_type().to_string()))?;
        let distance = geom1.distance(&geom2)?;
        Ok(Value::bool_val(distance <= max_dist))
    }

    fn eval_st_equals(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_EQUALS requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        Ok(Value::bool_val(geom1 == geom2))
    }

    fn eval_st_intersects(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_INTERSECTS requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let intersects = geom1.intersects(&geom2)?;
        Ok(Value::bool_val(intersects))
    }

    fn eval_st_touches(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_TOUCHES requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let touches = geometries_touch(&geom1, &geom2);
        Ok(Value::bool_val(touches))
    }

    fn eval_st_within(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_WITHIN requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let within = geom2.contains(&geom1)?;
        Ok(Value::bool_val(within))
    }

    fn eval_st_boundary(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_BOUNDARY requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let boundary = compute_boundary(&geom);
        Ok(Value::geography(boundary.to_wkt()))
    }

    fn eval_st_buffer(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 2 {
            return Err(Error::invalid_query(
                "ST_BUFFER requires at least 2 arguments".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let dist_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog_val.is_null() || dist_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let distance = dist_val
            .as_f64()
            .or_else(|| dist_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &dist_val.data_type().to_string()))?;
        let buffer = compute_buffer(&geom, distance, 32);
        Ok(Value::geography(buffer.to_wkt()))
    }

    fn eval_st_bufferwithtolerance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() < 3 {
            return Err(Error::invalid_query(
                "ST_BUFFERWITHTOLERANCE requires at least 3 arguments".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let dist_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        let tol_val = Self::evaluate_expr(&args[2], batch, row_idx)?;
        if geog_val.is_null() || dist_val.is_null() || tol_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let distance = dist_val
            .as_f64()
            .or_else(|| dist_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &dist_val.data_type().to_string()))?;
        let tolerance = tol_val
            .as_f64()
            .or_else(|| tol_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &tol_val.data_type().to_string()))?;
        let segments = (std::f64::consts::PI / tolerance.to_radians()).ceil() as usize;
        let segments = segments.clamp(4, 360);
        let buffer = compute_buffer(&geom, distance, segments);
        Ok(Value::geography(buffer.to_wkt()))
    }

    fn eval_st_closestpoint(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_CLOSESTPOINT requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let closest = find_closest_point(&geom1, &geom2);
        Ok(Value::geography(closest.to_wkt()))
    }

    fn eval_st_convexhull(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_CONVEXHULL requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let hull = compute_convex_hull(&geom);
        Ok(Value::geography(hull.to_wkt()))
    }

    fn eval_st_difference(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_DIFFERENCE requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let _geom2 = Self::parse_geography(&geog2_val)?;
        Ok(Value::geography(geom1.to_wkt()))
    }

    fn eval_st_intersection(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_INTERSECTION requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let intersection = compute_intersection(&geom1, &geom2);
        Ok(Value::geography(intersection.to_wkt()))
    }

    fn eval_st_simplify(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_SIMPLIFY requires exactly 2 arguments".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let tol_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog_val.is_null() || tol_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let _tolerance = tol_val
            .as_f64()
            .or_else(|| tol_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &tol_val.data_type().to_string()))?;
        Ok(Value::geography(geom.to_wkt()))
    }

    fn eval_st_snaptogrid(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_SNAPTOGRID requires exactly 2 arguments".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let grid_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog_val.is_null() || grid_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let grid_size = grid_val
            .as_f64()
            .or_else(|| grid_val.as_i64().map(|i| i as f64))
            .ok_or_else(|| Error::type_mismatch("FLOAT64", &grid_val.data_type().to_string()))?;
        let snapped = snap_to_grid(&geom, grid_size);
        Ok(Value::geography(snapped.to_wkt()))
    }

    fn eval_st_union(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(Error::invalid_query(
                "ST_UNION requires exactly 2 arguments".to_string(),
            ));
        }
        let geog1_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let geog2_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
        if geog1_val.is_null() || geog2_val.is_null() {
            return Ok(Value::null());
        }
        let geom1 = Self::parse_geography(&geog1_val)?;
        let geom2 = Self::parse_geography(&geog2_val)?;
        let union = compute_union(&geom1, &geom2);
        Ok(Value::geography(union.to_wkt()))
    }

    fn eval_st_boundingbox(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_BOUNDINGBOX requires exactly 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let bbox = compute_bounding_box(&geom);
        Ok(Value::geography(bbox.to_wkt()))
    }

    fn eval_st_geohash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_GEOHASH requires at least 1 argument".to_string(),
            ));
        }
        let geog_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if geog_val.is_null() {
            return Ok(Value::null());
        }
        let geom = Self::parse_geography(&geog_val)?;
        let precision = if args.len() > 1 {
            let prec_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
            prec_val.as_i64().unwrap_or(12) as usize
        } else {
            12
        };

        match geom {
            Geometry::Point { lon, lat } => {
                let hash = encode_geohash(lon, lat, precision);
                Ok(Value::string(hash))
            }
            _ => Err(Error::invalid_query(
                "ST_GEOHASH requires a POINT geometry".to_string(),
            )),
        }
    }

    fn eval_st_geogpointfromgeohash(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.is_empty() {
            return Err(Error::invalid_query(
                "ST_GEOGPOINTFROMGEOHASH requires exactly 1 argument".to_string(),
            ));
        }
        let hash_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        if hash_val.is_null() {
            return Ok(Value::null());
        }
        let hash = hash_val
            .as_str()
            .ok_or_else(|| Error::type_mismatch("STRING", &hash_val.data_type().to_string()))?;
        let (lon, lat) = decode_geohash(hash)?;
        let point = Geometry::Point { lon, lat };
        Ok(Value::geography(point.to_wkt()))
    }
}

const EARTH_RADIUS_M: f64 = 6_371_008.8;

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

fn max_distance(geom1: &Geometry, geom2: &Geometry) -> f64 {
    let points1 = collect_points(geom1);
    let points2 = collect_points(geom2);

    let mut max_dist = 0.0;
    for (lon1, lat1) in &points1 {
        for (lon2, lat2) in &points2 {
            let dist = haversine_distance(*lat1, *lon1, *lat2, *lon2);
            if dist > max_dist {
                max_dist = dist;
            }
        }
    }
    max_dist
}

fn collect_points(geom: &Geometry) -> Vec<(f64, f64)> {
    match geom {
        Geometry::Point { lon, lat } => vec![(*lon, *lat)],
        Geometry::MultiPoint { points } => points.clone(),
        Geometry::LineString { points } => points.clone(),
        Geometry::Polygon { rings } => rings.iter().flat_map(|r| r.iter().copied()).collect(),
        Geometry::Empty => vec![],
    }
}

fn compute_azimuth(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let d_lon = (lon2 - lon1).to_radians();
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();

    let x = d_lon.sin() * lat2_rad.cos();
    let y = lat1_rad.cos() * lat2_rad.sin() - lat1_rad.sin() * lat2_rad.cos() * d_lon.cos();

    x.atan2(y)
}

fn compute_centroid(geom: &Geometry) -> Result<Geometry> {
    match geom {
        Geometry::Point { lon, lat } => Ok(Geometry::Point {
            lon: *lon,
            lat: *lat,
        }),
        Geometry::MultiPoint { points } => {
            if points.is_empty() {
                return Ok(Geometry::Empty);
            }
            let sum: (f64, f64) = points
                .iter()
                .fold((0.0, 0.0), |acc, p| (acc.0 + p.0, acc.1 + p.1));
            let n = points.len() as f64;
            Ok(Geometry::Point {
                lon: sum.0 / n,
                lat: sum.1 / n,
            })
        }
        Geometry::LineString { points } => {
            if points.is_empty() {
                return Ok(Geometry::Empty);
            }
            let sum: (f64, f64) = points
                .iter()
                .fold((0.0, 0.0), |acc, p| (acc.0 + p.0, acc.1 + p.1));
            let n = points.len() as f64;
            Ok(Geometry::Point {
                lon: sum.0 / n,
                lat: sum.1 / n,
            })
        }
        Geometry::Polygon { rings } => {
            if rings.is_empty() || rings[0].is_empty() {
                return Ok(Geometry::Empty);
            }
            let ring = &rings[0];
            let sum: (f64, f64) = ring
                .iter()
                .fold((0.0, 0.0), |acc, p| (acc.0 + p.0, acc.1 + p.1));
            let n = ring.len() as f64;
            Ok(Geometry::Point {
                lon: sum.0 / n,
                lat: sum.1 / n,
            })
        }
        Geometry::Empty => Ok(Geometry::Empty),
    }
}

fn geometries_touch(geom1: &Geometry, geom2: &Geometry) -> bool {
    match (geom1, geom2) {
        (Geometry::Polygon { rings: rings1 }, Geometry::Polygon { rings: rings2 }) => {
            if rings1.is_empty() || rings2.is_empty() {
                return false;
            }
            let ring1 = &rings1[0];
            let ring2 = &rings2[0];

            for p1 in ring1 {
                for p2 in ring2 {
                    if (p1.0 - p2.0).abs() < 1e-10 && (p1.1 - p2.1).abs() < 1e-10 {
                        return true;
                    }
                }
            }
            false
        }
        _ => false,
    }
}

fn compute_boundary(geom: &Geometry) -> Geometry {
    match geom {
        Geometry::Point { .. } => Geometry::Empty,
        Geometry::MultiPoint { .. } => Geometry::Empty,
        Geometry::LineString { points } => {
            if points.is_empty() {
                return Geometry::Empty;
            }
            if points.first() == points.last() {
                return Geometry::Empty;
            }
            Geometry::Point {
                lon: points[0].0,
                lat: points[0].1,
            }
        }
        Geometry::Polygon { rings } => {
            if rings.is_empty() {
                return Geometry::Empty;
            }
            Geometry::LineString {
                points: rings[0].clone(),
            }
        }
        Geometry::Empty => Geometry::Empty,
    }
}

fn compute_buffer(geom: &Geometry, distance: f64, segments: usize) -> Geometry {
    match geom {
        Geometry::Point { lon, lat } => {
            let mut ring = Vec::with_capacity(segments + 1);
            let distance_deg = distance / 111_195.0;

            for i in 0..segments {
                let angle = 2.0 * std::f64::consts::PI * (i as f64) / (segments as f64);
                let dx = distance_deg * angle.cos();
                let dy = distance_deg * angle.sin();
                ring.push((lon + dx, lat + dy));
            }
            ring.push(ring[0]);

            Geometry::Polygon { rings: vec![ring] }
        }
        _ => geom.clone(),
    }
}

fn find_closest_point(geom1: &Geometry, geom2: &Geometry) -> Geometry {
    let points1 = collect_points(geom1);
    let (lon2, lat2) = match geom2 {
        Geometry::Point { lon, lat } => (*lon, *lat),
        _ => {
            let centroid = compute_centroid(geom2).unwrap_or(Geometry::Empty);
            match centroid {
                Geometry::Point { lon, lat } => (lon, lat),
                _ => return Geometry::Empty,
            }
        }
    };

    let mut closest = None;
    let mut min_dist = f64::MAX;

    for (lon, lat) in points1 {
        let dist = haversine_distance(lat, lon, lat2, lon2);
        if dist < min_dist {
            min_dist = dist;
            closest = Some((lon, lat));
        }
    }

    match closest {
        Some((lon, lat)) => Geometry::Point { lon, lat },
        None => Geometry::Empty,
    }
}

fn compute_convex_hull(geom: &Geometry) -> Geometry {
    let points = collect_points(geom);
    if points.len() < 3 {
        return match points.len() {
            0 => Geometry::Empty,
            1 => Geometry::Point {
                lon: points[0].0,
                lat: points[0].1,
            },
            2 => Geometry::LineString { points },
            _ => unreachable!(),
        };
    }

    let mut sorted = points.clone();
    sorted.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal))
    });

    fn cross(o: (f64, f64), a: (f64, f64), b: (f64, f64)) -> f64 {
        (a.0 - o.0) * (b.1 - o.1) - (a.1 - o.1) * (b.0 - o.0)
    }

    let mut lower = Vec::new();
    for p in &sorted {
        while lower.len() >= 2 && cross(lower[lower.len() - 2], lower[lower.len() - 1], *p) <= 0.0 {
            lower.pop();
        }
        lower.push(*p);
    }

    let mut upper = Vec::new();
    for p in sorted.iter().rev() {
        while upper.len() >= 2 && cross(upper[upper.len() - 2], upper[upper.len() - 1], *p) <= 0.0 {
            upper.pop();
        }
        upper.push(*p);
    }

    lower.pop();
    upper.pop();
    lower.append(&mut upper);

    if !lower.is_empty() {
        lower.push(lower[0]);
    }

    Geometry::Polygon { rings: vec![lower] }
}

fn compute_intersection(geom1: &Geometry, geom2: &Geometry) -> Geometry {
    match (geom1, geom2) {
        (Geometry::Polygon { rings: rings1 }, Geometry::Polygon { rings: rings2 }) => {
            if rings1.is_empty() || rings2.is_empty() {
                return Geometry::Empty;
            }
            Geometry::Polygon {
                rings: vec![rings1[0].clone()],
            }
        }
        _ => geom1.clone(),
    }
}

fn snap_to_grid(geom: &Geometry, grid_size: f64) -> Geometry {
    match geom {
        Geometry::Point { lon, lat } => {
            let snapped_lon = (lon / grid_size).round() * grid_size;
            let snapped_lat = (lat / grid_size).round() * grid_size;
            Geometry::Point {
                lon: snapped_lon,
                lat: snapped_lat,
            }
        }
        Geometry::MultiPoint { points } => {
            let snapped: Vec<(f64, f64)> = points
                .iter()
                .map(|(lon, lat)| {
                    (
                        (lon / grid_size).round() * grid_size,
                        (lat / grid_size).round() * grid_size,
                    )
                })
                .collect();
            Geometry::MultiPoint { points: snapped }
        }
        Geometry::LineString { points } => {
            let snapped: Vec<(f64, f64)> = points
                .iter()
                .map(|(lon, lat)| {
                    (
                        (lon / grid_size).round() * grid_size,
                        (lat / grid_size).round() * grid_size,
                    )
                })
                .collect();
            Geometry::LineString { points: snapped }
        }
        Geometry::Polygon { rings } => {
            let snapped_rings: Vec<Vec<(f64, f64)>> = rings
                .iter()
                .map(|ring| {
                    ring.iter()
                        .map(|(lon, lat)| {
                            (
                                (lon / grid_size).round() * grid_size,
                                (lat / grid_size).round() * grid_size,
                            )
                        })
                        .collect()
                })
                .collect();
            Geometry::Polygon {
                rings: snapped_rings,
            }
        }
        Geometry::Empty => Geometry::Empty,
    }
}

fn compute_union(geom1: &Geometry, geom2: &Geometry) -> Geometry {
    match (geom1, geom2) {
        (Geometry::Polygon { rings: rings1 }, Geometry::Polygon { .. }) => {
            if rings1.is_empty() {
                return geom2.clone();
            }
            geom1.clone()
        }
        _ => geom1.clone(),
    }
}

fn compute_bounding_box(geom: &Geometry) -> Geometry {
    let points = collect_points(geom);
    if points.is_empty() {
        return Geometry::Empty;
    }

    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;

    for (lon, lat) in &points {
        min_lon = min_lon.min(*lon);
        max_lon = max_lon.max(*lon);
        min_lat = min_lat.min(*lat);
        max_lat = max_lat.max(*lat);
    }

    let ring = vec![
        (min_lon, min_lat),
        (min_lon, max_lat),
        (max_lon, max_lat),
        (max_lon, min_lat),
        (min_lon, min_lat),
    ];

    Geometry::Polygon { rings: vec![ring] }
}

fn geom_to_wkb(geom: &Geometry) -> Vec<u8> {
    let mut wkb = Vec::new();
    wkb.push(1u8);

    match geom {
        Geometry::Point { lon, lat } => {
            wkb.extend_from_slice(&1u32.to_le_bytes());
            wkb.extend_from_slice(&lon.to_le_bytes());
            wkb.extend_from_slice(&lat.to_le_bytes());
        }
        Geometry::MultiPoint { points } => {
            wkb.extend_from_slice(&4u32.to_le_bytes());
            wkb.extend_from_slice(&(points.len() as u32).to_le_bytes());
            for (lon, lat) in points {
                wkb.push(1u8);
                wkb.extend_from_slice(&1u32.to_le_bytes());
                wkb.extend_from_slice(&lon.to_le_bytes());
                wkb.extend_from_slice(&lat.to_le_bytes());
            }
        }
        Geometry::LineString { points } => {
            wkb.extend_from_slice(&2u32.to_le_bytes());
            wkb.extend_from_slice(&(points.len() as u32).to_le_bytes());
            for (lon, lat) in points {
                wkb.extend_from_slice(&lon.to_le_bytes());
                wkb.extend_from_slice(&lat.to_le_bytes());
            }
        }
        Geometry::Polygon { rings } => {
            wkb.extend_from_slice(&3u32.to_le_bytes());
            wkb.extend_from_slice(&(rings.len() as u32).to_le_bytes());
            for ring in rings {
                wkb.extend_from_slice(&(ring.len() as u32).to_le_bytes());
                for (lon, lat) in ring {
                    wkb.extend_from_slice(&lon.to_le_bytes());
                    wkb.extend_from_slice(&lat.to_le_bytes());
                }
            }
        }
        Geometry::Empty => {
            wkb.extend_from_slice(&7u32.to_le_bytes());
            wkb.extend_from_slice(&0u32.to_le_bytes());
        }
    }

    wkb
}

const GEOHASH_CHARS: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";

fn encode_geohash(lon: f64, lat: f64, precision: usize) -> String {
    let mut min_lon = -180.0;
    let mut max_lon = 180.0;
    let mut min_lat = -90.0;
    let mut max_lat = 90.0;

    let mut hash = String::with_capacity(precision);
    let mut bit = 0u8;
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
            hash.push(GEOHASH_CHARS[ch as usize] as char);
            ch = 0;
            bit = 0;
        }
    }

    hash
}

fn decode_geohash(hash: &str) -> Result<(f64, f64)> {
    let mut min_lon = -180.0;
    let mut max_lon = 180.0;
    let mut min_lat = -90.0;
    let mut max_lat = 90.0;

    let mut is_lon = true;

    for c in hash.chars() {
        let idx = GEOHASH_CHARS
            .iter()
            .position(|&x| x == c as u8)
            .ok_or_else(|| Error::invalid_query(format!("Invalid geohash character: {}", c)))?;

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

    Ok(((min_lon + max_lon) / 2.0, (min_lat + max_lat) / 2.0))
}

impl ProjectionWithExprExec {
    pub(super) fn evaluate_custom_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "GREATCIRCLEDISTANCE" => {
                if args.len() != 4 {
                    return Err(Error::invalid_query(
                        "greatCircleDistance requires 4 arguments (lat1, lon1, lat2, lon2)"
                            .to_string(),
                    ));
                }
                let lat1 = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lon1 = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lat2 = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lon2 = Self::evaluate_expr(&args[3], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                yachtsql_functions::geography::great_circle_distance(lat1, lon1, lat2, lon2)
            }
            "GEODISTANCE" => {
                if args.len() != 4 {
                    return Err(Error::invalid_query(
                        "geoDistance requires 4 arguments (lat1, lon1, lat2, lon2)".to_string(),
                    ));
                }
                let lat1 = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lon1 = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lat2 = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lon2 = Self::evaluate_expr(&args[3], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                yachtsql_functions::geography::geo_distance(lat1, lon1, lat2, lon2)
            }
            "POINTINELLIPSES" => {
                if args.len() < 6 || (args.len() - 2) % 4 != 0 {
                    return Err(Error::invalid_query(
                        "pointInEllipses requires (x, y, x_center, y_center, a, b, ...)"
                            .to_string(),
                    ));
                }
                let x = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let y = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let mut ellipses = Vec::new();
                for arg in args.iter().skip(2) {
                    let v = Self::evaluate_expr(arg, batch, row_idx)?
                        .as_f64()
                        .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                    ellipses.push(v);
                }
                yachtsql_functions::geography::point_in_ellipses_fn(x, y, &ellipses)
            }
            "POINTINPOLYGON" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "pointInPolygon requires 2 arguments (point, polygon)".to_string(),
                    ));
                }
                let point = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let polygon = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let (x, y) = if let Some(gp) = point.as_geo_point() {
                    (gp.x, gp.y)
                } else if let Some(pt) = point.as_point() {
                    (pt.x, pt.y)
                } else if let Some(s) = point.as_struct() {
                    let x = s.values().next().and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let y = s.values().nth(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
                    (x, y)
                } else {
                    return Err(Error::type_mismatch(
                        "Point or Tuple",
                        &point.data_type().to_string(),
                    ));
                };
                let polygon_points: Vec<(f64, f64)> =
                    if let Some(geo_poly) = polygon.as_geo_polygon() {
                        if geo_poly.is_empty() || geo_poly[0].is_empty() {
                            vec![]
                        } else {
                            geo_poly[0].iter().map(|p| (p.x, p.y)).collect()
                        }
                    } else if let Some(arr) = polygon.as_array() {
                        arr.iter()
                            .filter_map(|v| {
                                if let Some(gp) = v.as_geo_point() {
                                    Some((gp.x, gp.y))
                                } else if let Some(s) = v.as_struct() {
                                    let px = s.values().next().and_then(|v| v.as_f64())?;
                                    let py = s.values().nth(1).and_then(|v| v.as_f64())?;
                                    Some((px, py))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    } else {
                        return Err(Error::type_mismatch(
                            "Polygon or Array",
                            &polygon.data_type().to_string(),
                        ));
                    };
                yachtsql_functions::geography::point_in_polygon_fn(x, y, &polygon_points)
            }
            "GEOHASHENCODE" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query(
                        "geohashEncode requires 2-3 arguments (lon, lat, [precision])".to_string(),
                    ));
                }
                let lon = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lat = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let precision = if args.len() == 3 {
                    Self::evaluate_expr(&args[2], batch, row_idx)?
                        .as_i64()
                        .unwrap_or(12) as u8
                } else {
                    12
                };
                yachtsql_functions::geography::geohash_encode(lat, lon, precision)
            }
            "GEOHASHDECODE" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "geohashDecode requires 1 argument (hash)".to_string(),
                    ));
                }
                let hash = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let hash_str = hash
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", &hash.data_type().to_string()))?;
                yachtsql_functions::geography::geohash_decode(hash_str)
            }
            "GEOHASHESINBOX" => {
                if args.len() != 5 {
                    return Err(Error::invalid_query("geohashesInBox requires 5 arguments (min_lon, min_lat, max_lon, max_lat, precision)".to_string()));
                }
                let min_lon = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let min_lat = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let max_lon = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let max_lat = Self::evaluate_expr(&args[3], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let precision = Self::evaluate_expr(&args[4], batch, row_idx)?
                    .as_i64()
                    .unwrap_or(4) as u8;
                yachtsql_functions::geography::geohashes_in_box(
                    min_lon, min_lat, max_lon, max_lat, precision,
                )
            }
            "H3ISVALID" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3IsValid requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_is_valid(h3_index)
            }
            "H3GETRESOLUTION" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3GetResolution requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_get_resolution(h3_index)
            }
            "H3EDGELENGTHM" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3EdgeLengthM requires 1 argument".to_string(),
                    ));
                }
                let resolution = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::h3_edge_length_m(resolution)
            }
            "H3EDGEANGLE" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3EdgeAngle requires 1 argument".to_string(),
                    ));
                }
                let resolution = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::h3_edge_angle(resolution)
            }
            "H3HEXAREAKM2" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3HexAreaKm2 requires 1 argument".to_string(),
                    ));
                }
                let resolution = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::h3_hex_area_km2(resolution)
            }
            "H3TOGEO" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3ToGeo requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_to_geo(h3_index)
            }
            "H3TOGEOBOUNDARY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3ToGeoBoundary requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_to_geo_boundary(h3_index)
            }
            "GEOTOH3" => {
                if args.len() != 3 {
                    return Err(Error::invalid_query(
                        "geoToH3 requires 3 arguments (lat, lon, resolution)".to_string(),
                    ));
                }
                let lat = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lon = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let resolution = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::geo_to_h3(lat, lon, resolution)
            }
            "H3KRING" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "h3kRing requires 2 arguments (h3_index, k)".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                let k = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::h3_k_ring(h3_index, k)
            }
            "H3GETBASECELL" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3GetBaseCell requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_get_base_cell(h3_index)
            }
            "H3ISPENTAGON" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3IsPentagon requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_is_pentagon(h3_index)
            }
            "H3ISRESCLASSIII" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3IsResClassIII requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_is_res_class_iii(h3_index)
            }
            "H3GETFACES" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3GetFaces requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_get_faces(h3_index)
            }
            "H3CELLAREAM2" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3CellAreaM2 requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_cell_area_m2(h3_index)
            }
            "H3CELLAREARADS2" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "h3CellAreaRads2 requires 1 argument".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_cell_area_rads2(h3_index)
            }
            "H3TOPARENT" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "h3ToParent requires 2 arguments (h3_index, resolution)".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                let resolution = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::h3_to_parent(h3_index, resolution)
            }
            "H3TOCHILDREN" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "h3ToChildren requires 2 arguments (h3_index, resolution)".to_string(),
                    ));
                }
                let h3_index = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                let resolution = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::h3_to_children(h3_index, resolution)
            }
            "H3DISTANCE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "h3Distance requires 2 arguments".to_string(),
                    ));
                }
                let h3_index1 = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                let h3_index2 = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_distance(h3_index1, h3_index2)
            }
            "H3LINE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "h3Line requires 2 arguments".to_string(),
                    ));
                }
                let h3_index1 = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                let h3_index2 = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::h3_line(h3_index1, h3_index2)
            }
            "S2CELLIDTOLONGLAT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "S2CellIdToLongLat requires 1 argument".to_string(),
                    ));
                }
                let s2_cell_id = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u64;
                yachtsql_functions::geography::s2_cell_id_to_long_lat(s2_cell_id)
            }
            "LONGLATTOS2CELLID" | "LONGLATTTOS2CELLID" => {
                if args.len() != 3 {
                    return Err(Error::invalid_query(
                        "longLatToS2CellId requires 3 arguments (lon, lat, level)".to_string(),
                    ));
                }
                let lon = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let lat = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let level = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::geography::long_lat_to_s2_cell_id(lon, lat, level)
            }
            "PROTOCOL" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "protocol requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::protocol(&url)
            }
            "DOMAIN" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "domain requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::domain(&url)
            }
            "DOMAINWITHOUTWWW" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "domainWithoutWWW requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::domain_without_www(&url)
            }
            "TOPLEVELDOMAIN" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "topLevelDomain requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::top_level_domain(&url)
            }
            "FIRSTSIGNIFICANTSUBDOMAIN" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "firstSignificantSubdomain requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::first_significant_subdomain(&url)
            }
            "PORT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("port requires 1 argument".to_string()));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::port(&url)
            }
            "PATH" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("path requires 1 argument".to_string()));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::path(&url)
            }
            "PATHFULL" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "pathFull requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::path_full(&url)
            }
            "QUERYSTRING" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "queryString requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::query_string(&url)
            }
            "FRAGMENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "fragment requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::fragment(&url)
            }
            "QUERYSTRINGANDFRAGMENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "queryStringAndFragment requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::query_string_and_fragment(&url)
            }
            "EXTRACTURLPARAMETER" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "extractURLParameter requires 2 arguments (url, name)".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                let name = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::extract_url_parameter(&url, &name)
            }
            "EXTRACTURLPARAMETERS" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "extractURLParameters requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::extract_url_parameters(&url)
            }
            "EXTRACTURLPARAMETERNAMES" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "extractURLParameterNames requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::extract_url_parameter_names(&url)
            }
            "URLHIERARCHY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "URLHierarchy requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::url_hierarchy(&url)
            }
            "URLPATHHIERARCHY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "URLPathHierarchy requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::url_path_hierarchy(&url)
            }
            "DECODEURLCOMPONENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "decodeURLComponent requires 1 argument".to_string(),
                    ));
                }
                let encoded = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::decode_url_component(&encoded)
            }
            "ENCODEURLCOMPONENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "encodeURLComponent requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::encode_url_component(&s)
            }
            "ENCODEURLFORMCOMPONENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "encodeURLFormComponent requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::encode_url_form_component(&s)
            }
            "DECODEURLFORMCOMPONENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "decodeURLFormComponent requires 1 argument".to_string(),
                    ));
                }
                let encoded = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::decode_url_form_component(&encoded)
            }
            "NETLOC" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "netloc requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::netloc(&url)
            }
            "CUTTOFIRSTSIGNIFICANTSUBDOMAIN" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "cutToFirstSignificantSubdomain requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::cut_to_first_significant_subdomain(&url)
            }
            "CUTWWW" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "cutWWW requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::cut_www(&url)
            }
            "CUTQUERYSTRING" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "cutQueryString requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::cut_query_string(&url)
            }
            "CUTFRAGMENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "cutFragment requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::cut_fragment(&url)
            }
            "CUTQUERYSTRINGANDFRAGMENT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "cutQueryStringAndFragment requires 1 argument".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::cut_query_string_and_fragment(&url)
            }
            "CUTURLPARAMETER" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "cutURLParameter requires 2 arguments (url, name)".to_string(),
                    ));
                }
                let url = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                let name = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::cut_url_parameter(&url, &name)
            }
            "HOSTNAME" => yachtsql_functions::misc::hostname(),
            "FQDN" => yachtsql_functions::misc::fqdn(),
            "VERSION" => yachtsql_functions::misc::version(),
            "UPTIME" => yachtsql_functions::misc::uptime(),
            "TIMEZONE" => yachtsql_functions::misc::timezone(),
            "CURRENTDATABASE" => yachtsql_functions::misc::current_database(),
            "CURRENTUSER" => yachtsql_functions::misc::current_user(),
            "ISCONSTANT" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isConstant requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::misc::is_constant(value)
            }
            "ISFINITE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isFinite requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                yachtsql_functions::misc::is_finite(value)
            }
            "ISINFINITE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isInfinite requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                yachtsql_functions::misc::is_infinite(value)
            }
            "ISNAN" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isNaN requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                yachtsql_functions::misc::is_nan(value)
            }
            "BAR" => {
                if args.len() < 4 {
                    return Err(Error::invalid_query("bar requires 4 arguments".to_string()));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let min = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let max = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let width = Self::evaluate_expr(&args[3], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::misc::bar(value, min, max, width)
            }
            "FORMATREADABLESIZE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "formatReadableSize requires 1 argument".to_string(),
                    ));
                }
                let bytes = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::misc::format_readable_size(bytes)
            }
            "FORMATREADABLEQUANTITY" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "formatReadableQuantity requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::misc::format_readable_quantity(value)
            }
            "FORMATREADABLETIMEDELTA" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "formatReadableTimeDelta requires 1 argument".to_string(),
                    ));
                }
                let seconds = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::misc::format_readable_time_delta(seconds)
            }
            "SLEEP" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "sleep requires 1 argument".to_string(),
                    ));
                }
                let seconds = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                yachtsql_functions::misc::sleep(seconds)
            }
            "THROWIF" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "throwIf requires 2 arguments".to_string(),
                    ));
                }
                let condition_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let condition = condition_val
                    .as_i64()
                    .map(|i| i != 0)
                    .unwrap_or_else(|| condition_val.as_bool().unwrap_or(false));
                let message = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .unwrap_or("")
                    .to_string();
                yachtsql_functions::misc::throw_if(condition, &message)
            }
            "MATERIALIZE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "materialize requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::misc::materialize(value)
            }
            "IGNORE" => {
                let values: Vec<Value> = args
                    .iter()
                    .map(|a| Self::evaluate_expr(a, batch, row_idx))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                yachtsql_functions::misc::ignore(values)
            }
            "IDENTITY" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "identity requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                yachtsql_functions::misc::identity(value)
            }
            "GETSETTING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "getSetting requires 1 argument".to_string(),
                    ));
                }
                let name = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::misc::get_setting(&name)
            }
            "TRANSFORM" => {
                if args.len() < 4 {
                    return Err(Error::invalid_query(
                        "transform requires 4 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let from_array = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let to_array = Self::evaluate_expr(&args[2], batch, row_idx)?;
                let default = Self::evaluate_expr(&args[3], batch, row_idx)?;
                let from_vec = from_array
                    .as_array()
                    .ok_or_else(|| Error::type_mismatch("ARRAY", "other"))?
                    .clone();
                let to_vec = to_array
                    .as_array()
                    .ok_or_else(|| Error::type_mismatch("ARRAY", "other"))?
                    .clone();
                yachtsql_functions::misc::transform(value, &from_vec, &to_vec, default)
            }
            "MODELEVALUATE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "modelEvaluate requires at least 1 argument".to_string(),
                    ));
                }
                let model_name = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                let values: Vec<Value> = args
                    .iter()
                    .skip(1)
                    .map(|a| Self::evaluate_expr(a, batch, row_idx))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                yachtsql_functions::misc::model_evaluate(&model_name, values)
            }
            "RUNNINGACCUMULATE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "runningAccumulate requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(value)
            }
            "HASCOLUMNINTABLE" => Ok(Value::bool_val(true)),
            "HEX" => {
                if args.is_empty() {
                    return Err(Error::invalid_query("hex requires 1 argument".to_string()));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if let Some(fs) = value.as_fixed_string() {
                    yachtsql_functions::encoding::hex_encode_bytes(&fs.data)
                } else if let Some(s) = value.as_str() {
                    yachtsql_functions::encoding::hex_encode(s)
                } else if let Some(b) = value.as_bytes() {
                    yachtsql_functions::encoding::hex_encode_bytes(b)
                } else {
                    Err(Error::type_mismatch("STRING or BYTES", "other"))
                }
            }
            "UNHEX" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "unhex requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let s = if let Some(fs) = value.as_fixed_string() {
                    fs.to_string_lossy()
                } else if let Some(str_val) = value.as_str() {
                    str_val.to_string()
                } else {
                    return Err(Error::type_mismatch("STRING", "other"));
                };
                yachtsql_functions::encoding::unhex(&s)
            }
            "BASE64ENCODE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "base64Encode requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::encoding::base64_encode(&s)
            }
            "BASE64DECODE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "base64Decode requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::encoding::base64_decode(&s)
            }
            "TRYBASE64DECODE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "tryBase64Decode requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::encoding::try_base64_decode(&s)
            }
            "BASE58ENCODE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "base58Encode requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::encoding::base58_encode(&s)
            }
            "BASE58DECODE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "base58Decode requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::encoding::base58_decode(&s)
            }
            "BIN" => {
                if args.is_empty() {
                    return Err(Error::invalid_query("bin requires 1 argument".to_string()));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bin(value)
            }
            "UNBIN" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "unbin requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::encoding::unbin(&s)
            }
            "BITSHIFTLEFT" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitShiftLeft requires 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let shift = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_shift_left(value, shift)
            }
            "BITSHIFTRIGHT" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitShiftRight requires 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let shift = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_shift_right(value, shift)
            }
            "BITAND" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitAnd requires 2 arguments".to_string(),
                    ));
                }
                let a = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let b = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_and(a, b)
            }
            "BITOR" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitOr requires 2 arguments".to_string(),
                    ));
                }
                let a = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let b = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_or(a, b)
            }
            "BITXOR" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitXor requires 2 arguments".to_string(),
                    ));
                }
                let a = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let b = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_xor(a, b)
            }
            "BITNOT" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "bitNot requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_not(value)
            }
            "BITCOUNT" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "bitCount requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_count(value)
            }
            "BITTEST" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitTest requires 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let pos = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_test(value, pos)
            }
            "BITTESTALL" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitTestAll requires at least 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let positions: Vec<i64> = args
                    .iter()
                    .skip(1)
                    .map(|a| {
                        Self::evaluate_expr(a, batch, row_idx)?
                            .as_i64()
                            .ok_or_else(|| Error::type_mismatch("INT64", "other"))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                yachtsql_functions::encoding::bit_test_all(value, &positions)
            }
            "BITTESTANY" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitTestAny requires at least 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let positions: Vec<i64> = args
                    .iter()
                    .skip(1)
                    .map(|a| {
                        Self::evaluate_expr(a, batch, row_idx)?
                            .as_i64()
                            .ok_or_else(|| Error::type_mismatch("INT64", "other"))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                yachtsql_functions::encoding::bit_test_any(value, &positions)
            }
            "BITROTATELEFT" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitRotateLeft requires 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u8;
                let shift = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_rotate_left(value, shift)
            }
            "BITROTATERIGHT" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitRotateRight requires 2 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u8;
                let shift = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_rotate_right(value, shift)
            }
            "BITHAMMINGDISTANCE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "bitHammingDistance requires 2 arguments".to_string(),
                    ));
                }
                let a = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let b = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_hamming_distance(a, b)
            }
            "BITSLICE" => {
                if args.len() < 3 {
                    return Err(Error::invalid_query(
                        "bitSlice requires 3 arguments".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                    as u8;
                let start = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let length = Self::evaluate_expr(&args[2], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_slice(value, start, length)
            }
            "BYTESWAP" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "byteSwap requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::byte_swap_16(value as u16)
            }
            "BITPOSITIONSTOARRAY" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "bitPositionsToArray requires 1 argument".to_string(),
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::encoding::bit_positions_to_array(value)
            }
            "CHAR" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "char requires at least 1 argument".to_string(),
                    ));
                }
                let codes: Vec<i64> = args
                    .iter()
                    .map(|a| {
                        Self::evaluate_expr(a, batch, row_idx)?
                            .as_i64()
                            .ok_or_else(|| Error::type_mismatch("INT64", "other"))
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                yachtsql_functions::encoding::char_fn(&codes)
            }
            "ISNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Value::bool_val(val.is_null()))
            }
            "ISNOTNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isNotNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Value::bool_val(!val.is_null()))
            }
            "ASSUMENOTNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "assumeNotNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    Err(Error::invalid_query(
                        "assumeNotNull: value is NULL".to_string(),
                    ))
                } else {
                    Ok(val)
                }
            }
            "TONULLABLE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toNullable requires 1 argument".to_string(),
                    ));
                }
                Self::evaluate_expr(&args[0], batch, row_idx)
            }
            "ISZEROORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isZeroOrNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::bool_val(true));
                }
                let is_zero = val.as_i64().map(|v| v == 0).unwrap_or(false)
                    || val.as_f64().map(|v| v == 0.0).unwrap_or(false);
                Ok(Value::bool_val(is_zero))
            }
            "IPV4NUMTOSTRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IPv4NumToString requires 1 argument".to_string(),
                    ));
                }
                let num = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::network::ipv4_num_to_string(num)
            }
            "IPV4STRINGTONUM" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IPv4StringToNum requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::ipv4_string_to_num(&s)
            }
            "IPV4NUMTOSTRINGCLASSC" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IPv4NumToStringClassC requires 1 argument".to_string(),
                    ));
                }
                let num = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::network::ipv4_num_to_string_class_c(num)
            }
            "IPV4TOIPV6" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IPv4ToIPv6 requires 1 argument".to_string(),
                    ));
                }
                let arg_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if let Some(ipv4) = arg_val.as_ipv4() {
                    Ok(Value::ipv6(ipv4.to_ipv6()))
                } else if let Some(num) = arg_val.as_i64() {
                    yachtsql_functions::network::ipv4_to_ipv6(num)
                } else {
                    Err(Error::type_mismatch(
                        "IPv4 or INT64",
                        &arg_val.data_type().to_string(),
                    ))
                }
            }
            "IPV6NUMTOSTRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IPv6NumToString requires 1 argument".to_string(),
                    ));
                }
                let bytes = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_bytes()
                    .ok_or_else(|| Error::type_mismatch("BYTES", "other"))?
                    .to_vec();
                yachtsql_functions::network::ipv6_num_to_string(&bytes)
            }
            "IPV6STRINGTONUM" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "IPv6StringToNum requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::ipv6_string_to_num(&s)
            }
            "TOIPV4" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toIPv4 requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::to_ipv4(&s)
            }
            "TOIPV6" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toIPv6 requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::to_ipv6(&s)
            }
            "TOIPV4ORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toIPv4OrNull requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::to_ipv4_or_null(&s)
            }
            "TOIPV6ORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toIPv6OrNull requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::to_ipv6_or_null(&s)
            }
            "ISIPV4STRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isIPv4String requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::is_ipv4_string(&s)
            }
            "ISIPV6STRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "isIPv6String requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::is_ipv6_string(&s)
            }
            "ISIPADDRESSINRANGE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "isIPAddressInRange requires 2 arguments".to_string(),
                    ));
                }
                let addr = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                let cidr = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::is_ip_address_in_range(&addr, &cidr)
            }
            "IPV4CIDRTORANGE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "IPv4CIDRToRange requires 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let addr = if let Some(s) = val.as_str() {
                    s.to_string()
                } else if let Some(ip) = val.as_ipv4() {
                    ip.to_string()
                } else {
                    return Err(Error::type_mismatch("STRING or IPv4", "other"));
                };
                let prefix = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::network::ipv4_cidr_to_range(&addr, prefix)
            }
            "IPV6CIDRTORANGE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "IPv6CIDRToRange requires 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let addr = if let Some(s) = val.as_str() {
                    s.to_string()
                } else if let Some(ip) = val.as_ipv6() {
                    ip.to_string()
                } else {
                    return Err(Error::type_mismatch("STRING or IPv6", "other"));
                };
                let prefix = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::network::ipv6_cidr_to_range(&addr, prefix)
            }
            "MACNUMTOSTRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "MACNumToString requires 1 argument".to_string(),
                    ));
                }
                let num = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::network::mac_num_to_string(num)
            }
            "MACSTRINGTONUM" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "MACStringToNum requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::mac_string_to_num(&s)
            }
            "MACSTRINGTOOUI" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "MACStringToOUI requires 1 argument".to_string(),
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
                yachtsql_functions::network::mac_string_to_oui(&s)
            }
            "TOINT8" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toInt8 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_int8(&val)
            }
            "TOINT16" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toInt16 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_int16(&val)
            }
            "TOINT32" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toInt32 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_int32(&val)
            }
            "TOINT64" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toInt64 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_int64(&val)
            }
            "TOUINT8" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toUInt8 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_uint8(&val)
            }
            "TOUINT16" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toUInt16 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_uint16(&val)
            }
            "TOUINT32" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toUInt32 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_uint32(&val)
            }
            "TOUINT64" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toUInt64 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_uint64(&val)
            }
            "TOFLOAT32" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toFloat32 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_float32(&val)
            }
            "TOFLOAT64" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toFloat64 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_float64(&val)
            }
            "TOSTRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toString requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_string(&val)
            }
            "TOFIXEDSTRING" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "toFixedString requires 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_string(&val)
            }
            "TODATE" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toDate requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_date(&val)
            }
            "TODATETIME" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toDateTime requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_datetime(&val)
            }
            "TODATETIME64" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toDateTime64 requires at least 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::convert_to_datetime(&val)
            }
            "TODECIMAL32" | "TODECIMAL64" | "TODECIMAL128" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toDecimal requires at least 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let scale = if args.len() > 1 {
                    Self::evaluate_expr(&args[1], batch, row_idx)?
                        .as_i64()
                        .unwrap_or(0) as u32
                } else {
                    0
                };
                Self::convert_to_decimal(&val, scale)
            }
            "TOINT64ORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toInt64OrNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::convert_to_int64(&val).unwrap_or_else(|_| Value::null()))
            }
            "TOINT64ORZERO" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toInt64OrZero requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::convert_to_int64(&val).unwrap_or_else(|_| Value::int64(0)))
            }
            "TOFLOAT64ORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toFloat64OrNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::convert_to_float64(&val).unwrap_or_else(|_| Value::null()))
            }
            "TOFLOAT64ORZERO" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toFloat64OrZero requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::convert_to_float64(&val).unwrap_or_else(|_| Value::float64(0.0)))
            }
            "TODATEORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toDateOrNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::convert_to_date(&val).unwrap_or_else(|_| Value::null()))
            }
            "TODATETIMEORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toDateTimeOrNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::convert_to_datetime(&val).unwrap_or_else(|_| Value::null()))
            }
            "REINTERPRETASINT64" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "reinterpretAsInt64 requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::reinterpret_as_int64(&val)
            }
            "REINTERPRETASSTRING" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "reinterpretAsString requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::reinterpret_as_string(&val)
            }
            "TOTYPENAME" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "toTypeName requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Value::string(Self::get_clickhouse_type_name(&val)))
            }
            "ACCURATECAST" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "accurateCast requires 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let type_name = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .unwrap_or("Int64")
                    .to_string();
                Self::accurate_cast(&val, &type_name)
            }
            "ACCURATECASTORNULL" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "accurateCastOrNull requires 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let type_name = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .unwrap_or("Int64")
                    .to_string();
                Ok(Self::accurate_cast(&val, &type_name).unwrap_or_else(|_| Value::null()))
            }
            "PARSEDATETIME" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "parseDateTime requires 2 arguments".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let fmt = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_str()
                    .unwrap_or("%Y-%m-%d %H:%M:%S")
                    .to_string();
                Self::parse_datetime(&val, &fmt)
            }
            "PARSEDATETIMEBESTEFFORT" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "parseDateTimeBestEffort requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Self::parse_datetime_best_effort(&val)
            }
            "PARSEDATETIMEBESTEFFORTORNULL" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "parseDateTimeBestEffortOrNull requires 1 argument".to_string(),
                    ));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                Ok(Self::parse_datetime_best_effort(&val).unwrap_or_else(|_| Value::null()))
            }
            "POSITION"
            | "POSITIONCASEINSENSITIVE"
            | "POSITIONUTF8"
            | "POSITIONCASEINSENSITIVEUTF8" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "position requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needle = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let case_insensitive =
                    name == "POSITIONCASEINSENSITIVE" || name == "POSITIONCASEINSENSITIVEUTF8";
                Self::string_position(&haystack, &needle, case_insensitive)
            }
            "LOCATE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "locate requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needle = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::string_position(&haystack, &needle, false)
            }
            "COUNTSUBSTRINGS" | "COUNTSUBSTRINGSCASEINSENSITIVE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "countSubstrings requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needle = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let case_insensitive = name == "COUNTSUBSTRINGSCASEINSENSITIVE";
                Self::count_substrings(&haystack, &needle, case_insensitive)
            }
            "MATCH" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "match requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let pattern = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::regex_match(&haystack, &pattern)
            }
            "MULTISEARCHANY" | "MULTIMATCHANY" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(format!(
                        "{} requires 2 arguments",
                        name
                    )));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needles = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::multi_search_any(&haystack, &needles, name == "MULTIMATCHANY")
            }
            "MULTISEARCHFIRSTINDEX" | "MULTIMATCHANYINDEX" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(format!(
                        "{} requires 2 arguments",
                        name
                    )));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needles = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::multi_search_first_index(&haystack, &needles, name == "MULTIMATCHANYINDEX")
            }
            "MULTISEARCHFIRSTPOSITION" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "multiSearchFirstPosition requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needles = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::multi_search_first_position(&haystack, &needles)
            }
            "MULTISEARCHALLPOSITIONS" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "multiSearchAllPositions requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let needles = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::multi_search_all_positions(&haystack, &needles)
            }
            "MULTIMATCHALLINDICES" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "multiMatchAllIndices requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let patterns = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::multi_match_all_indices(&haystack, &patterns)
            }
            "EXTRACT" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "extract requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let pattern = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::regex_extract(&haystack, &pattern)
            }
            "EXTRACTGROUPS" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "extractGroups requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let pattern = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::extract_groups(&haystack, &pattern)
            }
            "COUNTMATCHES" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "countMatches requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let pattern = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::count_matches(&haystack, &pattern)
            }
            "HASTOKEN" | "HASTOKENCASEINSENSITIVE" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "hasToken requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let token = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let case_insensitive = name == "HASTOKENCASEINSENSITIVE";
                Self::has_token(&haystack, &token, case_insensitive)
            }
            "STARTSWITH" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "startsWith requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let prefix = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::starts_with(&haystack, &prefix)
            }
            "ENDSWITH" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(
                        "endsWith requires 2 arguments".to_string(),
                    ));
                }
                let haystack = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let suffix = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::ends_with(&haystack, &suffix)
            }
            "NGRAMDISTANCE" | "NGRAMSEARCH" => {
                if args.len() < 2 {
                    return Err(Error::invalid_query(format!(
                        "{} requires 2 arguments",
                        name
                    )));
                }
                let str1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let str2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Self::ngram_distance(&str1, &str2)
            }

            "HOST" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    Ok(Value::string(inet.addr.to_string()))
                } else {
                    Err(Error::type_mismatch("INET", &value.data_type().to_string()))
                }
            }
            "FAMILY" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    Ok(Value::int64(inet.family() as i64))
                } else {
                    Err(Error::type_mismatch("INET", &value.data_type().to_string()))
                }
            }
            "MASKLEN" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    let len = inet.prefix_len.unwrap_or(inet.max_prefix_len());
                    Ok(Value::int64(len as i64))
                } else if let Some(cidr) = value.as_cidr() {
                    Ok(Value::int64(cidr.prefix_len as i64))
                } else {
                    Err(Error::type_mismatch(
                        "INET or CIDR",
                        &value.data_type().to_string(),
                    ))
                }
            }
            "NETMASK" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    match inet.netmask() {
                        Some(mask) => Ok(Value::inet(
                            yachtsql_core::types::network::InetAddr::new(mask),
                        )),
                        None => {
                            let max_mask = if inet.is_ipv4() {
                                std::net::IpAddr::V4(std::net::Ipv4Addr::new(255, 255, 255, 255))
                            } else {
                                std::net::IpAddr::V6(std::net::Ipv6Addr::new(
                                    0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff, 0xffff,
                                ))
                            };
                            Ok(Value::inet(yachtsql_core::types::network::InetAddr::new(
                                max_mask,
                            )))
                        }
                    }
                } else {
                    Err(Error::type_mismatch("INET", &value.data_type().to_string()))
                }
            }
            "NETWORK" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    match inet.network() {
                        Some(cidr) => Ok(Value::cidr(cidr)),
                        None => {
                            let prefix = inet.max_prefix_len();
                            Ok(Value::cidr(yachtsql_core::types::network::CidrAddr {
                                network: inet.addr,
                                prefix_len: prefix,
                            }))
                        }
                    }
                } else {
                    Err(Error::type_mismatch("INET", &value.data_type().to_string()))
                }
            }
            "BROADCAST" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    match inet.broadcast() {
                        Some(bcast) => {
                            let inet_broadcast = yachtsql_core::types::network::InetAddr {
                                addr: bcast,
                                prefix_len: inet.prefix_len,
                            };
                            Ok(Value::inet(inet_broadcast))
                        }
                        None => Ok(value.clone()),
                    }
                } else {
                    Err(Error::type_mismatch("INET", &value.data_type().to_string()))
                }
            }
            "HOSTMASK" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
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
                    Ok(Value::inet(yachtsql_core::types::network::InetAddr::new(
                        hostmask,
                    )))
                } else {
                    Err(Error::type_mismatch("INET", &value.data_type().to_string()))
                }
            }
            "INET_SAME_FAMILY" => {
                let a = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let b = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if a.is_null() || b.is_null() {
                    return Ok(Value::null());
                }
                match (a.as_inet(), b.as_inet()) {
                    (Some(ia), Some(ib)) => Ok(Value::bool_val(ia.is_ipv4() == ib.is_ipv4())),
                    _ => Err(Error::type_mismatch(
                        "INET, INET",
                        &format!("{:?}, {:?}", a.data_type(), b.data_type()),
                    )),
                }
            }
            "ABBREV" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    match inet.prefix_len {
                        Some(prefix) if prefix == inet.max_prefix_len() => {
                            Ok(Value::string(inet.addr.to_string()))
                        }
                        Some(_) => Ok(Value::string(inet.to_string())),
                        None => Ok(Value::string(inet.addr.to_string())),
                    }
                } else if let Some(cidr) = value.as_cidr() {
                    Ok(Value::string(cidr.to_string()))
                } else {
                    Err(Error::type_mismatch(
                        "INET or CIDR",
                        &value.data_type().to_string(),
                    ))
                }
            }
            "SET_MASKLEN" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let len = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if value.is_null() || len.is_null() {
                    return Ok(Value::null());
                }
                let new_len = len
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", &len.data_type().to_string()))?
                    as u8;
                if let Some(inet) = value.as_inet() {
                    Ok(Value::inet(yachtsql_core::types::network::InetAddr {
                        addr: inet.addr,
                        prefix_len: Some(new_len),
                    }))
                } else if let Some(cidr) = value.as_cidr() {
                    let truncated_network = match cidr.network {
                        std::net::IpAddr::V4(ip) => {
                            let bits = u32::from_be_bytes(ip.octets());
                            let mask = if new_len == 0 {
                                0u32
                            } else if new_len >= 32 {
                                !0u32
                            } else {
                                !0u32 << (32 - new_len)
                            };
                            std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                                (bits & mask).to_be_bytes(),
                            ))
                        }
                        std::net::IpAddr::V6(ip) => {
                            let bits = u128::from_be_bytes(ip.octets());
                            let mask = if new_len == 0 {
                                0u128
                            } else if new_len >= 128 {
                                !0u128
                            } else {
                                !0u128 << (128 - new_len)
                            };
                            std::net::IpAddr::V6(std::net::Ipv6Addr::from(
                                (bits & mask).to_be_bytes(),
                            ))
                        }
                    };
                    match yachtsql_core::types::network::CidrAddr::new(truncated_network, new_len) {
                        Ok(new_cidr) => Ok(Value::cidr(new_cidr)),
                        Err(e) => Err(Error::invalid_operation(format!(
                            "Invalid prefix length: {}",
                            e
                        ))),
                    }
                } else {
                    Err(Error::type_mismatch(
                        "INET or CIDR",
                        &value.data_type().to_string(),
                    ))
                }
            }
            "TEXT" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(inet) = value.as_inet() {
                    Ok(Value::string(inet.to_string()))
                } else if let Some(cidr) = value.as_cidr() {
                    Ok(Value::string(cidr.to_string()))
                } else {
                    Ok(Value::string(format!("{:?}", value)))
                }
            }
            "INET_MERGE" => {
                let a = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let b = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if a.is_null() || b.is_null() {
                    return Ok(Value::null());
                }
                match (a.as_inet(), b.as_inet()) {
                    (Some(ia), Some(ib)) => {
                        if ia.is_ipv4() != ib.is_ipv4() {
                            return Err(Error::invalid_operation(
                                "Cannot merge IPv4 and IPv6 addresses".to_string(),
                            ));
                        }
                        if ia.is_ipv4() {
                            let a_bytes: [u8; 4] = match ia.addr {
                                std::net::IpAddr::V4(ip) => ip.octets(),
                                _ => unreachable!(),
                            };
                            let b_bytes: [u8; 4] = match ib.addr {
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
                            Ok(Value::cidr(
                                yachtsql_core::types::network::CidrAddr::new(
                                    network_ip,
                                    common_bits,
                                )
                                .unwrap(),
                            ))
                        } else {
                            let a_bytes: [u8; 16] = match ia.addr {
                                std::net::IpAddr::V6(ip) => ip.octets(),
                                _ => unreachable!(),
                            };
                            let b_bytes: [u8; 16] = match ib.addr {
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
                            Ok(Value::cidr(
                                yachtsql_core::types::network::CidrAddr::new(
                                    network_ip,
                                    common_bits,
                                )
                                .unwrap(),
                            ))
                        }
                    }
                    _ => Err(Error::type_mismatch(
                        "INET, INET",
                        &format!("{:?}, {:?}", a.data_type(), b.data_type()),
                    )),
                }
            }
            "TRUNC" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(mac) = value.as_macaddr() {
                    Ok(Value::macaddr(mac.trunc()))
                } else {
                    Err(Error::type_mismatch(
                        "MACADDR",
                        &value.data_type().to_string(),
                    ))
                }
            }
            "MACADDR8_SET7BIT" => {
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(mac) = value.as_macaddr8() {
                    let mut octets = mac.octets;
                    octets[0] |= 0x02;
                    let new_mac = yachtsql_core::types::MacAddress {
                        octets,
                        is_eui64: mac.is_eui64,
                    };
                    Ok(Value::macaddr8(new_mac))
                } else {
                    Err(Error::type_mismatch(
                        "MACADDR8",
                        &value.data_type().to_string(),
                    ))
                }
            }
            "TOLOWCARDINALITY" => Self::eval_to_low_cardinality(args, batch, row_idx),
            "LOWCARDINALITYINDICES" => Self::eval_low_cardinality_indices(args, batch, row_idx),
            "LOWCARDINALITYKEYS" => Self::eval_low_cardinality_keys(args, batch, row_idx),
            "TOUUID" => Self::eval_to_uuid(args, batch, row_idx),

            "L2DISTANCE" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("L2Distance requires 2 arguments"));
                }
                let p1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let p2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let (x1, y1) = if let Some(gp) = p1.as_geo_point() {
                    (gp.x, gp.y)
                } else if let Some(pt) = p1.as_point() {
                    (pt.x, pt.y)
                } else {
                    return Err(Error::type_mismatch("Point", &p1.data_type().to_string()));
                };
                let (x2, y2) = if let Some(gp) = p2.as_geo_point() {
                    (gp.x, gp.y)
                } else if let Some(pt) = p2.as_point() {
                    (pt.x, pt.y)
                } else {
                    return Err(Error::type_mismatch("Point", &p2.data_type().to_string()));
                };
                let dx = x2 - x1;
                let dy = y2 - y1;
                Ok(Value::float64((dx * dx + dy * dy).sqrt()))
            }

            "POLYGONAREACARTESIAN" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "polygonAreaCartesian requires 1 argument",
                    ));
                }
                let poly = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if let Some(polygon) = poly.as_geo_polygon() {
                    if polygon.is_empty() || polygon[0].is_empty() {
                        return Ok(Value::float64(0.0));
                    }
                    let ring = &polygon[0];
                    let mut area = 0.0;
                    for i in 0..ring.len() {
                        let j = (i + 1) % ring.len();
                        area += ring[i].x * ring[j].y;
                        area -= ring[j].x * ring[i].y;
                    }
                    Ok(Value::float64((area / 2.0).abs()))
                } else {
                    Err(Error::type_mismatch(
                        "Polygon",
                        &poly.data_type().to_string(),
                    ))
                }
            }

            "POLYGONPERIMETERCARTESIAN" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "polygonPerimeterCartesian requires 1 argument",
                    ));
                }
                let poly = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if let Some(polygon) = poly.as_geo_polygon() {
                    if polygon.is_empty() || polygon[0].is_empty() {
                        return Ok(Value::float64(0.0));
                    }
                    let ring = &polygon[0];
                    let mut perimeter = 0.0;
                    for i in 0..ring.len() {
                        let j = (i + 1) % ring.len();
                        let dx = ring[j].x - ring[i].x;
                        let dy = ring[j].y - ring[i].y;
                        perimeter += (dx * dx + dy * dy).sqrt();
                    }
                    Ok(Value::float64(perimeter))
                } else {
                    Err(Error::type_mismatch(
                        "Polygon",
                        &poly.data_type().to_string(),
                    ))
                }
            }

            "POLYGONCONVEXHULLCARTESIAN" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "polygonConvexHullCartesian requires 1 argument",
                    ));
                }
                let poly = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if let Some(polygon) = poly.as_geo_polygon() {
                    if polygon.is_empty() {
                        return Ok(Value::geo_polygon(vec![]));
                    }
                    Ok(Value::geo_polygon(polygon.clone()))
                } else {
                    Err(Error::type_mismatch(
                        "Polygon",
                        &poly.data_type().to_string(),
                    ))
                }
            }

            "POLYGONSINTERSECTIONCARTESIAN" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "polygonsIntersectionCartesian requires 2 arguments",
                    ));
                }
                let _p1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let _p2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Ok(Value::geo_multipolygon(vec![]))
            }

            "POLYGONSUNIONCARTESIAN" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "polygonsUnionCartesian requires 2 arguments",
                    ));
                }
                let _p1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let _p2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
                Ok(Value::geo_multipolygon(vec![]))
            }

            "TOYEAR" => {
                use chrono::Datelike;
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = value.as_timestamp() {
                    return Ok(Value::int64(ts.year() as i64));
                }
                if let Some(dt) = value.as_datetime() {
                    return Ok(Value::int64(dt.year() as i64));
                }
                if let Some(d) = value.as_date() {
                    return Ok(Value::int64(d.year() as i64));
                }
                if let Some(d32) = value.as_date32() {
                    if let Some(date) = d32.to_naive_date() {
                        return Ok(Value::int64(date.year() as i64));
                    }
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP, DATETIME, DATE, or DATE32",
                    &value.data_type().to_string(),
                ))
            }

            "TOMONTH" => {
                use chrono::Datelike;
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = value.as_timestamp() {
                    return Ok(Value::int64(ts.month() as i64));
                }
                if let Some(dt) = value.as_datetime() {
                    return Ok(Value::int64(dt.month() as i64));
                }
                if let Some(d) = value.as_date() {
                    return Ok(Value::int64(d.month() as i64));
                }
                if let Some(d32) = value.as_date32() {
                    if let Some(date) = d32.to_naive_date() {
                        return Ok(Value::int64(date.month() as i64));
                    }
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP, DATETIME, DATE, or DATE32",
                    &value.data_type().to_string(),
                ))
            }

            "TODAYOFMONTH" => {
                use chrono::Datelike;
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = value.as_timestamp() {
                    return Ok(Value::int64(ts.day() as i64));
                }
                if let Some(dt) = value.as_datetime() {
                    return Ok(Value::int64(dt.day() as i64));
                }
                if let Some(d) = value.as_date() {
                    return Ok(Value::int64(d.day() as i64));
                }
                if let Some(d32) = value.as_date32() {
                    if let Some(date) = d32.to_naive_date() {
                        return Ok(Value::int64(date.day() as i64));
                    }
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP, DATETIME, DATE, or DATE32",
                    &value.data_type().to_string(),
                ))
            }

            "TOHOUR" => {
                use chrono::Timelike;
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = value.as_timestamp() {
                    return Ok(Value::int64(ts.hour() as i64));
                }
                if let Some(dt) = value.as_datetime() {
                    return Ok(Value::int64(dt.hour() as i64));
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP or DATETIME",
                    &value.data_type().to_string(),
                ))
            }

            "TOMINUTE" => {
                use chrono::Timelike;
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = value.as_timestamp() {
                    return Ok(Value::int64(ts.minute() as i64));
                }
                if let Some(dt) = value.as_datetime() {
                    return Ok(Value::int64(dt.minute() as i64));
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP or DATETIME",
                    &value.data_type().to_string(),
                ))
            }

            "TOSECOND" => {
                use chrono::Timelike;
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if value.is_null() {
                    return Ok(Value::null());
                }
                if let Some(ts) = value.as_timestamp() {
                    return Ok(Value::int64(ts.second() as i64));
                }
                if let Some(dt) = value.as_datetime() {
                    return Ok(Value::int64(dt.second() as i64));
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP or DATETIME",
                    &value.data_type().to_string(),
                ))
            }

            "RAND" | "RAND32" => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<u32>() as i64))
            }

            "RAND64" => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<i64>()))
            }

            "RANDCONSTANT" => {
                use rand::Rng;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(rng.r#gen::<u32>() as i64))
            }

            "RANDUNIFORM" => {
                use rand::Rng;
                if args.len() != 2 {
                    return Err(Error::invalid_query("randUniform requires 2 arguments"));
                }
                let min = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let max = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[1], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(rng.gen_range(min..max)))
            }

            "RANDNORMAL" => {
                use rand_distr::{Distribution, Normal};
                if args.len() != 2 {
                    return Err(Error::invalid_query("randNormal requires 2 arguments"));
                }
                let mean = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let stddev = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[1], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let normal =
                    Normal::new(mean, stddev).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(normal.sample(&mut rng)))
            }

            "RANDLOGNORMAL" => {
                use rand_distr::{Distribution, LogNormal};
                if args.len() != 2 {
                    return Err(Error::invalid_query("randLogNormal requires 2 arguments"));
                }
                let mean = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let stddev = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[1], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let log_normal = LogNormal::new(mean, stddev)
                    .map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(log_normal.sample(&mut rng)))
            }

            "RANDEXPONENTIAL" => {
                use rand_distr::{Distribution, Exp};
                if args.len() != 1 {
                    return Err(Error::invalid_query("randExponential requires 1 argument"));
                }
                let lambda = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let exp = Exp::new(lambda).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(exp.sample(&mut rng)))
            }

            "RANDCHISQUARED" => {
                use rand_distr::{ChiSquared, Distribution};
                if args.len() != 1 {
                    return Err(Error::invalid_query("randChiSquared requires 1 argument"));
                }
                let k = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let chi_squared =
                    ChiSquared::new(k).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(chi_squared.sample(&mut rng)))
            }

            "RANDSTUDENTT" => {
                use rand_distr::{Distribution, StudentT};
                if args.len() != 1 {
                    return Err(Error::invalid_query("randStudentT requires 1 argument"));
                }
                let df = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let student_t =
                    StudentT::new(df).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(student_t.sample(&mut rng)))
            }

            "RANDFISHERF" => {
                use rand_distr::{Distribution, FisherF};
                if args.len() != 2 {
                    return Err(Error::invalid_query("randFisherF requires 2 arguments"));
                }
                let d1 = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let d2 = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[1], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let fisher_f =
                    FisherF::new(d1, d2).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::float64(fisher_f.sample(&mut rng)))
            }

            "RANDBERNOULLI" => {
                use rand_distr::{Bernoulli, Distribution};
                if args.len() != 1 {
                    return Err(Error::invalid_query("randBernoulli requires 1 argument"));
                }
                let p = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let bernoulli =
                    Bernoulli::new(p).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(if bernoulli.sample(&mut rng) { 1 } else { 0 }))
            }

            "RANDBINOMIAL" => {
                use rand_distr::{Binomial, Distribution};
                if args.len() != 2 {
                    return Err(Error::invalid_query("randBinomial requires 2 arguments"));
                }
                let n = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let p = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[1], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let binomial =
                    Binomial::new(n as u64, p).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(binomial.sample(&mut rng) as i64))
            }

            "RANDNEGATIVEBINOMIAL" => {
                use rand::Rng;
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "randNegativeBinomial requires 2 arguments",
                    ));
                }
                let r = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let p = Self::evaluate_expr(&args[1], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[1], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let mut rng = rand::thread_rng();
                let mut count = 0i64;
                let mut successes = 0;
                while successes < r {
                    if rng.gen_bool(p) {
                        successes += 1;
                    } else {
                        count += 1;
                    }
                }
                Ok(Value::int64(count))
            }

            "RANDPOISSON" => {
                use rand_distr::{Distribution, Poisson};
                if args.len() != 1 {
                    return Err(Error::invalid_query("randPoisson requires 1 argument"));
                }
                let lambda = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_f64()
                    .or_else(|| {
                        Self::evaluate_expr(&args[0], batch, row_idx)
                            .ok()?
                            .as_i64()
                            .map(|i| i as f64)
                    })
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let poisson =
                    Poisson::new(lambda).map_err(|e| Error::invalid_query(e.to_string()))?;
                let mut rng = rand::thread_rng();
                Ok(Value::int64(poisson.sample(&mut rng) as i64))
            }

            "RANDOMSTRING" => {
                use rand::Rng;
                if args.len() != 1 {
                    return Err(Error::invalid_query("randomString requires 1 argument"));
                }
                let length = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let mut rng = rand::thread_rng();
                let bytes: Vec<u8> = (0..length).map(|_| rng.r#gen::<u8>()).collect();
                Ok(Value::bytes(bytes))
            }

            "RANDOMFIXEDSTRING" => {
                use rand::Rng;
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "randomFixedString requires 1 argument",
                    ));
                }
                let length = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let mut rng = rand::thread_rng();
                let bytes: Vec<u8> = (0..length).map(|_| rng.r#gen::<u8>()).collect();
                Ok(Value::bytes(bytes))
            }

            "RANDOMPRINTABLEASCII" => {
                use rand::Rng;
                if args.len() != 1 {
                    return Err(Error::invalid_query(
                        "randomPrintableASCII requires 1 argument",
                    ));
                }
                let length = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let mut rng = rand::thread_rng();
                let s: String = (0..length)
                    .map(|_| rng.gen_range(32..=126u8) as char)
                    .collect();
                Ok(Value::string(s))
            }

            "RANDOMSTRINGUTF8" => {
                use rand::Rng;
                if args.len() != 1 {
                    return Err(Error::invalid_query("randomStringUTF8 requires 1 argument"));
                }
                let length = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let mut rng = rand::thread_rng();
                let s: String = (0..length)
                    .map(|_| char::from_u32(rng.gen_range(0x20..=0x7E)).unwrap_or('?'))
                    .collect();
                Ok(Value::string(s))
            }

            "FAKEDATA" => {
                use rand::Rng;
                if args.len() != 1 {
                    return Err(Error::invalid_query("fakeData requires 1 argument"));
                }
                let data_type = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let type_str = data_type
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?;
                let mut rng = rand::thread_rng();
                let result = match type_str.to_lowercase().as_str() {
                    "name" => {
                        let first = ["John", "Jane", "Alice", "Bob"][rng.gen_range(0..4)];
                        let last = ["Smith", "Johnson", "Williams", "Brown"][rng.gen_range(0..4)];
                        format!("{} {}", first, last)
                    }
                    "email" => format!(
                        "{}@example.com",
                        (0..8)
                            .map(|_| rng.gen_range(b'a'..=b'z') as char)
                            .collect::<String>()
                    ),
                    "phone" => format!(
                        "+1-{:03}-{:03}-{:04}",
                        rng.gen_range(100..999),
                        rng.gen_range(100..999),
                        rng.gen_range(1000..9999)
                    ),
                    _ => format!("fake_{}", type_str),
                };
                Ok(Value::string(result))
            }

            "GENERATEUUIDV4" => {
                let uuid = uuid::Uuid::new_v4();
                Ok(Value::string(uuid.to_string()))
            }

            "TOUUIDORNULL" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toUUIDOrNull requires 1 argument"));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if s.is_null() {
                    return Ok(Value::null());
                }
                let uuid_str = s
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?;
                match uuid::Uuid::parse_str(uuid_str) {
                    Ok(uuid) => Ok(Value::string(uuid.to_string())),
                    Err(_) => Ok(Value::null()),
                }
            }

            "TOUUIDORZERO" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toUUIDOrZero requires 1 argument"));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if s.is_null() {
                    return Ok(Value::string(
                        "00000000-0000-0000-0000-000000000000".to_string(),
                    ));
                }
                let uuid_str = s
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?;
                match uuid::Uuid::parse_str(uuid_str) {
                    Ok(uuid) => Ok(Value::string(uuid.to_string())),
                    Err(_) => Ok(Value::string(
                        "00000000-0000-0000-0000-000000000000".to_string(),
                    )),
                }
            }

            "UUIDSTRINGTONUM" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UUIDStringToNum requires 1 argument"));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let uuid_str = s
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?;
                let uuid = uuid::Uuid::parse_str(uuid_str).map_err(|_| {
                    Error::invalid_query(format!(
                        "UUIDStringToNum: invalid UUID string: {}",
                        uuid_str
                    ))
                })?;
                Ok(Value::bytes(uuid.as_bytes().to_vec()))
            }

            "UUIDNUMTOSTRING" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("UUIDNumToString requires 1 argument"));
                }
                let bytes_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let bytes = bytes_val
                    .as_bytes()
                    .ok_or_else(|| Error::type_mismatch("BYTES", "other"))?;
                if bytes.len() != 16 {
                    return Err(Error::invalid_query(
                        "UUIDNumToString: bytes must be exactly 16 bytes",
                    ));
                }
                let uuid_bytes: [u8; 16] = bytes
                    .try_into()
                    .map_err(|_| Error::invalid_query("UUIDNumToString: invalid bytes"))?;
                let uuid = uuid::Uuid::from_bytes(uuid_bytes);
                Ok(Value::string(uuid.to_string()))
            }

            "SERVERUUID" => {
                use std::sync::LazyLock;
                static SERVER_UUID: LazyLock<uuid::Uuid> = LazyLock::new(uuid::Uuid::new_v4);
                Ok(Value::string(SERVER_UUID.to_string()))
            }

            "GENERATEULID" => {
                let ulid = ulid::Ulid::new();
                Ok(Value::string(ulid.to_string()))
            }

            "ULIDSTRINGTODATETIME" | "ULIDTODATETIME" => {
                if args.is_empty() {
                    return Err(Error::invalid_query(
                        "ULIDStringToDateTime requires 1 argument",
                    ));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let ulid_str = s
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?;
                let ulid = ulid::Ulid::from_string(ulid_str).map_err(|_| {
                    Error::invalid_query(format!(
                        "ULIDStringToDateTime: invalid ULID string: {}",
                        ulid_str
                    ))
                })?;
                let timestamp_ms = ulid.timestamp_ms();
                let secs = (timestamp_ms / 1000) as i64;
                let nsecs = ((timestamp_ms % 1000) * 1_000_000) as u32;
                let dt = chrono::DateTime::from_timestamp(secs, nsecs).ok_or_else(|| {
                    Error::invalid_query("ULIDStringToDateTime: invalid timestamp")
                })?;
                Ok(Value::timestamp(dt))
            }

            "TOULID" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("toULID requires 1 argument"));
                }
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let ulid_str = s
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?;
                let ulid = ulid::Ulid::from_string(ulid_str).map_err(|_| {
                    Error::invalid_query(format!("toULID: invalid ULID string: {}", ulid_str))
                })?;
                Ok(Value::string(ulid.to_string()))
            }

            "TUMBLE" | "TUMBLESTART" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "tumble/tumbleStart requires 2 arguments",
                    ));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let interval = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let interval_secs = if let Some(iv) = interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let ts_secs = ts_val.timestamp();
                let window_num = ts_secs / interval_secs;
                let window_start_secs = window_num * interval_secs;
                let result = chrono::DateTime::from_timestamp(window_start_secs, 0)
                    .ok_or_else(|| Error::invalid_query("tumble: invalid result timestamp"))?;
                Ok(Value::datetime(result))
            }

            "TUMBLEEND" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("tumbleEnd requires 2 arguments"));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let interval = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let interval_secs = if let Some(iv) = interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let ts_secs = ts_val.timestamp();
                let window_num = ts_secs / interval_secs;
                let window_end_secs = (window_num + 1) * interval_secs;
                let result = chrono::DateTime::from_timestamp(window_end_secs, 0)
                    .ok_or_else(|| Error::invalid_query("tumbleEnd: invalid result timestamp"))?;
                Ok(Value::datetime(result))
            }

            "HOP" | "HOPSTART" => {
                if args.len() != 3 {
                    return Err(Error::invalid_query("hop/hopStart requires 3 arguments"));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let hop_interval = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let window_interval = Self::evaluate_expr(&args[2], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let hop_secs = if let Some(iv) = hop_interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = hop_interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let window_secs = if let Some(iv) = window_interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = window_interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let ts_secs = ts_val.timestamp();
                let earliest_possible = ts_secs - window_secs + hop_secs;
                let hop_num = (earliest_possible + hop_secs - 1) / hop_secs;
                let hop_start_secs = hop_num * hop_secs;
                let result = chrono::DateTime::from_timestamp(hop_start_secs, 0)
                    .ok_or_else(|| Error::invalid_query("hop: invalid result timestamp"))?;
                Ok(Value::datetime(result))
            }

            "HOPEND" => {
                if args.len() != 3 {
                    return Err(Error::invalid_query("hopEnd requires 3 arguments"));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let hop_interval = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let window_interval = Self::evaluate_expr(&args[2], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let hop_secs = if let Some(iv) = hop_interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = hop_interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let window_secs = if let Some(iv) = window_interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = window_interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let ts_secs = ts_val.timestamp();
                let earliest_possible = ts_secs - window_secs + hop_secs;
                let hop_num = (earliest_possible + hop_secs - 1) / hop_secs;
                let hop_start_secs = hop_num * hop_secs;
                let hop_end_secs = hop_start_secs + window_secs;
                let result = chrono::DateTime::from_timestamp(hop_end_secs, 0)
                    .ok_or_else(|| Error::invalid_query("hopEnd: invalid result timestamp"))?;
                Ok(Value::datetime(result))
            }

            "TIMESLOT" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("timeSlot requires 1 argument"));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let ts_secs = ts_val.timestamp();
                let slot_secs = 1800;
                let slot_num = ts_secs / slot_secs;
                let slot_start_secs = slot_num * slot_secs;
                let result = chrono::DateTime::from_timestamp(slot_start_secs, 0)
                    .ok_or_else(|| Error::invalid_query("timeSlot: invalid result timestamp"))?;
                Ok(Value::datetime(result))
            }

            "TIMESLOTS" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query("timeSlots requires 2 or 3 arguments"));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let duration = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let duration_secs = duration
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                let slot_secs = if args.len() == 3 {
                    let slot_size = Self::evaluate_expr(&args[2], batch, row_idx)?;
                    slot_size
                        .as_i64()
                        .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                } else {
                    1800
                };
                if slot_secs <= 0 {
                    return Err(Error::invalid_query(
                        "timeSlots: slot size must be positive",
                    ));
                }
                let start_secs = ts_val.timestamp();
                let slot_num = start_secs / slot_secs;
                let slot_start_secs = slot_num * slot_secs;
                let end_secs = start_secs + duration_secs;

                let mut slots = Vec::new();
                let mut current = slot_start_secs;
                while current < end_secs {
                    let slot_dt = chrono::DateTime::from_timestamp(current, 0)
                        .ok_or_else(|| Error::invalid_query("timeSlots: invalid slot timestamp"))?;
                    slots.push(Value::datetime(slot_dt));
                    current += slot_secs;
                }
                Ok(Value::array(slots))
            }

            "WINDOWID" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("windowID requires 1 argument"));
                }
                let ts = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                Ok(Value::int64(ts_val.timestamp()))
            }

            "DATE_BIN" => {
                if args.len() < 2 || args.len() > 3 {
                    return Err(Error::invalid_query("date_bin requires 2 or 3 arguments"));
                }
                let interval = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let ts = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let ts_val = ts
                    .as_datetime()
                    .or_else(|| ts.as_timestamp())
                    .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?;
                let interval_secs = if let Some(iv) = interval.as_interval() {
                    (iv.months as i64) * 30 * 24 * 3600
                        + (iv.days as i64) * 24 * 3600
                        + iv.micros / 1_000_000
                } else if let Some(i) = interval.as_i64() {
                    i
                } else {
                    return Err(Error::type_mismatch("INTERVAL", "other"));
                };
                let origin_secs = if args.len() == 3 {
                    let origin = Self::evaluate_expr(&args[2], batch, row_idx)?;
                    origin
                        .as_datetime()
                        .or_else(|| origin.as_timestamp())
                        .ok_or_else(|| Error::type_mismatch("DATETIME or TIMESTAMP", "other"))?
                        .timestamp()
                } else {
                    0
                };
                let ts_secs = ts_val.timestamp();
                let offset = ts_secs - origin_secs;
                let bin_num = offset / interval_secs;
                let bin_start_secs = origin_secs + bin_num * interval_secs;
                let result = chrono::DateTime::from_timestamp(bin_start_secs, 0)
                    .ok_or_else(|| Error::invalid_query("date_bin: invalid result timestamp"))?;
                Ok(Value::datetime(result))
            }

            "ROUNDBANKERS" => {
                if args.is_empty() || args.len() > 2 {
                    return Err(Error::invalid_query(
                        "roundBankers requires 1 or 2 arguments",
                    ));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let v = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let decimals = if args.len() == 2 {
                    Self::evaluate_expr(&args[1], batch, row_idx)?
                        .as_i64()
                        .ok_or_else(|| Error::type_mismatch("INT64", "other"))?
                        as i32
                } else {
                    0
                };
                let multiplier = 10_f64.powi(decimals);
                let scaled = v * multiplier;
                let floor_scaled = scaled.floor();
                let frac = scaled - floor_scaled;
                let rounded = if (frac - 0.5).abs() < 1e-10 {
                    if floor_scaled as i64 % 2 == 0 {
                        floor_scaled
                    } else {
                        floor_scaled + 1.0
                    }
                } else {
                    scaled.round()
                };
                Ok(Value::float64(rounded / multiplier))
            }

            "ROUNDDOWN" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("roundDown requires 2 arguments"));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let v = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                let thresholds_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
                let thresholds = thresholds_val.as_array().ok_or_else(|| {
                    Error::invalid_query("roundDown: second argument must be an array")
                })?;
                let mut sorted_thresholds: Vec<f64> = thresholds
                    .iter()
                    .filter_map(|t| t.as_f64().or_else(|| t.as_i64().map(|i| i as f64)))
                    .collect();
                sorted_thresholds
                    .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let result = sorted_thresholds
                    .iter()
                    .rev()
                    .find(|&&t| t <= v)
                    .copied()
                    .unwrap_or(sorted_thresholds.first().copied().unwrap_or(0.0));
                Ok(Value::float64(result))
            }

            "ROUNDTOEXP2" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("roundToExp2 requires 1 argument"));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let v = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?;
                if v <= 0.0 {
                    return Ok(Value::int64(0));
                }
                let exp = v.log2().floor() as u32;
                Ok(Value::int64(1i64 << exp))
            }

            "ROUNDDURATION" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("roundDuration requires 1 argument"));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let v = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?
                    as i64;
                let thresholds = [
                    0, 1, 10, 30, 60, 120, 180, 240, 300, 600, 1200, 1800, 3600, 7200, 18000,
                    36000, 86400, 172800, 604800, 2592000, 31536000,
                ];
                let result = thresholds
                    .iter()
                    .rev()
                    .find(|&&t| t <= v)
                    .copied()
                    .unwrap_or(0);
                Ok(Value::int64(result))
            }

            "ROUNDAGE" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("roundAge requires 1 argument"));
                }
                let value = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let age = value
                    .as_f64()
                    .or_else(|| value.as_i64().map(|i| i as f64))
                    .ok_or_else(|| Error::type_mismatch("FLOAT64", "other"))?
                    as i64;
                let thresholds = [0, 1, 18, 25, 35, 45, 55];
                let result = thresholds
                    .iter()
                    .rev()
                    .find(|&&t| t <= age)
                    .copied()
                    .unwrap_or(0);
                Ok(Value::int64(result))
            }

            "ADDSECONDS" => {
                use chrono::Duration;
                if args.len() != 2 {
                    return Err(Error::invalid_query("addSeconds requires 2 arguments"));
                }
                let dt_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let seconds_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if dt_val.is_null() || seconds_val.is_null() {
                    return Ok(Value::null());
                }
                let seconds = seconds_val.as_i64().ok_or_else(|| {
                    Error::type_mismatch("INT64", &seconds_val.data_type().to_string())
                })?;
                if let Some(ts) = dt_val.as_timestamp() {
                    return ts
                        .checked_add_signed(Duration::seconds(seconds))
                        .map(Value::timestamp)
                        .ok_or_else(|| Error::invalid_query("Date overflow in addSeconds"));
                }
                if let Some(dt) = dt_val.as_datetime() {
                    return dt
                        .checked_add_signed(Duration::seconds(seconds))
                        .map(Value::datetime)
                        .ok_or_else(|| Error::invalid_query("Date overflow in addSeconds"));
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP or DATETIME",
                    &dt_val.data_type().to_string(),
                ))
            }

            "SUBTRACTSECONDS" => {
                use chrono::Duration;
                if args.len() != 2 {
                    return Err(Error::invalid_query("subtractSeconds requires 2 arguments"));
                }
                let dt_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let seconds_val = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if dt_val.is_null() || seconds_val.is_null() {
                    return Ok(Value::null());
                }
                let seconds = seconds_val.as_i64().ok_or_else(|| {
                    Error::type_mismatch("INT64", &seconds_val.data_type().to_string())
                })?;
                if let Some(ts) = dt_val.as_timestamp() {
                    return ts
                        .checked_sub_signed(Duration::seconds(seconds))
                        .map(Value::timestamp)
                        .ok_or_else(|| Error::invalid_query("Date overflow in subtractSeconds"));
                }
                if let Some(dt) = dt_val.as_datetime() {
                    return dt
                        .checked_sub_signed(Duration::seconds(seconds))
                        .map(Value::datetime)
                        .ok_or_else(|| Error::invalid_query("Date overflow in subtractSeconds"));
                }
                Err(Error::type_mismatch(
                    "TIMESTAMP or DATETIME",
                    &dt_val.data_type().to_string(),
                ))
            }

            "NOW64" => {
                use chrono::Utc;
                Ok(Value::datetime(Utc::now()))
            }

            "EMPTY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("empty requires 1 argument"));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::int64(1));
                }
                if let Some(uuid) = val.as_uuid() {
                    return Ok(Value::int64(if uuid.is_nil() { 1 } else { 0 }));
                }
                if let Some(s) = val.as_str() {
                    if s == "00000000-0000-0000-0000-000000000000" {
                        return Ok(Value::int64(1));
                    }
                    return Ok(Value::int64(if s.is_empty() { 1 } else { 0 }));
                }
                if let Some(arr) = val.as_array() {
                    return Ok(Value::int64(if arr.is_empty() { 1 } else { 0 }));
                }
                Ok(Value::int64(0))
            }

            "NOTEMPTY" => {
                if args.len() != 1 {
                    return Err(Error::invalid_query("notEmpty requires 1 argument"));
                }
                let val = Self::evaluate_expr(&args[0], batch, row_idx)?;
                if val.is_null() {
                    return Ok(Value::int64(0));
                }
                if let Some(uuid) = val.as_uuid() {
                    return Ok(Value::int64(if uuid.is_nil() { 0 } else { 1 }));
                }
                if let Some(s) = val.as_str() {
                    if s == "00000000-0000-0000-0000-000000000000" {
                        return Ok(Value::int64(0));
                    }
                    return Ok(Value::int64(if s.is_empty() { 0 } else { 1 }));
                }
                if let Some(arr) = val.as_array() {
                    return Ok(Value::int64(if arr.is_empty() { 0 } else { 1 }));
                }
                Ok(Value::int64(1))
            }

            "NULLIN" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("nullIn requires 2 arguments"));
                }
                let needle = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let haystack = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if let Some(arr) = haystack.as_array() {
                    for val in arr {
                        if needle.is_null() && val.is_null() {
                            return Ok(Value::bool_val(true));
                        }
                        if !needle.is_null() && !val.is_null() && needle == *val {
                            return Ok(Value::bool_val(true));
                        }
                    }
                }
                Ok(Value::bool_val(false))
            }

            "NOTNULLIN" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query("notNullIn requires 2 arguments"));
                }
                let needle = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let haystack = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if let Some(arr) = haystack.as_array() {
                    for val in arr {
                        if needle.is_null() && val.is_null() {
                            return Ok(Value::bool_val(false));
                        }
                        if !needle.is_null() && !val.is_null() && needle == *val {
                            return Ok(Value::bool_val(false));
                        }
                    }
                }
                Ok(Value::bool_val(true))
            }

            "NULLINIGNORENULL" => {
                if args.len() != 2 {
                    return Err(Error::invalid_query(
                        "nullInIgnoreNull requires 2 arguments",
                    ));
                }
                let needle = Self::evaluate_expr(&args[0], batch, row_idx)?;
                let haystack = Self::evaluate_expr(&args[1], batch, row_idx)?;
                if needle.is_null() {
                    return Ok(Value::bool_val(false));
                }
                if let Some(arr) = haystack.as_array() {
                    for val in arr {
                        if !val.is_null() && needle == *val {
                            return Ok(Value::bool_val(true));
                        }
                    }
                }
                Ok(Value::bool_val(false))
            }

            _ => Err(Error::unsupported_feature(format!(
                "Unknown custom function: {}",
                name
            ))),
        }
    }

    fn convert_to_int8(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as i8 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as i8 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i8>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("Int8", &val.data_type().to_string()))
    }

    fn convert_to_int16(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as i16 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as i16 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i16>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("Int16", &val.data_type().to_string()))
    }

    fn convert_to_int32(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as i32 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as i32 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i32>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("Int32", &val.data_type().to_string()))
    }

    fn convert_to_int64(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i64>() {
                return Ok(Value::int64(i));
            }
        }
        Err(Error::type_mismatch("Int64", &val.data_type().to_string()))
    }

    fn convert_to_uint8(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as u8 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as u8 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u8>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt8", &val.data_type().to_string()))
    }

    fn convert_to_uint16(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as u16 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as u16 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u16>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt16", &val.data_type().to_string()))
    }

    fn convert_to_uint32(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as u32 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as u32 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u32>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt32", &val.data_type().to_string()))
    }

    fn convert_to_uint64(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i as u64 as i64));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::int64(f as u64 as i64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u64>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt64", &val.data_type().to_string()))
    }

    fn convert_to_float32(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::float64(f as f32 as f64));
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::float64(i as f32 as f64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(f) = s.trim().parse::<f32>() {
                return Ok(Value::float64(f as f64));
            }
        }
        Err(Error::type_mismatch(
            "Float32",
            &val.data_type().to_string(),
        ))
    }

    fn convert_to_float64(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::float64(f));
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::float64(i as f64));
        }
        if let Some(s) = val.as_str() {
            if let Ok(f) = s.trim().parse::<f64>() {
                return Ok(Value::float64(f));
            }
        }
        Err(Error::type_mismatch(
            "Float64",
            &val.data_type().to_string(),
        ))
    }

    fn convert_to_string(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(s) = val.as_str() {
            return Ok(Value::string(s.to_string()));
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::string(i.to_string()));
        }
        if let Some(f) = val.as_f64() {
            return Ok(Value::string(f.to_string()));
        }
        if let Some(b) = val.as_bool() {
            return Ok(Value::string(if b { "true" } else { "false" }.to_string()));
        }
        if let Some(d) = val.as_date() {
            return Ok(Value::string(d.to_string()));
        }
        if let Some(t) = val.as_timestamp() {
            return Ok(Value::string(t.to_rfc3339()));
        }
        Ok(Value::string(format!("{:?}", val)))
    }

    fn convert_to_date(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(d) = val.as_date() {
            return Ok(Value::date(d));
        }
        if let Some(t) = val.as_timestamp() {
            return Ok(Value::date(t.date_naive()));
        }
        if let Some(s) = val.as_str() {
            if let Ok(d) = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d") {
                return Ok(Value::date(d));
            }
        }
        Err(Error::type_mismatch("Date", &val.data_type().to_string()))
    }

    fn convert_to_datetime(val: &Value) -> Result<Value> {
        use chrono::{NaiveDateTime, TimeZone, Utc};

        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(t) = val.as_timestamp() {
            return Ok(Value::timestamp(t));
        }
        if let Some(d) = val.as_date() {
            let dt = d.and_hms_opt(0, 0, 0).unwrap();
            return Ok(Value::timestamp(Utc.from_utc_datetime(&dt)));
        }
        if let Some(s) = val.as_str() {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S") {
                return Ok(Value::timestamp(Utc.from_utc_datetime(&dt)));
            }
            if let Ok(dt) = NaiveDateTime::parse_from_str(s.trim(), "%Y-%m-%d %H:%M:%S%.f") {
                return Ok(Value::timestamp(Utc.from_utc_datetime(&dt)));
            }
            if let Ok(d) = chrono::NaiveDate::parse_from_str(s.trim(), "%Y-%m-%d") {
                let dt = d.and_hms_opt(0, 0, 0).unwrap();
                return Ok(Value::timestamp(Utc.from_utc_datetime(&dt)));
            }
        }
        Err(Error::type_mismatch(
            "DateTime",
            &val.data_type().to_string(),
        ))
    }

    fn convert_to_decimal(val: &Value, scale: u32) -> Result<Value> {
        use rust_decimal::Decimal;

        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(n) = val.as_numeric() {
            let mut d = n;
            d.rescale(scale);
            return Ok(Value::numeric(d));
        }
        if let Some(i) = val.as_i64() {
            let mut d = Decimal::from(i);
            d.rescale(scale);
            return Ok(Value::numeric(d));
        }
        if let Some(f) = val.as_f64() {
            if let Some(mut d) = Decimal::from_f64_retain(f) {
                d.rescale(scale);
                return Ok(Value::numeric(d));
            }
        }
        if let Some(s) = val.as_str() {
            if let Ok(mut d) = s.trim().parse::<Decimal>() {
                d.rescale(scale);
                return Ok(Value::numeric(d));
            }
        }
        Err(Error::type_mismatch(
            "Decimal",
            &val.data_type().to_string(),
        ))
    }

    fn reinterpret_as_int64(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            return Ok(Value::int64(i));
        }
        if let Some(s) = val.as_str() {
            let bytes = s.as_bytes();
            let mut arr = [0u8; 8];
            let len = bytes.len().min(8);
            arr[..len].copy_from_slice(&bytes[..len]);
            return Ok(Value::int64(i64::from_le_bytes(arr)));
        }
        if let Some(b) = val.as_bytes() {
            let mut arr = [0u8; 8];
            let len = b.len().min(8);
            arr[..len].copy_from_slice(&b[..len]);
            return Ok(Value::int64(i64::from_le_bytes(arr)));
        }
        Err(Error::type_mismatch(
            "reinterpretable value",
            &val.data_type().to_string(),
        ))
    }

    fn reinterpret_as_string(val: &Value) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        if let Some(i) = val.as_i64() {
            let bytes = i.to_le_bytes();
            let s: String = bytes
                .iter()
                .take_while(|&&b| b != 0)
                .map(|&b| b as char)
                .collect();
            return Ok(Value::string(s));
        }
        if let Some(b) = val.as_bytes() {
            let s: String = b
                .iter()
                .take_while(|&&byte| byte != 0)
                .map(|&byte| byte as char)
                .collect();
            return Ok(Value::string(s));
        }
        Err(Error::type_mismatch(
            "reinterpretable value",
            &val.data_type().to_string(),
        ))
    }

    fn get_clickhouse_type_name(val: &Value) -> String {
        if val.is_null() {
            return "Null".to_string();
        }
        match val.data_type() {
            yachtsql_core::types::DataType::Int64 => "Int64".to_string(),
            yachtsql_core::types::DataType::Float64 => "Float64".to_string(),
            yachtsql_core::types::DataType::String => "String".to_string(),
            yachtsql_core::types::DataType::Bool => "Bool".to_string(),
            yachtsql_core::types::DataType::Date => "Date".to_string(),
            yachtsql_core::types::DataType::Timestamp => "DateTime".to_string(),
            yachtsql_core::types::DataType::Numeric(_) => "Decimal".to_string(),
            yachtsql_core::types::DataType::Bytes => "String".to_string(),
            yachtsql_core::types::DataType::Array(_) => "Array".to_string(),
            yachtsql_core::types::DataType::Struct(_) => "Tuple".to_string(),
            yachtsql_core::types::DataType::Json => "JSON".to_string(),
            _ => "Unknown".to_string(),
        }
    }

    fn accurate_cast(val: &Value, type_name: &str) -> Result<Value> {
        if val.is_null() {
            return Ok(Value::null());
        }
        match type_name.to_uppercase().as_str() {
            "INT8" => Self::accurate_cast_to_int8(val),
            "INT16" => Self::accurate_cast_to_int16(val),
            "INT32" => Self::accurate_cast_to_int32(val),
            "INT64" => Self::convert_to_int64(val),
            "UINT8" => Self::accurate_cast_to_uint8(val),
            "UINT16" => Self::accurate_cast_to_uint16(val),
            "UINT32" => Self::accurate_cast_to_uint32(val),
            "UINT64" => Self::convert_to_uint64(val),
            "FLOAT32" => Self::convert_to_float32(val),
            "FLOAT64" => Self::convert_to_float64(val),
            "STRING" => Self::convert_to_string(val),
            "DATE" => Self::convert_to_date(val),
            "DATETIME" => Self::convert_to_datetime(val),
            _ => Err(Error::unsupported_feature(format!(
                "accurateCast to type: {}",
                type_name
            ))),
        }
    }

    fn accurate_cast_to_int8(val: &Value) -> Result<Value> {
        if let Some(i) = val.as_i64() {
            if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                return Ok(Value::int64(i as i8 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for Int8",
                i
            )));
        }
        if let Some(f) = val.as_f64() {
            let i = f as i64;
            if i >= i8::MIN as i64 && i <= i8::MAX as i64 {
                return Ok(Value::int64(f as i8 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for Int8",
                f
            )));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i8>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("Int8", &val.data_type().to_string()))
    }

    fn accurate_cast_to_int16(val: &Value) -> Result<Value> {
        if let Some(i) = val.as_i64() {
            if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                return Ok(Value::int64(i as i16 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for Int16",
                i
            )));
        }
        if let Some(f) = val.as_f64() {
            let i = f as i64;
            if i >= i16::MIN as i64 && i <= i16::MAX as i64 {
                return Ok(Value::int64(f as i16 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for Int16",
                f
            )));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i16>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("Int16", &val.data_type().to_string()))
    }

    fn accurate_cast_to_int32(val: &Value) -> Result<Value> {
        if let Some(i) = val.as_i64() {
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                return Ok(Value::int64(i as i32 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for Int32",
                i
            )));
        }
        if let Some(f) = val.as_f64() {
            let i = f as i64;
            if i >= i32::MIN as i64 && i <= i32::MAX as i64 {
                return Ok(Value::int64(f as i32 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for Int32",
                f
            )));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<i32>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("Int32", &val.data_type().to_string()))
    }

    fn accurate_cast_to_uint8(val: &Value) -> Result<Value> {
        if let Some(i) = val.as_i64() {
            if i >= 0 && i <= u8::MAX as i64 {
                return Ok(Value::int64(i as u8 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for UInt8",
                i
            )));
        }
        if let Some(f) = val.as_f64() {
            let i = f as i64;
            if i >= 0 && i <= u8::MAX as i64 {
                return Ok(Value::int64(f as u8 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for UInt8",
                f
            )));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u8>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt8", &val.data_type().to_string()))
    }

    fn accurate_cast_to_uint16(val: &Value) -> Result<Value> {
        if let Some(i) = val.as_i64() {
            if i >= 0 && i <= u16::MAX as i64 {
                return Ok(Value::int64(i as u16 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for UInt16",
                i
            )));
        }
        if let Some(f) = val.as_f64() {
            let i = f as i64;
            if i >= 0 && i <= u16::MAX as i64 {
                return Ok(Value::int64(f as u16 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for UInt16",
                f
            )));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u16>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt16", &val.data_type().to_string()))
    }

    fn accurate_cast_to_uint32(val: &Value) -> Result<Value> {
        if let Some(i) = val.as_i64() {
            if i >= 0 && i <= u32::MAX as i64 {
                return Ok(Value::int64(i as u32 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for UInt32",
                i
            )));
        }
        if let Some(f) = val.as_f64() {
            let i = f as i64;
            if i >= 0 && i <= u32::MAX as i64 {
                return Ok(Value::int64(f as u32 as i64));
            }
            return Err(Error::invalid_query(format!(
                "Value {} is out of range for UInt32",
                f
            )));
        }
        if let Some(s) = val.as_str() {
            if let Ok(i) = s.trim().parse::<u32>() {
                return Ok(Value::int64(i as i64));
            }
        }
        Err(Error::type_mismatch("UInt32", &val.data_type().to_string()))
    }

    fn parse_datetime(val: &Value, fmt: &str) -> Result<Value> {
        use chrono::{NaiveDateTime, TimeZone, Utc};

        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &val.data_type().to_string()))?;
        let strftime_fmt = Self::clickhouse_to_strftime(fmt);
        let dt = NaiveDateTime::parse_from_str(s.trim(), &strftime_fmt)
            .map_err(|e| Error::invalid_query(format!("Failed to parse datetime: {}", e)))?;
        Ok(Value::timestamp(Utc.from_utc_datetime(&dt)))
    }

    fn clickhouse_to_strftime(fmt: &str) -> String {
        fmt.to_string()
    }

    fn parse_datetime_best_effort(val: &Value) -> Result<Value> {
        use chrono::{NaiveDate, NaiveDateTime, TimeZone, Utc};

        if val.is_null() {
            return Ok(Value::null());
        }
        let s = val
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &val.data_type().to_string()))?;
        let s = s.trim();

        let formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M:%S%.f",
            "%Y/%m/%d %H:%M:%S",
            "%d-%m-%Y %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
            "%b %d, %Y %I:%M %p",
            "%B %d, %Y %I:%M %p",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
        ];

        for fmt in &formats {
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, fmt) {
                return Ok(Value::timestamp(Utc.from_utc_datetime(&dt)));
            }
        }

        let date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y", "%d/%m/%Y"];
        for fmt in &date_formats {
            if let Ok(d) = NaiveDate::parse_from_str(s, fmt) {
                let dt = d.and_hms_opt(0, 0, 0).unwrap();
                return Ok(Value::timestamp(Utc.from_utc_datetime(&dt)));
            }
        }

        Err(Error::invalid_query(format!(
            "Could not parse datetime: {}",
            s
        )))
    }

    fn string_position(haystack: &Value, needle: &Value, case_insensitive: bool) -> Result<Value> {
        if haystack.is_null() || needle.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let n = needle
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &needle.data_type().to_string()))?;

        let pos = if case_insensitive {
            h.to_lowercase().find(&n.to_lowercase())
        } else {
            h.find(n)
        };

        Ok(Value::int64(pos.map(|p| p as i64 + 1).unwrap_or(0)))
    }

    fn count_substrings(haystack: &Value, needle: &Value, case_insensitive: bool) -> Result<Value> {
        if haystack.is_null() || needle.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let n = needle
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &needle.data_type().to_string()))?;

        if n.is_empty() {
            return Ok(Value::int64(0));
        }

        let count = if case_insensitive {
            h.to_lowercase().matches(&n.to_lowercase()).count()
        } else {
            h.matches(n).count()
        };

        Ok(Value::int64(count as i64))
    }

    fn regex_match(haystack: &Value, pattern: &Value) -> Result<Value> {
        if haystack.is_null() || pattern.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let p = pattern
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &pattern.data_type().to_string()))?;

        let re = regex::Regex::new(p)
            .map_err(|e| Error::invalid_query(format!("Invalid regex: {}", e)))?;
        Ok(Value::int64(if re.is_match(h) { 1 } else { 0 }))
    }

    fn multi_search_any(haystack: &Value, needles: &Value, is_regex: bool) -> Result<Value> {
        if haystack.is_null() || needles.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let arr = needles
            .as_array()
            .ok_or_else(|| Error::type_mismatch("Array", &needles.data_type().to_string()))?;

        for needle in arr {
            if let Some(n) = needle.as_str() {
                let found = if is_regex {
                    regex::Regex::new(n)
                        .map(|re| re.is_match(h))
                        .unwrap_or(false)
                } else {
                    h.contains(n)
                };
                if found {
                    return Ok(Value::int64(1));
                }
            }
        }
        Ok(Value::int64(0))
    }

    fn multi_search_first_index(
        haystack: &Value,
        needles: &Value,
        is_regex: bool,
    ) -> Result<Value> {
        if haystack.is_null() || needles.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let arr = needles
            .as_array()
            .ok_or_else(|| Error::type_mismatch("Array", &needles.data_type().to_string()))?;

        let mut first_pos = usize::MAX;
        let mut first_idx = 0;

        for (idx, needle) in arr.iter().enumerate() {
            if let Some(n) = needle.as_str() {
                let pos = if is_regex {
                    regex::Regex::new(n)
                        .ok()
                        .and_then(|re| re.find(h).map(|m| m.start()))
                } else {
                    h.find(n)
                };
                if let Some(p) = pos {
                    if p < first_pos {
                        first_pos = p;
                        first_idx = idx + 1;
                    }
                }
            }
        }
        Ok(Value::int64(first_idx as i64))
    }

    fn multi_search_first_position(haystack: &Value, needles: &Value) -> Result<Value> {
        if haystack.is_null() || needles.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let arr = needles
            .as_array()
            .ok_or_else(|| Error::type_mismatch("Array", &needles.data_type().to_string()))?;

        let mut first_pos = usize::MAX;

        for needle in arr {
            if let Some(n) = needle.as_str() {
                if let Some(pos) = h.find(n) {
                    first_pos = first_pos.min(pos);
                }
            }
        }

        Ok(Value::int64(if first_pos == usize::MAX {
            0
        } else {
            first_pos as i64 + 1
        }))
    }

    fn multi_search_all_positions(haystack: &Value, needles: &Value) -> Result<Value> {
        if haystack.is_null() || needles.is_null() {
            return Ok(Value::array(vec![]));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let arr = needles
            .as_array()
            .ok_or_else(|| Error::type_mismatch("Array", &needles.data_type().to_string()))?;

        let mut result = Vec::new();
        for needle in arr {
            if let Some(n) = needle.as_str() {
                let positions: Vec<Value> = h
                    .match_indices(n)
                    .map(|(pos, _)| Value::int64(pos as i64 + 1))
                    .collect();
                result.push(Value::array(positions));
            } else {
                result.push(Value::array(vec![]));
            }
        }
        Ok(Value::array(result))
    }

    fn multi_match_all_indices(haystack: &Value, patterns: &Value) -> Result<Value> {
        if haystack.is_null() || patterns.is_null() {
            return Ok(Value::array(vec![]));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let arr = patterns
            .as_array()
            .ok_or_else(|| Error::type_mismatch("Array", &patterns.data_type().to_string()))?;

        let mut result = Vec::new();
        for (idx, pattern) in arr.iter().enumerate() {
            if let Some(p) = pattern.as_str() {
                if let Ok(re) = regex::Regex::new(p) {
                    if re.is_match(h) {
                        result.push(Value::int64((idx + 1) as i64));
                    }
                }
            }
        }
        Ok(Value::array(result))
    }

    fn regex_extract(haystack: &Value, pattern: &Value) -> Result<Value> {
        if haystack.is_null() || pattern.is_null() {
            return Ok(Value::string(String::new()));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let p = pattern
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &pattern.data_type().to_string()))?;

        let re = regex::Regex::new(p)
            .map_err(|e| Error::invalid_query(format!("Invalid regex: {}", e)))?;
        let result = re
            .find(h)
            .map(|m| m.as_str().to_string())
            .unwrap_or_default();
        Ok(Value::string(result))
    }

    fn extract_groups(haystack: &Value, pattern: &Value) -> Result<Value> {
        if haystack.is_null() || pattern.is_null() {
            return Ok(Value::array(vec![]));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let p = pattern
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &pattern.data_type().to_string()))?;

        let re = regex::Regex::new(p)
            .map_err(|e| Error::invalid_query(format!("Invalid regex: {}", e)))?;
        let mut result = Vec::new();
        if let Some(caps) = re.captures(h) {
            for i in 1..caps.len() {
                if let Some(m) = caps.get(i) {
                    result.push(Value::string(m.as_str().to_string()));
                }
            }
        }
        Ok(Value::array(result))
    }

    fn count_matches(haystack: &Value, pattern: &Value) -> Result<Value> {
        if haystack.is_null() || pattern.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let p = pattern
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &pattern.data_type().to_string()))?;

        let re = regex::Regex::new(p)
            .map_err(|e| Error::invalid_query(format!("Invalid regex: {}", e)))?;
        Ok(Value::int64(re.find_iter(h).count() as i64))
    }

    fn has_token(haystack: &Value, token: &Value, case_insensitive: bool) -> Result<Value> {
        if haystack.is_null() || token.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let t = token
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &token.data_type().to_string()))?;

        let words: Vec<&str> = h.split_whitespace().collect();
        let found = if case_insensitive {
            words.iter().any(|w| w.eq_ignore_ascii_case(t))
        } else {
            words.contains(&t)
        };
        Ok(Value::int64(if found { 1 } else { 0 }))
    }

    fn starts_with(haystack: &Value, prefix: &Value) -> Result<Value> {
        if haystack.is_null() || prefix.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let p = prefix
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &prefix.data_type().to_string()))?;
        Ok(Value::int64(if h.starts_with(p) { 1 } else { 0 }))
    }

    fn ends_with(haystack: &Value, suffix: &Value) -> Result<Value> {
        if haystack.is_null() || suffix.is_null() {
            return Ok(Value::int64(0));
        }
        let h = haystack
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &haystack.data_type().to_string()))?;
        let s = suffix
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &suffix.data_type().to_string()))?;
        Ok(Value::int64(if h.ends_with(s) { 1 } else { 0 }))
    }

    fn ngram_distance(str1: &Value, str2: &Value) -> Result<Value> {
        if str1.is_null() || str2.is_null() {
            return Ok(Value::float64(1.0));
        }
        let s1 = str1
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &str1.data_type().to_string()))?;
        let s2 = str2
            .as_str()
            .ok_or_else(|| Error::type_mismatch("String", &str2.data_type().to_string()))?;

        use std::collections::HashSet;

        fn get_trigrams(s: &str) -> HashSet<String> {
            let chars: Vec<char> = s.chars().collect();
            if chars.len() < 3 {
                let mut set = HashSet::new();
                set.insert(s.to_string());
                return set;
            }
            chars.windows(3).map(|w| w.iter().collect()).collect()
        }

        let t1 = get_trigrams(s1);
        let t2 = get_trigrams(s2);
        let intersection = t1.intersection(&t2).count();
        let union = t1.union(&t2).count();

        if union == 0 {
            return Ok(Value::float64(0.0));
        }

        Ok(Value::float64(1.0 - (intersection as f64 / union as f64)))
    }
}
