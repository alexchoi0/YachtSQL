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
