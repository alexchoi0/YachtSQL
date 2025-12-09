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
                let (x, y) = if let Some(s) = point.as_struct() {
                    let x = s.values().next().and_then(|v| v.as_f64()).unwrap_or(0.0);
                    let y = s.values().nth(1).and_then(|v| v.as_f64()).unwrap_or(0.0);
                    (x, y)
                } else {
                    return Err(Error::type_mismatch(
                        "TUPLE",
                        &point.data_type().to_string(),
                    ));
                };
                let polygon_points: Vec<(f64, f64)> = if let Some(arr) = polygon.as_array() {
                    arr.iter()
                        .filter_map(|v| {
                            if let Some(s) = v.as_struct() {
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
                        "ARRAY",
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
                if let Some(s) = value.as_str() {
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
                let s = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
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
                let num = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_i64()
                    .ok_or_else(|| Error::type_mismatch("INT64", "other"))?;
                yachtsql_functions::network::ipv4_to_ipv6(num)
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
                let addr = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
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
                let addr = Self::evaluate_expr(&args[0], batch, row_idx)?
                    .as_str()
                    .ok_or_else(|| Error::type_mismatch("STRING", "other"))?
                    .to_string();
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
                        Some(bcast) => Ok(Value::inet(
                            yachtsql_core::types::network::InetAddr::new(bcast),
                        )),
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
                    match yachtsql_core::types::network::CidrAddr::new(cidr.network, new_len) {
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

            _ => Err(Error::unsupported_feature(format!(
                "Unknown custom function: {}",
                name
            ))),
        }
    }
}
