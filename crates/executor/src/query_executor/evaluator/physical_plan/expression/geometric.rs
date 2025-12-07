use yachtsql_core::error::Result;
use yachtsql_core::types::Value;
use yachtsql_functions::geometric;
use yachtsql_optimizer::expr::Expr;

use super::super::ProjectionWithExprExec;
use crate::Table;

impl ProjectionWithExprExec {
    pub(super) fn evaluate_geometric_function(
        name: &str,
        args: &[Expr],
        batch: &Table,
        row_idx: usize,
    ) -> Result<Value> {
        match name {
            "POINT" => Self::eval_point_constructor(args, batch, row_idx),
            "BOX" => Self::eval_box_constructor(args, batch, row_idx),
            "CIRCLE" => Self::eval_circle_constructor(args, batch, row_idx),
            "AREA" => Self::eval_area(args, batch, row_idx),
            "CENTER" => Self::eval_center(args, batch, row_idx),
            "DIAMETER" => Self::eval_diameter(args, batch, row_idx),
            "RADIUS" => Self::eval_radius(args, batch, row_idx),
            "WIDTH" => Self::eval_width(args, batch, row_idx),
            "HEIGHT" => Self::eval_height(args, batch, row_idx),
            "DISTANCE" => Self::eval_distance(args, batch, row_idx),
            "CONTAINS" => Self::eval_contains(args, batch, row_idx),
            "CONTAINED_BY" => Self::eval_contained_by(args, batch, row_idx),
            "OVERLAPS" => Self::eval_overlaps(args, batch, row_idx),
            _ => Err(crate::error::Error::invalid_query(format!(
                "Unknown geometric function: {}",
                name
            ))),
        }
    }

    fn eval_point_constructor(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "POINT requires exactly 2 arguments (x, y)",
            ));
        }
        let x = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let y = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::point_constructor(&x, &y)
    }

    fn eval_box_constructor(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "BOX requires exactly 2 arguments (point1, point2)",
            ));
        }
        let p1 = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let p2 = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::box_constructor(&p1, &p2)
    }

    fn eval_circle_constructor(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "CIRCLE requires exactly 2 arguments (center, radius)",
            ));
        }
        let center = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let radius = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::circle_constructor(&center, &radius)
    }

    fn eval_area(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "AREA requires exactly 1 argument",
            ));
        }
        let shape = Self::evaluate_expr(&args[0], batch, row_idx)?;
        geometric::area(&shape)
    }

    fn eval_center(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "CENTER requires exactly 1 argument",
            ));
        }
        let shape = Self::evaluate_expr(&args[0], batch, row_idx)?;
        geometric::center(&shape)
    }

    fn eval_diameter(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "DIAMETER requires exactly 1 argument",
            ));
        }
        let circle = Self::evaluate_expr(&args[0], batch, row_idx)?;
        geometric::diameter(&circle)
    }

    fn eval_radius(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "RADIUS requires exactly 1 argument",
            ));
        }
        let circle = Self::evaluate_expr(&args[0], batch, row_idx)?;
        geometric::radius(&circle)
    }

    fn eval_width(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "WIDTH requires exactly 1 argument",
            ));
        }
        let box_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        geometric::width(&box_val)
    }

    fn eval_height(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 1 {
            return Err(crate::error::Error::invalid_query(
                "HEIGHT requires exactly 1 argument",
            ));
        }
        let box_val = Self::evaluate_expr(&args[0], batch, row_idx)?;
        geometric::height(&box_val)
    }

    fn eval_distance(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "DISTANCE requires exactly 2 arguments",
            ));
        }
        let a = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::distance(&a, &b)
    }

    fn eval_contains(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "CONTAINS requires exactly 2 arguments",
            ));
        }
        let container = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let contained = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::contains(&container, &contained)
    }

    fn eval_contained_by(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "CONTAINED_BY requires exactly 2 arguments",
            ));
        }
        let inner = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let outer = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::contained_by(&inner, &outer)
    }

    fn eval_overlaps(args: &[Expr], batch: &Table, row_idx: usize) -> Result<Value> {
        if args.len() != 2 {
            return Err(crate::error::Error::invalid_query(
                "OVERLAPS requires exactly 2 arguments",
            ));
        }
        let a = Self::evaluate_expr(&args[0], batch, row_idx)?;
        let b = Self::evaluate_expr(&args[1], batch, row_idx)?;
        geometric::overlaps(&a, &b)
    }
}
