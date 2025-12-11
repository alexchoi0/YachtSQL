use sqlparser::ast;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::{BinaryOp, Expr, LiteralValue, StructLiteralField};
use yachtsql_ir::plan::{LogicalPlan, PlanNode};

use super::super::LogicalPlanBuilder;

impl LogicalPlanBuilder {
    pub(super) fn convert_compound_identifier_expr(&self, idents: &[ast::Ident]) -> Result<Expr> {
        match idents.len() {
            0 => Err(Error::unsupported_feature(
                "Empty compound identifier".to_string(),
            )),
            1 => Ok(Expr::column(idents[0].value.clone())),
            2 => self.convert_two_part_identifier(idents),
            _ => self.build_struct_field_access_expr(idents),
        }
    }

    pub(super) fn convert_two_part_identifier(&self, idents: &[ast::Ident]) -> Result<Expr> {
        debug_assert_eq!(idents.len(), 2, "Expected exactly 2 identifiers");

        if idents[0].value.eq_ignore_ascii_case("excluded") {
            return Ok(Expr::Excluded {
                column: idents[1].value.clone(),
            });
        }

        Ok(Expr::qualified_column(
            idents[0].value.clone(),
            idents[1].value.clone(),
        ))
    }

    pub(super) fn build_struct_field_access_expr(&self, idents: &[ast::Ident]) -> Result<Expr> {
        if idents.len() == 3 {
            let schema_name = &idents[0].value;
            let table_name = &idents[1].value;
            let column_name = &idents[2].value;

            if self.is_current_alias(schema_name) {
                let base = Expr::qualified_column(schema_name.clone(), table_name.clone());
                return Ok(Expr::StructFieldAccess {
                    expr: Box::new(base),
                    field: column_name.clone(),
                });
            }

            let qualified_table = format!("{}.{}", schema_name, table_name);
            return Ok(Expr::qualified_column(qualified_table, column_name.clone()));
        }

        let (mut current_expr, start_idx) = if self.is_current_alias(&idents[0].value) {
            (
                Expr::qualified_column(idents[0].value.clone(), idents[1].value.clone()),
                2,
            )
        } else {
            (Expr::column(idents[0].value.clone()), 1)
        };

        for ident in &idents[start_idx..] {
            current_expr = Expr::StructFieldAccess {
                expr: Box::new(current_expr),
                field: ident.value.clone(),
            };
        }

        Ok(current_expr)
    }

    pub(super) fn build_struct_literal_fields(
        &self,
        values: &[ast::Expr],
        fields: &[ast::StructField],
    ) -> Result<Vec<StructLiteralField>> {
        if !fields.is_empty() {
            fields
                .iter()
                .enumerate()
                .zip(values.iter())
                .map(|((idx, field_def), value_expr)| {
                    let name = field_def
                        .field_name
                        .as_ref()
                        .map(|ident| ident.value.clone())
                        .unwrap_or_else(|| Self::make_struct_field_name(idx));
                    let expr = self.sql_expr_to_expr(value_expr)?;
                    let declared_type =
                        Some(Self::sql_datatype_to_datatype(&field_def.field_type)?);
                    Ok(StructLiteralField {
                        name,
                        expr,
                        declared_type,
                    })
                })
                .collect()
        } else {
            values
                .iter()
                .enumerate()
                .map(|(idx, value_expr)| match value_expr {
                    ast::Expr::Named { expr, name } => Ok(StructLiteralField {
                        name: name.value.clone(),
                        expr: self.sql_expr_to_expr(expr)?,
                        declared_type: None,
                    }),
                    _ => Ok(StructLiteralField {
                        name: Self::make_struct_field_name(idx),
                        expr: self.sql_expr_to_expr(value_expr)?,
                        declared_type: None,
                    }),
                })
                .collect()
        }
    }

    pub(super) fn make_struct_field_name(index: usize) -> String {
        format!("unnamed_field_{}", index + 1)
    }

    pub(crate) fn build_using_condition(&self, cols: &[ast::Ident]) -> Result<Expr> {
        if cols.is_empty() {
            return Ok(Expr::Literal(LiteralValue::Boolean(true)));
        }

        let mut condition = Expr::binary_op(
            Expr::column(cols[0].value.clone()),
            BinaryOp::Equal,
            Expr::column(cols[0].value.clone()),
        );

        for col in &cols[1..] {
            condition = Expr::binary_op(
                condition,
                BinaryOp::And,
                Expr::binary_op(
                    Expr::column(col.value.clone()),
                    BinaryOp::Equal,
                    Expr::column(col.value.clone()),
                ),
            );
        }

        Ok(condition)
    }

    pub(crate) fn build_natural_join_condition(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<Expr> {
        let left_cols = self.get_column_names(left)?;
        let right_cols = self.get_column_names(right)?;

        let common_cols: Vec<String> = left_cols
            .iter()
            .filter(|col| right_cols.contains(col))
            .cloned()
            .collect();

        if common_cols.is_empty() {
            return Ok(Expr::Literal(LiteralValue::Boolean(true)));
        }

        let mut condition = Expr::binary_op(
            Expr::column(common_cols[0].clone()),
            BinaryOp::Equal,
            Expr::column(common_cols[0].clone()),
        );

        for col in &common_cols[1..] {
            condition = Expr::binary_op(
                condition,
                BinaryOp::And,
                Expr::binary_op(
                    Expr::column(col.clone()),
                    BinaryOp::Equal,
                    Expr::column(col.clone()),
                ),
            );
        }

        Ok(condition)
    }

    pub(crate) fn get_common_columns(
        &self,
        left: &LogicalPlan,
        right: &LogicalPlan,
    ) -> Result<Vec<String>> {
        let left_cols = self.get_column_names(left)?;
        let right_cols = self.get_column_names(right)?;

        Ok(left_cols
            .iter()
            .filter(|col| right_cols.contains(col))
            .cloned()
            .collect())
    }

    pub(super) fn get_column_names(&self, plan: &LogicalPlan) -> Result<Vec<String>> {
        match plan.root.as_ref() {
            PlanNode::Scan {
                table_name,
                projection,
                ..
            } => {
                if let Some(proj) = projection {
                    Ok(proj.clone())
                } else if let Some(storage) = &self.storage {
                    let storage_guard = storage.borrow();
                    let parts: Vec<&str> = table_name.split('.').collect();
                    let (dataset_id, table_id) = match parts.len() {
                        1 => ("default", parts[0]),
                        2 => (parts[0], parts[1]),
                        3 => (parts[1], parts[2]),
                        _ => {
                            return Err(Error::invalid_query(format!(
                                "Invalid table name: {}",
                                table_name
                            )));
                        }
                    };

                    let dataset = storage_guard.get_dataset(dataset_id).ok_or_else(|| {
                        Error::invalid_query(format!("Dataset not found: {}", dataset_id))
                    })?;
                    let table = dataset
                        .get_table(table_id)
                        .ok_or_else(|| Error::table_not_found(table_name.clone()))?;
                    let cols: Vec<String> = table
                        .schema()
                        .fields()
                        .iter()
                        .map(|f| f.name.clone())
                        .collect();
                    Ok(cols)
                } else {
                    Err(Error::unsupported_feature(
                        "NATURAL JOIN requires storage access or explicit column information"
                            .to_string(),
                    ))
                }
            }
            PlanNode::Projection { expressions, .. } => {
                let mut cols = Vec::new();
                for (expr, alias) in expressions {
                    if let Some(name) = alias {
                        cols.push(name.clone());
                    } else if let Expr::Column { name, .. } = expr {
                        cols.push(name.clone());
                    }
                }
                Ok(cols)
            }
            PlanNode::Join { .. } => Err(Error::unsupported_feature(
                "NATURAL JOIN on nested joins not yet supported".to_string(),
            )),
            _ => Err(Error::unsupported_feature(
                "Cannot determine columns for NATURAL JOIN".to_string(),
            )),
        }
    }
}
