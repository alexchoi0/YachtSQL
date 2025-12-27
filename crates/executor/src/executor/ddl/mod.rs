mod io;

use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AlterColumnAction, AlterTableOp, ColumnDef, FunctionArg, FunctionBody, ProcedureArg,
};
use yachtsql_storage::{Field, FieldMode, Schema, Table, TableSchemaOps};

use super::PlanExecutor;
use crate::catalog::{ColumnDefault, UserFunction, UserProcedure};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::PhysicalPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
        query: Option<&PhysicalPlan>,
    ) -> Result<Table> {
        if self.catalog.get_table(table_name).is_some() {
            if or_replace {
                self.catalog.drop_table(table_name)?;
            } else if if_not_exists {
                return Ok(Table::empty(Schema::new()));
            } else {
                return Err(Error::InvalidQuery(format!(
                    "Table {} already exists",
                    table_name
                )));
            }
        }

        let schema_collation = table_name
            .find('.')
            .map(|dot_pos| &table_name[..dot_pos])
            .and_then(|schema_name| {
                self.catalog
                    .get_schema_option(schema_name, "default_collate")
            });

        if let Some(query_plan) = query {
            let result = self.execute_plan(query_plan)?;
            let schema = if let Some(plan_schema) = query_plan.schema()
                && !plan_schema.fields.is_empty()
            {
                super::plan_schema_to_schema(plan_schema)
            } else {
                result.schema().clone()
            };
            let values: Vec<Vec<Value>> = result
                .rows()?
                .into_iter()
                .map(|r| r.values().to_vec())
                .collect();
            let table = Table::from_values(schema.clone(), values)?;
            self.catalog.insert_table(table_name, table)?;
            return Ok(Table::empty(schema));
        }

        let mut schema = Schema::new();
        let mut defaults = Vec::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            let mut field = Field::new(&col.name, col.data_type.clone(), mode);
            if let Some(ref collation) = col.collation {
                field = field.with_collation(collation);
            } else if col.data_type == DataType::String
                && let Some(ref collation) = schema_collation
            {
                field = field.with_collation(collation);
            }
            schema.add_field(field);
            if let Some(ref default_expr) = col.default_value {
                defaults.push(ColumnDefault {
                    column_name: col.name.clone(),
                    default_expr: default_expr.clone(),
                });
            }
        }

        let table = Table::empty(schema);
        self.catalog.insert_table(table_name, table)?;
        if !defaults.is_empty() {
            self.catalog.set_table_defaults(table_name, defaults);
        }

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_tables(
        &mut self,
        table_names: &[String],
        if_exists: bool,
    ) -> Result<Table> {
        for table_name in table_names {
            if self.catalog.get_table(table_name).is_none() {
                if if_exists {
                    continue;
                }
                return Err(Error::TableNotFound(table_name.to_string()));
            }
            self.catalog.drop_table(table_name)?;
        }
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_table(
        &mut self,
        table_name: &str,
        operation: &AlterTableOp,
        if_exists: bool,
    ) -> Result<Table> {
        let table_opt = self.catalog.get_table(table_name);
        if table_opt.is_none() {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::TableNotFound(table_name.to_string()));
        }
        let _table = table_opt.unwrap();

        match operation {
            AlterTableOp::AddColumn {
                column,
                if_not_exists,
            } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

                if *if_not_exists && table.schema().field(&column.name).is_some() {
                    return Ok(Table::empty(Schema::new()));
                }

                let mode = if column.nullable {
                    FieldMode::Nullable
                } else {
                    FieldMode::Required
                };
                let field = Field::new(&column.name, column.data_type.clone(), mode);

                let default_value = match &column.default_value {
                    Some(default_expr) => {
                        let empty_schema = yachtsql_storage::Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema);
                        let empty_record = yachtsql_storage::Record::new();
                        evaluator.evaluate(default_expr, &empty_record).ok()
                    }
                    None => None,
                };

                table.add_column(field, default_value)?;

                if let Some(ref default_expr) = column.default_value {
                    let mut defaults = self
                        .catalog
                        .get_table_defaults(table_name)
                        .cloned()
                        .unwrap_or_default();
                    defaults.push(ColumnDefault {
                        column_name: column.name.clone(),
                        default_expr: default_expr.clone(),
                    });
                    self.catalog.set_table_defaults(table_name, defaults);
                }

                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::DropColumn { name, if_exists } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                if *if_exists && table.schema().field(name).is_none() {
                    return Ok(Table::empty(Schema::new()));
                }
                table.drop_column(name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::RenameColumn { old_name, new_name } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                table.rename_column(old_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::RenameTable { new_name } => {
                self.catalog.rename_table(table_name, new_name)?;
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::SetOptions { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::AlterColumn { name, action } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
                match action {
                    AlterColumnAction::SetNotNull => {
                        table.set_column_not_null(name)?;
                    }
                    AlterColumnAction::DropNotNull => {
                        table.set_column_nullable(name)?;
                    }
                    AlterColumnAction::SetDefault { default } => {
                        let empty_schema = yachtsql_storage::Schema::new();
                        let evaluator = IrEvaluator::new(&empty_schema);
                        let empty_record = yachtsql_storage::Record::new();
                        let default_value = evaluator.evaluate(default, &empty_record)?;
                        table.set_column_default(name, default_value)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .cloned()
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        defaults.push(ColumnDefault {
                            column_name: name.clone(),
                            default_expr: default.clone(),
                        });
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    AlterColumnAction::DropDefault => {
                        table.drop_column_default(name)?;

                        let mut defaults = self
                            .catalog
                            .get_table_defaults(table_name)
                            .cloned()
                            .unwrap_or_default();
                        defaults.retain(|d| d.column_name.to_uppercase() != name.to_uppercase());
                        self.catalog.set_table_defaults(table_name, defaults);
                    }
                    AlterColumnAction::SetDataType { data_type } => {
                        table.set_column_data_type(name, data_type.clone())?;
                    }
                    AlterColumnAction::SetOptions { collation } => {
                        if let Some(collate) = collation {
                            table.set_column_collation(name, collate.clone())?;
                        }
                    }
                }
                Ok(Table::empty(Schema::new()))
            }
            AlterTableOp::AddConstraint { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::DropConstraint { .. } => Ok(Table::empty(Schema::new())),
            AlterTableOp::DropPrimaryKey => Ok(Table::empty(Schema::new())),
        }
    }

    pub fn execute_truncate(&mut self, table_name: &str) -> Result<Table> {
        let table = self
            .catalog
            .get_table_mut(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
        table.clear();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_view(
        &mut self,
        name: &str,
        query_sql: &str,
        column_aliases: &[String],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        self.catalog.create_view(
            name,
            query_sql.to_string(),
            column_aliases.to_vec(),
            or_replace,
            if_not_exists,
        )?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_view(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        self.catalog.drop_view(name, if_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_schema(
        &mut self,
        name: &str,
        if_not_exists: bool,
        or_replace: bool,
    ) -> Result<Table> {
        if or_replace && self.catalog.schema_exists(name) {
            self.catalog.drop_schema(name, true, true)?;
        }
        self.catalog.create_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_schema(
        &mut self,
        name: &str,
        if_exists: bool,
        cascade: bool,
    ) -> Result<Table> {
        self.catalog.drop_schema(name, if_exists, cascade)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_undrop_schema(&mut self, name: &str, if_not_exists: bool) -> Result<Table> {
        self.catalog.undrop_schema(name, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_alter_schema(
        &mut self,
        name: &str,
        options: &[(String, String)],
    ) -> Result<Table> {
        let option_map: std::collections::HashMap<String, String> =
            options.iter().cloned().collect();
        self.catalog.alter_schema_options(name, option_map)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_function(
        &mut self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
        is_aggregate: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.function_exists(name) {
            return Ok(Table::empty(Schema::new()));
        }
        let func = UserFunction {
            name: name.to_string(),
            parameters: args.to_vec(),
            return_type: return_type.clone(),
            body: body.clone(),
            is_temporary: is_temp,
            is_aggregate,
        };
        self.catalog.create_function(func, or_replace)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_function(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.function_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!("Function not found: {}", name)));
        }
        self.catalog.drop_function(name)?;
        self.refresh_user_functions();
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_snapshot(
        &mut self,
        snapshot_name: &str,
        source_name: &str,
        if_not_exists: bool,
    ) -> Result<Table> {
        if if_not_exists && self.catalog.get_table(snapshot_name).is_some() {
            return Ok(Table::empty(Schema::new()));
        }

        let source_table = self
            .catalog
            .get_table(source_name)
            .ok_or_else(|| Error::TableNotFound(source_name.to_string()))?;

        let snapshot = source_table.clone();
        self.catalog.insert_table(snapshot_name, snapshot)?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_snapshot(&mut self, snapshot_name: &str, if_exists: bool) -> Result<Table> {
        if if_exists && self.catalog.get_table(snapshot_name).is_none() {
            return Ok(Table::empty(Schema::new()));
        }

        self.catalog
            .drop_table(snapshot_name)
            .map_err(|_| Error::TableNotFound(snapshot_name.to_string()))?;

        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_create_procedure(
        &mut self,
        name: &str,
        args: &[ProcedureArg],
        body: &[PhysicalPlan],
        or_replace: bool,
        if_not_exists: bool,
    ) -> Result<Table> {
        let body_plans = body.iter().map(executor_plan_to_logical_plan).collect();
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: body_plans,
        };
        self.catalog
            .create_procedure(proc, or_replace, if_not_exists)?;
        Ok(Table::empty(Schema::new()))
    }

    pub fn execute_drop_procedure(&mut self, name: &str, if_exists: bool) -> Result<Table> {
        if !self.catalog.procedure_exists(name) {
            if if_exists {
                return Ok(Table::empty(Schema::new()));
            }
            return Err(Error::InvalidQuery(format!(
                "Procedure not found: {}",
                name
            )));
        }
        self.catalog.drop_procedure(name)?;
        Ok(Table::empty(Schema::new()))
    }
}

fn executor_plan_to_logical_plan(plan: &PhysicalPlan) -> yachtsql_ir::LogicalPlan {
    use yachtsql_ir::LogicalPlan;
    match plan {
        PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
            ..
        } => LogicalPlan::Scan {
            table_name: table_name.clone(),
            schema: schema.clone(),
            projection: projection.clone(),
        },
        PhysicalPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(executor_plan_to_logical_plan(input)),
            predicate: predicate.clone(),
        },
        PhysicalPlan::Project {
            input,
            expressions,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(executor_plan_to_logical_plan(input)),
            expressions: expressions.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            schema,
            ..
        } => LogicalPlan::Join {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            join_type: *join_type,
            condition: condition.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::CrossJoin {
            left,
            right,
            schema,
            ..
        } => LogicalPlan::Join {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            join_type: yachtsql_ir::JoinType::Cross,
            condition: None,
            schema: schema.clone(),
        },
        PhysicalPlan::HashJoin {
            left,
            right,
            join_type,
            left_keys,
            right_keys,
            schema,
            ..
        } => {
            let condition = if left_keys.len() == 1 {
                Some(yachtsql_ir::Expr::BinaryOp {
                    left: Box::new(left_keys[0].clone()),
                    op: yachtsql_ir::BinaryOp::Eq,
                    right: Box::new(right_keys[0].clone()),
                })
            } else {
                let equalities: Vec<yachtsql_ir::Expr> = left_keys
                    .iter()
                    .zip(right_keys.iter())
                    .map(|(l, r)| yachtsql_ir::Expr::BinaryOp {
                        left: Box::new(l.clone()),
                        op: yachtsql_ir::BinaryOp::Eq,
                        right: Box::new(r.clone()),
                    })
                    .collect();
                equalities
                    .into_iter()
                    .reduce(|acc, e| yachtsql_ir::Expr::BinaryOp {
                        left: Box::new(acc),
                        op: yachtsql_ir::BinaryOp::And,
                        right: Box::new(e),
                    })
            };
            LogicalPlan::Join {
                left: Box::new(executor_plan_to_logical_plan(left)),
                right: Box::new(executor_plan_to_logical_plan(right)),
                join_type: *join_type,
                condition,
                schema: schema.clone(),
            }
        }
        PhysicalPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
            ..
        } => LogicalPlan::Aggregate {
            input: Box::new(executor_plan_to_logical_plan(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            schema: schema.clone(),
            grouping_sets: grouping_sets.clone(),
        },
        PhysicalPlan::Sort {
            input, sort_exprs, ..
        } => LogicalPlan::Sort {
            input: Box::new(executor_plan_to_logical_plan(input)),
            sort_exprs: sort_exprs.clone(),
        },
        PhysicalPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(executor_plan_to_logical_plan(input)),
            limit: *limit,
            offset: *offset,
        },
        PhysicalPlan::TopN {
            input,
            sort_exprs,
            limit,
        } => LogicalPlan::Limit {
            input: Box::new(LogicalPlan::Sort {
                input: Box::new(executor_plan_to_logical_plan(input)),
                sort_exprs: sort_exprs.clone(),
            }),
            limit: Some(*limit),
            offset: None,
        },
        PhysicalPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(executor_plan_to_logical_plan(input)),
        },
        PhysicalPlan::Union {
            inputs,
            all,
            schema,
            ..
        } => {
            let mut iter = inputs.iter();
            let first = iter.next().map(executor_plan_to_logical_plan);
            iter.fold(first, |acc, p| {
                Some(LogicalPlan::SetOperation {
                    left: Box::new(acc.unwrap()),
                    right: Box::new(executor_plan_to_logical_plan(p)),
                    op: yachtsql_ir::SetOperationType::Union,
                    all: *all,
                    schema: schema.clone(),
                })
            })
            .unwrap_or_else(|| LogicalPlan::Empty {
                schema: schema.clone(),
            })
        }
        PhysicalPlan::Intersect {
            left,
            right,
            all,
            schema,
            ..
        } => LogicalPlan::SetOperation {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            op: yachtsql_ir::SetOperationType::Intersect,
            all: *all,
            schema: schema.clone(),
        },
        PhysicalPlan::Except {
            left,
            right,
            all,
            schema,
            ..
        } => LogicalPlan::SetOperation {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            op: yachtsql_ir::SetOperationType::Except,
            all: *all,
            schema: schema.clone(),
        },
        PhysicalPlan::Window {
            input,
            window_exprs,
            schema,
            ..
        } => LogicalPlan::Window {
            input: Box::new(executor_plan_to_logical_plan(input)),
            window_exprs: window_exprs.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Unnest {
            input,
            columns,
            schema,
        } => LogicalPlan::Unnest {
            input: Box::new(executor_plan_to_logical_plan(input)),
            columns: columns.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
            input: Box::new(executor_plan_to_logical_plan(input)),
            predicate: predicate.clone(),
        },
        PhysicalPlan::WithCte { ctes, body, .. } => LogicalPlan::WithCte {
            ctes: ctes.clone(),
            body: Box::new(executor_plan_to_logical_plan(body)),
        },
        PhysicalPlan::Values { values, schema } => LogicalPlan::Values {
            values: values.clone(),
            schema: schema.clone(),
        },
        PhysicalPlan::Empty { schema } => LogicalPlan::Empty {
            schema: schema.clone(),
        },
        PhysicalPlan::Insert {
            table_name,
            columns,
            source,
        } => LogicalPlan::Insert {
            table_name: table_name.clone(),
            columns: columns.clone(),
            source: Box::new(executor_plan_to_logical_plan(source)),
        },
        PhysicalPlan::Update {
            table_name,
            alias,
            assignments,
            from,
            filter,
        } => LogicalPlan::Update {
            table_name: table_name.clone(),
            alias: alias.clone(),
            assignments: assignments.clone(),
            from: from
                .as_ref()
                .map(|p| Box::new(executor_plan_to_logical_plan(p))),
            filter: filter.clone(),
        },
        PhysicalPlan::Delete {
            table_name,
            alias,
            filter,
        } => LogicalPlan::Delete {
            table_name: table_name.clone(),
            alias: alias.clone(),
            filter: filter.clone(),
        },
        PhysicalPlan::Merge {
            target_table,
            source,
            on,
            clauses,
        } => LogicalPlan::Merge {
            target_table: target_table.clone(),
            source: Box::new(executor_plan_to_logical_plan(source)),
            on: on.clone(),
            clauses: clauses.clone(),
        },
        PhysicalPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
            query,
        } => LogicalPlan::CreateTable {
            table_name: table_name.clone(),
            columns: columns.clone(),
            if_not_exists: *if_not_exists,
            or_replace: *or_replace,
            query: query
                .as_ref()
                .map(|q| Box::new(executor_plan_to_logical_plan(q))),
        },
        PhysicalPlan::DropTable {
            table_names,
            if_exists,
        } => LogicalPlan::DropTable {
            table_names: table_names.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::AlterTable {
            table_name,
            operation,
            if_exists,
        } => LogicalPlan::AlterTable {
            table_name: table_name.clone(),
            operation: operation.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::Truncate { table_name } => LogicalPlan::Truncate {
            table_name: table_name.clone(),
        },
        PhysicalPlan::CreateView {
            name,
            query,
            query_sql,
            column_aliases,
            or_replace,
            if_not_exists,
        } => LogicalPlan::CreateView {
            name: name.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
            query_sql: query_sql.clone(),
            column_aliases: column_aliases.clone(),
            or_replace: *or_replace,
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::DropView { name, if_exists } => LogicalPlan::DropView {
            name: name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::CreateSchema {
            name,
            if_not_exists,
            or_replace,
        } => LogicalPlan::CreateSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
            or_replace: *or_replace,
        },
        PhysicalPlan::DropSchema {
            name,
            if_exists,
            cascade,
        } => LogicalPlan::DropSchema {
            name: name.clone(),
            if_exists: *if_exists,
            cascade: *cascade,
        },
        PhysicalPlan::UndropSchema {
            name,
            if_not_exists,
        } => LogicalPlan::UndropSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::AlterSchema { name, options } => LogicalPlan::AlterSchema {
            name: name.clone(),
            options: options.clone(),
        },
        PhysicalPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
            is_aggregate,
        } => LogicalPlan::CreateFunction {
            name: name.clone(),
            args: args.clone(),
            return_type: return_type.clone(),
            body: body.clone(),
            or_replace: *or_replace,
            if_not_exists: *if_not_exists,
            is_temp: *is_temp,
            is_aggregate: *is_aggregate,
        },
        PhysicalPlan::DropFunction { name, if_exists } => LogicalPlan::DropFunction {
            name: name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::CreateProcedure {
            name,
            args,
            body,
            or_replace,
            if_not_exists,
        } => LogicalPlan::CreateProcedure {
            name: name.clone(),
            args: args.clone(),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            or_replace: *or_replace,
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::DropProcedure { name, if_exists } => LogicalPlan::DropProcedure {
            name: name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::Call {
            procedure_name,
            args,
        } => LogicalPlan::Call {
            procedure_name: procedure_name.clone(),
            args: args.clone(),
        },
        PhysicalPlan::ExportData { options, query } => LogicalPlan::ExportData {
            options: options.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
        },
        PhysicalPlan::Declare {
            name,
            data_type,
            default,
        } => LogicalPlan::Declare {
            name: name.clone(),
            data_type: data_type.clone(),
            default: default.clone(),
        },
        PhysicalPlan::SetVariable { name, value } => LogicalPlan::SetVariable {
            name: name.clone(),
            value: value.clone(),
        },
        PhysicalPlan::SetMultipleVariables { names, value } => LogicalPlan::SetMultipleVariables {
            names: names.clone(),
            value: value.clone(),
        },
        PhysicalPlan::If {
            condition,
            then_branch,
            else_branch,
        } => LogicalPlan::If {
            condition: condition.clone(),
            then_branch: then_branch
                .iter()
                .map(executor_plan_to_logical_plan)
                .collect(),
            else_branch: else_branch
                .as_ref()
                .map(|b| b.iter().map(executor_plan_to_logical_plan).collect()),
        },
        PhysicalPlan::While {
            condition,
            body,
            label,
        } => LogicalPlan::While {
            condition: condition.clone(),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            label: label.clone(),
        },
        PhysicalPlan::Loop { body, label } => LogicalPlan::Loop {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            label: label.clone(),
        },
        PhysicalPlan::Block { body, label } => LogicalPlan::Block {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            label: label.clone(),
        },
        PhysicalPlan::For {
            variable,
            query,
            body,
        } => LogicalPlan::For {
            variable: variable.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
        },
        PhysicalPlan::Return { value } => LogicalPlan::Return {
            value: value.clone(),
        },
        PhysicalPlan::Raise { message, level } => LogicalPlan::Raise {
            message: message.clone(),
            level: *level,
        },
        PhysicalPlan::ExecuteImmediate {
            sql_expr,
            into_variables,
            using_params,
        } => LogicalPlan::ExecuteImmediate {
            sql_expr: sql_expr.clone(),
            into_variables: into_variables.clone(),
            using_params: using_params.clone(),
        },
        PhysicalPlan::Break { label } => LogicalPlan::Break {
            label: label.clone(),
        },
        PhysicalPlan::Continue { label } => LogicalPlan::Continue {
            label: label.clone(),
        },
        PhysicalPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        } => LogicalPlan::CreateSnapshot {
            snapshot_name: snapshot_name.clone(),
            source_name: source_name.clone(),
            if_not_exists: *if_not_exists,
        },
        PhysicalPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        } => LogicalPlan::DropSnapshot {
            snapshot_name: snapshot_name.clone(),
            if_exists: *if_exists,
        },
        PhysicalPlan::Repeat {
            body,
            until_condition,
        } => LogicalPlan::Repeat {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            until_condition: until_condition.clone(),
        },
        PhysicalPlan::LoadData {
            table_name,
            options,
            temp_table,
            temp_schema,
        } => LogicalPlan::LoadData {
            table_name: table_name.clone(),
            options: options.clone(),
            temp_table: *temp_table,
            temp_schema: temp_schema.clone(),
        },
        PhysicalPlan::Sample {
            input,
            sample_type,
            sample_value,
        } => LogicalPlan::Sample {
            input: Box::new(executor_plan_to_logical_plan(input)),
            sample_type: match sample_type {
                yachtsql_optimizer::SampleType::Rows => yachtsql_ir::SampleType::Rows,
                yachtsql_optimizer::SampleType::Percent => yachtsql_ir::SampleType::Percent,
            },
            sample_value: *sample_value,
        },
        PhysicalPlan::Assert { condition, message } => LogicalPlan::Assert {
            condition: condition.clone(),
            message: message.clone(),
        },
        PhysicalPlan::Grant {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => LogicalPlan::Grant {
            roles: roles.clone(),
            resource_type: resource_type.clone(),
            resource_name: resource_name.clone(),
            grantees: grantees.clone(),
        },
        PhysicalPlan::Revoke {
            roles,
            resource_type,
            resource_name,
            grantees,
        } => LogicalPlan::Revoke {
            roles: roles.clone(),
            resource_type: resource_type.clone(),
            resource_name: resource_name.clone(),
            grantees: grantees.clone(),
        },
        PhysicalPlan::BeginTransaction => LogicalPlan::BeginTransaction,
        PhysicalPlan::Commit => LogicalPlan::Commit,
        PhysicalPlan::Rollback => LogicalPlan::Rollback,
        PhysicalPlan::TryCatch {
            try_block,
            catch_block,
        } => LogicalPlan::TryCatch {
            try_block: try_block
                .iter()
                .map(|(p, sql)| (executor_plan_to_logical_plan(p), sql.clone()))
                .collect(),
            catch_block: catch_block
                .iter()
                .map(executor_plan_to_logical_plan)
                .collect(),
        },
        PhysicalPlan::GapFill {
            input,
            ts_column,
            bucket_width,
            value_columns,
            partitioning_columns,
            origin,
            input_schema,
            schema,
        } => LogicalPlan::GapFill {
            input: Box::new(executor_plan_to_logical_plan(input)),
            ts_column: ts_column.clone(),
            bucket_width: bucket_width.clone(),
            value_columns: value_columns.clone(),
            partitioning_columns: partitioning_columns.clone(),
            origin: origin.clone(),
            input_schema: input_schema.clone(),
            schema: schema.clone(),
        },
    }
}
