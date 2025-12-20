use yachtsql_common::error::{Error, Result};
use yachtsql_common::types::{DataType, Value};
use yachtsql_ir::{
    AlterColumnAction, AlterTableOp, ColumnDef, ExportOptions, FunctionArg, FunctionBody,
    ProcedureArg,
};
use yachtsql_storage::{Field, FieldMode, Schema, Table, TableSchemaOps};

use super::PlanExecutor;
use crate::catalog::{ColumnDefault, UserFunction, UserProcedure};
use crate::ir_evaluator::IrEvaluator;
use crate::plan::ExecutorPlan;

impl<'a> PlanExecutor<'a> {
    pub fn execute_create_table(
        &mut self,
        table_name: &str,
        columns: &[ColumnDef],
        if_not_exists: bool,
        or_replace: bool,
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

        let mut schema = Schema::new();
        let mut defaults = Vec::new();
        for col in columns {
            let mode = if col.nullable {
                FieldMode::Nullable
            } else {
                FieldMode::Required
            };
            schema.add_field(Field::new(&col.name, col.data_type.clone(), mode));
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
    ) -> Result<Table> {
        let _table = self
            .catalog
            .get_table(table_name)
            .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;

        match operation {
            AlterTableOp::AddColumn { column } => {
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

                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
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
            AlterTableOp::DropColumn { name } => {
                let table = self
                    .catalog
                    .get_table_mut(table_name)
                    .ok_or_else(|| Error::TableNotFound(table_name.to_string()))?;
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
                    AlterColumnAction::SetDefault { .. }
                    | AlterColumnAction::DropDefault
                    | AlterColumnAction::SetDataType { .. } => {
                        return Err(Error::UnsupportedFeature(format!(
                            "ALTER COLUMN {:?} not yet implemented",
                            action
                        )));
                    }
                }
                Ok(Table::empty(Schema::new()))
            }
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

    pub fn execute_create_schema(&mut self, name: &str, if_not_exists: bool) -> Result<Table> {
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

    pub fn execute_create_function(
        &mut self,
        name: &str,
        args: &[FunctionArg],
        return_type: &DataType,
        body: &FunctionBody,
        or_replace: bool,
        if_not_exists: bool,
        is_temp: bool,
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

    pub fn execute_export(
        &mut self,
        options: &ExportOptions,
        query: &ExecutorPlan,
    ) -> Result<Table> {
        let _data = self.execute_plan(query)?;
        Err(Error::UnsupportedFeature(
            "EXPORT DATA not yet implemented in new executor".into(),
        ))
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
        body: &[ExecutorPlan],
        or_replace: bool,
    ) -> Result<Table> {
        let body_plans = body
            .iter()
            .map(|p| executor_plan_to_logical_plan(p))
            .collect();
        let proc = UserProcedure {
            name: name.to_string(),
            parameters: args.to_vec(),
            body: body_plans,
        };
        self.catalog.create_procedure(proc, or_replace)?;
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

fn executor_plan_to_logical_plan(plan: &ExecutorPlan) -> yachtsql_ir::LogicalPlan {
    use yachtsql_ir::LogicalPlan;
    match plan {
        ExecutorPlan::TableScan {
            table_name,
            schema,
            projection,
        } => LogicalPlan::Scan {
            table_name: table_name.clone(),
            schema: schema.clone(),
            projection: projection.clone(),
        },
        ExecutorPlan::Filter { input, predicate } => LogicalPlan::Filter {
            input: Box::new(executor_plan_to_logical_plan(input)),
            predicate: predicate.clone(),
        },
        ExecutorPlan::Project {
            input,
            expressions,
            schema,
        } => LogicalPlan::Project {
            input: Box::new(executor_plan_to_logical_plan(input)),
            expressions: expressions.clone(),
            schema: schema.clone(),
        },
        ExecutorPlan::NestedLoopJoin {
            left,
            right,
            join_type,
            condition,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            join_type: *join_type,
            condition: condition.clone(),
            schema: schema.clone(),
        },
        ExecutorPlan::CrossJoin {
            left,
            right,
            schema,
        } => LogicalPlan::Join {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            join_type: yachtsql_ir::JoinType::Cross,
            condition: None,
            schema: schema.clone(),
        },
        ExecutorPlan::HashAggregate {
            input,
            group_by,
            aggregates,
            schema,
            grouping_sets,
        } => LogicalPlan::Aggregate {
            input: Box::new(executor_plan_to_logical_plan(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            schema: schema.clone(),
            grouping_sets: grouping_sets.clone(),
        },
        ExecutorPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
            input: Box::new(executor_plan_to_logical_plan(input)),
            sort_exprs: sort_exprs.clone(),
        },
        ExecutorPlan::Limit {
            input,
            limit,
            offset,
        } => LogicalPlan::Limit {
            input: Box::new(executor_plan_to_logical_plan(input)),
            limit: *limit,
            offset: *offset,
        },
        ExecutorPlan::TopN {
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
        ExecutorPlan::Distinct { input } => LogicalPlan::Distinct {
            input: Box::new(executor_plan_to_logical_plan(input)),
        },
        ExecutorPlan::Union {
            inputs,
            all,
            schema,
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
        ExecutorPlan::Intersect {
            left,
            right,
            all,
            schema,
        } => LogicalPlan::SetOperation {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            op: yachtsql_ir::SetOperationType::Intersect,
            all: *all,
            schema: schema.clone(),
        },
        ExecutorPlan::Except {
            left,
            right,
            all,
            schema,
        } => LogicalPlan::SetOperation {
            left: Box::new(executor_plan_to_logical_plan(left)),
            right: Box::new(executor_plan_to_logical_plan(right)),
            op: yachtsql_ir::SetOperationType::Except,
            all: *all,
            schema: schema.clone(),
        },
        ExecutorPlan::Window {
            input,
            window_exprs,
            schema,
        } => LogicalPlan::Window {
            input: Box::new(executor_plan_to_logical_plan(input)),
            window_exprs: window_exprs.clone(),
            schema: schema.clone(),
        },
        ExecutorPlan::Unnest {
            input,
            columns,
            schema,
        } => LogicalPlan::Unnest {
            input: Box::new(executor_plan_to_logical_plan(input)),
            columns: columns.clone(),
            schema: schema.clone(),
        },
        ExecutorPlan::Qualify { input, predicate } => LogicalPlan::Qualify {
            input: Box::new(executor_plan_to_logical_plan(input)),
            predicate: predicate.clone(),
        },
        ExecutorPlan::WithCte { ctes, body } => LogicalPlan::WithCte {
            ctes: ctes.clone(),
            body: Box::new(executor_plan_to_logical_plan(body)),
        },
        ExecutorPlan::Values { values, schema } => LogicalPlan::Values {
            values: values.clone(),
            schema: schema.clone(),
        },
        ExecutorPlan::Empty { schema } => LogicalPlan::Empty {
            schema: schema.clone(),
        },
        ExecutorPlan::Insert {
            table_name,
            columns,
            source,
        } => LogicalPlan::Insert {
            table_name: table_name.clone(),
            columns: columns.clone(),
            source: Box::new(executor_plan_to_logical_plan(source)),
        },
        ExecutorPlan::Update {
            table_name,
            assignments,
            filter,
        } => LogicalPlan::Update {
            table_name: table_name.clone(),
            assignments: assignments.clone(),
            filter: filter.clone(),
        },
        ExecutorPlan::Delete { table_name, filter } => LogicalPlan::Delete {
            table_name: table_name.clone(),
            filter: filter.clone(),
        },
        ExecutorPlan::Merge {
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
        ExecutorPlan::CreateTable {
            table_name,
            columns,
            if_not_exists,
            or_replace,
        } => LogicalPlan::CreateTable {
            table_name: table_name.clone(),
            columns: columns.clone(),
            if_not_exists: *if_not_exists,
            or_replace: *or_replace,
        },
        ExecutorPlan::DropTable {
            table_names,
            if_exists,
        } => LogicalPlan::DropTable {
            table_names: table_names.clone(),
            if_exists: *if_exists,
        },
        ExecutorPlan::AlterTable {
            table_name,
            operation,
        } => LogicalPlan::AlterTable {
            table_name: table_name.clone(),
            operation: operation.clone(),
        },
        ExecutorPlan::Truncate { table_name } => LogicalPlan::Truncate {
            table_name: table_name.clone(),
        },
        ExecutorPlan::CreateView {
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
        ExecutorPlan::DropView { name, if_exists } => LogicalPlan::DropView {
            name: name.clone(),
            if_exists: *if_exists,
        },
        ExecutorPlan::CreateSchema {
            name,
            if_not_exists,
        } => LogicalPlan::CreateSchema {
            name: name.clone(),
            if_not_exists: *if_not_exists,
        },
        ExecutorPlan::DropSchema {
            name,
            if_exists,
            cascade,
        } => LogicalPlan::DropSchema {
            name: name.clone(),
            if_exists: *if_exists,
            cascade: *cascade,
        },
        ExecutorPlan::CreateFunction {
            name,
            args,
            return_type,
            body,
            or_replace,
            if_not_exists,
            is_temp,
        } => LogicalPlan::CreateFunction {
            name: name.clone(),
            args: args.clone(),
            return_type: return_type.clone(),
            body: body.clone(),
            or_replace: *or_replace,
            if_not_exists: *if_not_exists,
            is_temp: *is_temp,
        },
        ExecutorPlan::DropFunction { name, if_exists } => LogicalPlan::DropFunction {
            name: name.clone(),
            if_exists: *if_exists,
        },
        ExecutorPlan::CreateProcedure {
            name,
            args,
            body,
            or_replace,
        } => LogicalPlan::CreateProcedure {
            name: name.clone(),
            args: args.clone(),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            or_replace: *or_replace,
        },
        ExecutorPlan::DropProcedure { name, if_exists } => LogicalPlan::DropProcedure {
            name: name.clone(),
            if_exists: *if_exists,
        },
        ExecutorPlan::Call {
            procedure_name,
            args,
        } => LogicalPlan::Call {
            procedure_name: procedure_name.clone(),
            args: args.clone(),
        },
        ExecutorPlan::ExportData { options, query } => LogicalPlan::ExportData {
            options: options.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
        },
        ExecutorPlan::Declare {
            name,
            data_type,
            default,
        } => LogicalPlan::Declare {
            name: name.clone(),
            data_type: data_type.clone(),
            default: default.clone(),
        },
        ExecutorPlan::SetVariable { name, value } => LogicalPlan::SetVariable {
            name: name.clone(),
            value: value.clone(),
        },
        ExecutorPlan::If {
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
        ExecutorPlan::While { condition, body } => LogicalPlan::While {
            condition: condition.clone(),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
        },
        ExecutorPlan::Loop { body, label } => LogicalPlan::Loop {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            label: label.clone(),
        },
        ExecutorPlan::For {
            variable,
            query,
            body,
        } => LogicalPlan::For {
            variable: variable.clone(),
            query: Box::new(executor_plan_to_logical_plan(query)),
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
        },
        ExecutorPlan::Return { value } => LogicalPlan::Return {
            value: value.clone(),
        },
        ExecutorPlan::Raise { message, level } => LogicalPlan::Raise {
            message: message.clone(),
            level: *level,
        },
        ExecutorPlan::Break => LogicalPlan::Break,
        ExecutorPlan::Continue => LogicalPlan::Continue,
        ExecutorPlan::CreateSnapshot {
            snapshot_name,
            source_name,
            if_not_exists,
        } => LogicalPlan::CreateSnapshot {
            snapshot_name: snapshot_name.clone(),
            source_name: source_name.clone(),
            if_not_exists: *if_not_exists,
        },
        ExecutorPlan::DropSnapshot {
            snapshot_name,
            if_exists,
        } => LogicalPlan::DropSnapshot {
            snapshot_name: snapshot_name.clone(),
            if_exists: *if_exists,
        },
        ExecutorPlan::Repeat {
            body,
            until_condition,
        } => LogicalPlan::Repeat {
            body: body.iter().map(executor_plan_to_logical_plan).collect(),
            until_condition: until_condition.clone(),
        },
    }
}
