use crate::plan::PhysicalPlan;

pub fn executor_plan_to_logical_plan(plan: &PhysicalPlan) -> yachtsql_ir::LogicalPlan {
    use yachtsql_ir::LogicalPlan;
    match plan {
        PhysicalPlan::TableScan {
            table_name,
            schema,
            projection,
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
            parallel: _,
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
            parallel: _,
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
            parallel: _,
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
        } => LogicalPlan::Aggregate {
            input: Box::new(executor_plan_to_logical_plan(input)),
            group_by: group_by.clone(),
            aggregates: aggregates.clone(),
            schema: schema.clone(),
            grouping_sets: grouping_sets.clone(),
        },
        PhysicalPlan::Sort { input, sort_exprs } => LogicalPlan::Sort {
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
            parallel: _,
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
            parallel: _,
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
            parallel: _,
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
        PhysicalPlan::WithCte {
            ctes,
            body,
            parallel_ctes: _,
        } => LogicalPlan::WithCte {
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
