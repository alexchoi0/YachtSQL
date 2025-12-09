use sqlparser::ast::{self, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use yachtsql_core::error::{Error, Result};
use yachtsql_ir::expr::Expr;
use yachtsql_ir::plan::{
    AlterTableOperation, ConflictAction, LogicalPlan, MergeWhenMatched, MergeWhenNotMatched,
    PlanNode,
};

use super::LogicalPlanBuilder;

type ReturningList = Vec<(Expr, Option<String>)>;

type AssignmentList = Vec<(String, Expr)>;

#[allow(dead_code)]
impl LogicalPlanBuilder {
    fn update_to_plan(
        &self,
        table: &ast::TableWithJoins,
        assignments: &[ast::Assignment],
        selection: &Option<ast::Expr>,
    ) -> Result<LogicalPlan> {
        let table_name = match &table.relation {
            ast::TableFactor::Table { name, .. } => Self::object_name_to_string(name),
            _ => {
                return Err(Error::unsupported_feature(
                    "UPDATE with complex table expressions not supported".to_string(),
                ));
            }
        };

        let mut assignment_pairs = Vec::new();
        for assignment in assignments {
            let column = match &assignment.target {
                ast::AssignmentTarget::ColumnName(object_name) => {
                    Self::object_name_to_string(object_name)
                }
                _ => {
                    return Err(Error::unsupported_feature(
                        "UPDATE with complex assignment targets not supported".to_string(),
                    ));
                }
            };

            let value_expr = self.sql_expr_to_expr(&assignment.value)?;
            assignment_pairs.push((column, value_expr));
        }

        let predicate = if let Some(where_expr) = selection {
            Some(self.sql_expr_to_expr(where_expr)?)
        } else {
            None
        };

        Ok(LogicalPlan::new(PlanNode::Update {
            table_name,
            assignments: assignment_pairs,
            predicate,
        }))
    }

    fn delete_to_plan(
        &self,
        tables: &[ast::ObjectName],
        from: &ast::FromTable,
        selection: &Option<ast::Expr>,
    ) -> Result<LogicalPlan> {
        if !tables.is_empty() {
            return Err(Error::unsupported_feature(
                "DELETE with explicit table list not supported - use DELETE FROM table syntax"
                    .to_string(),
            ));
        }

        let from_tables = match from {
            ast::FromTable::WithFromKeyword(tables) | ast::FromTable::WithoutKeyword(tables) => {
                tables
            }
        };

        if from_tables.len() != 1 {
            return Err(Error::unsupported_feature(
                "DELETE with multiple tables not supported".to_string(),
            ));
        }

        let table_name = match &from_tables[0].relation {
            ast::TableFactor::Table { name, .. } => Self::object_name_to_string(name),
            _ => {
                return Err(Error::unsupported_feature(
                    "DELETE with complex table expressions not supported".to_string(),
                ));
            }
        };

        let predicate = if let Some(where_expr) = selection {
            Some(self.sql_expr_to_expr(where_expr)?)
        } else {
            None
        };

        Ok(LogicalPlan::new(PlanNode::Delete {
            table_name,
            predicate,
        }))
    }

    fn truncate_to_plan(&self, table_names: &[ast::TruncateTableTarget]) -> Result<LogicalPlan> {
        if table_names.len() != 1 {
            return Err(Error::unsupported_feature(
                "TRUNCATE with multiple tables not supported".to_string(),
            ));
        }

        let table_name = Self::object_name_to_string(&table_names[0].name);

        Ok(LogicalPlan::new(PlanNode::Truncate { table_name }))
    }

    fn alter_table_to_plan(
        &self,
        name: &ast::ObjectName,
        if_exists: bool,
        operations: &[ast::AlterTableOperation],
    ) -> Result<LogicalPlan> {
        let table_name = Self::object_name_to_string(name);

        if operations.len() != 1 {
            return Err(Error::unsupported_feature(
                "Multiple ALTER TABLE operations in a single statement not yet supported"
                    .to_string(),
            ));
        }

        let operation = match &operations[0] {
            ast::AlterTableOperation::AddColumn {
                column_keyword: _,
                if_not_exists,
                column_def,
                column_position: _,
            } => {
                let column_name = column_def.name.value.clone();
                let data_type = Self::sql_datatype_to_datatype(&column_def.data_type)?;

                let not_null = column_def
                    .options
                    .iter()
                    .any(|opt| matches!(opt.option, ast::ColumnOption::NotNull));

                let default_value = column_def.options.iter().find_map(|opt| {
                    if let ast::ColumnOption::Default(expr) = &opt.option {
                        Some(self.sql_expr_to_expr(expr).ok()?)
                    } else {
                        None
                    }
                });

                let generated_expression =
                    Self::parse_generated_expression_from_column_options(&column_def.options);

                AlterTableOperation::AddColumn {
                    column_name,
                    data_type,
                    if_not_exists: *if_not_exists,
                    not_null,
                    default_value,
                    generated_expression,
                }
            }
            ast::AlterTableOperation::DropColumn {
                column_names,
                if_exists,
                drop_behavior,
                ..
            } => {
                let column = column_names.first().ok_or_else(|| {
                    Error::invalid_query("DROP COLUMN requires at least one column".to_string())
                })?;
                let cascade = matches!(drop_behavior, Some(sqlparser::ast::DropBehavior::Cascade));
                AlterTableOperation::DropColumn {
                    column_name: column.value.clone(),
                    if_exists: *if_exists,
                    cascade,
                }
            }
            ast::AlterTableOperation::RenameColumn {
                old_column_name,
                new_column_name,
            } => AlterTableOperation::RenameColumn {
                old_name: old_column_name.value.clone(),
                new_name: new_column_name.value.clone(),
            },
            ast::AlterTableOperation::AlterColumn { column_name, op } => {
                let col_name = column_name.value.clone();
                match op {
                    ast::AlterColumnOperation::SetNotNull => AlterTableOperation::AlterColumn {
                        column_name: col_name,
                        new_data_type: None,
                        set_not_null: Some(true),
                        set_default: None,
                        drop_default: false,
                    },
                    ast::AlterColumnOperation::DropNotNull => AlterTableOperation::AlterColumn {
                        column_name: col_name,
                        new_data_type: None,
                        set_not_null: Some(false),
                        set_default: None,
                        drop_default: false,
                    },
                    ast::AlterColumnOperation::SetDefault { value } => {
                        let default_expr = self.sql_expr_to_expr(value)?;
                        AlterTableOperation::AlterColumn {
                            column_name: col_name,
                            new_data_type: None,
                            set_not_null: None,
                            set_default: Some(default_expr),
                            drop_default: false,
                        }
                    }
                    ast::AlterColumnOperation::DropDefault => AlterTableOperation::AlterColumn {
                        column_name: col_name,
                        new_data_type: None,
                        set_not_null: None,
                        set_default: None,
                        drop_default: true,
                    },
                    ast::AlterColumnOperation::SetDataType { data_type, .. } => {
                        let new_type = Self::sql_datatype_to_datatype(data_type)?;
                        AlterTableOperation::AlterColumn {
                            column_name: col_name,
                            new_data_type: Some(new_type),
                            set_not_null: None,
                            set_default: None,
                            drop_default: false,
                        }
                    }
                    _ => {
                        return Err(Error::unsupported_feature(format!(
                            "ALTER COLUMN operation not supported: {:?}",
                            op
                        )));
                    }
                }
            }
            ast::AlterTableOperation::RenameTable { table_name } => {
                let new_name = match table_name {
                    ast::RenameTableNameKind::As(name) | ast::RenameTableNameKind::To(name) => {
                        Self::object_name_to_string(name)
                    }
                };
                AlterTableOperation::RenameTable { new_name }
            }
            ast::AlterTableOperation::AddConstraint { constraint, .. } => {
                let table_constraint = Self::parse_table_constraint(constraint)?;
                AlterTableOperation::AddConstraint {
                    constraint: table_constraint,
                }
            }
            ast::AlterTableOperation::DropConstraint {
                name, if_exists, ..
            } => AlterTableOperation::DropConstraint {
                constraint_name: name.value.clone(),
                if_exists: *if_exists,
            },
            ast::AlterTableOperation::ModifyColumn {
                col_name,
                data_type,
                options,
                ..
            } => {
                let column_name = col_name.value.clone();
                let new_data_type = Some(Self::sql_datatype_to_datatype(data_type)?);

                let has_not_null = options
                    .iter()
                    .any(|opt| matches!(opt, ast::ColumnOption::NotNull));
                let has_null = options
                    .iter()
                    .any(|opt| matches!(opt, ast::ColumnOption::Null));

                let set_not_null = if has_not_null {
                    Some(true)
                } else if has_null {
                    Some(false)
                } else {
                    None
                };

                AlterTableOperation::AlterColumn {
                    column_name,
                    new_data_type,
                    set_not_null,
                    set_default: None,
                    drop_default: false,
                }
            }
            _ => {
                return Err(Error::unsupported_feature(format!(
                    "ALTER TABLE operation not supported: {:?}",
                    operations[0]
                )));
            }
        };

        Ok(LogicalPlan::new(PlanNode::AlterTable {
            table_name,
            operation,
            if_exists,
        }))
    }

    fn parse_generated_expression_from_column_options(
        options: &[ast::ColumnOptionDef],
    ) -> Option<yachtsql_storage::schema::GeneratedExpression> {
        options.iter().find_map(|opt| {
            if let ast::ColumnOption::Generated {
                generation_expr: Some(expr),
                generation_expr_mode,
                ..
            } = &opt.option
            {
                let mode = match generation_expr_mode {
                    Some(ast::GeneratedExpressionMode::Stored) => {
                        yachtsql_storage::schema::GenerationMode::Stored
                    }
                    Some(ast::GeneratedExpressionMode::Virtual) | None => {
                        yachtsql_storage::schema::GenerationMode::Virtual
                    }
                };
                Some(yachtsql_storage::schema::GeneratedExpression {
                    expression_sql: format!("{}", expr),

                    dependencies: Vec::new(),
                    generation_mode: mode,
                })
            } else {
                None
            }
        })
    }

    fn insert_to_plan(&self, insert: &ast::Insert) -> Result<LogicalPlan> {
        let table_name = Self::table_object_to_string(&insert.table)?;
        let columns = Self::extract_optional_column_list(&insert.columns);

        if let Some(ast::OnInsert::OnConflict(on_conflict)) = &insert.on {
            let values = self.extract_insert_values(insert)?;
            let conflict_target = self.parse_conflict_target(&on_conflict.conflict_target)?;
            let conflict_action = self.parse_conflict_action(&on_conflict.action)?;
            let where_clause = self.extract_conflict_where_clause(&on_conflict.action)?;
            let returning = self.parse_optional_returning(&insert.returning)?;

            Ok(LogicalPlan::new(PlanNode::InsertOnConflict {
                table_name,
                columns,
                values,
                conflict_target,
                conflict_action,
                where_clause,
                returning,
            }))
        } else {
            self.insert_regular_to_plan(&table_name, columns, insert)
        }
    }

    fn insert_regular_to_plan(
        &self,
        table_name: &str,
        columns: Option<Vec<String>>,
        insert: &ast::Insert,
    ) -> Result<LogicalPlan> {
        let source = insert.source.as_ref().ok_or_else(|| {
            Error::unsupported_feature("INSERT without source not supported".to_string())
        })?;

        match &*source.body {
            ast::SetExpr::Values(values) => {
                let mut value_rows = Vec::new();
                for row in &values.rows {
                    let row_values = row
                        .iter()
                        .map(|expr| self.sql_expr_to_expr(expr))
                        .collect::<Result<Vec<_>>>()?;
                    value_rows.push(row_values);
                }

                Ok(LogicalPlan::new(PlanNode::Insert {
                    table_name: table_name.to_string(),
                    columns,
                    values: Some(value_rows),
                    source: None,
                }))
            }
            ast::SetExpr::Select(_) => {
                let source_plan = self.query_to_plan(source)?;

                Ok(LogicalPlan::new(PlanNode::Insert {
                    table_name: table_name.to_string(),
                    columns,
                    values: None,
                    source: Some(Box::new(source_plan.root().clone())),
                }))
            }
            _ => Err(Error::unsupported_feature(
                "Complex INSERT source not yet supported".to_string(),
            )),
        }
    }

    fn table_object_to_string(table: &ast::TableObject) -> Result<String> {
        match table {
            ast::TableObject::TableName(name) => Ok(Self::object_name_to_string(name)),
            ast::TableObject::TableFunction(func) => Err(Error::unsupported_feature(format!(
                "INSERT INTO TABLE FUNCTION targets are not supported: {}",
                func
            ))),
        }
    }

    fn extract_insert_values(&self, insert: &ast::Insert) -> Result<Vec<Vec<Expr>>> {
        insert
            .source
            .as_ref()
            .ok_or_else(|| {
                Error::unsupported_feature(
                    "INSERT without VALUES clause not yet supported".to_string(),
                )
            })
            .and_then(|source| self.extract_values_from_query(source))
    }

    fn parse_conflict_target(&self, target: &Option<ast::ConflictTarget>) -> Result<Vec<String>> {
        match target {
            Some(ast::ConflictTarget::Columns(cols)) => Ok(Self::ident_list_to_strings(cols)),
            Some(ast::ConflictTarget::OnConstraint(name)) => Err(Error::unsupported_feature(
                format!("ON CONFLICT ON CONSTRAINT not yet supported: {}", name),
            )),
            None => Err(Error::unsupported_feature(
                "ON CONFLICT without conflict target not supported".to_string(),
            )),
        }
    }

    fn parse_conflict_action(&self, action: &ast::OnConflictAction) -> Result<ConflictAction> {
        match action {
            ast::OnConflictAction::DoNothing => Ok(ConflictAction::DoNothing),
            ast::OnConflictAction::DoUpdate(do_update) => {
                let assignments = self.parse_assignments(&do_update.assignments)?;
                Ok(ConflictAction::Update { assignments })
            }
        }
    }

    fn extract_conflict_where_clause(
        &self,
        action: &ast::OnConflictAction,
    ) -> Result<Option<Expr>> {
        match action {
            ast::OnConflictAction::DoUpdate(do_update) => do_update
                .selection
                .as_ref()
                .map(|sel| self.sql_expr_to_expr(sel))
                .transpose(),
            ast::OnConflictAction::DoNothing => Ok(None),
        }
    }

    fn extract_values_from_query(&self, query: &ast::Query) -> Result<Vec<Vec<Expr>>> {
        match &*query.body {
            ast::SetExpr::Values(values) => {
                let mut result = Vec::new();
                for row in &values.rows {
                    let row_values = row
                        .iter()
                        .map(|expr| self.sql_expr_to_expr(expr))
                        .collect::<Result<Vec<_>>>()?;
                    result.push(row_values);
                }
                Ok(result)
            }
            ast::SetExpr::Select(_) => Err(Error::unsupported_feature(
                "INSERT INTO ... SELECT not yet supported".to_string(),
            )),
            _ => Err(Error::unsupported_feature(
                "Complex INSERT source not yet supported".to_string(),
            )),
        }
    }

    fn parse_optional_returning(
        &self,
        returning: &Option<Vec<ast::SelectItem>>,
    ) -> Result<Option<ReturningList>> {
        returning
            .as_ref()
            .map(|items| self.parse_returning(items))
            .transpose()
    }

    #[allow(unreachable_patterns)]
    fn parse_returning(&self, items: &[ast::SelectItem]) -> Result<ReturningList> {
        items
            .iter()
            .map(|item| match item {
                ast::SelectItem::UnnamedExpr(expr) => Ok((self.sql_expr_to_expr(expr)?, None)),
                ast::SelectItem::ExprWithAlias { expr, alias } => {
                    Ok((self.sql_expr_to_expr(expr)?, Some(alias.value.clone())))
                }
                ast::SelectItem::Wildcard(_) => Ok((Expr::Wildcard, None)),
                ast::SelectItem::QualifiedWildcard(kind, _) => {
                    let qualifier = match kind {
                        ast::SelectItemQualifiedWildcardKind::ObjectName(object_name) => {
                            Self::object_name_to_string(object_name)
                        }
                        ast::SelectItemQualifiedWildcardKind::Expr(expr) => {
                            return Err(Error::unsupported_feature(format!(
                                "Expression-qualified wildcard not supported in RETURNING: {}",
                                expr
                            )));
                        }
                    };
                    Ok((Expr::QualifiedWildcard { qualifier }, None))
                }
                _ => Err(Error::unsupported_feature(
                    "Complex RETURNING item not supported".to_string(),
                )),
            })
            .collect()
    }

    fn parse_assignments(&self, assignments: &[ast::Assignment]) -> Result<AssignmentList> {
        assignments
            .iter()
            .map(|assignment| {
                let column = self.parse_assignment_target(&assignment.target)?;
                let value = self.sql_expr_to_expr(&assignment.value)?;
                Ok((column, value))
            })
            .collect()
    }

    fn parse_assignment_target(&self, target: &ast::AssignmentTarget) -> Result<String> {
        match target {
            ast::AssignmentTarget::ColumnName(name) => Ok(Self::object_name_to_string(name)),
            ast::AssignmentTarget::Tuple(_) => Err(Error::unsupported_feature(
                "Multi-column assignment target not supported".to_string(),
            )),
        }
    }

    pub fn merge_to_plan(&self, stmt: &ast::Statement) -> Result<LogicalPlan> {
        if let Statement::Merge {
            table,
            source,
            on,
            clauses,
            ..
        } = stmt
        {
            let (target_table, target_alias) = self.parse_merge_target_table(table)?;
            let (source_plan, source_alias) = self.parse_merge_source(source)?;
            let on_condition = self.sql_expr_to_expr(on)?;
            let (when_matched, when_not_matched, when_not_matched_by_source) =
                self.parse_merge_when_clauses(clauses)?;
            let returning = self
                .take_merge_returning()
                .map(|returning_sql| self.parse_merge_returning_clause(&returning_sql))
                .transpose()?;

            Ok(LogicalPlan::new(PlanNode::Merge {
                target_table,
                target_alias,
                source: Box::new(source_plan),
                source_alias: Some(source_alias),
                on_condition,
                when_matched,
                when_not_matched,
                when_not_matched_by_source,
                returning,
            }))
        } else {
            Err(Error::unsupported_feature(
                "Expected MERGE statement".to_string(),
            ))
        }
    }

    fn parse_merge_returning_clause(&self, returning_sql: &str) -> Result<ReturningList> {
        let select_sql = format!("SELECT {}", returning_sql);
        let dialect = GenericDialect {};
        let statements = SqlParser::parse_sql(&dialect, &select_sql).map_err(|err| {
            Error::parse_error(format!("Failed to parse MERGE RETURNING clause: {}", err))
        })?;

        if statements.len() != 1 {
            return Err(Error::parse_error(
                "MERGE RETURNING clause produced unexpected statements".to_string(),
            ));
        }

        match &statements[0] {
            Statement::Query(query) => {
                if let ast::SetExpr::Select(select) = &*query.body {
                    self.parse_returning(&select.projection)
                } else {
                    Err(Error::parse_error(
                        "MERGE RETURNING clause must be a projection list".to_string(),
                    ))
                }
            }
            _ => Err(Error::parse_error(
                "MERGE RETURNING clause parsing produced non-SELECT statement".to_string(),
            )),
        }
    }

    fn parse_merge_target_table(
        &self,
        table: &ast::TableFactor,
    ) -> Result<(String, Option<String>)> {
        match table {
            ast::TableFactor::Table { name, alias, .. } => {
                let table_name = Self::object_name_to_string(name);
                let table_alias = alias.as_ref().map(|a| a.name.value.clone());
                Ok((table_name, table_alias))
            }
            _ => Err(Error::unsupported_feature(
                "Complex target table in MERGE not supported".to_string(),
            )),
        }
    }

    fn parse_merge_source(&self, source: &ast::TableFactor) -> Result<(PlanNode, String)> {
        match source {
            ast::TableFactor::Table { name, alias, .. } => {
                let table_name = Self::object_name_to_string(name);
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .unwrap_or_else(|| table_name.clone());
                let plan = PlanNode::Scan {
                    table_name,
                    projection: None,
                    alias: None,
                    only: false,
                };
                Ok((plan, alias))
            }
            ast::TableFactor::Derived {
                lateral: _lateral,
                subquery,
                alias,
                ..
            } => {
                let alias = alias
                    .as_ref()
                    .map(|a| a.name.value.clone())
                    .ok_or_else(|| {
                        Error::unsupported_feature(
                            "Derived table in MERGE must have alias".to_string(),
                        )
                    })?;
                let subquery_plan = self.query_to_plan(subquery)?;
                let plan = PlanNode::SubqueryScan {
                    subquery: Box::new(subquery_plan.root().clone()),
                    alias: alias.clone(),
                };
                Ok((plan, alias))
            }
            _ => Err(Error::unsupported_feature(
                "Complex source in MERGE not supported".to_string(),
            )),
        }
    }

    fn parse_merge_when_clauses(
        &self,
        clauses: &[ast::MergeClause],
    ) -> Result<(
        Vec<MergeWhenMatched>,
        Vec<MergeWhenNotMatched>,
        Vec<yachtsql_ir::plan::MergeWhenNotMatchedBySource>,
    )> {
        use yachtsql_ir::plan::MergeWhenNotMatchedBySource;

        let mut when_matched = Vec::new();
        let mut when_not_matched = Vec::new();
        let mut when_not_matched_by_source = Vec::new();

        for clause in clauses {
            let condition = self.parse_optional_condition(&clause.predicate)?;

            match (&clause.clause_kind, &clause.action) {
                (ast::MergeClauseKind::Matched, ast::MergeAction::Update { assignments }) => {
                    when_matched.push(self.parse_merge_update(assignments, condition)?);
                }
                (ast::MergeClauseKind::Matched, ast::MergeAction::Delete) => {
                    when_matched.push(MergeWhenMatched::Delete { condition });
                }
                (
                    ast::MergeClauseKind::NotMatched | ast::MergeClauseKind::NotMatchedByTarget,
                    ast::MergeAction::Insert(insert_expr),
                ) => {
                    when_not_matched.push(self.parse_merge_insert(insert_expr, condition)?);
                }
                (
                    ast::MergeClauseKind::NotMatchedBySource,
                    ast::MergeAction::Update { assignments },
                ) => {
                    let assigns = self.parse_assignments(assignments)?;
                    when_not_matched_by_source.push(MergeWhenNotMatchedBySource::Update {
                        condition,
                        assignments: assigns,
                    });
                }
                (ast::MergeClauseKind::NotMatchedBySource, ast::MergeAction::Delete) => {
                    when_not_matched_by_source
                        .push(MergeWhenNotMatchedBySource::Delete { condition });
                }
                _ => {
                    return Err(Error::unsupported_feature(format!(
                        "Unsupported MERGE clause: {:?} with {:?}",
                        clause.clause_kind, clause.action
                    )));
                }
            }
        }

        Ok((when_matched, when_not_matched, when_not_matched_by_source))
    }

    fn parse_optional_condition(&self, predicate: &Option<ast::Expr>) -> Result<Option<Expr>> {
        predicate
            .as_ref()
            .map(|pred| self.sql_expr_to_expr(pred))
            .transpose()
    }

    fn parse_merge_update(
        &self,
        assignments: &[ast::Assignment],
        condition: Option<Expr>,
    ) -> Result<MergeWhenMatched> {
        let assigns = self.parse_assignments(assignments)?;
        Ok(MergeWhenMatched::Update {
            condition,
            assignments: assigns,
        })
    }

    fn parse_merge_insert(
        &self,
        insert_expr: &ast::MergeInsertExpr,
        condition: Option<Expr>,
    ) -> Result<MergeWhenNotMatched> {
        let columns = Self::extract_optional_column_list(&insert_expr.columns).unwrap_or_default();
        let values = self.parse_merge_insert_values(&insert_expr.kind)?;

        Ok(MergeWhenNotMatched::Insert {
            condition,
            columns,
            values,
        })
    }

    fn parse_merge_insert_values(&self, kind: &ast::MergeInsertKind) -> Result<Vec<Expr>> {
        match kind {
            ast::MergeInsertKind::Values(vals) => {
                if vals.rows.is_empty() {
                    return Err(Error::unsupported_feature(
                        "Empty VALUES in MERGE INSERT".to_string(),
                    ));
                }
                vals.rows[0]
                    .iter()
                    .map(|e| self.sql_expr_to_expr(e))
                    .collect()
            }
            ast::MergeInsertKind::Row => Err(Error::unsupported_feature(
                "INSERT ROW in MERGE not yet supported".to_string(),
            )),
        }
    }
}
