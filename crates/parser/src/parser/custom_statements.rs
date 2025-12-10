use sqlparser::tokenizer::Token;
use yachtsql_core::error::{Error, Result};

use super::helpers::ParserHelpers;
use crate::pattern_matcher::{PatternMatcher, TokenPattern};
use crate::validator::{
    AlterDomainAction, CompositeTypeField, CustomStatement, DiagnosticsAssignment, DiagnosticsItem,
    DiagnosticsScope, DomainConstraint, ExportFormat, SetConstraintsMode, SetConstraintsTarget,
};

pub struct CustomStatementParser;

impl CustomStatementParser {
    pub fn parse_set_constraints(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SET") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CONSTRAINTS") {
            return Ok(None);
        }

        let constraints = if ParserHelpers::consume_keyword(tokens, &mut idx, "ALL") {
            SetConstraintsTarget::All
        } else {
            let mut names = Vec::new();
            loop {
                let name = match tokens.get(idx) {
                    Some(Token::Word(word)) => {
                        idx += 1;
                        word.value.clone()
                    }
                    _ => {
                        if names.is_empty() {
                            return Err(Error::parse_error(
                                "SET CONSTRAINTS requires ALL or constraint name(s)".to_string(),
                            ));
                        }
                        break;
                    }
                };
                names.push(name);

                if matches!(tokens.get(idx), Some(Token::Comma)) {
                    idx += 1;
                    continue;
                }
                break;
            }
            SetConstraintsTarget::Named(names)
        };

        let mode = if ParserHelpers::consume_keyword(tokens, &mut idx, "DEFERRED") {
            SetConstraintsMode::Deferred
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "IMMEDIATE") {
            SetConstraintsMode::Immediate
        } else {
            return Err(Error::parse_error(
                "SET CONSTRAINTS requires DEFERRED or IMMEDIATE".to_string(),
            ));
        };

        if matches!(tokens.get(idx), Some(Token::SemiColon)) {
            idx += 1;
        }

        if idx < tokens.len() {
            return Err(Error::parse_error(
                "Unexpected tokens after SET CONSTRAINTS statement".to_string(),
            ));
        }

        Ok(Some(CustomStatement::SetConstraints { mode, constraints }))
    }

    pub fn parse_abort(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ABORT") {
            return Ok(None);
        }

        ParserHelpers::consume_keyword(tokens, &mut idx, "TRANSACTION");
        ParserHelpers::consume_keyword(tokens, &mut idx, "WORK");

        if matches!(tokens.get(idx), Some(Token::SemiColon)) {
            idx += 1;
        }

        if idx < tokens.len() {
            return Ok(None);
        }

        Ok(Some(CustomStatement::Abort))
    }

    pub fn parse_begin_transaction_with_deferrable(
        tokens: &[&Token],
    ) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "BEGIN") {
            return Ok(None);
        }

        ParserHelpers::consume_keyword(tokens, &mut idx, "TRANSACTION");
        ParserHelpers::consume_keyword(tokens, &mut idx, "WORK");

        let mut isolation_level = None;
        let mut read_only = None;
        let mut deferrable = None;
        let mut has_deferrable = false;

        while idx < tokens.len() {
            if matches!(tokens.get(idx), Some(Token::SemiColon)) {
                idx += 1;
                break;
            }

            if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "ISOLATION", "LEVEL") {
                let level = Self::parse_isolation_level(tokens, &mut idx)?;
                isolation_level = Some(level);
            } else if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "READ", "ONLY") {
                read_only = Some(true);
            } else if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "READ", "WRITE") {
                read_only = Some(false);
            } else if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "NOT", "DEFERRABLE") {
                deferrable = Some(false);
                has_deferrable = true;
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "DEFERRABLE") {
                deferrable = Some(true);
                has_deferrable = true;
            } else {
                break;
            }
        }

        if idx < tokens.len() && !matches!(tokens.get(idx), Some(Token::SemiColon)) {
            return Ok(None);
        }

        if !has_deferrable {
            return Ok(None);
        }

        Ok(Some(CustomStatement::BeginTransaction {
            isolation_level,
            read_only,
            deferrable,
        }))
    }

    fn parse_isolation_level(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        if ParserHelpers::consume_keyword_pair(tokens, idx, "READ", "UNCOMMITTED") {
            return Ok("READ UNCOMMITTED".to_string());
        }
        if ParserHelpers::consume_keyword_pair(tokens, idx, "READ", "COMMITTED") {
            return Ok("READ COMMITTED".to_string());
        }
        if ParserHelpers::consume_keyword_pair(tokens, idx, "REPEATABLE", "READ") {
            return Ok("REPEATABLE READ".to_string());
        }
        if ParserHelpers::consume_keyword(tokens, idx, "SERIALIZABLE") {
            return Ok("SERIALIZABLE".to_string());
        }
        Err(Error::parse_error(
            "Expected isolation level after ISOLATION LEVEL".to_string(),
        ))
    }

    pub fn parse_refresh_materialized_view(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let pattern = vec![
            TokenPattern::Keyword("REFRESH"),
            TokenPattern::Keyword("MATERIALIZED"),
            TokenPattern::Keyword("VIEW"),
            TokenPattern::OptionalKeyword("CONCURRENTLY"),
            TokenPattern::ObjectName,
        ];

        Self::match_and_build_custom_statement(tokens, &pattern, |matched| {
            let name = matched.first_object_name().ok_or_else(|| {
                Error::parse_error("REFRESH MATERIALIZED VIEW requires object name")
            })?;
            Ok(CustomStatement::RefreshMaterializedView {
                name: name.clone(),
                concurrently: matched.has_optional_keyword("CONCURRENTLY"),
            })
        })
    }

    pub fn parse_drop_materialized_view(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let pattern = vec![
            TokenPattern::Keyword("DROP"),
            TokenPattern::Keyword("MATERIALIZED"),
            TokenPattern::Keyword("VIEW"),
            TokenPattern::OptionalKeywordPair("IF", "EXISTS"),
            TokenPattern::ObjectName,
            TokenPattern::OptionalKeyword("CASCADE"),
        ];

        Self::match_and_build_custom_statement(tokens, &pattern, |matched| {
            let name = matched
                .first_object_name()
                .ok_or_else(|| Error::parse_error("DROP MATERIALIZED VIEW requires object name"))?;
            Ok(CustomStatement::DropMaterializedView {
                name: name.clone(),
                if_exists: matched.has_optional_pair("IF", "EXISTS"),
                cascade: matched.has_optional_keyword("CASCADE"),
            })
        })
    }

    pub fn parse_get_diagnostics(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "GET") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DIAGNOSTICS") {
            return Ok(None);
        }

        let mut scope = DiagnosticsScope::Current;
        if ParserHelpers::consume_keyword(tokens, &mut idx, "EXCEPTION") {
            let number = ParserHelpers::parse_i64(tokens, &mut idx)?;
            if number != 1 {
                return Err(Error::invalid_query(
                    "GET DIAGNOSTICS only supports EXCEPTION 1",
                ));
            }
            scope = DiagnosticsScope::Exception;
        }

        if idx >= tokens.len() {
            return Err(Error::invalid_query(
                "GET DIAGNOSTICS requires at least one assignment",
            ));
        }

        let mut assignments = Vec::new();

        loop {
            if matches!(tokens.get(idx), Some(Token::SemiColon)) {
                idx += 1;
                break;
            }

            let assignment = Self::parse_diagnostics_assignment(tokens, &mut idx)?;
            assignments.push(assignment);

            if idx >= tokens.len() {
                break;
            }

            if matches!(tokens.get(idx), Some(Token::Comma)) {
                idx += 1;
                continue;
            }

            if matches!(tokens.get(idx), Some(Token::SemiColon)) {
                idx += 1;
            }
            break;
        }

        if assignments.is_empty() {
            return Err(Error::invalid_query(
                "GET DIAGNOSTICS requires at least one assignment",
            ));
        }

        if idx != tokens.len() {
            return Err(Error::parse_error(
                "Unexpected tokens after GET DIAGNOSTICS assignment list",
            ));
        }

        Ok(Some(CustomStatement::GetDiagnostics { scope, assignments }))
    }

    pub fn parse_create_sequence(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SEQUENCE") {
            return Ok(None);
        }

        let if_not_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "NOT")
            && ParserHelpers::consume_keyword(tokens, &mut idx, "EXISTS");

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        let mut start_value = None;
        let mut increment = None;
        let mut min_value = None;
        let mut max_value = None;
        let mut cycle = None;
        let mut cache = None;
        let mut owned_by = None;

        while idx < tokens.len() {
            if matches!(tokens.get(idx), Some(Token::SemiColon)) {
                break;
            }

            if ParserHelpers::consume_keyword(tokens, &mut idx, "START") {
                ParserHelpers::consume_keyword(tokens, &mut idx, "WITH");
                start_value = Some(ParserHelpers::parse_i64(tokens, &mut idx)?);
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "INCREMENT") {
                ParserHelpers::consume_keyword(tokens, &mut idx, "BY");
                increment = Some(ParserHelpers::parse_i64(tokens, &mut idx)?);
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "NO") {
                if ParserHelpers::consume_keyword(tokens, &mut idx, "MINVALUE") {
                    min_value = Some(None);
                } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MAXVALUE") {
                    max_value = Some(None);
                } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CYCLE") {
                    cycle = Some(false);
                } else {
                    return Err(Error::parse_error(
                        "Unexpected keyword after NO".to_string(),
                    ));
                }
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MINVALUE") {
                min_value = Some(Some(ParserHelpers::parse_i64(tokens, &mut idx)?));
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MAXVALUE") {
                max_value = Some(Some(ParserHelpers::parse_i64(tokens, &mut idx)?));
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CYCLE") {
                cycle = Some(true);
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CACHE") {
                cache = Some(ParserHelpers::parse_u32(tokens, &mut idx)?);
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "OWNED") {
                if !ParserHelpers::consume_keyword(tokens, &mut idx, "BY") {
                    return Err(Error::parse_error("Expected BY after OWNED".to_string()));
                }
                owned_by = Some(ParserHelpers::parse_owned_by(tokens, &mut idx)?);
            } else {
                break;
            }
        }

        Ok(Some(CustomStatement::CreateSequence {
            if_not_exists,
            name,
            start_value,
            increment,
            min_value,
            max_value,
            cycle,
            cache,
            owned_by,
        }))
    }

    pub fn parse_alter_sequence(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SEQUENCE") {
            return Ok(None);
        }

        let if_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "EXISTS");

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        let mut restart = None;
        let mut increment = None;
        let mut min_value = None;
        let mut max_value = None;
        let mut cycle = None;
        let mut owned_by = None;

        while idx < tokens.len() {
            if matches!(tokens.get(idx), Some(Token::SemiColon)) {
                break;
            }

            if ParserHelpers::consume_keyword(tokens, &mut idx, "RESTART") {
                ParserHelpers::consume_keyword(tokens, &mut idx, "WITH");

                if ParserHelpers::peek_is_number(tokens, idx) {
                    restart = Some(Some(ParserHelpers::parse_i64(tokens, &mut idx)?));
                } else {
                    restart = Some(None);
                }
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "INCREMENT") {
                ParserHelpers::consume_keyword(tokens, &mut idx, "BY");
                increment = Some(ParserHelpers::parse_i64(tokens, &mut idx)?);
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "NO") {
                if ParserHelpers::consume_keyword(tokens, &mut idx, "MINVALUE") {
                    min_value = Some(None);
                } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MAXVALUE") {
                    max_value = Some(None);
                } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CYCLE") {
                    cycle = Some(false);
                } else {
                    return Err(Error::parse_error(
                        "Unexpected keyword after NO".to_string(),
                    ));
                }
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MINVALUE") {
                min_value = Some(Some(ParserHelpers::parse_i64(tokens, &mut idx)?));
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "MAXVALUE") {
                max_value = Some(Some(ParserHelpers::parse_i64(tokens, &mut idx)?));
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CYCLE") {
                cycle = Some(true);
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "OWNED") {
                if !ParserHelpers::consume_keyword(tokens, &mut idx, "BY") {
                    return Err(Error::parse_error("Expected BY after OWNED".to_string()));
                }
                if ParserHelpers::consume_keyword(tokens, &mut idx, "NONE") {
                    owned_by = Some(None);
                } else {
                    owned_by = Some(Some(ParserHelpers::parse_owned_by(tokens, &mut idx)?));
                }
            } else {
                break;
            }
        }

        Ok(Some(CustomStatement::AlterSequence {
            if_exists,
            name,
            restart,
            increment,
            min_value,
            max_value,
            cycle,
            owned_by,
        }))
    }

    pub fn parse_drop_sequence(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DROP") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SEQUENCE") {
            return Ok(None);
        }

        let if_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "EXISTS");

        let mut names = Vec::new();
        loop {
            names.push(ParserHelpers::parse_object_name_at(tokens, &mut idx)?);

            if matches!(tokens.get(idx), Some(Token::Comma)) {
                idx += 1;
                continue;
            } else {
                break;
            }
        }

        let cascade = ParserHelpers::consume_keyword(tokens, &mut idx, "CASCADE");
        let restrict = ParserHelpers::consume_keyword(tokens, &mut idx, "RESTRICT");

        Ok(Some(CustomStatement::DropSequence {
            if_exists,
            names,
            cascade,
            restrict,
        }))
    }

    pub fn parse_alter_table_restart_identity(
        tokens: &[&Token],
    ) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return Ok(None);
        }

        let table_name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "COLUMN") {
            return Ok(None);
        }

        let column_name = match tokens.get(idx) {
            Some(Token::Word(word)) => {
                idx += 1;
                word.value.clone()
            }
            _ => return Ok(None),
        };

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "RESTART") {
            return Ok(None);
        }

        let restart_with = if ParserHelpers::consume_keyword(tokens, &mut idx, "WITH")
            || ParserHelpers::peek_is_number(tokens, idx)
        {
            Some(ParserHelpers::parse_i64(tokens, &mut idx)?)
        } else {
            None
        };

        if idx < tokens.len() {
            if matches!(tokens[idx], Token::SemiColon) {
                idx += 1;
            } else {
                return Ok(None);
            }
        }

        if idx < tokens.len() {
            return Ok(None);
        }

        Ok(Some(CustomStatement::AlterTableRestartIdentity {
            table: table_name,
            column: column_name,
            restart_with,
        }))
    }

    pub fn parse_create_type(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TYPE") {
            return Ok(None);
        }

        let if_not_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "NOT")
            && ParserHelpers::consume_keyword(tokens, &mut idx, "EXISTS");

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "AS") {
            return Err(Error::parse_error(
                "Expected AS after type name".to_string(),
            ));
        }

        if ParserHelpers::check_keyword(tokens, idx, "ENUM") {
            return Ok(None);
        }

        if !matches!(tokens.get(idx), Some(Token::LParen)) {
            return Err(Error::parse_error(
                "Expected '(' after AS in CREATE TYPE".to_string(),
            ));
        }
        idx += 1;

        let mut fields = Vec::new();
        loop {
            if matches!(tokens.get(idx), Some(Token::RParen)) {
                break;
            }

            let field = Self::parse_composite_type_field(tokens, &mut idx)?;
            fields.push(field);

            if matches!(tokens.get(idx), Some(Token::Comma)) {
                idx += 1;
                continue;
            }
            if matches!(tokens.get(idx), Some(Token::RParen)) {
                break;
            }

            return Err(Error::parse_error(
                "Expected ',' or ')' after field definition".to_string(),
            ));
        }

        if !matches!(tokens.get(idx), Some(Token::RParen)) {
            return Err(Error::parse_error(
                "Expected ')' at end of CREATE TYPE".to_string(),
            ));
        }
        idx += 1;

        if matches!(tokens.get(idx), Some(Token::SemiColon)) {
            idx += 1;
        }

        if idx < tokens.len() {
            return Err(Error::parse_error(format!(
                "Unexpected tokens after CREATE TYPE: {:?}",
                tokens[idx]
            )));
        }

        Ok(Some(CustomStatement::CreateType {
            if_not_exists,
            name,
            fields,
        }))
    }

    pub fn parse_drop_type(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DROP") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TYPE") {
            return Ok(None);
        }

        let if_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "EXISTS");

        let mut names = Vec::new();
        loop {
            names.push(ParserHelpers::parse_object_name_at(tokens, &mut idx)?);

            if matches!(tokens.get(idx), Some(Token::Comma)) {
                idx += 1;
                continue;
            } else {
                break;
            }
        }

        let cascade = ParserHelpers::consume_keyword(tokens, &mut idx, "CASCADE");
        let restrict = ParserHelpers::consume_keyword(tokens, &mut idx, "RESTRICT");

        Ok(Some(CustomStatement::DropType {
            if_exists,
            names,
            cascade,
            restrict,
        }))
    }

    pub fn parse_create_domain(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DOMAIN") {
            return Ok(None);
        }

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        if !ParserHelpers::consume_keyword(tokens, &mut idx, "AS") {
            return Err(Error::parse_error(
                "Expected AS after domain name".to_string(),
            ));
        }

        let base_type = Self::parse_data_type(tokens, &mut idx)?;

        let mut default_value = None;
        let mut not_null = false;
        let mut constraints = Vec::new();

        while idx < tokens.len() {
            if matches!(tokens.get(idx), Some(Token::SemiColon)) {
                break;
            }

            if ParserHelpers::consume_keyword(tokens, &mut idx, "DEFAULT") {
                default_value = Some(Self::parse_expression_until_clause(tokens, &mut idx)?);
            } else if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "NOT", "NULL") {
                not_null = true;
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CONSTRAINT") {
                let constraint_name = Self::parse_identifier(tokens, &mut idx)?;
                if !ParserHelpers::consume_keyword(tokens, &mut idx, "CHECK") {
                    return Err(Error::parse_error(
                        "Expected CHECK after constraint name".to_string(),
                    ));
                }
                let expression = Self::parse_parenthesized_expression(tokens, &mut idx)?;
                constraints.push(DomainConstraint {
                    name: Some(constraint_name),
                    expression,
                });
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "CHECK") {
                let expression = Self::parse_parenthesized_expression(tokens, &mut idx)?;
                constraints.push(DomainConstraint {
                    name: None,
                    expression,
                });
            } else {
                break;
            }
        }

        Ok(Some(CustomStatement::CreateDomain {
            name,
            base_type,
            default_value,
            not_null,
            constraints,
        }))
    }

    pub fn parse_alter_domain(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "ALTER") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DOMAIN") {
            return Ok(None);
        }

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        let action = if ParserHelpers::consume_keyword(tokens, &mut idx, "ADD") {
            let constraint_name = if ParserHelpers::consume_keyword(tokens, &mut idx, "CONSTRAINT")
            {
                Some(Self::parse_identifier(tokens, &mut idx)?)
            } else {
                None
            };
            if !ParserHelpers::consume_keyword(tokens, &mut idx, "CHECK") {
                return Err(Error::parse_error(
                    "Expected CHECK after ADD [CONSTRAINT name]".to_string(),
                ));
            }
            let expression = Self::parse_parenthesized_expression(tokens, &mut idx)?;
            AlterDomainAction::AddConstraint {
                name: constraint_name,
                expression,
            }
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "DROP") {
            if ParserHelpers::consume_keyword(tokens, &mut idx, "CONSTRAINT") {
                let constraint_name = Self::parse_identifier(tokens, &mut idx)?;
                AlterDomainAction::DropConstraint {
                    name: constraint_name,
                }
            } else if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "NOT", "NULL") {
                AlterDomainAction::DropNotNull
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "DEFAULT") {
                AlterDomainAction::DropDefault
            } else {
                return Err(Error::parse_error(
                    "Expected CONSTRAINT, DEFAULT, or NOT NULL after DROP".to_string(),
                ));
            }
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "SET") {
            if ParserHelpers::consume_keyword_pair(tokens, &mut idx, "NOT", "NULL") {
                AlterDomainAction::SetNotNull
            } else if ParserHelpers::consume_keyword(tokens, &mut idx, "DEFAULT") {
                let value = Self::parse_expression_until_clause(tokens, &mut idx)?;
                AlterDomainAction::SetDefault { value }
            } else {
                return Err(Error::parse_error(
                    "Expected NOT NULL or DEFAULT after SET".to_string(),
                ));
            }
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "RENAME") {
            if !ParserHelpers::consume_keyword(tokens, &mut idx, "CONSTRAINT") {
                return Err(Error::parse_error(
                    "Expected CONSTRAINT after RENAME".to_string(),
                ));
            }
            let old_name = Self::parse_identifier(tokens, &mut idx)?;
            if !ParserHelpers::consume_keyword(tokens, &mut idx, "TO") {
                return Err(Error::parse_error(
                    "Expected TO after old constraint name".to_string(),
                ));
            }
            let new_name = Self::parse_identifier(tokens, &mut idx)?;
            AlterDomainAction::RenameConstraint { old_name, new_name }
        } else if ParserHelpers::consume_keyword(tokens, &mut idx, "VALIDATE") {
            if !ParserHelpers::consume_keyword(tokens, &mut idx, "CONSTRAINT") {
                return Err(Error::parse_error(
                    "Expected CONSTRAINT after VALIDATE".to_string(),
                ));
            }
            let constraint_name = Self::parse_identifier(tokens, &mut idx)?;
            AlterDomainAction::ValidateConstraint {
                name: constraint_name,
            }
        } else {
            return Err(Error::parse_error(
                "Expected ADD, DROP, SET, RENAME, or VALIDATE after domain name".to_string(),
            ));
        };

        Ok(Some(CustomStatement::AlterDomain { name, action }))
    }

    pub fn parse_drop_domain(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DROP") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DOMAIN") {
            return Ok(None);
        }

        let if_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "EXISTS");

        let mut names = Vec::new();
        loop {
            names.push(ParserHelpers::parse_object_name_at(tokens, &mut idx)?);

            if matches!(tokens.get(idx), Some(Token::Comma)) {
                idx += 1;
                continue;
            } else {
                break;
            }
        }

        let cascade = ParserHelpers::consume_keyword(tokens, &mut idx, "CASCADE");
        let restrict = ParserHelpers::consume_keyword(tokens, &mut idx, "RESTRICT");

        Ok(Some(CustomStatement::DropDomain {
            if_exists,
            names,
            cascade,
            restrict,
        }))
    }

    fn parse_data_type(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        let mut type_parts = Vec::new();
        let mut paren_depth = 0;

        while *idx < tokens.len() {
            if paren_depth == 0 {
                if let Some(Token::Word(w)) = tokens.get(*idx) {
                    let upper = w.value.to_uppercase();
                    if matches!(upper.as_str(), "DEFAULT" | "NOT" | "CONSTRAINT" | "CHECK") {
                        break;
                    }
                }
                if matches!(tokens.get(*idx), Some(Token::SemiColon)) {
                    break;
                }
            }

            match tokens.get(*idx) {
                Some(Token::LParen) => {
                    type_parts.push("(".to_string());
                    paren_depth += 1;
                    *idx += 1;
                }
                Some(Token::RParen) => {
                    if paren_depth > 0 {
                        type_parts.push(")".to_string());
                        paren_depth -= 1;
                        *idx += 1;
                    } else {
                        break;
                    }
                }
                Some(Token::Comma) if paren_depth > 0 => {
                    type_parts.push(",".to_string());
                    *idx += 1;
                }
                Some(Token::Word(w)) => {
                    type_parts.push(w.value.clone());
                    *idx += 1;
                }
                Some(Token::Number(n, _)) => {
                    type_parts.push(n.clone());
                    *idx += 1;
                }
                _ => break,
            }
        }

        if type_parts.is_empty() {
            return Err(Error::parse_error("Expected data type".to_string()));
        }

        let mut result = String::new();
        for (i, part) in type_parts.iter().enumerate() {
            if i > 0
                && !part.starts_with('(')
                && !part.starts_with(')')
                && !part.starts_with(',')
                && !type_parts[i - 1].ends_with('(')
                && !type_parts[i - 1].ends_with(',')
            {
                result.push(' ');
            }
            result.push_str(part);
        }

        Ok(result)
    }

    fn parse_identifier(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        match tokens.get(*idx) {
            Some(Token::Word(w)) => {
                *idx += 1;
                Ok(w.value.clone())
            }
            _ => Err(Error::parse_error("Expected identifier".to_string())),
        }
    }

    fn parse_parenthesized_expression(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        if !matches!(tokens.get(*idx), Some(Token::LParen)) {
            return Err(Error::parse_error(
                "Expected '(' to start expression".to_string(),
            ));
        }
        *idx += 1;

        let mut expr_parts = Vec::new();
        let mut paren_depth = 1;

        while *idx < tokens.len() && paren_depth > 0 {
            match tokens.get(*idx) {
                Some(Token::LParen) => {
                    expr_parts.push("(".to_string());
                    paren_depth += 1;
                    *idx += 1;
                }
                Some(Token::RParen) => {
                    paren_depth -= 1;
                    if paren_depth > 0 {
                        expr_parts.push(")".to_string());
                    }
                    *idx += 1;
                }
                Some(token) => {
                    expr_parts.push(Self::token_to_string(token));
                    *idx += 1;
                }
                None => break,
            }
        }

        if paren_depth != 0 {
            return Err(Error::parse_error("Unbalanced parentheses".to_string()));
        }

        Ok(Self::join_expression_parts(&expr_parts))
    }

    fn parse_expression_until_clause(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        let mut expr_parts = Vec::new();
        let mut paren_depth = 0;

        while *idx < tokens.len() {
            if paren_depth == 0 {
                if let Some(Token::Word(w)) = tokens.get(*idx) {
                    let upper = w.value.to_uppercase();
                    if matches!(upper.as_str(), "NOT" | "CONSTRAINT" | "CHECK") {
                        break;
                    }
                }
                if matches!(tokens.get(*idx), Some(Token::SemiColon)) {
                    break;
                }
            }

            match tokens.get(*idx) {
                Some(Token::LParen) => {
                    expr_parts.push("(".to_string());
                    paren_depth += 1;
                    *idx += 1;
                }
                Some(Token::RParen) => {
                    if paren_depth > 0 {
                        expr_parts.push(")".to_string());
                        paren_depth -= 1;
                        *idx += 1;
                    } else {
                        break;
                    }
                }
                Some(token) => {
                    expr_parts.push(Self::token_to_string(token));
                    *idx += 1;
                }
                None => break,
            }
        }

        if expr_parts.is_empty() {
            return Err(Error::parse_error("Expected expression".to_string()));
        }

        Ok(Self::join_expression_parts(&expr_parts))
    }

    fn token_to_string(token: &Token) -> String {
        match token {
            Token::Word(w) => w.value.clone(),
            Token::Number(n, _) => n.clone(),
            Token::SingleQuotedString(s) => format!("'{}'", s),
            Token::DoubleQuotedString(s) => format!("\"{}\"", s),
            Token::Comma => ",".to_string(),
            Token::Period => ".".to_string(),
            Token::Plus => "+".to_string(),
            Token::Minus => "-".to_string(),
            Token::Mul => "*".to_string(),
            Token::Div => "/".to_string(),
            Token::Mod => "%".to_string(),
            Token::Eq => "=".to_string(),
            Token::Neq => "<>".to_string(),
            Token::Lt => "<".to_string(),
            Token::Gt => ">".to_string(),
            Token::LtEq => "<=".to_string(),
            Token::GtEq => ">=".to_string(),
            Token::LParen => "(".to_string(),
            Token::RParen => ")".to_string(),
            Token::LBracket => "[".to_string(),
            Token::RBracket => "]".to_string(),
            Token::Colon => ":".to_string(),
            Token::DoubleColon => "::".to_string(),
            Token::Ampersand => "&".to_string(),
            Token::Pipe => "|".to_string(),
            Token::Caret => "^".to_string(),
            Token::ExclamationMark => "!".to_string(),
            _ => format!("{}", token),
        }
    }

    fn join_expression_parts(parts: &[String]) -> String {
        let mut result = String::new();
        for (i, part) in parts.iter().enumerate() {
            if i > 0 {
                let prev = &parts[i - 1];

                let needs_space = !part.starts_with(')')
                    && !part.starts_with(',')
                    && !part.starts_with('.')
                    && !prev.ends_with('(')
                    && !prev.ends_with('.');

                if needs_space {
                    result.push(' ');
                }
            }
            result.push_str(part);
        }
        result
    }

    fn parse_composite_type_field(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<CompositeTypeField> {
        let name = match tokens.get(*idx) {
            Some(Token::Word(word)) => {
                *idx += 1;
                word.value.clone()
            }
            Some(Token::SingleQuotedString(s)) | Some(Token::DoubleQuotedString(s)) => {
                *idx += 1;
                s.clone()
            }
            _ => {
                return Err(Error::parse_error(
                    "Expected field name in composite type".to_string(),
                ));
            }
        };

        let data_type = ParserHelpers::parse_data_type(tokens, idx)?;

        Ok(CompositeTypeField { name, data_type })
    }

    fn parse_diagnostics_assignment(
        tokens: &[&Token],
        idx: &mut usize,
    ) -> Result<DiagnosticsAssignment> {
        if !matches!(tokens.get(*idx), Some(Token::Colon)) {
            return Err(Error::parse_error(
                "Expected ':' before diagnostics target identifier",
            ));
        }
        *idx += 1;

        let target = Self::parse_identifier_token(tokens, idx)?;

        if !matches!(tokens.get(*idx), Some(Token::Eq)) {
            return Err(Error::parse_error(
                "Expected '=' in GET DIAGNOSTICS assignment",
            ));
        }
        *idx += 1;

        let item = Self::parse_diagnostics_item(tokens, idx)?;

        Ok(DiagnosticsAssignment { target, item })
    }

    fn parse_diagnostics_item(tokens: &[&Token], idx: &mut usize) -> Result<DiagnosticsItem> {
        match tokens.get(*idx) {
            Some(Token::Word(word)) => {
                *idx += 1;
                match word.value.to_uppercase().as_str() {
                    "RETURNED_SQLSTATE" => Ok(DiagnosticsItem::ReturnedSqlstate),
                    "MESSAGE_TEXT" => Ok(DiagnosticsItem::MessageText),
                    "ROW_COUNT" => Ok(DiagnosticsItem::RowCount),
                    other => Err(Error::parse_error(format!(
                        "Unsupported GET DIAGNOSTICS item '{}': expected RETURNED_SQLSTATE, MESSAGE_TEXT, or ROW_COUNT",
                        other
                    ))),
                }
            }
            _ => Err(Error::parse_error(
                "Expected diagnostics item keyword (RETURNED_SQLSTATE, MESSAGE_TEXT, ROW_COUNT)",
            )),
        }
    }

    fn parse_identifier_token(tokens: &[&Token], idx: &mut usize) -> Result<String> {
        match tokens.get(*idx) {
            Some(Token::Word(word)) => {
                *idx += 1;
                Ok(word.value.clone())
            }
            _ => Err(Error::parse_error(
                "Expected identifier in GET DIAGNOSTICS assignment",
            )),
        }
    }

    fn match_and_build_custom_statement<F>(
        tokens: &[&Token],
        pattern: &[TokenPattern],
        builder: F,
    ) -> Result<Option<CustomStatement>>
    where
        F: FnOnce(&crate::pattern_matcher::PatternMatch) -> Result<CustomStatement>,
    {
        match PatternMatcher::match_pattern(tokens, pattern)? {
            Some(matched) => Ok(Some(builder(&matched)?)),
            None => Ok(None),
        }
    }

    pub fn parse_exists_table(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let pattern = vec![
            TokenPattern::Keyword("EXISTS"),
            TokenPattern::Keyword("TABLE"),
            TokenPattern::ObjectName,
        ];

        Self::match_and_build_custom_statement(tokens, &pattern, |matched| {
            let name = matched
                .first_object_name()
                .ok_or_else(|| Error::parse_error("EXISTS TABLE requires table name"))?;
            Ok(CustomStatement::ExistsTable { name: name.clone() })
        })
    }

    pub fn parse_exists_database(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let pattern = vec![
            TokenPattern::Keyword("EXISTS"),
            TokenPattern::Keyword("DATABASE"),
            TokenPattern::ObjectName,
        ];

        Self::match_and_build_custom_statement(tokens, &pattern, |matched| {
            let name = matched
                .first_object_name()
                .ok_or_else(|| Error::parse_error("EXISTS DATABASE requires database name"))?;
            Ok(CustomStatement::ExistsDatabase { name: name.clone() })
        })
    }

    pub fn parse_loop_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();

        let (label, rest) = Self::extract_label(trimmed);

        if !rest.to_uppercase().starts_with("LOOP") {
            return Ok(None);
        }

        let after_loop = rest[4..].trim();

        let end_pattern_upper = if let Some(ref label) = label {
            format!("END LOOP {}", label.to_uppercase())
        } else {
            "END LOOP".to_string()
        };
        let end_idx =
            Self::find_end_keyword(&after_loop.to_uppercase(), &end_pattern_upper, "LOOP");
        let end_idx = end_idx.ok_or_else(|| Error::parse_error("LOOP requires END LOOP"))?;

        let body = after_loop[..end_idx].trim().to_string();

        Ok(Some(CustomStatement::Loop { label, body }))
    }

    pub fn parse_repeat_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();

        let (label, rest) = Self::extract_label(trimmed);

        if !rest.to_uppercase().starts_with("REPEAT") {
            return Ok(None);
        }

        let after_repeat = rest[6..].trim();

        let upper = after_repeat.to_uppercase();
        let until_idx = upper.rfind("UNTIL").ok_or_else(|| {
            Error::parse_error("REPEAT requires UNTIL condition before END REPEAT")
        })?;

        let body = after_repeat[..until_idx].trim().to_string();
        let after_until = after_repeat[until_idx + 5..].trim();

        let end_pattern_upper = if let Some(ref label) = label {
            format!("END REPEAT {}", label.to_uppercase())
        } else {
            "END REPEAT".to_string()
        };
        let end_idx = after_until
            .to_uppercase()
            .find(&end_pattern_upper)
            .ok_or_else(|| Error::parse_error("REPEAT requires END REPEAT"))?;

        let until_condition = after_until[..end_idx].trim().to_string();

        Ok(Some(CustomStatement::Repeat {
            label,
            body,
            until_condition,
        }))
    }

    pub fn parse_for_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();

        let (label, rest) = Self::extract_label(trimmed);

        if !rest.to_uppercase().starts_with("FOR") {
            return Ok(None);
        }

        let after_for = rest[3..].trim();

        let upper = after_for.to_uppercase();
        let in_idx = upper
            .find(" IN ")
            .ok_or_else(|| Error::parse_error("FOR requires IN keyword"))?;

        let variable = after_for[..in_idx].trim().to_string();
        let after_in = after_for[in_idx + 4..].trim();

        let do_idx = after_in
            .to_uppercase()
            .find(" DO")
            .ok_or_else(|| Error::parse_error("FOR requires DO keyword after query"))?;

        let query_part = after_in[..do_idx].trim();
        let query = if query_part.starts_with('(') && query_part.ends_with(')') {
            query_part[1..query_part.len() - 1].to_string()
        } else {
            query_part.to_string()
        };

        let after_do = after_in[do_idx + 3..].trim();

        let end_pattern_upper = if let Some(ref label) = label {
            format!("END FOR {}", label.to_uppercase())
        } else {
            "END FOR".to_string()
        };
        let end_idx = after_do
            .to_uppercase()
            .find(&end_pattern_upper)
            .ok_or_else(|| Error::parse_error("FOR requires END FOR"))?;

        let body = after_do[..end_idx].trim().to_string();

        Ok(Some(CustomStatement::For {
            label,
            variable,
            query,
            body,
        }))
    }

    pub fn parse_leave_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        if !upper.starts_with("LEAVE") {
            return Ok(None);
        }

        let after_leave = trimmed[5..].trim();

        let label = if after_leave.is_empty() || after_leave == ";" {
            None
        } else {
            let label_str = after_leave.trim_end_matches(';').trim();
            if label_str.is_empty() {
                None
            } else {
                Some(label_str.to_string())
            }
        };

        Ok(Some(CustomStatement::Leave { label }))
    }

    pub fn parse_continue_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        if !upper.starts_with("CONTINUE") {
            return Ok(None);
        }

        let after_continue = trimmed[8..].trim();

        let label = if after_continue.is_empty() || after_continue == ";" {
            None
        } else {
            let label_str = after_continue.trim_end_matches(';').trim();
            if label_str.is_empty() {
                None
            } else {
                Some(label_str.to_string())
            }
        };

        Ok(Some(CustomStatement::Continue { label }))
    }

    pub fn parse_break_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        if !upper.starts_with("BREAK") {
            return Ok(None);
        }

        let after_break = trimmed[5..].trim();

        let label = if after_break.is_empty() || after_break == ";" {
            None
        } else {
            let label_str = after_break.trim_end_matches(';').trim();
            if label_str.is_empty() {
                None
            } else {
                Some(label_str.to_string())
            }
        };

        Ok(Some(CustomStatement::Break { label }))
    }

    fn extract_label(sql: &str) -> (Option<String>, &str) {
        let first_colon = sql.find(':');
        if let Some(colon_idx) = first_colon {
            let potential_label = sql[..colon_idx].trim();
            if !potential_label.is_empty()
                && potential_label
                    .chars()
                    .all(|c| c.is_alphanumeric() || c == '_')
            {
                let first_keyword = sql[colon_idx + 1..].trim();
                let upper = first_keyword.to_uppercase();
                if upper.starts_with("LOOP")
                    || upper.starts_with("REPEAT")
                    || upper.starts_with("FOR")
                    || upper.starts_with("BEGIN")
                    || upper.starts_with("WHILE")
                {
                    return (Some(potential_label.to_string()), first_keyword);
                }
            }
        }
        (None, sql)
    }

    fn find_end_keyword(sql_upper: &str, end_pattern: &str, block_type: &str) -> Option<usize> {
        let mut depth = 0;
        let mut pos = 0;
        let end_block = format!("END {}", block_type);

        while pos < sql_upper.len() {
            if sql_upper[pos..].starts_with(&end_block) {
                if sql_upper[pos..].starts_with(end_pattern) {
                    if depth == 0 {
                        return Some(pos);
                    }
                    depth -= 1;
                }
                pos += end_block.len();
                continue;
            }

            if sql_upper[pos..].starts_with(block_type) {
                let before = if pos > 0 {
                    sql_upper.as_bytes()[pos - 1]
                } else {
                    b' '
                };
                if !before.is_ascii_alphanumeric() && before != b'_' {
                    depth += 1;
                }
                pos += block_type.len();
                continue;
            }

            pos += 1;
        }

        None
    }

    pub fn parse_create_snapshot_table(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "CREATE") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SNAPSHOT") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return Ok(None);
        }

        let if_not_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "NOT")
            && ParserHelpers::consume_keyword(tokens, &mut idx, "EXISTS");

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        if !ParserHelpers::consume_keyword(tokens, &mut idx, "CLONE") {
            return Err(Error::parse_error(
                "CREATE SNAPSHOT TABLE requires CLONE clause".to_string(),
            ));
        }

        let source_table = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        let mut for_system_time = None;
        if ParserHelpers::consume_keyword(tokens, &mut idx, "FOR")
            && ParserHelpers::consume_keyword(tokens, &mut idx, "SYSTEM_TIME")
        {
            if !ParserHelpers::consume_keyword(tokens, &mut idx, "AS") {
                return Err(Error::parse_error(
                    "FOR SYSTEM_TIME requires AS OF clause".to_string(),
                ));
            }
            if !ParserHelpers::consume_keyword(tokens, &mut idx, "OF") {
                return Err(Error::parse_error(
                    "FOR SYSTEM_TIME requires AS OF clause".to_string(),
                ));
            }
            let expr_start = idx;
            let mut paren_depth = 0;
            while idx < tokens.len() {
                match tokens.get(idx) {
                    Some(Token::LParen) => {
                        paren_depth += 1;
                        idx += 1;
                    }
                    Some(Token::RParen) => {
                        if paren_depth > 0 {
                            paren_depth -= 1;
                            idx += 1;
                        } else {
                            break;
                        }
                    }
                    Some(Token::SemiColon) => break,
                    Some(Token::Word(w))
                        if paren_depth == 0 && w.value.eq_ignore_ascii_case("OPTIONS") =>
                    {
                        break;
                    }
                    _ => idx += 1,
                }
            }
            let expr_tokens = &tokens[expr_start..idx];
            let expr_str = Self::tokens_to_string(expr_tokens);
            for_system_time = Some(expr_str);
        }

        let mut options = Vec::new();
        if ParserHelpers::consume_keyword(tokens, &mut idx, "OPTIONS") {
            if !matches!(tokens.get(idx), Some(Token::LParen)) {
                return Err(Error::parse_error(
                    "OPTIONS requires parentheses".to_string(),
                ));
            }
            idx += 1;

            while idx < tokens.len() {
                if matches!(tokens.get(idx), Some(Token::RParen)) {
                    break;
                }

                let key = match tokens.get(idx) {
                    Some(Token::Word(w)) => {
                        idx += 1;
                        w.value.clone()
                    }
                    _ => break,
                };

                if !matches!(tokens.get(idx), Some(Token::Eq)) {
                    return Err(Error::parse_error("Expected '=' in OPTIONS".to_string()));
                }
                idx += 1;

                let value_start = idx;
                let mut paren_depth = 0;
                while idx < tokens.len() {
                    match tokens.get(idx) {
                        Some(Token::LParen) => {
                            paren_depth += 1;
                            idx += 1;
                        }
                        Some(Token::RParen) => {
                            if paren_depth > 0 {
                                paren_depth -= 1;
                                idx += 1;
                            } else {
                                break;
                            }
                        }
                        Some(Token::Comma) if paren_depth == 0 => break,
                        _ => idx += 1,
                    }
                }
                let value_tokens = &tokens[value_start..idx];
                let value = Self::tokens_to_string(value_tokens);
                options.push((key, value));

                if matches!(tokens.get(idx), Some(Token::Comma)) {
                    idx += 1;
                }
            }
        }

        Ok(Some(CustomStatement::CreateSnapshotTable {
            name,
            source_table,
            if_not_exists,
            for_system_time,
            options,
        }))
    }

    pub fn parse_drop_snapshot_table(tokens: &[&Token]) -> Result<Option<CustomStatement>> {
        let mut idx = 0;

        if !ParserHelpers::expect_keyword(tokens, &mut idx, "DROP") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "SNAPSHOT") {
            return Ok(None);
        }
        if !ParserHelpers::expect_keyword(tokens, &mut idx, "TABLE") {
            return Ok(None);
        }

        let if_exists = ParserHelpers::consume_keyword_pair(tokens, &mut idx, "IF", "EXISTS");

        let name = ParserHelpers::parse_object_name_at(tokens, &mut idx)?;

        Ok(Some(CustomStatement::DropSnapshotTable { name, if_exists }))
    }

    fn tokens_to_string(tokens: &[&Token]) -> String {
        let mut result = String::new();
        for (i, token) in tokens.iter().enumerate() {
            if i > 0 {
                result.push(' ');
            }
            result.push_str(&Self::token_to_string(token));
        }
        result
    }

    pub fn parse_while_statement(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();

        let (label, rest) = Self::extract_label(trimmed);

        if !rest.to_uppercase().starts_with("WHILE") {
            return Ok(None);
        }

        let after_while = rest[5..].trim();
        let upper = after_while.to_uppercase();

        let do_idx = upper
            .find(" DO")
            .ok_or_else(|| Error::parse_error("WHILE requires DO keyword"))?;

        let condition = after_while[..do_idx].trim().to_string();
        let after_do = after_while[do_idx + 3..].trim();

        let end_pattern_upper = if let Some(ref label) = label {
            format!("END WHILE {}", label.to_uppercase())
        } else {
            "END WHILE".to_string()
        };

        let end_idx = after_do
            .to_uppercase()
            .find(&end_pattern_upper)
            .ok_or_else(|| Error::parse_error("WHILE requires END WHILE"))?;

        let body = after_do[..end_idx].trim().to_string();

        Ok(Some(CustomStatement::While {
            label,
            condition,
            body,
        }))
    }

    pub fn parse_export_data(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        if !upper.starts_with("EXPORT DATA") && !upper.starts_with("EXPORT  DATA") {
            return Ok(None);
        }

        let options_start = upper
            .find("OPTIONS")
            .ok_or_else(|| Error::parse_error("EXPORT DATA requires OPTIONS clause".to_string()))?;

        let options_paren_start = trimmed[options_start..].find('(').ok_or_else(|| {
            Error::parse_error("EXPORT DATA OPTIONS requires parentheses".to_string())
        })? + options_start;

        let mut depth = 1;
        let mut options_paren_end = options_paren_start + 1;
        for (i, ch) in trimmed[options_paren_start + 1..].char_indices() {
            match ch {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if depth == 0 {
                        options_paren_end = options_paren_start + 1 + i;
                        break;
                    }
                }
                _ => {}
            }
        }

        let options_str = &trimmed[options_paren_start + 1..options_paren_end];

        let mut uri = String::new();
        let mut format = ExportFormat::Csv;
        let mut overwrite = false;
        let mut header = false;
        let mut field_delimiter = None;
        let mut compression = None;

        for part in options_str.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            let eq_idx = match part.find('=') {
                Some(idx) => idx,
                None => continue,
            };

            let key = part[..eq_idx].trim().to_uppercase();
            let value = part[eq_idx + 1..].trim();
            let value_unquoted = value.trim_matches(|c| c == '\'' || c == '"');

            match key.as_str() {
                "URI" => uri = value_unquoted.to_string(),
                "FORMAT" => {
                    format = match value_unquoted.to_uppercase().as_str() {
                        "CSV" => ExportFormat::Csv,
                        "JSON" => ExportFormat::Json,
                        "PARQUET" => ExportFormat::Parquet,
                        "AVRO" => ExportFormat::Avro,
                        _ => {
                            return Err(Error::parse_error(format!(
                                "Unknown EXPORT DATA format: {}",
                                value_unquoted
                            )));
                        }
                    };
                }
                "OVERWRITE" => overwrite = value_unquoted.to_uppercase() == "TRUE",
                "HEADER" => header = value_unquoted.to_uppercase() == "TRUE",
                "FIELD_DELIMITER" => {
                    if !value_unquoted.is_empty() {
                        field_delimiter = Some(value_unquoted.chars().next().unwrap());
                    }
                }
                "COMPRESSION" => compression = Some(value_unquoted.to_string()),
                _ => {}
            }
        }

        let as_idx = upper[options_paren_end..].find(" AS ").ok_or_else(|| {
            Error::parse_error("EXPORT DATA requires AS clause with query".to_string())
        })? + options_paren_end;

        let query = trimmed[as_idx + 4..]
            .trim()
            .trim_end_matches(';')
            .to_string();

        Ok(Some(CustomStatement::ExportData {
            uri,
            format,
            overwrite,
            header,
            field_delimiter,
            compression,
            query,
        }))
    }

    pub fn parse_load_data(sql: &str) -> Result<Option<CustomStatement>> {
        let trimmed = sql.trim();
        let upper = trimmed.to_uppercase();

        if !upper.starts_with("LOAD DATA") && !upper.starts_with("LOAD  DATA") {
            return Ok(None);
        }

        let after_load_data = &trimmed[9..].trim_start();
        let after_load_data_upper = after_load_data.to_uppercase();

        let overwrite = after_load_data_upper.starts_with("OVERWRITE");
        let table_start = if overwrite {
            after_load_data[9..].trim_start()
        } else if after_load_data_upper.starts_with("INTO") {
            after_load_data[4..].trim_start()
        } else {
            return Err(Error::parse_error(
                "LOAD DATA requires INTO or OVERWRITE keyword".to_string(),
            ));
        };

        let table_start_upper = table_start.to_uppercase();
        let is_temp = table_start_upper.starts_with("TEMP TABLE")
            || table_start_upper.starts_with("TEMP  TABLE")
            || table_start_upper.starts_with("TEMPORARY TABLE");

        let table_name_start = if is_temp {
            if table_start_upper.starts_with("TEMP TABLE") {
                table_start[10..].trim_start()
            } else if table_start_upper.starts_with("TEMP  TABLE") {
                table_start[11..].trim_start()
            } else {
                table_start[15..].trim_start()
            }
        } else {
            table_start
        };

        let table_name_end = table_name_start
            .find(|c: char| c.is_whitespace() || c == '(')
            .unwrap_or(table_name_start.len());

        let table_name_str = &table_name_start[..table_name_end];
        let table_name = ParserHelpers::object_name_from_string(table_name_str);

        let after_table_name = table_name_start[table_name_end..].trim_start();

        let temp_table_schema = if is_temp && after_table_name.starts_with('(') {
            let close_paren = after_table_name.find(')').ok_or_else(|| {
                Error::parse_error("LOAD DATA INTO TEMP TABLE schema requires closing ')'")
            })?;
            let schema_str = &after_table_name[1..close_paren];
            let schema: Vec<(String, String)> = schema_str
                .split(',')
                .map(|s| {
                    let parts: Vec<&str> = s.trim().splitn(2, char::is_whitespace).collect();
                    if parts.len() == 2 {
                        (parts[0].to_string(), parts[1].trim().to_string())
                    } else {
                        (parts[0].to_string(), "STRING".to_string())
                    }
                })
                .collect();
            Some(schema)
        } else {
            None
        };

        let from_idx = upper.find("FROM FILES").ok_or_else(|| {
            Error::parse_error("LOAD DATA requires FROM FILES clause".to_string())
        })?;

        let files_start = trimmed[from_idx + 10..].trim_start();
        let files_paren_start = files_start.find('(').ok_or_else(|| {
            Error::parse_error("LOAD DATA FROM FILES requires parentheses".to_string())
        })?;

        let mut depth = 1;
        let mut files_paren_end = files_paren_start + 1;
        for (i, ch) in files_start[files_paren_start + 1..].char_indices() {
            match ch {
                '(' | '[' => depth += 1,
                ')' | ']' => {
                    depth -= 1;
                    if depth == 0 {
                        files_paren_end = files_paren_start + 1 + i;
                        break;
                    }
                }
                _ => {}
            }
        }

        let files_str = &files_start[files_paren_start + 1..files_paren_end];

        let mut format = ExportFormat::Csv;
        let mut uris: Vec<String> = Vec::new();
        let mut allow_schema_update = false;

        for part in files_str.split(',') {
            let part = part.trim();
            if part.is_empty() {
                continue;
            }

            if let Some(eq_idx) = part.find('=') {
                let key = part[..eq_idx].trim().to_uppercase();
                let value = part[eq_idx + 1..].trim();
                let value_unquoted = value.trim_matches(|c| c == '\'' || c == '"');

                match key.as_str() {
                    "FORMAT" => {
                        format = match value_unquoted.to_uppercase().as_str() {
                            "CSV" => ExportFormat::Csv,
                            "JSON" => ExportFormat::Json,
                            "PARQUET" => ExportFormat::Parquet,
                            "AVRO" => ExportFormat::Avro,
                            _ => {
                                return Err(Error::parse_error(format!(
                                    "Unknown LOAD DATA format: {}",
                                    value_unquoted
                                )));
                            }
                        };
                    }
                    "URIS" => {
                        let uris_str = value.trim_start_matches('[').trim_end_matches(']');
                        for uri in uris_str.split(',') {
                            let uri_trimmed = uri.trim().trim_matches(|c| c == '\'' || c == '"');
                            if !uri_trimmed.is_empty() {
                                uris.push(uri_trimmed.to_string());
                            }
                        }
                    }
                    "ALLOW_SCHEMA_UPDATE" => {
                        allow_schema_update = value_unquoted.to_uppercase() == "TRUE";
                    }
                    _ => {}
                }
            } else if part.starts_with('\'') || part.starts_with('"') {
                let uri = part.trim_matches(|c| c == '\'' || c == '"');
                if !uri.is_empty() {
                    uris.push(uri.to_string());
                }
            }
        }

        Ok(Some(CustomStatement::LoadData {
            table_name,
            overwrite,
            is_temp,
            temp_table_schema,
            format,
            uris,
            allow_schema_update,
        }))
    }
}
