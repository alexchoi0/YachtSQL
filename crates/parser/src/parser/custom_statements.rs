use sqlparser::tokenizer::Token;
use yachtsql_core::error::{Error, Result};

use super::helpers::ParserHelpers;
use crate::pattern_matcher::{PatternMatcher, TokenPattern};
use crate::validator::{
    AlterDomainAction, CompositeTypeField, CustomStatement, DiagnosticsAssignment, DiagnosticsItem,
    DiagnosticsScope, DomainConstraint, SetConstraintsMode, SetConstraintsTarget,
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
}
