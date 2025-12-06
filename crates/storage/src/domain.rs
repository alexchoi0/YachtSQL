use indexmap::IndexMap;
use yachtsql_core::error::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DomainConstraint {
    pub name: Option<String>,

    pub expression: String,

    pub validated: bool,

    pub enforced: bool,
}

impl DomainConstraint {
    pub fn new(name: Option<String>, expression: String) -> Self {
        Self {
            name,
            expression,
            validated: true,
            enforced: true,
        }
    }
}

#[derive(Debug, Clone)]
pub struct DomainDefinition {
    pub name: String,

    pub base_type: String,

    pub default_value: Option<String>,

    pub not_null: bool,

    pub constraints: Vec<DomainConstraint>,
}

impl DomainDefinition {
    pub fn new(name: String, base_type: String) -> Self {
        Self {
            name,
            base_type,
            default_value: None,
            not_null: false,
            constraints: Vec::new(),
        }
    }

    pub fn with_default(mut self, value: String) -> Self {
        self.default_value = Some(value);
        self
    }

    pub fn with_not_null(mut self) -> Self {
        self.not_null = true;
        self
    }

    pub fn with_constraint(mut self, name: Option<String>, expression: String) -> Self {
        self.constraints
            .push(DomainConstraint::new(name, expression));
        self
    }

    pub fn add_constraint(&mut self, name: Option<String>, expression: String) {
        self.constraints
            .push(DomainConstraint::new(name, expression));
    }

    pub fn drop_constraint(&mut self, constraint_name: &str) -> Result<()> {
        let initial_len = self.constraints.len();
        self.constraints
            .retain(|c| c.name.as_deref() != Some(constraint_name));

        if self.constraints.len() == initial_len {
            return Err(Error::invalid_query(format!(
                "Constraint '{}' does not exist on domain '{}'",
                constraint_name, self.name
            )));
        }

        Ok(())
    }

    pub fn rename_constraint(&mut self, old_name: &str, new_name: String) -> Result<()> {
        for constraint in &mut self.constraints {
            if constraint.name.as_deref() == Some(old_name) {
                constraint.name = Some(new_name);
                return Ok(());
            }
        }

        Err(Error::invalid_query(format!(
            "Constraint '{}' does not exist on domain '{}'",
            old_name, self.name
        )))
    }

    pub fn validate_constraint(&mut self, constraint_name: &str) -> Result<()> {
        for constraint in &mut self.constraints {
            if constraint.name.as_deref() == Some(constraint_name) {
                constraint.validated = true;
                return Ok(());
            }
        }

        Err(Error::invalid_query(format!(
            "Constraint '{}' does not exist on domain '{}'",
            constraint_name, self.name
        )))
    }

    pub fn set_default(&mut self, value: String) {
        self.default_value = Some(value);
    }

    pub fn drop_default(&mut self) {
        self.default_value = None;
    }

    pub fn set_not_null(&mut self) {
        self.not_null = true;
    }

    pub fn drop_not_null(&mut self) {
        self.not_null = false;
    }
}

#[derive(Debug, Clone, Default)]
pub struct DomainRegistry {
    domains: IndexMap<String, DomainDefinition>,
}

impl DomainRegistry {
    pub fn new() -> Self {
        Self {
            domains: IndexMap::new(),
        }
    }

    pub fn create_domain(&mut self, domain: DomainDefinition) -> Result<()> {
        let name = domain.name.clone();
        if self.domains.contains_key(&name) {
            return Err(Error::invalid_query(format!(
                "Domain '{}' already exists",
                name
            )));
        }
        self.domains.insert(name, domain);
        Ok(())
    }

    pub fn get_domain(&self, name: &str) -> Option<&DomainDefinition> {
        self.domains.get(name)
    }

    pub fn get_domain_mut(&mut self, name: &str) -> Option<&mut DomainDefinition> {
        self.domains.get_mut(name)
    }

    pub fn domain_exists(&self, name: &str) -> bool {
        self.domains.contains_key(name)
    }

    pub fn drop_domain(&mut self, name: &str, if_exists: bool) -> Result<()> {
        if self.domains.shift_remove(name).is_some() || if_exists {
            Ok(())
        } else {
            Err(Error::invalid_query(format!(
                "Domain '{}' does not exist",
                name
            )))
        }
    }

    pub fn list_domains(&self) -> Vec<&String> {
        self.domains.keys().collect()
    }

    pub fn domains(&self) -> &IndexMap<String, DomainDefinition> {
        &self.domains
    }
}
