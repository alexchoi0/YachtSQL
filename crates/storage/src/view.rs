use std::collections::HashMap;

use yachtsql_core::error::{Error, Result};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum WithCheckOption {
    #[default]
    None,
    Local,
    Cascaded,
}

#[derive(Debug, Clone)]
pub struct ViewDefinition {
    pub name: String,
    pub sql: String,
    pub dependencies: Vec<String>,
    pub with_check_option: WithCheckOption,
    pub where_clause: Option<String>,
    pub materialized: bool,
    pub materialized_data: Option<Vec<crate::Row>>,
    pub materialized_schema: Option<crate::Schema>,
}

impl ViewDefinition {
    pub fn new(name: String, sql: String, dependencies: Vec<String>) -> Self {
        Self {
            name,
            sql,
            dependencies,
            with_check_option: WithCheckOption::None,
            where_clause: None,
            materialized: false,
            materialized_data: None,
            materialized_schema: None,
        }
    }

    pub fn new_materialized(name: String, sql: String, dependencies: Vec<String>) -> Self {
        Self {
            name,
            sql,
            dependencies,
            with_check_option: WithCheckOption::None,
            where_clause: None,
            materialized: true,
            materialized_data: Some(Vec::new()),
            materialized_schema: None,
        }
    }

    pub fn with_check_option(
        name: String,
        sql: String,
        dependencies: Vec<String>,
        check_option: WithCheckOption,
    ) -> Self {
        Self {
            name,
            sql,
            dependencies,
            with_check_option: check_option,
            where_clause: None,
            materialized: false,
            materialized_data: None,
            materialized_schema: None,
        }
    }

    pub fn set_check_option(&mut self, check_option: WithCheckOption) {
        self.with_check_option = check_option;
    }

    pub fn set_where_clause(&mut self, where_clause: Option<String>) {
        self.where_clause = where_clause;
    }

    pub fn refresh_materialized_data(&mut self, rows: Vec<crate::Row>, schema: crate::Schema) {
        if self.materialized {
            self.materialized_data = Some(rows);
            self.materialized_schema = Some(schema);
        }
    }

    pub fn get_materialized_data(&self) -> Option<&[crate::Row]> {
        self.materialized_data.as_deref()
    }

    pub fn get_materialized_schema(&self) -> Option<&crate::Schema> {
        self.materialized_schema.as_ref()
    }

    pub fn is_materialized(&self) -> bool {
        self.materialized
    }
}

#[derive(Debug, Clone, Default)]
pub struct ViewRegistry {
    views: HashMap<String, ViewDefinition>,
}

impl ViewRegistry {
    pub fn new() -> Self {
        Self {
            views: HashMap::new(),
        }
    }

    pub fn create_or_replace_view(&mut self, view: ViewDefinition) -> Result<bool> {
        if self.would_create_cycle(&view.name, &view.dependencies)? {
            return Err(Error::invalid_query(format!(
                "Circular dependency detected: view '{}' would create a cycle",
                view.name
            )));
        }

        let was_created = !self.views.contains_key(&view.name);
        self.views.insert(view.name.clone(), view);
        Ok(was_created)
    }

    pub fn drop_view(&mut self, name: &str, if_exists: bool) -> Result<bool> {
        if let Some(_view) = self.views.remove(name) {
            Ok(true)
        } else if if_exists {
            Ok(false)
        } else {
            Err(Error::invalid_query(format!(
                "View '{}' does not exist",
                name
            )))
        }
    }

    pub fn drop_view_cascade(&mut self, name: &str, if_exists: bool) -> Result<Vec<String>> {
        if !self.views.contains_key(name) {
            if if_exists {
                return Ok(Vec::new());
            } else {
                return Err(Error::invalid_query(format!(
                    "View '{}' does not exist",
                    name
                )));
            }
        }

        let dependent_views = self.find_dependent_views(name);

        for dep_view in &dependent_views {
            self.views.remove(dep_view);
        }

        self.views.remove(name);

        let mut dropped = dependent_views;
        dropped.insert(0, name.to_string());
        Ok(dropped)
    }

    pub fn drop_view_restrict(&mut self, name: &str, if_exists: bool) -> Result<bool> {
        if !self.views.contains_key(name) {
            if if_exists {
                return Ok(false);
            } else {
                return Err(Error::invalid_query(format!(
                    "View '{}' does not exist",
                    name
                )));
            }
        }

        let dependent_views = self.find_dependent_views(name);
        if !dependent_views.is_empty() {
            return Err(Error::invalid_query(format!(
                "Cannot drop view '{}': {} dependent view(s) exist: {}. Use CASCADE to drop dependents.",
                name,
                dependent_views.len(),
                dependent_views.join(", ")
            )));
        }

        self.views.remove(name);
        Ok(true)
    }

    pub fn get_view(&self, name: &str) -> Option<&ViewDefinition> {
        self.views.get(name)
    }

    pub fn get_view_mut(&mut self, name: &str) -> Option<&mut ViewDefinition> {
        self.views.get_mut(name)
    }

    pub fn exists(&self, name: &str) -> bool {
        self.views.contains_key(name)
    }

    pub fn list_views(&self) -> Vec<String> {
        self.views.keys().cloned().collect()
    }

    fn find_dependent_views(&self, name: &str) -> Vec<String> {
        let mut dependents = Vec::new();
        for (view_name, view_def) in &self.views {
            if view_def.dependencies.contains(&name.to_string()) {
                dependents.push(view_name.clone());

                let transitive_deps = self.find_dependent_views(view_name);
                dependents.extend(transitive_deps);
            }
        }
        dependents.sort();
        dependents.dedup();
        dependents
    }

    fn would_create_cycle(&self, view_name: &str, dependencies: &[String]) -> Result<bool> {
        for dep in dependencies {
            if dep == view_name {
                return Ok(true);
            }

            if let Some(dep_view) = self.views.get(dep)
                && self.view_depends_on(&dep_view.dependencies, view_name, &mut Vec::new())?
            {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn view_depends_on(
        &self,
        dependencies: &[String],
        target_view: &str,
        visited: &mut Vec<String>,
    ) -> Result<bool> {
        for dep in dependencies {
            if dep == target_view {
                return Ok(true);
            }

            if visited.contains(dep) {
                continue;
            }
            visited.push(dep.clone());

            if let Some(dep_view) = self.views.get(dep) {
                let depends = self.view_depends_on(&dep_view.dependencies, target_view, visited)?;
                if depends {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_create_and_get_view() {
        let mut registry = ViewRegistry::new();
        let view = ViewDefinition::new(
            "test_view".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );

        let was_created = registry.create_or_replace_view(view).unwrap();
        assert!(was_created);

        let retrieved = registry.get_view("test_view");
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().sql, "SELECT * FROM users");
    }

    #[test]
    fn test_replace_view() {
        let mut registry = ViewRegistry::new();
        let view1 = ViewDefinition::new(
            "test_view".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );
        registry.create_or_replace_view(view1).unwrap();

        let view2 = ViewDefinition::new(
            "test_view".to_string(),
            "SELECT id FROM users".to_string(),
            vec!["users".to_string()],
        );
        let was_created = registry.create_or_replace_view(view2).unwrap();
        assert!(!was_created);

        let retrieved = registry.get_view("test_view");
        assert_eq!(retrieved.unwrap().sql, "SELECT id FROM users");
    }

    #[test]
    fn test_drop_view() {
        let mut registry = ViewRegistry::new();
        let view = ViewDefinition::new(
            "test_view".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );
        registry.create_or_replace_view(view).unwrap();

        let was_dropped = registry.drop_view("test_view", false).unwrap();
        assert!(was_dropped);
        assert!(registry.get_view("test_view").is_none());
    }

    #[test]
    fn test_drop_view_if_exists() {
        let mut registry = ViewRegistry::new();
        let was_dropped = registry.drop_view("nonexistent", true).unwrap();
        assert!(!was_dropped);
    }

    #[test]
    fn test_circular_dependency_detection() {
        let mut registry = ViewRegistry::new();

        let view1 = ViewDefinition::new(
            "view1".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );
        registry.create_or_replace_view(view1).unwrap();

        let view2 = ViewDefinition::new(
            "view2".to_string(),
            "SELECT * FROM view1".to_string(),
            vec!["view1".to_string()],
        );
        registry.create_or_replace_view(view2).unwrap();

        let view1_circular = ViewDefinition::new(
            "view1".to_string(),
            "SELECT * FROM view2".to_string(),
            vec!["view2".to_string()],
        );
        let result = registry.create_or_replace_view(view1_circular);
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Circular dependency")
        );
    }

    #[test]
    fn test_find_dependent_views() {
        let mut registry = ViewRegistry::new();

        let view1 = ViewDefinition::new(
            "view1".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );
        registry.create_or_replace_view(view1).unwrap();

        let view2 = ViewDefinition::new(
            "view2".to_string(),
            "SELECT * FROM view1".to_string(),
            vec!["view1".to_string()],
        );
        registry.create_or_replace_view(view2).unwrap();

        let view3 = ViewDefinition::new(
            "view3".to_string(),
            "SELECT * FROM view2".to_string(),
            vec!["view2".to_string()],
        );
        registry.create_or_replace_view(view3).unwrap();

        let dependents = registry.find_dependent_views("view1");
        assert_eq!(dependents.len(), 2);
        assert!(dependents.contains(&"view2".to_string()));
        assert!(dependents.contains(&"view3".to_string()));
    }

    #[test]
    fn test_drop_view_cascade() {
        let mut registry = ViewRegistry::new();

        let view1 = ViewDefinition::new(
            "view1".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );
        registry.create_or_replace_view(view1).unwrap();

        let view2 = ViewDefinition::new(
            "view2".to_string(),
            "SELECT * FROM view1".to_string(),
            vec!["view1".to_string()],
        );
        registry.create_or_replace_view(view2).unwrap();

        let dropped = registry.drop_view_cascade("view1", false).unwrap();
        assert_eq!(dropped.len(), 2);
        assert!(dropped.contains(&"view1".to_string()));
        assert!(dropped.contains(&"view2".to_string()));

        assert!(registry.get_view("view1").is_none());
        assert!(registry.get_view("view2").is_none());
    }

    #[test]
    fn test_drop_view_restrict() {
        let mut registry = ViewRegistry::new();

        let view1 = ViewDefinition::new(
            "view1".to_string(),
            "SELECT * FROM users".to_string(),
            vec!["users".to_string()],
        );
        registry.create_or_replace_view(view1).unwrap();

        let view2 = ViewDefinition::new(
            "view2".to_string(),
            "SELECT * FROM view1".to_string(),
            vec!["view1".to_string()],
        );
        registry.create_or_replace_view(view2).unwrap();

        let result = registry.drop_view_restrict("view1", false);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("dependent"));

        let was_dropped = registry.drop_view_restrict("view2", false).unwrap();
        assert!(was_dropped);
    }
}
