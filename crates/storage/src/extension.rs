use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone)]
pub struct ExtensionMetadata {
    pub name: String,
    pub description: String,
    pub version: String,
    pub functions: Vec<String>,
    pub types: Vec<String>,
}

impl ExtensionMetadata {
    pub fn new(name: impl Into<String>, description: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: description.into(),
            version: "1.0".to_string(),
            functions: Vec::new(),
            types: Vec::new(),
        }
    }

    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = version.into();
        self
    }

    pub fn with_functions(mut self, functions: Vec<String>) -> Self {
        self.functions = functions;
        self
    }

    pub fn with_types(mut self, types: Vec<String>) -> Self {
        self.types = types;
        self
    }
}

#[derive(Debug, Clone)]
pub struct ExtensionRegistry {
    installed: HashSet<String>,
    available: HashMap<String, ExtensionMetadata>,
}

impl Default for ExtensionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl ExtensionRegistry {
    pub fn new() -> Self {
        let mut registry = Self {
            installed: HashSet::new(),
            available: HashMap::new(),
        };
        registry.register_builtin_extensions();
        registry
    }

    fn register_builtin_extensions(&mut self) {
        self.available.insert(
            "uuid-ossp".to_string(),
            ExtensionMetadata::new(
                "uuid-ossp",
                "Generate universally unique identifiers (UUIDs)",
            )
            .with_version("1.1")
            .with_functions(vec![
                "gen_random_uuid".to_string(),
                "uuid_generate_v1".to_string(),
                "uuid_generate_v1mc".to_string(),
                "uuid_generate_v3".to_string(),
                "uuid_generate_v4".to_string(),
                "uuid_generate_v5".to_string(),
                "uuid_nil".to_string(),
                "uuid_ns_dns".to_string(),
                "uuid_ns_url".to_string(),
                "uuid_ns_oid".to_string(),
                "uuid_ns_x500".to_string(),
            ])
            .with_types(vec!["uuid".to_string()]),
        );

        self.available.insert(
            "pgcrypto".to_string(),
            ExtensionMetadata::new("pgcrypto", "Cryptographic functions")
                .with_version("1.3")
                .with_functions(vec![
                    "gen_random_bytes".to_string(),
                    "gen_random_uuid".to_string(),
                    "digest".to_string(),
                    "hmac".to_string(),
                    "crypt".to_string(),
                    "gen_salt".to_string(),
                    "encrypt".to_string(),
                    "decrypt".to_string(),
                    "encrypt_iv".to_string(),
                    "decrypt_iv".to_string(),
                    "pgp_sym_encrypt".to_string(),
                    "pgp_sym_decrypt".to_string(),
                    "pgp_pub_encrypt".to_string(),
                    "pgp_pub_decrypt".to_string(),
                    "armor".to_string(),
                    "dearmor".to_string(),
                    "pgp_armor_headers".to_string(),
                    "pgp_key_id".to_string(),
                    "encode".to_string(),
                    "decode".to_string(),
                ]),
        );

        self.available.insert(
            "pg_trgm".to_string(),
            ExtensionMetadata::new("pg_trgm", "Text similarity using trigram matching")
                .with_version("1.6")
                .with_functions(vec![
                    "similarity".to_string(),
                    "show_trgm".to_string(),
                    "word_similarity".to_string(),
                    "strict_word_similarity".to_string(),
                    "show_limit".to_string(),
                    "set_limit".to_string(),
                ]),
        );

        self.available.insert(
            "hstore".to_string(),
            ExtensionMetadata::new("hstore", "Data type for storing key-value pairs")
                .with_version("1.8")
                .with_functions(vec![
                    "hstore".to_string(),
                    "akeys".to_string(),
                    "avals".to_string(),
                    "skeys".to_string(),
                    "svals".to_string(),
                    "each".to_string(),
                    "exist".to_string(),
                    "defined".to_string(),
                    "delete".to_string(),
                    "hs_concat".to_string(),
                    "hs_contains".to_string(),
                    "hs_contained".to_string(),
                    "tconvert".to_string(),
                    "hstore_to_json".to_string(),
                    "hstore_to_jsonb".to_string(),
                    "hstore_to_array".to_string(),
                    "hstore_to_matrix".to_string(),
                    "slice_array".to_string(),
                    "slice_hstore".to_string(),
                    "populate_record".to_string(),
                ])
                .with_types(vec!["hstore".to_string()]),
        );
    }

    pub fn is_available(&self, name: &str) -> bool {
        self.available.contains_key(name)
    }

    pub fn is_installed(&self, name: &str) -> bool {
        self.installed.contains(name)
    }

    pub fn get_extension(&self, name: &str) -> Option<&ExtensionMetadata> {
        self.available.get(name)
    }

    pub fn install(&mut self, name: &str) -> Result<&ExtensionMetadata, ExtensionError> {
        if !self.available.contains_key(name) {
            return Err(ExtensionError::NotAvailable(name.to_string()));
        }
        if self.installed.contains(name) {
            return Err(ExtensionError::AlreadyInstalled(name.to_string()));
        }
        self.installed.insert(name.to_string());
        Ok(self.available.get(name).unwrap())
    }

    pub fn uninstall(&mut self, name: &str) -> Result<(), ExtensionError> {
        if !self.installed.remove(name) {
            return Err(ExtensionError::NotInstalled(name.to_string()));
        }
        Ok(())
    }

    pub fn installed_extensions(&self) -> Vec<&String> {
        self.installed.iter().collect()
    }

    pub fn available_extensions(&self) -> Vec<&ExtensionMetadata> {
        self.available.values().collect()
    }

    pub fn installed_functions(&self) -> Vec<&str> {
        let mut functions = Vec::new();
        for name in &self.installed {
            if let Some(ext) = self.available.get(name) {
                for func in &ext.functions {
                    functions.push(func.as_str());
                }
            }
        }
        functions
    }

    pub fn function_available(&self, func_name: &str) -> bool {
        let func_upper = func_name.to_uppercase();
        for name in &self.installed {
            if let Some(ext) = self.available.get(name)
                && ext.functions.iter().any(|f| f.to_uppercase() == func_upper)
            {
                return true;
            }
        }
        false
    }

    pub fn function_provider(&self, func_name: &str) -> Option<&str> {
        let func_upper = func_name.to_uppercase();
        for name in &self.installed {
            if let Some(ext) = self.available.get(name)
                && ext.functions.iter().any(|f| f.to_uppercase() == func_upper)
            {
                return Some(&ext.name);
            }
        }
        None
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum ExtensionError {
    NotAvailable(String),

    AlreadyInstalled(String),

    NotInstalled(String),
}

impl std::fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExtensionError::NotAvailable(name) => {
                write!(
                    f,
                    "extension \"{}\" is not available. Available extensions: uuid-ossp, pgcrypto, pg_trgm, hstore",
                    name
                )
            }
            ExtensionError::AlreadyInstalled(name) => {
                write!(f, "extension \"{}\" already exists", name)
            }
            ExtensionError::NotInstalled(name) => {
                write!(f, "extension \"{}\" does not exist", name)
            }
        }
    }
}

impl std::error::Error for ExtensionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_registry_has_available_extensions() {
        let registry = ExtensionRegistry::new();
        assert!(registry.is_available("uuid-ossp"));
        assert!(registry.is_available("pgcrypto"));
        assert!(registry.is_available("pg_trgm"));
        assert!(registry.is_available("hstore"));
        assert!(!registry.is_available("nonexistent"));
    }

    #[test]
    fn test_install_extension() {
        let mut registry = ExtensionRegistry::new();
        assert!(!registry.is_installed("uuid-ossp"));

        let result = registry.install("uuid-ossp");
        assert!(result.is_ok());
        assert!(registry.is_installed("uuid-ossp"));
    }

    #[test]
    fn test_install_unavailable_extension() {
        let mut registry = ExtensionRegistry::new();
        let result = registry.install("nonexistent");
        assert!(matches!(result, Err(ExtensionError::NotAvailable(_))));
    }

    #[test]
    fn test_install_already_installed() {
        let mut registry = ExtensionRegistry::new();
        registry.install("uuid-ossp").unwrap();

        let result = registry.install("uuid-ossp");
        assert!(matches!(result, Err(ExtensionError::AlreadyInstalled(_))));
    }

    #[test]
    fn test_uninstall_extension() {
        let mut registry = ExtensionRegistry::new();
        registry.install("pgcrypto").unwrap();
        assert!(registry.is_installed("pgcrypto"));

        let result = registry.uninstall("pgcrypto");
        assert!(result.is_ok());
        assert!(!registry.is_installed("pgcrypto"));
    }

    #[test]
    fn test_uninstall_not_installed() {
        let mut registry = ExtensionRegistry::new();
        let result = registry.uninstall("uuid-ossp");
        assert!(matches!(result, Err(ExtensionError::NotInstalled(_))));
    }

    #[test]
    fn test_function_available_after_install() {
        let mut registry = ExtensionRegistry::new();
        assert!(!registry.function_available("gen_random_uuid"));

        registry.install("uuid-ossp").unwrap();
        assert!(registry.function_available("gen_random_uuid"));
        assert!(registry.function_available("uuid_generate_v4"));
    }

    #[test]
    fn test_function_provider() {
        let mut registry = ExtensionRegistry::new();
        registry.install("pgcrypto").unwrap();

        assert_eq!(
            registry.function_provider("gen_random_bytes"),
            Some("pgcrypto")
        );
        assert_eq!(registry.function_provider("nonexistent"), None);
    }

    #[test]
    fn test_installed_functions() {
        let mut registry = ExtensionRegistry::new();
        assert!(registry.installed_functions().is_empty());

        registry.install("uuid-ossp").unwrap();
        let funcs = registry.installed_functions();
        assert!(funcs.contains(&"gen_random_uuid"));
        assert!(funcs.contains(&"uuid_generate_v4"));
    }
}
