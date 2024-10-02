// config.rs
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TTL {
    NoTTL,
    GlobalTTL(u64),
    CustomTTL(u64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)] // Add PartialEq here
pub enum KeyType {
    Increment,
    UUID,
    String,
    Custom, // Use specific fields from the document
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CollectionConfig<'a> {
    pub key_field: Option<&'a str>,
    pub key_type: Option<KeyType>,
    pub unique_keys: Vec<&'a str>,
    pub not_null_fields: Vec<&'a str>,
    pub nullable_fields: Vec<&'a str>,
    pub field_types: Vec<(&'a str, &'a str)>,
    pub ttl: Option<TTL>,
}

impl<'a> CollectionConfig<'a> {
    pub fn new() -> Self {
        CollectionConfig {
            key_field: None,
            key_type: None,
            unique_keys: Vec::new(),
            not_null_fields: Vec::new(),
            nullable_fields: Vec::new(),
            field_types: Vec::new(),
            ttl: None,
        }
    }

    pub fn key(mut self, key_field: &'a str) -> Self {
        self.key_field = Some(key_field);
        self
    }

    pub fn key_type(mut self, key_type: KeyType) -> Self {
        self.key_type = Some(key_type);
        self
    }

    pub fn unique_keys(mut self, keys: Vec<&'a str>) -> Self {
        self.unique_keys = keys;
        self
    }

    pub fn not_null(mut self, fields: Vec<&'a str>) -> Self {
        self.not_null_fields = fields;
        self
    }

    pub fn nullable(mut self, fields: Vec<&'a str>) -> Self {
        self.nullable_fields = fields;
        self
    }

    pub fn field_types(mut self, types: Vec<(&'a str, &'a str)>) -> Self {
        self.field_types = types;
        self
    }

    pub fn ttl(mut self, ttl: TTL) -> Self {
        self.ttl = Some(ttl);
        self
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.key_type == Some(KeyType::Custom) && self.key_field.is_none() {
            return Err("Key field must be set when using Custom key type".to_string());
        }
        
        // 추가적인 유효성 검사
        if let Some(key_field) = self.key_field {
            if !self.field_types.iter().any(|&(field, _)| field == key_field) {
                return Err("Key field must be defined in field_types".to_string());
            }
        }

        // not_null_fields와 nullable_fields 중복 검사
        for field in &self.not_null_fields {
            if self.nullable_fields.contains(field) {
                return Err(format!("Field '{}' cannot be both not-null and nullable", field));
            }
        }

        Ok(())
    }
}
