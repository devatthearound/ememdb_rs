use serde_json::{Value, json};
use uuid::Uuid;
use std::{convert::Into, sync::Arc};
use crate::db::Collection;
use std::collections::HashMap;
use crate::db::DocumentEntry;
use dashmap::DashMap;

type Filter = Box<dyn Fn(&Value) -> bool + Send + Sync>;
pub type QueryResult = Result<Vec<Value>, String>;
pub type SuccessCallback = Box<dyn Fn(&Vec<Value>) + Send + Sync>;
pub type ErrorCallback = Box<dyn Fn(&String) + Send + Sync>;

pub struct JoinBuilder {
    src_collection: Arc<Collection>,
    target_collection: Arc<Collection>,
    src_key: String,
    target_key: String,
    filters: Vec<Filter>,
    selected_fields: Vec<String>,
    map_function: Option<Box<dyn Fn(Value) -> Value + Send + Sync>>,
}

impl JoinBuilder {
    pub fn new(src_collection: Arc<Collection>, target_collection: Arc<Collection>) -> Self {
        JoinBuilder {
            src_collection,
            target_collection,
            src_key: String::new(),
            target_key: String::new(),
            filters: vec![],
            selected_fields: vec![],
            map_function: None,
        }
    }

    pub fn select(mut self, fields: &str) -> Self {
        if fields == "*" {
            self.selected_fields = vec![];
            return self;
        }
        self.selected_fields = fields.split(',').map(|s| s.trim().to_string()).collect();
        self
    }

    pub fn on(mut self, src_key: &str, target_key: &str) -> Self {
        self.src_key = src_key.to_string();
        self.target_key = target_key.to_string();
        self
    }

    pub fn filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&Value) -> bool + Send + Sync + 'static,
    {
        self.filters.push(Box::new(filter));
        self
    }

    pub fn map<F>(mut self, f: F) -> Self
    where
        F: Fn(Value) -> Value + Send + Sync + 'static,
    {
        self.map_function = Some(Box::new(f));
        self
    }

    pub fn execute(self) -> Vec<Value> {
        let src_docs = self.src_collection.select("*").execute().unwrap();
        let mut results = Vec::new();
    
        for src_doc in src_docs {
            let mut joined_doc = src_doc.clone();
    
            if let Some(src_value) = src_doc.get(&self.src_key) {
                let src_value_str = src_value.to_string();
                let mut query = self.target_collection.select("*");
                let target_docs = query
                    .eq(&self.target_key, src_value_str) // Remove the & before src_value_str
                    .execute()
                    .unwrap();
    
                if let Some(target_doc) = target_docs.first() {
                    for (key, value) in target_doc.as_object().unwrap() {
                        if self.selected_fields.is_empty() || self.selected_fields.contains(key) {
                            joined_doc[format!("joined_{}", key)] = value.clone();
                        }
                    }
                } else {
                    for field in &self.selected_fields {
                        joined_doc[format!("joined_{}", field)] = Value::Null;
                    }
                }
            }
    
            if self.filters.iter().all(|filter| filter(&joined_doc)) {
                if let Some(map_fn) = &self.map_function {
                    joined_doc = map_fn(joined_doc);
                }
                results.push(joined_doc);
            }
        }
    
        results
    }
}

pub struct QueryBuilder {
    collection: Arc<Collection>,
    filters: Vec<Filter>,
    selected_fields: Vec<String>,
    success_callback: Option<SuccessCallback>,
    error_callback: Option<ErrorCallback>,
    joins: Vec<(String, String, Arc<Collection>, Arc<Collection>, Box<dyn Fn(String, String, Arc<Collection>, Arc<Collection>, Filter) -> Vec<Value> + Send + Sync>)>,
}

impl QueryBuilder {
    pub fn new(collection: Arc<Collection>) -> Self {
        QueryBuilder {
            collection,
            filters: vec![],
            selected_fields: vec![],
            success_callback: None,
            error_callback: None,
            joins: vec![],
        }
    }

    pub fn select(mut self, fields: Vec<String>) -> Self {
        self.selected_fields = fields;
        self
    }

    pub fn in_<T: Into<Value> + Clone>(mut self, key: &str, values: Vec<T>) -> Self {
        let values: Vec<Value> = values.into_iter().map(|v| v.into()).collect();
        let key = key.to_string(); // Convert &str to String
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(&key) {
                values.iter().any(|v| v == val)
            } else {
                false
            }
        }));
        self
    }
    pub fn eq<T: Into<Value>>(mut self, key: &str, value: T) -> Self {
        let value = value.into();
        let key = key.to_string();
        self.filters.push(Box::new(move |doc| {
            doc.get(&key).map_or(false, |val| val == &value)
        }));
        self
    }
    
    pub fn neq<T: Into<Value>>(mut self, key: &str, value: T) -> Self {
        let value = value.into();
        let key = key.to_string();
        self.filters.push(Box::new(move |doc| {
            doc.get(&key).map_or(true, |val| val != &value)
        }));
        self
    }

    pub fn gte<T: Into<f64>>(mut self, key: &str, value: T) -> Self {
        let value_f64: f64 = value.into();
        let key = key.to_string();
        self.filters.push(Box::new(move |doc| {
            doc.get(&key)
                .and_then(|val| val.as_f64())
                .map_or(false, |doc_val| doc_val >= value_f64)
        }));
        self
    }

    pub fn gt<T: Into<f64>>(mut self, key: &str, value: T) -> Self {
        let value_f64: f64 = value.into();
        let key = key.to_string();
        self.filters.push(Box::new(move |doc| {
            doc.get(&key)
                .and_then(|val| val.as_f64())
                .map_or(false, |doc_val| doc_val > value_f64)
        }));
        self
    }

    pub fn lte<T: Into<f64>>(mut self, key: &str, value: T) -> Self {
        let value_f64: f64 = value.into();
        let key = key.to_string();
        self.filters.push(Box::new(move |doc| {
            doc.get(&key)
                .and_then(|val| val.as_f64())
                .map_or(false, |doc_val| doc_val <= value_f64)
        }));
        self
    }

    pub fn lt<T: Into<f64>>(mut self, key: &str, value: T) -> Self {
        let value_f64: f64 = value.into();
        let key = key.to_string();
        self.filters.push(Box::new(move |doc| {
            doc.get(&key)
                .and_then(|val| val.as_f64())
                .map_or(false, |doc_val| doc_val < value_f64)
        }));
        self
    }

    pub fn on_success<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Vec<Value>) + Send + Sync + 'static,
    {
        self.success_callback = Some(Box::new(callback));
        self
    }

    pub fn on_fail<F>(mut self, callback: F) -> Self
    where
        F: Fn(&String) + Send + Sync + 'static,
    {
        self.error_callback = Some(Box::new(callback));
        self
    }

    pub fn map<F>(mut self, mapper: F) -> Self
    where
        F: Fn(&mut Value) + Send + Sync + 'static,
    {
        self.filters.push(Box::new(move |doc: &Value| {
            let mut mutable_doc = doc.clone();
            mapper(&mut mutable_doc);
            true
        }));
        self
    }

    pub fn filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&Value) -> bool + Send + Sync + 'static,
    {
        self.filters.push(Box::new(filter));
        self
    }

    pub fn join<F>(mut self, src_key: &str, target_key: &str, target_collection: Arc<Collection>, join_builder: F) -> Self
    where
        F: Fn(Arc<Collection>, Arc<Collection>) -> JoinBuilder + Send + Sync + 'static,
    {
        let join_function = Box::new(move |s: String, t: String, src: Arc<Collection>, target: Arc<Collection>, _: Filter| {
            let builder = join_builder(Arc::clone(&src), Arc::clone(&target));
            builder.on(&s, &t).execute()
        });

        self.joins.push((
            src_key.to_string(),
            target_key.to_string(),
            Arc::clone(&self.collection),
            Arc::clone(&target_collection),
            join_function
        ));
        self
    }

    pub fn execute(self) -> Result<Vec<Value>, String> {
        let mut results = vec![];

        for doc in self.collection.documents.iter() {
            let doc_value = doc.value().value.clone();

            if self.filters.iter().all(|filter| filter(&doc_value)) {
                let mut joined_docs = vec![doc_value];
                for (src_key, target_key, src_collection, target_collection, join_function) in &self.joins {
                    let new_joined_docs = join_function(
                        src_key.to_string(),
                        target_key.to_string(),
                        Arc::clone(src_collection),
                        Arc::clone(target_collection),
                        Box::new(|_| true)
                    );
                    
                    joined_docs = joined_docs.into_iter().flat_map(|existing_doc| {
                        if new_joined_docs.is_empty() {
                            vec![existing_doc]
                        } else {
                            new_joined_docs.iter().map(|joined_doc| {
                                let mut combined_doc = existing_doc.clone();
                                for (k, v) in joined_doc.as_object().unwrap() {
                                    combined_doc[k] = v.clone();
                                }
                                combined_doc
                            }).collect()
                        }
                    }).collect();
                }

                if !self.selected_fields.is_empty() {
                    joined_docs = joined_docs.into_iter().map(|doc| {
                        let mut selected_doc = json!({});
                        for field in &self.selected_fields {
                            if let Some(value) = doc.get(field) {
                                selected_doc[field] = value.clone();
                            }
                        }
                        selected_doc
                    }).collect();
                }

                results.extend(joined_docs);
            }
        }

        Ok(results)
    }
}