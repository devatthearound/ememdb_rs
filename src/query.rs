use serde_json::{Value, json};
use uuid::Uuid;
use std::convert::Into;
use crate::db::{Collection};
use std::collections::HashMap;
use crate::db::DocumentEntry;
use dashmap::DashMap;
// Query builder for chainable query operations
type Filter<'a> = Box<dyn Fn(&Value) -> bool + 'a>;
pub type QueryResult = Result<Vec<Value>, String>;
pub type SuccessCallback = Box<dyn Fn(&Vec<Value>)>;
pub type ErrorCallback = Box<dyn Fn(&String)>;
pub struct JoinBuilder<'a> {
    src_collection: &'a Collection,
    target_collection: &'a Collection,
    src_key: String,
    target_key: String,
    filters: Vec<Filter<'a>>,
    selected_fields: Vec<String>,
    map_function: Option<Box<dyn Fn(Value) -> Value + 'a>>,
}

impl<'a> JoinBuilder<'a> {
    pub fn new(src_collection: &'a Collection, target_collection: &'a Collection) -> Self {
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
        F: Fn(&Value) -> bool + 'a,
    {
        self.filters.push(Box::new(filter));
        self
    }

    pub fn map<F>(mut self, f: F) -> Self
    where
        F: Fn(Value) -> Value + 'a,
    {
        self.map_function = Some(Box::new(f));
        self
    }

    // pub fn execute(self) -> Vec<Value> {
    //     let src_docs = self.src_collection.select("*").execute().unwrap();
    //     let target_docs = self.target_collection.select("*").execute().unwrap();
    //     let mut results = Vec::new();

    //     for src_doc in src_docs {
    //         if let Some(src_value) = src_doc.get(&self.src_key) {
    //             let mut matched = false;
    //             for target_doc in target_docs.iter() {
    //                 if let Some(target_value) = target_doc.get(&self.target_key) {
    //                     if src_value == target_value {
    //                         matched = true;
    //                         let mut joined_doc = src_doc.clone();
    //                         for (key, value) in target_doc.as_object().unwrap() {
    //                             if !self.selected_fields.is_empty() && !self.selected_fields.contains(key) {
    //                                 continue;
    //                             }
    //                             // Add prefix to avoid field name conflicts
    //                             joined_doc[format!("joined_{}", key)] = value.clone();
    //                         }

    //                         if self.filters.iter().all(|filter| filter(&joined_doc)) {
    //                             if let Some(map_fn) = &self.map_function {
    //                                 joined_doc = map_fn(joined_doc);
    //                             }
    //                             results.push(joined_doc);
    //                         }
    //                     }
    //                 }
    //             }
    //             // If no match found, include the source document with null joined fields
    //             if !matched {
    //                 let mut joined_doc = src_doc.clone();
    //                 for field in &self.selected_fields {
    //                     joined_doc[format!("joined_{}", field)] = Value::Null;
    //                 }
    //                 results.push(joined_doc);
    //             }
    //         }
    //     }
    //     results
    // }
    pub fn execute(self) -> Vec<Value> {
        let src_docs = self.src_collection.select("*").execute().unwrap();
        let mut results = Vec::new();
    
        for src_doc in src_docs {
            let mut joined_doc = src_doc.clone();
    
            if let Some(src_value) = src_doc.get(&self.src_key) {
                let target_docs = self.target_collection
                    .select("*")
                    .eq(&self.target_key, src_value.to_string().as_str())
                    .execute()
                    .unwrap();
    
                if let Some(target_doc) = target_docs.first() {
                    for (key, value) in target_doc.as_object().unwrap() {
                        if self.selected_fields.is_empty() || self.selected_fields.contains(key) {
                            joined_doc[format!("joined_{}", key)] = value.clone();
                        }
                    }
                } else {
                    // If no match found, set joined fields to null
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

pub struct QueryBuilder<'a> {
    collection: &'a Collection,
    filters: Vec<Filter<'a>>,
    selected_fields: Vec<String>,
    success_callback: Option<SuccessCallback>,
    error_callback: Option<ErrorCallback>,
    joins: Vec<(String, String, &'a Collection, &'a Collection, Box<dyn Fn(String, String, Collection, &Collection, Filter) -> Vec<Value> + 'a>)>,
}


impl<'a> QueryBuilder<'a> {
    pub fn new(collection: &'a Collection) -> Self {
        QueryBuilder {
            collection,
            filters: vec![],
            selected_fields: vec![],
            success_callback: None,
            error_callback: None,
            joins: vec![],
        }
    }

    // fileds is vector of strings
    pub fn select(mut self, fields:Vec<String>) -> Self {
        self.selected_fields = fields;
        self
    }


    pub fn in_(mut self, key: &'a str, values: Vec<Value>) -> Self {
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                values.iter().any(|v| v == val)
            } else {
                false
            }
        }));
        self
    }

    pub fn eq(mut self, key: &'a str, value: &'a str) -> Self {
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                match val {
                    Value::Number(n) => {
                        if let Ok(compare_val) = value.parse::<f64>() {
                            return n.as_f64().unwrap() == compare_val;
                        }
                    },
                    Value::String(s) => return s == value,
                    _ => return false,
                }
            }
            false
        }));
        self
    }

    pub fn neq(mut self, key: &'a str, value: &'a str) -> Self {
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                match val {
                    Value::Number(n) => {
                        if let Ok(compare_val) = value.parse::<f64>() {
                            return n.as_f64().unwrap() != compare_val;
                        }
                    },
                    Value::String(s) => return s != value,
                    _ => return true,
                }
            }
            true
        }));
        self
    }

    pub fn gte<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
        let value_f64: f64 = value.into();
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                if let Some(doc_val) = val.as_f64() {
                    return doc_val >= value_f64;
                }
            }
            false
        }));
        self
    }

    pub fn gt<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
        let value_f64: f64 = value.into();
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                if let Some(doc_val) = val.as_f64() {
                    return doc_val > value_f64;
                }
            }
            false
        }));
        self
    }

    pub fn lte<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
        let value_f64: f64 = value.into();
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                if let Some(doc_val) = val.as_f64() {
                    return doc_val <= value_f64;
                }
            }
            false
        }));
        self
    }

    pub fn lt<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
        let value_f64: f64 = value.into();
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                if let Some(doc_val) = val.as_f64() {
                    return doc_val < value_f64;
                }
            }
            false
        }));
        self
    }

     pub fn in_values(mut self, key: &'a str, values: Vec<Value>) -> Self {
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                values.contains(val)
            } else {
                false
            }
        }));
        self
    }

    pub fn in_strings(mut self, key: &'a str, values: Vec<String>) -> Self {
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                if let Some(doc_str) = val.as_str() {
                    values.contains(&doc_str.to_string())
                } else {
                    false
                }
            } else {
                false
            }
        }));
        self
    }

    pub fn in_numbers(mut self, key: &'a str, values: Vec<f64>) -> Self {
        self.filters.push(Box::new(move |doc| {
            if let Some(val) = doc.get(key) {
                if let Some(doc_num) = val.as_f64() {
                    values.contains(&doc_num)
                } else {
                    false
                }
            } else {
                false
            }
        }));
        self
    }
   
    pub fn on_success<F>(mut self, callback: F) -> Self
    where
        F: Fn(&Vec<Value>) + 'static,
    {
        self.success_callback = Some(Box::new(callback));
        self
    }

    pub fn on_fail<F>(mut self, callback: F) -> Self
    where
        F: Fn(&String) + 'static,
    {
        self.error_callback = Some(Box::new(callback));
        self
    }

    pub fn map<F>(mut self, mapper: F) -> Self
    where
        F: Fn(&mut Value) + 'a,
    {
        // Add the mapper function to a list of mappers that will modify the documents later
        self.filters.push(Box::new(move |doc: &Value| {
            let mut mutable_doc = doc.clone();  // Clone the document for safe mutation
            mapper(&mut mutable_doc);           // Apply the mapper function to the cloned document
            true  // The filter function returns true to indicate that we keep the document
        }));
        self
    }

    pub fn filter<F>(mut self, filter: F) -> Self
    where
        F: Fn(&Value) -> bool + 'a,
    {
        self.filters.push(Box::new(filter));
        self
    }

    pub fn join<F>(mut self, src_key: &'a str, target_key: &'a str, target_collection: &'a Collection, join_builder: F) -> Self
    where
        F: Fn(&'a Collection, &'a Collection) -> JoinBuilder<'a> + 'a,
    {
        let join_function = Box::new(move |_: String, _: String, _: Collection, _: &Collection, _: Filter| {
            let builder = join_builder(self.collection, target_collection);
            builder.on(src_key, target_key).execute()
        });

        self.joins.push((src_key.to_string(), target_key.to_string(), self.collection, target_collection, join_function));
        self
    }

    pub fn execute(self) -> Result<Vec<Value>, String> {
        let mut results = vec![];

        for doc in self.collection.documents.iter() {
            let doc_value = doc.value().value.clone();

            if self.filters.iter().all(|filter| filter(&doc_value)) {
                let mut joined_docs = vec![doc_value];
                for (src_key, target_key, src_collection, target_collection, join_function) in &self.joins {
                    let new_joined_docs = join_function((*src_key).clone(), (*target_key).clone(), (*src_collection).clone(), target_collection, Box::new(|_| true));
                    
                    // Combine the joined documents with existing results
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

                // Apply field selection
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