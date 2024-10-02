use serde_json::{Value, json};
use std::convert::Into;
use crate::db::Collection;
use std::collections::HashMap;
// Query builder for chainable query operations
type Filter<'a> = Box<dyn Fn(&Value) -> bool + 'a>;
pub type QueryResult = Result<Vec<Value>, String>;
pub type SuccessCallback = Box<dyn Fn(&Vec<Value>)>;
pub type ErrorCallback = Box<dyn Fn(&String)>;

pub struct JoinBuilder<'a> {
    collection: Collection,
    filters: Vec<Filter<'a>>,
    selected_fields: Vec<String>,
    join_key: String,
    map_function: Option<Box<dyn Fn(Value) -> Value + 'a>>,
}

impl<'a> JoinBuilder<'a> {
    pub fn new(collection: Collection) -> Self {
        JoinBuilder {
            collection,
            filters: vec![],
            selected_fields: vec![],
            join_key: String::new(),
            map_function: None,
        }
    }

    pub fn select(mut self, fields: &str) -> Self {
        self.selected_fields = fields.split(',').map(|s| s.trim().to_string()).collect();
        self
    }

    pub fn eq(mut self, key: &'a str, value: &'a str) -> Self {
        self.filters.push(Box::new(move |doc| {
            doc.get(key).map_or(false, |v| v == value)
        }));
        self
    }

    pub fn map<F>(mut self, f: F) -> Self
    where
        F: Fn(Value) -> Value + 'a,
    {
        self.map_function = Some(Box::new(f));
        self
    }

    pub fn execute(self) -> Vec<Value> {
        let mut results = vec![];
        for doc in self.collection.documents.iter() {
            let doc_value = &doc.value().value;
            if self.filters.iter().all(|filter| filter(doc_value)) {
                let mut result = if self.selected_fields.is_empty() {
                    doc_value.clone()
                } else {
                    let mut selected = json!({});
                    for field in &self.selected_fields {
                        if let Some(value) = doc_value.get(field) {
                            selected[field] = value.clone();
                        }
                    }
                    selected
                };

                if let Some(map_fn) = &self.map_function {
                    result = map_fn(result);
                }

                results.push(result);
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
    joins: Vec<(String, Box<dyn Fn(&Value) -> Vec<Value> + 'a>)>,
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
    pub fn select(mut self, fields: Vec<&'a str>) -> Self {
        self.selected_fields = fields.into_iter().map(|s| s.to_string()).collect();
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

    // pub fn in_<T: PartialEq + Clone + 'static>(mut self, key: &'a str, values: Vec<T>) -> Self {
    //     self.filters.push(Box::new(move |doc| {
    //         if let Some(val) = doc.get(key) {
    //             for value in &values {
    //                 if let Some(doc_val) = val.as_f64() {
    //                     if let Ok(compare_val) = TryInto::<f64>::try_into(value.clone()) {
    //                         if doc_val == compare_val {
    //                             return true;
    //                         }
    //                     }
    //                 } else if let Some(doc_val) = val.as_str() {
    //                     if let Ok(compare_val) = TryInto::<String>::try_into(value.clone()) {
    //                         if doc_val == compare_val {
    //                             return true;
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         false
    //     }));
    //     self
    // }

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
    pub fn join<F>(mut self, join_key: &'a str, join_builder: F) -> Self
    where
        F: Fn(Collection) -> JoinBuilder<'a> + 'a,
    {
        let join_function = Box::new(move |doc: &Value| {
            if let Some(other_collection) = self.collection.db.get(join_key) {
                let join_builder = join_builder(other_collection);
                join_builder.execute()
            } else {
                vec![]
            }
        });
        self.joins.push((join_key.to_string(), join_function));
        self
    }
    
    pub fn execute(self) -> Result<Vec<Value>, String> {
        let mut results = vec![];

        for doc in self.collection.documents.iter() {
            let mut doc_value = doc.value().value.clone();

            if self.filters.iter().all(|filter| filter(&doc_value)) {
                for (join_key, join_function) in &self.joins {
                    let joined_docs = join_function(&doc_value);
                    doc_value[join_key] = Value::Array(joined_docs);
                }

                if !self.selected_fields.is_empty() {
                    let mut selected_doc = json!({});
                    for field in &self.selected_fields {
                        if let Some(value) = doc_value.get(field) {
                            selected_doc[field] = value.clone();
                        }
                    }
                    doc_value = selected_doc;
                }

                results.push(doc_value);
            }
        }

        Ok(results)
    }
}