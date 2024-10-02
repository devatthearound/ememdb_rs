use serde_json::{Value, json};
use std::convert::Into;
use crate::db::Collection;
// Query builder for chainable query operations
type Filter<'a> = Box<dyn Fn(&Value) -> bool + 'a>;
pub type QueryResult = Result<Vec<Value>, String>;
pub type SuccessCallback = Box<dyn Fn(&Vec<Value>)>;
pub type ErrorCallback = Box<dyn Fn(&String)>;

pub struct QueryBuilder<'a> {
    collection: &'a Collection,
    filters: Vec<Filter<'a>>,
    selected_fields: Vec<&'a str>,
    success_callback: Option<SuccessCallback>,
    error_callback: Option<ErrorCallback>,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(collection: &'a Collection) -> Self {
        QueryBuilder {
            collection,
            filters: vec![],
            selected_fields: vec![],
            success_callback: None,
            error_callback: None,
        }
    }


    // fileds is vector of strings
    pub fn select(mut self, fields: Vec<&'a str>) -> Self {
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

 
    pub fn execute(self) -> QueryResult {
        let mut results = vec![];

        for doc in self.collection.documents.iter() {
            let doc_value = &doc.value().value;

            let mut is_match = true;
            for filter in &self.filters {
                if !filter(doc_value) {
                    is_match = false;
                    break;
                }
            }

            if is_match {
                let fields = &self.selected_fields;
                let mut selected_doc = json!({});
                if fields.is_empty()  {
                    results.push(doc_value.clone());
                    continue;
                } else {
                    for field in fields {
                        if let Some(value) = doc_value.get(field) {
                            selected_doc[field] = value.clone();
                        }
                    }
                }
                results.push(selected_doc);
            }
        }

        let result = Ok(results);

        match &result {
            Ok(data) => {
                if let Some(ref success_cb) = self.success_callback {
                    success_cb(data);
                }
            }
            Err(error) => {
                if let Some(ref error_cb) = self.error_callback {
                    error_cb(error);
                }
            }
        }

        result
    }
}