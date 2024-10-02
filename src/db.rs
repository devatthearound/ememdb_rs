use dashmap::DashMap;
use serde_json::{Value, json};
use uuid::Uuid;
use std::time::{Duration, SystemTime};
use crate::config::{TTL, KeyType};
use crate::query::QueryBuilder;
// use crate::query::Query;

#[derive(Debug, Clone)]
pub enum OperationResult {
    Inserted {
        id: String,
        document: Value,
    },
    Updated {
        id: String,
        old_document: Value,
        new_document: Value,
    },
    Deleted {
        id: String,
        document: Value,
    },
}

#[derive(Debug)]
pub struct InMemoryDB {
    pub name: String,
    pub collections: DashMap<String, Collection>,
    pub default_ttl: TTL,
}

impl InMemoryDB {
    pub fn new(name: &str, default_ttl: TTL) -> Self {
        InMemoryDB {
            name: name.to_string(),
            collections: DashMap::new(),
            default_ttl,
        }
    }

    pub fn create<T: 'static>(&self) -> CollectionBuilder<T> {
        CollectionBuilder::new(self)
    }
}

#[derive(Debug, Clone)]
pub struct DocumentEntry {
    pub value: Value,
    pub expiration: Option<SystemTime>, // None means no TTL
}

#[derive(Debug, Clone)]
pub struct Collection {
    pub documents: DashMap<String, DocumentEntry>,
    pub key_field: Option<String>, // Field used as a key
    pub key_type: KeyType, // Type of key (Increment, UUID, Custom)
    pub unique_keys: Vec<String>, // Fields that must be unique
    pub next_id: u64, // Increment counter for auto-generated IDs
}

impl Collection {
    pub fn new(key_field: Option<String>, key_type: KeyType, unique_keys: Vec<&str>) -> Self {
        Collection {
            documents: DashMap::new(),
            key_field: key_field.map(String::from),
            key_type,
            unique_keys: unique_keys.iter().map(|&s| s.to_string()).collect(),
            next_id: 0,
        }
    }

    // Insert supporting single and multiple objects
   // Handle insert logic <div class="title">2024년도 강동구약사회 연수교육 조회서비스</div>
   pub fn insert(&mut self, mut document: Value, ttl: Option<TTL>) -> Result<OperationResult, String> {

    let key_field = self.key_field.as_ref().ok_or("Key field is not set.")?;

    // 키 생성
    let doc_id = match self.key_type {
        KeyType::Increment => {
            self.next_id += 1;
            self.next_id.to_string()
        }
        KeyType::UUID => Uuid::new_v4().to_string(),
        KeyType::String | KeyType::Custom => {
            document.get(key_field)
                .ok_or_else(|| format!("{} field not found in the document.", key_field))?
                .as_str()
                .ok_or_else(|| format!("{} is not a string.", key_field))?
                .to_string()
        }
    };

    // 자동 생성된 키를 문서에 추가
    if matches!(self.key_type, KeyType::Increment | KeyType::UUID) {
        document[key_field] = json!(doc_id.clone());
    }

    // TTL 처리
    let expiration = match ttl {
        Some(TTL::GlobalTTL(seconds)) | Some(TTL::CustomTTL(seconds)) => 
            Some(SystemTime::now() + Duration::from_secs(seconds)),
        Some(TTL::NoTTL) | None => None,
    };

    // 유니크 키 검증
    for unique_key in &self.unique_keys {
        if let Some(value) = document.get(unique_key) {
            if self.documents.iter().any(|r| r.value().value.get(unique_key) == Some(value)) {
                return Err(format!("Duplicate value for unique key: {}", unique_key));
            }
        }
    }

    // 문서를 컬렉션에 삽입
      self.documents.insert(doc_id.clone(), DocumentEntry { value: document.clone(), expiration });
        Ok(OperationResult::Inserted {
            id: doc_id,
            document,
        })
}
    // Update supporting single and multiple objects
    pub fn upsert(&mut self, document: Value, ttl: Option<TTL>) -> Result<OperationResult, String> {
        let key_field = self.key_field.as_ref().ok_or("Key field is not set.")?;
        let doc_id = document.get(key_field)
            .ok_or_else(|| format!("{} field not found in the document.", key_field))?
            .as_str()
            .ok_or_else(|| format!("{} is not a string.", key_field))?;
    
        // 문서 존재 여부 확인
        if self.documents.contains_key(doc_id) {
            // 문서가 존재하면 업데이트
            let old_document = self.documents.get(doc_id)
                .map(|entry| entry.value.clone())
                .ok_or("Failed to get existing document")?;
    
            let expiration = match ttl {
                Some(TTL::GlobalTTL(seconds)) | Some(TTL::CustomTTL(seconds)) => 
                    Some(SystemTime::now() + Duration::from_secs(seconds)),
                Some(TTL::NoTTL) | None => None,
            };
    
            self.documents.insert(doc_id.to_string(), DocumentEntry { value: document.clone(), expiration });
    
            Ok(OperationResult::Updated {
                id: doc_id.to_string(),
                old_document,
                new_document: document,
            })
        } else {
            // 문서가 존재하지 않으면 새로 삽입
            self.insert(document, ttl)
        }
    }
    pub fn update(&mut self, document: Value) -> Result<OperationResult, String> {
        let key_field = self.key_field.as_ref().ok_or("Key field is not set.")?;
        let doc_id = document.get(key_field)
            .ok_or("Key field not found in the document.")?
            .as_str()
            .ok_or("Key value is not a string.")?;

        if let Some(mut entry) = self.documents.get_mut(doc_id) {
            let old_document = entry.value.clone();
            entry.value = document.clone();
            Ok(OperationResult::Updated {
                id: doc_id.to_string(),
                old_document,
                new_document: document,
            })
        } else {
            Err("Document not found.".to_string())
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<OperationResult, String> {
        if let Some((_, entry)) = self.documents.remove(key) {
            Ok(OperationResult::Deleted {
                id: key.to_string(),
                document: entry.value,
            })
        } else {
            Err("Document not found.".to_string())
        }
    }

    // Select chainable operations for building queries
  
    pub fn select<'a>(&'a self, fields: &'a str) -> QueryBuilder<'a> {
        if fields == "*" || fields.is_empty() || fields == " "  {
            QueryBuilder::new(self).select(vec![])
        } else {
            let fields_vec = fields.split(",").map(|s| s.trim()).collect();
            QueryBuilder::new(self).select(fields_vec)
        }
    }
}

// CollectionBuilder to build collections with a key field and key type
pub struct CollectionBuilder<'a, T> {
    db: &'a InMemoryDB,
    name: String,
    key_field: Option<String>, // Field to use as a key
    key_type: KeyType, // Key type (Increment, UUID, Custom)
    unique_keys: Vec<&'a str>, // List of unique key fields
    _marker: std::marker::PhantomData<T>,
}

impl<'a, T> CollectionBuilder<'a, T> {
    pub fn new(db: &'a InMemoryDB) -> Self {
        CollectionBuilder {
            db,
            name: String::new(),
            key_field: None,
            key_type: KeyType::UUID,
            unique_keys: Vec::new(),
            _marker: std::marker::PhantomData,
        }
    }

    pub fn name(mut self, name: &str) -> Self {
        self.name = name.to_string();
        self
    }

    // Set the key field for the collection
    pub fn key(mut self, key_field: &str) -> Self {
        self.key_field = Some(key_field.to_string());
        self
    }

    // Set the key type (Increment, UUID, Custom)
    pub fn key_type(mut self, key_type: KeyType) -> Self {
        self.key_type = key_type;
        self
    }

    // Set unique keys
    pub fn unique_keys(mut self, keys: Vec<&'a str>) -> Self {
        self.unique_keys = keys;
        self
    }

    // Build the collection
    pub fn build(self) -> Collection {
        let collection = Collection::new(self.key_field, self.key_type, self.unique_keys);
        self.db.collections.insert(self.name.clone(), collection.clone());
        collection
    }
}

// // Query builder for chainable query operations
// type Filter<'a> = Box<dyn Fn(&Value) -> bool + 'a>;

// pub struct QueryBuilder<'a> {
//     collection: &'a Collection,
//     filters: Vec<Filter<'a>>,
//     selected_fields: Vec<&'a str>,
// }

// impl<'a> QueryBuilder<'a> {
//     pub fn new(collection: &'a Collection) -> Self {
//         QueryBuilder {
//             collection,
//             filters: vec![],
//             selected_fields: vec![],
//         }
//     }

//     // fileds is vector of strings
//     pub fn select(mut self, fields: Vec<&'a str>) -> Self {
//         self.selected_fields = fields;    
//         self
//     }
//     pub fn in_(mut self, key: &'a str, values: Vec<Value>) -> Self {
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 values.iter().any(|v| v == val)
//             } else {
//                 false
//             }
//         }));
//         self
//     }

//     pub fn eq(mut self, key: &'a str, value: &'a str) -> Self {
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 match val {
//                     Value::Number(n) => {
//                         if let Ok(compare_val) = value.parse::<f64>() {
//                             return n.as_f64().unwrap() == compare_val;
//                         }
//                     },
//                     Value::String(s) => return s == value,
//                     _ => return false,
//                 }
//             }
//             false
//         }));
//         self
//     }

//     pub fn neq(mut self, key: &'a str, value: &'a str) -> Self {
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 match val {
//                     Value::Number(n) => {
//                         if let Ok(compare_val) = value.parse::<f64>() {
//                             return n.as_f64().unwrap() != compare_val;
//                         }
//                     },
//                     Value::String(s) => return s != value,
//                     _ => return true,
//                 }
//             }
//             true
//         }));
//         self
//     }

//     pub fn gte<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
//         let value_f64: f64 = value.into();
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 if let Some(doc_val) = val.as_f64() {
//                     return doc_val >= value_f64;
//                 }
//             }
//             false
//         }));
//         self
//     }

//     pub fn gt<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
//         let value_f64: f64 = value.into();
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 if let Some(doc_val) = val.as_f64() {
//                     return doc_val > value_f64;
//                 }
//             }
//             false
//         }));
//         self
//     }

//     pub fn lte<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
//         let value_f64: f64 = value.into();
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 if let Some(doc_val) = val.as_f64() {
//                     return doc_val <= value_f64;
//                 }
//             }
//             false
//         }));
//         self
//     }

//     pub fn lt<T: Into<f64> + Copy>(mut self, key: &'a str, value: T) -> Self {
//         let value_f64: f64 = value.into();
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 if let Some(doc_val) = val.as_f64() {
//                     return doc_val < value_f64;
//                 }
//             }
//             false
//         }));
//         self
//     }

//     // pub fn in_<T: PartialEq + Clone + 'static>(mut self, key: &'a str, values: Vec<T>) -> Self {
//     //     self.filters.push(Box::new(move |doc| {
//     //         if let Some(val) = doc.get(key) {
//     //             for value in &values {
//     //                 if let Some(doc_val) = val.as_f64() {
//     //                     if let Ok(compare_val) = TryInto::<f64>::try_into(value.clone()) {
//     //                         if doc_val == compare_val {
//     //                             return true;
//     //                         }
//     //                     }
//     //                 } else if let Some(doc_val) = val.as_str() {
//     //                     if let Ok(compare_val) = TryInto::<String>::try_into(value.clone()) {
//     //                         if doc_val == compare_val {
//     //                             return true;
//     //                         }
//     //                     }
//     //                 }
//     //             }
//     //         }
//     //         false
//     //     }));
//     //     self
//     // }

//      pub fn in_values(mut self, key: &'a str, values: Vec<Value>) -> Self {
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 values.contains(val)
//             } else {
//                 false
//             }
//         }));
//         self
//     }

//     pub fn in_strings(mut self, key: &'a str, values: Vec<String>) -> Self {
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 if let Some(doc_str) = val.as_str() {
//                     values.contains(&doc_str.to_string())
//                 } else {
//                     false
//                 }
//             } else {
//                 false
//             }
//         }));
//         self
//     }

//     pub fn in_numbers(mut self, key: &'a str, values: Vec<f64>) -> Self {
//         self.filters.push(Box::new(move |doc| {
//             if let Some(val) = doc.get(key) {
//                 if let Some(doc_num) = val.as_f64() {
//                     values.contains(&doc_num)
//                 } else {
//                     false
//                 }
//             } else {
//                 false
//             }
//         }));
//         self
//     }
   

//     pub fn map<F>(mut self, mapper: F) -> Self
//     where
//         F: Fn(&mut Value) + 'a,
//     {
//         // Add the mapper function to a list of mappers that will modify the documents later
//         self.filters.push(Box::new(move |doc: &Value| {
//             let mut mutable_doc = doc.clone();  // Clone the document for safe mutation
//             mapper(&mut mutable_doc);           // Apply the mapper function to the cloned document
//             true  // The filter function returns true to indicate that we keep the document
//         }));
//         self
//     }

//     pub fn filter<F>(mut self, filter: F) -> Self
//     where
//         F: Fn(&Value) -> bool + 'a,
//     {
//         self.filters.push(Box::new(filter));
//         self
//     }

 
//     pub fn execute(self) -> Vec<Value> {
//         let mut results = vec![];

//         for doc in self.collection.documents.iter() {
//             let doc_value = &doc.value().value;

//             let mut is_match = true;
//             for filter in &self.filters {
//                 if !filter(doc_value) {
//                     is_match = false;
//                     break;
//                 }
//             }

//             if is_match {
//                 let fields = &self.selected_fields;
//                 let mut selected_doc = json!({});
//                 // if fields is empty, select all fields
//                 if fields.is_empty()  {
//                     results.push(doc_value.clone());
//                     continue;
//                 } else {
//                 for field in fields{
//                     if let Some(value) = doc_value.get(field) {
//                         selected_doc[field] = value.clone();
//                     }
//                 }
//             }
//                 results.push(selected_doc);
//             }
//         }

//         results
//     }
// }

