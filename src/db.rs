use dashmap::DashMap;
use serde_json::{Value, json};
use uuid::Uuid;
use std::{sync::Arc, time::{Duration, SystemTime}};
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

#[derive(Debug, Clone)]
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
        CollectionBuilder::new(Arc::new((*self).clone()))
    }

    pub fn get(&self, name: &str) -> Option<Collection> {
        self.collections.get(name).map(|c| c.clone())
    }

    pub fn collection_names(&self) -> Vec<String> {
        self.collections.iter().map(|r| r.key().clone()).collect()
    }
}

#[derive(Debug, Clone)]
pub struct DocumentEntry {
    pub value: Value,
    pub expiration: Option<SystemTime>, // None means no TTL
}

impl DocumentEntry {
    pub fn new(value: Value, expiration: Option<SystemTime>) -> Self {
        DocumentEntry {
            value,
            expiration,
        }
    }

    pub fn set(&mut self, value: Value) {
       self.value = value;
    }

    pub fn update (&mut self, value: Value) {
     // update specific fields in value
        let mut new_value = self.value.clone();
        for (key, val) in value.as_object().unwrap() {
            new_value[key] = val.clone();
        }
        self.value = new_value;
    }
}

//  create struct DashMap<String, DocumentEntry>
pub struct Document {
    pub documents: DashMap<String, DocumentEntry>,
} 

impl Document {
    pub fn new(pkey:&str, documents:Vec<DocumentEntry>) -> Self {
        let mut new_documents = DashMap::new();
        for doc in documents {
            new_documents.insert(pkey.to_string(), doc);
        }

        Document {
            documents: new_documents,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Collection {
    pub documents: DashMap<String, DocumentEntry>,
    pub key_field: Option<String>,
    pub key_type: KeyType,
    pub unique_keys: Vec<String>,
    pub next_id: Arc<std::sync::atomic::AtomicU64>,
    pub db_name: String,
    pub collection_name: String,
}

impl Collection {
    pub fn new(db_name: String, collection_name:String, key_field: Option<String>, key_type: KeyType, unique_keys: Vec<String>) -> Self {
        Collection {
            documents: DashMap::new(),
            key_field,
            key_type,
            unique_keys,
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            db_name,
            collection_name,
        }
    }

    // Insert supporting single and multiple objects
   // Handle insert logic <div class="title">2024년도 강동구약사회 연수교육 조회서비스</div>
   pub fn insert(&self, mut document: serde_json::Value, ttl: Option<TTL>) -> Result<OperationResult, String> {

    let key_field = self.key_field.as_ref().ok_or("Key field is not set.")?;

    // 키 생성
    let doc_id = match self.key_type {
        KeyType::Increment => {
            let doc_id = self.next_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst).to_string();
            doc_id
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
            let fields_vec: Vec<String> = fields.split(",").map(|s| s.to_string()).collect();
            QueryBuilder::new(self).select(fields_vec)
        }
    }

    pub fn reset_documents(&mut self, documents: Document) {
        self.documents.clear();
        self.documents = documents.documents;
    }
}


pub struct CollectionBuilder<T> {
    db: Arc<InMemoryDB>,
    name: String,
    key_field: Option<String>,
    key_type: KeyType,
    unique_keys: Vec<String>,
    _marker: std::marker::PhantomData<T>,
}
impl<'a, T> CollectionBuilder<T> {
    pub fn new(db: Arc<InMemoryDB>) -> Self {
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
            self.unique_keys = keys.iter().map(|&s| s.to_string()).collect();
            self
        }

    // Build the collection
    pub fn build(self) -> Collection {
        // let db_arc = Arc::clone(&self.db);
        
      let new_collection =  Collection::new(
            self.db.name.clone(),
            self.name.clone(),
            self.key_field,
            self.key_type,
            self.unique_keys
        );
    
    self.db.collections.insert(self.name.clone(), new_collection.clone());
    new_collection
    }
}
