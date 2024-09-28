use dashmap::DashMap;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime};
use regex::Regex;
pub struct InMemoryDB {
    pub name: String,
    pub collections: DashMap<String, Collection>,
    pub default_ttl: Option<u64>, // 전역 TTL (초 단위)
}

#[derive(Debug)]
pub struct DocumentEntry {
    value: Value,
    expiration: Option<SystemTime>, // None이면 TTL이 없음
}

#[derive(Debug)]
pub struct Collection {
    documents: DashMap<String, DocumentEntry>,
}

impl InMemoryDB {
    // 데이터베이스 이름과 기본 TTL을 설정하는 생성자
    pub fn new(name: &str, default_ttl: Option<u64>) -> Self {
        InMemoryDB {
            name: name.to_string(),
            collections: DashMap::new(),
            default_ttl,
        }
    }

    // 새로운 컬렉션 생성
    pub fn create_collection(&self, name: &str) {
        self.collections.insert(name.to_string(), Collection::new());
    }

    // 문서를 컬렉션에 추가 (옵션으로 TTL 제공)
    pub fn set(&self, collection_name: &str, doc_id: &str, document: Value, ttl_seconds: Option<u64>) {
        let expiration = self.calculate_expiration(ttl_seconds);

        if let Some(collection) = self.collections.get(collection_name) {
            collection.insert(doc_id, document, expiration);
        } else {
            let new_collection = Collection::new();
            new_collection.insert(doc_id, document, expiration);
            self.collections.insert(collection_name.to_string(), new_collection);
        }
    }

    // 전역 TTL 또는 개별 TTL을 기반으로 만료 시간 계산
    fn calculate_expiration(&self, ttl_seconds: Option<u64>) -> Option<SystemTime> {
        let ttl = ttl_seconds.or(self.default_ttl);
        ttl.map(|ttl| SystemTime::now() + Duration::new(ttl, 0))
    }

    // 문서 조회 (TTL이 만료된 경우 삭제)
    pub fn get(&self, collection_name: &str, doc_id: &str) -> Option<Value> {
        if let Some(collection) = self.collections.get(collection_name) {
            if doc_id == "*" {
                return Some(Value::Array(collection.get_all_documents()));
            }
            collection.get(doc_id)
        } else {
            None
        }
    }

    // Join 연산: 두 컬렉션의 특정 필드를 기준으로 문서들을 결합
    pub fn join(&self, collection1_name: &str, collection2_name: &str, join_field: &str) -> Vec<Value> {
        let mut results = Vec::new();

        let collection1 = self.collections.get(collection1_name);
        let collection2 = self.collections.get(collection2_name);

        if collection1.is_none() || collection2.is_none() {
            return results; // 컬렉션이 없으면 빈 결과 반환
        }

        let collection1 = collection1.unwrap();
        let collection2 = collection2.unwrap();

        for doc1 in collection1.documents.iter() {
            if let Some(field1_value) = doc1.value().value.get(join_field) {
                for doc2 in collection2.documents.iter() {
                    if let Some(field2_value) = doc2.value().value.get(join_field) {
                        if field1_value == field2_value {
                            let mut merged_doc = doc1.value().value.clone();
                            let mut doc2_clone = doc2.value().value.clone();
                            for (key, value) in doc2_clone.as_object_mut().unwrap() {
                                merged_doc[key] = value.clone();
                            }
                            results.push(merged_doc);
                        }
                    }
                }
            }
        }

        results
    }
}

impl Collection {
    pub fn new() -> Self {
        Collection {
            documents: DashMap::new(),
        }
    }

    // 문서 삽입 및 TTL 설정
    pub fn insert(&self, doc_id: &str, document: Value, expiration: Option<SystemTime>) {
        let entry = DocumentEntry { value: document, expiration };
        self.documents.insert(doc_id.to_string(), entry);
    }

    // 모든 문서 가져오기
    pub fn get_all_documents(&self) -> Vec<Value> {
        self.documents.iter().map(|entry| entry.value().value.clone()).collect()
    }

    // 문서 조회 (TTL이 만료된 문서 자동 삭제)
    pub fn get(&self, doc_id: &str) -> Option<Value> {
        if let Some(entry) = self.documents.get(doc_id) {
            if let Some(expiration) = entry.expiration {
                if SystemTime::now() > expiration {
                    self.documents.remove(doc_id);
                    return None; // TTL이 만료된 경우
                }
            }
            return Some(entry.value.clone());
        }
        None
    }
}
