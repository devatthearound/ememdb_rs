use serde_json::{Value, json};
use ememdb_rs::{InMemoryDB, TTL, KeyType, CollectionConfig, OperationResult};

fn main() {
    // Create an InMemoryDB instance
    let db = InMemoryDB::new("test_db", TTL::NoTTL);

    // Create a collection with configuration
    let collection_config = CollectionConfig::new()
        .key("user_id")
        .key_type(KeyType::String)
        .unique_keys(vec!["email"])
        .not_null(vec!["user_id", "email", "name"])
        .field_types(vec![
            ("user_id", "string"),
            ("email", "string"),
            ("name", "string"),
        ])
        .ttl(TTL::GlobalTTL(3600)); // Default TTL for the collection
    println!("Collection Config: {:?}", collection_config);
    let mut collection = db.create::<Value>()
        .name("users")
        .key("user_id")
        .key_type(KeyType::String)
        .build()   ;     // Insert a document with a Global TTL of 60 seconds
        
    println!("Collection: {:?}", collection);
    println!("db: {:?}", db);

    // Delete a document
    
}