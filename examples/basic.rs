use serde_json::{Value, json};
use ememdb_rs::{InMemoryDB, TTL, KeyType, CollectionConfig, OperationResult};

fn main() -> Result<(), String> {
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

    let user_data = json!({
        "user_id": "1234",
        "email": "alice@example.com",
        "name": "Alice"
    });
    match collection.insert(user_data.clone(), Some(TTL::GlobalTTL(60)))? {
        OperationResult::Inserted { id, document } => {
            println!("Inserted document with id: {}, document: {:?}", id, document);
        },
        _ => unreachable!(),
    }

    // Update a document
    let updated_data = json!({
        "user_id": "1234",
        "email": "alice.new@example.com",
        "name": "Alice Updated"
    });
    match collection.update(updated_data)? {
        OperationResult::Updated { id, old_document, new_document } => {
            println!("Updated document with id: {}", id);
            println!("Old document: {:?}", old_document);
            println!("New document: {:?}", new_document);
        },
        _ => unreachable!(),
    }

    // Delete a document
    match collection.delete("5678")? {
        OperationResult::Deleted { id, document } => {
            println!("Deleted document with id: {}, document: {:?}", id, document);
        },
        _ => unreachable!(),
    }
    Ok(())
}