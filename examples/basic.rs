use ememdb_rs::{InMemoryDB, Collection, TTL, KeyType, OperationResult};
use serde_json::{json, Value};
use std::sync::Arc;

fn main()  {
    // 1. InMemoryDB 인스턴스 생성 및 Arc로 감싸기
    let db = Arc::new(InMemoryDB::new("test_db", TTL::NoTTL));

    // 2. 컬렉션 생성
    let users_collection: Arc<Collection> = db.create::<Value>()
        .name("users")
        .key("user_id")
        .key_type(KeyType::String)
        .build().into();

    // 3. 문서 삽입
    let user_data = json!({
        "user_id": "1001",
        "name": "John Doe",
        "email": "john.doe@example.com",
        "age": 30
    });

    match users_collection.insert(user_data, Some(TTL::CustomTTL(3600))) {
        Ok(result) => {
            match result {
                OperationResult::Inserted { id, document } => {
                    println!("Document inserted successfully with ID: {}", id);
                    println!("Inserted document: {:?}", document);
                },
                _ => unreachable!("Unexpected result from insert operation"),
            }
        },
        Err(e) => {
            println!("Error inserting document: {}", e);
        }
    }

    // 4. 삽입된 데이터 확인
    let query_result = users_collection.select("*").execute();
    match query_result {
        Ok(documents) => {
            println!("All documents in the collection:");
            for doc in documents {
                println!("{:?}", doc);
            }
        },
        Err(e) => {
            println!("Error querying documents: {}", e);
        }
    }
    println!("db: {:?}", db);
    // let get_collection = db.get(
    // "users");
    // // 5. 컬렉션 조회
    // match get_collection {
    //     Ok(collection) => {
    //         println!("Collection retrieved successfully: {:?}", collection);
    //     },
    //     Err(_) => {
    //         println!("Error retrieving collection");
    //     }
    // }
}