use serde_json::{json, Value};
use ememdb_rs::{InMemoryDB, TTL, KeyType, OperationResult};

fn main() -> Result<(), String> {
    // InMemoryDB 인스턴스 생성
    let db = InMemoryDB::new("example_db", TTL::NoTTL);

    // 컬렉션 생성
    let mut users = db.create::<Value>()
        .name("users")
        .key("user_id")
        .key_type(KeyType::String)
        .unique_keys(vec!["email"])
        .build();

    // 새 사용자 추가 (삽입)
    let new_user = json!({
        "user_id": "1234",
        "email": "john@example.com",
        "name": "John Doe"
    });

    match users.upsert(new_user.clone(), Some(TTL::GlobalTTL(3600))) {
        Ok(OperationResult::Inserted { id, document }) => {
            println!("Inserted new user with id: {}", id);
            println!("Document: {:?}", document);
        },
        Ok(OperationResult::Updated { .. }) => unreachable!(),
        Ok(OperationResult::Deleted { .. }) => unreachable!(),
        Err(e) => println!("Error: {}", e),
    }

    // 기존 사용자 업데이트
    let updated_user = json!({
        "user_id": "1234",
        "email": "john@example.com",
        "name": "John Updated Doe",
        "age": 30
    });

    match users.upsert(updated_user.clone(), Some(TTL::CustomTTL(7200))) {
        Ok(OperationResult::Updated { id, old_document, new_document }) => {
            println!("Updated user with id: {}", id);
            println!("Old document: {:?}", old_document);
            println!("New document: {:?}", new_document);
        },
        Ok(OperationResult::Inserted { .. }) => unreachable!(),
        Ok(OperationResult::Deleted { .. }) => unreachable!(),
        Err(e) => println!("Error: {}", e),
    }

    // 존재하지 않는 사용자에 대한 upsert (새로운 삽입)
    let another_user = json!({
        "user_id": "5678",
        "email": "jane@example.com",
        "name": "Jane Doe"
    });

    match users.upsert(another_user.clone(), None) {
        Ok(OperationResult::Inserted { id, document }) => {
            println!("Inserted another user with id: {}", id);
            println!("Document: {:?}", document);
        },
        Ok(OperationResult::Updated { .. }) => unreachable!(),
        Ok(OperationResult::Deleted { .. }) => unreachable!(),
        Err(e) => println!("Error: {}", e),
    }

    // 모든 사용자 조회
    let all_users = users.select("*").execute();
    println!("All users after upsert operations:");
    for user in all_users {
        println!("{:?}", user);
    }

    Ok(())
}