use serde_json::json;
use std::sync::Arc;
use ememdb_rs::{InMemoryDB, Collection, TTL, KeyType, QueryBuilder, JoinBuilder};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 데이터베이스 초기화
    let db = Arc::new(InMemoryDB::new("sample_db", TTL::NoTTL));

    // 사용자 컬렉션 생성
    let users_collection = db.create::<String>()
        .name("users")
        .key("id")
        .key_type(KeyType::UUID)
        .unique_keys(vec!["email"])
        .build();

    // 주문 컬렉션 생성
    let orders_collection = db.create::<String>()
        .name("orders")
        .key("id")
        .key_type(KeyType::UUID)
        .build();

    // 사용자 데이터 삽입
    users_collection.insert(json!({
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    }), None)?;

    users_collection.insert(json!({
        "name": "Bob",
        "email": "bob@example.com",
        "age": 25
    }), None)?;

    // 주문 데이터 삽입
    orders_collection.insert(json!({
        "user_email": "alice@example.com",
        "product": "Laptop",
        "amount": 1000
    }), None)?;

    orders_collection.insert(json!({
        "user_email": "bob@example.com",
        "product": "Phone",
        "amount": 500
    }), None)?;

    orders_collection.insert(json!({
        "user_email": "alice@example.com",
        "product": "Headphones",
        "amount": 100
    }), None)?;

    // JOIN 작업 수행
    let join_result = users_collection
        .select("*")
        .eq("name", "Alice")
        // .join("email", "user_email", &)orders_collection, |src, target| {
        //     println!("Joining {} with {}", src.collection_name, target.db_name);
        //     JoinBuilder::new(src, target)
        //         .select("product,amount")
        //         .on("email", "user_email")
        // })
        .execute()?;

    println!("JOIN Result:");
    for user_with_orders in join_result {
        println!("{:#?}", user_with_orders);
    }

    Ok(())
}