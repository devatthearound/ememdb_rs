use ememdb_rs::{InMemoryDB, TTL, KeyType};
use serde_json::{json, Value};
use std::sync::Arc;

fn main() -> Result<(), String> {
    // 인메모리 데이터베이스 생성
    let db = Arc::new(InMemoryDB::new("example_db", TTL::NoTTL));

    // 사용자 컬렉션 생성
    let users = db.create::<Value>()
        .name("users")
        .key("user_id")
        .key_type(KeyType::String)
        .build();

    // 주문 컬렉션 생성
    let orders = db.create::<Value>()
        .name("orders")
        .key("order_id")
        .key_type(KeyType::String)
        .build();

    // 샘플 데이터 삽입
    users.insert(json!({
        "user_id": "1",
        "name": "Alice",
        "age": 30
    }), None)?;

    users.insert(json!({
        "user_id": "2",
        "name": "Bob",
        "age": 25
    }), None)?;

    orders.insert(json!({
        "order_id": "101",
        "user_id": "1",
        "product": "Laptop",
        "amount": 1000
    }), None)?;

    orders.insert(json!({
        "order_id": "102",
        "user_id": "1",
        "product": "Mouse",
        "amount": 25
    }), None)?;

    orders.insert(json!({
        "order_id": "103",
        "user_id": "2",
        "product": "Keyboard",
        "amount": 50
    }), None)?;

    // Join 쿼리 실행
    let result = users.select("name,age")
    .join("orders", |orders| {
        orders.select("").eq("user_id", "users.user_id")
    })
    .execute()?;

    // 결과 출력
    for user in result {
        println!("User: {} (Age: {})", user["name"], user["age"]);
        if let Some(orders) = user["orders"].as_array() {
            for order in orders {
                println!("  Order ID: {}", order["order_id"]);
                println!("  Product: {}", order["product"]);
                println!("  Original Amount: ${:.2}", order["amount"]);
                println!("  Discounted Amount: ${:.2}", order["discounted_amount"]);
                println!();
            }
        }
        println!("------------------------");
    }

    Ok(())
}