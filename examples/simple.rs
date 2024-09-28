use serde_json::json;
use ememdb_rs::InMemoryDB;

fn main() {
    // 데이터베이스 생성 시 이름과 전역 TTL 설정
    let db = InMemoryDB::new("my_in_memory_db", Some(10)); // 기본 TTL은 10초

    // 컬렉션 생성
    db.create_collection("users");
    db.create_collection("orders");

    // 사용자와 주문 데이터 추가
    db.set("users", "user1", json!({"id": 1, "name": "John"}), None); // 전역 TTL 사용
    db.set("users", "user2", json!({"id": 2, "name": "Jane"}), Some(20)); // 개별 TTL (20초)
    db.set("orders", "order1", json!({"user_id": 1, "amount": 100}), None); // 전역 TTL 사용
    db.set("orders", "order2", json!({"user_id": 2, "amount": 200}), Some(30)); // 개별 TTL (30초)

    // 단일 문서 조회
    let user1 = db.get("users", "user1").unwrap_or_else(|| json!({}));
    println!("user1: {}", user1);

    let all_users = db.get("users", "*").unwrap_or_else(|| json!([]));
    println!("all_users: {}", all_users);

    // Join 연산: users와 orders를 user_id로 결합
    let results = db.join("users", "orders", "id");

    for result in results {
        println!("Joined result: {}", result);
    }
}
