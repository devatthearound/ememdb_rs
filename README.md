ememdb_rs: 경량화된 인메모리 데이터베이스 라이브러리
ememdb_rs는 Rust로 작성된 경량화된 인메모리 데이터베이스 라이브러리입니다. 빠른 데이터 접근과 간단한 CRUD 작업을 지원하며, 복잡한 쿼리도 가능합니다.
설치
Cargo.toml 파일에 다음 줄을 추가하세요:
tomlCopy[dependencies]
ememdb_rs = "0.1.0"
사용법
메모리 데이터베이스 생성
ememdb_rs::{InMemoryDB, TTL};

let db = InMemoryDB::new("my_database", TTL::NoTTL);
컬렉션 생성
ememdb_rs::KeyType;

let users = db.create::<serde_json::Value>()
    .name("users")
    .key("user_id")
    .key_type(KeyType::String)
    .unique_keys(vec!["email"])
    .build();
데이터 삽입 (Insert)
serde_json::json;

let user = json!({
    "user_id": "1",
    "name": "John Doe",
    "email": "john@example.com"
});

match users.insert(user, None) {
    Ok(result) => println!("Inserted: {:?}", result),
    Err(e) => println!("Error: {}", e),
}
데이터 업데이트 (Update)
rustCopylet updated_user = json!({
    "user_id": "1",
    "name": "John Updated Doe",
    "email": "john@example.com"
});

match users.update(updated_user) {
    Ok(result) => println!("Updated: {:?}", result),
    Err(e) => println!("Error: {}", e),
}
데이터 삭제 (Delete)
rustCopymatch users.delete("1") {
    Ok(result) => println!("Deleted: {:?}", result),
    Err(e) => println!("Error: {}", e),
}
Upsert (삽입 또는 업데이트)
rustCopylet user = json!({
    "user_id": "2",
    "name": "Jane Doe",
    "email": "jane@example.com"
});

match users.upsert(user, None) {
    Ok(result) => println!("Upserted: {:?}", result),
    Err(e) => println!("Error: {}", e),
}
쿼리 실행과 콜백 함수 사용
rustCopylet result = users.select("name, email")
    .gt("age", 30)
    .on_success(|data| {
        println!("Query succeeded with {} results", data.len());
        for item in data {
            println!("{:?}", item);
        }
    })
    .on_fail(|error| {
        println!("An error occurred: {}", error);
    })
    .execute();
복잡한 조건 쿼리
rustCopylet result = users.select("*")
    .gt("age", 25)
    .lte("age", 50)
    .in_("status", vec!["active", "pending"])
    .neq("role", "admin")
    .execute();

match result {
    Ok(data) => println!("Query result: {:?}", data),
    Err(e) => println!("Query error: {}", e),
}
예제
사용자 관리 시스템
ememdb_rs::{InMemoryDB, TTL, KeyType};
use serde_json::json;

fn main() {
    let db = InMemoryDB::new("user_system", TTL::NoTTL);
    let users = db.create::<serde_json::Value>()
        .name("users")
        .key("user_id")
        .key_type(KeyType::String)
        .unique_keys(vec!["email"])
        .build();

    // 사용자 추가
    let user1 = json!({
        "user_id": "1",
        "name": "Alice",
        "email": "alice@example.com",
        "age": 30
    });
    users.insert(user1, None).unwrap();

    // 사용자 조회
    let result = users.select("*")
        .eq("name", "Alice")
        .on_success(|data| {
            println!("Found user: {:?}", data);
        })
        .execute();

    // 사용자 업데이트
    let updated_user = json!({
        "user_id": "1",
        "name": "Alice Updated",
        "email": "alice@example.com",
        "age": 31
    });
    users.update(updated_user).unwrap();

    // 업데이트된 사용자 조회
    let result = users.select("name, age")
        .gt("age", 30)
        .execute();

    println!("Updated users over 30: {:?}", result);
}
이 문서는 ememdb_rs 라이브러리의 기본적인 사용법을 설명합니다. 더 자세한 정보와 고급 기능에 대해서는 라이브러리의 공식 문서를 참조하세요.