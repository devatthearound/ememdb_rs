use serde_json::json;
use ememdb_rs::{InMemoryDB, TTL, KeyType, OperationResult};

fn main() -> Result<(), String> {
    // InMemoryDB 인스턴스 생성
    let mut db = InMemoryDB::new("example_db", TTL::NoTTL);

    // 사용자 컬렉션 생성
    let mut users = db.create::<serde_json::Value>()
        .name("users")
        .key("user_id")
        .key_type(KeyType::String)
        .build();

    // 샘플 사용자 데이터 삽입
    let sample_users = vec![
        json!({"user_id": "1", "name": "Alice", "age": 30, "score": 85}),
        json!({"user_id": "2", "name": "Bob", "age": 25, "score": 92}),
        json!({"user_id": "3", "name": "Charlie", "age": 35, "score": 78}),
        json!({"user_id": "4", "name": "David", "age": 28, "score": 88}),
        json!({"user_id": "5", "name": "Eve", "age": 32, "score": 95}),
    ];

    for user in sample_users {
        users.insert(user, None)?;
    }

    // 다양한 쿼리 실행
    println!("1. 나이가 30 이상인 사용자:");
    let result = users.select("*").gte("age", 28).execute();
    println!("{:?}", result);

    println!("\n2. 점수가 90 미만인 사용자:");
    let result = users.select("").lt("score", 90).execute();
    println!("{:?}", result);

    println!("\n3. 이름이 'Bob'인 사용자:");
    let result = users.select("*").eq("name", "Bob").execute();
    println!("{:?}", result);

    println!("\n4. 이름이 'Alice'가 아닌 사용자:");
    let result = users.select("*").neq("name", "Alice").execute();
    println!("{:?}", result);

    println!("\n5. 나이가 25에서 32 사이인 사용자:");
    let result = users.select("*").gte("age", 25).lte("age", 32).execute();
    println!("{:?}", result);

    println!("\n6. 점수가 80 초과이고 이름과 나이만 선택:");
    let result = users.select(
      "name, age"
    ).gt("score", 80).execute();
    println!("{:?}", result);

    let result = users.select("name, age")
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
    Ok(())
}