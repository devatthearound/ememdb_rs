// subscription.rs
use serde_json::Value;
use std::sync::{Arc, Mutex};

pub enum EventType<'a> {
    Insert,
    Update,
    Delete,
    ColumnUpdate(&'a str), // Event for specific column updates
}

type Callback<'a> = Arc<Mutex<dyn Fn(&str, &Value) + Send + Sync + 'a>>;

pub struct Subscription<'a> {
    pub event_type: EventType<'a>,
    pub callback: Callback<'a>, // Collection/document ID and updated data
}

impl<'a> Subscription<'a> {
    pub fn new(event_type: EventType<'a>, callback: impl Fn(&str, &Value) + Send + Sync + 'a) -> Self {
        Subscription {
            event_type,
            callback: Arc::new(Mutex::new(callback)) as Arc<Mutex<dyn Fn(&str, &Value) + Send + Sync + 'a>>,
        }
    }

    pub fn trigger(&self, id: &str, data: &Value) {
        if let Ok(callback) = self.callback.lock() {
            callback(id, data);
        } else {
            eprintln!("Failed to acquire lock for subscription callback");
        }
    }
}
