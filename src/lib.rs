// lib.rs

// Declare modules
pub mod db;
pub mod query;
pub mod config;
pub mod subscription;

// Re-export key items to make them accessible from outside the library
pub use db::{InMemoryDB, OperationResult, Collection, CollectionBuilder};            // Now users can access InMemoryDB from the root
pub use query::{QueryBuilder, JoinBuilder};       // Now users can access Query from the root
pub use config::{TTL, KeyType, CollectionConfig};     // Re-export multiple items from config
pub use subscription::Subscription;
