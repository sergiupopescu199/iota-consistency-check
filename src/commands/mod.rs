pub mod kv_store;
pub mod rest_kv;

/// Maximum number of items to fetch in a single request or concurrently.
pub const MAX_ITEMS_TO_FETCH: usize = 10_000;
