use duckdb::{DuckdbConnectionManager, params};
use r2d2::Pool;
use std::fs;

use crate::settings::DatabaseSettings;

pub fn db_init(config: &DatabaseSettings) -> Pool<DuckdbConnectionManager> {
    // 1. Create Manager based on the filename (e.g., "data.db" or ":memory:")
    let manager = if config.filename == ":memory:" {
        tracing::debug!("Initializing in memory database.");
        DuckdbConnectionManager::memory().unwrap()
    } else {
        tracing::debug!("Initializing database: {}", &config.filename);
        DuckdbConnectionManager::file(&config.filename).unwrap()
    };

    let pool = Pool::builder()
        .max_size(10)
        .build(manager)
        .expect("Failed to create pool");

    // 2. Handle the optional init_sql_path
    // If the path exists in config AND the file exists on disk, run it.
    if let Some(path) = &config.init_sql_path {
        match fs::read_to_string(path) {
            Ok(sql) => {
                let conn = pool.get().expect("Failed to get connection");
                conn.execute_batch(&sql)
                    .expect("Failed to execute init SQL script");
                tracing::debug!("Database initialized with: {}", path);
            }
            Err(_) => {
                tracing::debug!("Notice: Init script at '{}' not found. Skipping.", path);
            }
        }
    }

    let conn = pool.get().expect("Failed to get connection");
    conn.execute_batch(
        "CREATE TABLE IF NOT EXISTS logs (
            ts TIMESTAMP DEFAULT current_timestamp,
            msg TEXT
        );
        -- Add other table definitions here --
        "
    ).expect("Failed to run database migrations");

    let msg = "hoi";
    let _ = conn.execute("INSERT INTO logs (ts, msg) VALUES (current_timestamp, ?)", params![msg]).expect("Failed to get connection");

    pool
}
