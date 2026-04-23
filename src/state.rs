use duckdb::DuckdbConnectionManager;
use std::time::Instant;
pub type Pool = r2d2::Pool<DuckdbConnectionManager>;

use crate::settings::Settings;

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
    pub start_time: Instant, // Add this
    pub settings: Settings
}
