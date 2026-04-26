use duckdb::DuckdbConnectionManager;
use std::time::Instant;
use serde::Serialize;
pub type Pool = r2d2::Pool<DuckdbConnectionManager>;

use crate::settings::Settings;

#[derive(Clone)]
pub struct AppState {
    pub pool: Pool,
    pub start_time: Instant, // Add this
    pub settings: Settings
}

#[derive(Serialize)]
pub struct PagedResponse<T> {
    pub data: T,
    pub paging: PagingInfo,
}

#[derive(Serialize)]
pub struct PagingInfo {
    pub limit: usize,
    pub offset: usize,
    pub total: usize,
}
