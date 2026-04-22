use std::sync::Arc;

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;

use crate::state::AppState;

pub async fn health_check(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let uptime = state.start_time.elapsed().as_secs();

    // We explicitly move the pool and map errors to String
    let db_check = tokio::task::spawn_blocking(move || {
        // 1. Map r2d2::Error to String
        let conn = state.pool.get().map_err(|e| e.to_string())?;
        
        // 2. Map duckdb::Error to String
        conn.query_row("SELECT 1", [], |_| Ok(()))
            .map_err(|e| e.to_string())
    }).await;

    match db_check {
        // Result<Result<(), String>, JoinError>
        Ok(Ok(_)) => (
            StatusCode::OK,
            Json(json!({
                "status": "healthy",
                "database": "connected",
                "uptime_seconds": uptime,
                "version": env!("CARGO_PKG_VERSION")
            })),
        ),
        Ok(Err(e)) => {
            eprintln!("Database health check failed: {}", e);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({
                    "status": "unhealthy",
                    "database": "disconnected",
                    "error": e // Optional: show the error message
                })),
            )
        },
        Err(_) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"status": "error", "message": "Task join failed"})),
        ),
    }
}