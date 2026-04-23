use arrow_json::LineDelimitedWriter;
use axum::{
    Json,
    body::Bytes,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use axum_extra::{TypedHeader, headers::{Authorization, authorization::Bearer}};
use duckdb::params;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::state::AppState;
use std::fs;

#[derive(Deserialize)]
pub struct WriteRequest {
    message: String,
}

#[derive(Serialize)]
pub struct StatusResponse {
    status: String,
    message: String,
}

pub async fn trigger_write(
    State(state): State<Arc<AppState>>,
    auth: Option<TypedHeader<Authorization<Bearer>>>,
    Json(payload): Json<WriteRequest>
) -> (StatusCode, Json<StatusResponse>) {

     // 1. Validate the Token
    let is_authorized = auth
        .map(|TypedHeader(Authorization(bearer))| {
            bearer.token() == state.settings.server.api_token
        })
        .unwrap_or(false);

    if !is_authorized {
        return 
        (
            StatusCode::UNAUTHORIZED,
            Json(StatusResponse {
                status: "UNAUTHORIZED".to_string(),
                message: "Provide valid bearer".to_string(),
            })
        )
    };
    

    let pool = state.pool.clone();
    let msg = payload.message.clone();

    let settings = state.settings.clone();

    // Offload the synchronous DuckDB write to a blocking thread
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| e.to_string())?;

        if let Some(update_path) = &settings.database.update_sql_path {
            match fs::read_to_string(update_path) {
            Ok(sql) => {
               // let conn = pool.get().expect("Failed to get connection");
                conn.execute_batch(&sql)
                    .expect("Failed to execute init SQL script");
                tracing::debug!("Database initialized with: {}", update_path);
            }
            Err(_) => {
                tracing::debug!("Notice: Init script at '{}' not found. Skipping.", update_path);
            }
        }
        };

        conn.execute("INSERT INTO logs (msg) VALUES (?)", params![msg])
            .map_err(|e| e.to_string())

      

    })
    .await;

    match result {
        Ok(Ok(_)) => (
            StatusCode::CREATED,
            Json(StatusResponse {
                status: "success".to_string(),
                message: "Data written to DuckDB".to_string(),
            }),
        ),
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(StatusResponse {
                status: "error".to_string(),
                message: "Failed to write to database".to_string(),
            }),
        ),
    }
}

pub async fn get_logs(State(state): State<Arc<AppState>>) -> String {
    let pool = state.pool.clone();
    let count: i32 = tokio::task::spawn_blocking(move || {
        let conn = pool.get().unwrap();
        conn.query_row("SELECT count(*) FROM logs", [], |row| row.get(0))
            .unwrap()
    })
    .await
    .unwrap();

    format!("Total entries: {}", count)
}

// Gebruik State<Arc<AppState>> in plaats van Extension voor Axum 0.7
#[derive(Deserialize)]
pub struct LogParams {
    level: Option<String>,
    limit: Option<usize>,
}

pub async fn get_logs_streaming(
    State(state): State<Arc<AppState>>,
    auth: Option<TypedHeader<Authorization<Bearer>>>,
    Query(params): Query<LogParams>, // Extract ?level=error&limit=100
) -> Response {

    // 1. Validate the Token
    let is_authorized = auth
        .map(|TypedHeader(Authorization(bearer))| {
            bearer.token() == state.settings.server.api_token
        })
        .unwrap_or(false);

    if !is_authorized {
        return (StatusCode::UNAUTHORIZED, Json(StatusResponse {
            status: "UNAUTHORIZED".to_string(),
            message: "Provide valid bearer".to_string(),
        })).into_response();
    }

    let pool = state.pool.clone();
    let (tx, rx) = mpsc::channel(10);

    tokio::task::spawn_blocking(move || {
        let conn = match pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };

        // Build a dynamic query safely
        let mut query = "SELECT * FROM logs WHERE 1=1".to_string();
        if params.level.is_some() {
            query.push_str(" AND level = ?");
        }
        query.push_str(" LIMIT ?");

        let mut stmt = match conn.prepare(&query) {
            Ok(s) => s,
            Err(_) => return,
        };

        // Bind parameters dynamically
        let limit = params.limit.unwrap_or(1000) as i64;

        let arrow_reader_result = if let Some(ref level) = params.level {
            stmt.query_arrow(params![level, limit])
        } else {
            stmt.query_arrow(params![limit])
        };

        let arrow_reader = match arrow_reader_result {
            Ok(r) => r,
            Err(_) => return,
        };

        // arrow_reader is the Iterator that yields RecordBatch
        for batch in arrow_reader {
            if tx.blocking_send(batch).is_err() {
                break; // Client hung up
            }
        }
    });

    let stream = ReceiverStream::new(rx).map(|batch| {
        let mut buffer = Vec::new();
        let mut writer = LineDelimitedWriter::new(&mut buffer);
        writer.write(&batch).ok();
        Ok::<_, std::io::Error>(Bytes::from(buffer))
    });

    Response::builder()
        .header(header::CONTENT_TYPE, "application/x-ndjson")
        .body(axum::body::Body::from_stream(stream))
        .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response())
}
