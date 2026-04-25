use std::sync::Arc;

use arrow_json::LineDelimitedWriter;
use axum::{body::Bytes, extract::{Query, State}, http::{StatusCode, header}, response::{IntoResponse, Response}};
use axum_extra::{TypedHeader, headers::{Authorization, authorization::Bearer}};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use duckdb::params;
use crate::state::AppState;

// Gebruik State<Arc<AppState>> in plaats van Extension voor Axum 0.7
#[derive(Deserialize)]
pub struct LogParams {
    level: Option<String>,
    limit: Option<usize>,
}

pub async fn get_current_release(
    State(state): State<Arc<AppState>>,
    _auth: Option<TypedHeader<Authorization<Bearer>>>,
    Query(params): Query<LogParams>, // Extract ?level=error&limit=100
) -> Response {

    let pool = state.pool.clone();
    let (tx, rx) = mpsc::channel(10);

    tokio::task::spawn_blocking(move || {
        let conn = match pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };

        // Build a dynamic query safely
        let mut query = "SELECT * FROM r_release_history WHERE 1=1".to_string();
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
