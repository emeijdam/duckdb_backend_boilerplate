use std::sync::Arc;

use arrow_json::LineDelimitedWriter;
use arrow_json::writer::JsonArray;
use axum::{body::Bytes, extract::{Query, State}, http::{StatusCode, header}, response::{IntoResponse, Response}, Json};
use axum_extra::{TypedHeader, headers::{Authorization, authorization::Bearer}};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use duckdb::params;
use crate::state::{AppState, PagedResponse, PagingInfo};

// Gebruik State<Arc<AppState>> in plaats van Extension voor Axum 0.7
#[derive(Deserialize)]
pub struct RefreshLogParams {
    pub format: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

pub async fn get_refresh_log(
    State(state): State<Arc<AppState>>,
    _auth: Option<TypedHeader<Authorization<Bearer>>>,
    Query(params): Query<RefreshLogParams>, 
) -> Response {
    let limit = params.limit.unwrap_or(100);
    let offset = params.offset.unwrap_or(0);
    let format = params.format.clone().unwrap_or_else(|| "json".to_string());

    let pool = state.pool.clone();

    if format == "ndjson" {
        let (tx, rx) = mpsc::channel(10);
        tokio::task::spawn_blocking(move || {
            let conn = match pool.get() {
                Ok(c) => c,
                Err(_) => return,
            };

            let query = "SELECT * FROM data_refresh_log ORDER BY refresh_timestamp DESC".to_string();
            let mut stmt = match conn.prepare(&query) {
                Ok(s) => s,
                Err(_) => return,
            };

            let arrow_reader = match stmt.query_arrow([]) {
                Ok(r) => r,
                Err(_) => return,
            };

            for batch in arrow_reader {
                if tx.blocking_send(batch).is_err() {
                    break; 
                }
            }
        });

        let stream = ReceiverStream::new(rx).map(|batch| {
            let mut buffer = Vec::new();
            let mut writer = LineDelimitedWriter::new(&mut buffer);
            writer.write(&batch).ok();
            Ok::<_, std::io::Error>(Bytes::from(buffer))
        });

        return Response::builder()
            .header(header::CONTENT_TYPE, "application/x-ndjson")
            .body(axum::body::Body::from_stream(stream))
            .unwrap_or_else(|_| StatusCode::INTERNAL_SERVER_ERROR.into_response());
    }

    // Default JSON branch
    let result = tokio::task::spawn_blocking(move || {
        let conn = pool.get().map_err(|e| e.to_string())?;
        
        let total: usize = conn.query_row("SELECT count(*) FROM data_refresh_log", [], |row| row.get(0))
            .map_err(|e| e.to_string())?;

        let query = "SELECT * FROM data_refresh_log ORDER BY refresh_timestamp DESC LIMIT ? OFFSET ?".to_string();
        let mut stmt = conn.prepare(&query).map_err(|e| e.to_string())?;
        let arrow_reader = stmt.query_arrow(params![limit as i64, offset as i64]).map_err(|e| e.to_string())?;

        let mut buffer = Vec::new();
        let mut writer = arrow_json::WriterBuilder::new().build::<_, JsonArray>(&mut buffer);
        for batch in arrow_reader {
            writer.write(&batch).map_err(|e| e.to_string())?;
        }
        writer.finish().map_err(|e| e.to_string())?;

        let data: serde_json::Value = serde_json::from_slice(&buffer).map_err(|e| e.to_string())?;
        
        Ok::<(serde_json::Value, usize), String>((data, total))
    }).await;

    match result {
        Ok(Ok((data, total))) => {
            let response = PagedResponse {
                data,
                paging: PagingInfo {
                    limit,
                    offset,
                    total,
                },
            };
            Json(response).into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
