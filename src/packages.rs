use std::sync::Arc;

use arrow_json::LineDelimitedWriter;
use axum::{body::Bytes, extract::{Query, State}, http::{StatusCode, header}, response::{IntoResponse, Response}};
use axum_extra::{TypedHeader, headers::{Authorization, authorization::Bearer}};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::state::AppState;

// Gebruik State<Arc<AppState>> in plaats van Extension voor Axum 0.7
#[derive(Deserialize)]
pub struct PackageParams {
    package: Option<String>,
    _maintainer: Option<String>,
    _license: Option<String>,
    osv_safety_status: Option<String>,
    limit: Option<usize>,
    offset: Option<usize>,
    sort_by: Option<String>,
    sort_order: Option<String>,
}

pub async fn get_packages(
    State(state): State<Arc<AppState>>,
    _auth: Option<TypedHeader<Authorization<Bearer>>>,
    Query(params): Query<PackageParams>,
) -> Response {

    let pool = state.pool.clone();
    let (tx, rx) = mpsc::channel(10);

    tokio::task::spawn_blocking(move || {
        let conn = match pool.get() {
            Ok(c) => c,
            Err(_) => return,
        };

        // Build a dynamic query safely
        let mut query = "SELECT Package, Title, Description, Published, Version, status, osv_id, osv_safety_status FROM packages_search WHERE 1=1".to_string();
        let mut sql_params: Vec<Box<dyn duckdb::ToSql + Send>> = Vec::new();

        if let Some(ref p) = params.package {
            query.push_str(" AND (Package ILIKE ? OR Title ILIKE ? OR Description ILIKE ? OR Published ILIKE ? OR Version ILIKE ? OR status ILIKE ? OR osv_id ILIKE ? OR osv_safety_status ILIKE ?)");
            let pattern = format!("%{}%", p);
            for _ in 0..8 {
                sql_params.push(Box::new(pattern.clone()));
            }
        }

        if let Some(ref status) = params.osv_safety_status {
            if !status.is_empty() && status.to_uppercase() != "ALL" {
                query.push_str(" AND osv_safety_status = ?");
                sql_params.push(Box::new(status.to_uppercase()));
            }
        }

        // Maintainer and License are not currently in the package_safety_report view.
        // To support them, they should be added to the view in config/init.sql.

        if let Some(ref sb) = params.sort_by {
            // Whitelist sort columns to prevent SQL injection
            let safe_col = match sb.to_lowercase().as_str() {
                "package" => "Package",
                "version" => "Version",
                "published" => "Published",
                "title" => "Title",
                "status" => "status",
                "osv_id" => "osv_id",
                "osv_safety_status" => "osv_safety_status",
                _ => "Package",
            };
            
            let order = match params.sort_order.as_deref().unwrap_or_default().to_lowercase().as_str() {
                "desc" => "DESC",
                _ => "ASC",
            };
            
            query.push_str(&format!(" ORDER BY {} {}", safe_col, order));
        }

        query.push_str(" LIMIT ? OFFSET ?");
        sql_params.push(Box::new(params.limit.unwrap_or(1000) as i64));
        sql_params.push(Box::new(params.offset.unwrap_or(0) as i64));

        let mut stmt = match conn.prepare(&query) {
            Ok(s) => s,
            Err(e) => {
                tracing::error!("Failed to prepare query: {}, query: {}", e, query);
                return;
            },
        };

        // Convert Vec<Box<dyn ToSql + Send>> to &[&dyn ToSql]
        let params_refs: Vec<&dyn duckdb::ToSql> = sql_params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();

        let arrow_reader = match stmt.query_arrow(&params_refs[..]) {
            Ok(r) => r,
            Err(e) => {
                tracing::error!("Failed to execute query: {}, query: {}", e, query);
                return;
            },
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
