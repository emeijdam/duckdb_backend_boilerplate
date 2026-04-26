use std::sync::Arc;

use arrow_json::LineDelimitedWriter;
use arrow_json::writer::JsonArray;
use axum::{body::Bytes, extract::{Query, State}, http::{StatusCode, header}, response::{IntoResponse, Response}, Json};
use axum_extra::{TypedHeader, headers::{Authorization, authorization::Bearer}};
use futures_util::StreamExt;
use serde::Deserialize;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use crate::state::{AppState, PagedResponse, PagingInfo};

// Gebruik State<Arc<AppState>> in plaats van Extension voor Axum 0.7
#[derive(Deserialize)]
pub struct PackageParams {
    pub package: Option<String>,
    pub _maintainer: Option<String>,
    pub _license: Option<String>,
    pub osv_safety_status: Option<String>,
    pub format: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub sort_by: Option<String>,
    pub sort_order: Option<String>,
}

pub async fn get_packages(
    State(state): State<Arc<AppState>>,
    _auth: Option<TypedHeader<Authorization<Bearer>>>,
    Query(params): Query<PackageParams>,
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

            // Build a dynamic query safely
            let mut query = "SELECT Package, Title, Description, Published, Version, License, status, osv_id, osv_safety_status FROM packages_search WHERE 1=1".to_string();
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

            if let Some(ref sb) = params.sort_by {
                let safe_col = match sb.to_lowercase().as_str() {
                    "package" => "Package",
                    "version" => "Version",
                    "license" => "License",
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

            let mut stmt = match conn.prepare(&query) {
                Ok(s) => s,
                Err(_) => return,
            };

            let params_refs: Vec<&dyn duckdb::ToSql> = sql_params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();

            let arrow_reader = match stmt.query_arrow(&params_refs[..]) {
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

        // 1. Build common filter part
        let mut filter_clause = " FROM packages_search WHERE 1=1".to_string();
        let mut sql_params: Vec<Box<dyn duckdb::ToSql + Send>> = Vec::new();

        if let Some(ref p) = params.package {
            filter_clause.push_str(" AND (Package ILIKE ? OR Title ILIKE ? OR Description ILIKE ? OR Published ILIKE ? OR Version ILIKE ? OR status ILIKE ? OR osv_id ILIKE ? OR osv_safety_status ILIKE ?)");
            let pattern = format!("%{}%", p);
            for _ in 0..8 {
                sql_params.push(Box::new(pattern.clone()));
            }
        }

        if let Some(ref status) = params.osv_safety_status {
            if !status.is_empty() && status.to_uppercase() != "ALL" {
                filter_clause.push_str(" AND osv_safety_status = ?");
                sql_params.push(Box::new(status.to_uppercase()));
            }
        }

        // 2. Query total count
        let count_query = format!("SELECT COUNT(*){}", filter_clause);
        let params_refs_count: Vec<&dyn duckdb::ToSql> = sql_params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();
        let total: usize = conn.query_row(&count_query, &params_refs_count[..], |row| row.get(0))
            .map_err(|e| e.to_string())?;

        // 3. Query paged data
        let mut data_query = format!("SELECT Package, Title, Description, Published, Version, License, status, osv_id, osv_safety_status{}", filter_clause);
        
        if let Some(ref sb) = params.sort_by {
            let safe_col = match sb.to_lowercase().as_str() {
                "package" => "Package",
                "version" => "Version",
                "license" => "License",
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
            data_query.push_str(&format!(" ORDER BY {} {}", safe_col, order));
        }

        data_query.push_str(" LIMIT ? OFFSET ?");
        sql_params.push(Box::new(limit as i64));
        sql_params.push(Box::new(offset as i64));

        let params_refs_data: Vec<&dyn duckdb::ToSql> = sql_params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();
        let mut stmt = conn.prepare(&data_query).map_err(|e| e.to_string())?;
        let arrow_reader = stmt.query_arrow(&params_refs_data[..]).map_err(|e| e.to_string())?;

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
