//! Email-notification subscriptions — CROSV's own app-domain settings.
//!
//! Ownership boundary: the Kern portal owns *identity* (who the user is) and the
//! *entitlement* (whether their tenant may use CROSV at all). CROSV owns these
//! per-user *preferences* — opt-in and digest frequency — keyed by the Kern
//! identity the portal supplies. Nothing here lives in the portal.
//!
//!   GET  /subscriptions?user=<email>   the caller's subscription (or defaults)
//!   PUT  /subscriptions                upsert {frequency, email?, lists?}
//!   GET  /subscriptions/all            every active subscriber (bearer-gated;
//!                                       the future digest job reads this)
//!
//! Identity comes from the `X-CROSV-User` header — which, in production, the
//! entitlement gate injects from the verified session (and strips from inbound
//! requests). In local dev, with no gate, the SPA passes the Kern email it read
//! from `/api/me`; the backend trusts it. Self-service reads/writes are NOT
//! bearer-gated (a user manages their own prefs); the admin/digest listing is.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use axum_extra::{headers::{authorization::Bearer, Authorization}, TypedHeader};
use duckdb::{params, OptionalExt};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

const IDENTITY_HEADER: &str = "x-crosv-user";

/// Resolve the caller's identity: the gate-injected header wins; otherwise the
/// value the SPA supplied (dev). None → the caller is anonymous.
fn identity(headers: &HeaderMap, supplied: Option<&str>) -> Option<String> {
    headers
        .get(IDENTITY_HEADER)
        .and_then(|v| v.to_str().ok())
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .or_else(|| supplied.map(str::trim).filter(|s| !s.is_empty()).map(String::from))
}

fn valid_freq(f: &str) -> bool {
    matches!(f, "off" | "daily" | "weekly")
}

fn authorized(state: &AppState, auth: Option<TypedHeader<Authorization<Bearer>>>) -> bool {
    auth.map(|TypedHeader(Authorization(b))| b.token() == state.settings.server.api_token)
        .unwrap_or(false)
}

#[derive(Deserialize)]
pub struct SubQuery {
    pub user: Option<String>,
}

/// GET /subscriptions — the caller's own subscription, or sensible defaults
/// (frequency `off`, deliver to the identity, watch all lists) if none yet.
pub async fn get_subscription(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<SubQuery>,
) -> Response {
    let Some(user) = identity(&headers, q.user.as_deref()) else {
        return (StatusCode::BAD_REQUEST, "identity required (X-CROSV-User or ?user=)").into_response();
    };

    let pool = state.pool.clone();
    let key = user.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<Option<(String, String, String)>, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        conn.query_row(
            "SELECT email, frequency, lists FROM subscriptions WHERE user_key = ?",
            params![key],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, String>(2)?)),
        )
        .optional()
        .map_err(|e| e.to_string())
    })
    .await;

    match result {
        Ok(Ok(row)) => {
            let (email, frequency, lists_json) = row
                .unwrap_or_else(|| (user.clone(), "off".to_string(), "[]".to_string()));
            let lists: Vec<String> = serde_json::from_str(&lists_json).unwrap_or_default();
            Json(json!({
                "user": user,
                "email": email,
                "frequency": frequency,
                "lists": lists,
                "subscribed": frequency != "off",
            }))
            .into_response()
        }
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[derive(Deserialize)]
pub struct PutSub {
    pub user: Option<String>,
    pub email: Option<String>,
    pub frequency: String,
    pub lists: Option<Vec<String>>,
}

/// PUT /subscriptions — upsert the caller's preferences.
pub async fn put_subscription(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<PutSub>,
) -> Response {
    let Some(user) = identity(&headers, req.user.as_deref()) else {
        return (StatusCode::BAD_REQUEST, "identity required (X-CROSV-User or body.user)").into_response();
    };
    if !valid_freq(&req.frequency) {
        return (StatusCode::BAD_REQUEST, "frequency must be one of: off, daily, weekly").into_response();
    }
    let email = req
        .email
        .map(|e| e.trim().to_string())
        .filter(|e| !e.is_empty())
        .unwrap_or_else(|| user.clone());
    let lists = req.lists.unwrap_or_default();

    let pool = state.pool.clone();
    let (key, freq) = (user.clone(), req.frequency.clone());
    let result = tokio::task::spawn_blocking(move || -> Result<(), String> {
        let lists_json = serde_json::to_string(&lists).map_err(|e| e.to_string())?;
        let conn = pool.get().map_err(|e| e.to_string())?;
        // Upsert that PRESERVES last_alerts + last_sent (the event-diff state):
        // a settings change must not reset the digest baseline.
        conn.execute(
            "INSERT INTO subscriptions (user_key, email, frequency, lists) \
             VALUES (?, ?, ?, ?) \
             ON CONFLICT (user_key) DO UPDATE SET \
                email = excluded.email, frequency = excluded.frequency, \
                lists = excluded.lists, updated_at = current_localtimestamp()",
            params![key, email, freq, lists_json],
        )
        .map_err(|e| e.to_string())?;
        Ok(())
    })
    .await;

    match result {
        Ok(Ok(())) => Json(json!({
            "user": user,
            "frequency": req.frequency,
            "subscribed": req.frequency != "off",
            "saved": true,
        }))
        .into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /subscriptions/all — every active subscriber. Bearer-gated: this is the
/// operator/digest-job view, not self-service.
pub async fn list_subscriptions(
    State(state): State<Arc<AppState>>,
    auth: Option<TypedHeader<Authorization<Bearer>>>,
) -> Response {
    if !authorized(&state, auth) {
        return (StatusCode::UNAUTHORIZED, "Provide a valid bearer token").into_response();
    }
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let mut stmt = conn
            .prepare(
                "SELECT user_key, email, frequency, lists, updated_at::VARCHAR \
                 FROM subscriptions WHERE frequency <> 'off' ORDER BY user_key",
            )
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                Ok((
                    r.get::<_, String>(0)?,
                    r.get::<_, String>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, String>(3)?,
                    r.get::<_, String>(4)?,
                ))
            })
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in rows {
            let (user, email, frequency, lists_json, updated) = row.map_err(|e| e.to_string())?;
            let lists: Vec<String> = serde_json::from_str(&lists_json).unwrap_or_default();
            out.push(json!({
                "user": user, "email": email, "frequency": frequency,
                "lists": lists, "updated_at": updated,
            }));
        }
        Ok(json!({ "subscribers": out, "count": out.len() }))
    })
    .await;

    match result {
        Ok(Ok(v)) => Json(v).into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}
