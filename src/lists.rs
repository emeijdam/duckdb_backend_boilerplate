//! Curated package lists — the CROSV "curated repository builder".
//!
//! A list is a NAMED selection of mirror-eligible (CVE-clean AND license-clear)
//! packages. Each list is addressable at a stable URL that materializes as a
//! CRAN-compatible repository (src/contrib for desktop R + bin/emscripten/contrib
//! for WebR/Sparrow), so ONE url installs the vetted set from any R flavour:
//!   Sparrow/WebR :  repoUrl = <url>
//!   RStudio / R  :  install.packages(pkg, repos = "<url>")
//!
//!   POST /lists            {name, packages[]}  create/replace (bearer-gated)
//!   GET  /lists                                all lists
//!   GET  /lists/{name}                         manifest + install snippets

use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use axum_extra::{headers::{authorization::Bearer, Authorization}, TypedHeader};
use serde::Deserialize;
use serde_json::json;

use crate::state::AppState;

#[derive(Deserialize)]
pub struct CreateList {
    pub name: String,
    pub packages: Vec<String>,
}

/// URL-safe list name: [A-Za-z0-9_-], 1–64 chars.
fn slug_ok(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 64
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
}

/// Public base under which built list-repos are served. Set CROSV_MIRROR_BASE
/// (e.g. https://crosv.dasc.nl); the repo for a list lives at <base>/l/<name>.
fn repo_url(name: &str) -> String {
    let base = std::env::var("CROSV_MIRROR_BASE")
        .unwrap_or_else(|_| "https://crosv.dasc.nl".into());
    format!("{}/l/{}", base.trim_end_matches('/'), name)
}

fn authorized(state: &AppState, auth: Option<TypedHeader<Authorization<Bearer>>>) -> bool {
    auth.map(|TypedHeader(Authorization(b))| b.token() == state.settings.server.api_token)
        .unwrap_or(false)
}

/// Ready-to-paste install snippets for every consumer of the list URL.
fn install_snippets(url: &str, pkgs: &[String]) -> serde_json::Value {
    let vec = format!(
        "c({})",
        pkgs.iter().map(|p| format!("\"{p}\"")).collect::<Vec<_>>().join(", ")
    );
    json!({
        "sparrow_studio": format!("Package repo → {url}  (Settings ▸ Packages, or VITE_WEBR_REPO_URL)"),
        "webr":         format!("webr::install({vec}, repos = \"{url}\")"),
        "r_desktop":    format!("install.packages({vec}, repos = \"{url}\")"),
        "r_options":    format!("options(repos = c(CURATED = \"{url}\"))"),
    })
}

#[derive(Deserialize)]
pub struct BuildParams {
    /// WebR R version / contrib dir; must match the deployed WebR (0.6.0 → 4.6).
    pub rver: Option<String>,
}

/// POST /lists/{name}/build — materialize the list into a WebR repo (in-process,
/// Rust). Bearer-gated. Returns the build report (shipped, blocked, sha256s).
pub async fn build_list_handler(
    State(state): State<Arc<AppState>>,
    auth: Option<TypedHeader<Authorization<Bearer>>>,
    Path(name): Path<String>,
    axum::extract::Query(q): axum::extract::Query<BuildParams>,
) -> Response {
    if !authorized(&state, auth) {
        return (StatusCode::UNAUTHORIZED, "Provide a valid bearer token").into_response();
    }
    if !slug_ok(&name) {
        return (StatusCode::BAD_REQUEST, "invalid name").into_response();
    }
    let rver = q.rver.unwrap_or_else(|| "4.6".into());
    let out_root =
        std::env::var("CROSV_MIRROR_OUT").unwrap_or_else(|_| "/app/lists".into());
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || {
        crate::builder::build_list(&pool, &name, &rver, &out_root)
    })
    .await;
    match result {
        Ok(Ok(report)) => (StatusCode::OK, Json(report)).into_response(),
        Ok(Err(e)) if e == "no such list" => (StatusCode::NOT_FOUND, e).into_response(),
        Ok(Err(e)) => (StatusCode::BAD_GATEWAY, e).into_response(),
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// POST /lists — validate the selection is all mirror-eligible, then store it.
pub async fn create_list(
    State(state): State<Arc<AppState>>,
    auth: Option<TypedHeader<Authorization<Bearer>>>,
    Json(req): Json<CreateList>,
) -> Response {
    if !authorized(&state, auth) {
        return (StatusCode::UNAUTHORIZED, "Provide a valid bearer token").into_response();
    }
    if !slug_ok(&req.name) {
        return (StatusCode::BAD_REQUEST, "name must be 1–64 chars of [A-Za-z0-9_-]").into_response();
    }
    if req.packages.is_empty() {
        return (StatusCode::BAD_REQUEST, "packages must not be empty").into_response();
    }

    let pool = state.pool.clone();
    let (name, pkgs) = (req.name.clone(), req.packages.clone());
    let result = tokio::task::spawn_blocking(move || -> Result<Vec<String>, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        // Which of the requested packages are actually mirror-eligible?
        let ph = pkgs.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let q = format!(
            "SELECT DISTINCT Package FROM packages_search \
             WHERE mirror_eligible = true AND Package IN ({ph})"
        );
        let params: Vec<Box<dyn duckdb::ToSql + Send>> =
            pkgs.iter().map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql + Send>).collect();
        let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();
        let mut stmt = conn.prepare(&q).map_err(|e| e.to_string())?;
        let eligible: std::collections::HashSet<String> = stmt
            .query_map(&refs[..], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?
            .filter_map(|r| r.ok())
            .collect();
        let offenders: Vec<String> =
            pkgs.iter().filter(|p| !eligible.contains(*p)).cloned().collect();
        if !offenders.is_empty() {
            return Ok(offenders); // caller returns 422 with these
        }
        // Upsert (DuckDB: delete-then-insert avoids ON CONFLICT version issues).
        let pkgs_json = serde_json::to_string(&pkgs).map_err(|e| e.to_string())?;
        conn.execute("DELETE FROM curated_lists WHERE name = ?", duckdb::params![name])
            .map_err(|e| e.to_string())?;
        conn.execute(
            "INSERT INTO curated_lists (name, packages) VALUES (?, ?)",
            duckdb::params![name, pkgs_json],
        )
        .map_err(|e| e.to_string())?;
        Ok(vec![])
    })
    .await;

    match result {
        Ok(Ok(offenders)) if offenders.is_empty() => {
            let url = repo_url(&req.name);
            (
                StatusCode::CREATED,
                Json(json!({
                    "name": req.name,
                    "packages": req.packages,
                    "url": url,
                    "install": install_snippets(&url, &req.packages),
                    "note": "Build the repo to publish it: mirror/build_list.py --name <name>",
                })),
            )
                .into_response()
        }
        Ok(Ok(offenders)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({
                "error": "some packages are not mirror-eligible (fail the CVE/license gate)",
                "ineligible": offenders,
            })),
        )
            .into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /lists — all curated lists (name + package count).
pub async fn get_lists(State(state): State<Arc<AppState>>) -> Response {
    let pool = state.pool.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let mut stmt = conn
            .prepare("SELECT name, packages, created_at::VARCHAR FROM curated_lists ORDER BY name")
            .map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map([], |r| {
                let name: String = r.get(0)?;
                let pkgs: String = r.get(1)?;
                let created: String = r.get(2)?;
                Ok((name, pkgs, created))
            })
            .map_err(|e| e.to_string())?;
        let mut lists = Vec::new();
        for row in rows {
            let (name, pkgs, created) = row.map_err(|e| e.to_string())?;
            let packages: Vec<String> = serde_json::from_str(&pkgs).unwrap_or_default();
            lists.push(json!({
                "name": name,
                "count": packages.len(),
                "created_at": created,
                "url": repo_url(&name),
            }));
        }
        Ok(json!({ "lists": lists }))
    })
    .await;
    match result {
        Ok(Ok(v)) => Json(v).into_response(),
        _ => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

/// GET /lists/{name} — the list manifest: packages (with current vetted version),
/// the repo URL, and install snippets for each consumer.
pub async fn get_list(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    if !slug_ok(&name) {
        return (StatusCode::BAD_REQUEST, "invalid name").into_response();
    }
    let pool = state.pool.clone();
    let name2 = name.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<Option<Vec<String>>, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let pkgs: Option<String> = conn
            .query_row(
                "SELECT packages FROM curated_lists WHERE name = ?",
                duckdb::params![name2],
                |r| r.get(0),
            )
            .ok();
        Ok(pkgs.map(|s| serde_json::from_str::<Vec<String>>(&s).unwrap_or_default()))
    })
    .await;

    let pkgs = match result {
        Ok(Ok(Some(p))) => p,
        Ok(Ok(None)) => return (StatusCode::NOT_FOUND, "no such list").into_response(),
        _ => return StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    };

    // Attach the current vetted version + verdict for each package.
    let pool = state.pool.clone();
    let pkgs2 = pkgs.clone();
    let versions = tokio::task::spawn_blocking(move || -> Result<serde_json::Value, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let ph = pkgs2.iter().map(|_| "?").collect::<Vec<_>>().join(",");
        let q = format!(
            "SELECT Package, Version, license_spdx, requires_source, mirror_eligible \
             FROM packages_search WHERE Package IN ({ph})"
        );
        let params: Vec<Box<dyn duckdb::ToSql + Send>> =
            pkgs2.iter().map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql + Send>).collect();
        let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();
        let mut stmt = conn.prepare(&q).map_err(|e| e.to_string())?;
        let rows = stmt
            .query_map(&refs[..], |r| {
                Ok(json!({
                    "name": r.get::<_, String>(0)?,
                    "version": r.get::<_, String>(1)?,
                    "license": r.get::<_, Option<String>>(2)?,
                    "requires_source": r.get::<_, Option<bool>>(3)?.unwrap_or(false),
                    "mirror_eligible": r.get::<_, Option<bool>>(4)?.unwrap_or(false),
                }))
            })
            .map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for row in rows {
            out.push(row.map_err(|e| e.to_string())?);
        }
        Ok(json!(out))
    })
    .await;

    let packages = match versions {
        Ok(Ok(v)) => v,
        _ => json!([]),
    };
    let url = repo_url(&name);
    Json(json!({
        "name": name,
        "url": url,
        "packages": packages,
        "install": install_snippets(&url, &pkgs),
    }))
    .into_response()
}
