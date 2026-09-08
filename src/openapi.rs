//! GET /openapi.json — the machine-readable contract for everything this
//! service serves: the registry API, the curation API, and the static
//! curated-repository layout under `/l/<list>/` that R itself reads.
//!
//! Hand-maintained (no proc-macro dependency): the surface is small and the
//! descriptions are the point — they double as the API page's copy. Keep this
//! in step with `main.rs` routes; the frontend's /api page renders it.

use std::sync::Arc;

use axum::{extract::State, Json};
use serde_json::{json, Value};

use crate::state::AppState;

fn q(name: &str, desc: &str, schema: Value) -> Value {
    json!({ "name": name, "in": "query", "description": desc, "schema": schema })
}
fn p(name: &str, desc: &str) -> Value {
    json!({ "name": name, "in": "path", "required": true, "description": desc, "schema": { "type": "string" } })
}
fn s(t: &str) -> Value {
    json!({ "type": t })
}
fn ok(desc: &str) -> Value {
    json!({ "200": { "description": desc } })
}

pub async fn openapi(State(state): State<Arc<AppState>>) -> Json<Value> {
    let mirror_base = std::env::var("CROSV_MIRROR_BASE")
        .unwrap_or_else(|_| "https://crosv.dasc.nl".into())
        .trim_end_matches('/')
        .to_string();
    let portal = state.settings.server.portal_origin.trim_end_matches('/').to_string();

    let paging = |extra: Vec<Value>| -> Value {
        let mut v = vec![
            q("format", "`json` (paged envelope `{data, paging}`) or `ndjson` (streamed, one object per line, no paging).", json!({ "type": "string", "enum": ["json", "ndjson"], "default": "json" })),
            q("limit", "Page size (json format).", json!({ "type": "integer", "default": 100 })),
            q("offset", "Page offset (json format).", json!({ "type": "integer", "default": 0 })),
        ];
        v.extend(extra);
        json!(v)
    };

    let bearer_note = "Operator bearer token (`Authorization: Bearer <api_token>`).";
    let curation_note = if portal.is_empty() {
        "Operator bearer token.".to_string()
    } else {
        format!("Operator bearer token **or** a signed-in Kern portal session (`portal_session` cookie, verified against `{portal}/api/me`; send `credentials: 'include'` from `*.dasc.nl`).")
    };

    Json(json!({
        "openapi": "3.0.3",
        "info": {
            "title": "CROSV — R-Governance API",
            "version": env!("CARGO_PKG_VERSION"),
            "description": "OSV-integrated safety monitoring for the R package ecosystem: a vetted view of CRAN (CVE status via OSV.dev, license classification, mirror-eligibility gate), Kern-gated curation of named package lists, and the curated repositories those lists become — WebR (Sparrow R Studio), CRAN source and prebuilt macOS/Windows binaries — each with a signed CycloneDX SBOM.\n\nReads are public. Curation writes need the operator token or a portal session. The SBOM of a *list* is public (`/l/<list>/sbom.cdx.json`); the global `/sbom` of everything mirror-eligible is token-gated."
        },
        "servers": [
            { "url": "https://backend.dasc.nl", "description": "Registry + curation API" },
            { "url": mirror_base, "description": "Curated repositories (`/l/<list>/…`) — same files also served here" }
        ],
        "tags": [
            { "name": "Registry", "description": "Vetted view of CRAN: safety, license, mirror gate, dependencies." },
            { "name": "Curation", "description": "Named package lists and their builds (Kern-gated)." },
            { "name": "Repository", "description": "What R reads: the built curated repositories and their proof artefacts." },
            { "name": "Compliance", "description": "SBOM and signature material." },
            { "name": "Notifications", "description": "Vulnerability-digest subscriptions (keyed by Kern identity)." },
            { "name": "Operations", "description": "Health, data refresh, logs (operator)." }
        ],
        "components": {
            "securitySchemes": {
                "operatorToken": { "type": "http", "scheme": "bearer", "description": bearer_note },
                "portalSession": { "type": "apiKey", "in": "cookie", "name": "portal_session", "description": "Kern portal session cookie (Domain=.dasc.nl)." },
                "identity": { "type": "apiKey", "in": "header", "name": "x-crosv-user", "description": "Kern identity (email) the portal supplies for notification settings." }
            }
        },
        "paths": {
            "/health": { "get": { "tags": ["Operations"], "summary": "Liveness + DB connectivity",
                "responses": ok("`{status, database, uptime_seconds, version}`") } },

            "/release": { "get": { "tags": ["Registry"], "summary": "Current R release (and history)",
                "parameters": paging(vec![]),
                "responses": ok("R release rows: version, nickname, release date.") } },

            "/packages": { "get": { "tags": ["Registry"], "summary": "Search the vetted CRAN view",
                "description": "One row per CRAN-current package with OSV safety status (`SAFE`/`FIXED`/`VULNERABLE` + `osv_id`), license axis (`License`, `license_spdx`, `license_class`, `license_verdict` allow/review/deny, `requires_source`) and the derived **`mirror_eligible`** gate (CVE-clean AND license-clear). Also carries the raw `Depends`/`Imports`/`LinkingTo`/`Suggests`/`Enhances` fields.",
                "parameters": paging(vec![
                    q("package", "Free-text filter (ILIKE) over Package, Title, Description, Version, status, osv_id, osv_safety_status.", s("string")),
                    q("osv_safety_status", "Exact filter.", json!({ "type": "string", "enum": ["SAFE", "FIXED", "VULNERABLE", "ALL"] })),
                    q("license_verdict", "Exact filter.", json!({ "type": "string", "enum": ["allow", "review", "deny", "ALL"] })),
                    q("mirror_eligible", "Only packages that pass (or fail) the mirror gate.", s("boolean")),
                    q("sort_by", "Column to sort by.", json!({ "type": "string", "enum": ["package", "version", "license", "published", "title", "status", "osv_id", "osv_safety_status"] })),
                    q("sort_order", "asc | desc", json!({ "type": "string", "enum": ["asc", "desc"] })),
                ]),
                "responses": ok("Package rows (paged envelope or ndjson stream).") } },

            "/packages/{name}/deps": { "get": { "tags": ["Registry"], "summary": "Dependency + supply-chain view of one package",
                "description": "Parses the package's DESCRIPTION dependency fields and resolves every dependency against the vetted view: `hard` (Depends/Imports/LinkingTo) and `soft` (Suggests/Enhances) entries each with `base` (ships with R), `on_cran`, `version`, `osv_safety_status`, `license_verdict`, `mirror_eligible`; a `summary` roll-up (`vulnerable`, `not_mirror_eligible`, `unknown`); `curated` — every built list shipping this package with its WASM/source/binary artefacts, hashes, purl and SBOM URL; and `blocked_in` — lists that refused it at build time, with the reason.",
                "parameters": [ p("name", "CRAN package name, e.g. `ggplot2`.") ],
                "responses": { "200": { "description": "Dependency report." }, "404": { "description": "Not a CRAN-current package." } } } },

            "/lists": {
                "get": { "tags": ["Curation"], "summary": "All curated lists", "responses": ok("`[{name, count, created_at}]`") },
                "post": { "tags": ["Curation"], "summary": "Create or replace a curated list",
                    "description": format!("Roots only — the build resolves the dependency closure and applies the mirror gate. Auth: {curation_note}"),
                    "security": [ { "operatorToken": [] }, { "portalSession": [] } ],
                    "requestBody": { "required": true, "content": { "application/json": { "schema": { "type": "object", "required": ["name", "packages"],
                        "properties": { "name": { "type": "string", "description": "URL-safe, `[A-Za-z0-9_-]{1,64}`" }, "packages": { "type": "array", "items": s("string") } } },
                        "example": { "name": "studio", "packages": ["ggplot2", "svglite"] } } } },
                    "responses": { "201": { "description": "Created — returns the list manifest with install snippets." }, "401": { "description": "Sign in to the portal, or provide a valid operator token." } } } },

            "/lists/{name}": { "get": { "tags": ["Curation"], "summary": "A list's manifest + install snippets",
                "description": "Declared roots with their current vetted version/license/eligibility, the repository URL, and copy-paste snippets for WebR, Sparrow R Studio, desktop R (`install.packages(..., repos=)`), RStudio (Global Options ▸ Packages), `Rprofile.site` and `renv.lock`.",
                "parameters": [ p("name", "List name.") ],
                "responses": { "200": { "description": "Manifest." }, "404": { "description": "No such list." } } } },

            "/lists/{name}/build": { "post": { "tags": ["Curation"], "summary": "Build (materialize) a list's repositories",
                "description": format!("Resolves the closure, applies the gate, and writes under `/l/<name>/`: the WebR repo (`bin/emscripten/contrib/<R>/`), the CRAN source repo (`src/contrib/`, tarballs MD5-verified against CRAN's index), prebuilt macOS/Windows binaries (`bin/<platform>/contrib/<R>/`), `manifest.json` and the signed CycloneDX SBOM. Returns the build report (`shipped`, `blocked{{pkg: reason}}`, `src_shipped`, `src_blocked`, `binaries[]`, `components[]`, `sbom{{sha256, signature}}`). Idempotent; the hourly data refresh re-runs it. Auth: {curation_note}"),
                "security": [ { "operatorToken": [] }, { "portalSession": [] } ],
                "parameters": [ p("name", "List name."), q("rver", "WebR R version for the WASM index (`bin/emscripten/contrib/<rver>`).", json!({ "type": "string", "default": "4.6" })) ],
                "responses": { "200": { "description": "Build report." }, "401": { "description": "Not authorized." }, "404": { "description": "No such list." } } } },

            "/l/{name}/manifest.json": { "get": { "tags": ["Repository"], "summary": "Build manifest of a list",
                "description": "The last build report: components with `wasm_sha256`, `src_sha256`, per-target binaries, and `sbom.signature` (ECDSA-P256, IEEE-P1363 value for WebCrypto + `sig_file`/`public_key_file`).",
                "parameters": [ p("name", "List name.") ], "responses": ok("Manifest JSON.") } },
            "/l/{name}/sbom.cdx.json": { "get": { "tags": ["Compliance", "Repository"], "summary": "CycloneDX 1.5 SBOM of a built list",
                "description": "One `library` component per shipped package: SPDX license, OSV status, and SHA-256 of every served artefact (`crosv:wasm_sha256`, `crosv:src_sha256`, `crosv:bin_sha256[<target>]`). `vulnerabilities: []` is the explicit attestation. Signed — verify with `sbom.cdx.json.sig` + `cosign.pub` (see the About page).",
                "parameters": [ p("name", "List name.") ], "responses": ok("SBOM JSON.") } },
            "/l/{name}/sbom.cdx.json.sig": { "get": { "tags": ["Compliance"], "summary": "Signature over the SBOM bytes",
                "description": "ECDSA-P256-SHA256, DER, base64, one line. `openssl base64 -d -A -in sbom.cdx.json.sig -out sbom.sig && openssl dgst -sha256 -verify cosign.pub -signature sbom.sig sbom.cdx.json`",
                "parameters": [ p("name", "List name.") ], "responses": ok("base64 text.") } },
            "/l/{name}/cosign.pub": { "get": { "tags": ["Compliance"], "summary": "Signing public key (SPKI PEM)",
                "parameters": [ p("name", "List name.") ], "responses": ok("PEM.") } },
            "/l/{name}/src/contrib/PACKAGES": { "get": { "tags": ["Repository"], "summary": "CRAN source index (desktop R / RStudio / renv)",
                "description": "Use the list URL as a CRAN repo: `install.packages(\"ggplot2\", repos = \"<mirror>/l/<name>\")`. Tarballs live next to it as `<pkg>_<ver>.tar.gz`.",
                "parameters": [ p("name", "List name.") ], "responses": ok("DCF index (also `PACKAGES.gz`).") } },
            "/l/{name}/bin/{platform}/contrib/{rver}/PACKAGES": { "get": { "tags": ["Repository"], "summary": "Binary index — WebR or desktop platform",
                "description": "`platform` = `emscripten` (WebR / Sparrow R Studio), `windows`, `macosx/big-sur-arm64`, `macosx/sonoma-arm64`, `macosx/big-sur-x86_64`. R's `contrib.url(type = \"binary\")` builds exactly this path, so `install.packages(type = \"binary\")` needs no compiler.",
                "parameters": [ p("name", "List name."), p("platform", "Binary platform directory."), p("rver", "R minor version, e.g. `4.6`.") ], "responses": ok("DCF index (also `PACKAGES.gz`).") } },

            "/sbom": { "get": { "tags": ["Compliance"], "summary": "Global SBOM of everything mirror-eligible",
                "description": "CycloneDX of the whole eligible set (not a built list) — the procurement/infosec artefact. Token-gated.",
                "security": [ { "operatorToken": [] } ],
                "responses": { "200": { "description": "SBOM JSON." }, "401": { "description": "Provide a valid bearer token." } } } },

            "/subscriptions": {
                "get": { "tags": ["Notifications"], "summary": "My digest settings", "security": [ { "identity": [] } ],
                    "parameters": [ q("user", "Identity override (else `x-crosv-user`).", s("string")) ],
                    "responses": ok("`{email, frequency, lists}`") },
                "put": { "tags": ["Notifications"], "summary": "Set my digest settings", "security": [ { "identity": [] } ],
                    "requestBody": { "content": { "application/json": { "schema": { "type": "object", "properties": {
                        "email": s("string"), "frequency": { "type": "string", "enum": ["off", "daily", "weekly"] }, "lists": { "type": "array", "items": s("string"), "description": "Curated lists to watch; empty = all." } } } } } },
                    "responses": ok("Saved settings.") } },
            "/subscriptions/all": { "get": { "tags": ["Notifications"], "summary": "All subscriptions (operator)", "security": [ { "operatorToken": [] } ], "responses": ok("Rows.") } },
            "/digest/run": { "post": { "tags": ["Notifications"], "summary": "Run the vulnerability digest now (operator)", "security": [ { "operatorToken": [] } ],
                "parameters": [ q("force", "Send even if nothing changed since the last digest.", s("boolean")) ], "responses": ok("Digest run report.") } },

            "/refresh": { "post": { "tags": ["Operations"], "summary": "Reload the data files into DuckDB (operator)",
                "description": "Re-runs the init SQL over the CRAN/OSV/license CSVs. The hourly sync script calls this, then rebuilds the curated lists.",
                "security": [ { "operatorToken": [] } ], "responses": { "200": { "description": "Status." }, "401": { "description": "Unauthorized." } } } },
            "/refreshlog": { "get": { "tags": ["Operations"], "summary": "Data refresh history", "parameters": paging(vec![]), "responses": ok("Refresh log rows.") } },
            "/logs": { "get": { "tags": ["Operations"], "summary": "Recent server log (text)", "responses": ok("Plain text.") } },
            "/logstream": { "get": { "tags": ["Operations"], "summary": "Server log as rows (operator)", "security": [ { "operatorToken": [] } ],
                "parameters": paging(vec![ q("level", "Filter by level, e.g. `error`.", s("string")) ]), "responses": ok("Log rows.") } },
            "/openapi.json": { "get": { "tags": ["Operations"], "summary": "This document", "responses": ok("OpenAPI 3.0.3 JSON.") } }
        }
    }))
}
