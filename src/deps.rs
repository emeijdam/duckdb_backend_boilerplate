//! GET /packages/{name}/deps — the dependency + supply-chain view of ONE package.
//!
//! CRAN's DESCRIPTION index gives us the raw `Depends/Imports/LinkingTo/
//! Suggests/Enhances` strings; this endpoint parses them and resolves every
//! named dependency against `packages_search`, so the UI can answer the
//! auditor's question directly: "is everything this package pulls in
//! CVE-clean and mirror-eligible?" It also reports where the package is
//! actually shipped — which curated lists contain it (from the built
//! `manifest.json`s under $CROSV_MIRROR_OUT) with the exact WASM SHA-256 and
//! the SBOM that attests to it.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::Serialize;
use serde_json::json;

use crate::builder::BUNDLED;
use crate::lists::repo_url;
use crate::state::AppState;

/// One parsed dependency reference, e.g. `rlang (>= 1.1.0)` from `Imports`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DepRef {
    pub name: String,
    /// The DESCRIPTION field it came from (Depends / Imports / LinkingTo /
    /// Suggests / Enhances).
    pub kind: &'static str,
    /// Version constraint without parens, e.g. `>= 1.1.0`; empty if none.
    pub constraint: String,
}

/// Fields whose members are needed at install/load time — the ones the mirror
/// builder's closure follows and the ones that carry supply-chain risk.
pub const HARD_FIELDS: [&str; 3] = ["Depends", "Imports", "LinkingTo"];
/// Optional fields: pulled only for tests/vignettes/extra features.
pub const SOFT_FIELDS: [&str; 2] = ["Suggests", "Enhances"];

/// Parse one raw DESCRIPTION dependency field. Handles the index's quirks:
/// embedded newlines (also *inside* a constraint, `(>\n1.0.1)`), the literal
/// `NA` for an absent field, and the `R (>= x)` pseudo-dependency, which is
/// dropped (it's the R version requirement, not a package).
pub fn parse_dep_field(kind: &'static str, raw: Option<&str>) -> Vec<DepRef> {
    let Some(raw) = raw else { return vec![] };
    let flat = raw.split_whitespace().collect::<Vec<_>>().join(" ");
    if flat.is_empty() || flat == "NA" {
        return vec![];
    }
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    for part in flat.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let (name, constraint) = match part.split_once('(') {
            Some((n, c)) => (n.trim(), c.trim_end_matches(')').trim().to_string()),
            None => (part, String::new()),
        };
        if name.is_empty() || name == "R" || !seen.insert(name.to_string()) {
            continue;
        }
        out.push(DepRef { name: name.to_string(), kind, constraint });
    }
    out
}

/// Raw dependency fields of one package row, in DESCRIPTION order.
struct RawDeps {
    version: String,
    fields: [(&'static str, Option<String>); 5],
}

fn is_bundled(name: &str) -> bool {
    BUNDLED.contains(&name)
}

/// Resolved status of a dependency as the registry knows it.
#[derive(Serialize, Clone)]
struct Resolved {
    version: String,
    osv_safety_status: String,
    osv_id: Option<String>,
    license_spdx: Option<String>,
    license_verdict: String,
    mirror_eligible: bool,
}

/// Where a package is actually shipped: one entry per curated list whose built
/// manifest contains it. Pulled from `<CROSV_MIRROR_OUT>/l/<list>/manifest.json`
/// — the artifact that was built, not the list's declared roots.
#[derive(Serialize)]
struct Shipped {
    list: String,
    url: String,
    version: String,
    /// WebR artifact (Sparrow R Studio) — absent when r-wasm hasn't built it.
    wasm_sha256: Option<String>,
    bytes: Option<u64>,
    /// CRAN source tarball under `src/contrib/` (desktop R / RStudio / renv).
    src_version: Option<String>,
    src_sha256: Option<String>,
    src_bytes: Option<u64>,
    purl: String,
    sbom_url: String,
    sbom_sha256: Option<String>,
    sbom_generated: Option<String>,
    signed: bool,
    install: String,
}

#[derive(Serialize)]
struct BlockedIn {
    list: String,
    reason: String,
}

/// Scan the built manifests for `pkg`. Missing/unparseable manifests are
/// skipped — the endpoint must not fail because one list was never built.
fn scan_manifests(pkg: &str) -> (Vec<Shipped>, Vec<BlockedIn>) {
    let root = std::env::var("CROSV_MIRROR_OUT").unwrap_or_else(|_| "/app/lists".into());
    let mut shipped = Vec::new();
    let mut blocked = Vec::new();
    let Ok(entries) = std::fs::read_dir(format!("{root}/l")) else {
        return (shipped, blocked);
    };
    let mut names: Vec<String> = entries
        .flatten()
        .filter(|e| e.path().is_dir())
        .filter_map(|e| e.file_name().to_str().map(String::from))
        .collect();
    names.sort();
    for list in names {
        let Ok(bytes) = std::fs::read(format!("{root}/l/{list}/manifest.json")) else { continue };
        let Ok(m) = serde_json::from_slice::<serde_json::Value>(&bytes) else { continue };
        let url = repo_url(&list);
        if let Some(c) = m["components"]
            .as_array()
            .and_then(|cs| cs.iter().find(|c| c["name"].as_str() == Some(pkg)))
        {
            let version = c["version"].as_str().unwrap_or_default().to_string();
            let sbom = &m["sbom"];
            shipped.push(Shipped {
                purl: format!("pkg:cran/{pkg}@{version}"),
                install: format!("install.packages(\"{pkg}\", repos = \"{url}\")"),
                sbom_url: format!("{url}/{}", sbom["file"].as_str().unwrap_or("sbom.cdx.json")),
                sbom_sha256: sbom["sha256"].as_str().map(String::from),
                sbom_generated: sbom["generated"].as_str().map(String::from),
                signed: sbom["signature"].is_object(),
                list: list.clone(),
                url,
                version,
                wasm_sha256: c["wasm_sha256"].as_str().map(String::from),
                bytes: c["bytes"].as_u64(),
                src_version: c["src_version"].as_str().map(String::from),
                src_sha256: c["src_sha256"].as_str().map(String::from),
                src_bytes: c["src_bytes"].as_u64(),
            });
        } else if let Some(reason) = m["blocked"][pkg].as_str() {
            blocked.push(BlockedIn { list, reason: reason.to_string() });
        }
    }
    (shipped, blocked)
}

pub async fn get_package_deps(
    State(state): State<Arc<AppState>>,
    Path(name): Path<String>,
) -> Response {
    // Package names are [A-Za-z0-9.]; anything else can't be a CRAN package.
    if name.is_empty()
        || name.len() > 100
        || !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '.')
    {
        return (StatusCode::BAD_REQUEST, "invalid package name").into_response();
    }

    let pool = state.pool.clone();
    let pkg = name.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<Option<serde_json::Value>, String> {
        let conn = pool.get().map_err(|e| e.to_string())?;

        let raw: Option<RawDeps> = conn
            .query_row(
                "SELECT Version, Depends, Imports, LinkingTo, Suggests, Enhances \
                 FROM packages_search WHERE Package = ? LIMIT 1",
                duckdb::params![pkg],
                |r| {
                    Ok(RawDeps {
                        version: r.get(0)?,
                        fields: [
                            ("Depends", r.get(1)?),
                            ("Imports", r.get(2)?),
                            ("LinkingTo", r.get(3)?),
                            ("Suggests", r.get(4)?),
                            ("Enhances", r.get(5)?),
                        ],
                    })
                },
            )
            .ok();
        let Some(raw) = raw else { return Ok(None) };

        let mut hard: Vec<DepRef> = Vec::new();
        let mut soft: Vec<DepRef> = Vec::new();
        for (kind, val) in &raw.fields {
            let parsed = parse_dep_field(kind, val.as_deref());
            if HARD_FIELDS.contains(kind) { hard.extend(parsed) } else { soft.extend(parsed) }
        }

        // Resolve every non-base name in ONE query.
        let wanted: Vec<String> = hard
            .iter()
            .chain(soft.iter())
            .map(|d| d.name.clone())
            .filter(|n| !is_bundled(n))
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        let mut resolved: BTreeMap<String, Resolved> = BTreeMap::new();
        if !wanted.is_empty() {
            let ph = wanted.iter().map(|_| "?").collect::<Vec<_>>().join(",");
            let q = format!(
                "SELECT Package, Version, osv_safety_status, osv_id, license_spdx, \
                 license_verdict, mirror_eligible FROM packages_search WHERE Package IN ({ph})"
            );
            let params: Vec<Box<dyn duckdb::ToSql + Send>> =
                wanted.iter().map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql + Send>).collect();
            let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();
            let mut stmt = conn.prepare(&q).map_err(|e| e.to_string())?;
            let rows = stmt
                .query_map(&refs[..], |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        Resolved {
                            version: r.get(1)?,
                            osv_safety_status: r.get::<_, Option<String>>(2)?.unwrap_or_else(|| "SAFE".into()),
                            osv_id: r.get(3)?,
                            license_spdx: r.get(4)?,
                            license_verdict: r.get::<_, Option<String>>(5)?.unwrap_or_else(|| "review".into()),
                            mirror_eligible: r.get::<_, Option<bool>>(6)?.unwrap_or(false),
                        },
                    ))
                })
                .map_err(|e| e.to_string())?;
            for row in rows {
                let (n, res) = row.map_err(|e| e.to_string())?;
                resolved.insert(n, res);
            }
        }

        let describe = |d: &DepRef| -> serde_json::Value {
            let base = is_bundled(&d.name);
            let mut v = json!({
                "name": d.name, "kind": d.kind, "constraint": d.constraint,
                "base": base, "on_cran": base || resolved.contains_key(&d.name),
            });
            if let Some(r) = resolved.get(&d.name) {
                v["version"] = json!(r.version);
                v["osv_safety_status"] = json!(r.osv_safety_status);
                v["osv_id"] = json!(r.osv_id);
                v["license_spdx"] = json!(r.license_spdx);
                v["license_verdict"] = json!(r.license_verdict);
                v["mirror_eligible"] = json!(r.mirror_eligible);
            }
            v
        };

        // The roll-up an auditor reads first: over the HARD set only.
        let hard_base = hard.iter().filter(|d| is_bundled(&d.name)).count();
        let vulnerable: Vec<&str> = hard
            .iter()
            .filter(|d| resolved.get(&d.name).map(|r| r.osv_safety_status == "VULNERABLE").unwrap_or(false))
            .map(|d| d.name.as_str())
            .collect();
        let not_eligible: Vec<&str> = hard
            .iter()
            .filter(|d| resolved.get(&d.name).map(|r| !r.mirror_eligible).unwrap_or(false))
            .map(|d| d.name.as_str())
            .collect();
        let unknown: Vec<&str> = hard
            .iter()
            .filter(|d| !is_bundled(&d.name) && !resolved.contains_key(&d.name))
            .map(|d| d.name.as_str())
            .collect();

        let (shipped, blocked_in) = scan_manifests(&pkg);

        Ok(Some(json!({
            "package": pkg,
            "version": raw.version,
            "hard": hard.iter().map(describe).collect::<Vec<_>>(),
            "soft": soft.iter().map(describe).collect::<Vec<_>>(),
            "summary": {
                "hard_total": hard.len(),
                "hard_base": hard_base,
                "hard_cran": hard.len() - hard_base,
                "soft_total": soft.len(),
                "vulnerable": vulnerable,
                "not_mirror_eligible": not_eligible,
                "unknown": unknown,
            },
            "curated": shipped,
            "blocked_in": blocked_in,
        })))
    })
    .await;

    match result {
        Ok(Ok(Some(v))) => Json(v).into_response(),
        Ok(Ok(None)) => (StatusCode::NOT_FOUND, "no such package").into_response(),
        Ok(Err(e)) => {
            tracing::error!("deps({name}): {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_cran_index_quirks() {
        // Real ggplot2 4.0.3 Imports as it appears in packages.csv: embedded
        // newlines, one of them INSIDE a constraint.
        let raw = "cli, grDevices, grid, gtable (>= 0.3.6), isoband, lifecycle (>\n1.0.1), rlang (>= 1.1.0), S7, scales (>= 1.4.0), stats, vctrs\n(>= 0.6.0), withr (>= 2.5.0)";
        let deps = parse_dep_field("Imports", Some(raw));
        let names: Vec<&str> = deps.iter().map(|d| d.name.as_str()).collect();
        assert_eq!(
            names,
            ["cli", "grDevices", "grid", "gtable", "isoband", "lifecycle", "rlang", "S7", "scales", "stats", "vctrs", "withr"]
        );
        let lifecycle = deps.iter().find(|d| d.name == "lifecycle").unwrap();
        assert_eq!(lifecycle.constraint, "> 1.0.1");
        let vctrs = deps.iter().find(|d| d.name == "vctrs").unwrap();
        assert_eq!(vctrs.constraint, ">= 0.6.0");
        assert!(deps.iter().all(|d| d.kind == "Imports"));
    }

    #[test]
    fn drops_r_na_and_duplicates() {
        assert!(parse_dep_field("Depends", Some("R (>= 4.1)")).is_empty());
        assert!(parse_dep_field("LinkingTo", Some("NA")).is_empty());
        assert!(parse_dep_field("LinkingTo", Some("")).is_empty());
        assert!(parse_dep_field("LinkingTo", None).is_empty());
        let d = parse_dep_field("Depends", Some("R (>= 3.5), methods, methods, Rcpp (>= 1.0)"));
        assert_eq!(d.len(), 2);
        assert_eq!(d[1].name, "Rcpp");
        assert_eq!(d[1].constraint, ">= 1.0");
    }

    #[test]
    fn base_packages_are_recognised() {
        assert!(is_bundled("stats"));
        assert!(is_bundled("MASS"));
        assert!(!is_bundled("ggplot2"));
    }
}
