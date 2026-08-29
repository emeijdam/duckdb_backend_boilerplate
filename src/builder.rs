//! Curated-repository builder (API-driven, in-process — no external script).
//!
//! Materializes a named curated list into a WebR-format package repository by
//! re-hosting the WASM `.tgz`s r-wasm.org already built, restricted to the list
//! (and its dependency closure) and GATE-CHECKED so every member is
//! mirror-eligible. Runs inside the backend with direct DB access; triggered by
//! `POST /lists/{name}/build`.
//!
//! (Phase 2 — source `src/contrib` for desktop R, and compiling packages r-wasm
//! hasn't built via the rwasm/Docker toolchain — attaches here as further steps.)

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::PathBuf;

use duckdb::params;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::state::Pool;

const UPSTREAM: &str = "https://repo.r-wasm.org";

/// base + recommended packages ship inside WebR — never mirrored, skip in closure.
const BUNDLED: &[&str] = &[
    "base", "compiler", "datasets", "graphics", "grDevices", "grid", "methods",
    "parallel", "splines", "stats", "stats4", "tcltk", "tools", "utils", "translations",
    "KernSmooth", "MASS", "Matrix", "boot", "class", "cluster", "codetools", "foreign",
    "lattice", "mgcv", "nlme", "nnet", "rpart", "spatial", "survival",
];

#[derive(Serialize)]
pub struct Component {
    pub name: String,
    pub version: String,
    pub wasm_sha256: String,
    pub bytes: usize,
}

#[derive(Serialize)]
pub struct BuildReport {
    pub name: String,
    pub r_version: String,
    pub roots: Vec<String>,
    pub shipped: usize,
    pub blocked: HashMap<String, String>,
    pub out_dir: String,
    pub components: Vec<Component>,
}

struct Record {
    version: String,
    deps: HashSet<String>,
    raw: String,
}

fn http_get(url: &str) -> Result<Vec<u8>, String> {
    let resp = ureq::get(url).call().map_err(|e| format!("GET {url}: {e}"))?;
    let mut buf = Vec::new();
    resp.into_reader()
        .read_to_end(&mut buf)
        .map_err(|e| format!("read {url}: {e}"))?;
    Ok(buf)
}

/// Parse a CRAN/WebR PACKAGES (DCF) into name → record, keeping the raw block.
fn parse_packages(text: &str) -> HashMap<String, Record> {
    let mut out = HashMap::new();
    for block in text.split("\n\n") {
        let block = block.trim_matches('\n');
        if block.is_empty() {
            continue;
        }
        let (mut name, mut version, mut deps, mut key) =
            (String::new(), String::new(), HashSet::new(), String::new());
        let mut fields: HashMap<String, String> = HashMap::new();
        for line in block.lines() {
            if line.starts_with(' ') || line.starts_with('\t') {
                if let Some(v) = fields.get_mut(&key) {
                    v.push(' ');
                    v.push_str(line.trim());
                }
            } else if let Some((k, v)) = line.split_once(':') {
                key = k.trim().to_string();
                fields.insert(key.clone(), v.trim().to_string());
            }
        }
        if let Some(p) = fields.get("Package") {
            name = p.clone();
        }
        if let Some(v) = fields.get("Version") {
            version = v.clone();
        }
        for f in ["Depends", "Imports", "LinkingTo"] {
            if let Some(val) = fields.get(f) {
                for part in val.split(',') {
                    let d = part.split('(').next().unwrap_or("").trim();
                    if !d.is_empty() && d != "R" {
                        deps.insert(d.to_string());
                    }
                }
            }
        }
        if !name.is_empty() {
            out.insert(name.clone(), Record { version, deps, raw: block.to_string() });
        }
    }
    out
}

/// Transitive dependency closure over the upstream index, skipping bundled pkgs.
fn closure(roots: &[String], index: &HashMap<String, Record>) -> HashSet<String> {
    let bundled: HashSet<&str> = BUNDLED.iter().copied().collect();
    let mut seen = HashSet::new();
    let mut stack: Vec<String> = roots.to_vec();
    while let Some(p) = stack.pop() {
        if seen.contains(&p) || bundled.contains(p.as_str()) {
            continue;
        }
        seen.insert(p.clone());
        if let Some(rec) = index.get(&p) {
            for d in &rec.deps {
                if !seen.contains(d) {
                    stack.push(d.clone());
                }
            }
        }
    }
    seen
}

/// The mirror-eligible package names (CVE-clean AND license-clear) from the DB.
fn eligible_set(pool: &Pool) -> Result<HashSet<String>, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    let mut stmt = conn
        .prepare("SELECT DISTINCT Package FROM packages_search WHERE mirror_eligible = true")
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| r.get::<_, String>(0))
        .map_err(|e| e.to_string())?;
    Ok(rows.filter_map(|r| r.ok()).collect())
}

/// Load a curated list's package names.
fn list_packages(pool: &Pool, name: &str) -> Result<Option<Vec<String>>, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    let json: Option<String> = conn
        .query_row(
            "SELECT packages FROM curated_lists WHERE name = ?",
            params![name],
            |r| r.get(0),
        )
        .ok();
    Ok(json.map(|s| serde_json::from_str::<Vec<String>>(&s).unwrap_or_default()))
}

/// Build the WebR repo for `name` under `<out_root>/l/<name>/`.
pub fn build_list(
    pool: &Pool,
    name: &str,
    rver: &str,
    out_root: &str,
) -> Result<BuildReport, String> {
    let roots = list_packages(pool, name)?.ok_or_else(|| "no such list".to_string())?;
    let contrib = format!("bin/emscripten/contrib/{rver}");
    let index = parse_packages(&String::from_utf8_lossy(&http_get(&format!(
        "{UPSTREAM}/{contrib}/PACKAGES"
    ))?));
    let allow = eligible_set(pool)?;

    let want = closure(&roots, &index);
    // GATE: every closure member must be eligible AND present upstream.
    let mut blocked: HashMap<String, String> = HashMap::new();
    for pkg in &want {
        if !index.contains_key(pkg) {
            blocked.insert(pkg.clone(), "not built upstream (needs rwasm)".into());
        } else if !allow.contains(pkg) {
            blocked.insert(pkg.clone(), "NOT mirror-eligible (CVE/license gate)".into());
        }
    }
    let mut ship: Vec<String> = want.iter().filter(|p| !blocked.contains_key(*p)).cloned().collect();
    ship.sort();

    let out_dir = PathBuf::from(out_root).join("l").join(name);
    let contrib_dir = out_dir.join(&contrib);
    std::fs::create_dir_all(&contrib_dir).map_err(|e| e.to_string())?;

    let mut kept_raw = Vec::new();
    let mut components = Vec::new();
    for pkg in &ship {
        let rec = &index[pkg];
        let tgz = format!("{pkg}_{}.tgz", rec.version);
        let blob = http_get(&format!("{UPSTREAM}/{contrib}/{tgz}"))?;
        let sha = format!("{:x}", Sha256::digest(&blob));
        std::fs::write(contrib_dir.join(&tgz), &blob).map_err(|e| e.to_string())?;
        kept_raw.push(rec.raw.clone());
        components.push(Component {
            name: pkg.clone(),
            version: rec.version.clone(),
            wasm_sha256: sha,
            bytes: blob.len(),
        });
    }

    // Filtered PACKAGES + PACKAGES.gz (our vetted subset only).
    let packages = format!("{}\n", kept_raw.join("\n\n"));
    std::fs::write(contrib_dir.join("PACKAGES"), &packages).map_err(|e| e.to_string())?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(packages.as_bytes()).map_err(|e| e.to_string())?;
    std::fs::write(contrib_dir.join("PACKAGES.gz"), gz.finish().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;

    let report = BuildReport {
        name: name.to_string(),
        r_version: rver.to_string(),
        roots,
        shipped: ship.len(),
        blocked,
        out_dir: out_dir.to_string_lossy().to_string(),
        components,
    };
    std::fs::write(
        out_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;

    Ok(report)
}
