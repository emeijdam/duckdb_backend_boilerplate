//! Curated-repository builder (API-driven, in-process — no external script).
//!
//! Materializes a named curated list into a WebR-format package repository by
//! re-hosting the WASM `.tgz`s r-wasm.org already built, restricted to the list
//! (and its dependency closure) and GATE-CHECKED so every member is
//! mirror-eligible. Runs inside the backend with direct DB access; triggered by
//! `POST /lists/{name}/build`.
//!
//! Phase 2a (done): the same list is ALSO materialized as a classic CRAN
//! source repository under `src/contrib/` — the CRAN-current tarball of every
//! closure member, fetched from CRAN, verified against the MD5 CRAN publishes
//! in its own index (so a tampered download can never be shipped), and indexed
//! by a filtered `PACKAGES`. That is what desktop R / RStudio / renv / CI read:
//! `install.packages(x, repos = "<base>/l/<list>")` just works. Both artifacts
//! of a package (WASM + source) are bound by SHA-256 in the one SBOM.
//!
//! (Still open: prebuilt macOS/Windows binaries under `bin/<os>/…`, and compiling
//! packages r-wasm hasn't built via the rwasm/Docker toolchain.)

use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use duckdb::params;
use serde::Serialize;
use sha2::{Digest, Sha256};

use crate::state::Pool;

const UPSTREAM: &str = "https://repo.r-wasm.org";

/// CRAN mirror the source tarballs are fetched from. Override with
/// `CROSV_CRAN_UPSTREAM` (e.g. an internal mirror) — every download is still
/// MD5-verified against the CRAN index we ingested, so the mirror is untrusted.
fn cran_upstream() -> String {
    std::env::var("CROSV_CRAN_UPSTREAM")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "https://cloud.r-project.org".to_string())
        .trim_end_matches('/')
        .to_string()
}

/// base + recommended packages ship inside WebR — never mirrored, skip in closure.
pub(crate) const BUNDLED: &[&str] = &[
    "base", "compiler", "datasets", "graphics", "grDevices", "grid", "methods",
    "parallel", "splines", "stats", "stats4", "tcltk", "tools", "utils", "translations",
    "KernSmooth", "MASS", "Matrix", "boot", "class", "cluster", "codetools", "foreign",
    "lattice", "mgcv", "nlme", "nnet", "rpart", "spatial", "survival",
];

/// One shipped package. A component may carry a WASM artifact (WebR), a source
/// tarball (desktop R), or both; each is bound by its own SHA-256. `version` is
/// the WASM build's version when there is one, else the source version.
#[derive(Serialize)]
pub struct Component {
    pub name: String,
    pub version: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub wasm_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<usize>,
    /// CRAN-current version served under `src/contrib/` (the vetted row).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_version: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_sha256: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub src_bytes: Option<usize>,
}

#[derive(Serialize)]
pub struct BuildReport {
    pub name: String,
    pub r_version: String,
    pub roots: Vec<String>,
    /// Packages with a WASM artifact (WebR / Sparrow R Studio).
    pub shipped: usize,
    pub blocked: HashMap<String, String>,
    /// Packages with a source tarball under `src/contrib/` (desktop R).
    pub src_shipped: usize,
    pub src_blocked: HashMap<String, String>,
    /// Prebuilt CRAN binaries, one entry per mirrored `bin/<platform>/contrib/<R>`
    /// target (RStudio on macOS/Windows installs these without a compiler).
    pub binaries: Vec<BinTarget>,
    pub out_dir: String,
    pub components: Vec<Component>,
    /// The CRA compliance proof: a CycloneDX SBOM written next to the repo,
    /// pinned by its own SHA-256 so the manifest attests to an exact bill.
    pub sbom: Option<SbomRef>,
}

/// Pointer + integrity hash for the emitted CycloneDX SBOM (proof artifact).
#[derive(Serialize)]
pub struct SbomRef {
    /// Filename under the list root, e.g. `sbom.cdx.json`.
    pub file: String,
    pub spec: String,
    pub components: usize,
    pub generated: String,
    /// SHA-256 of the SBOM bytes — the tamper-evident fingerprint a customer
    /// (or an EU CRA audit) verifies against the served file.
    pub sha256: String,
    /// Present when a signing key is configured: the cryptographic proof of
    /// origin over the SBOM bytes. Absent = hash-pinned only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub signature: Option<SbomSignatureRef>,
}

/// Signature metadata carried in the manifest so any verifier — `openssl`,
/// cosign, or the browser's WebCrypto — can check the SBOM's provenance.
#[derive(Serialize)]
pub struct SbomSignatureRef {
    pub alg: String,
    pub key_id: String,
    /// base64 raw `r‖s` (IEEE-P1363) — verified directly by WebCrypto.
    pub value: String,
    pub format: String,
    /// base64(DER) signature file for `openssl`/cosign-family verifiers.
    pub sig_file: String,
    /// SPKI public-key file served alongside the SBOM.
    pub public_key_file: String,
}

/// Per-package compliance facts pulled from the DB, keyed by package name.
struct PkgMeta {
    spdx: String,
    verdict: String,
    requires_source: bool,
    osv_status: String,
    osv_id: String,
}

struct Record {
    version: String,
    deps: HashSet<String>,
    raw: String,
    /// Integrity fields CRAN's own index publishes for the artifact (the
    /// binary indexes carry MD5sum and, on macOS, SHA256sum; r-wasm's none).
    md5: Option<String>,
    sha256: Option<String>,
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
    // CRAN's Windows index is CRLF; normalise so block/line splitting holds.
    let text = text.replace("\r\n", "\n");
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
            let md5 = fields.get("MD5sum").map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty());
            let sha256 = fields.get("SHA256sum").map(|s| s.trim().to_lowercase()).filter(|s| !s.is_empty());
            out.insert(name.clone(), Record { version, deps, raw: block.to_string(), md5, sha256 });
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

/// Fetch per-package compliance facts (SPDX license, license verdict, OSV
/// status/id, source obligation) for the shipped set, from the vetted view.
fn sbom_meta(pool: &Pool, names: &[String]) -> Result<HashMap<String, PkgMeta>, String> {
    let mut out = HashMap::new();
    if names.is_empty() {
        return Ok(out);
    }
    let conn = pool.get().map_err(|e| e.to_string())?;
    let ph = names.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT Package, COALESCE(license_spdx,''), COALESCE(license_verdict,''), \
         COALESCE(requires_source,false), COALESCE(osv_safety_status,''), COALESCE(osv_id,'') \
         FROM packages_search WHERE Package IN ({ph})"
    );
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let params = duckdb::params_from_iter(names.iter());
    let rows = stmt
        .query_map(params, |r| {
            Ok((
                r.get::<_, String>(0)?,
                PkgMeta {
                    spdx: r.get::<_, String>(1)?,
                    verdict: r.get::<_, String>(2)?,
                    requires_source: r.get::<_, bool>(3)?,
                    osv_status: r.get::<_, String>(4)?,
                    osv_id: r.get::<_, String>(5)?,
                },
            ))
        })
        .map_err(|e| e.to_string())?;
    for r in rows {
        let (k, v) = r.map_err(|e| e.to_string())?;
        out.insert(k, v);
    }
    Ok(out)
}

/// UTC build time as RFC-3339, sourced from the DB (the backend's DuckDB has
/// the clock; the crate lacks a std time bridge we rely on elsewhere). Empty on
/// failure — a missing timestamp must not fail a build.
fn now_iso(pool: &Pool) -> String {
    pool.get()
        .ok()
        .and_then(|c| {
            c.query_row(
                // current_localtimestamp() avoids ICU-only timezone machinery.
                "SELECT strftime(current_localtimestamp(), '%Y-%m-%dT%H:%M:%SZ')",
                [],
                |r| r.get::<_, String>(0),
            )
            .ok()
        })
        .unwrap_or_default()
}

/// A deterministic `urn:uuid` derived from the BOM's own content — no RNG, so a
/// rebuild of the same bytes yields the same serial (reproducible SBOM).
fn deterministic_serial(seed: &str) -> String {
    let h = format!("{:x}", Sha256::digest(seed.as_bytes()));
    format!(
        "urn:uuid:{}-{}-{}-{}-{}",
        &h[0..8], &h[8..12], &h[12..16], &h[16..20], &h[20..32]
    )
}

/// Assemble the CycloneDX 1.5 SBOM for a built list. Each component binds its
/// served WASM artifact by SHA-256 (`hashes`), carries its SPDX license and OSV
/// status, and the top-level `vulnerabilities: []` is the explicit attestation
/// that every member passed the CVE gate — the EU CRA compliance proof.
fn build_sbom(
    list_name: &str,
    rver: &str,
    components: &[Component],
    binaries: &[BinTarget],
    meta: &HashMap<String, PkgMeta>,
    generated: &str,
) -> serde_json::Value {
    let comps: Vec<serde_json::Value> = components
        .iter()
        .map(|c| {
            let m = meta.get(&c.name);
            // Prebuilt binaries of this package, one per mirrored target.
            let bins: Vec<(&str, &BinArtifact)> = binaries
                .iter()
                .filter_map(|t| t.components.iter().find(|b| b.name == c.name).map(|b| (t.target.as_str(), b)))
                .collect();
            let mut licenses = Vec::new();
            if let Some(spdx) = m.map(|m| m.spdx.as_str()).filter(|s| !s.is_empty()) {
                licenses.push(serde_json::json!({ "license": { "id": spdx } }));
            }
            let mut properties = vec![
                serde_json::json!({ "name": "crosv:osv_status", "value": m.map(|m| m.osv_status.clone()).unwrap_or_default() }),
                serde_json::json!({ "name": "crosv:license_verdict", "value": m.map(|m| m.verdict.clone()).unwrap_or_default() }),
            ];
            // One component may bind two artifacts. `hashes` carries both; the
            // per-artifact properties say which hash is which, and the
            // `src_file` is the path a verifier fetches to recompute it.
            let mut hashes = Vec::new();
            if let (Some(sha), Some(bytes)) = (&c.wasm_sha256, c.bytes) {
                hashes.push(serde_json::json!({ "alg": "SHA-256", "content": sha }));
                properties.push(serde_json::json!({ "name": "crosv:wasm_sha256", "value": sha }));
                properties.push(serde_json::json!({ "name": "crosv:artifact_bytes", "value": bytes.to_string() }));
            }
            if let (Some(sha), Some(bytes), Some(ver)) = (&c.src_sha256, c.src_bytes, &c.src_version) {
                hashes.push(serde_json::json!({ "alg": "SHA-256", "content": sha }));
                properties.push(serde_json::json!({ "name": "crosv:src_sha256", "value": sha }));
                properties.push(serde_json::json!({ "name": "crosv:src_bytes", "value": bytes.to_string() }));
                properties.push(serde_json::json!({ "name": "crosv:src_file", "value": format!("src/contrib/{}_{ver}.tar.gz", c.name) }));
                if ver != &c.version {
                    properties.push(serde_json::json!({ "name": "crosv:src_version", "value": ver }));
                }
            }
            for (target, b) in &bins {
                hashes.push(serde_json::json!({ "alg": "SHA-256", "content": b.sha256 }));
                properties.push(serde_json::json!({ "name": format!("crosv:bin_sha256[{target}]"), "value": b.sha256 }));
                properties.push(serde_json::json!({ "name": format!("crosv:bin_file[{target}]"), "value": b.file }));
                if !b.index_verified {
                    properties.push(serde_json::json!({ "name": format!("crosv:bin_index_verified[{target}]"), "value": "false" }));
                }
            }
            if let Some(osv_id) = m.map(|m| m.osv_id.as_str()).filter(|s| !s.is_empty()) {
                properties.push(serde_json::json!({ "name": "crosv:osv_id", "value": osv_id }));
            }
            if m.map(|m| m.requires_source).unwrap_or(false) {
                properties.push(serde_json::json!({ "name": "crosv:requires_source", "value": "true" }));
            }
            serde_json::json!({
                "type": "library",
                "name": c.name,
                "version": c.version,
                "purl": format!("pkg:cran/{}@{}", c.name, c.version),
                "licenses": licenses,
                "hashes": hashes,
                "properties": properties,
            })
        })
        .collect();

    let seed = format!(
        "{list_name}|{generated}|{}",
        components
            .iter()
            .map(|c| format!(
                "{}:{}",
                c.wasm_sha256.as_deref().unwrap_or(""),
                c.src_sha256.as_deref().unwrap_or("")
            ))
            .collect::<Vec<_>>()
            .join(",")
    );

    serde_json::json!({
        "bomFormat": "CycloneDX",
        "specVersion": "1.5",
        "serialNumber": deterministic_serial(&seed),
        "version": 1,
        "metadata": {
            "timestamp": generated,
            "tools": [ {
                "vendor": "CROSV",
                "name": "crosv-curated-repo-builder",
                "version": env!("CARGO_PKG_VERSION")
            } ],
            "component": {
                "type": "application",
                "name": format!("curated-list:{list_name}"),
                "version": rver
            },
            "properties": [ {
                "name": "crosv:attestation",
                "value": "Every component is CVE-clean (OSV-verified) and license-cleared; each SHA-256 binds the exact WASM artifact served from this repository."
            } ]
        },
        "components": comps,
        // Empty by construction: the curated gate admits only components with no
        // known OSV vulnerability. This array IS the compliance assertion.
        "vulnerabilities": []
    })
}

/// Package names in a DESCRIPTION dependency field (`Depends`/`Imports`/…):
/// strip version constraints, drop the `R (>= x)` pseudo-dependency.
fn dep_names(field: &str) -> impl Iterator<Item = String> + '_ {
    field.split(',').filter_map(|part| {
        let d = part.split('(').next().unwrap_or("").trim();
        (!d.is_empty() && d != "R").then(|| d.to_string())
    })
}

/// One CRAN-current package as the ingested CRAN index describes it.
struct SrcRecord {
    version: String,
    /// MD5 CRAN publishes for the tarball — what every download is checked against.
    md5: String,
    deps: HashSet<String>,
    /// The `PACKAGES` DCF block for this package (fields R's
    /// `available.packages()` reads), rebuilt from the index columns.
    dcf: String,
}

/// The DCF fields `available.packages()` consumes, in CRAN's order.
const DCF_FIELDS: [&str; 15] = [
    "Package", "Version", "Priority", "Depends", "Imports", "LinkingTo", "Suggests",
    "Enhances", "License", "License_is_FOSS", "License_restricts_use", "OS_type", "Archs",
    "MD5sum", "NeedsCompilation",
];

/// Load the whole CRAN-current index (~20k rows, a few MB) from
/// `stage_cran_current`: the closure needs everyone's dependencies.
fn cran_index(pool: &Pool) -> Result<HashMap<String, SrcRecord>, String> {
    let conn = pool.get().map_err(|e| e.to_string())?;
    // CAST: read_csv auto-detects types per column; we want text for all.
    let cols = DCF_FIELDS
        .iter()
        .map(|f| format!("CAST(\"{f}\" AS VARCHAR)"))
        .collect::<Vec<_>>()
        .join(", ");
    let mut stmt = conn
        .prepare(&format!("SELECT {cols} FROM stage_cran_current"))
        .map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map([], |r| {
            let mut vals: Vec<Option<String>> = Vec::with_capacity(DCF_FIELDS.len());
            for i in 0..DCF_FIELDS.len() {
                vals.push(r.get::<_, Option<String>>(i)?);
            }
            Ok(vals)
        })
        .map_err(|e| e.to_string())?;

    let mut out = HashMap::new();
    for row in rows {
        let vals = row.map_err(|e| e.to_string())?;
        // Present = non-empty and not the literal NA the CSV export writes.
        // read_csv auto-types the yes/no columns as BOOLEAN, so the CAST
        // yields "true"/"false" — R's index wants CRAN's literal "yes"/"no".
        let present = |i: usize| -> Option<String> {
            vals[i]
                .as_deref()
                .map(|v| v.split_whitespace().collect::<Vec<_>>().join(" "))
                .filter(|v| !v.is_empty() && v != "NA")
                .map(|v| match (DCF_FIELDS[i], v.as_str()) {
                    ("NeedsCompilation" | "License_is_FOSS" | "License_restricts_use", "true") => "yes".to_string(),
                    ("NeedsCompilation" | "License_is_FOSS" | "License_restricts_use", "false") => "no".to_string(),
                    _ => v,
                })
        };
        let (Some(name), Some(version), Some(md5)) = (present(0), present(1), present(13)) else {
            continue;
        };
        let mut deps = HashSet::new();
        for i in [3usize, 4, 5] {
            if let Some(v) = present(i) {
                deps.extend(dep_names(&v));
            }
        }
        let dcf = DCF_FIELDS
            .iter()
            .enumerate()
            .filter_map(|(i, f)| present(i).map(|v| format!("{f}: {v}")))
            .collect::<Vec<_>>()
            .join("\n");
        out.insert(name, SrcRecord { version, md5: md5.to_lowercase(), deps, dcf });
    }
    Ok(out)
}

/// Dependency closure over the CRAN index (Depends+Imports+LinkingTo), skipping
/// what ships with R.
fn src_closure(roots: &[String], index: &HashMap<String, SrcRecord>) -> HashSet<String> {
    let bundled: HashSet<&str> = BUNDLED.iter().copied().collect();
    let mut seen = HashSet::new();
    let mut stack: Vec<String> = roots.to_vec();
    while let Some(p) = stack.pop() {
        if seen.contains(&p) || bundled.contains(p.as_str()) {
            continue;
        }
        seen.insert(p.clone());
        if let Some(rec) = index.get(&p) {
            stack.extend(rec.deps.iter().filter(|d| !seen.contains(*d)).cloned());
        }
    }
    seen
}

fn md5_hex(bytes: &[u8]) -> String {
    format!("{:x}", md5::Md5::digest(bytes))
}

/// A source tarball as shipped: verified bytes + hashes.
struct SrcArtifact {
    version: String,
    sha256: String,
    bytes: usize,
}

/// Get `<pkg>_<ver>.tar.gz` with the MD5 CRAN's index promises. Reuses the
/// file already on disk when it still matches (hourly rebuilds don't re-pull
/// CRAN), else fetches `src/contrib/` and falls back to `src/contrib/Archive/`
/// (the index can lag a CRAN release by an hour). Never returns unverified bytes.
fn fetch_source(pkg: &str, rec: &SrcRecord, dest: &Path) -> Result<Vec<u8>, String> {
    if let Ok(existing) = std::fs::read(dest) {
        if md5_hex(&existing) == rec.md5 {
            return Ok(existing);
        }
    }
    let file = format!("{pkg}_{}.tar.gz", rec.version);
    let cran = cran_upstream();
    let urls = [
        format!("{cran}/src/contrib/{file}"),
        format!("{cran}/src/contrib/Archive/{pkg}/{file}"),
    ];
    let mut last_err = String::new();
    for url in &urls {
        match http_get(url) {
            Ok(blob) => {
                let got = md5_hex(&blob);
                if got == rec.md5 {
                    return Ok(blob);
                }
                last_err = format!("MD5 mismatch for {url}: CRAN index {} vs downloaded {got}", rec.md5);
            }
            Err(e) => last_err = e,
        }
    }
    Err(last_err)
}

/// Materialize the source half of a list under `<out_dir>/src/contrib/`.
/// Returns the shipped artifacts (by name) and the blocked map with reasons.
fn build_source_repo(
    pool: &Pool,
    roots: &[String],
    allow: &HashSet<String>,
    out_dir: &Path,
) -> Result<(HashMap<String, SrcArtifact>, HashMap<String, String>), String> {
    let index = cran_index(pool)?;
    let want = src_closure(roots, &index);

    let mut blocked: HashMap<String, String> = HashMap::new();
    for pkg in &want {
        if !index.contains_key(pkg) {
            blocked.insert(pkg.clone(), "not on CRAN (current index)".into());
        } else if !allow.contains(pkg) {
            blocked.insert(pkg.clone(), "NOT mirror-eligible (CVE/license gate)".into());
        }
    }
    let mut ship: Vec<String> = want.iter().filter(|p| !blocked.contains_key(*p)).cloned().collect();
    ship.sort();

    let contrib_dir = out_dir.join("src").join("contrib");
    std::fs::create_dir_all(&contrib_dir).map_err(|e| e.to_string())?;

    let mut artifacts = HashMap::new();
    let mut kept_dcf = Vec::new();
    for pkg in &ship {
        let rec = &index[pkg];
        let file = format!("{pkg}_{}.tar.gz", rec.version);
        let dest = contrib_dir.join(&file);
        match fetch_source(pkg, rec, &dest) {
            Ok(blob) => {
                std::fs::write(&dest, &blob).map_err(|e| e.to_string())?;
                kept_dcf.push(rec.dcf.clone());
                artifacts.insert(
                    pkg.clone(),
                    SrcArtifact {
                        version: rec.version.clone(),
                        sha256: format!("{:x}", Sha256::digest(&blob)),
                        bytes: blob.len(),
                    },
                );
            }
            // A package we can't verify is a package we don't ship — and we
            // say why, rather than failing the whole build.
            Err(e) => {
                let _ = std::fs::remove_file(&dest);
                blocked.insert(pkg.clone(), format!("source fetch failed: {e}"));
            }
        }
    }

    // Filtered PACKAGES + PACKAGES.gz — R reads .gz first, falls back to plain.
    let packages = format!("{}\n", kept_dcf.join("\n\n"));
    std::fs::write(contrib_dir.join("PACKAGES"), &packages).map_err(|e| e.to_string())?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(packages.as_bytes()).map_err(|e| e.to_string())?;
    std::fs::write(contrib_dir.join("PACKAGES.gz"), gz.finish().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;

    // Drop tarballs of packages no longer in the list (a removed root, or a
    // package that just lost eligibility) so the served repo equals the bill.
    let keep: HashSet<String> = artifacts
        .iter()
        .map(|(p, a)| format!("{p}_{}.tar.gz", a.version))
        .collect();
    if let Ok(entries) = std::fs::read_dir(&contrib_dir) {
        for e in entries.flatten() {
            let n = e.file_name().to_string_lossy().to_string();
            if n.ends_with(".tar.gz") && !keep.contains(&n) {
                let _ = std::fs::remove_file(e.path());
            }
        }
    }

    Ok((artifacts, blocked))
}

/// One prebuilt binary as shipped for a target.
#[derive(Serialize, Clone)]
pub struct BinArtifact {
    pub name: String,
    pub version: String,
    pub file: String,
    pub sha256: String,
    pub bytes: usize,
    /// True when the download matched the checksum CRAN's index publishes
    /// (MD5sum, or SHA256sum where present). False = CRAN's index for this
    /// target carries no checksum; we still pin OUR sha256 in the SBOM.
    pub index_verified: bool,
}

/// The mirrored slice of one CRAN binary index, e.g. `windows/contrib/4.6`.
#[derive(Serialize)]
pub struct BinTarget {
    /// Path under `bin/`, exactly as R's `contrib.url(type = "binary")` builds it.
    pub target: String,
    pub shipped: usize,
    /// Packages in the list that CRAN has no binary for on this target — R
    /// falls back to the source tarball for these (needs a compiler).
    pub missing: Vec<String>,
    pub components: Vec<BinArtifact>,
}

/// The CRAN binary targets to mirror. Override with `CROSV_BIN_TARGETS`
/// (comma-separated `bin/`-relative paths). Defaults cover current R on the
/// three desktop platforms; CRAN's macOS directory name tracks the build OS
/// (big-sur-* for R ≤ 4.5, sonoma-arm64 for R 4.6 on Apple silicon).
fn bin_targets() -> Vec<String> {
    std::env::var("CROSV_BIN_TARGETS")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .map(|s| s.split(',').map(|t| t.trim().trim_matches('/').to_string()).filter(|t| !t.is_empty()).collect())
        .unwrap_or_else(|| {
            [
                "windows/contrib/4.5",
                "windows/contrib/4.6",
                "macosx/big-sur-arm64/contrib/4.5",
                "macosx/sonoma-arm64/contrib/4.6",
                "macosx/big-sur-x86_64/contrib/4.5",
                "macosx/big-sur-x86_64/contrib/4.6",
            ]
            .iter()
            .map(|s| s.to_string())
            .collect()
        })
}

/// Mirror the curated set from every CRAN binary target under
/// `<out_dir>/bin/<target>/`. A target whose index can't be fetched is
/// reported (shipped 0, everything missing) rather than failing the build;
/// per package, a checksum mismatch means "missing", never "shipped".
fn build_binary_repos(ship: &[String], out_dir: &Path) -> Vec<BinTarget> {
    let cran = cran_upstream();
    let mut out = Vec::new();
    for target in bin_targets() {
        let ext = if target.starts_with("windows") { "zip" } else { "tgz" };
        let dir = out_dir.join("bin").join(&target);
        let index = match http_get(&format!("{cran}/bin/{target}/PACKAGES")) {
            Ok(b) => parse_packages(&String::from_utf8_lossy(&b)),
            Err(e) => {
                tracing::warn!("bin target {target}: index unavailable: {e}");
                out.push(BinTarget { target, shipped: 0, missing: ship.to_vec(), components: vec![] });
                continue;
            }
        };
        if std::fs::create_dir_all(&dir).is_err() {
            out.push(BinTarget { target, shipped: 0, missing: ship.to_vec(), components: vec![] });
            continue;
        }

        let mut components = Vec::new();
        let mut missing = Vec::new();
        let mut kept_raw = Vec::new();
        for pkg in ship {
            let Some(rec) = index.get(pkg) else {
                missing.push(pkg.clone());
                continue;
            };
            let file = format!("{pkg}_{}.{ext}", rec.version);
            let dest = dir.join(&file);
            // Reuse a matching file on disk (hourly rebuilds), else fetch.
            let checks_out = |blob: &[u8]| -> Option<bool> {
                if let Some(sha) = &rec.sha256 {
                    return Some(&format!("{:x}", Sha256::digest(blob)) == sha);
                }
                rec.md5.as_ref().map(|m| &md5_hex(blob) == m)
            };
            let blob = match std::fs::read(&dest) {
                Ok(b) if checks_out(&b) != Some(false) => Some(b),
                _ => match http_get(&format!("{cran}/bin/{target}/{file}")) {
                    Ok(b) => Some(b),
                    Err(e) => {
                        tracing::warn!("bin {target}/{file}: {e}");
                        None
                    }
                },
            };
            let Some(blob) = blob else {
                missing.push(pkg.clone());
                continue;
            };
            let verified = checks_out(&blob);
            if verified == Some(false) {
                tracing::warn!("bin {target}/{file}: checksum mismatch vs CRAN index — not shipped");
                let _ = std::fs::remove_file(&dest);
                missing.push(pkg.clone());
                continue;
            }
            if std::fs::write(&dest, &blob).is_err() {
                missing.push(pkg.clone());
                continue;
            }
            kept_raw.push(rec.raw.clone());
            components.push(BinArtifact {
                name: pkg.clone(),
                version: rec.version.clone(),
                file: format!("bin/{target}/{file}"),
                sha256: format!("{:x}", Sha256::digest(&blob)),
                bytes: blob.len(),
                index_verified: verified.unwrap_or(false),
            });
        }

        // Filtered PACKAGES(.gz) for this target — the raw CRAN blocks, so
        // Built/Archs fields R uses for binary selection are preserved.
        let packages = format!("{}\n", kept_raw.join("\n\n"));
        let _ = std::fs::write(dir.join("PACKAGES"), &packages);
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        if gz.write_all(packages.as_bytes()).is_ok() {
            if let Ok(bytes) = gz.finish() {
                let _ = std::fs::write(dir.join("PACKAGES.gz"), bytes);
            }
        }
        // Prune binaries that left the list.
        let keep: HashSet<String> = components.iter().map(|c| c.file.rsplit('/').next().unwrap_or("").to_string()).collect();
        if let Ok(entries) = std::fs::read_dir(&dir) {
            for e in entries.flatten() {
                let n = e.file_name().to_string_lossy().to_string();
                if (n.ends_with(".zip") || n.ends_with(".tgz")) && !keep.contains(&n) {
                    let _ = std::fs::remove_file(e.path());
                }
            }
        }
        missing.sort();
        out.push(BinTarget { target, shipped: components.len(), missing, components });
    }
    out
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
            wasm_sha256: Some(sha),
            bytes: Some(blob.len()),
            src_version: None,
            src_sha256: None,
            src_bytes: None,
        });
    }

    // Filtered PACKAGES + PACKAGES.gz (our vetted subset only).
    let packages = format!("{}\n", kept_raw.join("\n\n"));
    std::fs::write(contrib_dir.join("PACKAGES"), &packages).map_err(|e| e.to_string())?;
    let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    gz.write_all(packages.as_bytes()).map_err(|e| e.to_string())?;
    std::fs::write(contrib_dir.join("PACKAGES.gz"), gz.finish().map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;

    // Phase 2a: the same list as a CRAN source repo (desktop R / RStudio /
    // renv). Its closure comes from CRAN's own dependency data, so it can ship
    // packages r-wasm never built — and vice versa; the two sets are merged
    // per package into one component list, one SBOM.
    let (src_artifacts, src_blocked) = build_source_repo(pool, &roots, &allow, &out_dir)?;
    let mut src_names: Vec<&String> = src_artifacts.keys().collect();
    src_names.sort();
    for pkg in src_names {
        let a = &src_artifacts[pkg];
        if let Some(c) = components.iter_mut().find(|c| &c.name == pkg) {
            c.src_version = Some(a.version.clone());
            c.src_sha256 = Some(a.sha256.clone());
            c.src_bytes = Some(a.bytes);
        } else {
            components.push(Component {
                name: pkg.clone(),
                version: a.version.clone(),
                wasm_sha256: None,
                bytes: None,
                src_version: Some(a.version.clone()),
                src_sha256: Some(a.sha256.clone()),
                src_bytes: Some(a.bytes),
            });
        }
    }
    components.sort_by(|a, b| a.name.cmp(&b.name));
    let src_shipped = src_artifacts.len();

    // CRA compliance proof: emit a CycloneDX SBOM for the vetted set, pinned by
    // its own SHA-256 so the manifest attests to an exact bill of materials.
    let generated = now_iso(pool);
    let all_names: Vec<String> = components.iter().map(|c| c.name.clone()).collect();

    // Phase 2b: prebuilt CRAN binaries for desktop R on macOS/Windows, for
    // every package that made it into the list (either artifact above).
    let binaries = build_binary_repos(&all_names, &out_dir);

    let meta = sbom_meta(pool, &all_names)?;
    let sbom = build_sbom(name, rver, &components, &binaries, &meta, &generated);
    let sbom_bytes = serde_json::to_vec_pretty(&sbom).map_err(|e| e.to_string())?;
    let sbom_sha = format!("{:x}", Sha256::digest(&sbom_bytes));
    std::fs::write(out_dir.join("sbom.cdx.json"), &sbom_bytes).map_err(|e| e.to_string())?;

    // Sign the bill (key-based ECDSA P-256), if a signing key is configured.
    // Writes the DER signature + the SPKI public key next to the SBOM; the raw
    // signature rides in the manifest for in-browser WebCrypto verification.
    let signature = crate::sign::load_signer().map(|signer| {
        let sig = signer.sign(&sbom_bytes);
        let _ = std::fs::write(out_dir.join("sbom.cdx.json.sig"), sig.der_b64.as_bytes());
        let _ = std::fs::write(out_dir.join("cosign.pub"), signer.public_pem.as_bytes());
        SbomSignatureRef {
            alg: sig.alg,
            key_id: sig.key_id,
            value: sig.raw_b64,
            format: "IEEE-P1363".to_string(),
            sig_file: "sbom.cdx.json.sig".to_string(),
            public_key_file: "cosign.pub".to_string(),
        }
    });

    let report = BuildReport {
        name: name.to_string(),
        r_version: rver.to_string(),
        roots,
        shipped: ship.len(),
        blocked,
        src_shipped,
        src_blocked,
        binaries,
        out_dir: out_dir.to_string_lossy().to_string(),
        sbom: Some(SbomRef {
            file: "sbom.cdx.json".to_string(),
            spec: "CycloneDX 1.5".to_string(),
            components: components.len(),
            generated,
            sha256: sbom_sha,
            signature,
        }),
        components,
    };
    std::fs::write(
        out_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&report).map_err(|e| e.to_string())?,
    )
    .map_err(|e| e.to_string())?;

    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dep_names_strips_constraints_and_r() {
        let v: Vec<String> =
            dep_names("R (>= 4.1), cli, gtable (>= 0.3.6), lifecycle (> 1.0.1)").collect();
        assert_eq!(v, ["cli", "gtable", "lifecycle"]);
    }

    #[test]
    fn src_closure_follows_index_and_skips_bundled() {
        let mut index = HashMap::new();
        let rec = |deps: &[&str]| SrcRecord {
            version: "1".into(),
            md5: String::new(),
            deps: deps.iter().map(|s| s.to_string()).collect(),
            dcf: String::new(),
        };
        index.insert("a".to_string(), rec(&["b", "stats"]));
        index.insert("b".to_string(), rec(&["c"]));
        index.insert("c".to_string(), rec(&[]));
        let got = src_closure(&["a".to_string()], &index);
        let mut v: Vec<&String> = got.iter().collect();
        v.sort();
        assert_eq!(v, ["a", "b", "c"]);
    }

    #[test]
    fn md5_matches_cran_convention() {
        // Lowercase hex, 32 chars — what packages.csv's MD5sum column carries.
        assert_eq!(md5_hex(b"abc"), "900150983cd24fb0d6963f7d28e17f72");
    }
}
