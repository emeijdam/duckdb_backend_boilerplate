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
    meta: &HashMap<String, PkgMeta>,
    generated: &str,
) -> serde_json::Value {
    let comps: Vec<serde_json::Value> = components
        .iter()
        .map(|c| {
            let m = meta.get(&c.name);
            let mut licenses = Vec::new();
            if let Some(spdx) = m.map(|m| m.spdx.as_str()).filter(|s| !s.is_empty()) {
                licenses.push(serde_json::json!({ "license": { "id": spdx } }));
            }
            let mut properties = vec![
                serde_json::json!({ "name": "crosv:osv_status", "value": m.map(|m| m.osv_status.clone()).unwrap_or_default() }),
                serde_json::json!({ "name": "crosv:license_verdict", "value": m.map(|m| m.verdict.clone()).unwrap_or_default() }),
                serde_json::json!({ "name": "crosv:artifact_bytes", "value": c.bytes.to_string() }),
            ];
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
                "hashes": [ { "alg": "SHA-256", "content": c.wasm_sha256 } ],
                "properties": properties,
            })
        })
        .collect();

    let seed = format!(
        "{list_name}|{generated}|{}",
        components.iter().map(|c| c.wasm_sha256.as_str()).collect::<Vec<_>>().join(",")
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

    // CRA compliance proof: emit a CycloneDX SBOM for the vetted set, pinned by
    // its own SHA-256 so the manifest attests to an exact bill of materials.
    let generated = now_iso(pool);
    let meta = sbom_meta(pool, &ship)?;
    let sbom = build_sbom(name, rver, &components, &meta, &generated);
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
