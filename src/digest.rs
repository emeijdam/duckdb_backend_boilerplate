//! Vulnerability-digest job.
//!
//! Periodically (and on demand via `POST /digest/run`) it walks every active
//! subscriber, checks the curated lists they watch against the CURRENT vetted
//! view, and emails a digest: what's still clean, and — the alert — any package
//! that has since fallen out of eligibility (a new CVE or a license change).
//! Every member was mirror-eligible when its list was created, so an
//! ineligible one today is a regression worth telling the owner about.
//!
//! Delivery uses SMTP (lettre) when configured; otherwise the rendered email is
//! logged, so the pipeline is verifiable without a mail server. `last_sent`
//! gates the cadence (daily vs weekly); `force` (the manual trigger) ignores it.

use std::sync::Arc;

use lettre::{
    transport::smtp::authentication::Credentials, AsyncSmtpTransport, AsyncTransport, Message,
    Tokio1Executor,
};
use serde_json::json;

use crate::state::AppState;

struct Alert {
    package: String,
    reason: String,
}

struct ListSummary {
    name: String,
    total: usize,
    alerts: Vec<Alert>,
}

/// One advisory, flattened across lists — the unit we snapshot and diff on.
#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct StoredAlert {
    /// list name
    l: String,
    /// package
    p: String,
    /// reason
    r: String,
}

impl StoredAlert {
    /// Identity for set membership: a package alerting within a list. A change
    /// of reason for the same (list, package) is treated as the same event.
    fn key(&self) -> String {
        format!("{}::{}", self.l, self.p)
    }
}

struct SubscriberDigest {
    user: String,
    email: String,
    frequency: String,
    lists: Vec<ListSummary>,
    /// The advisory set this subscriber was last emailed (from last_alerts).
    prev: Vec<StoredAlert>,
    /// True when never notified before (last_sent IS NULL) — send a baseline.
    first_time: bool,
    /// True when the cadence window (daily/weekly) has elapsed since last_sent.
    due: bool,
}

impl SubscriberDigest {
    fn alert_count(&self) -> usize {
        self.lists.iter().map(|l| l.alerts.len()).sum()
    }

    /// Current advisories flattened across all watched lists.
    fn current_flat(&self) -> Vec<StoredAlert> {
        self.lists
            .iter()
            .flat_map(|l| {
                l.alerts.iter().map(move |a| StoredAlert {
                    l: l.name.clone(),
                    p: a.package.clone(),
                    r: a.reason.clone(),
                })
            })
            .collect()
    }
}

/// Diff two advisory sets by identity: what's newly present, what cleared.
fn diff(prev: &[StoredAlert], current: &[StoredAlert]) -> (Vec<StoredAlert>, Vec<StoredAlert>) {
    use std::collections::HashSet;
    let prev_keys: HashSet<String> = prev.iter().map(|a| a.key()).collect();
    let cur_keys: HashSet<String> = current.iter().map(|a| a.key()).collect();
    let new_alerts = current.iter().filter(|a| !prev_keys.contains(&a.key())).cloned().collect();
    let resolved = prev.iter().filter(|a| !cur_keys.contains(&a.key())).cloned().collect();
    (new_alerts, resolved)
}

/// UTC timestamp from the DB (the crate has no std-time bridge we lean on).
fn now_iso(conn: &duckdb::Connection) -> String {
    // current_localtimestamp() is a plain TIMESTAMP (no tz) — avoids the ICU-only
    // timezone machinery that isn't reliably loaded in the bundled DuckDB.
    conn.query_row(
        "SELECT strftime(current_localtimestamp(), '%Y-%m-%dT%H:%M:%SZ')",
        [],
        |r| r.get::<_, String>(0),
    )
    .unwrap_or_default()
}

/// Read every subscriber that is due (or all active ones when `force`), and
/// compute their per-list summaries in one blocking DB pass.
fn gather(conn: &duckdb::Connection, force: bool) -> Result<Vec<SubscriberDigest>, String> {
    // Due = never sent, or older than the cadence for its frequency.
    let mut stmt = conn
        .prepare(
            "SELECT user_key, email, frequency, lists, \
             (last_sent IS NULL OR last_sent < current_localtimestamp() - \
                to_days(CASE frequency WHEN 'daily' THEN 1 ELSE 7 END)) AS due, \
             (last_sent IS NULL) AS first_time, \
             COALESCE(last_alerts, '[]') AS last_alerts \
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
                r.get::<_, bool>(4)?,
                r.get::<_, bool>(5)?,
                r.get::<_, String>(6)?,
            ))
        })
        .map_err(|e| e.to_string())?;

    // All list names, for subscribers who watch "all" (empty selection).
    let all_lists: Vec<String> = {
        let mut s = conn
            .prepare("SELECT name FROM curated_lists ORDER BY name")
            .map_err(|e| e.to_string())?;
        let it = s
            .query_map([], |r| r.get::<_, String>(0))
            .map_err(|e| e.to_string())?;
        it.filter_map(|r| r.ok()).collect()
    };

    let mut out = Vec::new();
    for row in rows {
        let (user, email, frequency, lists_json, due, first_time, last_alerts) =
            row.map_err(|e| e.to_string())?;
        // The event-diff decision happens in run_digest; gather everyone active
        // (except when force is off AND not due AND already-notified — those can
        // never send this cycle, so skip the DB work). first_time always gathers.
        if !force && !due && !first_time {
            continue;
        }
        let mut names: Vec<String> = serde_json::from_str(&lists_json).unwrap_or_default();
        if names.is_empty() {
            names = all_lists.clone();
        }
        let mut summaries = Vec::new();
        for name in names {
            summaries.push(summarize_list(conn, &name)?);
        }
        let prev: Vec<StoredAlert> = serde_json::from_str(&last_alerts).unwrap_or_default();
        out.push(SubscriberDigest {
            user,
            email,
            frequency,
            lists: summaries,
            prev,
            first_time,
            due,
        });
    }
    Ok(out)
}

/// One curated list's roots, and any that are no longer mirror-eligible.
fn summarize_list(conn: &duckdb::Connection, name: &str) -> Result<ListSummary, String> {
    let roots_json: Option<String> = conn
        .query_row(
            "SELECT packages FROM curated_lists WHERE name = ?",
            duckdb::params![name],
            |r| r.get(0),
        )
        .ok();
    let roots: Vec<String> = roots_json
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    if roots.is_empty() {
        return Ok(ListSummary { name: name.to_string(), total: 0, alerts: vec![] });
    }

    let ph = roots.iter().map(|_| "?").collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT Package, COALESCE(osv_safety_status,''), COALESCE(osv_id,''), \
         COALESCE(license_verdict,'') FROM packages_search \
         WHERE mirror_eligible = false AND Package IN ({ph})"
    );
    let params: Vec<Box<dyn duckdb::ToSql + Send>> = roots
        .iter()
        .map(|p| Box::new(p.clone()) as Box<dyn duckdb::ToSql + Send>)
        .collect();
    let refs: Vec<&dyn duckdb::ToSql> = params.iter().map(|p| &**p as &dyn duckdb::ToSql).collect();
    let mut stmt = conn.prepare(&sql).map_err(|e| e.to_string())?;
    let rows = stmt
        .query_map(&refs[..], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
            ))
        })
        .map_err(|e| e.to_string())?;

    let mut alerts = Vec::new();
    for row in rows {
        let (pkg, osv_status, osv_id, verdict) = row.map_err(|e| e.to_string())?;
        let reason = if !osv_id.is_empty() || (!osv_status.is_empty() && osv_status != "SAFE") {
            let id = if osv_id.is_empty() { osv_status } else { osv_id };
            format!("OSV advisory {id}")
        } else if !verdict.is_empty() && verdict != "allow" {
            format!("license now {verdict}")
        } else {
            "no longer mirror-eligible".to_string()
        };
        alerts.push(Alert { package: pkg, reason });
    }
    Ok(ListSummary { name: name.to_string(), total: roots.len(), alerts })
}

fn plural(n: usize) -> &'static str {
    if n == 1 { "y" } else { "ies" }
}

/// Render the plain-text digest for one subscriber. `baseline` is the first-ever
/// digest (establishes the snapshot); otherwise the email leads with the diff.
fn render(
    d: &SubscriberDigest,
    new_alerts: &[StoredAlert],
    resolved: &[StoredAlert],
    generated: &str,
    baseline: bool,
) -> (String, String) {
    let n = d.alert_count();
    let subject = if baseline {
        if n == 0 {
            "CROSV digest: baseline — all clear".to_string()
        } else {
            format!("CROSV digest: baseline — {n} advisor{}", plural(n))
        }
    } else if !new_alerts.is_empty() {
        format!("CROSV alert: {} new advisor{}", new_alerts.len(), plural(new_alerts.len()))
    } else if !resolved.is_empty() {
        format!("CROSV update: {} advisor{} resolved", resolved.len(), plural(resolved.len()))
    } else {
        "CROSV digest".to_string()
    };

    let mut body = String::new();
    body.push_str(&format!("CROSV vulnerability digest — {} cadence\n", d.frequency));
    body.push_str(&format!("As of {generated}\n\n"));

    if baseline {
        body.push_str("Baseline for your watched lists — you'll only hear from us again when this changes.\n\n");
    } else {
        body.push_str("What changed since your last digest:\n");
        if new_alerts.is_empty() && resolved.is_empty() {
            body.push_str("  (no change)\n");
        }
        for a in new_alerts {
            body.push_str(&format!("  \u{26A0} NEW  {} in {} — {}\n", a.p, a.l, a.r));
        }
        for a in resolved {
            body.push_str(&format!("  \u{2713} RESOLVED  {} in {} — was {}\n", a.p, a.l, a.r));
        }
        body.push('\n');
    }

    body.push_str("Current state:\n");
    for l in &d.lists {
        if l.alerts.is_empty() {
            body.push_str(&format!("  \u{2713} {} — {} packages — all clear\n", l.name, l.total));
        } else {
            body.push_str(&format!(
                "  \u{26A0} {} — {} packages — {} advisor{}:\n",
                l.name,
                l.total,
                l.alerts.len(),
                plural(l.alerts.len())
            ));
            for a in &l.alerts {
                body.push_str(&format!("      - {} : {}\n", a.package, a.reason));
            }
        }
    }
    body.push('\n');
    body.push_str(if n == 0 {
        "No action needed — every package in your watched lists is CVE-clean and license-clear.\n"
    } else {
        "Action: the flagged packages fell out of eligibility. Rebuild affected lists or pin known-good versions.\n"
    });
    body.push_str("\n— CROSV R-Governance\n");
    (subject, body)
}

fn build_mailer(state: &AppState) -> Option<AsyncSmtpTransport<Tokio1Executor>> {
    let e = &state.settings.email;
    if e.smtp_host.trim().is_empty() {
        return None;
    }
    // Plaintext for a dev relay (MailHog); STARTTLS on 587 for real providers.
    let mut b = if e.smtp_insecure {
        AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&e.smtp_host)
    } else {
        AsyncSmtpTransport::<Tokio1Executor>::starttls_relay(&e.smtp_host).ok()?
    }
    .port(e.smtp_port);
    if !e.smtp_username.is_empty() {
        b = b.credentials(Credentials::new(e.smtp_username.clone(), e.smtp_password.clone()));
    }
    Some(b.build())
}

/// Run one digest cycle. `force` ignores the per-subscriber cadence (used by the
/// manual trigger). Returns a JSON report of who was processed and what would be
/// sent — the demo/verification surface.
pub async fn run_digest(state: &Arc<AppState>, force: bool) -> serde_json::Value {
    let pool = state.pool.clone();
    let gathered = tokio::task::spawn_blocking(move || -> Result<(Vec<SubscriberDigest>, String), String> {
        let conn = pool.get().map_err(|e| e.to_string())?;
        let generated = now_iso(&conn);
        Ok((gather(&conn, force)?, generated))
    })
    .await;

    let (subs, generated) = match gathered {
        Ok(Ok(v)) => v,
        Ok(Err(e)) => return json!({ "error": format!("digest gather failed: {e}") }),
        Err(_) => return json!({ "error": "digest gather task panicked" }),
    };

    let mailer = build_mailer(state);
    let from = state.settings.email.from.clone();
    let mut report = Vec::new();
    // (user_key, new_snapshot_json) for those we email — persisted after.
    let mut to_persist: Vec<(String, String)> = Vec::new();

    for d in &subs {
        let current = d.current_flat();
        let (new_alerts, resolved) = diff(&d.prev, &current);
        let changed = !new_alerts.is_empty() || !resolved.is_empty();

        // Event-diff decision:
        //  • first_time → send a baseline (establishes the snapshot),
        //  • else send only when the advisory set CHANGED and the cadence
        //    window has elapsed (rate-limit),
        //  • force → always send.
        //  • otherwise skip — no email when nothing changed.
        let (send, reason) = if force {
            (true, "forced")
        } else if d.first_time {
            (true, "baseline")
        } else if changed && d.due {
            (true, "changed")
        } else if changed {
            (false, "changed-but-not-due")
        } else {
            (false, "unchanged")
        };

        let mut delivered = false;
        let mut preview: Option<String> = None;

        if send {
            let baseline = d.first_time; // first-ever digest establishes the snapshot
            let (subject, body) = render(d, &new_alerts, &resolved, &generated, baseline);
            preview = Some(body.clone());
            if let Some(m) = &mailer {
                match (from.parse(), d.email.parse()) {
                    (Ok(f), Ok(to)) => {
                        if let Ok(msg) = Message::builder()
                            .from(f)
                            .to(to)
                            .subject(subject.clone())
                            .body(body.clone())
                        {
                            match m.send(msg).await {
                                Ok(_) => delivered = true,
                                Err(e) => tracing::warn!("digest send to {} failed: {e}", d.email),
                            }
                        }
                    }
                    _ => tracing::warn!("digest: bad from/to address for {}", d.email),
                }
            } else {
                tracing::info!("digest (unsent, no SMTP) → {} | {}\n{}", d.email, subject, body);
            }
            // Snapshot the current advisory set as the new baseline to diff from.
            if let Ok(snap) = serde_json::to_string(&current) {
                to_persist.push((d.user.clone(), snap));
            }
        }

        report.push(json!({
            "user": d.user,
            "email": d.email,
            "frequency": d.frequency,
            "sent": send,
            "reason": reason,
            "new": new_alerts.iter().map(|a| format!("{} ({})", a.p, a.l)).collect::<Vec<_>>(),
            "resolved": resolved.iter().map(|a| format!("{} ({})", a.p, a.l)).collect::<Vec<_>>(),
            "alerts": d.alert_count(),
            "delivered": delivered,
            "preview": preview,
        }));
    }

    // Persist snapshot + last_sent for everyone we emailed.
    if !to_persist.is_empty() {
        let pool = state.pool.clone();
        let updates = to_persist.clone();
        let _ = tokio::task::spawn_blocking(move || {
            if let Ok(conn) = pool.get() {
                for (u, snap) in updates {
                    let _ = conn.execute(
                        "UPDATE subscriptions SET last_alerts = ?, \
                         last_sent = current_localtimestamp() WHERE user_key = ?",
                        duckdb::params![snap, u],
                    );
                }
            }
        })
        .await;
    }

    let sent = report.iter().filter(|r| r["sent"].as_bool().unwrap_or(false)).count();
    json!({
        "ran_at": generated,
        "forced": force,
        "smtp": mailer.is_some(),
        "evaluated": report.len(),
        "sent": sent,
        "digests": report,
    })
}

/// POST /digest/run — force a digest cycle now (bearer-gated). The report
/// includes each rendered email, so it doubles as a preview endpoint.
#[derive(serde::Deserialize)]
pub struct RunQuery {
    /// Default true: the manual trigger forces regardless of cadence. Pass
    /// `?force=false` to exercise the real due-check (skips subscribers not yet due).
    pub force: Option<bool>,
}

pub async fn run_digest_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    axum::extract::Query(q): axum::extract::Query<RunQuery>,
    auth: Option<axum_extra::TypedHeader<axum_extra::headers::Authorization<axum_extra::headers::authorization::Bearer>>>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let ok = auth
        .map(|axum_extra::TypedHeader(a)| a.token() == state.settings.server.api_token)
        .unwrap_or(false);
    if !ok {
        return (axum::http::StatusCode::UNAUTHORIZED, "Provide a valid bearer token").into_response();
    }
    axum::Json(run_digest(&state, q.force.unwrap_or(true)).await).into_response()
}

/// Background loop: wake every `digest_interval_secs` and run a (non-forced)
/// cycle. Spawned once at startup; soft-fails per tick.
pub fn spawn_digest_loop(state: Arc<AppState>) {
    let secs = state.settings.email.digest_interval_secs.max(60);
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(secs));
        // Skip the immediate first tick so startup isn't a mail burst.
        tick.tick().await;
        loop {
            tick.tick().await;
            let report = run_digest(&state, false).await;
            let n = report.get("processed").and_then(|v| v.as_u64()).unwrap_or(0);
            if n > 0 {
                tracing::info!("digest loop: processed {n} subscriber(s)");
            }
        }
    });
}
