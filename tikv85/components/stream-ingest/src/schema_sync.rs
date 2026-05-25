// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! DDL schema synchronization for PCR.
//!
//! # How PCR handles DDL today (no extra code changes needed)
//!
//! TiDB DDL metadata lives in system tables as regular KV writes:
//!   - `tidb_ddl_job` — DDL job queue and status
//!   - `mysql.tidb`   — TableInfo (CREATE TABLE, ALTER TABLE)
//!   - `mysql.columns` — Column definitions
//!   - `mysql.indexes` — Index definitions (ADD/DROP INDEX)
//!
//! These are all written through standard Percolator transactions and
//! captured by CDC as `CmdType::Put` commands. PCR replicates them in
//! real-time alongside user data — **no additional code changes needed**.
//!
//! # When DDL becomes visible on the target cluster
//!
//! Phase 1 — Active replication (pre-cutover):
//!   DDL KV data arrives in real-time on target TiKV. However, target
//!   TiDB instances cache table schemas and only reload when PD's global
//!   schema version changes. Since PCR doesn't replicate PD state,
//!   DDL changes are NOT visible to queries until Phase 2.
//!
//! Phase 2 — Post-cutover (automatic):
//!   After cutover, restart target TiDB instances. TiDB reads system tables
//!   from KV on startup — which now contain all replicated DDL metadata.
//!   Schema is fully consistent with the replicated user data.
//!
//!   This requires NO additional code — it's standard TiDB startup behavior.
//!
//! # Future enhancement: real-time schema reload during replication
//!
//! A ~1 week TiDB PR can add a "PCR schema version poll" mechanism:
//!   1. Target TiKV writes current resolved_ts to a well-known KV key
//!      (e.g., `/pcr/schema_version`) after each checkpoint
//!   2. Target TiDB polls this key periodically (e.g., every 5s)
//!   3. When the key value changes, TiDB triggers schema reload from KV
//!
//! TiDB-side change (pseudo-code):
//! ```go
//! // In tidb/domain/domain.go loadSchemaLoop():
//! if pcrTS := kv.Get("/pcr/schema_version"); pcrTS > lastPcrTS {
//!     // DDL KV data has been replicated past this point
//!     doReloadSchemas()
//!     lastPcrTS = pcrTS
//! }
//! ```
//!
//! TiKV-side change (this module):
//! ```ignore
//! // After checkpoint, write schema version key:
//! engine.put("/pcr/schema_version", resolved_ts);
//! ```
//!
//! Until this enhancement lands, DDL changes become visible at cutover time.
//!
//! # Comparison with CockroachDB PCR
//!
//! CRDB PCR replicates the ENTIRE virtual cluster (tenant), including system
//! tables AND the SQL layer's schema cache. TiDB PCR replicates KV data only.
//! The trade-off: simpler implementation (no SQL layer changes) vs. deferred
//! DDL visibility (at cutover instead of real-time).

/// Bump the target TiDB's schema version via PD so TiDB reloads
/// table metadata from KV and newly replicated tables become visible.
///
/// TiDB polls PD at `/tidb/ddl/global_schema_version` every schema lease
/// interval (default 1s). Incrementing this key triggers an immediate
/// schema reload on the next lease check, making DDL-discovered tables
/// visible without restart.
///
/// Uses PD HTTP API: POST /pd/api/v1/kv/put with the schema version key.
pub fn bump_target_schema_version(target_pd: &str) {
    // Read current schema version from PD
    let get_url = format!("http://{}/pd/api/v1/kv/key?key=/tidb/ddl/global_schema_version", target_pd);
    let current = match std::process::Command::new("curl")
        .args(&["-s", &get_url])
        .output()
    {
        Ok(o) => {
            let body = String::from_utf8_lossy(&o.stdout);
            serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v.get("value").and_then(|v| v.as_str()).map(|s| s.to_string()))
                .and_then(|s| u64::from_str_radix(&s, 16).ok())
                .unwrap_or(0)
        }
        Err(_) => 0,
    };

    let new_version = current.saturating_add(1);
    let put_url = format!("http://{}/pd/api/v1/kv/put", target_pd);
    let body = format!(
        r#"{{"key":"/tidb/ddl/global_schema_version","value":"{:016x}"}}"#,
        new_version
    );
    let _ = std::process::Command::new("curl")
        .args(&["-s", "-X", "POST", &put_url, "-d", &body])
        .output();

    slog_global::info!("PCR schema sync: bumped target schema version";
        "old" => current, "new" => new_version);
}

pub struct SchemaSync {
    target_pd: String,
    source_pd: String,
    last_synced_version: u64,
}

impl SchemaSync {
    pub fn new(target_pd: String, _target_tidb: String, _source_tidb: String, source_pd: String) -> Self {
        Self { target_pd, source_pd, last_synced_version: 0 }
    }

    /// Read source PD schema version (shell out for base64 + curl).
    fn read_pd_schema_version(pd_addr: &str) -> u64 {
        let script = r#"
k=$(printf '/tidb/ddl/global_schema_version' | base64)
r=$(curl -sf -X POST "http://PLACEHOLDER/v3/kv/range" -d "{\"key\":\"$k\"}" 2>/dev/null)
v=$(echo "$r" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d['kvs'][0]['value'])" 2>/dev/null)
[ -n "$v" ] && echo "$v" | base64 -d 2>/dev/null
"#.replace("PLACEHOLDER", pd_addr);
        let output = std::process::Command::new("bash")
            .args(&["-c", &script])
            .output();
        match output {
            Ok(o) => String::from_utf8_lossy(&o.stdout).trim().parse().unwrap_or(0),
            Err(_) => 0,
        }
    }

    /// Sync source PD schema version to target PD key.
    pub fn bump_schema(&mut self) {
        let src_version = Self::read_pd_schema_version(&self.source_pd);
        if src_version == 0 || src_version <= self.last_synced_version {
            return;
        }
        self.last_synced_version = src_version;

        let put_url = format!("http://{}/pd/api/v1/kv/put", self.target_pd);
        let body = format!(
            r#"{{"key":"/tidb/ddl/global_schema_version","value":"{:016x}"}}"#,
            src_version
        );
        let _ = std::process::Command::new("curl")
            .args(&["-s", "-X", "POST", &put_url, "-d", &body])
            .output();

        slog_global::info!("PCR schema sync: synced target schema version from source";
            "src_version" => src_version);
    }

    fn mysql_count(port: &str, query: &str) -> u64 {
        std::process::Command::new("mysql")
            .args(&["-u", "root", "-h", "127.0.0.1", "-P", port, "-N", "-e", query])
            .output()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().parse().unwrap_or(0))
            .unwrap_or(0)
    }
}
