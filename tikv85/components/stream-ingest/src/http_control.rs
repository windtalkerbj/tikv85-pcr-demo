// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! PCR HTTP control server — lightweight HTTP endpoint for pcr-ctl.
//!
//! Starts a tiny HTTP server on a dedicated port when stream-ingest is enabled.
//! This allows pcr-ctl to send start/pause/resume commands without modifying PD.
//!
//! Mirrors CRDB's pattern where SQL commands schedule jobs via system tables,
//! but adapted for TiKV's standalone PCR architecture.
//!
//! Endpoints:
//!   POST /pcr/control  {"action":"start","source_pd":"...","task":"..."}
//!   GET  /pcr/status   → {"task":"...","status":"...","message":"..."}

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;

use tikv_util::worker::Scheduler;

use crate::task::Task;

#[derive(serde::Deserialize)]
struct PcrControlRequest {
    action: String,
    task: Option<String>,
    source_pd: Option<String>,
    cutover_ts: Option<u64>,
    latest: Option<bool>,
}

#[derive(serde::Serialize)]
struct PcrStatusResponse {
    task: String,
    status: String,
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    lag_seconds: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingested_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ingested_kvs: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    active_subscriptions: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    frontier: Option<BTreeMap<String, u64>>,
}

pub struct PcrControlServer {
    port: u16,
    scheduler: Arc<Mutex<Option<Scheduler<Task>>>>,
}

impl PcrStatusResponse {
    fn simple(task: String, status: &str, message: String) -> Self {
        Self {
            task,
            status: status.to_string(),
            message,
            lag_seconds: None,
            ingested_bytes: None,
            ingested_kvs: None,
            active_subscriptions: None,
            frontier: None,
        }
    }
}

impl PcrControlServer {
    pub fn new(port: u16) -> Self {
        Self { port, scheduler: Arc::new(Mutex::new(None)) }
    }

    pub fn set_scheduler(&self, sched: Scheduler<Task>) {
        *self.scheduler.lock().unwrap() = Some(sched);
    }

    /// Start the HTTP server in a background thread
    pub fn start(self: Arc<Self>) {
        thread::Builder::new().name("pcr-http-ctrl".to_string()).spawn(move || {
            let listener = match TcpListener::bind(("127.0.0.1", self.port)) {
                Ok(l) => {
                    slog_global::info!("PCR HTTP control server started"; "port" => self.port);
                    l
                }
                Err(e) => {
                    slog_global::warn!("PCR HTTP control server failed to bind"; "port" => self.port, "error" => ?e);
                    return;
                }
            };

            for stream in listener.incoming() {
                match stream {
                    Ok(s) => {
                        let sched = self.scheduler.clone();
                        thread::spawn(move || handle_connection(s, sched));
                    }
                    Err(_) => break,
                }
            }
        }).ok();
    }
}

fn handle_connection(mut stream: TcpStream, scheduler: Arc<Mutex<Option<Scheduler<Task>>>>) {
    let mut reader = BufReader::new(stream.try_clone().unwrap_or_else(|_| unreachable!()));

    // Read HTTP request line
    let mut request_line = String::new();
    if reader.read_line(&mut request_line).is_err() { return; }
    let parts: Vec<&str> = request_line.split_whitespace().collect();
    if parts.len() < 2 { return; }
    let (method, path) = (parts[0], parts[1]);

    // Read headers (skip)
    let mut content_length = 0usize;
    loop {
        let mut line = String::new();
        if reader.read_line(&mut line).is_err() { break; }
        if line.trim().is_empty() { break; }
        if line.to_lowercase().starts_with("content-length:") {
            content_length = line.split(':').nth(1).unwrap_or("0").trim().parse().unwrap_or(0);
        }
    }

    // Read body
    let mut body = vec![0u8; content_length];
    if content_length > 0 {
        let _ = reader.read_exact(&mut body);
    }

    match (method, path) {
        ("POST", "/pcr/control") => {
            let req: PcrControlRequest = match serde_json::from_slice(&body) {
                Ok(r) => r,
                Err(e) => { http_respond(&mut stream, 400, &format!("{{\"error\":\"{}\"}}", e)); return; }
            };

            let s = scheduler.lock().unwrap();
            let sched = match s.as_ref() {
                Some(s) => s,
                None => { http_respond(&mut stream, 503, r#"{"error":"PCR not initialized"}"#); return; }
            };

            let task_name = req.task.unwrap_or_else(|| "pcr_default".to_string());

            let result = match req.action.as_str() {
                "create" => {
                    let source_pd = req.source_pd.clone().unwrap_or_default();
                    // Validate source_pd format: must be host:port with non-empty host
                    if source_pd.is_empty() || source_pd.split(':').count() != 2 || source_pd.starts_with(':') {
                        let err_msg = format!(
                            "Invalid source_pd '{}': must be PD HTTP address like 127.0.0.1:2379",
                            source_pd
                        );
                        slog_global::error!("{}", err_msg);
                        serde_json::to_string(&PcrStatusResponse::simple(
                            task_name.clone(), "error", err_msg,
                        ))
                    } else {
                        let _ = sched.schedule(Task::CreateReplication {
                            source_pd: source_pd.clone(),
                            task_name: task_name.clone(),
                        });
                        serde_json::to_string(&PcrStatusResponse::simple(
                            task_name.clone(), "creating",
                            format!("Full replication from {} starting (full scan)", source_pd),
                        ))
                    }
                }
                "start" => {
                    slog_global::warn!("PCR: 'start' action is deprecated, use 'create' — delegating to 'create'");
                    let source_pd = req.source_pd.clone().unwrap_or_default();
                    if source_pd.is_empty() || source_pd.split(':').count() != 2 || source_pd.starts_with(':') {
                        let err_msg = format!(
                            "Invalid source_pd '{}': must be PD HTTP address like 127.0.0.1:2379",
                            source_pd
                        );
                        slog_global::error!("{}", err_msg);
                        serde_json::to_string(&PcrStatusResponse::simple(
                            task_name.clone(), "error", err_msg,
                        ))
                    } else {
                        let _ = sched.schedule(Task::CreateReplication {
                            source_pd: source_pd.clone(),
                            task_name: task_name.clone(),
                        });
                        serde_json::to_string(&PcrStatusResponse::simple(
                            task_name.clone(), "creating",
                            format!("Full replication from {} starting (via deprecated 'start')", source_pd),
                        ))
                    }
                }
                "pause" => {
                    let _ = sched.schedule(Task::PauseReplication);
                    serde_json::to_string(&PcrStatusResponse::simple(
                        task_name, "paused", "Replication paused".into(),
                    ))
                }
                "resume" => {
                    // Verify checkpoint exists — resume is delta-only and requires prior replication.
                    let state_data_dir = std::path::Path::new("/tmp/pcr-tgt-data/pcr");
                    let checkpoint_file = state_data_dir.join("pcr_checkpoint_pcr_default.json");
                    let has_checkpoint = std::fs::metadata(&checkpoint_file).is_ok();
                    if !has_checkpoint {
                        let err_msg = "No checkpoint found — cannot resume without prior replication. Use 'create' for a fresh full scan.".to_string();
                        slog_global::error!("{}", err_msg);
                        serde_json::to_string(&PcrStatusResponse::simple(
                            task_name, "error", err_msg,
                        ))
                    } else {
                        let _ = sched.schedule(Task::ResumeReplication);
                        serde_json::to_string(&PcrStatusResponse::simple(
                            task_name, "running", "Replication resumed (delta-only from checkpoint)".into(),
                        ))
                    }
                }
                "cutover" => {
                    let cutover_ts = if req.latest.unwrap_or(false) {
                        // "TO LATEST": use max u64 as a sentinel.
                        // The event loop treats cutover_ts > 0 as "cutover requested"
                        // and waits for any progress (global_min > 0).
                        u64::MAX
                    } else {
                        req.cutover_ts.unwrap_or(0)
                    };
                    let _ = sched.schedule(Task::Cutover { cutover_ts });
                    serde_json::to_string(&PcrStatusResponse::simple(
                        task_name, "cutover",
                        format!("Cutover initiated at ts={}", cutover_ts),
                    ))
                }
                "shutdown" => {
                    let _ = sched.schedule(Task::Shutdown);
                    serde_json::to_string(&PcrStatusResponse::simple(
                        task_name, "shutdown", "PCR shutdown".into(),
                    ))
                }
                "activate" => {
                    let _ = sched.schedule(Task::Activate);
                    serde_json::to_string(&PcrStatusResponse::simple(
                        task_name, "activated",
                        "Target cluster activated — ready for read/write".into(),
                    ))
                }
                _ => Ok(format!("{{\"error\":\"unknown action: {}\"}}", req.action)),
            };
            http_respond(&mut stream, 200, &result.unwrap_or_default());
        }
        ("GET", "/pcr/status") => {
            // Read real PCR state from the checkpoint file.
            // State file is at the TiKV data dir, checkpoint is under <data_dir>/pcr/
            let state_data_dir = std::path::Path::new("/tmp/pcr-tgt-data/pcr");
            let state_file = state_data_dir.join("pcr_task_state");
            let checkpoint_file = state_data_dir.join("pcr_checkpoint_pcr_default.json");

            let raw_state = std::fs::read_to_string(&state_file)
                .unwrap_or_else(|_| "unknown".to_string());

            // Compute replication lag from checkpoint file
            let lag_secs: Option<f64> = std::fs::read_to_string(&checkpoint_file)
                .ok()
                .and_then(|data| serde_json::from_str::<serde_json::Value>(&data).ok())
                .and_then(|v| {
                    v.as_object().and_then(|obj| {
                        obj.values()
                            .filter_map(|v| v.as_u64())
                            .min()
                    })
                })
                .map(|min_ts| {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let resolved_secs = min_ts >> 18; // TS → seconds
                    now.saturating_sub(resolved_secs) as f64
                });

            // Determine actual state: if checkpoint exists with data, PCR has run.
            // If lag is None (no checkpoint data), PCR hasn't started.
            let (status_display, message): (String, String) = match raw_state.trim() {
                "Creating" => ("Creating".into(), "PCR replication starting with full scan".into()),
                "Subscribing" => ("Subscribing".into(), "PCR replication active — subscribed to source regions".into()),
                "Running" => ("Running".into(), "PCR replication running — actively ingesting data".into()),
                "Paused" => ("Paused".into(), "PCR replication paused".into()),
                "CuttingOver" => ("CuttingOver".into(), "Cutover in progress".into()),
                "Completed" => ("Completed".into(), "Cutover completed, waiting for activation".into()),
                "Activated" => ("Activated".into(), "Target cluster activated — ready for read/write".into()),
                "Failed" => ("Failed".into(), "PCR replication failed".into()),
                _ => {
                    // State file doesn't exist or is unknown — infer from checkpoint
                    if lag_secs.is_some() {
                        ("Running".into(), "PCR replication active — checkpoint data present".into())
                    } else {
                        ("Paused".into(), "PCR not started (use 'create' to begin)".into())
                    }
                }
            };

            // Load frontier for detailed status
            let frontier: Option<BTreeMap<String, u64>> =
                std::fs::read_to_string(&checkpoint_file).ok()
                    .and_then(|d| serde_json::from_str::<serde_json::Value>(&d).ok())
                    .and_then(|v| {
                        v.as_object().map(|obj| {
                            obj.iter()
                                .map(|(k, v)| (k.clone(), v.as_u64().unwrap_or(0)))
                                .collect()
                        })
                    });

            let resp = serde_json::to_string(&PcrStatusResponse {
                task: "pcr_default".into(),
                status: status_display,
                message,
                lag_seconds: lag_secs,
                ingested_bytes: None,
                ingested_kvs: None,
                active_subscriptions: None,
                frontier,
            }).unwrap_or_default();
            http_respond(&mut stream, 200, &resp);
        }
        _ => {
            http_respond(&mut stream, 404, r#"{"error":"not found"}"#);
        }
    }
}

fn http_respond(stream: &mut TcpStream, code: u16, body: &str) {
    let response = format!(
        "HTTP/1.1 {} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        code, body.len(), body
    );
    let _ = stream.write_all(response.as_bytes());
}
