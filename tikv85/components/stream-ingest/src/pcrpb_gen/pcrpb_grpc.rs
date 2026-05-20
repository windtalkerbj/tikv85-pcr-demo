// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

//! gRPC client stub for PcrStream (grpcio 0.10.x compatible).
//!
//! Replace with generated code when protoc-grpcio is integrated.

use super::pcrpb::*;

/// Consumer-side gRPC client for subscribing to PCR event streams.
#[derive(Clone)]
pub struct PcrStreamClient {
    _channel: String, // gRPC channel address (placeholder for prototype)
}

impl PcrStreamClient {
    pub fn new(_addr: &str) -> Self {
        Self { _channel: _addr.to_string() }
    }

    /// Subscribe to a PCR event stream.
    ///
    /// In production, this creates a server-streaming gRPC call.
    /// For the local prototype, the actual gRPC transport is handled
    /// by the StreamSubscriber using raw grpcio::Client API.
    pub fn subscribe_blocking(&self, _req: &PcrSubscribeRequest) -> Vec<PcrEvent> {
        // Placeholder — actual implementation uses grpcio::Client::server_streaming()
        Vec::new()
    }
}
