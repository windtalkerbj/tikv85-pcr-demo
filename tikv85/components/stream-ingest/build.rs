// Copyright 2024 TiKV Project Authors. Licensed under Apache-2.0.

/// Build script for PCR proto compilation.
///
/// Uses `protobuf-build` (same as kvproto) to compile `proto/pcrpb/pcrpb.proto`
/// into proper Rust protobuf types with real `Message` trait implementations.
///
/// Falls back to manual stubs if protoc is unavailable.
fn main() {
    println!("cargo:rerun-if-changed=../../proto/pcrpb/pcrpb.proto");
    println!("cargo:rerun-if-changed=src/pcrpb_gen/pcrpb_stub.rs");

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let proto_file = format!("{}/../../proto/pcrpb/pcrpb.proto", manifest_dir);
    let proto_root = format!("{}/../../proto", manifest_dir);
    let out_dir = std::env::var("OUT_DIR").unwrap();

    let has_protoc = std::process::Command::new("protoc")
        .arg("--version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);

    if has_protoc {
        // Use protobuf-build to generate:
        //   out/mod.rs         — pub mod pcrpb; pub mod pcrpb_grpc;
        //   out/pcrpb.rs       — real protobuf Message types
        //   out/pcrpb_grpc.rs  — gRPC client/server stubs
        protobuf_build::Builder::new()
            .files(&[&proto_file])
            .includes(&[&proto_root])
            .out_dir(&out_dir)
            .generate();
        println!("cargo:warning=PCR proto: generated via protobuf-build");
    } else {
        // Fallback: copy manual stubs, create a minimal mod.rs
        let stub_path = format!("{}/src/pcrpb_gen/pcrpb_stub.rs", manifest_dir);
        std::fs::copy(&stub_path, format!("{}/pcrpb.rs", out_dir)).ok();
        std::fs::write(
            format!("{}/mod.rs", out_dir),
            "pub mod pcrpb;\n",
        ).ok();
        println!("cargo:warning=PCR proto: protoc not found, using manual stubs");
    }
}
