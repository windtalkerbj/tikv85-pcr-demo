# TiKV Project Guide for AI Agents

## Project Overview

**TiKV** ("Ti" stands for Titanium) is an open-source, distributed, transactional key-value database written in Rust. It is a graduated project of the Cloud Native Computing Foundation (CNCF).

### Key Characteristics
- **Language**: Rust (with C++ components for RocksDB and gRPC)
- **Version**: 8.5.5
- **License**: Apache-2.0
- **Repository**: https://github.com/tikv/tikv/
- **Homepage**: https://tikv.org

### Architecture Highlights
- Uses the **Raft consensus algorithm** for distributed consensus
- Powered by **RocksDB** for persistent storage
- Uses **Placement Driver (PD)** for cluster management and auto-sharding
- Implements **Google Percolator**-style distributed transactions with ACID compliance
- Supports both classical key-value APIs and transactional APIs

### Main Components
- **Placement Driver (PD)**: Cluster manager that balances load and data
- **Store**: Contains RocksDB for local disk storage
- **Region**: Basic unit of key-value data movement, replicated to multiple nodes
- **Node**: Physical node in the cluster containing one or more stores

## Technology Stack

### Core Dependencies
- **Rust Toolchain**: nightly-2023-12-28 (pinned via `rust-toolchain.toml`)
- **Storage Engine**: RocksDB (via rust-rocksdb bindings)
- **Consensus**: Raft (via raft-rs)
- **RPC**: gRPC (via grpc-rs)
- **Async Runtime**: Tokio
- **Serialization**: Protocol Buffers (protobuf 2.8)

### Required Tools
- `rustup` - Rust toolchain manager
- `make` - Build automation
- `cmake` - Build tool (for gRPC)
- `protoc` - Protocol buffer compiler
- `gcc 5+` or `clang` - C++ compiler

### Key External Dependencies
- kvproto - Protocol buffer definitions
- tipb - TiDB protocol buffers
- raft-engine - Log storage engine
- yatp - Thread pool implementation

## Project Structure

```
tikv/
├── Cargo.toml              # Main workspace manifest
├── src/                    # Main library source
│   ├── lib.rs              # Library entry point
│   ├── config.rs           # Configuration management
│   ├── coprocessor.rs      # Coprocessor framework
│   ├── coprocessor_v2.rs   # Coprocessor v2 (plugin-based)
│   ├── import.rs           # Data import functionality
│   ├── read_pool.rs        # Read request pool
│   ├── server.rs           # Server implementation
│   └── storage.rs          # Storage engine interface
├── cmd/                    # Binary executables
│   ├── tikv-server/        # Main server binary
│   └── tikv-ctl/           # Control utility
├── components/             # Modular components (70+ crates)
│   ├── raftstore/          # Raft consensus implementation
│   ├── raftstore-v2/       # Next-gen raftstore
│   ├── tikv_util/          # Utility functions
│   ├── engine_rocks/       # RocksDB engine implementation
│   ├── engine_traits/      # Engine abstraction traits
│   ├── storage/            # Storage layer
│   ├── pd_client/          # Placement Driver client
│   ├── cdc/                # Change Data Capture
│   ├── backup/             # Backup functionality
│   ├── backup-stream/      # Streaming backup
│   ├── encryption/         # Encryption at rest
│   ├── security/           # Security utilities
│   └── ...
├── tests/                  # Integration tests
│   ├── benches/            # Benchmarks
│   ├── failpoints/         # Failpoint tests
│   └── integrations/       # Integration tests
├── scripts/                # Build and test scripts
├── etc/                    # Configuration templates
├── fuzz/                   # Fuzz testing
└── doc/                    # Documentation
```

### Key Component Categories

1. **Storage Engines** (`engine_*`)
   - `engine_traits`: Abstract storage engine interface
   - `engine_rocks`: RocksDB implementation
   - `engine_test`: Test engines
   - `raft_log_engine`: Raft log storage

2. **Raft Consensus** (`raftstore*`)
   - `raftstore`: Main Raft implementation
   - `raftstore-v2`: Next-generation implementation
   - `batch-system`: Raft message batching

3. **Query Processing** (`tidb_query_*`)
   - `tidb_query_common`: Common query utilities
   - `tidb_query_datatype`: Data type handling
   - `tidb_query_executors`: Query executors
   - `tidb_query_expr`: Expression evaluation
   - `tidb_query_aggr`: Aggregation functions

4. **Transaction Support** (`txn_types`, `concurrency_manager`, `cdc`, `resolved_ts`)

5. **Cloud Integration** (`cloud/`, `external_storage`)
   - AWS, Azure, GCP support

6. **Testing Utilities** (`test_*`)
   - Various test helpers and mock implementations

## Build System

### Important Make Targets

```bash
# Development builds
make build           # Development profile, unoptimized build
make run             # Run a development build
make release         # Optimized release build (thinLTO)

# Testing
make test            # Run full test suite
make dev             # Format + clippy + test (PR requirement)

# Code quality
make format          # Format code with rustfmt
make clippy          # Run clippy with TiKV-specific config
make audit           # Check for security vulnerabilities
make doc             # Generate documentation

# Distribution builds
make dist_release    # Full release build with LTO and debuginfo
make dist_artifacts  # Build Docker images and tarballs

# Specialized builds
make unportable_release  # Build with native CPU optimizations
make prof_release        # Build with jemalloc memory profiling
make fail_release        # Build with failpoints for chaos testing
```

### Build Features

Key feature flags (controlled via `ENABLE_FEATURES`):
- `jemalloc` (default) / `tcmalloc` / `mimalloc` / `snmalloc` - Memory allocators
- `mem-profiling` - Memory profiling support (Linux only)
- `portable` - Build portable binaries (march=x86-64)
- `sse` - Enable SSE4.2 optimizations
- `failpoints` - Enable failpoints for testing
- `openssl-vendored` - Statically link OpenSSL
- `pprof-fp` - Frame pointer support for profiling

### Environment Variables

- `TIKV_FRAME_POINTER=1` (default) - Enable frame pointers for profiling
- `FAIL_POINT=1` - Enable failpoints
- `RUSTFLAGS` - Additional Rust compiler flags
- `CARGO_BUILD_PIPELINING=true` (default) - Enable pipelined compilation

### Cargo Profiles

- **dev**: Fast compilation, no debuginfo by default
- **release**: Optimized with thinLTO
- **test**: Test-specific settings with overflow checks
- **bench**: Benchmark profile (similar to release)
- **dist_release**: Full optimization with complete debuginfo

## Testing Strategy

### Test Organization

1. **Unit Tests**: Embedded in source files (`#[cfg(test)]`)
2. **Integration Tests**: In `tests/` directory
   - `tests/integrations/`: Component integration tests
   - `tests/failpoints/`: Failpoint-based chaos tests
   - `tests/benches/`: Performance benchmarks

3. **Component Tests**: In individual component directories

### Running Tests

```bash
# Full test suite (required before PR)
make test

# Run specific test
./scripts/test $TESTNAME -- --nocapture

# Run with nextest
make test_with_nextest

# Docker-based testing
make docker_test
```

### Test Scripts

- `scripts/test-all`: Runs tests with various configurations
- `scripts/test`: Core test runner
- `scripts/clippy-all`: Runs clippy with all configurations

### Failpoints

TiKV uses the `fail` crate for failure injection testing. Tests in `tests/failpoints/` use these to simulate various failure scenarios.

## Code Style Guidelines

### Formatting (rustfmt.toml)

- **Maximum width**: 100 characters
- **Comment width**: 80 characters
- **Imports**: Grouped by Std/External/Crate, granularity at crate level
- **Newline style**: Unix
- Features enabled: `format_code_in_doc_comments`, `normalize_comments`, etc.

### Code Comment Style

See `CODE_COMMENT_STYLE.md` for detailed guidelines:

- Use **American English** (color, not colour)
- Use **standard capitalization** (TiKV, RocksDB, gRPC, etc.)
- Comments should be **descriptive** ("Opens the file") not imperative ("Open the file")
- Use `///` for doc comments, `//` for implementation comments
- Capitalize first letter and end with period
- Use "this" instead of "the" to refer to current things

### Clippy Configuration (clippy.toml)

- Disallows certain unsafe methods (see RUSTSEC entries)
- Custom disallowed methods for thread spawning
- Uses wrapper functions for proper hook handling

### Security Policy (deny.toml)

- Bans Rust crypto libraries (uses OpenSSL for FIPS 140-2 compliance)
- Allows specific license types (Apache-2.0, MIT, BSD-3-Clause, etc.)
- Ignores certain security advisories (with documented reasons)
- Restricts git sources to tikv, pingcap, rust-lang orgs

## Development Workflow

### Before Submitting a PR

1. Run the full development check:
   ```bash
   make dev
   ```
   This runs: format + clippy + tests

2. Ensure tests pass:
   ```bash
   make test
   ```

3. Link issues in PR body:
   ```
   Issue Number: close #123, ref #456
   ```

### Commit Message Format

PR title becomes the commit subject. Use commit-message block in PR body:

```
```commit-message
Detailed commit message body describing why and how.

* fix something 1
* fix something 2
```
```

Subject: max 50 characters
Body: wrapped at 72 characters

Must include `Signed-off-by` line (use `git commit -s`)

### Performance Critical Path

Files marked with `#[PerformanceCriticalPath]` require special attention:
- Avoid unnecessary synchronous I/O
- Avoid verbose logging (info level and above)
- Avoid global locks
- Keep synchronous tasks short

See `PERFORMANCE_CRITICAL_PATH.md` for details.

## Configuration

### Server Configuration

Configuration file: `etc/config-template.toml`

Key sections:
- `log`: Log level, format, file settings
- `quota`: CPU and bandwidth limitations
- `memory`: Memory usage limits and profiling
- Storage engine configuration (RocksDB)
- Raft configuration
- Security (TLS, encryption)

### Command Line Options

```bash
./tikv-server --pd-endpoints="127.0.0.1:2379" \
              --addr="127.0.0.1:20160" \
              --data-dir=/data/tikv \
              --log-file=/var/log/tikv.log
```

## Security Considerations

### Security Team
- Contact: tikv-security@lists.cncf.io
- PGP key available in `SECURITY.md`

### Supported Versions for Security Updates
- 6.x, 5.x, 4.x, 3.x, 2.x: Supported
- < 2.0: Not supported

### Security Features
- Encryption at rest (via `encryption` component)
- TLS support for connections
- FIPS 140-2 compliance mode (via OpenSSL)

### Vulnerability Reporting
Do NOT use GitHub Issues for security vulnerabilities. Use the security email above.

## Deployment

### Docker

```bash
make docker              # Build Docker image
make docker_tag          # Tag with git hash and tag
```

Image: `pingcap/tikv:latest`

Ports:
- 20160: TiKV service port
- 20180: Status port

### Dependencies for Running

TiKV requires PD (Placement Driver) to function:

```bash
# Start PD
./pd-server --name=pd --data-dir=/tmp/pd/data \
            --client-urls="http://127.0.0.1:2379" \
            --peer-urls="http://127.0.0.1:2380" \
            --initial-cluster="pd=http://127.0.0.1:2380"

# Start TiKV
./tikv-server --pd-endpoints="127.0.0.1:2379" \
              --addr="127.0.0.1:20160" \
              --data-dir=/tmp/tikv/data
```

## Additional Resources

- **Documentation**: https://tikv.org/docs/
- **Deep Dive**: https://tikv.org/deep-dive/
- **API Docs**: https://tikv.github.io
- **Community Chat**: https://tikv.org/chat
- **Slack**: https://slack.tidb.io/invite?team=tikv-wg

### Related Projects
- TiDB: https://github.com/pingcap/tidb
- PD: https://github.com/tikv/pd
- client-go: https://github.com/tikv/client-go
- raft-rs: https://github.com/tikv/raft-rs
- rust-rocksdb: https://github.com/tikv/rust-rocksdb

## Common Development Tasks

### Adding a New Component

1. Create directory under `components/`
2. Add `Cargo.toml` with proper metadata
3. Add to workspace members in root `Cargo.toml`
4. Add to workspace dependencies if needed

### Adding Tests

- Unit tests: In source files with `#[cfg(test)]`
- Integration tests: In `tests/integrations/` or component `tests/` directories
- Failpoint tests: In `tests/failpoints/`

### Debugging Tips

- Use `RUSTFLAGS=-Cdebuginfo=1` for line numbers in stack traces
- Use `RUSTFLAGS=-Cdebuginfo=2` for full debuginfo
- Enable failpoints with `FAIL_POINT=1` to test failure scenarios
- Use frame pointers (enabled by default) for profiling with `pprof`

### Memory Profiling

```bash
# Build with profiling support
make prof_release

# Set environment variable
export MALLOC_CONF=prof:true,prof_active:false

# Generate heap dump
# (use jeprof or similar tools to analyze)
```
