# TiDB PCR Patches

Apply to TiDB v8.5.0 source tree (`https://github.com/pingcap/tidb`, tag `v8.5.0`).

## Files

- `main.go.patch` — 73 lines: `cmd/tidb-server/main.go`
- `session.go.patch` — 2 hunks: `pkg/session/session.go`

## Changes

### main.go — createReadOnlyDomain + PCR_READ_ONLY handling

Adds `createReadOnlyDomain()` function and modifies `createStoreDDLOwnerMgrAndDomain()`:
- When `PCR_READ_ONLY=1`, disables DDL and catches BootstrapSession DefaultNotFound
- Falls back to bare domain (no schema load) when bootstrap fails

### session.go — skip runInBootstrapSession in PCR mode

- Adds `"os"` import
- Changes `runInBootstrapSession` guard: when `PCR_READ_ONLY=1`, skip bootstrap writes

## Apply

```bash
cd /path/to/tidb-v8.5.0
patch -p1 < tidb-patches/main.go.patch
patch -p1 < tidb-patches/session.go.patch
go build -o tidb-server ./cmd/tidb-server
```

## Verify

```bash
PCR_READ_ONLY=1 ./tidb-server -P 4101 --store=tikv --path=<target-pd>:2379
```
