#!/bin/bash
set -e
# PCR Demo Cluster Full Clean Restart
# Sources: develop/pcr-configs/, develop/pcr-prometheus.yml
# Verified paths: 2026-05-27

TIKV_BIN="/tmp/tikv-server-pcr"
PD_BIN="/Users/cjn/.tiup/components/pd/v8.5.0/pd-server"
TIDB="/Users/cjn/.tiup/components/tidb/v8.5.0/tidb-server"
CONFIGS="/Users/cjn/Documents/OpenSource/dig_tidb/develop/pcr-configs"
PROM_CFG="/Users/cjn/Documents/OpenSource/dig_tidb/develop/pcr-prometheus.yml"
PREFIX="/tmp/pcr"

# ----- kill -----
echo "=== Killing old ==="
pkill -9 tikv-server 2>/dev/null || true
pkill -9 pd-server    2>/dev/null || true
pkill -9 tidb-server  2>/dev/null || true
pkill prometheus      2>/dev/null || true
pkill -f 'grafana server' 2>/dev/null || true
sleep 2

# ----- clean -----
echo "=== Cleaning data ==="
rm -rf ${PREFIX}-src-data ${PREFIX}-tgt-data ${PREFIX}-src-pd ${PREFIX}-tgt-pd

# ----- PD -----
echo "=== Starting PDs ==="
cp ${CONFIGS}/pcr-src-tikv.toml ${PREFIX}-src-tikv.toml
cp ${CONFIGS}/pcr-tgt-tikv.toml ${PREFIX}-tgt-tikv.toml
$PD_BIN --name=pd-src --client-urls=http://127.0.0.1:3379 --peer-urls=http://127.0.0.1:3377 \
  --data-dir=${PREFIX}-src-pd --initial-cluster=pd-src=http://127.0.0.1:3377 > ${PREFIX}-pd-src.log 2>&1 &
$PD_BIN --name=pd-tgt --client-urls=http://127.0.0.1:3380 --peer-urls=http://127.0.0.1:3378 \
  --data-dir=${PREFIX}-tgt-pd --initial-cluster=pd-tgt=http://127.0.0.1:3378 > ${PREFIX}-pd-tgt.log 2>&1 &
for i in $(seq 1 20); do
  sleep 1; SRC=$(curl -s http://127.0.0.1:3379/pd/api/v1/health 2>/dev/null | grep -c '"health": true' || true)
  TGT=$(curl -s http://127.0.0.1:3380/pd/api/v1/health 2>/dev/null | grep -c '"health": true' || true)
  [ "$SRC" -ge 1 ] && [ "$TGT" -ge 1 ] && echo "  PDs ready (${i}s)" && break
  [ $i -eq 20 ] && echo "  PD timeout" && exit 1
done

# ----- TiKV -----
echo "=== Starting TiKVs ==="
cp /Users/cjn/Documents/OpenSource/dig_tidb/tikv85/target/debug/tikv-server ${TIKV_BIN} 2>/dev/null || true
$TIKV_BIN --pd-endpoints=127.0.0.1:3380 --addr=127.0.0.1:30161 \
  --config=${PREFIX}-tgt-tikv.toml > ${PREFIX}-tikv-tgt.log 2>&1 &
sleep 2
$TIKV_BIN --pd-endpoints=127.0.0.1:3379 --addr=127.0.0.1:30162 \
  --config=${PREFIX}-src-tikv.toml > ${PREFIX}-tikv-src.log 2>&1 &
for i in $(seq 1 40); do
  sleep 2; SRC=$(grep -c "TiKV is ready" ${PREFIX}-tikv-src.log 2>/dev/null || true)
  TGT=$(grep -c "TiKV is ready" ${PREFIX}-tikv-tgt.log 2>/dev/null || true)
  [ "$SRC" -ge 1 ] && [ "$TGT" -ge 1 ] && echo "  TiKVs ready ($((i*2))s)" && break
  [ $i -eq 40 ] && echo "  TiKV timeout" && exit 1
done

# ----- TiDB -----
echo "=== Starting TiDBs ==="
$TIDB -P 4100 --status=10082 --store=tikv --path=127.0.0.1:3379 > ${PREFIX}-tidb-src.log 2>&1 &
$TIDB -P 4101 --status=10081 --store=tikv --path=127.0.0.1:3380 > ${PREFIX}-tidb-tgt.log 2>&1 &
for i in $(seq 1 20); do
  sleep 2; SRC=$(mysql -u root -h 127.0.0.1 -P 4100 -N -e "SELECT 1" 2>/dev/null || true)
  TGT=$(mysql -u root -h 127.0.0.1 -P 4101 -N -e "SELECT 1" 2>/dev/null || true)
  [ "$SRC" = "1" ] && [ "$TGT" = "1" ] && echo "  TiDBs ready ($((i*2))s)" && break
  [ $i -eq 20 ] && echo "  TiDB timeout" && exit 1
done

# ----- PCR -----
echo "=== Creating PCR ==="
curl -s -X POST http://127.0.0.1:20190/pcr/control \
  -H "Content-Type: application/json" \
  -d '{"action":"create","source_pd":"127.0.0.1:3379"}' > /dev/null 2>&1
for i in $(seq 1 15); do
  sleep 2; STATE=$(curl -s http://127.0.0.1:20190/pcr/status 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('status',''))" 2>/dev/null)
  [ "$STATE" = "Subscribing" ] && echo "  PCR Subscribing ($((i*2))s)" && break
done

# ----- Monitoring -----
echo "=== Starting monitoring ==="
MON_DIR=/tmp/pcr-monitoring
mkdir -p ${MON_DIR}/prometheus-data ${MON_DIR}/grafana-data 2>/dev/null
cp ${PROM_CFG} ${MON_DIR}/prometheus.yml 2>/dev/null
prometheus --config.file ${MON_DIR}/prometheus.yml --web.listen-address=":9190" \
  --storage.tsdb.path="${MON_DIR}/prometheus-data" > ${MON_DIR}/prometheus.log 2>&1 &
GF_SERVER_HTTP_PORT=3100 GF_PATHS_DATA=${MON_DIR}/grafana-data \
  grafana server --homepath /opt/homebrew/Cellar/grafana/13.0.1/share/grafana > ${MON_DIR}/grafana.log 2>&1 &

echo ""
echo "=== Ready ==="
echo "  Source: mysql -u root -h 127.0.0.1 -P 4100"
echo "  Target: mysql -u root -h 127.0.0.1 -P 4101"
echo "  PCR:    curl http://127.0.0.1:20190/pcr/status"
echo "  Grafana: http://127.0.0.1:3100/d/pcr-demo"
