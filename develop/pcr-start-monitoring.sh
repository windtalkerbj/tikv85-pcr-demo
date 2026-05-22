#!/bin/bash
# Start Prometheus + Grafana for PCR Demo monitoring
# Ports: Prometheus=9190, Grafana=3100 (avoid conflict with tiup defaults 9090/3000)

MON_DIR=/tmp/pcr-monitoring
mkdir -p $MON_DIR/prometheus-data $MON_DIR/grafana-data

# Prometheus
pkill prometheus 2>/dev/null
prometheus --config.file $MON_DIR/prometheus.yml \
  --web.listen-address=":9190" \
  --storage.tsdb.path="$MON_DIR/prometheus-data" \
  > $MON_DIR/prometheus.log 2>&1 &
echo "Prometheus: http://127.0.0.1:9190"

# Grafana
pkill -f 'grafana server' 2>/dev/null
GF_SERVER_HTTP_PORT=3100 \
GF_PATHS_DATA=$MON_DIR/grafana-data \
grafana server --homepath /opt/homebrew/Cellar/grafana/13.0.1/share/grafana \
  > $MON_DIR/grafana.log 2>&1 &

sleep 8
curl -s http://127.0.0.1:3100/api/health > /dev/null 2>&1 && \
  echo "Grafana:  http://127.0.0.1:3100  (admin/admin)" || \
  echo "Grafana: still starting..."

echo ""
echo "PCR Dashboard: http://127.0.0.1:3100/d/pcr-demo"
echo "Prometheus UI: http://127.0.0.1:9190"
