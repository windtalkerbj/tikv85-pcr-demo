#!/bin/bash
# PCR Smoke Test — INSERT 1 row + convergence + sink health
# Usage: bash scripts/pcr-validate.sh

SRC_DB="mysql -u root -h 127.0.0.1 -P 4100"
TGT_DB="mysql -u root -h 127.0.0.1 -P 4101"
PCR_API="http://127.0.0.1:20190/pcr/status"
FAIL=0

# 1. PCR health
STATE=$(curl -s $PCR_API 2>/dev/null | python3 -c "import json,sys;d=json.load(sys.stdin);print(d['status'])" 2>/dev/null)
LAG=$(curl -s $PCR_API 2>/dev/null | python3 -c "import json,sys;print(json.load(sys.stdin).get('lag_seconds',0))" 2>/dev/null)
if [ "$STATE" = "Subscribing" ]; then
  echo "  ✅ PCR $STATE (lag=${LAG}s)"
else
  echo "  ❌ PCR state: $STATE"
  FAIL=1
fi

# 2. Ensure smoke table
$SRC_DB -e "CREATE DATABASE IF NOT EXISTS pcr" 2>/dev/null
$SRC_DB -e "CREATE TABLE IF NOT EXISTS pcr.smoke (id INT PRIMARY KEY AUTO_INCREMENT, v INT)" 2>/dev/null

# 3. Wait for full scan to include the table (if new)
sleep 5

# 4. INSERT
$SRC_DB -e "INSERT INTO pcr.smoke (v) VALUES ($RANDOM)" 2>/dev/null
SRC_COUNT=$($SRC_DB -N -e "SELECT COUNT(*) FROM pcr.smoke" 2>/dev/null || echo "0")
echo "  INSERT: SRC=$SRC_COUNT"

# 5. Wait convergence
for i in $(seq 1 20); do
  sleep 1
  TGT=$($TGT_DB -N -e "SELECT COUNT(*) FROM pcr.smoke" 2>/dev/null || echo "0")
  [ "$TGT" = "$SRC_COUNT" ] && [ "$SRC_COUNT" != "0" ] && echo "  ✅ converged ${i}s SRC=$SRC_COUNT TGT=$TGT" && break
done
[ "$TGT" != "$SRC_COUNT" ] && echo "  ❌ not converged SRC=$SRC_COUNT TGT=$TGT" && FAIL=1

# 6. Sink health
DISCONNECTS=$(grep -c 'sink disconnected' /tmp/pcr-tikv-src.log 2>/dev/null | tail -1 | tr -d '\n' || echo "0")
SKIPPED=$(grep -c 'SKIPPED.*sink is None' /tmp/pcr-tikv-src.log 2>/dev/null | tail -1 | tr -d '\n' || echo "0")
[ -z "$DISCONNECTS" ] && DISCONNECTS=0
[ -z "$SKIPPED" ] && SKIPPED=0
if [ "$DISCONNECTS" -eq 0 ] && [ "$SKIPPED" -eq 0 ]; then
  echo "  ✅ sink healthy (0 disconnect, 0 skipped)"
else
  echo "  ⚠️  disconnects=$DISCONNECTS skipped=$SKIPPED"
fi

# 7. Relay
RELAY=$(grep "PCR relay.*heartbeat" /tmp/pcr-tikv-src.log 2>/dev/null | tail -1 | grep -o 'events_processed=[0-9]*' | cut -d= -f2)
echo "  relay events: ${RELAY:-N/A}"

# Final
if [ $FAIL -eq 0 ]; then
  echo "  🟢 PASS"
else
  echo "  🔴 FAIL"
fi
exit $FAIL
