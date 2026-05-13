#!/usr/bin/env bash
# Bring up the RATLC pool stack:
#   - pool-manager (long-lived daemon, forks N bridge workers)
#   - api-server (stateless HTTP :4242 — restart freely)
#
# Usage:
#   ./scaffolding/pool/start.sh [POOL_SIZE]
#
# Stop:
#   node scaffolding/pool/ratlc-ctl.mjs shutdown
#   (or: kill $(cat /tmp/ratlc-pool.pid /tmp/ratlc-api.pid))
#
# Status:
#   node scaffolding/pool/ratlc-ctl.mjs status
#   node scaffolding/pool/ratlc-ctl.mjs watch 2
#
# Scale:
#   node scaffolding/pool/ratlc-ctl.mjs ramp-up 3
#   node scaffolding/pool/ratlc-ctl.mjs ramp-down 1

set -e
cd "$(dirname "$0")/../.."

POOL_SIZE="${1:-${POOL_SIZE:-2}}"
POOL_MODEL="${POOL_MODEL:-claude-opus-4-7-thinking-max-fast}"

# Stop anything stale
node scaffolding/pool/ratlc-ctl.mjs shutdown 2>/dev/null || true
sleep 2
lsof -ti :4242 | xargs kill -9 2>/dev/null || true
pkill -f "pool-manager.mjs" 2>/dev/null || true
pkill -f "bridge-worker.mjs" 2>/dev/null || true
pkill -f "api-server.mjs" 2>/dev/null || true
rm -f /tmp/ratlc-pool.sock /tmp/ratlc-pool.log /tmp/ratlc-api.log /tmp/ratlc-pool.pid /tmp/ratlc-api.pid
sleep 1

# Start pool manager
POOL_SIZE="$POOL_SIZE" POOL_MODEL="$POOL_MODEL" \
  nohup node scaffolding/pool/pool-manager.mjs > /tmp/ratlc-pool.log 2>&1 &
POOL_PID=$!
echo $POOL_PID > /tmp/ratlc-pool.pid
disown $POOL_PID 2>/dev/null || true
sleep 1

# Start api-server
nohup node scaffolding/pool/api-server.mjs > /tmp/ratlc-api.log 2>&1 &
API_PID=$!
echo $API_PID > /tmp/ratlc-api.pid
disown $API_PID 2>/dev/null || true
sleep 1

echo "════════════════════════════════════════════════════════════"
echo "  RATLC pool stack started"
echo "    Pool manager: pid=$POOL_PID, log /tmp/ratlc-pool.log"
echo "    API server:   pid=$API_PID, http://127.0.0.1:4242"
echo "    Target size:  $POOL_SIZE channels"
echo "    Model:        $POOL_MODEL"
echo "════════════════════════════════════════════════════════════"
echo ""
echo "  ▸ Status:   node scaffolding/pool/ratlc-ctl.mjs status"
echo "  ▸ Watch:    node scaffolding/pool/ratlc-ctl.mjs watch 2"
echo "  ▸ Scale up: node scaffolding/pool/ratlc-ctl.mjs ramp-up 3"
echo "  ▸ Shutdown: node scaffolding/pool/ratlc-ctl.mjs shutdown"
echo ""
echo "  Channels open sequentially — first will be ready in 1-5 min."
echo "  Tail the log to watch progress: tail -f /tmp/ratlc-pool.log"
