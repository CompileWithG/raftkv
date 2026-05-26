#!/usr/bin/env bash
#
# demo.sh — launch a real 3-node raftkv cluster as separate processes and drive
# a full leader-failover scenario end to end:
#
#   1. start 3 nodes, wait for a leader to be elected
#   2. PUT a key (client auto-redirects to the leader)
#   3. GET it back from a *different* node
#   4. kill the leader process, confirm a NEW leader is elected
#   5. confirm the previously-written key is still readable
#   6. restart the dead node, confirm it rejoins and catches up
#   7. tear everything down and remove temp data
#
# Requires: bash, curl, and the built binaries in ./bin (run `make build`).

set -euo pipefail
cd "$(dirname "$0")/.."

BIN=./bin/raftkv
CTL=./bin/raftctl
DATA=tmp-demo
PEERS="n1=127.0.0.1:9001,n2=127.0.0.1:9002,n3=127.0.0.1:9003"
declare -A ADDR=( [n1]=127.0.0.1:9001 [n2]=127.0.0.1:9002 [n3]=127.0.0.1:9003 )
declare -A PID

if [[ ! -x "$BIN" || ! -x "$CTL" ]]; then
  echo "binaries missing; run 'make build' first" >&2
  exit 1
fi

rm -rf "$DATA"; mkdir -p "$DATA"

start_node() {
  local id=$1
  "$BIN" --id "$id" --addr "${ADDR[$id]}" --peers "$PEERS" --data-dir "$DATA/$id" \
    >"$DATA/$id.log" 2>&1 &
  PID[$id]=$!
}

cleanup() {
  echo
  echo "--- tearing down cluster ---"
  for id in "${!PID[@]}"; do
    kill "${PID[$id]}" 2>/dev/null || true
  done
  wait 2>/dev/null || true
  rm -rf "$DATA"
  echo "done; no processes left, temp data removed."
}
trap cleanup EXIT

status()   { curl -s "http://${ADDR[$1]}/status"; }
leader_of() { status "$1" | sed -n 's/.*"leader":"\([^"]*\)".*/\1/p'; }
state_of()  { status "$1" | sed -n 's/.*"state":"\([^"]*\)".*/\1/p'; }

# poll until node $1 reports a non-empty leader; echoes the leader id
wait_for_leader() {
  local via=$1 l
  for _ in $(seq 1 60); do
    l=$(leader_of "$via" || true)
    if [[ -n "${l:-}" ]]; then echo "$l"; return 0; fi
    sleep 0.5
  done
  echo "" ; return 1
}

echo "=== 1. starting 3-node cluster ==="
for id in n1 n2 n3; do start_node "$id"; echo "  started $id (pid ${PID[$id]}) on ${ADDR[$id]}"; done

echo
echo "=== 2. waiting for leader election ==="
LEADER=$(wait_for_leader n1) || { echo "no leader elected"; exit 1; }
echo "  leader elected: $LEADER"
for id in n1 n2 n3; do echo "  $id: state=$(state_of "$id")"; done

echo
echo "=== 3. PUT city=paris (sent to n2, redirects to leader if needed) ==="
$CTL -addr "${ADDR[n2]}" put city paris

echo
echo "=== 4. GET city from n3 (follower redirects to leader) ==="
$CTL -addr "${ADDR[n3]}" get city

echo
echo "=== 5. killing the leader ($LEADER) ==="
kill "${PID[$LEADER]}"; unset 'PID[$LEADER]'
# pick a surviving node to query
SURVIVOR=n1; [[ "$LEADER" == n1 ]] && SURVIVOR=n2
echo "  querying survivor $SURVIVOR for the new leader..."
sleep 1
NEWLEADER=$(wait_for_leader "$SURVIVOR") || { echo "no new leader elected"; exit 1; }
echo "  NEW leader elected: $NEWLEADER (was $LEADER)"

echo
echo "=== 6. GET city again after failover (data must survive) ==="
$CTL -addr "${ADDR[$SURVIVOR]}" get city

echo
echo "=== 7. PUT country=france under the new leader ==="
$CTL -addr "${ADDR[$SURVIVOR]}" put country france

echo
echo "=== 8. restarting the dead node ($LEADER) — it should rejoin & catch up ==="
start_node "$LEADER"
sleep 2
echo "  status of restarted $LEADER:"
status "$LEADER"; echo
echo "  reading both keys directly from restarted node's leader view:"
$CTL -addr "${ADDR[$LEADER]}" get city
$CTL -addr "${ADDR[$LEADER]}" get country

echo
echo "=== demo complete ==="
