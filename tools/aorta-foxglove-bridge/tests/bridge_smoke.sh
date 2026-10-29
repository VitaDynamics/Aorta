#!/usr/bin/env bash
#
# Smoke test for aorta-foxglove-bridge.
#
# Quick: spawn the bridge, send a Foxglove WebSocket handshake
# (subprotocol `foxglove.sdk.v1`), assert HTTP 101 Switching Protocols
# AND the `serverInfo` frame round-trips.
#
# Full: in addition, spawn cli_talker for ~10s and assert the bridge
# still serves the handshake (no crash on live data).

set -euo pipefail

BRIDGE_BIN=""
TALKER_BIN=""
PROFILE="quick"

for arg in "$@"; do
    case "$arg" in
        --bridge-bin=*) BRIDGE_BIN="${arg#*=}" ;;
        --talker-bin=*) TALKER_BIN="${arg#*=}" ;;
        --profile=*) PROFILE="${arg#*=}" ;;
        *) echo "unknown arg: $arg" >&2; exit 2 ;;
    esac
done

if [[ -z "$BRIDGE_BIN" ]]; then
    echo "usage: $0 --bridge-bin=<path> [--talker-bin=<path>] [--profile=quick|full]" >&2
    exit 2
fi

case "$PROFILE" in
    quick) WINDOW=3 ;;
    full)  WINDOW=10 ;;
    *) echo "unknown profile: $PROFILE" >&2; exit 2 ;;
esac

PORT="$((40000 + RANDOM % 10000))"

cleanup() {
    if [[ -n "${BRIDGE_PID:-}" ]]; then kill "$BRIDGE_PID" 2>/dev/null || true; fi
    if [[ -n "${TALKER_PID:-}" ]]; then kill "$TALKER_PID" 2>/dev/null || true; fi
}
trap cleanup EXIT

"$BRIDGE_BIN" --group default --host 127.0.0.1 --port "$PORT" --log-level debug \
    >/tmp/aorta-foxglove-bridge-smoke.$$.log 2>&1 &
BRIDGE_PID=$!

sleep 1

if [[ "$PROFILE" == "full" && -n "$TALKER_BIN" ]]; then
    "$TALKER_BIN" --topic /demo/chatter &
    TALKER_PID=$!
fi

# Wait until either the WS port is up or we hit the window.
#
# `-o "$RESPONSE_FILE"` is load-bearing: with stdout piped, curl buffers the
# upgraded-connection bytes and the `--max-time` kill discards the buffer, so
# the serverInfo frame never reaches the capture (observed on macOS curl).
# Writing to a file flushes per-chunk. `grep -a` because the WS frame header
# bytes make the capture binary.
RESPONSE_FILE="$(mktemp)"
trap 'rm -f "$RESPONSE_FILE"; cleanup' EXIT
deadline=$(($(date +%s) + WINDOW))
ok=0
while [[ $(date +%s) -lt $deadline ]]; do
    curl -s -i --max-time 1 \
        -H "Connection: Upgrade" \
        -H "Upgrade: websocket" \
        -H "Sec-WebSocket-Version: 13" \
        -H "Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==" \
        -H "Sec-WebSocket-Protocol: foxglove.sdk.v1" \
        -o "$RESPONSE_FILE" \
        "http://127.0.0.1:$PORT/" || true
    if grep -aq "101 Switching Protocols" "$RESPONSE_FILE" \
        && grep -aq '"op":"serverInfo"' "$RESPONSE_FILE"; then
        ok=1
        break
    fi
    sleep 0.25
done

if [[ "$ok" != "1" ]]; then
    echo "FAIL: did not see Foxglove serverInfo handshake on port $PORT within ${WINDOW}s" >&2
    echo "--- bridge log ---" >&2
    cat /tmp/aorta-foxglove-bridge-smoke.$$.log >&2 || true
    exit 1
fi

if ! kill -0 "$BRIDGE_PID" 2>/dev/null; then
    echo "FAIL: bridge died after handshake" >&2
    exit 1
fi

# Graceful SIGTERM: systemd's default KillSignal is SIGTERM, so an unhandled
# one would terminate the bridge outright — no subscriber teardown, no
# forwarder flush, no run summary. Assert the signal takes the same graceful
# path SIGINT does.
kill -TERM "$BRIDGE_PID"

deadline=$(($(date +%s) + 10))
while kill -0 "$BRIDGE_PID" 2>/dev/null && [[ $(date +%s) -lt $deadline ]]; do
    sleep 0.25
done

if kill -0 "$BRIDGE_PID" 2>/dev/null; then
    echo "FAIL: bridge still alive 10s after SIGTERM" >&2
    echo "--- bridge log ---" >&2
    cat /tmp/aorta-foxglove-bridge-smoke.$$.log >&2 || true
    exit 1
fi

bridge_status=0
wait "$BRIDGE_PID" || bridge_status=$?
BRIDGE_PID=""

if [[ "$bridge_status" != "0" ]]; then
    echo "FAIL: bridge exited with status $bridge_status after SIGTERM (expected a clean 0)" >&2
    echo "--- bridge log ---" >&2
    cat /tmp/aorta-foxglove-bridge-smoke.$$.log >&2 || true
    exit 1
fi

if ! grep -aq "Stopping bridge (SIGTERM)" /tmp/aorta-foxglove-bridge-smoke.$$.log \
    || ! grep -aq "bridge summary:" /tmp/aorta-foxglove-bridge-smoke.$$.log; then
    echo "FAIL: SIGTERM did not take the graceful shutdown path (no signal log / no run summary)" >&2
    echo "--- bridge log ---" >&2
    cat /tmp/aorta-foxglove-bridge-smoke.$$.log >&2 || true
    exit 1
fi

echo "OK: bridge advertised foxglove.sdk.v1 with serverInfo on port $PORT (profile=$PROFILE)"
echo "OK: SIGTERM shut the bridge down gracefully (summary emitted, exit 0)"
