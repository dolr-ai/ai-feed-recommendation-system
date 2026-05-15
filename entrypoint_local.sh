#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ ! -f "venv/bin/activate" ]]; then
    echo "Missing local virtualenv at $SCRIPT_DIR/venv" >&2
    echo "Create it and install requirements before running this entrypoint." >&2
    exit 1
fi

source venv/bin/activate

export PYTHONPATH="$SCRIPT_DIR${PYTHONPATH:+:$PYTHONPATH}"

tcp_ready() {
    python - "$1" "$2" >/dev/null 2>&1 <<'PY'
import socket
import sys

with socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=1):
    pass
PY
}

KVROCKS_CONTAINER_NAME="${KVROCKS_CONTAINER_NAME:-kvrocks-local}"
KVROCKS_IMAGE="${KVROCKS_IMAGE:-apache/kvrocks}"
KVROCKS_HOST_PORT="${KVROCKS_HOST_PORT:-6666}"
KVROCKS_CONTAINER_PORT="${KVROCKS_CONTAINER_PORT:-6666}"

if ! command -v docker >/dev/null 2>&1; then
    echo "docker is required to start local KVRocks" >&2
    exit 1
fi

if docker ps --format '{{.Names}}' | grep -Fxq "$KVROCKS_CONTAINER_NAME"; then
    echo "Using running KVRocks container: $KVROCKS_CONTAINER_NAME"
elif docker ps -a --format '{{.Names}}' | grep -Fxq "$KVROCKS_CONTAINER_NAME"; then
    echo "Starting existing KVRocks container: $KVROCKS_CONTAINER_NAME"
    docker start "$KVROCKS_CONTAINER_NAME" >/dev/null
else
    echo "Creating KVRocks container: $KVROCKS_CONTAINER_NAME"
    docker run -d \
        --name "$KVROCKS_CONTAINER_NAME" \
        -p "$KVROCKS_HOST_PORT:$KVROCKS_CONTAINER_PORT" \
        "$KVROCKS_IMAGE" >/dev/null
fi

echo "Waiting for KVRocks on 127.0.0.1:$KVROCKS_HOST_PORT"
kvrocks_ready=false
for _ in {1..30}; do
    if python - "$KVROCKS_HOST_PORT" >/dev/null 2>&1 <<'PY'
import socket
import sys

with socket.create_connection(("127.0.0.1", int(sys.argv[1])), timeout=1) as sock:
    sock.sendall(b"*1\r\n$4\r\nPING\r\n")
    data = sock.recv(16)
    if not data.startswith(b"+PONG"):
        raise SystemExit(1)
PY
    then
        kvrocks_ready=true
        break
    fi
    sleep 1
done

if [[ "$kvrocks_ready" != "true" ]]; then
    echo "KVRocks did not become ready on 127.0.0.1:$KVROCKS_HOST_PORT" >&2
    docker logs --tail 50 "$KVROCKS_CONTAINER_NAME" >&2 || true
    exit 1
fi

export KVROCKS_HOST="${KVROCKS_HOST:-localhost}"
export KVROCKS_PORT="${KVROCKS_PORT:-$KVROCKS_HOST_PORT}"
export KVROCKS_PASSWORD="${KVROCKS_PASSWORD:-}"
export KVROCKS_TLS_ENABLED="${KVROCKS_TLS_ENABLED:-false}"
export KVROCKS_CLUSTER_ENABLED="${KVROCKS_CLUSTER_ENABLED:-false}"

CLICKHOUSE_TUNNEL_ENABLED="${CLICKHOUSE_TUNNEL_ENABLED:-true}"
CLICKHOUSE_TUNNEL_SSH_TARGET="${CLICKHOUSE_TUNNEL_SSH_TARGET:-ansuman@ansuman-1}"
CLICKHOUSE_TUNNEL_LOCAL_PORT="${CLICKHOUSE_TUNNEL_LOCAL_PORT:-18443}"
CLICKHOUSE_TUNNEL_REMOTE_HOST="${CLICKHOUSE_TUNNEL_REMOTE_HOST:-127.0.0.1}"
CLICKHOUSE_TUNNEL_REMOTE_PORT="${CLICKHOUSE_TUNNEL_REMOTE_PORT:-8443}"

export CLICKHOUSE_HOST="${CLICKHOUSE_HOST:-127.0.0.1}"
export CLICKHOUSE_PORT="${CLICKHOUSE_PORT:-$CLICKHOUSE_TUNNEL_LOCAL_PORT}"
export CLICKHOUSE_SECURE="${CLICKHOUSE_SECURE:-true}"
export CLICKHOUSE_VERIFY="${CLICKHOUSE_VERIFY:-false}"

if [[ "$CLICKHOUSE_TUNNEL_ENABLED" == "true" \
    && "$CLICKHOUSE_HOST" == "127.0.0.1" \
    && "$CLICKHOUSE_PORT" == "$CLICKHOUSE_TUNNEL_LOCAL_PORT" ]]; then
    if tcp_ready "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT"; then
        echo "Using existing ClickHouse tunnel on $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
    else
        if ! command -v ssh >/dev/null 2>&1; then
            echo "ssh is required to open the local ClickHouse tunnel" >&2
            exit 1
        fi
        echo "Opening ClickHouse tunnel: $CLICKHOUSE_HOST:$CLICKHOUSE_PORT -> $CLICKHOUSE_TUNNEL_SSH_TARGET:$CLICKHOUSE_TUNNEL_REMOTE_HOST:$CLICKHOUSE_TUNNEL_REMOTE_PORT"
        ssh -f -N \
            -o ExitOnForwardFailure=yes \
            -L "$CLICKHOUSE_TUNNEL_LOCAL_PORT:$CLICKHOUSE_TUNNEL_REMOTE_HOST:$CLICKHOUSE_TUNNEL_REMOTE_PORT" \
            "$CLICKHOUSE_TUNNEL_SSH_TARGET"
    fi

    echo "Waiting for ClickHouse tunnel on $CLICKHOUSE_HOST:$CLICKHOUSE_PORT"
    clickhouse_ready=false
    for _ in {1..30}; do
        if tcp_ready "$CLICKHOUSE_HOST" "$CLICKHOUSE_PORT"; then
            clickhouse_ready=true
            break
        fi
        sleep 1
    done

    if [[ "$clickhouse_ready" != "true" ]]; then
        echo "ClickHouse tunnel did not become ready on $CLICKHOUSE_HOST:$CLICKHOUSE_PORT" >&2
        exit 1
    fi
fi

export SCHEDULER_ENABLED="${SCHEDULER_ENABLED:-true}"
export FEED_SYNC_RUN_ON_STARTUP="${FEED_SYNC_RUN_ON_STARTUP:-true}"
export DISCOVERY_REFRESH_RUN_ON_STARTUP="${DISCOVERY_REFRESH_RUN_ON_STARTUP:-false}"
export FEED_RECSYS_JOBS_ENABLED="${FEED_RECSYS_JOBS_ENABLED:-true}"
export FEED_RECSYS_JOB_RUN_ON_STARTUP="${FEED_RECSYS_JOB_RUN_ON_STARTUP:-true}"
export INTERNAL_REQUEST_HMAC_SECRET="${INTERNAL_REQUEST_HMAC_SECRET:-local-dev-secret}"

HOST="${HOST:-${APP_HOST:-0.0.0.0}}"
PORT="${PORT:-${APP_PORT:-8000}}"
LOG_LEVEL="${LOG_LEVEL:-info}"

exec uvicorn src.main:app \
    --host "$HOST" \
    --port "$PORT" \
    --log-level "$LOG_LEVEL" \
    --reload
