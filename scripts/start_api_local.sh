#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

export PYTHONPATH="$REPO_ROOT${PYTHONPATH:+:$PYTHONPATH}"
export SCHEDULER_ENABLED="${SCHEDULER_ENABLED:-false}"
export FEED_SYNC_RUN_ON_STARTUP="${FEED_SYNC_RUN_ON_STARTUP:-false}"
export DISCOVERY_BOOST_RUN_ON_STARTUP="${DISCOVERY_BOOST_RUN_ON_STARTUP:-false}"

HOST="${HOST:-0.0.0.0}"
PORT="${PORT:-8000}"

exec uvicorn src.core.app:app --host "$HOST" --port "$PORT"
