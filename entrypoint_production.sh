#!/bin/bash
# Production deployment script for high-concurrency recommendation system
# Optimized for 10,000+ concurrent users with request queuing
# Docker-compatible version

set -e

CPU_CORES=$(sysctl -n hw.ncpu 2>/dev/null || nproc 2>/dev/null || echo "2")
WORKER_COUNT=2

echo "=========================================="
echo "Starting Production Recommendation System"
echo "=========================================="
echo "CPU Cores detected: $CPU_CORES"
echo "Worker count: $WORKER_COUNT"
echo "Redis max connections per worker: 100 (hardcoded in config.py)"
echo "Redis semaphore size per worker: 80 (hardcoded in config.py)"
echo "System somaxconn: $(cat /proc/sys/net/core/somaxconn 2>/dev/null || sysctl -n kern.ipc.somaxconn 2>/dev/null || echo 'unknown')"
echo "=========================================="

export ENABLE_DEBUG_LOGGING=false
export ENABLE_LUA_SCRIPT_LOGGING=false
export LOG_LEVEL=WARNING

# Prometheus multiprocess mode for multiple workers
export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
rm -rf $PROMETHEUS_MULTIPROC_DIR
mkdir -p $PROMETHEUS_MULTIPROC_DIR

cd src

uvicorn async_api_server:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers $WORKER_COUNT \
    --loop uvloop \
    --log-level info \
    --access-log \
    --timeout-keep-alive 5 \
    --backlog 4000