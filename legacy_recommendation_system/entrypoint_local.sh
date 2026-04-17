set -a
source venv/bin/activate

export PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc
rm -rf $PROMETHEUS_MULTIPROC_DIR
mkdir -p $PROMETHEUS_MULTIPROC_DIR

lsof -ti :8000 | xargs kill -9 2>/dev/null || true

cd legacy_recommendation_system
uvicorn async_api_server:app \
    --host 0.0.0.0 \
    --port 8000 \
    --workers 2 \
    --log-level info \
    --env-file ../.env