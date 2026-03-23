#!/bin/sh
set -e

exec uvicorn src.core.app:app --host 0.0.0.0 --port 8000 --workers 1
