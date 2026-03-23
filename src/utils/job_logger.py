import asyncio
import json
import logging
from datetime import datetime, timezone


class RedisJobLogHandler(logging.Handler):
    def __init__(self, redis_client, job_name: str):
        super().__init__()
        self._redis_client = redis_client
        self._job_name = job_name

    def emit(self, record: logging.LogRecord) -> None:
        try:
            payload = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "logger": record.name,
                "message": record.getMessage(),
            }
            loop = asyncio.get_running_loop()
            loop.create_task(
                self._redis_client.rpush(f"job_logs:{self._job_name}", json.dumps(payload))
            )
        except Exception:
            self.handleError(record)


def get_job_logger(name: str, redis_client) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.addHandler(RedisJobLogHandler(redis_client, name))
    return logger
