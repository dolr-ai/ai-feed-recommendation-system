import json
import logging
from datetime import datetime, timezone
from typing import Optional

from src.utils.job_logger import RedisJobLogHandler


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in {
                "name",
                "msg",
                "args",
                "levelname",
                "levelno",
                "pathname",
                "filename",
                "module",
                "exc_info",
                "exc_text",
                "stack_info",
                "lineno",
                "funcName",
                "created",
                "msecs",
                "relativeCreated",
                "thread",
                "threadName",
                "processName",
                "process",
            }:
                continue
            if value is None:
                continue
            payload[key] = value
        return json.dumps(payload, default=str)


class LoggerService:
    _instance: Optional["LoggerService"] = None

    def __new__(cls) -> "LoggerService":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._configured = False
        return cls._instance

    def configure(self, log_level: str = "INFO") -> None:
        if self._configured:
            return
        handler = logging.StreamHandler()
        handler.setFormatter(JsonFormatter())
        logging.root.setLevel(log_level)
        logging.root.handlers.clear()
        logging.root.addHandler(handler)
        logging.getLogger("httpx").setLevel(logging.WARNING)
        logging.getLogger("httpcore").setLevel(logging.WARNING)
        self._configured = True

    def get(self, name: str) -> logging.Logger:
        return logging.getLogger(f"influencer.{name}")

    def attach_redis_sink(self, name: str, redis_client) -> logging.Logger:
        logger = self.get(name)
        logger.addHandler(RedisJobLogHandler(redis_client, name))
        return logger
