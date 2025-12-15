"""
Custom logging handler for capturing background job logs to Redis.

This module provides a Redis-based logging system for background jobs,
allowing logs to be captured and retrieved via API endpoints.
"""

import logging
import json
import time
import asyncio
from typing import Optional, List, Dict
from datetime import datetime


class RedisJobLogHandler(logging.Handler):
    """
    Custom logging handler that captures logs to Redis.

    Stores logs as a list in Redis with the following structure:
    - Key: job:logs:{job_name}
    - Value: List of JSON log entries (newest first)
    - TTL: 24 hours

    Algorithm:
        1. Format log records as JSON with metadata
        2. Store in Redis list (LPUSH for newest first)
        3. Trim list to max size to prevent unbounded growth
        4. Set TTL to auto-cleanup old logs
    """

    def __init__(self, redis_client, job_name: str, max_logs: int = 1000):
        """
        Initialize the Redis log handler.

        Args:
            redis_client: Async Redis client instance
            job_name: Name of the job for log grouping
            max_logs: Maximum number of log entries to retain
        """
        super().__init__()
        self.redis_client = redis_client
        self.job_name = job_name
        self.max_logs = max_logs
        self.log_key = f"job:logs:{job_name}"

    def emit(self, record):
        """
        Emit a log record to Redis.

        This method is called by the logging system for each log record.
        Since we're in an async context, we use asyncio.create_task to
        handle the async Redis operation without blocking.

        Args:
            record: LogRecord instance to be logged
        """
        try:
            # Format log entry with metadata
            log_entry = {
                "timestamp": record.created,
                "timestamp_formatted": datetime.fromtimestamp(record.created).isoformat(),
                "level": record.levelname,
                "message": self.format(record),
                "module": record.module,
                "function": record.funcName,
                "line": record.lineno,
                "job_name": self.job_name
            }

            # Store in Redis asynchronously
            # Check if we're in an async context
            try:
                loop = asyncio.get_running_loop()
                # We're in an async context, create a task
                loop.create_task(self._store_log(log_entry))
            except RuntimeError:
                # No async loop running, use asyncio.run
                # This shouldn't happen in our use case but handle it
                asyncio.run(self._store_log(log_entry))

        except Exception:
            self.handleError(record)

    async def _store_log(self, log_entry: Dict):
        """
        Store log entry in Redis asynchronously.

        Algorithm:
            1. Serialize log entry to JSON
            2. Add to Redis list (newest first with LPUSH)
            3. Trim list to maintain max size
            4. Set TTL for automatic cleanup

        Args:
            log_entry: Dictionary containing log data
        """
        try:
            # Serialize to JSON
            json_log = json.dumps(log_entry)

            # Add to list (newest first)
            await self.redis_client.lpush(self.log_key, json_log)

            # Trim to max size (keep newest entries)
            await self.redis_client.ltrim(self.log_key, 0, self.max_logs - 1)

            # Set TTL to 24 hours
            await self.redis_client.expire(self.log_key, 86400)

        except Exception as e:
            # Fallback to console if Redis fails
            print(f"Failed to store log in Redis: {e}")
            print(f"Log entry: {log_entry}")


def get_job_logger(job_name: str, redis_client, level: str = "INFO") -> logging.Logger:
    """
    Get a logger configured to capture logs to both Redis and console.

    This function returns a logger that:
    1. Sends logs to Redis for API retrieval
    2. Also outputs to console for traditional monitoring
    3. Doesn't propagate to avoid duplicate logs

    Args:
        job_name: Name of the job (used as logger name and Redis key)
        redis_client: Async Redis client for log storage
        level: Logging level (default: INFO)

    Returns:
        Configured logger instance

    Algorithm:
        1. Get or create logger for the job
        2. Add Redis handler if not present
        3. Add console handler if not present
        4. Set level and disable propagation
    """
    logger = logging.getLogger(f"job.{job_name}")

    # Clear existing handlers to avoid duplicates
    logger.handlers.clear()

    # Add Redis handler for API access
    redis_handler = RedisJobLogHandler(redis_client, job_name)
    redis_handler.setFormatter(logging.Formatter('%(message)s'))
    redis_handler.setLevel(getattr(logging, level))
    logger.addHandler(redis_handler)

    # Add console handler for traditional logging
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        '%(asctime)s - JOB[%(name)s] - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(getattr(logging, level))
    logger.addHandler(console_handler)

    # Set logger level and prevent propagation
    logger.setLevel(getattr(logging, level))
    logger.propagate = False  # Don't propagate to root logger

    return logger


async def get_job_logs(
    redis_client,
    job_name: str,
    limit: int = 100
) -> List[Dict]:
    """
    Retrieve recent logs for a job from Redis.

    Args:
        redis_client: Async Redis client
        job_name: Name of the job
        limit: Maximum number of logs to retrieve

    Returns:
        List of log entries as dictionaries

    Algorithm:
        1. Fetch raw logs from Redis list
        2. Parse JSON entries
        3. Return parsed logs (newest first)
    """
    log_key = f"job:logs:{job_name}"
    raw_logs = await redis_client.lrange(log_key, 0, limit - 1)

    logs = []
    for raw_log in raw_logs:
        try:
            # Parse JSON log entry
            if isinstance(raw_log, bytes):
                raw_log = raw_log.decode('utf-8')
            log_entry = json.loads(raw_log)
            logs.append(log_entry)
        except json.JSONDecodeError as e:
            # Skip malformed entries
            continue

    return logs


async def clear_job_logs(redis_client, job_name: str) -> bool:
    """
    Clear all logs for a specific job.

    Args:
        redis_client: Async Redis client
        job_name: Name of the job

    Returns:
        True if logs were cleared, False otherwise

    Algorithm:
        1. Delete the Redis key containing logs
        2. Return success status
    """
    log_key = f"job:logs:{job_name}"
    result = await redis_client.delete(log_key)
    return bool(result)


async def get_job_log_stats(redis_client, job_name: str) -> Dict:
    """
    Get statistics about job logs.

    Args:
        redis_client: Async Redis client
        job_name: Name of the job

    Returns:
        Dictionary with log statistics

    Algorithm:
        1. Get total log count
        2. Get TTL remaining
        3. Get most recent log timestamp
        4. Return stats dictionary
    """
    log_key = f"job:logs:{job_name}"

    # Get total count
    total_logs = await redis_client.llen(log_key)

    # Get TTL
    ttl = await redis_client.ttl(log_key)

    # Get most recent log
    recent_log = None
    if total_logs > 0:
        raw_log = await redis_client.lindex(log_key, 0)
        try:
            if isinstance(raw_log, bytes):
                raw_log = raw_log.decode('utf-8')
            recent_log = json.loads(raw_log)
        except json.JSONDecodeError:
            pass

    return {
        "job_name": job_name,
        "total_logs": total_logs,
        "ttl_seconds": ttl,
        "ttl_hours": round(ttl / 3600, 2) if ttl > 0 else 0,
        "most_recent_log": recent_log["timestamp_formatted"] if recent_log else None,
        "most_recent_message": recent_log["message"] if recent_log else None
    }


if __name__ == "__main__":
    # Test the module
    import asyncio
    from utils.async_redis_utils import AsyncDragonflyService

    async def test_job_logger():
        """Test the job logging functionality."""
        # Initialize Redis
        service = AsyncDragonflyService()
        await service.connect()
        client = service.client

        # Get a job logger
        job_logger = get_job_logger("test_job", client)

        # Log some messages
        job_logger.info("Starting test job")
        job_logger.debug("Debug message (should not appear with INFO level)")
        job_logger.warning("This is a warning")
        job_logger.error("This is an error")

        # Wait a bit for async operations
        await asyncio.sleep(1)

        # Retrieve logs
        logs = await get_job_logs(client, "test_job", limit=10)
        print(f"\nRetrieved {len(logs)} logs:")
        for log in logs:
            print(f"  [{log['level']}] {log['message']}")

        # Get stats
        stats = await get_job_log_stats(client, "test_job")
        print(f"\nLog stats: {stats}")

        # Clear logs
        cleared = await clear_job_logs(client, "test_job")
        print(f"\nLogs cleared: {cleared}")

        await service.close()

    asyncio.run(test_job_logger())