import functools
import logging
import os
from datetime import datetime
import pathlib
from typing import Union


# Set up logging with environment variable
log_level_str = os.environ.get("LOG_LEVEL", "INFO")
log_level = getattr(logging, log_level_str, logging.INFO)


def get_logger(name: str) -> logging.Logger:
    """Simple logger function"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(log_level)
    return logger


def time_execution(func):
    """
    Decorator to time the execution of a function.
    Prints execution time but returns only the original result.
    """
    logger = get_logger(__name__)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = datetime.now()
        result = func(*args, **kwargs)
        elapsed_time = datetime.now() - start_time
        logger.debug(f"{func.__name__} completed in {elapsed_time}")
        return result

    return wrapper


def path_exists(path: Union[pathlib.Path, str]) -> bool:
    """
    Check if a path exists. Accepts both pathlib.Path and string paths.

    Args:
        path: pathlib.Path or string representing the path to check.

    Returns:
        True if the path exists, False otherwise.
    """
    # Handle empty string case
    if isinstance(path, str) and path == "":
        return False

    path_obj = pathlib.Path(path) if isinstance(path, str) else path
    return path_obj.exists()


def filter_transient_errors(event, hint):
    """
    Sentry before_send hook to filter out known transient errors.

    Algorithm:
        1. Check if the hint contains exception info (raised exceptions)
        2. If so, filter known transient exception types and messages:
           - TimeoutError: Redis connection timeouts during high load
           - ConnectionResetError: Transient network resets
           - Refill lock conflicts: Concurrent request race condition
           - Bloom filter race: Key already exists during initialization
        3. Also check event message/logentry for logger-captured events
           (Sentry logging integration captures logger.error() calls without
           exc_info, so they bypass the exception check above)
        4. Return None to drop the event, or the event to send it

    Args:
        event: Sentry event dict containing error details
        hint: Dict containing exc_info tuple (type, value, traceback)

    Returns:
        event dict if error should be sent to Sentry, None to drop it
    """
    TRANSIENT_PATTERNS = [
        "refill lock already held",
        "key already exists",
    ]

    if "exc_info" in hint:
        exc_type, exc_value, _ = hint["exc_info"]
        error_message = str(exc_value).lower()

        # Filter timeout errors (Redis connection timeouts)
        if exc_type.__name__ in ("TimeoutError", "ConnectionResetError"):
            return None

        # Filter known transient error messages
        if any(pattern in error_message for pattern in TRANSIENT_PATTERNS):
            return None

    # Safety net: also check event message for logger-captured events
    # (logger.error calls are captured by Sentry logging integration
    # without exc_info, so the check above misses them)
    message = (
        event.get("logentry", {}).get("message", "")
        or event.get("message", "")
        or ""
    ).lower()
    if any(pattern in message for pattern in TRANSIENT_PATTERNS):
        return None

    return event
