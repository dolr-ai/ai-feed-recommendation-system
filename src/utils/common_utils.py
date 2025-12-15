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
