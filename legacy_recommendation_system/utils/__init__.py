"""
Utils package for recsys-on-premise

This package contains utility modules for the recommendation system.
"""

from .redis_utils import KVRocksService, get_logger

__all__ = [
    "KVRocksService",
    "get_logger",
]

__version__ = "0.1.0"
