"""
recsys-on-premise source package

This is the main source package containing all components for the
recommendation system with self-hosted components.
"""

__version__ = "0.1.0"
__author__ = "Yral Team"
__email__ = "team@yral.com"

# Import main utilities for easy access
from .utils import DragonflyService, get_logger

__all__ = [
    "DragonflyService",
    "get_logger",
]
