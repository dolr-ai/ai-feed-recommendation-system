"""
Test Dragonfly client utility for tests.

IMPORTANT: Tests must NEVER connect to production KVRocks.
This module provides a dedicated client for the test Dragonfly instance.

Algorithm:
    1. Load .env file to get TEST_DRAGONFLY_* environment variables
    2. Validate that required variables are set
    3. Create Redis client with test credentials
    4. Verify connection before returning

Usage:
    from test_dragonfly_client import get_test_redis_client, get_test_redis_config

    client = get_test_redis_client()
    config = get_test_redis_config()  # For async services
"""

import os
import redis
from dotenv import load_dotenv

load_dotenv()


def get_test_redis_config() -> dict:
    """
    Get test Dragonfly connection config from TEST_DRAGONFLY_* env vars.

    Algorithm:
        1. Read TEST_DRAGONFLY_HOST from environment
        2. Raise ValueError if not set (tests require test credentials)
        3. Read remaining config with sensible defaults
        4. Return dict suitable for Redis client or AsyncKVRocksService

    Returns:
        dict: Connection config with keys:
            - host: str
            - port: int
            - password: str or None
            - ssl_enabled: bool
            - cluster_enabled: bool

    Raises:
        ValueError: If TEST_DRAGONFLY_HOST not set
    """
    host = os.getenv("TEST_DRAGONFLY_HOST")
    if not host:
        raise ValueError("TEST_DRAGONFLY_HOST not set. Tests require test credentials.")

    return {
        "host": host,
        "port": int(os.getenv("TEST_DRAGONFLY_PORT", "6379")),
        "password": os.getenv("TEST_DRAGONFLY_PASSWORD"),
        "ssl_enabled": os.getenv("TEST_DRAGONFLY_TLS_ENABLED", "").lower() == "true",
        "cluster_enabled": os.getenv("TEST_DRAGONFLY_CLUSTER_ENABLED", "").lower() == "true",
    }


def get_test_redis_client() -> redis.Redis:
    """
    Get a synchronous Redis client connected to test Dragonfly.

    Algorithm:
        1. Get config from get_test_redis_config()
        2. Create redis.Redis client with config
        3. Verify connection with ping()
        4. Return connected client

    Returns:
        redis.Redis: Client connected to test Dragonfly instance

    Raises:
        ValueError: If TEST_DRAGONFLY_HOST not set
        redis.ConnectionError: If connection to test fails
    """
    config = get_test_redis_config()

    client = redis.Redis(
        host=config["host"],
        port=config["port"],
        password=config["password"],
        decode_responses=True,
        socket_timeout=10,
    )

    # Verify connection works before returning
    client.ping()
    return client


if __name__ == "__main__":
    """Quick test of test Dragonfly connection."""
    client = get_test_redis_client()
    print(f"Connected to test Dragonfly: {client.ping()}")
    info = client.info("server")
    print(f"Server version: {info.get('redis_version', 'unknown')}")
    print(f"Connected clients: {info.get('connected_clients', 'unknown')}")
