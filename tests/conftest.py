"""
Pytest configuration for recommendation system tests.

IMPORTANT: Tests use TEST Dragonfly (TEST_DRAGONFLY_*), never production KVRocks.
This ensures tests cannot pollute or corrupt production data.

No mocks, no factories - just real test Dragonfly connections.
"""

import os
import uuid
import tempfile
import pytest
import redis
from redis.cluster import RedisCluster

# Load .env file if it exists
from dotenv import load_dotenv
load_dotenv()


def _write_pem_to_temp(content: str) -> str:
    """Write PEM content to temp file, return path."""
    fd, path = tempfile.mkstemp(suffix=".pem")
    os.write(fd, content.encode("utf-8"))
    os.close(fd)
    return path


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: requires Redis connection")


@pytest.fixture(scope="session")
def redis_client():
    """
    Session-scoped Redis client using TEST Dragonfly credentials.

    IMPORTANT: Uses TEST_DRAGONFLY_* env vars, never production KVRocks.
    Skips all tests if test Dragonfly is unavailable.
    """
    # IMPORTANT: Tests use TEST Dragonfly, never production KVRocks
    host = os.getenv("TEST_DRAGONFLY_HOST")
    if not host:
        pytest.skip("TEST_DRAGONFLY_HOST not set. Tests require test credentials.")

    port = int(os.getenv("TEST_DRAGONFLY_PORT", "6379"))
    password = os.getenv("TEST_DRAGONFLY_PASSWORD")
    tls_enabled = os.getenv("TEST_DRAGONFLY_TLS_ENABLED", "").lower() == "true"
    cluster_enabled = os.getenv("TEST_DRAGONFLY_CLUSTER_ENABLED", "").lower() == "true"

    connection_kwargs = {
        "host": host,
        "port": port,
        "password": password,
        "decode_responses": True,
        "socket_timeout": 10,
    }

    if tls_enabled:
        connection_kwargs["ssl"] = True
        connection_kwargs["ssl_cert_reqs"] = None

    try:
        if cluster_enabled:
            client = RedisCluster(**connection_kwargs)
        else:
            client = redis.Redis(**connection_kwargs)
        client.ping()
    except redis.ConnectionError as e:
        pytest.skip(f"Test Dragonfly not available: {e}")
    except Exception as e:
        pytest.skip(f"Test Dragonfly connection error: {e}")

    yield client
    client.close()


@pytest.fixture(scope="session")
def redis_config():
    """
    Return TEST Dragonfly connection config from environment.

    IMPORTANT: Uses TEST_DRAGONFLY_* env vars, never production KVRocks.

    Returns dict with host, port, password, ssl_enabled, cluster_enabled.
    No mTLS needed for test Dragonfly.
    """
    host = os.getenv("TEST_DRAGONFLY_HOST")
    if not host:
        pytest.skip("TEST_DRAGONFLY_HOST not set. Tests require test credentials.")

    return {
        "host": host,
        "port": int(os.getenv("TEST_DRAGONFLY_PORT", "6379")),
        "password": os.getenv("TEST_DRAGONFLY_PASSWORD"),
        "ssl_enabled": os.getenv("TEST_DRAGONFLY_TLS_ENABLED", "").lower() == "true",
        "cluster_enabled": os.getenv("TEST_DRAGONFLY_CLUSTER_ENABLED", "").lower() == "true",
        # No mTLS needed for test Dragonfly
        "ssl_ca_certs": None,
        "ssl_certfile": None,
        "ssl_keyfile": None,
    }


@pytest.fixture
def test_user_id():
    """
    Generate unique test user ID for isolation.

    Returns:
        str: User ID in format 'test_user_{8-char-uuid}'
    """
    return f"test_user_{uuid.uuid4().hex[:8]}"


@pytest.fixture
def cleanup_user(redis_client, test_user_id):
    """
    Auto-cleanup fixture that deletes test user keys after test.

    Usage: Include this fixture in tests that create user data.
    """
    yield test_user_id

    # Cleanup: delete all keys for this test user
    pattern = f"user:{test_user_id}:*"
    keys = redis_client.keys(pattern)
    if keys:
        redis_client.delete(*keys)
