"""
Pytest configuration for recommendation system tests.

Uses stage Redis from environment variables (DRAGONFLY_HOST, DRAGONFLY_PORT, DRAGONFLY_PASSWORD).
No mocks, no factories - just real Redis connections.
"""

import os
import uuid
import pytest
import redis

# Load .env file if it exists
from dotenv import load_dotenv
load_dotenv()


def pytest_configure(config):
    """Register custom markers."""
    config.addinivalue_line("markers", "integration: requires Redis connection")


@pytest.fixture(scope="session")
def redis_client():
    """
    Session-scoped Redis client using STAGE credentials from env.

    Reads connection params from STAGE_DRAGONFLY_* env vars.
    Stage Redis does not require TLS.
    Skips all tests if Redis is unavailable.
    """
    tls_enabled = os.getenv("STAGE_DRAGONFLY_TLS_ENABLED", "false").lower() == "true"

    connection_kwargs = {
        "host": os.getenv("STAGE_DRAGONFLY_HOST", "localhost"),
        "port": int(os.getenv("STAGE_DRAGONFLY_PORT", "6379")),
        "password": os.getenv("STAGE_DRAGONFLY_PASSWORD"),
        "decode_responses": True,
        "socket_timeout": 10,
    }

    if tls_enabled:
        connection_kwargs["ssl"] = True
        connection_kwargs["ssl_cert_reqs"] = None

    try:
        client = redis.Redis(**connection_kwargs)
        client.ping()
    except redis.ConnectionError as e:
        pytest.skip(f"Redis not available: {e}")
    except Exception as e:
        pytest.skip(f"Redis connection error: {e}")

    yield client
    client.close()


@pytest.fixture(scope="session")
def redis_config():
    """
    Return STAGE Redis connection config from environment.

    Returns dict with host, port, password, ssl_enabled for creating services.
    Stage Redis does not require TLS.
    """
    return {
        "host": os.getenv("STAGE_DRAGONFLY_HOST", "localhost"),
        "port": int(os.getenv("STAGE_DRAGONFLY_PORT", "6379")),
        "password": os.getenv("STAGE_DRAGONFLY_PASSWORD"),
        "ssl_enabled": os.getenv("STAGE_DRAGONFLY_TLS_ENABLED", "false").lower() == "true",
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
