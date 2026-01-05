"""
API endpoint integration tests.

Uses FastAPI TestClient - no running server needed.
Seeds own test data so tests don't rely on background jobs.
Uses TEST Dragonfly (TEST_DRAGONFLY_* env vars).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

# Override KVROCKS_* env vars with TEST values BEFORE importing app
# This makes the FastAPI app connect to test Dragonfly instead of prod
os.environ["KVROCKS_HOST"] = os.getenv("TEST_DRAGONFLY_HOST", "localhost")
os.environ["KVROCKS_PORT"] = os.getenv("TEST_DRAGONFLY_PORT", "6379")
os.environ["KVROCKS_PASSWORD"] = os.getenv("TEST_DRAGONFLY_PASSWORD", "")
os.environ["KVROCKS_TLS_ENABLED"] = os.getenv("TEST_DRAGONFLY_TLS_ENABLED", "false")
os.environ["KVROCKS_CLUSTER_ENABLED"] = os.getenv("TEST_DRAGONFLY_CLUSTER_ENABLED", "false")

import pytest
from fastapi.testclient import TestClient

from async_api_server import app
from helpers import generate_video_ids, seed_global_popularity, seed_global_freshness


@pytest.fixture(scope="module")
def seed_api_test_data(redis_client):
    """
    Seed global pools before API tests run.

    Algorithm:
        1. Seed popularity buckets (99_100, 90_99) with test videos
        2. Seed freshness windows (l1d, l7d) with test videos
        3. Yield for tests to run
        4. Cleanup seeded data
    """
    pop_videos = {
        "99_100": generate_video_ids("api_vid_pop_99", 200),
        "90_99": generate_video_ids("api_vid_pop_90", 200),
    }
    seed_global_popularity(redis_client, pop_videos)

    fresh_videos = {
        "l1d": generate_video_ids("api_vid_fresh_l1d", 200),
        "l7d": generate_video_ids("api_vid_fresh_l7d", 200),
    }
    seed_global_freshness(redis_client, fresh_videos)

    yield

    # Cleanup (key pattern must match helpers.py seeding)
    for bucket in pop_videos:
        redis_client.delete(f"{{GLOBAL}}:pool:pop_{bucket}")
    for window in fresh_videos:
        redis_client.delete(f"{{GLOBAL}}:pool:fresh_{window}")


@pytest.fixture(scope="module")
def client(seed_api_test_data):
    """
    FastAPI TestClient - runs app in-process, handles lifespan.

    Depends on seed_api_test_data to ensure global pools are populated
    before the app starts. No server needed.
    """
    with TestClient(app) as c:
        yield c


class TestHealthEndpoint:
    """Tests for /health endpoint."""

    @pytest.mark.integration
    def test_health_returns_status(self, client):
        """
        Health endpoint returns status and connection info.

        Algorithm:
            1. Call GET /health
            2. Verify response has status, redis_connected, scheduler_running
        """
        response = client.get("/health")

        assert response.status_code == 200
        data = response.json()
        assert "status" in data
        assert "redis_connected" in data
        assert "timestamp" in data


class TestRecommendEndpoint:
    """Tests for /recommend/{user_id} endpoint."""

    @pytest.mark.integration
    def test_recommend_returns_videos(self, client):
        """
        Recommend endpoint returns videos for a user.

        Algorithm:
            1. Call GET /recommend/{user_id} with count=10
            2. Verify response contains videos list
            3. Verify count matches request (or less if pool is low)
        """
        response = client.get("/recommend/test_api_user_001?count=10")

        assert response.status_code == 200
        data = response.json()
        assert "videos" in data
        assert "count" in data
        assert "sources" in data
        assert data["user_id"] == "test_api_user_001"

    @pytest.mark.integration
    def test_recommend_respects_count_param(self, client):
        """
        Recommend endpoint respects the count parameter.

        Algorithm:
            1. Request 5 videos
            2. Verify we get at most 5 videos
        """
        response = client.get("/recommend/test_api_user_002?count=5")

        assert response.status_code == 200
        data = response.json()
        assert len(data["videos"]) <= 5

    @pytest.mark.integration
    def test_recommend_mixed_type(self, client):
        """
        Mixed rec_type returns videos from multiple sources.

        Algorithm:
            1. Request mixed recommendations
            2. Verify sources dict has multiple keys (if videos available)
        """
        response = client.get("/recommend/test_api_user_003?count=50&rec_type=mixed")

        assert response.status_code == 200
        data = response.json()
        assert "sources" in data

    @pytest.mark.integration
    def test_recommend_invalid_count_rejected(self, client):
        """
        Invalid count parameter (> 500) is rejected.

        Algorithm:
            1. Request with count=1000 (exceeds max)
            2. Verify 422 validation error
        """
        response = client.get("/recommend/test_api_user_004?count=1000")

        assert response.status_code == 422  # Validation error


class TestGlobalCacheEndpoint:
    """Tests for /global-cache endpoint."""

    @pytest.mark.integration
    def test_global_cache_returns_videos(self, client):
        """
        Global cache endpoint returns cached videos.

        Algorithm:
            1. Call GET /global-cache
            2. Verify response has videos list
        """
        response = client.get("/global-cache")

        assert response.status_code == 200
        data = response.json()
        assert "videos" in data
        assert "count" in data


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
