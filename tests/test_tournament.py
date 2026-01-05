"""
Tournament endpoint integration tests.

Uses FastAPI TestClient - no running server needed.
Uses TEST Dragonfly (TEST_DRAGONFLY_* env vars).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

from dotenv import load_dotenv
load_dotenv()

# Override KVROCKS_* env vars with TEST values BEFORE importing app
os.environ["KVROCKS_HOST"] = os.getenv("TEST_DRAGONFLY_HOST", "localhost")
os.environ["KVROCKS_PORT"] = os.getenv("TEST_DRAGONFLY_PORT", "6379")
os.environ["KVROCKS_PASSWORD"] = os.getenv("TEST_DRAGONFLY_PASSWORD", "")
os.environ["KVROCKS_TLS_ENABLED"] = os.getenv("TEST_DRAGONFLY_TLS_ENABLED", "false")
os.environ["KVROCKS_CLUSTER_ENABLED"] = os.getenv("TEST_DRAGONFLY_CLUSTER_ENABLED", "false")

import pytest
from fastapi.testclient import TestClient

from async_api_server import app


@pytest.fixture(scope="module")
def client():
    """
    FastAPI TestClient - runs app in-process, handles lifespan.

    No seeding needed for tournament tests since they register their own tournaments.
    """
    with TestClient(app) as c:
        yield c


class TestTournamentVideosWithMetadata:
    """Tests for GET /tournament/{id}/videos endpoint."""

    @pytest.mark.integration
    def test_get_tournament_videos_with_metadata(self, client):
        """
        Test that GET /tournament/{id}/videos?with_metadata=true returns
        video metadata (canister_id, post_id, publisher_user_id).

        Algorithm:
            1. Register a new tournament with 3 videos using admin key
            2. Call GET /tournament/{id}/videos?with_metadata=true
            3. Verify response status is 200
            4. Verify video_count is exactly 3 (metadata is pre-stored at registration)
            5. Verify each video has metadata fields (video_id, canister_id, post_id, publisher_user_id)

        Uses only 3 videos to minimize cooldown impact.
        """
        admin_key = os.getenv("ADMIN_API_KEY", "")
        if not admin_key:
            pytest.skip("ADMIN_API_KEY not set in environment")

        # 1. Register tournament with 3 videos
        register_resp = client.post(
            "/tournament/register",
            json={"video_count": 3},
            headers={"X-Admin-Key": admin_key}
        )

        if register_resp.status_code == 503:
            pytest.skip("Not enough videos in pool for tournament registration")

        assert register_resp.status_code == 200, f"Registration failed: {register_resp.json()}"
        tournament_id = register_resp.json()["tournament_id"]

        # 2. Get videos WITH metadata
        response = client.get(f"/tournament/{tournament_id}/videos?with_metadata=true")

        # 3. Assert response structure
        assert response.status_code == 200, f"Get videos failed: {response.json()}"
        data = response.json()
        assert data["tournament_id"] == tournament_id
        # video_count should be exactly 3 (metadata is pre-stored at registration)
        assert data["video_count"] == 3, f"Expected video_count=3, got {data['video_count']}"
        assert len(data["videos"]) == 3

        # 4. Verify each video has metadata fields
        for video in data["videos"]:
            assert "video_id" in video, "Missing video_id in metadata"
            assert "canister_id" in video, "Missing canister_id in metadata"
            assert "post_id" in video, "Missing post_id in metadata"
            assert "publisher_user_id" in video, "Missing publisher_user_id in metadata"

    @pytest.mark.integration
    def test_get_tournament_videos_without_metadata(self, client):
        """
        Test that GET /tournament/{id}/videos (without metadata) returns just video IDs.

        Algorithm:
            1. Register a new tournament with 3 videos
            2. Call GET /tournament/{id}/videos (no with_metadata param)
            3. Verify response contains list of video ID strings

        Uses only 3 videos to minimize cooldown impact.
        """
        admin_key = os.getenv("ADMIN_API_KEY", "")
        if not admin_key:
            pytest.skip("ADMIN_API_KEY not set in environment")

        # 1. Register tournament with 3 videos
        register_resp = client.post(
            "/tournament/register",
            json={"video_count": 3},
            headers={"X-Admin-Key": admin_key}
        )

        if register_resp.status_code == 503:
            pytest.skip("Not enough videos in pool for tournament registration")

        assert register_resp.status_code == 200, f"Registration failed: {register_resp.json()}"
        tournament_id = register_resp.json()["tournament_id"]

        # 2. Get videos WITHOUT metadata
        response = client.get(f"/tournament/{tournament_id}/videos")

        # 3. Assert response structure
        assert response.status_code == 200, f"Get videos failed: {response.json()}"
        data = response.json()
        assert data["tournament_id"] == tournament_id
        assert data["video_count"] == 3
        assert len(data["videos"]) == 3

        # 4. Verify videos are strings (not dicts with metadata)
        for video in data["videos"]:
            assert isinstance(video, str), f"Expected string video ID, got {type(video)}"

    @pytest.mark.integration
    def test_video_count_consistent_with_and_without_metadata(self, client):
        """
        Test that video_count is the same whether with_metadata is true or false.

        This ensures metadata is pre-stored at registration and no filtering occurs.

        Algorithm:
            1. Register a tournament with 3 videos
            2. Get videos without metadata, record video_count
            3. Get videos with metadata, record video_count
            4. Assert both counts are equal

        Uses only 3 videos to minimize cooldown impact.
        """
        admin_key = os.getenv("ADMIN_API_KEY", "")
        if not admin_key:
            pytest.skip("ADMIN_API_KEY not set in environment")

        # 1. Register tournament with 3 videos
        register_resp = client.post(
            "/tournament/register",
            json={"video_count": 3},
            headers={"X-Admin-Key": admin_key}
        )

        if register_resp.status_code == 503:
            pytest.skip("Not enough videos in pool for tournament registration")

        assert register_resp.status_code == 200, f"Registration failed: {register_resp.json()}"
        tournament_id = register_resp.json()["tournament_id"]

        # 2. Get videos WITHOUT metadata
        resp_without = client.get(f"/tournament/{tournament_id}/videos")
        assert resp_without.status_code == 200
        count_without = resp_without.json()["video_count"]

        # 3. Get videos WITH metadata
        resp_with = client.get(f"/tournament/{tournament_id}/videos?with_metadata=true")
        assert resp_with.status_code == 200
        count_with = resp_with.json()["video_count"]

        # 4. Assert counts are equal
        assert count_without == count_with, (
            f"video_count mismatch: without_metadata={count_without}, with_metadata={count_with}"
        )
        assert count_without == 3, f"Expected video_count=3, got {count_without}"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
