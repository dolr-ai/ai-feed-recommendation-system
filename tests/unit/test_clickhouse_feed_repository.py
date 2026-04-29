from src.core.settings import Settings
from src.repository.clickhouse_feed_repository import ClickHouseFeedRepository


class StubClickHouseClient:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.calls = []

    async def fetch_all(self, query, parameters=None):
        self.calls.append((query, parameters or {}))
        return self.rows


def build_settings(**overrides) -> Settings:
    return Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        **overrides,
    )


async def test_get_global_popular_videos_uses_validity_and_exclusion_filters():
    client = StubClickHouseClient(rows=[{"video_id": "v1", "global_popularity_score": 99.0}])
    repo = ClickHouseFeedRepository(client, build_settings(clickhouse_database="yral"))

    rows = await repo.get_global_popular_videos(limit=25)

    assert rows == [{"video_id": "v1", "global_popularity_score": 99.0}]
    query, params = client.calls[0]
    assert "FROM yral.global_popular_videos_l7d FINAL gpv" in query
    assert "FROM yral.video_unique_v2 FINAL vu" in query
    assert "FROM yral.excluded_videos FINAL" in query
    assert "FROM yral.ugc_content_approval FINAL" in query
    assert "LIMIT %(limit)s" in query
    assert params == {"limit": 25}


async def test_get_ugc_discovery_videos_uses_impression_and_age_parameters():
    client = StubClickHouseClient(rows=[{"video_id": "v2", "upload_timestamp": "2026-04-29", "impression_count": 10}])
    repo = ClickHouseFeedRepository(client, build_settings(clickhouse_database="yral"))

    await repo.get_ugc_discovery_videos(max_views=150, max_age_days=5, limit=100)

    query, params = client.calls[0]
    assert "LEFT JOIN yral.video_statistics FINAL vs" in query
    assert "coalesce(vs.total_impressions, 0) < %(max_views)s" in query
    assert "us.upload_timestamp >= now() - INTERVAL %(max_age_days)s DAY" in query
    assert "LIMIT %(limit)s" in query
    assert params == {"max_views": 150, "max_age_days": 5, "limit": 100}


async def test_get_following_video_candidates_uses_follower_graph_and_user_parameter():
    client = StubClickHouseClient(rows=[{"video_id": "v3", "global_popularity_score": None}])
    repo = ClickHouseFeedRepository(client, build_settings(clickhouse_database="yral"))

    await repo.get_following_video_candidates(user_id="user-123", num_videos=300)

    query, params = client.calls[0]
    assert "FROM yral.follower_graph FINAL" in query
    assert "WHERE follower_id = %(user_id)s" in query
    assert "LIMIT %(num_videos)s" in query
    assert params == {"user_id": "user-123", "num_videos": 300}


async def test_get_excluded_video_ids_flattens_video_id_rows():
    client = StubClickHouseClient(rows=[{"video_id": "a"}, {"video_id": "b"}, {"video_id": ""}])
    repo = ClickHouseFeedRepository(client, build_settings(clickhouse_database="yral"))

    rows = await repo.get_excluded_video_ids()

    assert rows == ["a", "b"]
