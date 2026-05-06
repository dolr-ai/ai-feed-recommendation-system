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
    assert "FROM yral.global_popular_videos_l7d AS gpv" in query
    assert "FROM yral.video_unique_v2 AS vu FINAL" in query
    assert "FROM yral.excluded_videos FINAL" in query
    assert "FROM yral.ugc_content_approval FINAL" in query
    assert "LIMIT %(limit)s" in query
    assert params == {"limit": 25}


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


async def test_get_following_status_batch_uses_non_final_argmax_query_and_dense_defaults():
    client = StubClickHouseClient(rows=[{"following_id": "publisher-1", "is_following": 1}])
    repo = ClickHouseFeedRepository(client, build_settings(clickhouse_database="yral"))

    result = await repo.get_following_status_batch(
        viewer_user_id="viewer-1",
        publisher_ids=["publisher-1", "publisher-2"],
    )

    assert result == {
        "publisher-1": True,
        "publisher-2": False,
    }
    query, params = client.calls[0]
    assert "FROM yral.follower_graph" in query
    assert "FINAL" not in query
    assert "argMax(active, tuple(last_updated_timestamp, _updated_at))" in query
    assert params == {
        "viewer_user_id": "viewer-1",
        "publisher_ids": ("publisher-1", "publisher-2"),
    }


async def test_get_following_status_batch_caps_chunks_at_100_publishers():
    client = StubClickHouseClient()
    repo = ClickHouseFeedRepository(
        client,
        build_settings(
            clickhouse_database="yral",
            feed_recsys_follow_lookup_chunk_size=250,
        ),
    )

    publisher_ids = [f"publisher-{index}" for index in range(205)]

    result = await repo.get_following_status_batch(
        viewer_user_id="viewer-1",
        publisher_ids=publisher_ids,
    )

    assert len(result) == 205
    assert len(client.calls) == 3
    assert len(client.calls[0][1]["publisher_ids"]) == 100
    assert len(client.calls[1][1]["publisher_ids"]) == 100
    assert len(client.calls[2][1]["publisher_ids"]) == 5


async def test_get_recent_active_publisher_user_ids_uses_valid_sources_and_lookback():
    client = StubClickHouseClient(
        rows=[{"publisher_user_id": "publisher-2"}, {"publisher_user_id": "publisher-1"}]
    )
    repo = ClickHouseFeedRepository(client, build_settings(clickhouse_database="yral"))

    rows = await repo.get_recent_active_publisher_user_ids(limit=50, lookback_days=30)

    assert rows == ["publisher-2", "publisher-1"]
    query, params = client.calls[0]
    assert "FROM yral.ai_ugc AS aug FINAL" in query
    assert "FROM yral.bot_uploaded_content AS buc FINAL" in query
    assert "FROM yral.ugc_content_approval AS uca FINAL" in query
    assert "INNER JOIN valid_videos vv" in query
    assert "GROUP BY pa.publisher_user_id" in query
    assert "ORDER BY max(pa.source_timestamp) DESC, pa.publisher_user_id" in query
    assert "LIMIT %(limit)s" in query
    assert params == {"lookback_days": 30, "limit": 50}
