from src.core.settings import Settings
from src.repository.clickhouse_video_metadata_repository import (
    ClickHouseVideoMetadataRepository,
)


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


async def test_get_video_metadata_batch_uses_priority_sources_and_filters():
    client = StubClickHouseClient(
        rows=[
            {
                "video_id": "video-1",
                "canister_id": "2vxsx-fae",
                "post_id": "11",
                "publisher_user_id": "publisher-1",
            }
        ]
    )
    repo = ClickHouseVideoMetadataRepository(client, build_settings(clickhouse_database="yral"))

    metadata = await repo.get_video_metadata_batch(["video-1", "video-2"])

    assert metadata == {
        "video-1": {
            "canister_id": "profile-id",
            "post_id": "11",
            "publisher_user_id": "publisher-1",
        }
    }
    query, params = client.calls[0]
    assert "FROM yral.ai_ugc AS aug FINAL" in query
    assert "FROM yral.bot_uploaded_content AS buc FINAL" in query
    assert "FROM yral.ugc_content_approval AS uca FINAL" in query
    assert "INNER JOIN yral.video_unique_v2 AS vu FINAL" in query
    assert "FROM yral.excluded_videos FINAL" in query
    assert "WHERE aug.video_id IN %(video_ids)s" in query
    assert "pd.video_id AS video_id" in query
    assert "pd.canister_id AS canister_id" in query
    assert "pd.post_id AS post_id" in query
    assert "pd.publisher_user_id AS publisher_user_id" in query
    assert "ORDER BY priority ASC, source_timestamp DESC" in query
    assert params == {"video_ids": ("video-1", "video-2")}
