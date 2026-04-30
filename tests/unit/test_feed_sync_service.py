from src.services.feed_sync_service import FeedSyncService


class StubClickHouseVideoMetadataRepository:
    async def get_video_metadata_batch(self, video_ids):
        return {
            "video-1": {"post_id": "11", "publisher_user_id": "publisher-1"},
            "video-2": {"post_id": "22", "publisher_user_id": ""},
            "video-3": {"post_id": "", "publisher_user_id": "publisher-3"},
            "video-4": {"post_id": "44", "publisher_user_id": "publisher-4"},
        }


class StubSettings:
    feed_recsys_popularity_buckets = []


async def test_filter_video_ids_with_metadata_uses_clickhouse_eligibility_only():
    service = FeedSyncService(
        clickhouse_feed_repository=None,
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_feed_repository=None,
        chat_api_client=None,
        settings=StubSettings(),
    )

    result = await service._filter_video_ids_with_metadata(
        ["video-1", "video-2", "video-3", "video-4", "video-1", ""]
    )

    assert result == ["video-1", "video-4"]
