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
    feed_recsys_view_count_prewarm_enabled = True
    feed_recsys_view_count_prewarm_batch_size = 2
    feed_recsys_pool_ttl_sec = 3600
    feed_recsys_freshness_windows = ["l1d", "l7d"]
    feed_recsys_ugc_discovery_max_views = 200
    feed_recsys_ugc_discovery_max_age_days = 7
    feed_recsys_ugc_discovery_pool_limit = 100
    feed_recsys_ugc_discovery_pool_ttl_sec = 1800


class StubKVFeedRepository:
    def __init__(self):
        self.cached_reads = []
        self.cached_writes = []
        self.replaced_global_pools = []
        self.discovery_pool_payloads = []

    async def get_cached_video_view_counts(self, video_ids):
        self.cached_reads.append(video_ids)
        return {"video-1": {"num_views_loggedin": 1, "num_views_all": 2}}

    async def cache_video_view_counts(self, view_counts):
        self.cached_writes.append(view_counts)
        return len(view_counts)

    async def replace_global_pool(self, pool_name, video_ids, ttl_sec=None):
        self.replaced_global_pools.append((pool_name, video_ids, ttl_sec))
        return len(video_ids)

    async def replace_ugc_discovery_pool(self, videos, ttl_sec=None):
        self.discovery_pool_payloads.append((videos, ttl_sec))
        return len(videos)


class StubOffchainRewardsClient:
    def __init__(self):
        self.calls = []

    async def get_bulk_video_stats(self, video_ids):
        self.calls.append(video_ids)
        return {
            video_id: {"num_views_loggedin": index + 10, "num_views_all": index + 100}
            for index, video_id in enumerate(video_ids)
        }


class StubClickHouseFeedRepository:
    async def get_global_popular_videos(self):
        return [
            {"video_id": "video-1"},
            {"video_id": "video-2"},
            {"video_id": "video-4"},
        ]


async def test_filter_video_ids_with_metadata_uses_clickhouse_eligibility_only():
    service = FeedSyncService(
        clickhouse_feed_repository=None,
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_feed_repository=None,
        chat_api_client=None,
        offchain_rewards_client=None,
        settings=StubSettings(),
    )

    result = await service._filter_video_ids_with_metadata(
        ["video-1", "video-2", "video-3", "video-4", "video-1", ""]
    )

    assert result == ["video-1", "video-4"]


async def test_prewarm_video_view_count_cache_reads_cache_and_fetches_only_misses():
    kv_feed_repository = StubKVFeedRepository()
    offchain_rewards_client = StubOffchainRewardsClient()
    service = FeedSyncService(
        clickhouse_feed_repository=None,
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_feed_repository=kv_feed_repository,
        chat_api_client=None,
        offchain_rewards_client=offchain_rewards_client,
        settings=StubSettings(),
    )

    await service._prewarm_video_view_count_cache(
        ["video-1", "video-2", "video-3", "video-2", ""],
        source="popularity",
    )

    assert kv_feed_repository.cached_reads == [["video-1", "video-2", "video-3"]]
    assert offchain_rewards_client.calls == [["video-2", "video-3"]]
    assert kv_feed_repository.cached_writes == [
        {
            "video-2": {"num_views_loggedin": 10, "num_views_all": 100},
            "video-3": {"num_views_loggedin": 11, "num_views_all": 101},
        }
    ]


async def test_sync_global_popularity_pools_prewarms_view_count_cache_after_pool_write():
    kv_feed_repository = StubKVFeedRepository()
    offchain_rewards_client = StubOffchainRewardsClient()

    service = FeedSyncService(
        clickhouse_feed_repository=StubClickHouseFeedRepository(),
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_feed_repository=kv_feed_repository,
        chat_api_client=None,
        offchain_rewards_client=offchain_rewards_client,
        settings=StubSettings(),
    )
    service._bucket_popular_video_ids = lambda rows: {
        "99_100": ["video-1"],
        "90_99": ["video-4"],
    }

    inserted = await service.sync_global_popularity_pools()

    assert inserted == {"99_100": 1, "90_99": 1}
    assert kv_feed_repository.replaced_global_pools == [
        ("popular:99_100", ["video-1"], 3600),
        ("popular:90_99", ["video-4"], 3600),
    ]
    assert kv_feed_repository.cached_reads == [["video-1", "video-4"]]
    assert offchain_rewards_client.calls == [["video-4"]]
    assert kv_feed_repository.cached_writes == [
        {
            "video-4": {"num_views_loggedin": 10, "num_views_all": 100},
        }
    ]
