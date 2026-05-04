from src.services.feed_sync_service import FeedSyncService


class StubClickHouseVideoMetadataRepository:
    async def get_video_metadata_batch(self, video_ids):
        return {
            "video-1": {"post_id": "11", "publisher_user_id": "publisher-1"},
            "video-2": {"post_id": "22", "publisher_user_id": ""},
            "video-3": {"post_id": "", "publisher_user_id": "publisher-3"},
            "video-4": {"post_id": "44", "publisher_user_id": "publisher-4"},
        }


class StubKVVideoMetadataRepository:
    def __init__(self):
        self.cache_writes = []

    async def cache_video_metadata_batch(self, metadata_by_video_id):
        self.cache_writes.append(metadata_by_video_id)
        return len(metadata_by_video_id)


class StubSettings:
    feed_recsys_popularity_buckets = []
    feed_recsys_view_count_prewarm_enabled = True
    feed_recsys_view_count_prewarm_batch_size = 2
    feed_recsys_pool_ttl_sec = 3600
    feed_recsys_following_pool_ttl_sec = 10800
    feed_recsys_following_fetch_limit = 1000
    feed_recsys_freshness_windows = ["l1d", "l7d"]


class StubKVFeedRepository:
    def __init__(self):
        self.cached_reads = []
        self.cached_writes = []
        self.replaced_global_pools = []
        self.replaced_user_pools = []
        self.tracked_following_users = []

    async def get_cached_video_view_counts(self, video_ids):
        self.cached_reads.append(video_ids)
        return {"video-1": {"num_views_loggedin": 1, "num_views_all": 2}}

    async def cache_video_view_counts(self, view_counts):
        self.cached_writes.append(view_counts)
        return len(view_counts)

    async def replace_global_pool(self, pool_name, video_ids, ttl_sec=None):
        self.replaced_global_pools.append((pool_name, video_ids, ttl_sec))
        return len(video_ids)

    async def replace_user_pool(self, user_id, pool_name, video_ids, ttl_sec=None):
        self.replaced_user_pools.append((user_id, pool_name, video_ids, ttl_sec))
        return len(video_ids)

    async def get_tracked_following_sync_users(self):
        return list(self.tracked_following_users)


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

    async def get_following_video_candidates(self, user_id, num_videos=1000):
        if user_id == "user-a":
            return [{"video_id": "video-1"}, {"video_id": "video-4"}]
        return []


async def test_filter_video_ids_with_metadata_uses_clickhouse_eligibility_only():
    kv_video_metadata_repository = StubKVVideoMetadataRepository()
    service = FeedSyncService(
        clickhouse_feed_repository=None,
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_video_metadata_repository=kv_video_metadata_repository,
        kv_feed_repository=None,
        chat_api_client=None,
        offchain_rewards_client=None,
        settings=StubSettings(),
    )

    result = await service._filter_video_ids_with_metadata(
        ["video-1", "video-2", "video-3", "video-4", "video-1", ""]
    )

    assert result == ["video-1", "video-4"]
    assert kv_video_metadata_repository.cache_writes == [
        {
            "video-1": {"post_id": "11", "publisher_user_id": "publisher-1"},
            "video-4": {"post_id": "44", "publisher_user_id": "publisher-4"},
        }
    ]


async def test_prewarm_video_view_count_cache_reads_cache_and_fetches_only_misses():
    kv_feed_repository = StubKVFeedRepository()
    kv_video_metadata_repository = StubKVVideoMetadataRepository()
    offchain_rewards_client = StubOffchainRewardsClient()
    service = FeedSyncService(
        clickhouse_feed_repository=None,
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_video_metadata_repository=kv_video_metadata_repository,
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
    kv_video_metadata_repository = StubKVVideoMetadataRepository()
    offchain_rewards_client = StubOffchainRewardsClient()

    service = FeedSyncService(
        clickhouse_feed_repository=StubClickHouseFeedRepository(),
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_video_metadata_repository=kv_video_metadata_repository,
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
    assert kv_video_metadata_repository.cache_writes == [
        {"video-1": {"post_id": "11", "publisher_user_id": "publisher-1"}},
        {"video-4": {"post_id": "44", "publisher_user_id": "publisher-4"}},
    ]
    assert kv_feed_repository.cached_reads == [["video-1", "video-4"]]
    assert offchain_rewards_client.calls == [["video-4"]]
    assert kv_feed_repository.cached_writes == [
        {
            "video-4": {"num_views_loggedin": 10, "num_views_all": 100},
        }
    ]


async def test_sync_tracked_following_pools_refreshes_registered_users_only():
    kv_feed_repository = StubKVFeedRepository()
    kv_feed_repository.tracked_following_users = ["user-a", "user-b"]
    kv_video_metadata_repository = StubKVVideoMetadataRepository()
    service = FeedSyncService(
        clickhouse_feed_repository=StubClickHouseFeedRepository(),
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_video_metadata_repository=kv_video_metadata_repository,
        kv_feed_repository=kv_feed_repository,
        chat_api_client=None,
        offchain_rewards_client=None,
        settings=StubSettings(),
    )

    result = await service.sync_tracked_following_pools()

    assert result == {"users": 2, "videos": 2}
    assert kv_feed_repository.replaced_user_pools == [
        ("user-a", "following", ["video-1", "video-4"], 10800),
        ("user-b", "following", [], 10800),
    ]
