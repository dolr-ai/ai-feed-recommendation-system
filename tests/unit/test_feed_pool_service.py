import asyncio

from src.services.feed_pool_service import FeedPoolService


class StubKVFeedRepository:
    def __init__(self):
        self.user_pool_reads = [
            ["video-1", "video-2", "video-3"],
        ]
        self.removed_batches = []
        self.served_recent_batches = []
        self.refill_lock_calls = []
        self.released_lock_calls = []
        self.pool_sizes = [5, 5]

    async def get_user_pool(self, user_id, pool_name, limit, current_time=None):
        return self.user_pool_reads.pop(0) if self.user_pool_reads else []

    async def filter_excluded_videos(self, video_ids):
        return video_ids

    async def check_user_served_recent(self, user_id, video_ids):
        return {video_id: False for video_id in video_ids}

    async def check_user_bloom(self, user_id, video_ids):
        return {video_id: False for video_id in video_ids}

    async def remove_user_pool_videos(self, user_id, pool_name, video_ids):
        self.removed_batches.append((user_id, pool_name, video_ids))
        return len(video_ids)

    async def add_served_recent_videos(self, user_id, video_ids):
        self.served_recent_batches.append((user_id, video_ids))
        return len(video_ids)

    async def get_user_pool_size(self, user_id, pool_name, current_time=None):
        return self.pool_sizes.pop(0) if self.pool_sizes else 0

    async def acquire_refill_lock(self, user_id, pool_name, ttl_sec=None):
        self.refill_lock_calls.append((user_id, pool_name, ttl_sec))
        return True

    async def release_refill_lock(self, user_id, pool_name):
        self.released_lock_calls.append((user_id, pool_name))

    async def user_bloom_exists(self, user_id):
        return True

    async def get_served_recent_count(self, user_id):
        return 1


class StubFeedSyncService:
    def __init__(self):
        self.following_sync_calls = []

    async def sync_user_following_pool(self, user_id):
        self.following_sync_calls.append(user_id)


class StubSettings:
    feed_recsys_refill_threshold = 200
    feed_recsys_refill_max_attempts = 1
    feed_recsys_background_refill_threshold = 10
    feed_recsys_background_refill_target = 40
    feed_recsys_following_refill_threshold = 10
    feed_recsys_refill_lock_ttl_sec = 30
    feed_recsys_served_recent_ttl_sec = 86400
    feed_recsys_following_sync_cooldown_sec = 600
    feed_recsys_popularity_buckets = ["99_100"]
    feed_recsys_freshness_windows = ["l7d"]
    feed_recsys_pool_ttl_sec = 3600
    feed_recsys_ugc_ratio = 0.3
    feed_recsys_popularity_ratio = 0.6
    feed_recsys_freshness_ratio = 0.4
    feed_recsys_following_first_segment_size = 10
    feed_recsys_following_first_segment_max = 5
    feed_recsys_following_first_segment_min = 3
    feed_recsys_following_max_per_request = 30


async def test_fetch_pool_videos_marks_served_recent_and_schedules_background_refill():
    kv_feed_repository = StubKVFeedRepository()
    service = FeedPoolService(
        kv_feed_repository=kv_feed_repository,
        feed_sync_service=StubFeedSyncService(),
        settings=StubSettings(),
    )

    refill_calls = []

    async def _fake_refill(user_id, pool_name, target):
        refill_calls.append((user_id, pool_name, target))
        return 0

    service._refill_pool = _fake_refill

    try:
        result = await service._fetch_pool_videos("user-1", "popularity", 2)
        await asyncio.sleep(0)
        await service.close()
    finally:
        await service.close()

    assert result == ["video-1", "video-2"]
    assert kv_feed_repository.removed_batches == [
        ("user-1", "popularity", ["video-1", "video-2"])
    ]
    assert kv_feed_repository.served_recent_batches == [
        ("user-1", ["video-1", "video-2"])
    ]
    assert refill_calls == [("user-1", "popularity", 40)]
