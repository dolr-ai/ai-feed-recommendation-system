import asyncio

from src.services.feed_pool_service import FeedPoolService


USER_A = "dc23f-7vyti-xp4vt-gqhlt-3qq2p-qoocg-iweu4-vv4wv-ur56b-jq4ap-nae"
USER_B = "jjhwf-vqja5-n5ds4-d5pnp-i4zva-xgr3n-qspvp-g4k4e-yfabm-qlm7q-jae"


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


class IsolatedKVFeedRepository(StubKVFeedRepository):
    def __init__(self):
        super().__init__()
        self.user_pool_reads = []
        self.pools = {
            (USER_A, "popularity"): ["a-video-1", "a-video-2"],
            (USER_B, "popularity"): ["b-video-1", "b-video-2"],
        }

    async def get_user_pool(self, user_id, pool_name, limit, current_time=None):
        return list(self.pools.get((user_id, pool_name), []))[:limit]

    async def remove_user_pool_videos(self, user_id, pool_name, video_ids):
        current = self.pools.get((user_id, pool_name), [])
        remove_set = set(video_ids)
        self.pools[(user_id, pool_name)] = [
            video_id
            for video_id in current
            if video_id not in remove_set
        ]
        return await super().remove_user_pool_videos(user_id, pool_name, video_ids)


class StubFeedSyncService:
    def __init__(self):
        self.following_sync_calls = []

    async def sync_user_following_pool(self, user_id):
        self.following_sync_calls.append(user_id)
        return {"fetched": 0, "added": 0}


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


async def test_refill_pool_returns_refill_counts():
    service = FeedPoolService(
        kv_feed_repository=StubKVFeedRepository(),
        feed_sync_service=StubFeedSyncService(),
        settings=StubSettings(),
    )

    async def _fake_refill_popularity(user_id, target):
        assert user_id == USER_A
        assert target == 25
        return 17

    service.refill_popularity = _fake_refill_popularity

    assert await service._refill_pool(USER_A, "popularity", 25) == 17


async def test_mixed_feed_degrades_when_following_source_fails():
    service = FeedPoolService(
        kv_feed_repository=StubKVFeedRepository(),
        feed_sync_service=StubFeedSyncService(),
        settings=StubSettings(),
    )

    async def _fake_fetch_pool(user_id, pool_name, count):
        assert user_id == USER_A
        if pool_name == "following":
            raise RuntimeError("following unavailable")
        if pool_name == "ugc":
            return []
        if pool_name == "popularity":
            return ["popular-1"][:count]
        if pool_name == "freshness":
            return ["fresh-1", "fresh-2"][:count]
        return []

    service._fetch_pool_videos = _fake_fetch_pool

    videos, sources = await service._get_mixed_video_ids(USER_A, 3)

    assert set(videos) == {"popular-1", "fresh-1", "fresh-2"}
    assert sources == {
        "following": 0,
        "ugc": 0,
        "popularity": 1,
        "freshness": 2,
    }


async def test_user_pool_fetch_keeps_different_users_isolated():
    kv_feed_repository = IsolatedKVFeedRepository()
    service = FeedPoolService(
        kv_feed_repository=kv_feed_repository,
        feed_sync_service=StubFeedSyncService(),
        settings=StubSettings(),
    )

    user_a_videos = await service._fetch_pool_videos(USER_A, "popularity", 1)
    user_b_videos = await service._fetch_pool_videos(USER_B, "popularity", 1)

    assert user_a_videos == ["a-video-1"]
    assert user_b_videos == ["b-video-1"]
    assert kv_feed_repository.pools[(USER_A, "popularity")] == ["a-video-2"]
    assert kv_feed_repository.pools[(USER_B, "popularity")] == ["b-video-2"]
    assert kv_feed_repository.removed_batches == [
        (USER_A, "popularity", ["a-video-1"]),
        (USER_B, "popularity", ["b-video-1"]),
    ]
    assert kv_feed_repository.served_recent_batches == [
        (USER_A, ["a-video-1"]),
        (USER_B, ["b-video-1"]),
    ]
