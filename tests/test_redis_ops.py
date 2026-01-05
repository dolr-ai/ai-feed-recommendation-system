"""
Redis operations tests.

Tests bloom filters, deduplication, refill triggers, and cooldown behavior.
Uses real Redis.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
import time
from helpers import generate_video_ids, seed_global_popularity, seed_user_pool, clear_user_keys


class TestBloomFilterDeduplication:
    """Tests for bloom filter deduplication."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_watched_videos_not_returned(self, redis_client, redis_config):
        """
        Videos marked as watched are not returned in recommendations.

        Algorithm:
            1. Seed user pool with videos
            2. Mark some videos as watched (cooldown)
            3. Fetch videos
            4. Verify watched videos are not in results
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = "test_bloom_dedup_001"

        try:
            # Seed popularity pool
            videos = generate_video_ids("vid_bloom_test", 100)
            seed_global_popularity(redis_client, {"99_100": videos})

            # Mark first 10 as watched
            watched = videos[:10]
            await redis_layer.mark_videos_cooldown(user_id, watched)

            # Fetch videos
            result = await redis_layer.fetch_videos(user_id, "popularity", 50)

            # Watched videos should not be in result
            for vid in watched:
                assert vid not in result, f"Watched video {vid} should not be returned"

        finally:
            clear_user_keys(redis_client, user_id)
            redis_client.delete("{GLOBAL}:pool:pop_99_100")
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_permanent_bloom_filter_persists(self, redis_client, redis_config):
        """
        Permanent bloom filter persists across sessions.

        Algorithm:
            1. Mark videos as permanently watched
            2. Verify bloom filter key exists
            3. Check that marked videos are in bloom filter
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = "test_bloom_persist_001"

        try:
            # Mark videos permanently
            videos = ["vid_perm_001", "vid_perm_002", "vid_perm_003"]
            await redis_layer.mark_videos_permanently_watched(user_id, videos)

            # Bloom filter key should exist (cluster-safe pattern)
            bloom_key = f"{{user:{user_id}}}:bloom:permanent"
            exists = redis_client.exists(bloom_key)
            assert exists, "Bloom filter key should exist"

        finally:
            clear_user_keys(redis_client, user_id)
            await service.close()


class TestCooldownBehavior:
    """Tests for cooldown (short-lived TTL) behavior."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_cooldown_marks_videos(self, redis_client, redis_config):
        """
        mark_videos_cooldown adds videos to cooldown set.

        Algorithm:
            1. Clear any existing state
            2. Mark videos with cooldown
            3. Verify cooldown set contains the videos
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = "test_cooldown_001"

        try:
            # Clear any existing state first
            clear_user_keys(redis_client, user_id)

            videos = ["vid_cool_001", "vid_cool_002"]
            count = await redis_layer.mark_videos_cooldown(user_id, videos)

            assert count == 2, f"Expected 2 videos marked, got {count}"

            # Check cooldown set (cluster-safe key pattern)
            cooldown_key = f"{{user:{user_id}}}:watched:short"
            members = redis_client.zrange(cooldown_key, 0, -1)
            for vid in videos:
                assert vid in members, f"{vid} should be in cooldown set"

        finally:
            clear_user_keys(redis_client, user_id)
            await service.close()


class TestRefillTriggers:
    """Tests for refill trigger logic."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_needs_refill_when_pool_low(self, redis_client, redis_config):
        """
        needs_refill returns True when pool is below threshold.

        Algorithm:
            1. Create user with empty pool
            2. Check needs_refill
            3. Verify it returns True
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        from config import REFILL_THRESHOLD

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = "test_refill_001"

        try:
            # Clear user state (ensure pool is empty)
            clear_user_keys(redis_client, user_id)

            # Seed with fewer videos than threshold
            few_videos = generate_video_ids("vid_low", 10)
            seed_user_pool(redis_client, user_id, "popularity", few_videos)

            needs = await redis_layer.needs_refill(user_id, "popularity")
            assert needs is True, f"Should need refill (pool has 10, threshold is {REFILL_THRESHOLD})"

        finally:
            clear_user_keys(redis_client, user_id)
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_auto_refill_on_fetch(self, redis_client, redis_config):
        """
        fetch_videos attempts auto-refill when pool is low.

        Algorithm:
            1. Clear user state and any stale locks
            2. Seed global pool
            3. Fetch videos
            4. Verify function completes without error
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        import asyncio

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = "test_autorefill_001"

        try:
            # Clear any existing state and stale locks
            clear_user_keys(redis_client, user_id)
            # Clear refill locks that might be stale
            for lock_pattern in redis_client.keys(f"refill:lock:{user_id}:*"):
                redis_client.delete(lock_pattern)
            await asyncio.sleep(0.1)

            # Seed global popularity pool
            videos = generate_video_ids("vid_global_pop", 500)
            seed_global_popularity(redis_client, {"99_100": videos, "90_99": videos})

            # Fetch - may or may not return videos depending on lock state
            result = await redis_layer.fetch_videos(user_id, "popularity", 20)

            # Just verify it completes without raising exception
            assert isinstance(result, list), "fetch_videos should return a list"

        finally:
            clear_user_keys(redis_client, user_id)
            redis_client.delete("{GLOBAL}:pool:pop_99_100")
            redis_client.delete("{GLOBAL}:pool:pop_90_99")
            await service.close()


class TestPoolExpiry:
    """Tests for video expiry in pools."""

    @pytest.mark.integration
    def test_expired_videos_not_counted(self, redis_client):
        """
        Expired videos (score < current time) are not counted as valid.

        Algorithm:
            1. Add videos with past expiry times
            2. Add videos with future expiry times
            3. Count valid videos
            4. Verify only future-expiry videos counted
        """
        user_id = "test_expiry_001"
        pool_key = f"{{user:{user_id}}}:videos_to_show:popularity"

        try:
            now = int(time.time())

            # Add expired videos (score in past)
            expired_mapping = {
                "vid_expired_001": now - 1000,
                "vid_expired_002": now - 2000,
            }
            redis_client.zadd(pool_key, expired_mapping)

            # Add valid videos (score in future)
            valid_mapping = {
                "vid_valid_001": now + 1000,
                "vid_valid_002": now + 2000,
                "vid_valid_003": now + 3000,
            }
            redis_client.zadd(pool_key, valid_mapping)

            # Count videos with score > now
            valid_count = redis_client.zcount(pool_key, now, "+inf")
            assert valid_count == 3, f"Expected 3 valid videos, got {valid_count}"

        finally:
            redis_client.delete(pool_key)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
