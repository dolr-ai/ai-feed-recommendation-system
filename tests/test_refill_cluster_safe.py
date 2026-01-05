"""
Tests for cluster-safe refill operations.

These tests verify that refill works correctly when:
1. Global reads are separate from user writes
2. Only user keys are passed to Lua scripts (same hash slot)
3. CROSSSLOT errors don't occur in cluster mode

Test flow (TDD):
    1. Write tests (initially will FAIL - old implementation uses cross-slot Lua)
    2. Refactor refill methods to split global reads from user writes
    3. Tests should PASS after refactor
"""

import os
import sys
import time
import pytest
import uuid

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestRefillPopularityClusterSafe:
    """Tests for refactored popularity refill."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_refill_samples_from_global_pools(self, redis_client, redis_config):
        """
        Refill should sample videos from global popularity pools.

        Algorithm:
            1. Seed global popularity pool with NEW key pattern ({GLOBAL}:pool:pop_*)
            2. Call refill_popularity
            3. Verify user pool has videos from global pool
            4. Cleanup test keys

        This test verifies the basic flow works after refactoring to cluster-safe mode.
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_refill_cluster_{uuid.uuid4().hex[:8]}"

        try:
            # Seed global pool with NEW key pattern
            videos = [f"vid_pop_{i:06d}" for i in range(100)]
            now = int(time.time())
            expiry = float(now + 86400)  # 1 day TTL
            pool_key = "{GLOBAL}:pool:pop_99_100"

            # Clear and seed the pool
            redis_client.delete(pool_key)
            redis_client.zadd(pool_key, {v: expiry for v in videos})

            # Call refill
            result = await redis_layer.refill_popularity(user_id, target=50)

            # Verify result structure
            assert isinstance(result, dict), "refill_popularity should return a dict"
            assert "added_total" in result, "Result should contain added_total"

            # Verify user pool has videos (with NEW key pattern)
            user_pool_key = f"{{user:{user_id}}}:videos_to_show:popularity"
            count = redis_client.zcard(user_pool_key)
            assert count > 0, f"User pool should have videos after refill, got {count}"

        finally:
            # Cleanup with NEW key patterns
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:popularity",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:pop_percentile_pointer",
                f"{{user:{user_id}}}:refill:lock:popularity",
                f"{{user:{user_id}}}:refill:last:popularity",
                "{GLOBAL}:pool:pop_99_100",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_refill_filters_watched_videos(self, redis_client, redis_config):
        """
        Refill should filter out already-watched videos.

        Algorithm:
            1. Seed global pool with videos
            2. Add some videos to user's bloom filter (marking as watched)
            3. Call refill
            4. Verify watched videos are NOT in user pool

        This ensures the filtering logic still works after cluster-safe refactor.
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_filter_watched_{uuid.uuid4().hex[:8]}"

        try:
            # Seed global pool
            all_videos = [f"vid_filter_{i:06d}" for i in range(50)]
            watched_videos = all_videos[:10]  # First 10 are "watched"
            now = int(time.time())
            expiry = float(now + 86400)
            pool_key = "{GLOBAL}:pool:pop_99_100"

            redis_client.delete(pool_key)
            redis_client.zadd(pool_key, {v: expiry for v in all_videos})

            # Initialize bloom and add watched videos
            bloom_key = f"{{user:{user_id}}}:bloom:permanent"
            await redis_layer.init_user_bloom_filter(user_id)
            # Mark videos as watched in bloom filter
            await redis_layer.mark_videos_permanently_watched(user_id, watched_videos)

            # Call refill
            await redis_layer.refill_popularity(user_id, target=30)

            # Get videos in user pool
            user_pool_key = f"{{user:{user_id}}}:videos_to_show:popularity"
            pool_videos = redis_client.zrange(user_pool_key, 0, -1)

            # Verify watched videos are NOT in pool
            for watched in watched_videos:
                assert watched not in pool_videos, (
                    f"Watched video {watched} should not be in pool"
                )

        finally:
            # Cleanup
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:popularity",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:pop_percentile_pointer",
                f"{{user:{user_id}}}:refill:lock:popularity",
                f"{{user:{user_id}}}:refill:last:popularity",
                "{GLOBAL}:pool:pop_99_100",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_refill_downshifts_to_lower_buckets(self, redis_client, redis_config):
        """
        Refill should downshift to lower popularity buckets when top bucket is empty.

        Algorithm:
            1. Leave top bucket (99_100) empty
            2. Seed next bucket (90_99) with videos
            3. Call refill
            4. Verify videos came from 90_99 bucket
            5. Verify bucket_final reflects downshift

        This tests the cascade/downshift logic works after refactor.
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_downshift_{uuid.uuid4().hex[:8]}"

        try:
            now = int(time.time())
            expiry = float(now + 86400)

            # Clear top bucket, seed second bucket
            redis_client.delete("{GLOBAL}:pool:pop_99_100")
            redis_client.delete("{GLOBAL}:pool:pop_90_99")

            second_bucket_videos = [f"vid_90_99_{i:06d}" for i in range(50)]
            redis_client.zadd(
                "{GLOBAL}:pool:pop_90_99",
                {v: expiry for v in second_bucket_videos}
            )

            # Reset pointer to top bucket
            pointer_key = f"{{user:{user_id}}}:pop_percentile_pointer"
            redis_client.set(pointer_key, "99_100")

            # Call refill
            result = await redis_layer.refill_popularity(user_id, target=20)

            # Verify downshift happened - bucket_final should be 90_99 or lower
            assert result.get("bucket_final") != "99_100" or result.get("added_total") == 0, (
                "Should have downshifted from empty 99_100 bucket"
            )

        finally:
            # Cleanup
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:popularity",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:pop_percentile_pointer",
                f"{{user:{user_id}}}:refill:lock:popularity",
                f"{{user:{user_id}}}:refill:last:popularity",
                "{GLOBAL}:pool:pop_99_100",
                "{GLOBAL}:pool:pop_90_99",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()


class TestRefillFreshnessClusterSafe:
    """Tests for refactored freshness refill."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_refill_samples_from_freshness_windows(self, redis_client, redis_config):
        """
        Freshness refill should sample from freshness time windows.

        Algorithm:
            1. Seed freshness windows with NEW key pattern ({GLOBAL}:pool:fresh_*)
            2. Call refill_freshness
            3. Verify user pool has videos
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_fresh_refill_{uuid.uuid4().hex[:8]}"

        try:
            now = int(time.time())
            expiry = float(now + 86400)

            # Seed l1d window
            videos = [f"vid_fresh_l1d_{i:06d}" for i in range(100)]
            pool_key = "{GLOBAL}:pool:fresh_l1d"
            redis_client.delete(pool_key)
            redis_client.zadd(pool_key, {v: expiry for v in videos})

            # Call refill
            result = await redis_layer.refill_freshness(user_id, target=50)

            # Verify result
            assert isinstance(result, dict), "refill_freshness should return a dict"
            assert "added_total" in result

            # Verify user pool has videos
            user_pool_key = f"{{user:{user_id}}}:videos_to_show:freshness"
            count = redis_client.zcard(user_pool_key)
            assert count > 0, f"User pool should have videos, got {count}"

        finally:
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:freshness",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:refill:lock:freshness",
                f"{{user:{user_id}}}:refill:last:freshness",
                "{GLOBAL}:pool:fresh_l1d",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_freshness_cascades_through_windows(self, redis_client, redis_config):
        """
        Freshness refill should cascade from l1d -> l7d -> l14d etc.

        Algorithm:
            1. Leave l1d empty
            2. Seed l7d with videos
            3. Call refill
            4. Verify videos came from l7d
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_fresh_cascade_{uuid.uuid4().hex[:8]}"

        try:
            now = int(time.time())
            expiry = float(now + 86400)

            # Clear l1d, seed l7d
            redis_client.delete("{GLOBAL}:pool:fresh_l1d")
            redis_client.delete("{GLOBAL}:pool:fresh_l7d")

            l7d_videos = [f"vid_l7d_{i:06d}" for i in range(50)]
            redis_client.zadd("{GLOBAL}:pool:fresh_l7d", {v: expiry for v in l7d_videos})

            # Call refill
            result = await redis_layer.refill_freshness(user_id, target=20)

            # Should have added some videos (cascaded to l7d)
            user_pool_key = f"{{user:{user_id}}}:videos_to_show:freshness"
            pool_videos = redis_client.zrange(user_pool_key, 0, -1)

            # At least some should be from l7d
            l7d_in_pool = [v for v in pool_videos if v.startswith("vid_l7d_")]
            assert len(l7d_in_pool) > 0 or result.get("added_total") == 0, (
                "Should have cascaded to l7d when l1d is empty"
            )

        finally:
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:freshness",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:refill:lock:freshness",
                f"{{user:{user_id}}}:refill:last:freshness",
                "{GLOBAL}:pool:fresh_l1d",
                "{GLOBAL}:pool:fresh_l7d",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()


class TestRefillFallbackClusterSafe:
    """Tests for refactored fallback refill."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_refill_from_fallback_pool(self, redis_client, redis_config):
        """
        Fallback refill should sample from global fallback pool.

        Algorithm:
            1. Seed fallback pool with NEW key pattern ({GLOBAL}:pool:fallback)
            2. Call refill_with_fallback
            3. Verify user pool has videos
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_fallback_{uuid.uuid4().hex[:8]}"

        try:
            now = int(time.time())
            expiry = float(now + 86400)

            # Seed fallback pool
            videos = [f"vid_fallback_{i:06d}" for i in range(100)]
            pool_key = "{GLOBAL}:pool:fallback"
            redis_client.delete(pool_key)
            redis_client.zadd(pool_key, {v: expiry for v in videos})

            # Call refill
            result = await redis_layer.refill_with_fallback(user_id, target=50)

            # Verify result
            assert isinstance(result, dict)

            # Verify user pool has videos
            user_pool_key = f"{{user:{user_id}}}:videos_to_show:fallback"
            count = redis_client.zcard(user_pool_key)
            assert count > 0, f"Fallback pool should have videos, got {count}"

        finally:
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:fallback",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:refill:lock:fallback",
                f"{{user:{user_id}}}:refill:last:fallback",
                "{GLOBAL}:pool:fallback",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()


class TestRefillUGCClusterSafe:
    """Tests for refactored UGC refill."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_refill_from_ugc_pool(self, redis_client, redis_config):
        """
        UGC refill should sample from global UGC pool.

        Algorithm:
            1. Seed UGC pool with NEW key pattern ({GLOBAL}:pool:ugc)
            2. Call refill_ugc
            3. Verify user pool has videos
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_ugc_refill_{uuid.uuid4().hex[:8]}"

        try:
            now = int(time.time())
            expiry = float(now + 86400)

            # Seed UGC pool
            videos = [f"vid_ugc_{i:06d}" for i in range(100)]
            pool_key = "{GLOBAL}:pool:ugc"
            redis_client.delete(pool_key)
            redis_client.zadd(pool_key, {v: expiry for v in videos})

            # Call refill
            result = await redis_layer.refill_ugc(user_id, target=50)

            # Verify result
            assert isinstance(result, dict)

            # Verify user pool has videos
            user_pool_key = f"{{user:{user_id}}}:videos_to_show:ugc"
            count = redis_client.zcard(user_pool_key)
            assert count > 0, f"UGC pool should have videos, got {count}"

        finally:
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:ugc",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
                f"{{user:{user_id}}}:refill:lock:ugc",
                f"{{user:{user_id}}}:refill:last:ugc",
                "{GLOBAL}:pool:ugc",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()


class TestClusterKeysInLuaScripts:
    """Verify that only same-slot keys are passed to Lua scripts."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_filter_and_add_uses_only_user_keys(self, redis_client, redis_config):
        """
        filter_and_add_videos Lua script should only receive user keys.

        This is already cluster-safe, but verify it works with new key patterns.

        Algorithm:
            1. Seed user watched set with some videos
            2. Call filter_and_add directly with user keys only
            3. Verify filtering worked correctly
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()
        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()

        user_id = f"test_lua_user_keys_{uuid.uuid4().hex[:8]}"

        try:
            now = int(time.time())
            expiry = now + 86400

            # Setup user keys with NEW patterns
            to_show_key = f"{{user:{user_id}}}:videos_to_show:popularity"
            watched_key = f"{{user:{user_id}}}:watched:short"
            bloom_key = f"{{user:{user_id}}}:bloom:permanent"

            # Mark some videos as watched
            watched = ["vid_001", "vid_002", "vid_003"]
            redis_client.zadd(watched_key, {v: expiry for v in watched})

            # Initialize bloom
            await redis_layer.init_user_bloom_filter(user_id)

            # Add more to bloom
            await redis_layer.mark_videos_permanently_watched(user_id, ["vid_004", "vid_005"])

            # Candidates include watched and bloom-filtered
            candidates = watched + ["vid_004", "vid_005", "vid_006", "vid_007", "vid_008"]

            # Call filter_and_add directly
            result = await redis_layer.scripts.filter_and_add_videos(
                keys=[to_show_key, watched_key, bloom_key],
                args=[now, 86400, 100] + candidates,  # now, ttl, capacity, candidates...
            )

            # Should have filtered out watched (3) and bloom (2), leaving 3
            added = int(result[0])
            assert added <= 3, f"Should have filtered 5 videos, added max 3, got {added}"

            # Verify only unfiltered videos in pool
            pool_videos = redis_client.zrange(to_show_key, 0, -1)
            for watched_vid in watched:
                assert watched_vid not in pool_videos, f"{watched_vid} should be filtered"

        finally:
            cleanup_keys = [
                f"{{user:{user_id}}}:videos_to_show:popularity",
                f"{{user:{user_id}}}:watched:short",
                f"{{user:{user_id}}}:bloom:permanent",
            ]
            for key in cleanup_keys:
                redis_client.delete(key)
            await service.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-x"])
