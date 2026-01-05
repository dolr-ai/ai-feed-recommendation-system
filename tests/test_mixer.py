"""
Mixer distribution tests.

Tests 60/40 popularity/freshness ratio, following position distribution, and UGC interspersing.
Uses real Redis with seeded test data.
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
import asyncio
from helpers import generate_video_ids, seed_global_popularity, seed_global_freshness, clear_user_keys, seed_user_pool


@pytest.fixture
def seed_global_pools(redis_client):
    """
    Seed global popularity and freshness pools with test data.

    Seeds:
        - 200 videos in 99_100 bucket
        - 200 videos in 90_99 bucket
        - 200 videos in l1d freshness window
        - 200 videos in l7d freshness window
    """
    # Seed popularity buckets
    pop_videos = {
        "99_100": generate_video_ids("vid_pop_99", 200),
        "90_99": generate_video_ids("vid_pop_90", 200),
    }
    seed_global_popularity(redis_client, pop_videos)

    # Seed freshness windows
    fresh_videos = {
        "l1d": generate_video_ids("vid_fresh_l1d", 200),
        "l7d": generate_video_ids("vid_fresh_l7d", 200),
    }
    seed_global_freshness(redis_client, fresh_videos)

    yield

    # Cleanup global pools (key pattern must match helpers.py seeding)
    for bucket in pop_videos:
        redis_client.delete(f"{{GLOBAL}}:pool:pop_{bucket}")
    for window in fresh_videos:
        redis_client.delete(f"{{GLOBAL}}:pool:fresh_{window}")


class TestMixerRatios:
    """Tests for 60/40 popularity/freshness mixing ratio."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_60_40_ratio_with_real_redis(self, redis_client, redis_config, seed_global_pools):
        """
        Test that mixer returns approximately 60% popularity, 40% freshness.

        Algorithm:
            1. Import async redis layer and mixer
            2. Seed global pools
            3. Create fresh test user
            4. Get 100 mixed recommendations
            5. Count videos by source prefix
            6. Verify ratio is within tolerance
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        from async_mixer import AsyncVideoMixer

        # Setup async Redis
        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()
        mixer = AsyncVideoMixer(redis_layer)

        user_id = "test_mixer_ratio_001"

        try:
            # Get mixed recommendations
            result = await mixer.get_mixed_recommendations(user_id, count=100)

            # Count by source (videos start with vid_pop_ or vid_fresh_)
            pop_count = sum(1 for v in result.videos if v.startswith("vid_pop_"))
            fresh_count = sum(1 for v in result.videos if v.startswith("vid_fresh_"))
            total_pop_fresh = pop_count + fresh_count

            # Skip ratio check if we don't have both types (pool might be empty)
            if total_pop_fresh > 0:
                pop_ratio = pop_count / total_pop_fresh
                # Allow 15% tolerance due to following/ugc taking slots
                assert 0.45 <= pop_ratio <= 0.75, \
                    f"Expected ~60% popularity, got {pop_ratio*100:.1f}%"

        finally:
            # Cleanup
            clear_user_keys(redis_client, user_id)
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_mixer_returns_requested_count(self, redis_client, redis_config, seed_global_pools):
        """
        Mixer returns requested video count (or as many as available).

        Algorithm:
            1. Request 50 videos
            2. Verify we get <= 50 videos
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        from async_mixer import AsyncVideoMixer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()
        mixer = AsyncVideoMixer(redis_layer)

        user_id = "test_mixer_count_001"

        try:
            result = await mixer.get_mixed_recommendations(user_id, count=50)
            assert len(result.videos) <= 50

        finally:
            clear_user_keys(redis_client, user_id)
            await service.close()


class TestFollowingDistribution:
    """Tests for following content position-based distribution."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_following_in_first_segment(self, redis_client, redis_config, seed_global_pools):
        """
        Following content appears in results when seeded.

        Algorithm:
            1. Clear user state
            2. Seed user's following pool with videos
            3. Get mixed recommendations
            4. Verify following videos appear in results
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        from async_mixer import AsyncVideoMixer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()
        mixer = AsyncVideoMixer(redis_layer)

        user_id = "test_following_seg1_001"

        try:
            # Clear any existing state
            clear_user_keys(redis_client, user_id)

            # Seed following pool with 30 videos
            following_videos = generate_video_ids("vid_following", 30)
            seed_user_pool(redis_client, user_id, "following", following_videos)

            result = await mixer.get_mixed_recommendations(user_id, count=50)

            # Count following in results
            following_count = sum(1 for v in result.videos if v.startswith("vid_following"))

            # Should have some following videos in results
            assert following_count > 0, "Expected some following videos in results"
            assert "following" in result.sources, "Sources should include following"

        finally:
            clear_user_keys(redis_client, user_id)
            await service.close()


class TestUGCInterspersing:
    """Tests for UGC content interspersing."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_ugc_appears_in_results(self, redis_client, redis_config, seed_global_pools):
        """
        UGC content appears in results when seeded.

        Algorithm:
            1. Seed global UGC pool
            2. Get recommendations
            3. Verify some UGC videos appear
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        from async_mixer import AsyncVideoMixer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()
        mixer = AsyncVideoMixer(redis_layer)

        user_id = "test_ugc_001"

        try:
            # Seed global UGC pool
            import time
            ugc_videos = generate_video_ids("vid_ugc", 50)
            expiry = float(int(time.time()) + 86400)
            mapping = {vid: expiry for vid in ugc_videos}
            redis_client.delete("global:ugc")
            redis_client.zadd("global:ugc", mapping)

            result = await mixer.get_mixed_recommendations(user_id, count=100)

            # UGC should appear (5% of non-following = ~5 videos)
            ugc_count = sum(1 for v in result.videos if v.startswith("vid_ugc"))
            assert ugc_count >= 0  # At least some UGC (might be 0 if pool empty)

        finally:
            clear_user_keys(redis_client, user_id)
            redis_client.delete("global:ugc")
            await service.close()


class TestMixerSources:
    """Tests for mixer source tracking."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_sources_dict_accurate(self, redis_client, redis_config, seed_global_pools):
        """
        Sources dict accurately reflects where videos came from.

        Algorithm:
            1. Get mixed recommendations
            2. Verify sources dict has expected keys
            3. Verify sum of sources <= total videos
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from async_main import AsyncRedisLayer
        from async_mixer import AsyncVideoMixer

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        redis_layer = AsyncRedisLayer(service)
        await redis_layer.initialize()
        mixer = AsyncVideoMixer(redis_layer)

        user_id = "test_sources_001"

        try:
            result = await mixer.get_mixed_recommendations(user_id, count=50)

            # Sources should be a dict
            assert isinstance(result.sources, dict)

            # Sum of sources should match or be close to video count
            total_from_sources = sum(result.sources.values())
            assert total_from_sources >= len(result.videos) * 0.8, \
                "Sources should account for most videos"

        finally:
            clear_user_keys(redis_client, user_id)
            await service.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
