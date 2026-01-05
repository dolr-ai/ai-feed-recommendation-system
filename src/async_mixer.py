##
"""
ASYNC Production Video Feed Mixer for the recommendation system.

This is the async version of mixer.py, preserving ALL business logic exactly.
Only changes: added async/await for non-blocking Redis operations.

This module handles mixing videos from different feed types (popularity
and freshness) based on configurable ratios. It ensures diversity
in recommendations while maintaining quality.

The mixer:
1. Fetches videos from different feed types
2. Applies mixing ratios (60% popularity, 40% freshness)
3. Handles insufficient videos gracefully
4. Deduplicates and shuffles results
5. Falls back to global pools when needed

No simulation data - production ready for real data from BigQuery/Redis.
"""

import random
import logging
import time
import asyncio
from typing import List, Dict, Optional
from dataclasses import dataclass

from async_main import AsyncRedisLayer
from config import (
    MIXER_RATIOS,
    FALLBACK_RATIOS,
    VIDEOS_PER_REQUEST,
    FOLLOWING_SYNC_COOLDOWN,
    FOLLOWING_REFILL_THRESHOLD,
    FOLLOWING_SEGMENT_1_MIN,
    FOLLOWING_SEGMENT_1_MAX,
    FOLLOWING_SEGMENT_2_MIN,
    UGC_RATIO,
)

logger = logging.getLogger(__name__)


@dataclass
class MixerResult:
    """Result of the mixing operation."""
    videos: List[str]
    sources: Dict[str, int]  # Count of videos from each source
    insufficient_feeds: List[str]  # Feeds that had insufficient videos
    used_fallback: bool


class AsyncVideoMixer:
    """
    ASYNC version - Mixes videos from different feed types to create diverse recommendations.

    The mixer fetches videos from popularity and freshness feeds,
    then combines them based on configured ratios. It handles edge cases like
    insufficient videos and ensures no duplicates in the final result.

    Algorithm (SAME AS SYNC):
    1. Calculate target counts for each feed based on ratios
    2. Fetch popularity videos (60% of total)
    3. Fetch freshness videos (40% of total)
    4. Use fallback if insufficient
    5. Shuffle and return
    """

    def __init__(self, redis_layer: AsyncRedisLayer, kvrocks_service=None):
        """
        Initialize the async video mixer.

        Args:
            redis_layer: Async Redis layer for fetching videos from feeds
            kvrocks_service: Optional AsyncKVRocksService for background jobs
        """
        self.redis = redis_layer
        self.kvrocks_service = kvrocks_service
        logger.info("AsyncVideoMixer initialized (production mode)")

    async def _maybe_trigger_following_sync(self, user_id: str) -> None:
        """
        Check if following pool needs sync and trigger background job if rate limit allows.

        This is ON-DEMAND only - NOT a periodic job for all users.
        Only triggers sync when BOTH conditions are true:
        1. Pool size < FOLLOWING_REFILL_THRESHOLD (10 videos)
        2. Last sync was >= FOLLOWING_SYNC_COOLDOWN (10 minutes) ago (or never synced)

        Algorithm:
            1. Get pool size for user's following pool
            2. If pool size >= threshold, return (no sync needed)
            3. Check last sync timestamp
            4. If within cooldown period, return (rate limited)
            5. Update last_sync timestamp FIRST (prevents concurrent triggers)
            6. Fire-and-forget background sync task

        Args:
            user_id: User ID to check and potentially sync
        """
        pool_key = self.redis._key_videos_to_show(user_id, "following")
        last_sync_key = self.redis._key_following_last_sync(user_id)

        # Check pool size
        pool_size = await self.redis.db.zcard(pool_key)

        if pool_size >= FOLLOWING_REFILL_THRESHOLD:
            # Pool has enough videos, no sync needed
            return

        # Check rate limit (10 minutes between syncs)
        last_sync = await self.redis.db.get(last_sync_key)
        now = int(time.time())

        if last_sync is not None:
            try:
                last_sync_time = int(last_sync)
                if (now - last_sync_time) < FOLLOWING_SYNC_COOLDOWN:
                    # Within cooldown period, skip
                    logger.debug(
                        f"Following sync rate-limited for {user_id} "
                        f"(last sync {now - last_sync_time}s ago, cooldown={FOLLOWING_SYNC_COOLDOWN}s)"
                    )
                    return
            except (ValueError, TypeError):
                pass  # Invalid last_sync value, proceed with sync

        # Update last_sync timestamp FIRST (prevents concurrent triggers)
        await self.redis.db.set(last_sync_key, str(now), ex=FOLLOWING_SYNC_COOLDOWN * 2)

        # Trigger background sync for THIS USER only (fire-and-forget)
        asyncio.create_task(self._background_sync_following(user_id)) #TODO : Sanitize this function
        logger.info(f"Triggered following sync for user {user_id} (pool_size={pool_size})")

    async def _background_sync_following(self, user_id: str) -> None:
        """
        Background task to sync user's following pool from BigQuery.

        This is a fire-and-forget task that runs asynchronously without blocking
        the API response.

        Algorithm:
            1. Import sync job (lazy import to avoid circular deps)
            2. Call sync_user_following_pool() with redis_layer and kvrocks_service
            3. Log results
            4. Handle errors gracefully (log but don't crash)

        Args:
            user_id: User to sync following pool for
        """
        try:
            # Lazy import to avoid circular dependencies
            from background_jobs import sync_user_following_pool

            if self.kvrocks_service is None:
                logger.warning(f"Cannot sync following for {user_id}: kvrocks_service not available")
                return

            stats = await sync_user_following_pool(
                self.redis,
                self.kvrocks_service,
                user_id
            )
            logger.info(f"Background following sync completed for {user_id}: {stats}")

        except Exception as e:
            # Log but don't crash - this is best-effort background work
            logger.warning(f"Background following sync failed for {user_id}: {e}")

    async def getMixedRecommendation(
        self,
        user_id: str,
        count: int = VIDEOS_PER_REQUEST,
        ratios: Optional[Dict[str, float]] = None
    ) -> MixerResult:
        """
        Get mixed video recommendations with following and UGC content prioritization.

        This function implements position-based mixing that prioritizes content:
        - Following: position-based (3-5 in first 10 slots, 10+ in next 40)
        - UGC: 5% of remaining slots after following (interspersed throughout)
        - Pop/Fresh: 60/40 split of what's left

        CRITICAL ORDER: Pull following videos FIRST (triggers cooldown), then
        UGC, then fill remaining slots with popularity/freshness.

        Algorithm:
            1. Check if following pool needs sync (rate-limited, fire-and-forget)
            2. Pull following videos FIRST (triggers cooldown)
            3. Calculate remaining slots after following
            4. Pull UGC videos (5% of remaining slots)
            5. Calculate remaining for pop/fresh (60/40 split)
            6. Construct segments with position-based mixing
            7. Intersperse UGC throughout the non-following portion
            8. Shuffle within segments, combine, and return

        Args:
            user_id: User ID
            count: Number of videos to return (default from config)
            ratios: Optional custom ratios for popularity/freshness (defaults to 60/40)

        Returns:
            MixerResult with mixed videos and source breakdown
        """
        # Step 0: Check if following pool needs sync (rate-limited, fire-and-forget)
        await self._maybe_trigger_following_sync(user_id)

        # Use production ratios for non-following: 60% popularity, 40% freshness
        if ratios is None:
            ratios = {"popularity": 0.60, "freshness": 0.40}

        # Track results
        sources = {}
        insufficient_feeds = []
        used_fallback = False

        # Step 1: Pull following videos FIRST (triggers cooldown, unchanged)
        following_needed = min(30, count // 3)
        following_videos = await self.redis.fetch_videos(user_id, "following", following_needed)
        sources["following"] = len(following_videos)

        if len(following_videos) < following_needed:
            insufficient_feeds.append("following")
            logger.debug(f"Following feed insufficient: {len(following_videos)}/{following_needed}")

        # Step 2: Calculate remaining slots after following
        remaining_slots = count - len(following_videos)

        # Step 3: Pull UGC videos (5% of remaining slots, not total)
        ugc_needed = max(1, int(remaining_slots * UGC_RATIO))
        ugc_videos = await self.redis.fetch_videos(user_id, "ugc", ugc_needed)
        sources["ugc"] = len(ugc_videos)

        if len(ugc_videos) < ugc_needed:
            insufficient_feeds.append("ugc")
            logger.debug(f"UGC feed insufficient: {len(ugc_videos)}/{ugc_needed}")

        # Step 4: Calculate segment allocations for following
        # Segment 1 (first 10 positions): 3-5 from following
        # Segment 2 (positions 11-50): remainder of following (target 10+)
        seg1_size = min(10, count)
        seg2_size = max(0, count - seg1_size)

        # Allocate following to segments (max 5 in seg1, rest in seg2)
        seg1_following_count = min(len(following_videos), FOLLOWING_SEGMENT_1_MAX)
        # Ensure minimum of 3 in seg1 if we have enough following videos
        seg1_following_count = max(seg1_following_count, min(FOLLOWING_SEGMENT_1_MIN, len(following_videos)))
        seg2_following_count = len(following_videos) - seg1_following_count

        seg1_following = following_videos[:seg1_following_count]
        seg2_following = following_videos[seg1_following_count:]

        # Step 5: Calculate remaining needs for popularity/freshness
        # Remaining = count - following - ugc
        slots_for_pop_fresh = remaining_slots - len(ugc_videos)
        pop_needed = int(slots_for_pop_fresh * ratios["popularity"])
        fresh_needed = slots_for_pop_fresh - pop_needed

        popularity_videos = await self.redis.fetch_videos(user_id, "popularity", pop_needed)
        sources["popularity"] = len(popularity_videos)
        if len(popularity_videos) < pop_needed:
            insufficient_feeds.append("popularity")

        freshness_videos = await self.redis.fetch_videos(user_id, "freshness", fresh_needed)
        sources["freshness"] = len(freshness_videos)
        if len(freshness_videos) < fresh_needed:
            insufficient_feeds.append("freshness")

        # Combine pop + fresh videos and shuffle
        other_videos = popularity_videos + freshness_videos
        random.shuffle(other_videos)

        # Step 6: Construct segments
        seg1_remaining = seg1_size - len(seg1_following)
        segment_1 = list(seg1_following) + other_videos[:seg1_remaining]
        random.shuffle(segment_1)

        segment_2 = list(seg2_following) + other_videos[seg1_remaining:]
        random.shuffle(segment_2)

        # Combine segments
        all_videos = segment_1 + segment_2

        # Step 7: Intersperse UGC throughout the non-following portion
        if ugc_videos:
            all_videos = self._intersperse_ugc(all_videos, ugc_videos, count)

        # Step 8: Use fallback if still need more
        remaining_needed = count - len(all_videos)
        if remaining_needed > 0:
            fallback_videos = await self._fetch_fallback(user_id, remaining_needed)
            all_videos.extend(fallback_videos)
            sources["fallback"] = len(fallback_videos)
            used_fallback = True

            if len(fallback_videos) < remaining_needed:
                logger.warning(f"Even fallback insufficient: {len(fallback_videos)}/{remaining_needed}")

        # Trim to requested count (in case we fetched too many)
        all_videos = all_videos[:count]

        # Log the result
        logger.info(
            f"Mixed {len(all_videos)}/{count} videos for user {user_id}. "
            f"Sources: {sources} (following in seg1={len(seg1_following)}, seg2={len(seg2_following)}, ugc={len(ugc_videos)})"
        )

        return MixerResult(
            videos=all_videos,
            sources=sources,
            insufficient_feeds=insufficient_feeds,
            used_fallback=used_fallback
        )

    def _intersperse_ugc(
        self,
        videos: List[str],
        ugc_videos: List[str],
        total_count: int
    ) -> List[str]:
        """
        Intersperse UGC videos throughout the feed at roughly even intervals.

        This ensures UGC content is distributed across the feed rather than
        clustered at the beginning or end.

        Algorithm:
            1. Calculate interval: len(videos) / (len(ugc) + 1)
            2. Insert each UGC video at position: interval * (i + 1)
            3. Adjust for list bounds

        Args:
            videos: Base video list (pop + fresh + following)
            ugc_videos: UGC videos to intersperse
            total_count: Target total count

        Returns:
            Combined list with UGC interspersed
        """
        if not ugc_videos:
            return videos

        result = list(videos)
        interval = max(1, len(result) // (len(ugc_videos) + 1))

        for i, ugc_vid in enumerate(ugc_videos):
            insert_pos = min((i + 1) * interval, len(result))
            result.insert(insert_pos, ugc_vid)

        return result

    async def get_mixed_recommendations(
        self,
        user_id: str,
        count: int = VIDEOS_PER_REQUEST,
        ratios: Optional[Dict[str, float]] = None
    ) -> MixerResult:
        """
        ASYNC VERSION - Get mixed video recommendations using production ratios.

        This method provides backward compatibility with the existing API.
        It delegates to getMixedRecommendation() with appropriate ratios.

        For production, we ignore similarity feed (future VectorDB integration)
        and use only popularity and freshness.

        Args:
            user_id: User ID
            count: Number of videos to return
            ratios: Custom mixing ratios (uses production defaults if None)

        Returns:
            MixerResult with mixed videos and metadata
        """
        # Use production ratios if not specified
        if ratios is None:
            ratios = {"popularity": 0.60, "freshness": 0.40}

        return await self.getMixedRecommendation(user_id, count, ratios)

    def _calculate_feed_counts(self, total: int, ratios: Dict[str, float]) -> Dict[str, int]:
        """
        Calculate how many videos to fetch from each feed.

        No changes from sync version (pure logic, no Redis).

        Args:
            total: Total videos needed
            ratios: Mixing ratios for each feed type

        Returns:
            Dictionary mapping feed type to count
        """
        counts = {}

        # Calculate counts based on ratios
        for feed_type, ratio in ratios.items():
            counts[feed_type] = int(total * ratio)

        # Adjust for rounding errors
        total_calculated = sum(counts.values())
        if total_calculated < total:
            # Add remainder to the largest feed
            largest_feed = max(ratios.keys(), key=lambda k: ratios[k])
            counts[largest_feed] += total - total_calculated

        return counts

    async def _fetch_fallback(self, user_id: str, count: int) -> List[str]:
        """
        ASYNC VERSION - Fetch videos from fallback pool.

        Used when primary feeds don't have enough videos.

        Args:
            user_id: User ID
            count: Number of videos needed

        Returns:
            List of fallback video IDs
        """
        logger.info(f"Fetching {count} videos from fallback for user {user_id}")

        fallback_videos = await self.redis.fetch_videos(
            user_id, "fallback", count
        )

        if len(fallback_videos) < count:
            logger.warning(f"Even fallback has insufficient videos: {len(fallback_videos)}/{count}")

        return fallback_videos

    async def get_feed_stats(self, user_id: str) -> Dict:
        """
        ASYNC VERSION - Get statistics about feed availability for a user.

        Args:
            user_id: User ID

        Returns:
            Dictionary with feed statistics including:
            - Valid video counts per feed (including following)
            - Whether feeds need refill
            - Current percentile pointer
            - Bloom filter status
            - Following sync status
        """
        from config import REFILL_THRESHOLD

        pop_count = await self.redis.count_valid_videos_to_show(user_id, "popularity")
        fresh_count = await self.redis.count_valid_videos_to_show(user_id, "freshness")
        fallback_count = await self.redis.count_valid_videos_to_show(user_id, "fallback")
        following_count = await self.redis.count_valid_videos_to_show(user_id, "following")
        ugc_count = await self.redis.count_valid_videos_to_show(user_id, "ugc")

        # Get following sync status
        last_sync_key = self.redis._key_following_last_sync(user_id)
        last_sync = await self.redis.db.get(last_sync_key)
        last_sync_time = int(last_sync) if last_sync else None

        stats = {
            "popularity": {
                "valid_count": pop_count,
                "needs_refill": pop_count < REFILL_THRESHOLD,
                "percentile_pointer": await self.redis.get_pop_percentile_pointer(user_id),
            },
            "freshness": {
                "valid_count": fresh_count,
                "needs_refill": fresh_count < REFILL_THRESHOLD,
            },
            "fallback": {
                "valid_count": fallback_count,
                "needs_refill": fallback_count < REFILL_THRESHOLD,
            },
            "ugc": {
                "valid_count": ugc_count,
                "needs_refill": ugc_count < 10,  # Smaller threshold for UGC (5% of feed)
            },
            "following": {
                "valid_count": following_count,
                "needs_sync": following_count < FOLLOWING_REFILL_THRESHOLD,
                "last_sync_timestamp": last_sync_time,
            },
            "bloom_filter_exists": await self.redis.db.exists(
                self.redis._key_bloom_permanent(user_id)
            ),
        }

        return stats


##

if __name__ == "__main__":
    # Test the async production mixer
    import os
    import re
    import json
    import asyncio
    from pathlib import Path
    from utils.async_redis_utils import AsyncKVRocksService
    from dotenv import load_dotenv
    
    # Load .env from project root (relative to this script's location)
    env_path = Path(__file__).parent.parent / ".env"
    load_dotenv(env_path)
    
    # Manually load SERVICE_CRED from .env to handle escape sequences properly
    # python-dotenv with single quotes doesn't interpret \n as newlines
    if env_path.exists():
        env_content = env_path.read_text()
        match = re.search(r"SERVICE_CRED=['\"](.+?)['\"](?:\n|$)", env_content, re.DOTALL)
        if match:
            service_cred = match.group(1)
            try:
                json.loads(service_cred)  # Validate JSON before setting
                os.environ["SERVICE_CRED"] = service_cred
                print("SERVICE_CRED loaded successfully from .env")
            except json.JSONDecodeError as e:
                print(f"SERVICE_CRED JSON invalid: {e}")
                print(f"Problematic chars around position {e.pos}: {repr(service_cred[max(0,e.pos-10):e.pos+10])}")

    logging.basicConfig(level=logging.INFO)

    async def test_async_mixer():
        """Test the async mixer with production ratios."""
        # Initialize components
        os.environ.setdefault("KVROCKS_HOST", "localhost")
        os.environ.setdefault("KVROCKS_PORT", "6379")
        os.environ.setdefault("KVROCKS_PASSWORD", "")

        kvrocks = AsyncKVRocksService(
            ssl_enabled=os.environ.get("KVROCKS_TLS_ENABLED", "false").lower() == "true",
            cluster_enabled=os.environ.get("KVROCKS_CLUSTER_ENABLED", "false").lower() == "true",
        )
        client = await kvrocks.connect()

        redis_layer = AsyncRedisLayer(kvrocks)
        await redis_layer.initialize()

        # Pass kvrocks_service to enable background following sync

        mixer = AsyncVideoMixer(redis_layer, kvrocks_service=kvrocks)

        # Test mixing for a user with production ratios
        test_user = "dqh4f-d6xax-w7tfa-l5vp2-3pyzx-gpap3-gdbrk-ninow-yk7ig-qj3b7-rqe"

        # Test the new getMixedRecommendation function (with following integration)
        result = await mixer.getMixedRecommendation(test_user, count=50)

        print(f"\nAsync production mixed recommendations for {test_user}:")
        print(f"  Total videos: {len(result.videos)}")
        print(f"  Sources: {result.sources}")
        print(f"  Following: {result.sources.get('following', 0)}")
        print(f"  Popularity: {result.sources.get('popularity', 0)}")
        print(f"  Freshness: {result.sources.get('freshness', 0)}")
        print(f"  Insufficient feeds: {result.insufficient_feeds}")
        print(f"  Used fallback: {result.used_fallback}")
        print(f"  Sample videos: {result.videos[:5] if result.videos else 'None'}")

        # Test feed stats
        stats = await mixer.get_feed_stats(test_user)
        print(f"\nFeed statistics:")
        for feed_type, feed_stats in stats.items():
            if isinstance(feed_stats, dict):
                print(f"  {feed_type}: {feed_stats}")

        # Cleanup
        await kvrocks.close()

    # Run the async test
    asyncio.run(test_async_mixer())
