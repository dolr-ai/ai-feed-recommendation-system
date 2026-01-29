"""
Async Recommendation System - Redis Layer Implementation

This is the ASYNC version of main.py with ALL business logic preserved exactly.
Only change is adding async/await for non-blocking I/O operations.

This module handles all Redis operations for the video recommendation system.
It uses Lua scripts for atomic operations and implements the following data structures:

Per User:
    1. short_lived_ttl_videos_watched_set: Sorted set with TTL tracking (1 day)
    2. permanent_bloom_filter: Bloom filter for confirmed watched videos (permanent)
    3. videos_to_show_set: Sorted set with individual video TTLs (3 days max)

Global:
    - Global video pools organized by engagement score and freshness

Key Design Patterns:
    - Sorted sets with scores as expiry timestamps for individual TTL management
    - Lazy cleanup of expired entries during fetch operations
    - Lua scripts to minimize round trips to Redis
    - User-level bloom filters (1% error rate, 1M capacity)

Lua Script Logging:
    - Set ENABLE_LUA_SCRIPT_LOGGING = True to see detailed Lua script execution logs
    - Logs include: script name, keys, args, result, result type, and result length
"""

import os
import time
import random
import json
import logging
import asyncio
import traceback
from typing import List, Dict, Tuple, Optional, Any, Union
from utils.async_redis_utils import AsyncKVRocksService

logger = logging.getLogger(__name__)


# ============================================================================
# CONFIGURATION CONSTANTS (EXACT SAME AS SYNC VERSION)
# ============================================================================

# TTL values (in seconds)
TTL_WATCHED_SET = 24 * 60 * 60  # 1 day
TTL_VIDEOS_TO_SHOW = 3 * 24 * 60 * 60  # 3 days
TTL_PERCENTILE_POINTER = 24 * 60 * 60  # 1 day reset for popularity percentile pointer

# Popularity bucket TTL (in seconds) - tier-based expiry for gradual refresh
POPULARITY_BUCKET_TTL = {
    "99_100": 1 * 24 * 60 * 60,   # 1 day - most popular, refresh frequently
    "90_99": 2 * 24 * 60 * 60,    # 2 days
    "80_90": 2 * 24 * 60 * 60,    # 2 days
    "70_80": 3 * 24 * 60 * 60,    # 3 days
    "60_70": 3 * 24 * 60 * 60,    # 3 days
    "50_60": 3 * 24 * 60 * 60,    # 3 days
    "40_50": 4 * 24 * 60 * 60,    # 4 days
    "30_40": 4 * 24 * 60 * 60,    # 4 days
    "20_30": 5 * 24 * 60 * 60,    # 5 days
    "10_20": 5 * 24 * 60 * 60,    # 5 days
    "0_10": 5 * 24 * 60 * 60,     # 5 days - least popular, longer tail
}

# Import configuration from centralized config
from config import (
    VIDEOS_TO_SHOW_CAPACITY,
    REFILL_THRESHOLD,
    VIDEOS_PER_REQUEST,
    ENABLE_LUA_SCRIPT_LOGGING,
    REDIS_SEMAPHORE_SIZE,
    PERCENTILE_BUCKETS,
    FRESHNESS_WINDOWS,
    BLOOM_ERROR_RATE,
    BLOOM_INITIAL_CAPACITY,
    BLOOM_EXPANSION,
    BLOOM_TTL_DAYS,
    TTL_FOLLOWING_VIDEOS,
    FOLLOWING_SYNC_COOLDOWN,
    FOLLOWING_REFILL_THRESHOLD,
    TTL_UGC_VIDEOS,
    TOURNAMENT_TTL_SECONDS,
    TOURNAMENT_DURATION_SECONDS,
    TOURNAMENT_RETENTION_SECONDS,
    TOURNAMENT_VIDEO_REUSE_COOLDOWN_SECONDS,
    EXCLUDE_SET_KEY,
    # UGC Discovery Pool config
    UGC_DISCOVERY_PUSH_PENALTY,
    TTL_UGC_USER_POOL,
)

# TTL safety margin (consider videos with < 1% TTL remaining as expired)
TTL_SAFETY_MARGIN = 0.01


# ============================================================================
# ASYNC LUA SCRIPTS (SAME SCRIPTS, ASYNC EXECUTION)
# ============================================================================


class AsyncLuaScripts:
    """
    Container for all Lua scripts used in Redis operations (ASYNC version).
    Scripts are registered on initialization and called as await script_obj(keys, args).

    ALL LUA SCRIPTS ARE EXACTLY THE SAME AS SYNC VERSION.
    Only the execution is now async.

    Lua Script Logging:
        Set enable_logging=True to see detailed execution logs for each Lua script call.
    """

    def __init__(self, redis_client, enable_logging: bool = ENABLE_LUA_SCRIPT_LOGGING):
        """
        Initialize and register all Lua scripts (async version).

        Args:
            redis_client: Async Redis client instance
            enable_logging: Enable detailed logging for Lua script execution
        """
        self.client = redis_client
        self.enable_logging = enable_logging

        # Script objects will be initialized in async initialize method
        self.fetch_and_consume = None
        self.add_videos_with_ttl = None
        self.filter_and_add_videos = None
        self.count_valid_videos = None
        self.check_videos_not_watched = None

    async def initialize(self):
        """
        Async initialization of all Lua scripts.
        Must be called after creating the AsyncLuaScripts instance.
        """
        # Register all scripts (async)
        # Note: refill scripts removed - cluster-safe refills now use Python + filter_and_add_videos
        self.fetch_and_consume = self._register_fetch_and_consume()
        self.add_videos_with_ttl = self._register_add_videos_with_ttl()
        self.filter_and_add_videos = self._register_filter_and_add_videos()
        self.count_valid_videos = self._register_count_valid_videos()
        self.check_videos_not_watched = self._register_check_videos_not_watched()

        logger.info(
            f"Async Lua scripts registered successfully (logging={'ON' if self.enable_logging else 'OFF'})"
        )

    def _log_script_call(self, script_name: str, keys: List, args: List, result):
        """
        Log Lua script execution details if logging is enabled.
        (SAME AS SYNC VERSION - no change needed)

        Args:
            script_name: Name of the script
            keys: Keys passed to the script
            args: Arguments passed to the script
            result: Result returned by the script
        """
        if not self.enable_logging:
            return

        logger.info(f"[LUA] {script_name}")
        logger.info(f"  Keys: {keys}")
        logger.info(f"  Args: {args}")
        logger.info(f"  Result: {result}")
        logger.info(f"  Result Type: {type(result).__name__}")
        if isinstance(result, list):
            logger.info(f"  Result Length: {len(result)}")

    async def _execute_script(self, script, script_name: str, keys: List, args: List, semaphore=None):
        """
        Execute a Lua script with retry on errors (ASYNC VERSION).

        Algorithm:
        1. Attempt script execution (with semaphore if provided)
        2. On any error, wait and retry (up to 3 times)
        3. If all retries fail, raise last error (goes to Sentry)

        Args:
            script: Registered script object
            script_name: Name of the script for logging
            keys: Keys to pass to the script
            args: Arguments to pass to the script
            semaphore: Optional semaphore for connection pool protection

        Returns:
            Script execution result

        Raises:
            Last exception if all retries exhausted
        """
        max_retries = 3
        delay = 0.5
        last_error = None

        for attempt in range(max_retries):
            try:
                if semaphore:
                    async with semaphore:
                        result = await script(keys=keys, args=args)
                        self._log_script_call(script_name, keys, args, result)
                        return result
                else:
                    result = await script(keys=keys, args=args)
                    self._log_script_call(script_name, keys, args, result)
                    return result

            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    logger.warning(
                        f"Redis error in {script_name} (attempt {attempt+1}/{max_retries}), "
                        f"retrying in {delay}s: {str(e)[:100]}"
                    )
                    await asyncio.sleep(delay)
                    delay *= 2

        raise last_error

    def _register_fetch_and_consume(self):
        """
        Atomically fetch videos, remove from pool, and mark as consumed (cooldown).

        This is THE core fetch operation that ensures:
        1. Pool is ALWAYS clean (sent videos removed immediately)
        2. Videos can never be sent twice (marked for cooldown atomically)
        3. Client doesn't need to call any other methods

        Note: Exclude set filtering (reported + NSFW) is done in Python after this
        script returns, to avoid CROSSSLOT errors in cluster mode.

        Algorithm:
        1. Cleanup expired from to_show, watched
        2. Fetch candidates from to_show (up to max_to_check)
        3. For each candidate:
           - Check if in watched set or bloom filter
           - If unwatched:
             * Add to result list
             * ZREM from to_show (remove from pool)
             * ZADD to watched with cooldown TTL
           - If watched: ZREM from to_show (cleanup stale entry)
        4. Stop when we have requested_count OR checked all
        5. Return {videos, removed_from_pool, filtered_count}

        KEYS[1]: videos_to_show sorted set key
        KEYS[2]: short_lived_watched sorted set key (cooldown)
        KEYS[3]: permanent bloom filter key
        ARGV[1]: current timestamp
        ARGV[2]: number of videos requested
        ARGV[3]: max videos to check (buffer for filtering)
        ARGV[4]: cooldown TTL in seconds

        Returns: Table with {videos[], removed_from_pool, filtered_by_watched, filtered_by_bloom}
        """
        script = """
        local to_show_key = KEYS[1]
        local watched_key = KEYS[2]
        local bloom_key = KEYS[3]
        local current_time = tonumber(ARGV[1])
        local requested_count = tonumber(ARGV[2])
        local max_to_check = tonumber(ARGV[3])
        local cooldown_ttl = tonumber(ARGV[4])
        local cooldown_expiry = current_time + cooldown_ttl

        -- Cleanup expired entries
        redis.call('ZREMRANGEBYSCORE', to_show_key, '-inf', current_time)
        redis.call('ZREMRANGEBYSCORE', watched_key, '-inf', current_time)

        -- Fetch candidates from pool
        local candidates = redis.call('ZRANGEBYSCORE', to_show_key, current_time, '+inf', 'LIMIT', 0, max_to_check)

        local videos_to_send = {}
        local removed_from_pool = 0
        local filtered_by_watched = 0
        local filtered_by_bloom = 0

        -- Process each candidate
        for _, video_id in ipairs(candidates) do
            -- Stop if we have enough videos
            if #videos_to_send >= requested_count then
                break
            end

            -- Check if in short-lived watched set (cooldown)
            local in_watched = redis.call('ZSCORE', watched_key, video_id)

            if not in_watched then
                -- Check bloom filter (permanent watch history)
                local in_bloom = redis.call('BF.EXISTS', bloom_key, video_id)

                if in_bloom == 0 then
                    -- UNWATCHED - this video is sendable!
                    -- 1. Add to result
                    table.insert(videos_to_send, video_id)
                    -- 2. Remove from pool (atomic consumption)
                    redis.call('ZREM', to_show_key, video_id)
                    -- 3. Mark in cooldown set (prevent immediate re-send)
                    redis.call('ZADD', watched_key, cooldown_expiry, video_id)
                    removed_from_pool = removed_from_pool + 1
                else
                    -- In bloom - remove stale entry from pool (cleanup)
                    redis.call('ZREM', to_show_key, video_id)
                    filtered_by_bloom = filtered_by_bloom + 1
                end
            else
                -- In cooldown - remove stale entry from pool (cleanup)
                redis.call('ZREM', to_show_key, video_id)
                filtered_by_watched = filtered_by_watched + 1
            end
        end

        return {videos_to_send, removed_from_pool, filtered_by_watched, filtered_by_bloom}
        """
        return self.client.register_script(script)

    def _register_add_videos_with_ttl(self):
        """
        Add videos to sorted set with expiry timestamp as score.
        EXACT SAME LUA SCRIPT AS SYNC VERSION.

        KEYS[1]: videos_to_show sorted set key
        ARGV[1]: current timestamp
        ARGV[2]: TTL in seconds
        ARGV[3..N]: video IDs to add

        Returns: Number of videos added
        """
        script = """
        local key = KEYS[1]
        local current_time = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local expiry_time = current_time + ttl

        local count = 0
        for i = 3, #ARGV do
            local video_id = ARGV[i]
            -- Add with expiry_time as score
            redis.call('ZADD', key, expiry_time, video_id)
            count = count + 1
        end

        return count
        """
        return self.client.register_script(script)

    def _register_filter_and_add_videos(self):
        """
        Filter unwatched videos and add them to videos_to_show set.
        EXACT SAME LUA SCRIPT AS SYNC VERSION.
        ATOMIC operation - filters and adds in one Redis call.

        KEYS[1]: videos_to_show sorted set key
        KEYS[2]: short_lived_watched sorted set key
        KEYS[3]: permanent bloom filter key
        ARGV[1]: current timestamp
        ARGV[2]: TTL in seconds
        ARGV[3]: max_capacity (int) - maximum pool size to cap at after adding
        ARGV[4..N]: video IDs to filter and add

        Returns: Table with {added_count, filtered_by_watched, filtered_by_bloom, unwatched_videos}
        """
        script = """
        local to_show_key = KEYS[1]
        local watched_key = KEYS[2]
        local bloom_key = KEYS[3]
        local current_time = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local max_capacity = tonumber(ARGV[3])
        local expiry_time = current_time + ttl

        -- Helper function to cap pool size at max_capacity (keeps newest by expiry score)
        local function cap_pool_size(pool_key, capacity)
            if capacity and capacity > 0 then
                local current_size = redis.call('ZCARD', pool_key)
                if current_size > capacity then
                    redis.call('ZREMRANGEBYRANK', pool_key, 0, current_size - capacity - 1)
                end
            end
        end

        -- First, cleanup expired entries from watched set
        redis.call('ZREMRANGEBYSCORE', watched_key, '-inf', current_time)

        local unwatched = {}
        local filtered_by_watched = 0
        local filtered_by_bloom = 0

        -- Filter unwatched videos (video IDs start from ARGV[4])
        for i = 4, #ARGV do
            local video_id = ARGV[i]

            -- Check if in short-lived watched set
            local in_watched = redis.call('ZSCORE', watched_key, video_id)

            if not in_watched then
                -- Check bloom filter
                local in_bloom = redis.call('BF.EXISTS', bloom_key, video_id)

                if in_bloom == 0 then
                    -- Video is unwatched, add to list
                    table.insert(unwatched, video_id)
                else
                    -- Filtered by bloom filter (permanent watched)
                    filtered_by_bloom = filtered_by_bloom + 1
                end
            else
                -- Filtered by short-lived watched set
                filtered_by_watched = filtered_by_watched + 1
            end
        end

        -- Add unwatched videos to to_show set
        local added_count = 0
        for _, video_id in ipairs(unwatched) do
            redis.call('ZADD', to_show_key, expiry_time, video_id)
            added_count = added_count + 1
        end

        -- Cap pool size before returning
        cap_pool_size(to_show_key, max_capacity)

        -- Return detailed statistics
        return {added_count, filtered_by_watched, filtered_by_bloom, unwatched}
        """
        return self.client.register_script(script)

    def _register_count_valid_videos(self):
        """
        Count videos with sufficient TTL remaining in the pool.
        EXACT SAME LUA SCRIPT AS SYNC VERSION.

        Since pool is always clean (fetch_and_consume removes videos atomically),
        we only need to count videos with valid TTL. No filtering needed.

        KEYS[1]: videos_to_show sorted set key
        ARGV[1]: current timestamp
        ARGV[2]: safety margin (0.01 for 1%)
        ARGV[3]: max TTL

        Returns: Count of valid videos
        """
        script = """
        local to_show_key = KEYS[1]
        local current_time = tonumber(ARGV[1])
        local safety_margin = tonumber(ARGV[2])
        local max_ttl = tonumber(ARGV[3])

        -- Calculate minimum acceptable expiry time
        local min_acceptable_time = current_time + (max_ttl * safety_margin)

        -- Cleanup expired entries
        redis.call('ZREMRANGEBYSCORE', to_show_key, '-inf', current_time)

        -- Count videos with valid TTL (pool is always clean, no filtering needed)
        local count = redis.call('ZCOUNT', to_show_key, min_acceptable_time, '+inf')

        return count
        """
        return self.client.register_script(script)

    def _register_check_videos_not_watched(self):
        """
        Filter out videos that exist in watched set or bloom filter.
        EXACT SAME LUA SCRIPT AS SYNC VERSION.

        KEYS[1]: short_lived_watched sorted set key
        KEYS[2]: permanent bloom filter key
        ARGV[1]: current timestamp
        ARGV[2..N]: video IDs to check

        Returns: List of video IDs that are NOT watched
        """
        script = """
        local watched_key = KEYS[1]
        local bloom_key = KEYS[2]
        local current_time = tonumber(ARGV[1])

        -- First, cleanup expired entries from watched set
        redis.call('ZREMRANGEBYSCORE', watched_key, '-inf', current_time)

        local unwatched = {}
        for i = 2, #ARGV do
            local video_id = ARGV[i]

            -- Check if in short-lived watched set
            local in_watched = redis.call('ZSCORE', watched_key, video_id)

            if not in_watched then
                -- Check bloom filter
                local in_bloom = redis.call('BF.EXISTS', bloom_key, video_id)

                if in_bloom == 0 then
                    table.insert(unwatched, video_id)
                end
            end
        end

        return unwatched
        """
        return self.client.register_script(script)

    # REMOVED register_refill_from_popularity_buckets(), register_refill_from_freshness_cascade(), register_refill_from_single_source()
    # Due to CROSSSLOT errors, and incompatibility with KVROCKS. This piece is migrated to Python service that does the refill in cluster-safe manner.


# ============================================================================
# ASYNC REDIS LAYER (BUSINESS LOGIC PRESERVED)
# ============================================================================


class AsyncRedisLayer:
    """
    High-level async Redis operations for the recommendation system.
    Manages user-specific video recommendations and watch history.

    THIS IS THE ASYNC VERSION WITH ALL BUSINESS LOGIC PRESERVED.
    Only change is async/await for non-blocking I/O.
    """

    def __init__(
        self,
        kvrocks_service: AsyncKVRocksService,
        enable_lua_logging: bool = ENABLE_LUA_SCRIPT_LOGGING,
    ):
        """
        Initialize async Redis layer with KVRocks service.

        Args:
            kvrocks_service: AsyncKVRocksService instance
            enable_lua_logging: Enable detailed logging for Lua script execution
        """
        self.db = kvrocks_service
        self.client = None  # Will be set in async initialize
        self.scripts = None  # Will be set in async initialize
        self.enable_lua_logging = enable_lua_logging

    async def initialize(self):
        """
        Async initialization that must be called after creating AsyncRedisLayer.
        Sets up the client connection and initializes Lua scripts.
        """
        self.client = await self.db.get_client()
        self.scripts = AsyncLuaScripts(self.client, enable_logging=self.enable_lua_logging)
        await self.scripts.initialize()

        # Connection pool protection: Limit concurrent Redis operations
        from config import REDIS_SEMAPHORE_SIZE
        self.redis_semaphore = asyncio.Semaphore(REDIS_SEMAPHORE_SIZE)
        logger.info(f"AsyncRedisLayer initialized with semaphore size {REDIS_SEMAPHORE_SIZE}")

    # ------------------------------------------------------------------------
    # Key Generators (NO CHANGE NEEDED - just return strings)
    # ------------------------------------------------------------------------

    def _key_videos_to_show(self, user_id: str, rec_type: str = "default") -> str:
        """
        Generate key for user's videos_to_show sorted set.
        Uses hash tag {user:ID} for cluster-safe operations.

        Args:
            user_id: User ID
            rec_type: Recommendation type (e.g., 'popularity', 'freshness', 'fallback')
        """
        return f"{{user:{user_id}}}:videos_to_show:{rec_type}"

    def _key_watched_short_lived(self, user_id: str) -> str:
        """Generate key for user's short-lived watched set. Uses hash tag for cluster."""
        return f"{{user:{user_id}}}:watched:short"

    def _key_bloom_permanent(self, user_id: str) -> str:
        """Generate key for user's permanent bloom filter. Uses hash tag for cluster."""
        return f"{{user:{user_id}}}:bloom:permanent"

    def _key_global_pop_set(self, bucket: str) -> str:
        """Generate key for global popularity SET bucket. Uses {GLOBAL} hash tag for cluster."""
        return f"{{GLOBAL}}:pool:pop_{bucket}"

    def _key_global_fresh_zset(self, window: str) -> str:
        """Generate key for global freshness ZSET window. Uses {GLOBAL} hash tag for cluster."""
        return f"{{GLOBAL}}:pool:fresh_{window}"

    def _key_global_fallback_zset(self) -> str:
        """Generate key for global fallback ZSET. Uses {GLOBAL} hash tag for cluster."""
        return "{GLOBAL}:pool:fallback"

    def _key_global_ugc_zset(self) -> str:
        """Generate key for global UGC (User-Generated Content) ZSET. Uses {GLOBAL} hash tag."""
        return "{GLOBAL}:pool:ugc"

    def _key_global_ugc_discovery_zset(self) -> str:
        """Generate key for global UGC Discovery Pool ZSET. Uses {GLOBAL} hash tag."""
        return "{GLOBAL}:pool:ugc_discovery"

    def _key_global_ugc_discovery_pushes(self) -> str:
        """Generate key for UGC Discovery push counts HASH. Uses {GLOBAL} hash tag."""
        return "{GLOBAL}:ugc_discovery:pushes"

    def _key_global_ugc_discovery_timestamps(self) -> str:
        """Generate key for UGC Discovery upload timestamps HASH. Uses {GLOBAL} hash tag."""
        return "{GLOBAL}:ugc_discovery:timestamps"

    def _key_pop_percentile_pointer(self, user_id: str) -> str:
        """Pointer key for user's popularity percentile bucket. Uses hash tag for cluster."""
        return f"{{user:{user_id}}}:pop_percentile_pointer"

    def _key_refill_failures(self, user_id: str, rec_type: str) -> str:
        """Key for tracking consecutive refill failures. Uses hash tag for cluster."""
        return f"{{user:{user_id}}}:refill_failures:{rec_type}"

    def _key_refill_lock(self, user_id: str, rec_type: str) -> str:
        """Lock key for refilling a user's rec_type. Uses hash tag for cluster."""
        return f"{{user:{user_id}}}:refill:lock:{rec_type}"

    def _key_metrics_attempts(self, rec_type: str) -> str:
        return f"metrics:refill:attempts:{rec_type}"

    def _key_metrics_failures(self, rec_type: str) -> str:
        return f"metrics:refill:failures:{rec_type}"

    def _key_refill_last_stats(self, user_id: str, rec_type: str) -> str:
        """Key for refill stats. Uses hash tag for cluster."""
        return f"{{user:{user_id}}}:refill:last:{rec_type}"

    def _key_following_last_sync(self, user_id: str) -> str:
        """Key for tracking last sync timestamp for user's following pool. Uses hash tag."""
        return f"{{user:{user_id}}}:following:last_sync"

    def _key_tournament_videos(self, tournament_id: str) -> str:
        """Key for storing tournament video list."""
        return f"tournament:{tournament_id}:videos"

    def _key_tournament_meta(self, tournament_id: str) -> str:
        """Key for storing tournament metadata (created_at, video_count)."""
        return f"tournament:{tournament_id}:meta"

    def _key_tournament_recent_videos(self) -> str:
        """Key for tracking recently-used tournament videos (ZSET with timestamps)."""
        return "tournament:recent_videos"

    def _key_tournament_registry(self) -> str:
        """Key for central registry of all tournament IDs (ZSET with created_at timestamps)."""
        return "tournament:all"

    # ------------------------------------------------------------------------
    # Bloom Filter Operations (ASYNC VERSIONS)
    # ------------------------------------------------------------------------

    async def init_user_bloom_filter(self, user_id: str) -> bool:
        """
        Initialize bloom filter for a user if it doesn't exist.
        ASYNC VERSION - same business logic.

        The bloom filter is PERMANENT (no TTL/expiry) and persists indefinitely
        to track all videos the user has ever watched.

        Args:
            user_id: User ID

        Returns:
            True if created, False if already exists
        """
        bloom_key = self._key_bloom_permanent(user_id)

        if await self.db.exists(bloom_key):
            logger.debug(f"Bloom filter already exists for user {user_id}, refreshing TTL")
            # Refresh TTL for existing bloom filter (sliding expiry)
            await self.db.expire(bloom_key, BLOOM_TTL_DAYS * 86400)
            return False

        try:
            # Create bloom filter with EXPANSION parameter for auto-scaling
            await self.db.bf_reserve(
                bloom_key, BLOOM_ERROR_RATE, BLOOM_INITIAL_CAPACITY,
                expansion=BLOOM_EXPANSION
            )
            # Set initial TTL (will auto-delete after 30 days of inactivity)
            await self.db.expire(bloom_key, BLOOM_TTL_DAYS * 86400)
            logger.info(
                f"Created bloom filter for user {user_id} with expansion={BLOOM_EXPANSION}, "
                f"initial_capacity={BLOOM_INITIAL_CAPACITY}, TTL={BLOOM_TTL_DAYS} days"
            )
            return True
        except Exception as e:
            # Filter might already exist (race condition)
            if "item exists" in str(e).lower():
                logger.debug(f"Bloom filter created concurrently for user {user_id}")
                # Still set TTL even if created concurrently
                await self.db.expire(bloom_key, BLOOM_TTL_DAYS * 86400)
                return False
            raise

    async def mark_videos_permanently_watched(self, user_id: str, video_ids: List[str]) -> int:
        """
        Mark videos as permanently watched in the bloom filter.
        ASYNC VERSION - same business logic.

        These videos are stored PERMANENTLY (no expiry) to ensure the user
        never sees the same video twice, even across sessions.

        Args:
            user_id: User ID
            video_ids: List of video IDs to mark as permanently watched

        Returns:
            Number of videos marked as permanently watched
        """
        if not video_ids:
            return 0

        bloom_key = self._key_bloom_permanent(user_id)

        # Ensure bloom filter exists (will be permanent/no TTL)
        await self.init_user_bloom_filter(user_id)

        # Add videos in batch (these persist forever)
        results = await self.db.bf_madd(bloom_key, *video_ids)
        count = sum(results)

        # Refresh TTL on bloom filter (sliding expiry - keeps bloom alive for active users)
        await self.db.expire(bloom_key, BLOOM_TTL_DAYS * 86400)

        logger.debug(
            f"Added {count}/{len(video_ids)} videos to bloom for user {user_id}, TTL refreshed"
        )
        return count

    async def check_videos_in_bloom(
        self, user_id: str, video_ids: List[str]
    ) -> Dict[str, bool]:
        """
        Check which videos exist in the permanent bloom filter.
        ASYNC VERSION - same business logic.

        Args:
            user_id: User ID
            video_ids: List of video IDs to check

        Returns:
            Dict mapping video_id -> exists (True/False)
        """
        if not video_ids:
            return {}

        bloom_key = self._key_bloom_permanent(user_id)

        # Check if bloom filter exists
        if not await self.db.exists(bloom_key):
            return {vid: False for vid in video_ids}

        # Refresh TTL on bloom filter access (sliding expiry)
        await self.db.expire(bloom_key, BLOOM_TTL_DAYS * 86400)

        results = await self.db.bf_mexists(bloom_key, *video_ids)
        return dict(zip(video_ids, results))

    # ------------------------------------------------------------------------
    # Pointer Helpers (Popularity Percentiles) - ASYNC VERSIONS
    # ------------------------------------------------------------------------

    PERCENTILE_BUCKETS_ORDER = [
        "99_100",
        "90_99",
        "80_90",
        "70_80",
        "60_70",
        "50_60",
        "40_50",
        "30_40",
        "20_30",
        "10_20",
        "0_10",
    ]

    FRESHNESS_WINDOWS_ORDER = ["l1d", "l7d", "l14d", "l30d", "l90d"]

    async def get_pop_percentile_pointer(self, user_id: str) -> str:
        """
        Get user's popularity percentile pointer.
        ASYNC VERSION - same business logic.

        Algorithm (UNCHANGED):
        - Try to read string at `user:{user_id}:pop_percentile_pointer`.
        - If missing or invalid, default to highest bucket "99_100" and persist with 24h TTL.

        Returns:
        - Bucket string like "99_100".
        """
        key = self._key_pop_percentile_pointer(user_id)
        val = await self.db.get(key)
        if isinstance(val, bytes):
            val = val.decode("utf-8")
        if val and val in self.PERCENTILE_BUCKETS_ORDER:
            return val
        # Persist default with TTL so that it naturally resets every ~24h
        default_bucket = self.PERCENTILE_BUCKETS_ORDER[0]
        try:
            await self.db.set(key, default_bucket, ex=TTL_PERCENTILE_POINTER)
        except Exception:
            # Fail fast policy elsewhere; here we still return default if write fails
            pass
        return default_bucket

    async def set_pop_percentile_pointer(self, user_id: str, bucket: str) -> bool:
        """
        Set user's popularity percentile pointer.
        ASYNC VERSION - same business logic.

        Args:
        - bucket: one of PERCENTILE_BUCKETS_ORDER values

        Returns:
        - True if set succeeded.
        """
        if bucket not in self.PERCENTILE_BUCKETS_ORDER:
            raise ValueError(f"Invalid percentile bucket: {bucket}")
        key = self._key_pop_percentile_pointer(user_id)
        # Preserve existing TTL if present; otherwise set a fresh 24h TTL
        ttl_seconds = -2
        try:
            ttl_seconds = int(await self.db.ttl(key))
        except Exception:
            ttl_seconds = -2
        if ttl_seconds and ttl_seconds > 0:
            return bool(await self.db.set(key, bucket, ex=ttl_seconds))
        else:
            return bool(await self.db.set(key, bucket, ex=TTL_PERCENTILE_POINTER))

    async def downshift_pop_percentile_pointer(self, user_id: str) -> str:
        """
        Move pointer to next lower percentile bucket (softer content).
        ASYNC VERSION - same business logic.

        Returns the new bucket (or unchanged last bucket if already at bottom).
        """
        current = await self.get_pop_percentile_pointer(user_id)
        idx = self.PERCENTILE_BUCKETS_ORDER.index(current)
        if idx + 1 < len(self.PERCENTILE_BUCKETS_ORDER):
            new_bucket = self.PERCENTILE_BUCKETS_ORDER[idx + 1]
            # Preserve existing TTL window when downshifting
            await self.set_pop_percentile_pointer(user_id, new_bucket)
            return new_bucket
        return current

    # ------------------------------------------------------------------------
    # Exclude Set Operations (Reported + NSFW filtering)
    # ------------------------------------------------------------------------

    async def _filter_excluded_videos(
        self, video_ids: List[str]
    ) -> Tuple[List[str], int]:
        """
        Filter out videos that are in the exclude set (reported + NSFW).

        Uses SMISMEMBER for batch checking against the global exclude set.
        This is done in Python (not Lua) to avoid CROSSSLOT errors in cluster mode,
        since the exclude set uses {GLOBAL} hash tag while user keys use {user:ID}.

        Algorithm:
            1. Check if exclude set exists (fail open if not)
            2. Use SMISMEMBER to batch-check all video_ids
            3. Filter out videos that are in the exclude set
            4. Return filtered list and count of excluded videos

        Args:
            video_ids: List of video IDs to filter

        Returns:
            Tuple of (filtered_video_ids, excluded_count)
        """
        if not video_ids:
            return [], 0

        try:
            # Check if exclude set exists - fail open if not
            exists = await self.db.exists(EXCLUDE_SET_KEY)
            if not exists:
                return video_ids, 0

            # Batch check using SMISMEMBER (returns list of 0/1 for each video)
            results = await self.client.smismember(EXCLUDE_SET_KEY, video_ids)

            # Filter out excluded videos
            filtered = []
            excluded_count = 0
            for video_id, is_excluded in zip(video_ids, results):
                if is_excluded:
                    excluded_count += 1
                else:
                    filtered.append(video_id)

            if excluded_count > 0:
                logger.debug(f"Filtered {excluded_count} excluded videos from {len(video_ids)} candidates")

            return filtered, excluded_count

        except Exception as e:
            # Fail open - if exclude check fails, return all videos
            logger.warning(f"Exclude set check failed, returning all videos: {e}")
            return video_ids, 0

    # ------------------------------------------------------------------------
    # UGC Discovery Push Count Tracking
    # ------------------------------------------------------------------------

    async def _increment_ugc_push_counts(self, video_ids: List[str]) -> None:
        """
        Atomically increment push counts for UGC videos SERVED to users.

        This is called at FETCH TIME (after fetch_and_consume Lua returns videos)
        to ensure push_count reflects actual impressions (videos delivered to users),
        not potential impressions (videos added to pools).

        Algorithm:
            1. Use Redis HINCRBY (atomic at Redis level - serialized per key)
            2. Pipeline multiple increments for efficiency
            3. Fire-and-forget - don't block on result

        Thread-safe: YES
            - Redis HINCRBY is atomic per field
            - Pipeline batches execute atomically
            - No race conditions between workers

        Args:
            video_ids: List of video IDs actually served to the user
        """
        if not video_ids:
            return

        pushes_key = self._key_global_ugc_discovery_pushes()

        try:
            pipe = self.client.pipeline()
            for vid in video_ids:
                pipe.hincrby(pushes_key, vid, 1)
            await pipe.execute()

            logger.debug(f"Incremented push counts for {len(video_ids)} UGC videos")

        except Exception as e:
            # Non-critical - log and continue
            # Push counts are used for prioritization, not correctness
            logger.warning(f"Failed to increment UGC push counts: {e}")

    # ------------------------------------------------------------------------
    # Videos To Show Operations (MAIN OPERATIONS - ASYNC VERSIONS)
    # ------------------------------------------------------------------------

    async def fetch_videos(
        self,
        user_id: str,
        rec_type: str,
        count: int = VIDEOS_PER_REQUEST,
        auto_refill: bool = True,
        max_refill_attempts: int = 3,
    ) -> List[str]:
        """
        Atomically fetch and consume videos from user's pool with auto-refill.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        This is THE main fetch method that:
        1. Atomically fetches videos, removes from pool, marks as consumed
        2. Auto-refills if insufficient videos (up to max_refill_attempts)
        3. Tries fallback source if primary sources exhausted
        4. Returns guaranteed count OR logs exhaustion

        Pool is ALWAYS clean after this call - sent videos removed immediately.
        Client doesn't need to call any other methods - everything is handled atomically.

        Algorithm (UNCHANGED):
        1. Call fetch_and_consume Lua script (atomic fetch + remove + cooldown)
        2. If got requested count → return
        3. If insufficient AND auto_refill=True:
           a. Check valid count in pool
           b. If below threshold:
              - Call refill (refill_popularity/refill_freshness)
              - Retry fetch
              - Repeat up to max_refill_attempts
        4. If STILL insufficient:
           - Try fallback source
        5. Return videos (or log exhaustion if truly empty)

        Args:
            user_id: User ID
            rec_type: Recommendation type ('popularity', 'freshness', 'fallback')
            count: Number of videos requested
            auto_refill: Auto-refill if pool below threshold (default: True)
            max_refill_attempts: Max refill attempts before giving up (default: 3)

        Returns:
            List of video IDs (atomically consumed from pool)
        """
        await self.init_user_bloom_filter(user_id)

        to_show_key = self._key_videos_to_show(user_id, rec_type)
        watched_key = self._key_watched_short_lived(user_id)
        bloom_key = self._key_bloom_permanent(user_id)
        # max_to_check = min(count * 20, 10000)  # Buffer for watched/bloom filtering
        max_to_check = VIDEOS_TO_SHOW_CAPACITY

        videos = []
        total_removed = 0
        total_filtered_watched = 0
        total_filtered_bloom = 0
        total_filtered_exclude = 0

        # Fetch with retry loop to handle exclude filtering reducing count
        # Max 3 quick retries before falling back to auto-refill
        for fetch_attempt in range(3):
            needed = count - len(videos)
            if needed <= 0:
                break

            # Request extra to account for exclude filtering (50% buffer)
            fetch_count = int(needed * 1.5) + 10
            current_time = int(time.time())

            result = await self.scripts._execute_script(
                self.scripts.fetch_and_consume,
                "fetch_and_consume",
                keys=[to_show_key, watched_key, bloom_key],
                args=[current_time, fetch_count, max_to_check, TTL_WATCHED_SET],
                semaphore=self.redis_semaphore,
            )

            batch_videos = result[0]
            total_removed += result[1]
            total_filtered_watched += result[2]
            total_filtered_bloom += result[3]

            if not batch_videos:
                # Pool exhausted, break to trigger auto-refill
                break

            # Filter out excluded videos (reported + NSFW) - done in Python to avoid CROSSSLOT
            batch_videos, batch_excluded = await self._filter_excluded_videos(batch_videos)
            total_filtered_exclude += batch_excluded

            videos.extend(batch_videos)

            # If no videos were excluded this batch, pool is clean - no need to retry
            if batch_excluded == 0:
                break

        # Get pool count for logging and refill decision
        valid_count_in_pool = await self.count_valid_videos_to_show(user_id, rec_type)

        logger.info(
            f"Fetched {len(videos)}/{count} videos for {user_id}:{rec_type} "
            f"(removed={total_removed}, filtered_w={total_filtered_watched}, filtered_b={total_filtered_bloom}, filtered_ex={total_filtered_exclude}), "
            f"pool has {valid_count_in_pool} valid videos"
        )

        # Trigger background refill if pool is low (fire-and-forget, non-blocking)
        if valid_count_in_pool < REFILL_THRESHOLD and len(videos) >= count:
            asyncio.create_task(self._background_refill(user_id, rec_type))
            logger.info(
                f"Triggered background refill for {user_id}:{rec_type} "
                f"(pool={valid_count_in_pool}<{REFILL_THRESHOLD})"
            )

        # If got enough videos, return immediately
        if len(videos) >= count:
            return videos[:count]

        # Insufficient videos - try auto-refill
        if auto_refill and len(videos) < count:
            logger.info(f"Insufficient videos ({len(videos)}/{count}), trying auto-refill for {user_id}:{rec_type}")

            for attempt in range(1, max_refill_attempts + 1):
                # Check if refill needed
                valid_count = await self.count_valid_videos_to_show(user_id, rec_type)
                if valid_count >= REFILL_THRESHOLD/2:
                    logger.info(f"Pool has {valid_count} videos, no refill needed")
                    break

                logger.info(f"Refill attempt {attempt}/{max_refill_attempts} for {user_id}:{rec_type}")

                # Trigger appropriate refill
                try:
                    if rec_type == "popularity":
                        await self.refill_popularity(user_id, target=VIDEOS_TO_SHOW_CAPACITY)
                    elif rec_type == "freshness":
                        await self.refill_freshness(user_id, target=VIDEOS_TO_SHOW_CAPACITY)
                    elif rec_type == "fallback":
                        await self.refill_with_fallback(user_id, target=REFILL_THRESHOLD*10)
                    elif rec_type == "following":
                        # Following requires BigQuery - cannot auto-refill from RedisLayer
                        # Mixer handles this via _maybe_trigger_following_sync
                        logger.debug(f"Skipping auto-refill for {user_id}:following (requires BigQuery)")
                        break
                    elif rec_type == "ugc":
                        await self.refill_ugc(user_id, target=300)
                    else:
                        logger.warning(f"Unknown rec_type '{rec_type}', skipping refill")
                        break
                except Exception as e:
                    logger.error(f"Refill failed for {user_id}:{rec_type}: {e}")
                    break

                # Retry fetch after refill
                current_time = int(time.time())
                needed = count - len(videos) + 10  # Extra buffer for exclude filtering
                result = await self.scripts._execute_script(
                    self.scripts.fetch_and_consume,
                    "fetch_and_consume",
                    keys=[to_show_key, watched_key, bloom_key],
                    args=[current_time, needed, max_to_check, TTL_WATCHED_SET],
                    semaphore=self.redis_semaphore,
                )

                new_videos = result[0]
                removed = result[1]

                # Filter excluded videos
                new_filtered_exclude = 0
                if new_videos:
                    new_videos, new_filtered_exclude = await self._filter_excluded_videos(new_videos)

                logger.info(f"Refill attempt {attempt}: fetched {len(new_videos)} additional videos (removed={removed}, filtered_ex={new_filtered_exclude})")

                videos.extend(new_videos)

                if len(videos) >= count:
                    logger.info(f"Reached target count after refill attempt {attempt}")
                    return videos[:count]

            # Still insufficient after refills - try fallback if not already fallback
            if rec_type != "fallback" and len(videos) < count:
                logger.warning(
                    f"Primary source exhausted for {user_id}:{rec_type}, trying fallback "
                    f"(have {len(videos)}/{count})"
                )
                try:
                    await self.refill_with_fallback(user_id, target=REFILL_THRESHOLD)
                    fallback_key = self._key_videos_to_show(user_id, "fallback")
                    current_time = int(time.time())
                    needed = count - len(videos) + 10  # Extra buffer for exclude filtering
                    result = await self.scripts._execute_script(
                        self.scripts.fetch_and_consume,
                        "fetch_and_consume",
                        keys=[fallback_key, watched_key, bloom_key],
                        args=[current_time, needed, max_to_check, TTL_WATCHED_SET],
                        semaphore=self.redis_semaphore,
                    )
                    fallback_videos = result[0]

                    # Filter excluded videos
                    fallback_filtered_exclude = 0
                    if fallback_videos:
                        fallback_videos, fallback_filtered_exclude = await self._filter_excluded_videos(fallback_videos)

                    logger.info(f"Fetched {len(fallback_videos)} from fallback (filtered_ex={fallback_filtered_exclude})")
                    videos.extend(fallback_videos)
                except Exception as e:
                    logger.error(f"Fallback fetch failed: {e}")

        # Log final result
        if len(videos) < count:
            logger.warning(
                f"Could not fulfill request: got {len(videos)}/{count} videos for {user_id}:{rec_type} "
                f"(sources exhausted after {max_refill_attempts} refill attempts)"
            )
        else:
            logger.info(f"Successfully fetched {len(videos)} videos for {user_id}:{rec_type}")

        final_videos = videos[:count]  # Trim to requested count

        # Increment push counts for UGC videos actually served
        # This tracks ACTUAL impressions for priority-based discovery
        if rec_type == "ugc" and final_videos:
            await self._increment_ugc_push_counts(final_videos)

        return final_videos

    async def add_videos_to_show(
        self,
        user_id: str,
        video_ids: List[str],
        rec_type: str = "default",
        ttl: int = TTL_VIDEOS_TO_SHOW,
        filter_watched: bool = True,
    ) -> int:
        """
        Add videos to the user's to_show set with individual TTLs.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        IMPORTANT: By default, uses ATOMIC Lua script that:
        1. Filters out videos in permanent bloom filter (confirmed watched)
        2. Filters out videos in short-lived watched set (recently sent)
        3. Adds only unwatched videos with individual TTL

        All in ONE Redis call for maximum performance and atomicity.

        Args:
            user_id: User ID
            video_ids: List of video IDs to add
            rec_type: Recommendation type (e.g., 'popularity', 'freshness', 'fallback')
            ttl: Time-to-live in seconds
            filter_watched: If True, filter out watched videos before adding (default: True)

        Returns:
            Number of videos added (after filtering)
        """
        if not video_ids:
            return 0

        to_show_key = self._key_videos_to_show(user_id, rec_type)
        current_time = int(time.time())

        if filter_watched:
            # ATOMIC OPERATION: Filter and add in ONE Lua script call
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)

            # Ensure bloom filter exists before Lua script runs
            await self.init_user_bloom_filter(user_id)

            # Execute atomic filter + add operation
            result = await self.scripts._execute_script(
                self.scripts.filter_and_add_videos,
                "filter_and_add_videos",
                keys=[to_show_key, watched_key, bloom_key],
                args=[current_time, ttl] + video_ids,
                semaphore=self.redis_semaphore,
            )

            # Parse result: [added_count, filtered_by_watched, filtered_by_bloom, unwatched_videos_list]
            added_count = result[0]
            filtered_by_watched = result[1]
            filtered_by_bloom = result[2]
            total_filtered = filtered_by_watched + filtered_by_bloom

            # Log detailed filtering statistics
            if total_filtered > 0:
                logger.info(
                    f"Filter stats for user {user_id} (type: {rec_type}): "
                    f"Total={len(video_ids)}, Added={added_count}, "
                    f"Filtered={total_filtered} (watched_set={filtered_by_watched}, bloom={filtered_by_bloom})"
                )

            if added_count == 0:
                logger.info(
                    f"All {len(video_ids)} videos were already watched by user {user_id}, nothing to add (type: {rec_type})"
                )
            else:
                logger.debug(
                    f"Added {added_count} videos to show for user {user_id} (type: {rec_type})"
                )

            return added_count
        else:
            # Skip filtering, just add all videos
            count = await self.scripts._execute_script(
                self.scripts.add_videos_with_ttl,
                "add_videos_with_ttl",
                keys=[to_show_key],
                args=[current_time, ttl] + video_ids,
                semaphore=self.redis_semaphore,
            )

            logger.debug(
                f"Added {count} videos to show for user {user_id} (type: {rec_type}) WITHOUT filtering"
            )
            return count

    async def fetch_from_global_pools_raw(
        self,
        count: int = 1000
    ) -> Dict[str, Any]:
        """
        Fetch videos directly from global popularity and freshness pools without user-specific filtering.

        This method returns a 50:50 mix of videos from global pools for bulk caching,
        pre-warming, or analytics. NO user-specific filtering is applied (no bloom filter,
        no watched deduplication).

        Args:
            count: Total number of videos to fetch (max 10000)

        Returns:
            Dictionary with:
                - videos: List of video IDs (deduplicated and shuffled)
                - count: Actual number of videos returned
                - sources: Breakdown by popularity buckets and freshness windows
                - timestamp: Unix timestamp

        Algorithm:
            1. Calculate target: 50% from popularity, 50% from freshness
            2. Fetch from popularity buckets sequentially (99_100 → 90_99 → ...) until quota met
            3. Fetch from freshness windows sequentially (l1d → l7d → ...) until quota met
            4. If popularity didn't meet quota, fetch more from freshness
            5. If freshness didn't meet quota, fetch more from popularity
            6. Deduplicate and shuffle
        """
        current_time = int(time.time())

        # Calculate targets (50-50 split)
        pop_target = count // 2
        fresh_target = count - pop_target

        all_videos = set()
        sources = {
            "popularity": {},
            "freshness": {}
        }

        # Phase 1: Fetch from popularity buckets (top buckets first)
        pop_fetched = 0
        for bucket in PERCENTILE_BUCKETS:
            if pop_fetched >= pop_target:
                break

            remaining_needed = pop_target - pop_fetched
            bucket_key = self._key_global_pop_set(bucket)

            videos = await self._fetch_from_zset(bucket_key, remaining_needed, current_time)

            if videos:
                # Deduplicate at source level
                new_videos = [v for v in videos if v not in all_videos]
                all_videos.update(new_videos)
                sources["popularity"][bucket] = len(new_videos)
                pop_fetched += len(new_videos)

        # Phase 2: Fetch from freshness windows (freshest first)
        fresh_fetched = 0
        for window in FRESHNESS_WINDOWS:
            if fresh_fetched >= fresh_target:
                break

            remaining_needed = fresh_target - fresh_fetched
            window_key = self._key_global_fresh_zset(window)

            videos = await self._fetch_from_zset(window_key, remaining_needed, current_time)

            if videos:
                # Deduplicate at source level
                new_videos = [v for v in videos if v not in all_videos]
                all_videos.update(new_videos)
                sources["freshness"][window] = len(new_videos)
                fresh_fetched += len(new_videos)

        # Phase 3: Fallback - if popularity didn't meet quota, fetch more from freshness
        if pop_fetched < pop_target:
            pop_deficit = pop_target - pop_fetched
            logger.info(f"Popularity deficit: {pop_deficit}, fetching more from freshness")

            for window in FRESHNESS_WINDOWS:
                if pop_deficit <= 0:
                    break

                window_key = self._key_global_fresh_zset(window)
                videos = await self._fetch_from_zset(window_key, pop_deficit, current_time)

                if videos:
                    new_videos = [v for v in videos if v not in all_videos]
                    all_videos.update(new_videos)
                    sources["freshness"][window] = sources["freshness"].get(window, 0) + len(new_videos)
                    pop_deficit -= len(new_videos)

        # Phase 4: Fallback - if freshness didn't meet quota, fetch more from popularity
        if fresh_fetched < fresh_target:
            fresh_deficit = fresh_target - fresh_fetched
            logger.info(f"Freshness deficit: {fresh_deficit}, fetching more from popularity")

            for bucket in PERCENTILE_BUCKETS:
                if fresh_deficit <= 0:
                    break

                bucket_key = self._key_global_pop_set(bucket)
                videos = await self._fetch_from_zset(bucket_key, fresh_deficit, current_time)

                if videos:
                    new_videos = [v for v in videos if v not in all_videos]
                    all_videos.update(new_videos)
                    sources["popularity"][bucket] = sources["popularity"].get(bucket, 0) + len(new_videos)
                    fresh_deficit -= len(new_videos)

        # Convert to list and shuffle
        video_list = list(all_videos)
        random.shuffle(video_list)

        logger.info(
            f"Fetched {len(video_list)} videos from global pools (requested: {count}). "
            f"Popularity: {pop_fetched}/{pop_target}, Freshness: {fresh_fetched}/{fresh_target}"
        )

        return {
            "videos": video_list,
            "count": len(video_list),
            "sources": sources,
            "timestamp": int(time.time())
        }

    async def _fetch_from_zset(
        self,
        key: str,
        count: int,
        min_score: int
    ) -> List[str]:
        """
        Helper method to fetch videos from a ZSET with score >= min_score.

        Args:
            key: Redis ZSET key
            count: Number of videos to fetch
            min_score: Minimum score (typically current timestamp)

        Returns:
            List of video IDs

        Algorithm:
            1. Use ZRANGEBYSCORE to fetch videos with score >= min_score
            2. Limit to count videos
            3. Return list
        """
        if count <= 0:
            return []

        try:
            # Fetch videos with valid TTL (score >= min_score)
            videos = await self.db.zrangebyscore(
                key,
                min_score,  # Positional argument
                "+inf",     # Positional argument
                start=0,
                num=count
            )
            return list(videos) if videos else []
        except Exception as e:
            logger.warning(f"Error fetching from {key}: {e}")
            return []

    async def count_valid_videos_to_show(
        self, user_id: str, rec_type: str = "default"
    ) -> int:
        """
        Count videos in to_show set with sufficient TTL remaining.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Since pool is always clean (fetch_videos removes consumed videos atomically),
        we only need to count videos with valid TTL. No filtering needed.

        Args:
            user_id: User ID
            rec_type: Recommendation type

        Returns:
            Count of valid videos in pool
        """
        to_show_key = self._key_videos_to_show(user_id, rec_type)
        current_time = int(time.time())

        count = await self.scripts._execute_script(
            self.scripts.count_valid_videos,
            "count_valid_videos",
            keys=[to_show_key],
            args=[current_time, TTL_SAFETY_MARGIN, TTL_VIDEOS_TO_SHOW],
            semaphore=self.redis_semaphore,
        )

        return count

    async def needs_refill(self, user_id: str, rec_type: str) -> bool:
        """
        Check if a user's feed needs refill.
        ASYNC VERSION - same logic.

        Args:
            user_id: User ID
            rec_type: Recommendation type

        Returns:
            True if pool has fewer videos than REFILL_THRESHOLD
        """
        count = await self.count_valid_videos_to_show(user_id, rec_type)
        return count < REFILL_THRESHOLD

    # ------------------------------------------------------------------------
    # Refill Orchestration (ASYNC VERSIONS - SAME BUSINESS LOGIC)
    # ------------------------------------------------------------------------

    async def _acquire_lock(self, user_id: str, rec_type: str, ttl_seconds: int = 30) -> None:
        """
        Acquire a short-lived lock for refilling a user's rec_type set.
        ASYNC VERSION - same logic.

        Raises RuntimeError if lock is already held.
        """
        lock_key = self._key_refill_lock(user_id, rec_type)
        client = await self.db.get_client()
        ok = await client.set(lock_key, "1", nx=True, ex=ttl_seconds)
        if not ok:
            raise RuntimeError(f"Refill lock already held for {user_id}:{rec_type}")

    async def _release_lock(self, user_id: str, rec_type: str) -> None:
        """Release the refill lock if present. ASYNC VERSION."""
        lock_key = self._key_refill_lock(user_id, rec_type)
        try:
            await self.db.delete(lock_key)
        except Exception as e:
            logger.warning(f"Failed to release lock {lock_key}: {e}")

    async def _record_refill_stats(self, user_id: str, rec_type: str, stats: Dict[str, int]) -> None:
        """Persist last refill stats and metrics counters. ASYNC VERSION."""
        await self.db.incr(self._key_metrics_attempts(rec_type))
        await self.db.set(self._key_refill_last_stats(user_id, rec_type), json.dumps(stats))

    async def refill_popularity(
        self,
        user_id: str,
        target: int = VIDEOS_TO_SHOW_CAPACITY,
        batch_size: int = 200,
        max_iterations: int = 10,
        ttl: int = TTL_VIDEOS_TO_SHOW,
    ) -> Dict[str, int]:
        """
        Refill user's `videos_to_show:popularity` from global popularity ZSETs.
        CLUSTER-SAFE VERSION - splits global reads from user writes.

        Algorithm:
            1. Acquire lock `{user:ID}:refill:lock:popularity`
            2. Ensure bloom exists
            3. Read current bucket pointer from `{user:ID}:pop_percentile_pointer`
            4. Sample from global pools in Python (ZRANDMEMBER - cross-slot OK)
            5. Call filter_and_add_videos Lua script (user keys only - cluster-safe)
            6. Update pointer to final bucket

        Args:
            user_id: User identifier
            target: Target number of videos to add
            batch_size: Number of videos to sample per iteration
            max_iterations: Max iterations per bucket before downshifting
            ttl: TTL for videos in to_show set

        Returns:
            Dict with fields: added_total, iterations, filtered_watched, filtered_bloom,
            valid_after, bucket_start, bucket_final, sources_exhausted
        """
        rec_type = "popularity"
        await self._acquire_lock(user_id, rec_type)
        try:
            await self.init_user_bloom_filter(user_id)
            to_show_key = self._key_videos_to_show(user_id, rec_type)
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)
            pointer_key = self._key_pop_percentile_pointer(user_id)

            now = int(time.time())

            # Step 1: Read current bucket pointer
            current_bucket = await self.db.get(pointer_key)
            if current_bucket:
                current_bucket = current_bucket.decode('utf-8') if isinstance(current_bucket, bytes) else current_bucket
            else:
                current_bucket = "99_100"  # Default to highest bucket

            bucket_start = current_bucket

            # Find starting index in bucket order
            try:
                start_idx = self.PERCENTILE_BUCKETS_ORDER.index(current_bucket)
            except ValueError:
                start_idx = 0
                current_bucket = self.PERCENTILE_BUCKETS_ORDER[0]

            # Step 2: Sample from global pools (Python - cross-slot OK)
            candidates = []
            iterations_total = 0
            bucket_final = current_bucket
            sources_exhausted = True

            for bucket_idx in range(start_idx, len(self.PERCENTILE_BUCKETS_ORDER)):
                bucket = self.PERCENTILE_BUCKETS_ORDER[bucket_idx]
                pool_key = self._key_global_pop_set(bucket)

                # Try this bucket for max_iterations
                for _ in range(max_iterations):
                    iterations_total += 1

                    # Sample from global pool (ZRANDMEMBER with scores)
                    batch = await self.db.zrandmember(pool_key, count=batch_size, withscores=True)

                    if not batch:
                        break  # Bucket empty, move to next

                    # Filter expired videos (score > now means valid)
                    valid_batch = []
                    for i in range(0, len(batch), 2):
                        vid = batch[i]
                        score = float(batch[i + 1]) if batch[i + 1] else 0
                        if score > now:
                            if isinstance(vid, bytes):
                                vid = vid.decode('utf-8')
                            valid_batch.append(vid)

                    candidates.extend(valid_batch)

                    # Check if we have enough candidates (2x buffer for filtering)
                    if len(candidates) >= target * 2:
                        bucket_final = bucket
                        sources_exhausted = False
                        break

                if not sources_exhausted:
                    break
                bucket_final = bucket

            # Step 3: Filter and add using Lua script (user keys only - cluster-safe)
            added_total = 0
            filtered_watched = 0
            filtered_bloom = 0

            if candidates:
                result = await self.scripts._execute_script(
                    self.scripts.filter_and_add_videos,
                    "filter_and_add_videos",
                    keys=[to_show_key, watched_key, bloom_key],
                    args=[now, ttl, VIDEOS_TO_SHOW_CAPACITY] + candidates,
                    semaphore=self.redis_semaphore,
                )
                added_total = int(result[0])
                filtered_watched = int(result[1])
                filtered_bloom = int(result[2])

                # Check if we met the target
                if added_total >= target:
                    sources_exhausted = False

            # Step 4: Update pointer to final bucket
            await self.db.set(pointer_key, bucket_final, ex=TTL_PERCENTILE_POINTER)

            # Count valid videos in pool
            valid_after = await self.db.zcount(to_show_key, now, "+inf")

            stats = {
                "added_total": added_total,
                "iterations": iterations_total,
                "filtered_watched": filtered_watched,
                "filtered_bloom": filtered_bloom,
                "valid_after": valid_after,
                "bucket_start": bucket_start,
                "bucket_final": bucket_final,
                "sources_exhausted": bool(sources_exhausted),
            }

            # Record metrics
            if sources_exhausted:
                await self.db.incr(self._key_metrics_failures(rec_type))

            await self._record_refill_stats(user_id, rec_type, stats)
            return stats
        finally:
            await self._release_lock(user_id, rec_type)

    async def refill_freshness(
        self,
        user_id: str,
        target: int = VIDEOS_TO_SHOW_CAPACITY,
        windows: Optional[List[str]] = None,
        batch_size: int = 200,
        max_iterations: int = 10,
        ttl: int = TTL_VIDEOS_TO_SHOW,
    ) -> Dict[str, int]:
        """
        Refill user's `videos_to_show:freshness` from cascading freshness windows.
        CLUSTER-SAFE VERSION - splits global reads from user writes.

        Algorithm:
            1. Acquire lock for freshness rec_type
            2. Ensure bloom filter exists
            3. Cascade through freshness windows (l1d -> l7d -> l14d -> l30d -> l90d):
               - For each window, sample videos using ZRANDMEMBER (Python - cross-slot OK)
               - Filter expired videos (score > now)
               - Accumulate candidates until 2x target reached
            4. Call filter_and_add_videos Lua script (user keys only - cluster-safe)
            5. Record stats and release lock

        Args:
            user_id: User ID to refill freshness pool for
            target: Target number of videos to add (default: VIDEOS_TO_SHOW_CAPACITY)
            windows: Optional list of windows to use (default: all 5)
            batch_size: Batch size for ZRANDMEMBER sampling (default: 200)
            max_iterations: Max iterations per window (default: 10)
            ttl: TTL for videos in the pool (default: TTL_VIDEOS_TO_SHOW)

        Returns:
            Dict with stats: added_total, iterations, filtered_watched, filtered_bloom,
            valid_after, sources_exhausted
        """
        rec_type = "freshness"
        await self._acquire_lock(user_id, rec_type)
        try:
            await self.init_user_bloom_filter(user_id)
            to_show_key = self._key_videos_to_show(user_id, rec_type)
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)

            now = int(time.time())

            # Step 1: Sample from freshness windows (Python - cross-slot OK)
            # Cascade through windows: l1d -> l7d -> l14d -> l30d -> l90d
            windows_to_use = windows or self.FRESHNESS_WINDOWS_ORDER
            candidates = []
            iterations_total = 0
            sources_exhausted = True

            for window in windows_to_use:
                pool_key = self._key_global_fresh_zset(window)

                # Try this window for max_iterations
                for _ in range(max_iterations):
                    iterations_total += 1

                    # Sample from global pool (ZRANDMEMBER with scores)
                    batch = await self.db.zrandmember(pool_key, count=batch_size, withscores=True)

                    if not batch:
                        break  # Window empty, move to next

                    # Filter expired videos (score > now means valid)
                    valid_batch = []
                    for i in range(0, len(batch), 2):
                        vid = batch[i]
                        score = float(batch[i + 1]) if batch[i + 1] else 0
                        if score > now:
                            if isinstance(vid, bytes):
                                vid = vid.decode('utf-8')
                            valid_batch.append(vid)

                    candidates.extend(valid_batch)

                    # Check if we have enough candidates (2x buffer for filtering)
                    if len(candidates) >= target * 2:
                        sources_exhausted = False
                        break

                if not sources_exhausted:
                    break

            # Step 2: Filter and add using Lua script (user keys only - cluster-safe)
            added_total = 0
            filtered_watched = 0
            filtered_bloom = 0

            if candidates:
                result = await self.scripts._execute_script(
                    self.scripts.filter_and_add_videos,
                    "filter_and_add_videos",
                    keys=[to_show_key, watched_key, bloom_key],
                    args=[now, ttl, VIDEOS_TO_SHOW_CAPACITY] + candidates,
                    semaphore=self.redis_semaphore,
                )
                added_total = int(result[0])
                filtered_watched = int(result[1])
                filtered_bloom = int(result[2])

                # Check if we met the target
                if added_total >= target:
                    sources_exhausted = False

            # Count valid videos in pool
            valid_after = await self.db.zcount(to_show_key, now, "+inf")

            stats = {
                "added_total": added_total,
                "iterations": iterations_total,
                "filtered_watched": filtered_watched,
                "filtered_bloom": filtered_bloom,
                "valid_after": valid_after,
                "sources_exhausted": bool(sources_exhausted),
            }

            # Record metrics
            if sources_exhausted:
                await self.db.incr(self._key_metrics_failures(rec_type))

            await self._record_refill_stats(user_id, rec_type, stats)
            return stats
        finally:
            await self._release_lock(user_id, rec_type)

    async def _refill_from_single_pool(
        self,
        user_id: str,
        rec_type: str,
        global_pool_key: str,
        target: int,
        batch_size: int,
        max_iterations: int,
        ttl: int,
    ) -> Dict[str, int]:
        """
        Helper to refill user pool from a single global pool.
        CLUSTER-SAFE VERSION - splits global reads from user writes.

        Algorithm:
            1. Acquire lock for rec_type
            2. Ensure bloom filter exists
            3. Sample from global pool using ZRANDMEMBER (Python - cross-slot OK)
            4. Filter expired videos (score > now)
            5. Call filter_and_add_videos Lua script (user keys only - cluster-safe)
            6. Record stats and release lock

        Args:
            user_id: User ID to refill pool for
            rec_type: Feed type (fallback, ugc)
            global_pool_key: Redis key for the global pool to sample from
            target: Target number of videos to add
            batch_size: Batch size for ZRANDMEMBER sampling
            max_iterations: Max iterations for sampling
            ttl: TTL for videos in the pool

        Returns:
            Dict with stats: added_total, iterations, filtered_watched, filtered_bloom,
            valid_after, sources_exhausted
        """
        await self._acquire_lock(user_id, rec_type)
        try:
            await self.init_user_bloom_filter(user_id)
            to_show_key = self._key_videos_to_show(user_id, rec_type)
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)

            now = int(time.time())

            # Step 1: Sample from global pool (Python - cross-slot OK)
            candidates = []
            iterations_total = 0
            sources_exhausted = True

            for _ in range(max_iterations):
                iterations_total += 1

                # Sample from global pool (ZRANDMEMBER with scores)
                batch = await self.db.zrandmember(global_pool_key, count=batch_size, withscores=True)

                if not batch:
                    break  # Pool empty

                # Filter expired videos (score > now means valid)
                valid_batch = []
                for i in range(0, len(batch), 2):
                    vid = batch[i]
                    score = float(batch[i + 1]) if batch[i + 1] else 0
                    if score > now:
                        if isinstance(vid, bytes):
                            vid = vid.decode('utf-8')
                        valid_batch.append(vid)

                candidates.extend(valid_batch)

                # Check if we have enough candidates (2x buffer for filtering)
                if len(candidates) >= target * 2:
                    sources_exhausted = False
                    break

            # Step 2: Filter and add using Lua script (user keys only - cluster-safe)
            added_total = 0
            filtered_watched = 0
            filtered_bloom = 0

            if candidates:
                result = await self.scripts._execute_script(
                    self.scripts.filter_and_add_videos,
                    "filter_and_add_videos",
                    keys=[to_show_key, watched_key, bloom_key],
                    args=[now, ttl, VIDEOS_TO_SHOW_CAPACITY] + candidates,
                    semaphore=self.redis_semaphore,
                )
                added_total = int(result[0])
                filtered_watched = int(result[1])
                filtered_bloom = int(result[2])

                # Check if we met the target
                if added_total >= target:
                    sources_exhausted = False

            # Count valid videos in pool
            valid_after = await self.db.zcount(to_show_key, now, "+inf")

            stats = {
                "added_total": added_total,
                "iterations": iterations_total,
                "filtered_watched": filtered_watched,
                "filtered_bloom": filtered_bloom,
                "valid_after": valid_after,
                "sources_exhausted": bool(sources_exhausted),
            }

            # Record metrics
            if sources_exhausted:
                await self.db.incr(self._key_metrics_failures(rec_type))

            await self._record_refill_stats(user_id, rec_type, stats)
            return stats
        finally:
            await self._release_lock(user_id, rec_type)

    async def refill_with_fallback(
        self,
        user_id: str,
        target: int = REFILL_THRESHOLD,
        batch_size: int = 200,
        max_iterations: int = 5,
        ttl: int = TTL_VIDEOS_TO_SHOW,
    ) -> Dict[str, int]:
        """
        Refill user's `videos_to_show:fallback` from global fallback zset (last resort).
        CLUSTER-SAFE VERSION - delegates to helper that splits global reads from user writes.

        Algorithm:
            1. Get global fallback pool key
            2. Delegate to _refill_from_single_pool helper

        Args:
            user_id: User ID to refill fallback pool for
            target: Target number of videos to add (default: REFILL_THRESHOLD)
            batch_size: Batch size for ZRANDMEMBER sampling (default: 200)
            max_iterations: Max iterations for sampling (default: 5)
            ttl: TTL for videos in the pool (default: TTL_VIDEOS_TO_SHOW)

        Returns:
            Dict with stats: added_total, iterations, filtered_watched, filtered_bloom,
            valid_after, sources_exhausted
        """
        return await self._refill_from_single_pool(
            user_id=user_id,
            rec_type="fallback",
            global_pool_key=self._key_global_fallback_zset(),
            target=target,
            batch_size=batch_size,
            max_iterations=max_iterations,
            ttl=ttl,
        )

    async def refill_following(
        self,
        user_id: str,
        video_ids: List[str],
        ttl: int = TTL_FOLLOWING_VIDEOS,
    ) -> int:
        """
        Refill user's following pool with videos fetched from BigQuery.

        This method takes pre-fetched video_ids (from BigQuery) and adds them to
        the user's following pool after filtering through bloom + watched:short.

        Algorithm:
            1. Acquire lock for 'following' rec_type
            2. Ensure user bloom filter exists
            3. Filter videos through Lua script (bloom + watched:short)
            4. Add filtered videos to user:{user_id}:videos_to_show:following
            5. Release lock

        Args:
            user_id: User ID to refill following pool for
            video_ids: List of video IDs fetched from BigQuery
            ttl: TTL for videos in the pool (default 3 days)

        Returns:
            Number of videos added to the pool
        """
        rec_type = "following"

        if not video_ids:
            logger.debug(f"No videos to add to following pool for user {user_id}")
            return 0

        await self._acquire_lock(user_id, rec_type)
        try:
            # Ensure bloom filter exists for user
            await self.init_user_bloom_filter(user_id)

            to_show_key = self._key_videos_to_show(user_id, rec_type)
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)
            now = int(time.time())

            # Use filter_and_add_videos Lua script to filter and add in one atomic op
            # This script filters against bloom and watched:short, then adds to pool
            result = await self.scripts._execute_script(
                self.scripts.filter_and_add_videos,
                "filter_and_add_videos",
                keys=[to_show_key, watched_key, bloom_key],
                args=[now, ttl, VIDEOS_TO_SHOW_CAPACITY] + video_ids,
                semaphore=self.redis_semaphore,
            )

            # Result format: [added_count, filtered_watched, filtered_bloom]
            added = int(result[0])
            filtered_watched = int(result[1])
            filtered_bloom = int(result[2])

            logger.info(
                f"Following refill for {user_id}: added={added}, "
                f"filtered_watched={filtered_watched}, filtered_bloom={filtered_bloom}"
            )

            return added
        finally:
            await self._release_lock(user_id, rec_type)

    async def refill_ugc(
        self,
        user_id: str,
        target: int = 300,
        batch_size: int = 500,
        max_iterations: int = 3,
        ttl: int = TTL_UGC_USER_POOL,
    ) -> Dict[str, int]:
        """
        Refill user's `videos_to_show:ugc` from global UGC DISCOVERY pool.
        Uses PRIORITY-BASED sampling: newest videos AND least-pushed videos first.

        This replaces the old ZRANDMEMBER approach that favored older videos.
        The priority formula ensures fresh uploads get discovered:
            priority = upload_timestamp - (push_count * PUSH_PENALTY_SECONDS)

        Algorithm:
            1. Acquire lock for ugc refill
            2. Ensure bloom filter exists
            3. Sample candidates from discovery pool ZSET (by expiry, valid only)
            4. Batch-fetch push counts and timestamps from HASH
            5. Calculate priority for each video
            6. Sort by priority descending, take top candidates
            7. Filter via Lua (bloom + watched:short check)
            8. Add to user pool with SHORT TTL (1 hour for freshness)
            9. Release lock

        Note: Push count increment happens at FETCH TIME (in fetch_videos),
        not here. This ensures push_count reflects actual impressions.

        Args:
            user_id: User ID to refill UGC pool for
            target: Target number of videos in pool (default: 300)
            batch_size: Batch size for sampling (default: 500, larger to allow sorting)
            max_iterations: Max refill iterations (default: 3)
            ttl: TTL for videos in user pool (default: 1 hour for freshness)

        Returns:
            Dict with refill stats: added_total, iterations, filtered_watched,
            filtered_bloom, valid_after, sources_exhausted
        """
        await self._acquire_lock(user_id, "ugc")
        try:
            await self.init_user_bloom_filter(user_id)
            to_show_key = self._key_videos_to_show(user_id, "ugc")
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)

            # UGC Discovery pool keys
            pool_key = self._key_global_ugc_discovery_zset()
            pushes_key = self._key_global_ugc_discovery_pushes()
            timestamps_key = self._key_global_ugc_discovery_timestamps()

            now = int(time.time())

            # Step 1: Sample candidates from discovery pool (Python - cross-slot OK)
            candidates = []
            iterations_total = 0
            sources_exhausted = True

            for _ in range(max_iterations):
                iterations_total += 1

                # Sample from global pool - get more than needed to allow sorting
                # Use ZRANGEBYSCORE to get valid (non-expired) entries
                batch = await self.db.zrangebyscore(
                    pool_key, now, '+inf', start=0, num=batch_size, withscores=True
                )

                # DEBUG: Log batch type and first few elements after zrangebyscore
                logger.debug(
                    f"[UGC refill] zrangebyscore batch: type={type(batch).__name__}, "
                    f"len={len(batch) if batch else 0}, "
                    f"first_3={batch[:3] if batch else []}, "
                    f"first_elem_type={type(batch[0]).__name__ if batch else 'N/A'}"
                )

                if not batch:
                    break  # Pool empty or all expired

                # Extract video IDs from batch
                # Note: zrangebyscore with withscores=True returns [(vid, score), ...]
                # This is different from zrandmember which returns [vid, score, vid, score, ...]
                batch_vids = []
                for item in batch:
                    if isinstance(item, tuple):
                        vid = item[0]
                    else:
                        vid = item
                    if isinstance(vid, bytes):
                        vid = vid.decode('utf-8')
                    batch_vids.append(vid)

                # DEBUG: Log batch_vids type and sample after extraction
                logger.debug(
                    f"[UGC refill] batch_vids: type={type(batch_vids).__name__}, "
                    f"len={len(batch_vids)}, "
                    f"first_3={batch_vids[:3] if batch_vids else []}, "
                    f"first_elem_type={type(batch_vids[0]).__name__ if batch_vids else 'N/A'}"
                )

                if not batch_vids:
                    break

                # Step 2: Batch-fetch push counts and timestamps
                push_counts_raw = await self.client.hmget(pushes_key, *batch_vids)
                timestamps_raw = await self.client.hmget(timestamps_key, *batch_vids)

                # DEBUG: Log raw push counts and timestamps
                logger.debug(
                    f"[UGC refill] push_counts_raw: type={type(push_counts_raw).__name__}, "
                    f"len={len(push_counts_raw) if push_counts_raw else 0}, "
                    f"first_3={push_counts_raw[:3] if push_counts_raw else []}"
                )
                logger.debug(
                    f"[UGC refill] timestamps_raw: type={type(timestamps_raw).__name__}, "
                    f"len={len(timestamps_raw) if timestamps_raw else 0}, "
                    f"first_3={timestamps_raw[:3] if timestamps_raw else []}"
                )

                # Step 3: Calculate priority for each video
                # priority = upload_timestamp - (push_count * PUSH_PENALTY)
                for i, vid in enumerate(batch_vids):
                    # Parse push count (default 0 if missing)
                    push_count = 0
                    if push_counts_raw[i]:
                        try:
                            raw_val = push_counts_raw[i]
                            if isinstance(raw_val, bytes):
                                raw_val = raw_val.decode('utf-8')
                            push_count = int(raw_val)
                        except (ValueError, TypeError):
                            pass

                    # Parse timestamp (skip if missing - data integrity issue)
                    ts = 0
                    if timestamps_raw[i]:
                        try:
                            raw_ts = timestamps_raw[i]
                            if isinstance(raw_ts, bytes):
                                raw_ts = raw_ts.decode('utf-8')
                            ts = int(raw_ts)
                        except (ValueError, TypeError):
                            pass

                    if ts == 0:
                        # Skip videos with missing timestamp
                        continue

                    # Calculate priority: newer videos + less-pushed = higher priority
                    priority = ts - (push_count * UGC_DISCOVERY_PUSH_PENALTY)
                    candidates.append((vid, priority, push_count))

                # Check if we have enough candidates (2x buffer for filtering)
                if len(candidates) >= target * 2:
                    sources_exhausted = False
                    break

            # Step 4: Sort by priority descending, take top candidates
            candidates.sort(key=lambda x: x[1], reverse=True)
            top_candidates = [vid for vid, _, _ in candidates[:target * 2]]

            # DEBUG: Log top_candidates before Lua script call
            logger.debug(
                f"[UGC refill] top_candidates: type={type(top_candidates).__name__}, "
                f"len={len(top_candidates)}, "
                f"first_5={top_candidates[:5] if top_candidates else []}, "
                f"first_elem_type={type(top_candidates[0]).__name__ if top_candidates else 'N/A'}"
            )
            # DEBUG: Also check if any element is unexpectedly a tuple
            if top_candidates:
                for idx, vid in enumerate(top_candidates[:10]):
                    if not isinstance(vid, (str, bytes, int, float)):
                        logger.debug(
                            f"[UGC refill] WARNING: top_candidates[{idx}] is unexpected type: "
                            f"type={type(vid).__name__}, value={vid}"
                        )

            # Step 5: Filter and add using Lua script (user keys only - cluster-safe)
            added_total = 0
            filtered_watched = 0
            filtered_bloom = 0

            if top_candidates:
                try:
                    # DEBUG: Log full args before Lua call
                    lua_args = [now, ttl, VIDEOS_TO_SHOW_CAPACITY] + top_candidates
                    logger.debug(
                        f"[UGC refill] Lua script args: "
                        f"now={now}, ttl={ttl}, capacity={VIDEOS_TO_SHOW_CAPACITY}, "
                        f"args_len={len(lua_args)}, "
                        f"args_types={[type(a).__name__ for a in lua_args[:10]]}"
                    )

                    result = await self.scripts._execute_script(
                        self.scripts.filter_and_add_videos,
                        "filter_and_add_videos",
                        keys=[to_show_key, watched_key, bloom_key],
                        args=lua_args,
                        semaphore=self.redis_semaphore,
                    )

                    added_total = result[0]
                    filtered_watched = result[1]
                    filtered_bloom = result[2]
                except Exception as lua_err:
                    logger.error(
                        f"[UGC refill] Lua script failed for user {user_id}: {lua_err}\n"
                        f"Traceback:\n{traceback.format_exc()}"
                    )
                    raise

            # Get final valid count
            valid_after = await self.count_valid_videos_to_show(user_id, "ugc")

            stats = {
                "added_total": added_total,
                "iterations": iterations_total,
                "filtered_watched": filtered_watched,
                "filtered_bloom": filtered_bloom,
                "valid_after": valid_after,
                "sources_exhausted": sources_exhausted,
                "candidates_sampled": len(candidates),
            }

            logger.info(
                f"UGC Discovery refill for {user_id}: added={added_total}, "
                f"filtered_w={filtered_watched}, filtered_b={filtered_bloom}, "
                f"candidates={len(candidates)}, valid_after={valid_after}"
            )

            return stats

        except Exception as e:
            logger.error(
                f"[UGC refill] Exception in refill_ugc for user {user_id}: {e}\n"
                f"Traceback:\n{traceback.format_exc()}"
            )
            raise

        finally:
            await self._release_lock(user_id, "ugc")

    async def _background_refill(self, user_id: str, rec_type: str):
        """
        Background refill task (fire-and-forget).

        Algorithm:
        1. Proactively refills user pool when it drops below REFILL_THRESHOLD
        2. Runs async without blocking the API response
        3. Handles lock conflicts gracefully (another refill may be running)
        4. Logs errors but doesn't crash (best-effort background work)

        Args:
            user_id: User ID
            rec_type: Feed type (popularity, freshness, fallback)

        Note: All refill_* methods already handle locks via _acquire_lock/_release_lock.
        We just need to catch RuntimeError if lock is held.
        """
        try:
            if rec_type == "popularity":
                await self.refill_popularity(user_id, target=VIDEOS_TO_SHOW_CAPACITY)
            elif rec_type == "freshness":
                await self.refill_freshness(user_id, target=VIDEOS_TO_SHOW_CAPACITY)
            elif rec_type == "fallback":
                await self.refill_with_fallback(user_id, target=REFILL_THRESHOLD * 10)
            elif rec_type == "following":
                # Following is handled separately by mixer's _maybe_trigger_following_sync
                # because it requires BigQuery access which AsyncRedisLayer doesn't have
                logger.debug(f"Skipping background refill for {user_id}:following (handled by mixer)")
                return
            elif rec_type == "ugc":
                # UGC refill from global UGC pool (smaller target since UGC is only 5% of feed)
                await self.refill_ugc(user_id, target=100)
            else:
                logger.warning(f"Unknown rec_type for background refill: {rec_type}")
                return

            logger.info(f"Background refill completed for {user_id}:{rec_type}")

        except RuntimeError as e:
            # Lock already held - another refill is running, skip gracefully
            if "lock already held" in str(e):
                logger.debug(f"Background refill skipped for {user_id}:{rec_type} (lock held)")
            else:
                logger.warning(f"Background refill failed for {user_id}:{rec_type}: {e}")
        except Exception as e:
            # Log but don't crash - this is best-effort background work
            logger.warning(f"Background refill failed for {user_id}:{rec_type}: {e}")

    # ------------------------------------------------------------------------
    # Fresh User Bootstrap (ASYNC VERSION)
    # ------------------------------------------------------------------------

    async def bootstrap_fresh_user(self, user_id: str) -> Dict[str, Dict[str, int]]:
        """
        Bootstrap a fresh user (no bloom, no short-lived watched history).
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Algorithm (UNCHANGED):
        - If bloom missing and watched:short ZCARD == 0 → treat as fresh.
        - Initialize bloom; set percentile pointer to 99_100.
        - Refill popularity with target=REFILL_THRESHOLD*5.
        - Refill freshness with target=REFILL_THRESHOLD*2.

        Returns:
        - Dict with keys 'popularity' and 'freshness' mapping to their refill stats.
        """
        watched_key = self._key_watched_short_lived(user_id)
        is_bloom_present = await self.db.exists(self._key_bloom_permanent(user_id))
        watched_count = await self.db.zcard(watched_key)
        if is_bloom_present or watched_count > 0:
            logger.debug(f"Skipping bootstrap for non-fresh user {user_id} (bloom={is_bloom_present}, watched={watched_count})")
            return {"popularity": {}, "freshness": {}}

        # init
        await self.init_user_bloom_filter(user_id)
        await self.set_pop_percentile_pointer(user_id, self.PERCENTILE_BUCKETS_ORDER[0])

        pop_stats = await self.refill_popularity(user_id, target=REFILL_THRESHOLD * 5)
        fresh_stats = await self.refill_freshness(user_id, target=REFILL_THRESHOLD * 2)
        return {"popularity": pop_stats, "freshness": fresh_stats}

    # ------------------------------------------------------------------------
    # Watched Videos Operations (ASYNC VERSIONS)
    # ------------------------------------------------------------------------

    async def filter_unwatched_videos(self, user_id: str, video_ids: List[str]) -> List[str]:
        """
        Filter out videos that have been watched (in short-lived set or bloom filter).
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Args:
            user_id: User ID
            video_ids: List of video IDs to filter

        Returns:
            List of unwatched video IDs
        """
        if not video_ids:
            return []

        watched_key = self._key_watched_short_lived(user_id)
        bloom_key = self._key_bloom_permanent(user_id)
        current_time = int(time.time())

        # Ensure bloom filter exists
        await self.init_user_bloom_filter(user_id)

        # Use Lua script for efficient filtering
        unwatched = await self.scripts._execute_script(
            self.scripts.check_videos_not_watched,
            "check_videos_not_watched",
            keys=[watched_key, bloom_key],
            args=[current_time] + video_ids,
            semaphore=self.redis_semaphore,
        )

        logger.debug(
            f"Filtered {len(video_ids)} -> {len(unwatched)} unwatched videos for user {user_id}"
        )
        return unwatched

    async def mark_videos_cooldown(self, user_id: str, video_ids: List[str], ttl: int = TTL_WATCHED_SET) -> int:
        """
        Mark videos with temporary cooldown (won't be recommended again for TTL period).
        ASYNC VERSION - NEW METHOD for API compatibility.

        Args:
            user_id: User ID
            video_ids: List of video IDs to mark
            ttl: Cooldown period in seconds

        Returns:
            Number of videos marked
        """
        if not video_ids:
            return 0

        watched_key = self._key_watched_short_lived(user_id)
        expiry_time = int(time.time()) + ttl

        # Add videos to watched set with expiry as score
        mapping = {vid: expiry_time for vid in video_ids}
        count = await self.db.zadd(watched_key, mapping)

        logger.debug(f"Marked {count} videos with cooldown for user {user_id}")
        return count

    # ------------------------------------------------------------------------
    # User Statistics (ASYNC VERSIONS)
    # ------------------------------------------------------------------------

    async def get_user_stats(self, user_id: str, rec_type: str = "default") -> Dict:
        """
        Get statistics for a user's recommendation state.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Args:
            user_id: User ID
            rec_type: Recommendation type

        Returns:
            Dictionary with user statistics
        """
        valid_count = await self.count_valid_videos_to_show(user_id, rec_type)
        stats = {
            "user_id": user_id,
            "rec_type": rec_type,
            "videos_to_show_count": valid_count,
            "needs_refill": valid_count < REFILL_THRESHOLD,
            "bloom_filter_exists": await self.db.exists(self._key_bloom_permanent(user_id)),
        }

        # Count total videos in to_show (including expired)
        key = self._key_videos_to_show(user_id, rec_type)
        stats["total_videos_in_set"] = await self.db.zcard(key)

        return stats

    async def get_all_rec_types_stats(self, user_id: str, rec_types: List[str]) -> Dict:
        """
        Get statistics for all recommendation types for a user.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Args:
            user_id: User ID
            rec_types: List of recommendation types to check

        Returns:
            Dictionary with stats for each rec_type
        """
        all_stats = {
            "user_id": user_id,
            "bloom_filter_exists": await self.db.exists(self._key_bloom_permanent(user_id)),
            "rec_types": {},
        }

        for rec_type in rec_types:
            valid_count = await self.count_valid_videos_to_show(user_id, rec_type)
            all_stats["rec_types"][rec_type] = {
                "valid_count": valid_count,
                "total_count": await self.db.zcard(
                    self._key_videos_to_show(user_id, rec_type)
                ),
                "needs_refill": valid_count < REFILL_THRESHOLD,
            }

        return all_stats

    async def debug_user_state(self, user_id: str) -> str:
        """
        Get user state as formatted string for debugging.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Returns string showing: bloom filter status, watched videos count,
        and videos_to_show count for each rec_type with sample videos.
        """
        from datetime import datetime
        current_time = int(time.time())

        lines = [f"\n{'='*60}", f"USER: {user_id}", f"{'='*60}"]

        # Bloom filter
        bloom_key = self._key_bloom_permanent(user_id)
        lines.append(f"Bloom Filter: {'EXISTS' if await self.db.exists(bloom_key) else 'NOT FOUND'}")

        # Watched short-lived
        watched_key = self._key_watched_short_lived(user_id)
        watched_count = await self.db.zcard(watched_key)
        lines.append(f"Watched (short-lived): {watched_count} videos")
        if watched_count > 0:
            client = await self.db.get_client()
            for vid, score in await client.zrange(watched_key, 0, 4, withscores=True):
                dt = datetime.fromtimestamp(int(score)).strftime('%H:%M:%S')
                lines.append(f"  - {vid} (expires: {dt})")

        # Rec types
        rec_keys = await self.db.keys(f"user:{user_id}:videos_to_show:*")
        lines.append(f"\nRec Types: {len(rec_keys)}")
        for key in rec_keys:
            rec_type = key.split(":")[-1]
            total = await self.db.zcard(key)
            valid = await self.count_valid_videos_to_show(user_id, rec_type)
            needs_refill = valid < REFILL_THRESHOLD
            lines.append(f"\n[{rec_type}] {valid}/{total} valid (refill: {needs_refill})")
            if total > 0:
                client = await self.db.get_client()
                for vid, score in await client.zrange(key, 0, 2, withscores=True):
                    expires_in = int(score - current_time)
                    lines.append(f"  - {vid} (expires in {expires_in}s)")

        lines.append(f"{'='*60}\n")
        return "\n".join(lines)

    # ------------------------------------------------------------------------
    # Tournament Operations
    # ------------------------------------------------------------------------

    async def tournament_exists(self, tournament_id: str) -> bool:
        """
        Check if a tournament with the given ID already exists.

        Algorithm:
            1. Check if tournament videos key exists in Redis
            2. Return True if exists, False otherwise

        Args:
            tournament_id: Unique tournament identifier

        Returns:
            bool: True if tournament exists, False otherwise
        """
        videos_key = self._key_tournament_videos(tournament_id)
        exists = await self.db.exists(videos_key)
        return exists > 0

    async def get_recently_used_tournament_videos(self) -> set:
        """
        Get set of video IDs used in tournaments within the reuse cooldown window.

        Algorithm:
            1. Get current timestamp
            2. Calculate cutoff timestamp (now - cooldown period)
            3. Fetch video IDs from ZSET with score > cutoff
            4. Return as set for O(1) lookup

        Returns:
            set: Set of video IDs used in recent tournaments
        """
        recent_key = self._key_tournament_recent_videos()
        current_time = int(time.time())
        cutoff_time = current_time - TOURNAMENT_VIDEO_REUSE_COOLDOWN_SECONDS

        # Get videos with timestamp > cutoff (i.e., within cooldown window)
        client = await self.db.get_client()
        recent_videos = await client.zrangebyscore(
            recent_key,
            min=cutoff_time,
            max="+inf"
        )

        return set(recent_videos)

    async def store_tournament_videos(
        self,
        tournament_id: str,
        videos_with_metadata: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Store video set with metadata for a tournament and track videos as recently used.

        Algorithm:
            1. Store videos as JSON strings in a Redis LIST (preserves order + includes metadata)
            2. Store tournament metadata in a HASH (created_at, video_count)
            3. Set TTL on both keys (30 days - retention period)
            4. Add tournament_id to registry ZSET with created_at timestamp
            5. Add video IDs to recent_videos ZSET with current timestamp
            6. Cleanup old entries from recent_videos ZSET

        Args:
            tournament_id: Unique tournament identifier
            videos_with_metadata: List of dicts, each containing:
                - video_id (str): Video identifier
                - canister_id (str): Canister ID
                - post_id (int/str): Post ID
                - publisher_user_id (str): Publisher user ID

        Returns:
            dict: {created_at: str, video_count: int}
        """
        videos_key = self._key_tournament_videos(tournament_id)
        meta_key = self._key_tournament_meta(tournament_id)
        recent_key = self._key_tournament_recent_videos()
        registry_key = self._key_tournament_registry()

        current_time = int(time.time())
        created_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(current_time))

        client = await self.db.get_client()

        # Use pipeline for atomicity
        pipe = client.pipeline()

        # Store videos as JSON strings in LIST (preserves order + metadata)
        pipe.delete(videos_key)
        if videos_with_metadata:
            # Convert each video dict to JSON string for storage
            json_videos = [json.dumps(v) for v in videos_with_metadata]
            pipe.rpush(videos_key, *json_videos)
        # Use retention TTL (30 days) to allow inspection of expired tournaments
        pipe.expire(videos_key, TOURNAMENT_RETENTION_SECONDS)

        # Store tournament metadata
        pipe.hset(meta_key, mapping={
            "created_at": created_at,
            "video_count": len(videos_with_metadata)
        })
        pipe.expire(meta_key, TOURNAMENT_RETENTION_SECONDS)

        # Add tournament to registry ZSET with created_at timestamp as score
        pipe.zadd(registry_key, {tournament_id: current_time})

        # Track these videos as recently used (by video_id)
        # Score = timestamp when added to tournament
        video_ids = [v["video_id"] for v in videos_with_metadata]
        video_scores = {vid: current_time for vid in video_ids}
        if video_scores:
            pipe.zadd(recent_key, video_scores)

        # Cleanup old entries from recent_videos (older than cooldown period)
        cutoff_time = current_time - TOURNAMENT_VIDEO_REUSE_COOLDOWN_SECONDS
        pipe.zremrangebyscore(recent_key, "-inf", cutoff_time)

        await pipe.execute()

        logger.info(
            f"Stored tournament {tournament_id} with {len(videos_with_metadata)} videos (with metadata), "
            f"TTL={TOURNAMENT_RETENTION_SECONDS}s (30-day retention)"
        )

        return {
            "created_at": created_at,
            "video_count": len(videos_with_metadata)
        }

    async def get_tournament_videos(
        self,
        tournament_id: str,
        with_metadata: bool = False
    ) -> Optional[Union[List[str], List[Dict[str, Any]]]]:
        """
        Retrieve video set for a tournament, optionally with metadata.

        Algorithm:
            1. Check if tournament videos key exists
            2. If not, return None (tournament not found)
            3. Fetch all video entries from LIST (stored as JSON strings)
            4. If with_metadata=True, parse JSON and return full dicts
            5. If with_metadata=False, parse JSON and return just video_ids

        Args:
            tournament_id: Tournament identifier
            with_metadata: If True, return full metadata dicts; if False, return just video_ids

        Returns:
            If with_metadata=True: List[Dict] with video_id, canister_id, post_id, publisher_user_id
            If with_metadata=False: List[str] of video_ids
            None if tournament not found
        """
        videos_key = self._key_tournament_videos(tournament_id)

        # Check existence first
        if not await self.db.exists(videos_key):
            return None

        client = await self.db.get_client()
        json_entries = await client.lrange(videos_key, 0, -1)

        # Parse JSON entries
        videos = [json.loads(entry) for entry in json_entries]

        if with_metadata:
            return videos
        else:
            # Extract just video_ids
            return [v["video_id"] for v in videos]

    async def get_tournament_meta(self, tournament_id: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve metadata for a tournament.

        Algorithm:
            1. Check if tournament meta key exists
            2. If not, return None
            3. Fetch all fields from HASH
            4. Return as dict

        Args:
            tournament_id: Tournament identifier

        Returns:
            dict with created_at, video_count if exists, None otherwise
        """
        meta_key = self._key_tournament_meta(tournament_id)

        # Check existence first
        if not await self.db.exists(meta_key):
            return None

        client = await self.db.get_client()
        meta = await client.hgetall(meta_key)

        if not meta:
            return None

        return {
            "created_at": meta.get("created_at", ""),
            "video_count": int(meta.get("video_count", 0))
        }

    async def get_tournament_overlap(
        self,
        tournament_a_id: str,
        tournament_b_id: str,
        include_video_ids: bool = True
    ) -> Optional[Dict[str, Any]]:
        """
        Calculate video overlap between two tournaments.

        Algorithm:
            1. Validate both tournament IDs are different
            2. Fetch video IDs for tournament A (using get_tournament_videos)
            3. Fetch video IDs for tournament B
            4. If either tournament doesn't exist, return error dict
            5. Convert both lists to sets for O(1) intersection
            6. Calculate intersection (overlapping videos)
            7. Compute overlap percentages relative to each tournament
            8. Return overlap statistics

        Args:
            tournament_a_id: First tournament identifier
            tournament_b_id: Second tournament identifier
            include_video_ids: If True, include list of overlapping video IDs in response

        Returns:
            Dict with overlap statistics:
                - tournament_a_id: str
                - tournament_b_id: str
                - tournament_a_video_count: int
                - tournament_b_video_count: int
                - overlap_count: int
                - overlap_percentage_a: float (overlap as % of tournament A)
                - overlap_percentage_b: float (overlap as % of tournament B)
                - overlapping_video_ids: List[str] (if include_video_ids=True)
            Error dict if either tournament not found or same tournament provided
        """
        # Same tournament check
        if tournament_a_id == tournament_b_id:
            return {
                "error": "same_tournament",
                "message": "Cannot compare a tournament with itself"
            }

        # Fetch video IDs for both tournaments (without metadata for efficiency)
        videos_a = await self.get_tournament_videos(tournament_a_id, with_metadata=False)
        videos_b = await self.get_tournament_videos(tournament_b_id, with_metadata=False)

        # Handle tournament not found cases
        if videos_a is None:
            return {
                "error": "tournament_not_found",
                "tournament_id": tournament_a_id,
                "message": f"Tournament {tournament_a_id} does not exist"
            }
        if videos_b is None:
            return {
                "error": "tournament_not_found",
                "tournament_id": tournament_b_id,
                "message": f"Tournament {tournament_b_id} does not exist"
            }

        # Convert to sets and calculate intersection
        set_a = set(videos_a)
        set_b = set(videos_b)
        overlapping = set_a & set_b

        # Calculate percentages (handle division by zero)
        count_a = len(videos_a)
        count_b = len(videos_b)
        overlap_count = len(overlapping)

        percentage_a = (overlap_count / count_a * 100) if count_a > 0 else 0.0
        percentage_b = (overlap_count / count_b * 100) if count_b > 0 else 0.0

        # Build response
        result = {
            "tournament_a_id": tournament_a_id,
            "tournament_b_id": tournament_b_id,
            "tournament_a_video_count": count_a,
            "tournament_b_video_count": count_b,
            "overlap_count": overlap_count,
            "overlap_percentage_a": round(percentage_a, 2),
            "overlap_percentage_b": round(percentage_b, 2),
        }

        if include_video_ids:
            result["overlapping_video_ids"] = sorted(list(overlapping))

        return result

    async def list_all_tournaments(self) -> List[Dict[str, Any]]:
        """
        List all tournaments with their current status.

        Algorithm:
            1. Get all tournament IDs from registry ZSET (tournament:all)
            2. For backward compat, also SCAN for tournament:*:meta keys not in registry
            3. For each tournament, fetch metadata (created_at, video_count)
            4. Calculate status: "active" if now < created_at + DURATION, else "expired"
            5. Calculate expires_at: created_at + DURATION_SECONDS
            6. Return list sorted by created_at descending

        Returns:
            List of dicts with: tournament_id, status, video_count, created_at, expires_at
        """
        registry_key = self._key_tournament_registry()
        client = await self.db.get_client()
        current_time = int(time.time())

        # Get tournament IDs from registry ZSET (score = created_at timestamp)
        registry_entries = await client.zrange(registry_key, 0, -1, withscores=True)
        registry_ids = set()
        tournaments = []

        for tournament_id, created_at_score in registry_entries:
            registry_ids.add(tournament_id)
            # Fetch metadata for this tournament
            meta = await self.get_tournament_meta(tournament_id)
            if meta is None:
                # Tournament data expired/deleted but still in registry - clean up
                await client.zrem(registry_key, tournament_id)
                continue

            created_at_ts = int(created_at_score)
            expires_at_ts = created_at_ts + TOURNAMENT_DURATION_SECONDS

            # Determine status: active if within duration, else expired
            status = "active" if current_time < expires_at_ts else "expired"

            tournaments.append({
                "tournament_id": tournament_id,
                "status": status,
                "video_count": meta["video_count"],
                "created_at": meta["created_at"],
                "expires_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_at_ts))
            })

        # Backward compatibility: scan for tournament:*:meta keys not in registry
        # This handles tournaments created before the registry was introduced
        cursor = 0
        while True:
            cursor, keys = await client.scan(cursor, match="tournament:*:meta", count=100)
            for key in keys:
                # Extract tournament_id from key pattern "tournament:{id}:meta"
                parts = key.split(":")
                if len(parts) == 3:
                    tid = parts[1]
                    if tid not in registry_ids and tid != "recent_videos" and tid != "all":
                        # Found a tournament not in registry - add it
                        meta = await self.get_tournament_meta(tid)
                        if meta:
                            # Parse created_at to get timestamp
                            try:
                                created_at_struct = time.strptime(meta["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                                created_at_ts = int(time.mktime(created_at_struct))
                            except ValueError:
                                created_at_ts = current_time  # Fallback

                            expires_at_ts = created_at_ts + TOURNAMENT_DURATION_SECONDS
                            status = "active" if current_time < expires_at_ts else "expired"

                            # Add to registry for future queries
                            await client.zadd(registry_key, {tid: created_at_ts})

                            tournaments.append({
                                "tournament_id": tid,
                                "status": status,
                                "video_count": meta["video_count"],
                                "created_at": meta["created_at"],
                                "expires_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(expires_at_ts))
                            })
                            registry_ids.add(tid)

            if cursor == 0:
                break

        # Sort by created_at descending (newest first)
        tournaments.sort(key=lambda x: x["created_at"], reverse=True)

        return tournaments

    async def delete_tournament(self, tournament_id: str) -> Optional[Dict[str, Any]]:
        """
        Delete a tournament and clear its videos from cooldown.

        Algorithm:
            1. Check if tournament exists (return None if not)
            2. Get tournament metadata for status calculation
            3. Fetch all video IDs from tournament:{id}:videos
            4. Remove each video_id from tournament:recent_videos ZSET (clears cooldown)
            5. Delete tournament:{id}:videos LIST
            6. Delete tournament:{id}:meta HASH
            7. Remove tournament_id from tournament:all registry ZSET
            8. Return deletion summary

        Args:
            tournament_id: Tournament to delete

        Returns:
            Dict with: tournament_id, deleted, status_before_deletion, videos_removed_from_cooldown, message
            None if tournament not found
        """
        # Check if tournament exists
        videos_key = self._key_tournament_videos(tournament_id)
        meta_key = self._key_tournament_meta(tournament_id)
        recent_key = self._key_tournament_recent_videos()
        registry_key = self._key_tournament_registry()

        if not await self.db.exists(videos_key):
            return None

        client = await self.db.get_client()
        current_time = int(time.time())

        # Get metadata for status determination before deletion
        meta = await self.get_tournament_meta(tournament_id)
        status_before = "expired"
        if meta:
            try:
                created_at_struct = time.strptime(meta["created_at"], "%Y-%m-%dT%H:%M:%SZ")
                created_at_ts = int(time.mktime(created_at_struct))
                expires_at_ts = created_at_ts + TOURNAMENT_DURATION_SECONDS
                status_before = "active" if current_time < expires_at_ts else "expired"
            except ValueError:
                pass

        # Fetch video IDs to remove from cooldown
        video_ids = await self.get_tournament_videos(tournament_id, with_metadata=False)
        videos_removed = 0

        if video_ids:
            # Remove videos from cooldown ZSET using pipeline
            pipe = client.pipeline()
            for vid in video_ids:
                pipe.zrem(recent_key, vid)
            results = await pipe.execute()
            # Count how many were actually removed (1 if existed, 0 if not)
            videos_removed = sum(1 for r in results if r > 0)

        # Delete tournament data and remove from registry using pipeline
        pipe = client.pipeline()
        pipe.delete(videos_key)
        pipe.delete(meta_key)
        pipe.zrem(registry_key, tournament_id)
        await pipe.execute()

        logger.info(
            f"Deleted tournament {tournament_id} (was {status_before}), "
            f"removed {videos_removed} videos from cooldown"
        )

        return {
            "tournament_id": tournament_id,
            "deleted": True,
            "status_before_deletion": status_before,
            "videos_removed_from_cooldown": videos_removed,
            "message": f"Tournament deleted and {videos_removed} videos removed from cooldown"
        }