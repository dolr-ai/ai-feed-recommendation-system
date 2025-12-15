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
from typing import List, Dict, Tuple, Optional, Any
from utils.async_redis_utils import AsyncDragonflyService

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
        self.refill_from_popularity_buckets = None
        self.refill_from_freshness_cascade = None
        self.refill_from_single_source = None

    async def initialize(self):
        """
        Async initialization of all Lua scripts.
        Must be called after creating the AsyncLuaScripts instance.
        """
        # Register all scripts (async)
        self.fetch_and_consume = self._register_fetch_and_consume()
        self.add_videos_with_ttl = self._register_add_videos_with_ttl()
        self.filter_and_add_videos = self._register_filter_and_add_videos()
        self.count_valid_videos = self._register_count_valid_videos()
        self.check_videos_not_watched = self._register_check_videos_not_watched()
        self.refill_from_popularity_buckets = self._register_refill_from_popularity_buckets()
        self.refill_from_freshness_cascade = self._register_refill_from_freshness_cascade()
        self.refill_from_single_source = self._register_refill_from_single_source()

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

        EXACT SAME LUA SCRIPT AS SYNC VERSION - NO BUSINESS LOGIC CHANGE.

        This is THE core fetch operation that ensures:
        1. Pool is ALWAYS clean (sent videos removed immediately)
        2. Videos can never be sent twice (marked for cooldown atomically)
        3. Client doesn't need to call any other methods

        Algorithm (UNCHANGED):
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

    def _register_refill_from_popularity_buckets(self):
        """
        Refill a user's to_show zset from popularity buckets with internal downshift logic.
        EXACT SAME LUA SCRIPT AS SYNC VERSION - NO BUSINESS LOGIC CHANGE.

        KEYS[1]: to_show_key (ZSET)
        KEYS[2]: watched_key (ZSET)
        KEYS[3]: bloom_key (BF)
        KEYS[4]: pointer_key (STRING) - stores current bucket pointer
        KEYS[5-15]: Global popularity bucket keys in order (pop_99_100, pop_90_99, ..., pop_0_10) - 11 ZSETs
        ARGV[1]: now (int)
        ARGV[2]: ttl_seconds (int)
        ARGV[3]: target_count (int)
        ARGV[4]: batch_size (int)
        ARGV[5]: max_iterations_per_bucket (int) - try each bucket this many times before downshifting
        ARGV[6]: max_capacity (int) - maximum pool size to cap at after refill

        Returns: {added_total, iterations_total, filtered_watched, filtered_bloom, valid_after, bucket_start, bucket_final, sources_exhausted}
        Algorithm (UNCHANGED):
        - Bucket order in Lua: ["99_100", "90_99", ..., "0_10"] mapped to KEYS[5-15]
        - Read current bucket from pointer_key
        - For each bucket starting from current:
          - Try for exactly max_iterations_per_bucket (10) iterations
          - If target met during iterations, update pointer and return success
          - If 10 iterations done and target not met = "10 errors" → downshift to next bucket
        - Update pointer to final bucket used
        - Return stats with exhaustion flag if all buckets tried
        """
        script = """
        local to_show_key = KEYS[1]
        local watched_key = KEYS[2]
        local bloom_key = KEYS[3]
        local pointer_key = KEYS[4]
        -- KEYS[5-15]: Global popularity bucket keys (99_100 down to 0_10)
        local now = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local target = tonumber(ARGV[3])
        local batch_size = tonumber(ARGV[4])
        local max_iters_per_bucket = tonumber(ARGV[5])
        local max_capacity = tonumber(ARGV[6])

        -- Helper function to cap pool size at max_capacity (keeps newest by expiry score)
        local function cap_pool_size(pool_key, capacity)
            if capacity and capacity > 0 then
                local current_size = redis.call('ZCARD', pool_key)
                if current_size > capacity then
                    redis.call('ZREMRANGEBYRANK', pool_key, 0, current_size - capacity - 1)
                end
            end
        end

        -- Hardcoded bucket order (from highest to lowest popularity)
        local buckets = {"99_100", "90_99", "80_90", "70_80", "60_70", "50_60", "40_50", "30_40", "20_30", "10_20", "0_10"}

        -- Map bucket names to KEYS indices (KEYS[5] = 99_100, KEYS[6] = 90_99, ..., KEYS[15] = 0_10)
        local bucket_to_key_idx = {
            ["99_100"] = 5, ["90_99"] = 6, ["80_90"] = 7, ["70_80"] = 8, ["60_70"] = 9,
            ["50_60"] = 10, ["40_50"] = 11, ["30_40"] = 12, ["20_30"] = 13, ["10_20"] = 14, ["0_10"] = 15
        }

        -- Read current bucket pointer from Redis
        local current_bucket = redis.call('GET', pointer_key)
        if not current_bucket then
            current_bucket = "99_100"  -- Default to highest bucket
        end

        -- Find starting index in bucket list
        local start_idx = 1
        for i, bucket in ipairs(buckets) do
            if bucket == current_bucket then
                start_idx = i
                break
            end
        end

        -- Helper function to count unwatched videos with valid TTL
        local function count_unwatched_valid(to_show, watched, bloom, min_time)
            local candidates = redis.call('ZRANGEBYSCORE', to_show, min_time, '+inf')
            local unwatched_count = 0
            for _, vid in ipairs(candidates) do
                local in_watched = redis.call('ZSCORE', watched, vid)
                if not in_watched then
                    local in_bloom = redis.call('BF.EXISTS', bloom, vid)
                    if in_bloom == 0 then
                        unwatched_count = unwatched_count + 1
                    end
                end
            end
            return unwatched_count
        end

        -- Housekeeping: cleanup to_show and watched once
        redis.call('ZREMRANGEBYSCORE', to_show_key, '-inf', now)
        redis.call('ZREMRANGEBYSCORE', watched_key, '-inf', now)

        local added_total = 0
        local filtered_watched = 0
        local filtered_bloom = 0
        local iterations_total = 0
        local valid_count = 0
        local seen_videos = {}  -- Track videos added in this execution to avoid counting duplicates
        local expiry_time = now + ttl
        local min_ok = now + (ttl * 0.01)
        local bucket_start = current_bucket
        local bucket_final = current_bucket
        local sources_exhausted = 1  -- Assume exhausted unless we meet target

        -- Cascade through buckets starting from current pointer
        for bucket_idx = start_idx, #buckets do
            local bucket = buckets[bucket_idx]
            local source_key = KEYS[bucket_to_key_idx[bucket]]

            -- Cleanup expired from this bucket
            redis.call('ZREMRANGEBYSCORE', source_key, '-inf', now)

            -- Try this bucket for exactly max_iters_per_bucket iterations (10 errors = downshift)
            for i = 1, max_iters_per_bucket do
                iterations_total = iterations_total + 1

                -- Random sampling from ZSET
                local candidates_with_scores = redis.call('ZRANDMEMBER', source_key, batch_size, 'WITHSCORES')

                -- Parse and filter expired videos
                local candidates = {}
                if candidates_with_scores and #candidates_with_scores > 0 then
                    for j = 1, #candidates_with_scores, 2 do
                        local vid = candidates_with_scores[j]
                        local score = tonumber(candidates_with_scores[j + 1])
                        if score and score > now then
                            table.insert(candidates, vid)
                        end
                    end
                end

                -- If no candidates, bucket is empty, break to next bucket immediately
                if (not candidates) or (#candidates == 0) then
                    break
                end

                -- Filter and add videos
                for _, vid in ipairs(candidates) do
                    local in_watched = redis.call('ZSCORE', watched_key, vid)
                    if not in_watched then
                        local in_bloom = redis.call('BF.EXISTS', bloom_key, vid)
                        if in_bloom == 0 then
                            -- Not watched - add to to_show (ZADD is idempotent, updates score if exists)
                            redis.call('ZADD', to_show_key, expiry_time, vid)
                            -- Only count as new if we haven't seen it in this execution
                            if not seen_videos[vid] then
                                added_total = added_total + 1
                                valid_count = valid_count + 1
                                seen_videos[vid] = true
                            end
                        else
                            -- In bloom filter - remove from to_show_key if exists (cleanup)
                            redis.call('ZREM', to_show_key, vid)
                            filtered_bloom = filtered_bloom + 1
                        end
                    else
                        -- In cooldown set - remove from to_show_key if exists (cleanup)
                        redis.call('ZREM', to_show_key, vid)
                        filtered_watched = filtered_watched + 1
                    end
                end

                -- Check if target met using incremental count (O(1) instead of O(N))
                if valid_count >= target then
                    bucket_final = bucket
                    sources_exhausted = 0
                    -- Update pointer to successful bucket
                    redis.call('SET', pointer_key, bucket_final)
                    -- Cap pool size before returning
                    cap_pool_size(to_show_key, max_capacity)
                    local valid_after = count_unwatched_valid(to_show_key, watched_key, bloom_key, min_ok)
                    return {added_total, iterations_total, filtered_watched, filtered_bloom, valid_after, bucket_start, bucket_final, sources_exhausted}
                end
            end

            -- 10 iterations done, target not met = "10 errors"
            -- Downshift to next bucket (continue loop)
            bucket_final = bucket
        end

        -- All buckets exhausted, update pointer to last bucket tried
        redis.call('SET', pointer_key, bucket_final)

        -- Cap pool size before returning
        cap_pool_size(to_show_key, max_capacity)
        local valid_final = count_unwatched_valid(to_show_key, watched_key, bloom_key, min_ok)
        return {added_total, iterations_total, filtered_watched, filtered_bloom, valid_final, bucket_start, bucket_final, sources_exhausted}
        """
        return self.client.register_script(script)

    def _register_refill_from_freshness_cascade(self):
        """
        Refill a user's to_show zset from freshness windows with internal cascade logic.
        EXACT SAME LUA SCRIPT AS SYNC VERSION - NO BUSINESS LOGIC CHANGE.

        KEYS[1]: to_show_key (ZSET)
        KEYS[2]: watched_key (ZSET)
        KEYS[3]: bloom_key (BF)
        KEYS[4-8]: Global freshness window keys in order (fresh_l1d, fresh_l7d, fresh_l14d, fresh_l30d, fresh_l90d) - 5 ZSETs
        ARGV[1]: now (int)
        ARGV[2]: ttl_seconds (int)
        ARGV[3]: target_count (int)
        ARGV[4]: batch_size (int)
        ARGV[5]: max_iterations_per_window (int) - try each window this many times before cascading
        ARGV[6]: max_capacity (int) - maximum pool size to cap at after refill

        Returns: {added_total, iterations_total, filtered_watched, filtered_bloom, valid_after, sources_exhausted}
        Algorithm (UNCHANGED):
        - Window order in Lua: ["l1d", "l7d", "l14d", "l30d", "l90d"] mapped to KEYS[4-8]
        - For each window in order:
          - Try for exactly max_iterations_per_window iterations
          - If target met during iterations, return success
          - If iterations done and target not met → cascade to next window
        - Return stats with exhaustion flag if all windows tried
        """
        script = """
        local to_show_key = KEYS[1]
        local watched_key = KEYS[2]
        local bloom_key = KEYS[3]
        -- KEYS[4-8]: Global freshness window keys (fresh_l1d, fresh_l7d, fresh_l14d, fresh_l30d, fresh_l90d)
        local now = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local target = tonumber(ARGV[3])
        local batch_size = tonumber(ARGV[4])
        local max_iters_per_window = tonumber(ARGV[5])
        local max_capacity = tonumber(ARGV[6])

        -- Helper function to cap pool size at max_capacity (keeps newest by expiry score)
        local function cap_pool_size(pool_key, capacity)
            if capacity and capacity > 0 then
                local current_size = redis.call('ZCARD', pool_key)
                if current_size > capacity then
                    redis.call('ZREMRANGEBYRANK', pool_key, 0, current_size - capacity - 1)
                end
            end
        end

        -- Hardcoded freshness window order (from freshest to oldest)
        local windows = {"l1d", "l7d", "l14d", "l30d", "l90d"}

        -- Map window names to KEYS indices (KEYS[4] = l1d, KEYS[5] = l7d, ..., KEYS[8] = l90d)
        local window_to_key_idx = {
            ["l1d"] = 4, ["l7d"] = 5, ["l14d"] = 6, ["l30d"] = 7, ["l90d"] = 8
        }

        -- Helper function to count unwatched videos with valid TTL
        local function count_unwatched_valid(to_show, watched, bloom, min_time)
            local candidates = redis.call('ZRANGEBYSCORE', to_show, min_time, '+inf')
            local unwatched_count = 0
            for _, vid in ipairs(candidates) do
                local in_watched = redis.call('ZSCORE', watched, vid)
                if not in_watched then
                    local in_bloom = redis.call('BF.EXISTS', bloom, vid)
                    if in_bloom == 0 then
                        unwatched_count = unwatched_count + 1
                    end
                end
            end
            return unwatched_count
        end

        -- Housekeeping: cleanup to_show and watched once
        redis.call('ZREMRANGEBYSCORE', to_show_key, '-inf', now)
        redis.call('ZREMRANGEBYSCORE', watched_key, '-inf', now)

        local added_total = 0
        local filtered_watched = 0
        local filtered_bloom = 0
        local iterations_total = 0
        local valid_count = 0
        local seen_videos = {}  -- Track videos added in this execution to avoid counting duplicates
        local expiry_time = now + ttl
        local min_ok = now + (ttl * 0.01)
        local sources_exhausted = 1  -- Assume exhausted unless we meet target

        -- Cascade through freshness windows
        for _, window in ipairs(windows) do
            local source_key = KEYS[window_to_key_idx[window]]

            -- Cleanup expired from this window
            redis.call('ZREMRANGEBYSCORE', source_key, '-inf', now)

            -- Try this window for max_iters_per_window iterations
            for i = 1, max_iters_per_window do
                iterations_total = iterations_total + 1

                -- Random sampling from ZSET
                local candidates_with_scores = redis.call('ZRANDMEMBER', source_key, batch_size, 'WITHSCORES')

                -- Parse and filter expired videos
                local candidates = {}
                if candidates_with_scores and #candidates_with_scores > 0 then
                    for j = 1, #candidates_with_scores, 2 do
                        local vid = candidates_with_scores[j]
                        local score = tonumber(candidates_with_scores[j + 1])
                        if score and score > now then
                            table.insert(candidates, vid)
                        end
                    end
                end

                -- If no candidates, window is empty, break to next window immediately
                if (not candidates) or (#candidates == 0) then
                    break
                end

                -- Filter and add videos
                for _, vid in ipairs(candidates) do
                    local in_watched = redis.call('ZSCORE', watched_key, vid)
                    if not in_watched then
                        local in_bloom = redis.call('BF.EXISTS', bloom_key, vid)
                        if in_bloom == 0 then
                            -- Not watched - add to to_show (ZADD is idempotent, updates score if exists)
                            redis.call('ZADD', to_show_key, expiry_time, vid)
                            -- Only count as new if we haven't seen it in this execution
                            if not seen_videos[vid] then
                                added_total = added_total + 1
                                valid_count = valid_count + 1
                                seen_videos[vid] = true
                            end
                        else
                            -- In bloom filter - remove from to_show_key if exists (cleanup)
                            redis.call('ZREM', to_show_key, vid)
                            filtered_bloom = filtered_bloom + 1
                        end
                    else
                        -- In cooldown set - remove from to_show_key if exists (cleanup)
                        redis.call('ZREM', to_show_key, vid)
                        filtered_watched = filtered_watched + 1
                    end
                end

                -- Check if target met using incremental count (O(1) instead of O(N))
                if valid_count >= target then
                    sources_exhausted = 0
                    -- Cap pool size before returning
                    cap_pool_size(to_show_key, max_capacity)
                    local valid_after = count_unwatched_valid(to_show_key, watched_key, bloom_key, min_ok)
                    return {added_total, iterations_total, filtered_watched, filtered_bloom, valid_after, sources_exhausted}
                end
            end

            -- Max iterations done for this window, cascade to next
        end

        -- All windows exhausted
        -- Cap pool size before returning
        cap_pool_size(to_show_key, max_capacity)
        local valid_final = count_unwatched_valid(to_show_key, watched_key, bloom_key, min_ok)
        return {added_total, iterations_total, filtered_watched, filtered_bloom, valid_final, sources_exhausted}
        """
        return self.client.register_script(script)

    def _register_refill_from_single_source(self):
        """
        Refill from a single global ZSET source (for fallback pool).
        EXACT SAME LUA SCRIPT AS SYNC VERSION - NO BUSINESS LOGIC CHANGE.

        KEYS[1]: to_show_key (ZSET)
        KEYS[2]: watched_key (ZSET)
        KEYS[3]: bloom_key (BF)
        KEYS[4]: source_key (ZSET)
        ARGV[1]: now (int)
        ARGV[2]: ttl_seconds (int)
        ARGV[3]: target_count (int)
        ARGV[4]: batch_size (int)
        ARGV[5]: max_iterations (int)
        ARGV[6]: max_capacity (int) - maximum pool size to cap at after refill

        Returns: {added_total, iterations, filtered_watched, filtered_bloom, valid_after, sources_exhausted}
        """
        script = """
        local to_show_key = KEYS[1]
        local watched_key = KEYS[2]
        local bloom_key = KEYS[3]
        local source_key = KEYS[4]
        local now = tonumber(ARGV[1])
        local ttl = tonumber(ARGV[2])
        local target = tonumber(ARGV[3])
        local batch_size = tonumber(ARGV[4])
        local max_iters = tonumber(ARGV[5])
        local max_capacity = tonumber(ARGV[6])

        -- Helper function to cap pool size at max_capacity (keeps newest by expiry score)
        local function cap_pool_size(pool_key, capacity)
            if capacity and capacity > 0 then
                local current_size = redis.call('ZCARD', pool_key)
                if current_size > capacity then
                    redis.call('ZREMRANGEBYRANK', pool_key, 0, current_size - capacity - 1)
                end
            end
        end

        -- Helper function to count unwatched videos with valid TTL
        local function count_unwatched_valid(to_show, watched, bloom, min_time)
            local candidates = redis.call('ZRANGEBYSCORE', to_show, min_time, '+inf')
            local unwatched_count = 0
            for _, vid in ipairs(candidates) do
                local in_watched = redis.call('ZSCORE', watched, vid)
                if not in_watched then
                    local in_bloom = redis.call('BF.EXISTS', bloom, vid)
                    if in_bloom == 0 then
                        unwatched_count = unwatched_count + 1
                    end
                end
            end
            return unwatched_count
        end

        -- Housekeeping
        redis.call('ZREMRANGEBYSCORE', source_key, '-inf', now)
        redis.call('ZREMRANGEBYSCORE', to_show_key, '-inf', now)
        redis.call('ZREMRANGEBYSCORE', watched_key, '-inf', now)

        local added_total = 0
        local filtered_watched = 0
        local filtered_bloom = 0
        local iterations = 0
        local valid_count = 0
        local seen_videos = {}  -- Track videos added in this execution to avoid counting duplicates
        local expiry_time = now + ttl
        local min_ok = now + (ttl * 0.01)

        for i = 1, max_iters do
            iterations = iterations + 1

            local candidates_with_scores = redis.call('ZRANDMEMBER', source_key, batch_size, 'WITHSCORES')

            local candidates = {}
            if candidates_with_scores and #candidates_with_scores > 0 then
                for j = 1, #candidates_with_scores, 2 do
                    local vid = candidates_with_scores[j]
                    local score = tonumber(candidates_with_scores[j + 1])
                    if score and score > now then
                        table.insert(candidates, vid)
                    end
                end
            end

            if (not candidates) or (#candidates == 0) then
                break
            end

            for _, vid in ipairs(candidates) do
                local in_watched = redis.call('ZSCORE', watched_key, vid)
                if not in_watched then
                    local in_bloom = redis.call('BF.EXISTS', bloom_key, vid)
                    if in_bloom == 0 then
                        -- Not watched - add to to_show (ZADD is idempotent, updates score if exists)
                        redis.call('ZADD', to_show_key, expiry_time, vid)
                        -- Only count as new if we haven't seen it in this execution
                        if not seen_videos[vid] then
                            added_total = added_total + 1
                            valid_count = valid_count + 1
                            seen_videos[vid] = true
                        end
                    else
                        -- In bloom filter - remove from to_show_key if exists (cleanup)
                        redis.call('ZREM', to_show_key, vid)
                        filtered_bloom = filtered_bloom + 1
                    end
                else
                    -- In cooldown set - remove from to_show_key if exists (cleanup)
                    redis.call('ZREM', to_show_key, vid)
                    filtered_watched = filtered_watched + 1
                end
            end

            -- Check if target met using incremental count (O(1) instead of O(N))
            if valid_count >= target then
                -- Cap pool size before returning
                cap_pool_size(to_show_key, max_capacity)
                local valid_after = count_unwatched_valid(to_show_key, watched_key, bloom_key, min_ok)
                return {added_total, iterations, filtered_watched, filtered_bloom, valid_after, 0}
            end
        end

        -- Cap pool size before returning
        cap_pool_size(to_show_key, max_capacity)
        local valid_final = count_unwatched_valid(to_show_key, watched_key, bloom_key, min_ok)
        return {added_total, iterations, filtered_watched, filtered_bloom, valid_final, 1}
        """
        return self.client.register_script(script)


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
        dragonfly_service: AsyncDragonflyService,
        enable_lua_logging: bool = ENABLE_LUA_SCRIPT_LOGGING,
    ):
        """
        Initialize async Redis layer with Dragonfly service.

        Args:
            dragonfly_service: AsyncDragonflyService instance
            enable_lua_logging: Enable detailed logging for Lua script execution
        """
        self.db = dragonfly_service
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
        SAME AS SYNC VERSION - no change needed.

        Args:
            user_id: User ID
            rec_type: Recommendation type (e.g., 'popularity', 'freshness', 'fallback')
        """
        return f"user:{user_id}:videos_to_show:{rec_type}"

    def _key_watched_short_lived(self, user_id: str) -> str:
        """Generate key for user's short-lived watched set. SAME AS SYNC."""
        return f"user:{user_id}:watched:short"

    def _key_bloom_permanent(self, user_id: str) -> str:
        """Generate key for user's permanent bloom filter. SAME AS SYNC."""
        return f"user:{user_id}:bloom:permanent"

    def _key_global_pop_set(self, bucket: str) -> str:
        """Generate key for global popularity SET bucket. SAME AS SYNC."""
        return f"user:GLOBAL:pool:pop_{bucket}"

    def _key_global_fresh_zset(self, window: str) -> str:
        """Generate key for global freshness ZSET window. SAME AS SYNC."""
        return f"user:GLOBAL:pool:fresh_{window}"

    def _key_global_fallback_zset(self) -> str:
        """Generate key for global fallback ZSET. SAME AS SYNC."""
        return "user:GLOBAL:pool:fallback"

    def _key_global_ugc_zset(self) -> str:
        """Generate key for global UGC (User-Generated Content) ZSET."""
        return "user:GLOBAL:pool:ugc"

    def _key_pop_percentile_pointer(self, user_id: str) -> str:
        """Pointer key for user's popularity percentile bucket. SAME AS SYNC."""
        return f"user:{user_id}:pop_percentile_pointer"

    def _key_refill_failures(self, user_id: str, rec_type: str) -> str:
        """Key for tracking consecutive refill failures. SAME AS SYNC."""
        return f"user:{user_id}:refill_failures:{rec_type}"

    def _key_refill_lock(self, user_id: str, rec_type: str) -> str:
        """Lock key for refilling a user's rec_type. SAME AS SYNC."""
        return f"user:{user_id}:refill:lock:{rec_type}"

    def _key_metrics_attempts(self, rec_type: str) -> str:
        return f"metrics:refill:attempts:{rec_type}"

    def _key_metrics_failures(self, rec_type: str) -> str:
        return f"metrics:refill:failures:{rec_type}"

    def _key_refill_last_stats(self, user_id: str, rec_type: str) -> str:
        return f"user:{user_id}:refill:last:{rec_type}"

    def _key_following_last_sync(self, user_id: str) -> str:
        """Key for tracking last sync timestamp for user's following pool."""
        return f"user:{user_id}:following:last_sync"

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
            # Overwrite value and re-apply remaining TTL
            ok = bool(await self.db.set(key, bucket))
            try:
                await self.db.expire(key, ttl_seconds)
            except Exception:
                # If expire fails, return the set result since value still updated
                return ok
            return ok
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
        current_time = int(time.time())

        # Initial fetch attempt + parallel count check (no added latency)
        max_to_check = min(count * 10, 10000)  # Buffer for filtering

        # Run fetch and count in parallel
        fetch_task = self.scripts._execute_script(
            self.scripts.fetch_and_consume,
            "fetch_and_consume",
            keys=[to_show_key, watched_key, bloom_key],
            args=[current_time, count, max_to_check, TTL_WATCHED_SET],
            semaphore=self.redis_semaphore,
        )
        count_task = self.count_valid_videos_to_show(user_id, rec_type)

        # Wait for both to complete
        result, valid_count_in_pool = await asyncio.gather(fetch_task, count_task)

        videos = result[0]
        removed_from_pool = result[1]
        filtered_watched = result[2]
        filtered_bloom = result[3]

        logger.info(
            f"Fetched {len(videos)}/{count} videos for {user_id}:{rec_type} "
            f"(removed={removed_from_pool}, filtered_w={filtered_watched}, filtered_b={filtered_bloom}), "
            f"pool has {valid_count_in_pool} valid videos"
        )

        # Trigger background refill if pool is low (fire-and-forget, non-blocking)
        if valid_count_in_pool < REFILL_THRESHOLD:
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
                    else:
                        logger.warning(f"Unknown rec_type '{rec_type}', skipping refill")
                        break
                except Exception as e:
                    logger.error(f"Refill failed for {user_id}:{rec_type}: {e}")
                    break

                # Retry fetch after refill
                current_time = int(time.time())
                result = await self.scripts._execute_script(
                    self.scripts.fetch_and_consume,
                    "fetch_and_consume",
                    keys=[to_show_key, watched_key, bloom_key],
                    args=[current_time, count - len(videos), max_to_check, TTL_WATCHED_SET],
                    semaphore=self.redis_semaphore,
                )

                new_videos = result[0]
                removed = result[1]
                logger.info(f"Refill attempt {attempt}: fetched {len(new_videos)} additional videos (removed={removed})")

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
                    result = await self.scripts._execute_script(
                        self.scripts.fetch_and_consume,
                        "fetch_and_consume",
                        keys=[fallback_key, watched_key, bloom_key],
                        args=[current_time, count - len(videos), max_to_check, TTL_WATCHED_SET],
                        semaphore=self.redis_semaphore,
                    )
                    fallback_videos = result[0]
                    logger.info(f"Fetched {len(fallback_videos)} from fallback")
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

        return videos[:count]  # Trim to requested count

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
        except Exception:
            pass

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
        Refill user's `videos_to_show:popularity` from global popularity ZSETs with Lua-managed downshift.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Algorithm (UNCHANGED):
        - Acquire lock `user:{user_id}:refill:lock:popularity`.
        - Ensure bloom exists.
        - Call Lua script with pointer_key - Lua handles everything:
          * Reads current bucket from pointer_key
          * Hardcoded bucket order in Lua: ["99_100", "90_99", ..., "0_10"]
          * Tries current bucket for 10 iterations
          * If target not met after 10 iterations = "10 errors" → downshifts to next bucket
          * Continues until target met or all buckets exhausted
          * Updates pointer_key to final bucket
        - Persist stats and release lock.

        Returns:
        - Dict with fields: added_total, iterations, filtered_watched, filtered_bloom, valid_after,
          bucket_start, bucket_final, sources_exhausted
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

            # Build list of all 11 global popularity bucket keys (KEYS[5-15])
            bucket_keys = [self._key_global_pop_set(bucket) for bucket in self.PERCENTILE_BUCKETS_ORDER]

            # Call Lua script - passes pointer_key and all bucket keys, Lua manages downshift internally
            result = await self.scripts._execute_script(
                self.scripts.refill_from_popularity_buckets,
                "refill_from_popularity_buckets",
                keys=[to_show_key, watched_key, bloom_key, pointer_key] + bucket_keys,
                args=[now, ttl, target, batch_size, max_iterations, VIDEOS_TO_SHOW_CAPACITY],
                semaphore=self.redis_semaphore,
            )

            # Parse result (8 values)
            added_total = int(result[0])
            iterations = int(result[1])
            filtered_watched = int(result[2])
            filtered_bloom = int(result[3])
            valid_after = int(result[4])
            bucket_start = result[5].decode('utf-8') if isinstance(result[5], bytes) else result[5]
            bucket_final = result[6].decode('utf-8') if isinstance(result[6], bytes) else result[6]
            sources_exhausted = int(result[7])  # 1 if exhausted, 0 if target met

            stats = {
                "added_total": added_total,
                "iterations": iterations,
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
        Refill user's `videos_to_show:freshness` from cascading freshness windows with Lua-managed cascade.
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Algorithm (UNCHANGED):
        - Acquire lock `user:{user_id}:refill:lock:freshness`.
        - Ensure bloom exists.
        - Call Lua script - Lua handles everything:
          * Hardcoded window order in Lua: ["l1d", "l7d", "l14d", "l30d", "l90d"]
          * Tries each window for 10 iterations
          * If target not met after 10 iterations → cascades to next window
          * Continues until target met or all windows exhausted
        - Persist stats and release lock.

        Returns:
        - Dict with stats including: added_total, iterations, filtered_watched, filtered_bloom,
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

            # Build list of all 5 global freshness window keys (KEYS[4-8])
            window_keys = [self._key_global_fresh_zset(w) for w in self.FRESHNESS_WINDOWS_ORDER]

            # Call Lua script - Lua manages window cascade internally
            result = await self.scripts._execute_script(
                self.scripts.refill_from_freshness_cascade,
                "refill_from_freshness_cascade",
                keys=[to_show_key, watched_key, bloom_key] + window_keys,
                args=[now, ttl, target, batch_size, max_iterations, VIDEOS_TO_SHOW_CAPACITY],
                semaphore=self.redis_semaphore,
            )

            # Parse result (6 values)
            added_total = int(result[0])
            iterations = int(result[1])
            filtered_watched = int(result[2])
            filtered_bloom = int(result[3])
            valid_after = int(result[4])
            sources_exhausted = int(result[5])  # 1 if exhausted, 0 if target met

            stats = {
                "added_total": added_total,
                "iterations": iterations,
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
        ASYNC VERSION - EXACT SAME BUSINESS LOGIC AS SYNC.

        Algorithm (UNCHANGED):
        - Acquire lock for `fallback` rec_type.
        - Use `refill_from_single_source` with `user:GLOBAL:pool:fallback`.
        - Persist stats and release lock.
        """
        rec_type = "fallback"
        await self._acquire_lock(user_id, rec_type)
        try:
            await self.init_user_bloom_filter(user_id)
            to_show_key = self._key_videos_to_show(user_id, rec_type)
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)
            global_zset_key = self._key_global_fallback_zset()
            now = int(time.time())

            result = await self.scripts._execute_script(
                self.scripts.refill_from_single_source,
                "refill_from_single_source",
                keys=[to_show_key, watched_key, bloom_key, global_zset_key],
                args=[now, ttl, target, batch_size, max_iterations, VIDEOS_TO_SHOW_CAPACITY],
                semaphore=self.redis_semaphore,
            )

            # Parse result (6 values)
            stats = {
                "added_total": int(result[0]),
                "iterations": int(result[1]),
                "filtered_watched": int(result[2]),
                "filtered_bloom": int(result[3]),
                "valid_after": int(result[4]),
                "sources_exhausted": bool(int(result[5])),
            }

            # Record metrics
            if stats["sources_exhausted"]:
                await self.db.incr(self._key_metrics_failures(rec_type))

            await self._record_refill_stats(user_id, rec_type, stats)
            return stats
        finally:
            await self._release_lock(user_id, rec_type)

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
            expiry = now + ttl

            # Use filter_and_add_videos Lua script to filter and add in one atomic op
            # This script filters against bloom and watched:short, then adds to pool
            result = await self.scripts._execute_script(
                self.scripts.filter_and_add_videos,
                "filter_and_add_videos",
                keys=[to_show_key, watched_key, bloom_key],
                args=[now, expiry, VIDEOS_TO_SHOW_CAPACITY] + video_ids,
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
        target: int = 100,
        batch_size: int = 100,
        max_iterations: int = 5,
        ttl: int = TTL_UGC_VIDEOS,
    ) -> Dict[str, int]:
        """
        Refill user's `videos_to_show:ugc` from global UGC pool.

        This method refills the user's UGC pool from the global UGC pool
        (user:GLOBAL:pool:ugc), filtering through bloom and watched:short.

        Algorithm:
            1. Acquire lock for 'ugc' rec_type
            2. Ensure user bloom filter exists
            3. Use refill_from_single_source Lua script with global UGC pool
            4. Persist stats and release lock

        Args:
            user_id: User ID to refill UGC pool for
            target: Target number of videos in pool (default: 100, smaller since UGC is 5% of feed)
            batch_size: Batch size for sampling (default: 100)
            max_iterations: Max refill iterations (default: 5)
            ttl: TTL for videos (default: from config)

        Returns:
            Dict with refill stats: added_total, iterations, filtered_watched,
            filtered_bloom, valid_after, sources_exhausted
        """
        rec_type = "ugc"
        await self._acquire_lock(user_id, rec_type)
        try:
            # Ensure bloom filter exists for user
            await self.init_user_bloom_filter(user_id)

            to_show_key = self._key_videos_to_show(user_id, rec_type)
            watched_key = self._key_watched_short_lived(user_id)
            bloom_key = self._key_bloom_permanent(user_id)
            global_ugc_key = self._key_global_ugc_zset()
            now = int(time.time())

            # Use refill_from_single_source Lua script (same as fallback)
            result = await self.scripts._execute_script(
                self.scripts.refill_from_single_source,
                "refill_from_single_source",
                keys=[to_show_key, watched_key, bloom_key, global_ugc_key],
                args=[now, ttl, target, batch_size, max_iterations, VIDEOS_TO_SHOW_CAPACITY],
                semaphore=self.redis_semaphore,
            )

            # Parse result (6 values)
            stats = {
                "added_total": int(result[0]),
                "iterations": int(result[1]),
                "filtered_watched": int(result[2]),
                "filtered_bloom": int(result[3]),
                "valid_after": int(result[4]),
                "sources_exhausted": bool(int(result[5])),
            }

            # Record metrics
            if stats["sources_exhausted"]:
                await self.db.incr(self._key_metrics_failures(rec_type))

            await self._record_refill_stats(user_id, rec_type, stats)

            logger.info(
                f"UGC refill for {user_id}: added={stats['added_total']}, "
                f"filtered_watched={stats['filtered_watched']}, filtered_bloom={stats['filtered_bloom']}"
            )

            return stats
        finally:
            await self._release_lock(user_id, rec_type)

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