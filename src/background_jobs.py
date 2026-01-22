"""
Background jobs for syncing BigQuery data to Redis.

This module implements three scheduled jobs that run every 6 hours:
1. sync_global_popularity_pools: Syncs popular videos into percentile buckets
2. sync_freshness_windows: Syncs fresh videos into time windows
3. sync_user_bloom_filters: Syncs user watch history into bloom filters

All jobs use distributed locking to prevent concurrent execution across
multiple workers and include jitter to prevent thundering herd problems.
"""

import os
import socket
import time
import logging
import asyncio
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta

import pandas as pd
import numpy as np

from utils.bigquery_client import BigQueryClient
from async_main import AsyncRedisLayer, POPULARITY_BUCKET_TTL, TTL_VIDEOS_TO_SHOW
from utils.async_redis_utils import AsyncKVRocksService
from job_logger import get_job_logger
import aiohttp

from config import (
    JOB_LOCK_TTL,
    JOB_LOCK_KEY_PREFIX,
    BQ_BATCH_SIZE,
    BQ_PIPELINE_SIZE,
    PERCENTILE_BUCKETS,
    FRESHNESS_WINDOWS,
    BLOOM_ERROR_RATE,
    BLOOM_INITIAL_CAPACITY,
    BLOOM_EXPANSION,
    BLOOM_TTL_DAYS,
    TTL_UGC_VIDEOS,
    UGC_POOL_CAPACITY,
    GCHAT_WEBHOOK_URL,
    GCHAT_ALERT_MENTION,
    EXCLUDE_SET_KEY,
    EXCLUDE_SET_TEMP_KEY,
)
from utils.metrics_utils import hourly_metrics

logger = logging.getLogger(__name__)


# ============================================================================
# DISTRIBUTED LOCKING
# ============================================================================

async def acquire_job_lock(
    redis_client,
    job_name: str,
    ttl: int = JOB_LOCK_TTL
) -> bool:
    """
    Acquire distributed lock for job execution.

    Args:
        redis_client: Async Redis client
        job_name: Unique name for the job
        ttl: Lock expiry time in seconds

    Returns:
        True if lock acquired, False if another worker holds it

    Algorithm:
        1. Generate unique worker ID from hostname and PID
        2. Attempt to SET lock key with NX (not exists) and EX (expiry)
        3. Return success/failure based on SET result
    """
    lock_key = f"{JOB_LOCK_KEY_PREFIX}{job_name}"
    worker_id = f"{socket.gethostname()}:{os.getpid()}"

    try:
        # SET with NX (only if not exists) and EX (expiry)
        result = await redis_client.set(
            lock_key,
            worker_id,
            nx=True,  # Only set if key doesn't exist
            ex=ttl    # Expire after TTL seconds
        )

        if result:
            logger.info(f"Acquired lock for {job_name} (worker: {worker_id}, TTL: {ttl}s)")
            return True
        else:
            # Another worker has the lock
            current_holder = await redis_client.get(lock_key)
            logger.info(f"Lock for {job_name} held by: {current_holder}")
            return False

    except Exception as e:
        logger.error(f"Error acquiring lock for {job_name}: {e}")
        return False


async def release_job_lock(redis_client, job_name: str) -> bool:
    """
    Release distributed lock after job completion.

    Args:
        redis_client: Async Redis client
        job_name: Unique name for the job

    Returns:
        True if lock released, False otherwise

    Algorithm:
        1. Generate lock key
        2. Check if we own the lock (optional safety check)
        3. Delete the lock key
    """
    lock_key = f"{JOB_LOCK_KEY_PREFIX}{job_name}"
    worker_id = f"{socket.gethostname()}:{os.getpid()}"

    try:
        # Safety check: only release if we own the lock
        current_holder = await redis_client.get(lock_key)
        if current_holder and current_holder == worker_id:
            result = await redis_client.delete(lock_key)
            logger.info(f"Released lock for {job_name}")
            return bool(result)
        else:
            logger.warning(f"Cannot release lock for {job_name} - not owned by this worker")
            return False

    except Exception as e:
        logger.error(f"Error releasing lock for {job_name}: {e}")
        return False


# ============================================================================
# SYNC JOB 1: GLOBAL POPULARITY POOLS
# ============================================================================

async def sync_global_popularity_pools(
    redis_layer: AsyncRedisLayer,
    kvrocks_service: AsyncKVRocksService
) -> None:
    """
    Sync popular videos from BigQuery to Redis percentile buckets.

    Args:
        redis_layer: AsyncRedisLayer instance
        kvrocks_service: AsyncKVRocksService instance

    Algorithm:
        1. Acquire distributed lock
        2. Fetch popular videos with DS scores from BigQuery
        3. Calculate percentile buckets using pandas qcut
        4. Clear existing Redis pools
        5. Batch insert videos into Redis ZSETs with expiry scores
        6. Release lock
    """
    job_name = "popularity_sync"
    start_time = time.time()

    # Get job-specific logger
    job_logger = get_job_logger(job_name, kvrocks_service.client)

    # Try to acquire lock
    if not await acquire_job_lock(kvrocks_service.client, job_name):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return

    try:
        job_logger.info("Starting global popularity pools sync from BigQuery...")

        # Initialize BigQuery client
        bq_client = BigQueryClient()

        # Fetch popular videos with scores
        job_logger.info("Fetching popular videos from BigQuery...")
        popular_df = bq_client.fetch_popular_videos()

        if popular_df.empty:
            job_logger.warning("No popular videos fetched from BigQuery")
            return

        job_logger.info(f"Fetched {len(popular_df)} popular videos")

        # Calculate percentile buckets
        job_logger.info("Calculating percentile buckets...")
        percentile_buckets = _calculate_percentile_buckets(
            popular_df,
            score_column='global_popularity_score'
        )

        # Process each percentile bucket
        now = int(time.time())
        total_inserted = 0

        for bucket_name, video_ids in percentile_buckets.items():
            if not video_ids:
                continue

            video_ids = await filter_videos_with_metadata(
                video_ids, kvrocks_service.client, job_logger
            )
            if not video_ids:
                job_logger.info(f"No videos with metadata for bucket {bucket_name}")
                continue

            # Get bucket-specific TTL
            ttl = POPULARITY_BUCKET_TTL.get(bucket_name, 3 * 24 * 60 * 60)
            expiry = now + ttl

            # Clear existing bucket
            bucket_key = redis_layer._key_global_pop_set(bucket_name) # TODO: INSTEAD OF DELETING, add gradual soft ingestion
            await kvrocks_service.client.delete(bucket_key)

            # Insert videos in batches using pipeline
            for batch in _batch_list(video_ids, BQ_PIPELINE_SIZE):
                video_dict = {vid: float(expiry) for vid in batch}
                await kvrocks_service.client.zadd(bucket_key, video_dict)

            total_inserted += len(video_ids)
            job_logger.info(f"Inserted {len(video_ids)} videos into {bucket_name} (TTL: {ttl}s)")

        elapsed = time.time() - start_time
        job_logger.info(f"Popularity sync completed: {total_inserted} videos in {elapsed:.2f}s")

    except Exception as e:
        job_logger.error(f"Error in popularity sync: {e}", exc_info=True)
        raise

    finally:
        # Always release lock
        await release_job_lock(kvrocks_service.client, job_name)


def _calculate_percentile_buckets(
    df: pd.DataFrame,
    score_column: str
) -> Dict[str, List[str]]:
    """
    Calculate percentile buckets for videos based on scores.

    Args:
        df: DataFrame with video_id and score columns
        score_column: Name of the score column

    Returns:
        Dict mapping bucket name to list of video IDs

    Algorithm:
        1. Define percentile boundaries (0, 10, 20, ..., 90, 99, 100)
        2. Use pandas qcut to assign videos to buckets
        3. Group by bucket and extract video IDs
        4. Map to standard bucket names (0_10, 10_20, etc.)
    """
    # Define percentile boundaries
    percentiles = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 99, 100]

    # Calculate percentile values
    df['percentile'] = pd.qcut(
        df[score_column],
        q=[p/100 for p in percentiles],
        labels=False,
        duplicates='drop'
    )

    # Map to bucket names
    bucket_mapping = {
        0: "0_10",
        1: "10_20",
        2: "20_30",
        3: "30_40",
        4: "40_50",
        5: "50_60",
        6: "60_70",
        7: "70_80",
        8: "80_90",
        9: "90_99",
        10: "99_100"
    }

    # Group videos by bucket
    buckets = {}
    for idx, bucket_name in bucket_mapping.items():
        mask = df['percentile'] == idx
        video_ids = df.loc[mask, 'video_id'].tolist()
        buckets[bucket_name] = video_ids

    return buckets


# ============================================================================
# SYNC JOB 2: FRESHNESS WINDOWS
# ============================================================================

async def sync_freshness_windows(
    redis_layer: AsyncRedisLayer,
    kvrocks_service: AsyncKVRocksService
) -> None:
    """
    Sync fresh videos from BigQuery to Redis time-based windows.

    Args:
        redis_layer: AsyncRedisLayer instance
        kvrocks_service: AsyncKVRocksService instance

    Algorithm:
        1. Acquire distributed lock
        2. Fetch videos with upload timestamps from BigQuery
        3. Videos already bucketed by query (l1d, l7d, l14d, l30d, l90d)
        4. Clear existing Redis pools
        5. Batch insert into Redis ZSETs with TTL scores
        6. Release lock
    """
    job_name = "freshness_sync"
    start_time = time.time()

    # Get job-specific logger
    job_logger = get_job_logger(job_name, kvrocks_service.client)

    # Try to acquire lock
    if not await acquire_job_lock(kvrocks_service.client, job_name):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return

    try:
        job_logger.info("Starting freshness windows sync from BigQuery...")

        # Initialize BigQuery client
        bq_client = BigQueryClient()

        # Fetch fresh videos (already bucketed by query)
        job_logger.info("Fetching fresh videos from BigQuery...")
        fresh_df = bq_client.fetch_fresh_videos()

        if fresh_df.empty:
            job_logger.warning("No fresh videos fetched from BigQuery")
            return

        job_logger.info(f"Fetched {len(fresh_df)} fresh videos")

        # Process each freshness window
        now = int(time.time())
        total_inserted = 0

        for window in FRESHNESS_WINDOWS:
            # Get videos for this window
            window_videos = fresh_df[fresh_df['bucket'] == window]['video_id'].tolist()

            if not window_videos:
                job_logger.info(f"No videos for window {window}")
                continue

            window_videos = await filter_videos_with_metadata(
                window_videos, kvrocks_service.client, job_logger
            )
            if not window_videos:
                job_logger.info(f"No videos with metadata for window {window}")
                continue

            # Clear existing window
            window_key = redis_layer._key_global_fresh_zset(window)
            await kvrocks_service.client.delete(window_key)

            # All freshness videos use same TTL
            expiry = now + TTL_VIDEOS_TO_SHOW

            # Insert videos in batches
            for batch in _batch_list(window_videos, BQ_PIPELINE_SIZE):
                video_dict = {vid: float(expiry) for vid in batch}
                await kvrocks_service.client.zadd(window_key, video_dict)

            total_inserted += len(window_videos)
            job_logger.info(f"Inserted {len(window_videos)} videos into {window}")

        elapsed = time.time() - start_time
        job_logger.info(f"Freshness sync completed: {total_inserted} videos in {elapsed:.2f}s")

    except Exception as e:
        job_logger.error(f"Error in freshness sync: {e}", exc_info=True)
        raise

    finally:
        # Always release lock
        await release_job_lock(kvrocks_service.client, job_name)


# ============================================================================
# SYNC JOB 3: USER BLOOM FILTERS
# ============================================================================

async def sync_user_bloom_filters(
    redis_layer: AsyncRedisLayer,
    kvrocks_service: AsyncKVRocksService
) -> None:
    """
    Sync user watch history from BigQuery to Redis bloom filters.

    Args:
        redis_layer: AsyncRedisLayer instance
        kvrocks_service: AsyncKVRocksService instance

    Algorithm:
        1. Acquire distributed lock
        2. Fetch user watch history from last 12 hours
        3. Group by user_id
        4. For each user:
           - Ensure bloom filter exists
           - Add watched videos using BF.MADD
        5. Release lock
    """
    job_name = "bloom_sync"
    start_time = time.time()

    # Get job-specific logger
    job_logger = get_job_logger(job_name, kvrocks_service.client)

    # Try to acquire lock
    if not await acquire_job_lock(kvrocks_service.client, job_name):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return

    try:
        job_logger.info("Starting user bloom filter sync from BigQuery...")

        # Initialize BigQuery client
        bq_client = BigQueryClient()

        # Fetch user watch history from last 12 hours
        job_logger.info("Fetching user watch history from BigQuery...")
        history_df = bq_client.fetch_user_watch_history(hours_back=12)

        if history_df.empty:
            job_logger.warning("No user watch history fetched from BigQuery")
            return

        total_users = history_df['user_id'].nunique()
        total_interactions = len(history_df)
        job_logger.info(f"Fetched {total_interactions} interactions from {total_users} users")

        # Group by user
        user_groups = history_df.groupby('user_id')['video_id'].apply(list).to_dict()

        # Process users in batches
        users_processed = 0
        videos_added = 0

        for user_id, video_ids in user_groups.items():
            if not video_ids:
                continue

            # Ensure bloom filter exists for user
            bloom_key = redis_layer._key_bloom_permanent(user_id)
            try:
                # Check if bloom filter exists
                exists = await kvrocks_service.client.exists(bloom_key)
                if not exists:
                    continue
                    # Create bloom filter with EXPANSION for auto-scaling
                    # await kvrocks_service.client.execute_command(
                    #     'BF.RESERVE', bloom_key, BLOOM_ERROR_RATE, BLOOM_INITIAL_CAPACITY,
                    #     'EXPANSION', BLOOM_EXPANSION
                    # )
                    # # Set initial TTL (auto-cleanup after 30 days of inactivity)
                    # await kvrocks_service.client.expire(bloom_key, BLOOM_TTL_DAYS * 86400)
                    # job_logger.debug(f"Created bloom filter for user {user_id} with expansion and {BLOOM_TTL_DAYS} day TTL")
                else:
                    # Refresh TTL for existing bloom (sliding expiry)
                    await kvrocks_service.client.expire(bloom_key, BLOOM_TTL_DAYS * 86400)
            except Exception as e:
                job_logger.warning(f"Error checking/creating bloom filter for {user_id}: {e}")
                continue

            # Add videos to bloom filter in batches
            for batch in _batch_list(video_ids, BQ_PIPELINE_SIZE):
                try:
                    results = await kvrocks_service.bf_madd(bloom_key, *batch)
                    added = sum(results)
                    videos_added += added
                    job_logger.debug(f"Added {added} new videos to bloom for {user_id}")
                except Exception as e:
                    job_logger.warning(f"Error adding videos to bloom for {user_id}: {e}")

            # Refresh TTL after adding videos (sliding expiry - keeps bloom alive for active users)
            if videos_added > 0:
                await kvrocks_service.client.expire(bloom_key, BLOOM_TTL_DAYS * 86400)

            users_processed += 1

            # Log progress periodically
            if users_processed % 100 == 0:
                job_logger.info(f"Processed {users_processed}/{total_users} users")

        elapsed = time.time() - start_time
        job_logger.info(
            f"Bloom sync completed: {users_processed} users, "
            f"{videos_added} videos added in {elapsed:.2f}s"
        )

    except Exception as e:
        job_logger.error(f"Error in bloom sync: {e}", exc_info=True)
        raise

    finally:
        # Always release lock
        await release_job_lock(kvrocks_service.client, job_name)


# ============================================================================
# SYNC JOB 4: USER FOLLOWING POOL (ON-DEMAND)
# ============================================================================

async def sync_user_following_pool(
    redis_layer: AsyncRedisLayer,
    kvrocks_service: AsyncKVRocksService,
    user_id: str
) -> Dict[str, int]:
    """
    Sync a single user's following pool from BigQuery (on-demand).

    This is an ON-DEMAND sync job triggered when:
    - Pool size < 10 videos AND
    - Last sync was >= 10 minutes ago

    This is NOT a periodic job for all users - it only runs for a specific user
    when their following pool needs refilling.

    Algorithm:
        1. Acquire distributed lock for user:{user_id}:following_sync
        2. Create BigQueryClient() inside (pattern compliance)
        3. Call bq_client.fetch_followed_users_content(user_id, num_videos)
        4. Filter through Lua (bloom + watched:short)
        5. Add to user:{user_id}:videos_to_show:following with TTL
        6. Update last_sync timestamp
        7. Release lock

    Args:
        redis_layer: AsyncRedisLayer instance for Redis operations
        kvrocks_service: AsyncKVRocksService instance for low-level Redis access
        user_id: User to sync following pool for

    Returns:
        Dict with stats: {"fetched": N, "added": M}
    """
    job_name = f"following_sync:{user_id}"
    start_time = time.time()

    # Get job-specific logger (pattern compliance)
    job_logger = get_job_logger(job_name, kvrocks_service.client)

    # Try to acquire lock (pattern compliance) - shorter TTL since this is per-user
    if not await acquire_job_lock(kvrocks_service.client, job_name, ttl=60):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return {"fetched": 0, "added": 0}

    try:
        job_logger.info(f"Starting following pool sync for user {user_id}...")

        # Initialize BigQuery client INSIDE (pattern compliance)
        bq_client = BigQueryClient()

        # Fetch followed users' content
        df = bq_client.fetch_followed_users_content(user_id, num_videos=1000)

        if df.empty:
            job_logger.info(f"No followed content for user {user_id}")
            return {"fetched": 0, "added": 0}

        job_logger.info(f"Fetched {len(df)} videos from followed users for {user_id}")

        # Filter and add via Redis layer (uses Lua for bloom + watched:short filtering)
        video_ids = df['video_id'].tolist()
        added = await redis_layer.refill_following(user_id, video_ids)

        elapsed = time.time() - start_time
        job_logger.info(
            f"Following sync for {user_id} completed: "
            f"fetched={len(df)}, added={added} in {elapsed:.2f}s"
        )

        return {"fetched": len(df), "added": added}

    except Exception as e:
        job_logger.error(f"Error in following sync for {user_id}: {e}", exc_info=True)
        raise

    finally:
        # Always release lock (pattern compliance)
        await release_job_lock(kvrocks_service.client, job_name)


# ============================================================================
# SYNC JOB 5: UGC (USER-GENERATED CONTENT) POOL
# ============================================================================

async def sync_ugc_pool(
    redis_layer: AsyncRedisLayer,
    kvrocks_service: AsyncKVRocksService
) -> None:
    """
    Sync UGC videos from BigQuery to Redis global UGC pool.

    This job runs every 6 hours and populates the global UGC pool with
    fresh user-generated content ordered by creation timestamp.

    Algorithm:
        1. Acquire distributed lock for 'ugc_sync' job
        2. Fetch UGC videos from BigQuery (ordered by freshness)
           - Sources: ai_ugc + ugc_content_approval (is_approved = TRUE)
           - Excludes rejected videos (is_approved = FALSE)
        3. Clear existing Redis pool (user:GLOBAL:pool:ugc)
        4. Batch insert videos with expiry timestamp as score
        5. Release lock

    Args:
        redis_layer: AsyncRedisLayer instance for key generation
        kvrocks_service: AsyncKVRocksService instance for Redis operations

    Returns:
        None
    """
    job_name = "ugc_sync"
    start_time = time.time()

    # Get job-specific logger
    job_logger = get_job_logger(job_name, kvrocks_service.client)

    # Try to acquire lock
    if not await acquire_job_lock(kvrocks_service.client, job_name):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return

    try:
        job_logger.info("Starting UGC pool sync from BigQuery...")

        # Initialize BigQuery client
        bq_client = BigQueryClient()

        # Fetch UGC videos (ordered by freshness)
        job_logger.info("Fetching UGC videos from BigQuery...")
        ugc_df = bq_client.fetch_ugc_videos(limit=UGC_POOL_CAPACITY)

        if ugc_df.empty:
            job_logger.warning("No UGC videos fetched from BigQuery")
            return

        job_logger.info(f"Fetched {len(ugc_df)} UGC videos")

        video_ids = ugc_df['video_id'].tolist()
        video_ids = await filter_videos_with_metadata(
            video_ids, kvrocks_service.client, job_logger
        )
        if not video_ids:
            job_logger.warning("No UGC videos with metadata in KVRocks")
            return

        # Calculate expiry for TTL management
        now = int(time.time())
        expiry = now + TTL_UGC_VIDEOS

        # Clear existing UGC pool
        ugc_key = redis_layer._key_global_ugc_zset()
        await kvrocks_service.client.delete(ugc_key)

        # Insert videos with expiry as score
        total_inserted = 0

        for batch in _batch_list(video_ids, BQ_PIPELINE_SIZE):
            video_dict = {vid: float(expiry) for vid in batch}
            await kvrocks_service.client.zadd(ugc_key, video_dict)
            total_inserted += len(batch)

        elapsed = time.time() - start_time
        job_logger.info(
            f"UGC sync completed: {total_inserted} videos in {elapsed:.2f}s "
            f"(TTL: {TTL_UGC_VIDEOS}s)"
        )

    except Exception as e:
        job_logger.error(f"Error in UGC sync: {e}", exc_info=True)
        raise

    finally:
        # Always release lock
        await release_job_lock(kvrocks_service.client, job_name)


# ============================================================================
# SYNC JOB 6: REPORTED NSFW EXCLUDE SET
# ============================================================================

async def sync_reported_nsfw_exclude_set(
    redis_layer: AsyncRedisLayer,
    kvrocks_service: AsyncKVRocksService
) -> None:
    """
    Sync reported + NSFW videos from DAG-maintained BigQuery table to Redis.

    Uses FULL REFRESH pattern (DAG handles incremental logic):
    1. Query BigQuery for all video_ids in reported_nsfw_videos table
    2. Atomic swap: write to temp key, then RENAME to final key
    3. This ensures the Redis SET always matches the BQ table

    Algorithm:
        1. Acquire distributed lock for 'exclude_sync' job
        2. Query BigQuery for all excluded video_ids
        3. Write to temp key using SADD in batches
        4. RENAME temp key to final key (atomic swap)
        5. Release lock

    Args:
        redis_layer: AsyncRedisLayer instance (for pattern compliance, not used directly)
        kvrocks_service: AsyncKVRocksService instance for Redis operations
    """
    job_name = "exclude_sync"
    start_time = time.time()

    job_logger = get_job_logger(job_name, kvrocks_service.client)

    if not await acquire_job_lock(kvrocks_service.client, job_name, ttl=300):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return

    try:
        job_logger.info("Starting exclude set sync from BigQuery...")

        # Initialize BigQuery client
        bq_client = BigQueryClient()

        # Fetch all excluded video_ids from DAG-maintained table
        df = bq_client.fetch_reported_nsfw_videos()

        exclude_key = EXCLUDE_SET_KEY
        temp_key = EXCLUDE_SET_TEMP_KEY

        if df.empty:
            job_logger.info("No reported + NSFW videos to exclude, clearing set")
            await kvrocks_service.client.delete(exclude_key)
            return

        video_ids = df['video_id'].tolist()
        job_logger.info(f"Found {len(video_ids)} videos to exclude")

        # Delete temp key if exists (from failed previous run)
        await kvrocks_service.client.delete(temp_key)

        # SADD to temp key in batches
        for batch in _batch_list(video_ids, BQ_PIPELINE_SIZE):
            await kvrocks_service.client.sadd(temp_key, *batch)

        # Atomic swap: RENAME temp to final
        await kvrocks_service.client.rename(temp_key, exclude_key)

        elapsed = time.time() - start_time
        job_logger.info(
            f"Exclude sync completed: {len(video_ids)} videos in {elapsed:.2f}s"
        )

    except Exception as e:
        job_logger.error(f"Error in exclude sync: {e}", exc_info=True)
        # Cleanup temp key on failure
        try:
            await kvrocks_service.client.delete(temp_key)
        except Exception:
            pass
        raise

    finally:
        await release_job_lock(kvrocks_service.client, job_name)


# ============================================================================
# SYNC JOB 7: GOOGLE CHAT METRICS WEBHOOK
# ============================================================================

async def send_metrics_to_gchat(
    kvrocks_service: AsyncKVRocksService,
    webhook_url: str
) -> None:
    """
    Send 24-hour metrics summary to Google Chat webhook.

    This job runs at 9 AM, 3 PM, 9 PM IST (production only).
    Uses distributed locking to ensure only one worker sends the message.

    Algorithm:
        1. Acquire distributed lock for 'gchat_metrics' job
        2. Get 24h metrics summary from hourly_metrics store
        3. Format as simple text message for Google Chat
        4. POST to webhook URL with retries (3 attempts, exponential backoff)
        5. Release lock

    Args:
        kvrocks_service: AsyncKVRocksService instance for locking
        webhook_url: Google Chat webhook URL

    Returns:
        None
    """
    job_name = "gchat_metrics"
    start_time = time.time()

    # Get job-specific logger
    job_logger = get_job_logger(job_name, kvrocks_service.client)

    # Try to acquire lock (short TTL since this is a quick job)
    if not await acquire_job_lock(kvrocks_service.client, job_name, ttl=300):
        job_logger.info(f"Skipping {job_name} - another worker is handling it")
        return

    try:
        job_logger.info("Starting Google Chat metrics report...")

        # Get 24h metrics summary
        summary = hourly_metrics.get_summary()

        if not summary.get("endpoints"):
            job_logger.warning("No metrics data available to send")
            return

        # Format message
        message = format_gchat_message(summary)

        # Send to webhook with retries
        async with aiohttp.ClientSession() as session:
            for attempt in range(3):
                try:
                    async with session.post(
                        webhook_url,
                        json={"text": message},
                        timeout=aiohttp.ClientTimeout(total=10)
                    ) as resp:
                        if resp.status == 200:
                            job_logger.info(f"Metrics sent to Google Chat (status={resp.status})")
                            break
                        else:
                            body = await resp.text()
                            job_logger.warning(
                                f"Webhook returned {resp.status}: {body[:200]}, retrying..."
                            )
                except aiohttp.ClientError as e:
                    job_logger.warning(f"Attempt {attempt + 1} failed: {e}")

                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s
                else:
                    job_logger.error("All 3 attempts to send webhook failed")

        elapsed = time.time() - start_time
        job_logger.info(f"Google Chat metrics job completed in {elapsed:.2f}s")

    except Exception as e:
        job_logger.error(f"Error sending metrics to Google Chat: {e}", exc_info=True)
        raise

    finally:
        await release_job_lock(kvrocks_service.client, job_name)


def format_gchat_message(summary: dict) -> str:
    """
    Format metrics summary as plain text for Google Chat.

    Args:
        summary: Dict from hourly_metrics.get_summary() with structure:
            {
                "window": "24h",
                "hours_with_data": int,
                "endpoints": {
                    "GET /endpoint": {
                        "total_requests": int,
                        "errors_4xx": int,
                        "errors_5xx": int,
                        "error_rate_pct": float,
                        "avg_latency_ms": float,
                        "p50_ms": float,
                        "p90_ms": float,
                        "p99_ms": float
                    }
                }
            }

    Returns:
        Formatted string for Google Chat message
    """
    env = os.getenv("ENV", "production")
    lines = [
        f"Feed API Metrics Summary ({summary.get('window', '24h')})",
        f"Environment: {env}",
        f"Hours with data: {summary.get('hours_with_data', 0)}",
        ""
    ]

    for endpoint, stats in summary.get("endpoints", {}).items():
        total = stats.get("total_requests", 0)
        err_4xx = stats.get("errors_4xx", 0)
        err_5xx = stats.get("errors_5xx", 0)
        err_pct = stats.get("error_rate_pct", 0.0)
        p50 = stats.get("p50_ms", 0)
        p90 = stats.get("p90_ms", 0)
        p99 = stats.get("p99_ms", 0)

        lines.append(endpoint)
        lines.append(
            f"  Requests (sampled, 1 worker): {total:,} | "
            f"Errors: {err_pct:.1f}% (4xx: {err_4xx}, 5xx: {err_5xx})"
        )
        lines.append(f"  Latency: p50={p50:.0f}ms | p90={p90:.0f}ms | p99={p99:.0f}ms")
        lines.append("")

    return "\n".join(lines)


async def send_error_alert_to_gchat(
    webhook_url: str,
    endpoint: str,
    status_code: int,
    user_id: str = None
) -> None:
    """
    Send real-time error alert to Google Chat (fire-and-forget).

    This is called from the middleware when a non-200 response occurs.
    No retry logic - if it fails, we just log and move on.

    Algorithm:
        1. Format error message with endpoint, status, user_id, timestamp
        2. Include @mention for immediate attention
        3. POST to webhook (single attempt, no retry)

    Args:
        webhook_url: Google Chat webhook URL
        endpoint: Normalized endpoint path (e.g., "GET /recommend/{user_id}")
        status_code: HTTP status code (4xx or 5xx)
        user_id: User ID from request path if available

    Returns:
        None
    """
    try:
        env = os.getenv("ENV", "production")
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")

        # Format message with mention
        lines = [
            f"ERROR ALERT {GCHAT_ALERT_MENTION}",
            "",
            f"Endpoint: {endpoint}",
            f"Status: {status_code}",
        ]

        if user_id:
            lines.append(f"User ID: {user_id}")

        lines.extend([
            f"Time: {timestamp}",
            f"Environment: {env}",
        ])

        message = "\n".join(lines)

        # Fire-and-forget POST (single attempt)
        async with aiohttp.ClientSession() as session:
            async with session.post(
                webhook_url,
                json={"text": message},
                timeout=aiohttp.ClientTimeout(total=5)
            ) as resp:
                if resp.status != 200:
                    logger.warning(f"Error alert webhook returned {resp.status}")

    except Exception as e:
        # Don't raise - this is fire-and-forget
        logger.warning(f"Failed to send error alert: {e}")


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def _batch_list(items: List, batch_size: int):
    """
    Split a list into batches.

    Args:
        items: List to split
        batch_size: Size of each batch

    Yields:
        Batches of items

    Algorithm:
        1. Iterate through list in steps of batch_size
        2. Yield slices of the list
    """
    for i in range(0, len(items), batch_size):
        yield items[i:i + batch_size]


async def filter_videos_with_metadata(
    video_ids: List[str],
    kvrocks_client,
    job_logger
) -> List[str]:
    if not video_ids:
        return []

    pipe = kvrocks_client.pipeline()
    for vid in video_ids:
        pipe.hgetall(f"offchain:metadata:video_details:{vid}")
    results = await pipe.execute()

    # Filter to videos with problematic metadata TODO: Raise this up with the backend team
    valid_ids = [vid for vid, data in zip(video_ids, results) if data]

    filtered_count = len(video_ids) - len(valid_ids)
    if filtered_count > 0:
        job_logger.debug(
            f"Filtered {filtered_count}/{len(video_ids)} videos without metadata"
        )

    return valid_ids


def _create_expiry_scores(
    video_ids: List[str],
    ttl: int,
    base_time: Optional[int] = None
) -> Dict[str, float]:
    """
    Create a dict of video_id -> expiry timestamp for Redis ZADD.

    Args:
        video_ids: List of video IDs
        ttl: Time to live in seconds
        base_time: Base timestamp (defaults to current time)

    Returns:
        Dict mapping video_id to expiry timestamp as float

    Algorithm:
        1. Get current time if not provided
        2. Calculate expiry as base_time + ttl
        3. Create dict with all videos having same expiry
    """
    if base_time is None:
        base_time = int(time.time())

    expiry = float(base_time + ttl)
    return {vid: expiry for vid in video_ids}


# ============================================================================
# MAIN ENTRY POINT FOR TESTING
# ============================================================================

if __name__ == "__main__":
    import asyncio
    from utils.async_redis_utils import AsyncKVRocksService
    from async_main import AsyncRedisLayer

    logging.basicConfig(level=logging.INFO)

    async def test_jobs():
        """Test the background jobs with mock data."""
        # Initialize Redis connection
        kvrocks_service = AsyncKVRocksService(
            host=os.getenv("KVROCKS_HOST", "localhost"),
            port=int(os.getenv("KVROCKS_PORT", "6379")),
            password=os.getenv("KVROCKS_PASSWORD")
        )
        await kvrocks_service.connect()

        redis_layer = AsyncRedisLayer(kvrocks_service)
        await redis_layer.initialize()

        # Test lock acquisition
        logger.info("\nTesting lock acquisition...")
        lock1 = await acquire_job_lock(kvrocks_service.client, "test_job", ttl=10)
        logger.info(f"First lock attempt: {lock1}")

        lock2 = await acquire_job_lock(kvrocks_service.client, "test_job", ttl=10)
        logger.info(f"Second lock attempt (should fail): {lock2}")

        # Release lock
        released = await release_job_lock(kvrocks_service.client, "test_job")
        logger.info(f"Lock released: {released}")

        # Test sync jobs (will use real BigQuery if SERVICE_CRED is set)
        if os.getenv("SERVICE_CRED"):
            logger.info("\nTesting sync jobs with BigQuery...")

            # Test popularity sync
            logger.info("\nTesting popularity sync...")
            await sync_global_popularity_pools(redis_layer, kvrocks_service)

            # Test freshness sync
            logger.info("\nTesting freshness sync...")
            await sync_freshness_windows(redis_layer, kvrocks_service)

            # Test bloom sync
            logger.info("\nTesting bloom sync...")
            await sync_user_bloom_filters(redis_layer, kvrocks_service)

            # Test UGC sync
            logger.info("\nTesting UGC sync...")
            await sync_ugc_pool(redis_layer, kvrocks_service)

            # Test following sync (on-demand, per-user)
            logger.info("\nTesting following sync (on-demand)...")
            test_user_id = "test_user_123"  # Replace with real user_id for testing
            try:
                stats = await sync_user_following_pool(redis_layer, kvrocks_service, test_user_id)
                logger.info(f"Following sync result: {stats}")
            except Exception as e:
                logger.error(f"Following sync failed: {e}")
        else:
            logger.warning("SERVICE_CRED not set - skipping BigQuery tests")

        await kvrocks_service.close()

    # Run tests
    asyncio.run(test_jobs())