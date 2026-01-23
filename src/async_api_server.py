"""
API Endpoints:
- GET /recommend/{user_id}: Get personalized video recommendations
- POST /feedback/{user_id}: Mark videos as watched
- GET /health: Health check
- GET /metrics: Prometheus metrics
- GET /feed-stats/{user_id}: Get feed statistics for debugging
- GET /status: Comprehensive system status
- GET /debug/user/{user_id}: User state inspection
- GET /global-cache: Get videos from global cache (with in-memory caching)
"""

import os
import time
import logging
import asyncio
import random
import pandas as pd
from datetime import datetime
from typing import List, Dict, Optional, Any
from contextlib import asynccontextmanager
from dataclasses import dataclass, field

import sentry_sdk
from sentry_sdk.integrations.starlette import StarletteIntegration
from sentry_sdk.integrations.fastapi import FastApiIntegration

from utils.common_utils import filter_transient_errors

# Initialize Sentry before FastAPI app
sentry_sdk.init(
    dsn=os.getenv(
        "SENTRY_DSN",
        "",
    ),
    send_default_pii=True,
    traces_sample_rate=float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.01")),
    environment=os.getenv("ENV", "production"),
    # environment="stage",
    release=f"feed-server@{os.getenv('APP_VERSION', '2.0.0-async')}",
    auto_session_tracking=True,
    # session_sample_rate=float(os.getenv("SENTRY_SESSION_SAMPLE_RATE", "1.0")),
    integrations=[
        StarletteIntegration(),
        FastApiIntegration(),
    ],
    before_send=filter_transient_errors,
)

from fastapi import FastAPI, HTTPException, BackgroundTasks, Query, Request, Header, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse, JSONResponse
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException
from pydantic import BaseModel, Field
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_EXECUTED
from prometheus_client import Counter, Histogram, Gauge, REGISTRY, CollectorRegistry, generate_latest, CONTENT_TYPE_LATEST
from prometheus_client.multiprocess import MultiProcessCollector

from async_main import AsyncRedisLayer
from utils.async_redis_utils import AsyncKVRocksService
from utils.metrics_utils import hourly_metrics
from async_mixer import AsyncVideoMixer
from background_jobs import (
    sync_global_popularity_pools,
    sync_freshness_windows,
    sync_user_bloom_filters,
    sync_ugc_pool,
    send_metrics_to_gchat,
    send_error_alert_to_gchat,
    format_gchat_message,
    sync_reported_nsfw_exclude_set,
)
from job_logger import get_job_logs, clear_job_logs, get_job_log_stats
from metadata_handler import AsyncMetadataHandler
from config import (
    REDIS_CONFIG,
    CORS_ORIGINS,
    CORS_ALLOW_CREDENTIALS,
    CORS_ALLOW_METHODS,
    CORS_ALLOW_HEADERS,
    VIDEOS_PER_REQUEST,
    GLOBAL_REFRESH_INTERVAL,
    BLOOM_SYNC_INTERVAL,
    BIGQUERY_SYNC_INTERVAL,
    SYNC_JITTER_MIN,
    SYNC_JITTER_MAX,
    JOB_LOCK_KEY_PREFIX,
    REFILL_THRESHOLD,
    PERCENTILE_BUCKETS,
    FRESHNESS_WINDOWS,
    LOG_LEVEL,
    LOG_FORMAT,
    ENABLE_DEBUG_LOGGING,
    REDIS_SEMAPHORE_SIZE,
    GCHAT_WEBHOOK_URL,
    GCHAT_METRICS_HOURS_UTC,
    GCHAT_METRICS_MINUTE_UTC,
    GCHAT_ERROR_ALERT_LIMIT,
    GCHAT_ERROR_ALERT_WINDOW,
    GCHAT_EXCLUDED_STATUS_CODES,
    ADMIN_API_KEY,
    TOURNAMENT_VIDEO_POOL_SIZE,
    STUBBED_CANISTER_ID,
    EXCLUDE_SYNC_INTERVAL,
)

# Configure logging
# LOG_LEVEL is automatically set to WARNING when ENABLE_DEBUG_LOGGING=False in config.py
# This disables debug/info logs for production and stress testing while keeping error/warning logs
logging.basicConfig(level=LOG_LEVEL, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

# Global instances (initialized in lifespan)
redis_layer: Optional[AsyncRedisLayer] = None
video_mixer: Optional[AsyncVideoMixer] = None
scheduler: Optional[AsyncIOScheduler] = None
kvrocks_service: Optional[AsyncKVRocksService] = None
metadata_handler: Optional[AsyncMetadataHandler] = None


# ============================================================================
# Prometheus Metrics
# ============================================================================
# Use REGISTRY to avoid duplicate registration when module is re-imported
# (happens with viztracer, uvicorn workers, etc.)

from prometheus_client import REGISTRY

def _get_or_create_metric(metric_class, name, description, labelnames=None, **kwargs):
    """
    Returns existing metric if already registered, otherwise creates new one.
    Prevents 'Duplicated timeseries' error on module re-import (viztracer, uvicorn workers).

    Algorithm:
    1. Attempt to create the metric
    2. If ValueError (duplicate), find and return the existing collector
    3. For Counters, internal name is without '_total' suffix
    """
    try:
        if labelnames:
            return metric_class(name, description, labelnames, **kwargs)
        return metric_class(name, description, **kwargs)
    except ValueError:
        # Metric already registered - find and return it
        # Counter 'foo_total' is stored as 'foo' internally
        base_name = name.replace('_total', '') if name.endswith('_total') else name
        for collector in REGISTRY._names_to_collectors.values():
            if hasattr(collector, '_name') and collector._name in (base_name, name):
                return collector
        raise  # Re-raise if we can't find it

REQUEST_COUNT = _get_or_create_metric(
    Counter,
    'http_requests_total',
    'Total HTTP requests',
    ['method', 'endpoint', 'status_code']
)

REQUEST_LATENCY = _get_or_create_metric(
    Histogram,
    'http_request_duration_seconds',
    'HTTP request latency in seconds',
    ['method', 'endpoint'],
    buckets=[0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 15, 20]
)

ACTIVE_REQUESTS = _get_or_create_metric(
    Gauge,
    'http_requests_active',
    'Number of active HTTP requests'
)


# ============================================================================
# Global Cache for /global-cache endpoint
# ============================================================================


@dataclass
class GlobalCacheStore:
    """
    In-memory cache for global-cache endpoint.

    Stores 1000 pre-fetched videos (with and without metadata) for instant sampling.
    Refreshed every hour by background job. Users get random samples from this cache
    for near-instant responses instead of hitting Redis/BigQuery each time.

    Attributes:
        videos: List of 1000 video IDs (without metadata)
        videos_with_metadata: List of 1000 video dicts with canister_id, post_id, etc.
        sources: Breakdown of video sources (popularity buckets, freshness windows)
        timestamp: Unix timestamp of when cache was last refreshed
        ttl: Time-to-live in seconds (1 hour)
        lock: asyncio.Lock for thread-safe cache updates
    """

    videos: List[str] = field(default_factory=list)
    videos_with_metadata: List[Dict] = field(default_factory=list)
    sources: Dict[str, Any] = field(default_factory=dict)
    timestamp: float = 0.0
    ttl: int = 3600  # 1 hour TTL
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def is_valid(self) -> bool:
        """
        Check if cache is populated and not expired.

        Returns:
            True if cache has videos and is within TTL, False otherwise.
        """
        return self.timestamp > 0 and (time.time() - self.timestamp) < self.ttl

    def sample_videos(self, count: int) -> List[str]:
        """
        Randomly sample N videos from cached 1000.

        Args:
            count: Number of videos to sample

        Returns:
            List of randomly sampled video IDs
        """
        if count >= len(self.videos):
            return self.videos.copy()
        return random.sample(self.videos, count)

    def sample_videos_with_metadata(self, count: int) -> List[Dict]:
        """
        Randomly sample N videos with metadata from cached 1000.

        Args:
            count: Number of videos to sample

        Returns:
            List of randomly sampled video dicts with metadata
        """
        if count >= len(self.videos_with_metadata):
            return self.videos_with_metadata.copy()
        return random.sample(self.videos_with_metadata, count)


# Global cache instance
global_cache: GlobalCacheStore = GlobalCacheStore()

# Error alert rate limiter (moved to utils/rate_limiter.py)
from utils.rate_limiter import ErrorAlertRateLimiter
error_alert_limiter = ErrorAlertRateLimiter(
    limit=GCHAT_ERROR_ALERT_LIMIT,
    window=GCHAT_ERROR_ALERT_WINDOW
)


# ============================================================================
# Pydantic Models
# ============================================================================


class RecommendationRequest(BaseModel):
    """Request model for getting recommendations."""

    count: int = Field(default=VIDEOS_PER_REQUEST, ge=1, le=500)
    rec_type: Optional[str] = Field(
        default="mixed", description="Type: mixed, popularity, freshness"
    )
    exclude_watched: bool = Field(default=True, description="Exclude watched videos")


class RecommendationResponse(BaseModel):
    """Response model for recommendations."""

    user_id: str
    videos: List[str]
    count: int
    sources: Dict[str, int]
    timestamp: int


class VideoMetadata(BaseModel):
    """Video metadata with canister and post IDs."""

    video_id: str
    canister_id: str
    post_id: str
    publisher_user_id: str


class VideoMetadataV2(BaseModel):
    """Video metadata with canister, post IDs, and view counts (V2)."""

    video_id: str
    canister_id: str
    post_id: str
    publisher_user_id: str
    num_views_loggedin: int = Field(default=0, description="Number of logged-in views")
    num_views_all: int = Field(default=0, description="Total number of views")


class RecommendationWithMetadataResponse(BaseModel):
    """Response model for recommendations with metadata."""

    user_id: str
    videos: List[VideoMetadata]  # Enhanced with metadata
    count: int
    sources: Dict[str, int]
    timestamp: int


class RecommendationWithMetadataV2Response(BaseModel):
    """Response model for recommendations with metadata and view counts (V2)."""

    user_id: str
    videos: List[VideoMetadataV2]  # Enhanced with metadata and view counts
    count: int
    sources: Dict[str, int]
    timestamp: int


class GlobalCacheResponse(BaseModel):
    """Response model for global cache endpoint."""

    videos: List[str]
    count: int
    sources: Dict[
        str, Dict[str, int]
    ]  # {"popularity": {"99_100": 50, ...}, "freshness": {"l1d": 200, ...}}
    timestamp: int


class GlobalCacheWithMetadataResponse(BaseModel):
    """Response model for global cache endpoint with metadata."""

    videos: List[VideoMetadata]
    count: int
    sources: Dict[str, Dict[str, int]]
    timestamp: int


# class FeedbackRequest(BaseModel):
#     """Request model for marking videos as watched."""
#
#     video_ids: List[str] = Field(..., min_length=1, max_length=1000)
#     interaction_type: str = Field(
#         default="view", description="Type: view, like, share, comment"
#     )
#     mark_permanent: bool = Field(
#         default=False, description="Add to permanent bloom filter"
#     )
#
#
# class FeedbackResponse(BaseModel):
#     """Response model for feedback."""
#
#     success: bool
#     videos_marked: int
#     message: str


class HealthResponse(BaseModel):
    """Response model for health check."""

    status: str
    timestamp: int
    redis_connected: bool
    scheduler_running: bool
    uptime_seconds: float


# ============================================================================
# Tournament Models
# ============================================================================


class TournamentRegisterRequest(BaseModel):
    """Request model for registering a new tournament."""

    tournament_id: Optional[str] = Field(default=None, max_length=128, description="Unique tournament identifier. If not provided, server generates UUID.")
    video_count: int = Field(..., ge=1, le=500, description="Number of videos for this tournament")


class TournamentRegisterResponse(BaseModel):
    """Response model for tournament registration."""

    tournament_id: str
    video_count: int
    created_at: str


class TournamentVideosResponse(BaseModel):
    """Response model for tournament videos (without metadata)."""

    tournament_id: str
    video_count: int
    videos: List[str]


class TournamentVideosWithMetadataResponse(BaseModel):
    """Response model for tournament videos with metadata."""

    tournament_id: str
    video_count: int
    videos: List[VideoMetadata]


class TournamentErrorResponse(BaseModel):
    """Error response model for tournament endpoints."""

    error: str
    message: str
    available_count: Optional[int] = None
    requested_count: Optional[int] = None


class TournamentOverlapResponse(BaseModel):
    """Response model for tournament video overlap check."""

    tournament_a_id: str
    tournament_b_id: str
    tournament_a_video_count: int
    tournament_b_video_count: int
    overlap_count: int
    overlap_percentage_a: float = Field(
        description="Overlap as percentage of tournament A's videos"
    )
    overlap_percentage_b: float = Field(
        description="Overlap as percentage of tournament B's videos"
    )
    overlapping_video_ids: Optional[List[str]] = Field(
        default=None,
        description="List of overlapping video IDs (only if include_video_ids=True)"
    )


class TournamentListItem(BaseModel):
    """Single tournament in list response."""

    tournament_id: str
    status: str = Field(description="'active' or 'expired'")
    video_count: int
    created_at: str
    expires_at: str = Field(description="When tournament transitions from active to expired")


class TournamentListResponse(BaseModel):
    """Response for listing all tournaments."""

    count: int
    active_count: int
    expired_count: int
    tournaments: List[TournamentListItem]


class TournamentDeleteResponse(BaseModel):
    """Response for tournament deletion."""

    tournament_id: str
    deleted: bool
    status_before_deletion: str = Field(description="Was 'active' or 'expired' before deletion")
    videos_removed_from_cooldown: int
    message: str


# ============================================================================
# Background Jobs (ALL ASYNC)
# ============================================================================


async def refresh_global_pools():
    """
    This job runs every 6 hours with jitter to:
    1. Sync popularity percentile buckets from BigQuery
    2. Sync freshness windows from BigQuery
    3. Sync UGC pool from BigQuery

    Applies random jitter to prevent thundering herd when multiple workers
    try to acquire the lock simultaneously.
    """
    # Apply random jitter to prevent thundering herd
    jitter = random.randint(SYNC_JITTER_MIN, SYNC_JITTER_MAX)
    logger.info(f"Applying {jitter}s jitter before global pools refresh")
    await asyncio.sleep(jitter)

    start_time = time.time()

    try:
        # Check if BigQuery credentials are available
        if not os.getenv("SERVICE_CRED"):
            logger.warning("SERVICE_CRED not set - skipping BigQuery sync")
            return

        # Run all sync jobs
        logger.info("Starting global pools refresh from BigQuery...")

        # Sync popularity pools
        await sync_global_popularity_pools(redis_layer, kvrocks_service)

        # Sync freshness windows
        await sync_freshness_windows(redis_layer, kvrocks_service)

        # Sync UGC pool (user-generated content for 5% bias)
        await sync_ugc_pool(redis_layer, kvrocks_service)

        elapsed = time.time() - start_time
        logger.info(f"Global pools refresh completed in {elapsed:.2f}s")

    except Exception as e:
        logger.error(f"Error refreshing global pools: {e}", exc_info=True)
        # Don't raise - let scheduler retry at next interval


async def sync_bloom_filters():
    """

    This job runs every 6 hours with jitter to:
    1. Get user watch history from last 12 hours from BigQuery
    2. Update bloom filters with watched videos

    Applies random jitter to prevent thundering herd when multiple workers
    try to acquire the lock simultaneously.
    """
    # Apply random jitter to prevent thundering herd
    jitter = random.randint(SYNC_JITTER_MIN, SYNC_JITTER_MAX)
    logger.info(f"Applying {jitter}s jitter before bloom filter sync")
    await asyncio.sleep(jitter)

    start_time = time.time()

    try:
        # Check if BigQuery credentials are available
        if not os.getenv("SERVICE_CRED"):
            logger.warning("SERVICE_CRED not set - skipping BigQuery sync")
            return

        # Run bloom filter sync
        logger.info("Starting bloom filter sync from BigQuery...")
        await sync_user_bloom_filters(redis_layer, kvrocks_service)

        elapsed = time.time() - start_time
        logger.info(f"Bloom filter sync completed in {elapsed:.2f}s")

    except Exception as e:
        logger.error(f"Error syncing bloom filters: {e}", exc_info=True)
        # Don't raise - let scheduler retry at next interval


async def send_gchat_metrics_report():
    """
    Send metrics summary to Google Chat webhook.

    This job runs at 9 AM, 3 PM, 9 PM IST (production only).
    Wrapper function that calls the actual job with the webhook URL.
    """
    if not GCHAT_WEBHOOK_URL:
        # No webhook configured - skip silently (expected for stage)
        return

    try:
        await send_metrics_to_gchat(kvrocks_service, GCHAT_WEBHOOK_URL)
    except Exception as e:
        logger.error(f"Error sending metrics to Google Chat: {e}", exc_info=True)


async def refresh_global_cache():
    """
    Background job to refresh global cache with 1000 videos.

    This job runs every hour to pre-fetch 1000 videos (with metadata) into memory.
    The /global-cache endpoint then samples from this cache for near-instant responses.

    Algorithm:
        1. Check if redis_layer is initialized
        2. Fetch 1000 videos from global pools (Redis ZSET reads)
        3. Convert all 1000 to metadata format (BigQuery + pipelined Redis mappings)
        4. Update cache atomically under lock
        5. Log timing and success/failure
    """
    global global_cache, redis_layer

    if redis_layer is None:
        logger.warning("Redis layer not initialized, skipping global cache refresh")
        return

    try:
        logger.info("Refreshing global cache with 1000 videos...")
        start_time = time.time()

        # Fetch 1000 videos from global pools (50% popularity, 50% freshness)
        result = await redis_layer.fetch_from_global_pools_raw(1000)
        videos = result["videos"]
        sources = result["sources"]

        if not videos:
            logger.warning("No videos fetched from global pools for cache")
            return

        # Convert to metadata format (async, reuses shared KVRocks client)
        videos_with_metadata = await metadata_handler.batch_convert_video_ids(videos)

        # Update cache atomically under lock
        async with global_cache.lock:
            global_cache.videos = videos
            global_cache.videos_with_metadata = videos_with_metadata
            global_cache.sources = sources
            global_cache.timestamp = time.time()

        elapsed = time.time() - start_time
        logger.info(
            f"Global cache refreshed: {len(videos)} videos, "
            f"{len(videos_with_metadata)} with metadata, took {elapsed:.2f}s"
        )

    except Exception as e:
        logger.error(f"Failed to refresh global cache: {e}", exc_info=True)
        # Don't raise - let scheduler retry at next interval


async def sync_exclude_set():
    """
    Sync reported + NSFW videos exclude set from BigQuery to Redis.

    This job runs every 5 minutes to keep the exclude set in sync with the
    DAG-maintained BigQuery table (ds__reported_nsfw_videos).

    Applies random jitter to prevent thundering herd when multiple workers
    try to acquire the lock simultaneously.
    """
    jitter = random.randint(0, 30)
    await asyncio.sleep(jitter)

    if not os.getenv("SERVICE_CRED"):
        logger.warning("SERVICE_CRED not set - skipping exclude set sync")
        return

    try:
        await sync_reported_nsfw_exclude_set(redis_layer, kvrocks_service)
    except Exception as e:
        logger.error(f"Exclude set sync failed: {e}", exc_info=True)


def job_listener(event):
    """Listen to job events for monitoring."""
    if event.exception:
        logger.error(f"Job {event.job_id} crashed: {event.exception}")
    else:
        logger.debug(f"Job {event.job_id} executed successfully")


# ============================================================================
# FastAPI App with Async Lifespan
# ============================================================================


@asynccontextmanager
async def lifespan(app: FastAPI):
    """

    Startup:
    - Initialize async Redis connection with connection pooling
    - Initialize async mixer (production mode, 60/40 split)
    - Start background scheduler
    - Prepare for external data integration

    Shutdown:
    - Stop scheduler
    - Close Redis connection pool
    - Clean up resources
    """
    global redis_layer, video_mixer, scheduler, kvrocks_service, metadata_handler

    # Startup
    logger.info("Starting async recommendation system (production mode)...")

    # Initialize async Redis with connection pooling
    try:
        kvrocks_service = AsyncKVRocksService(**REDIS_CONFIG)
        await asyncio.wait_for(kvrocks_service.connect(), timeout=30.0)
        logger.info("Async Redis connection pool established")
    except asyncio.TimeoutError:
        logger.error(
            "KVRocks connection timeout after 30s - check KVROCKS_HOST and network"
        )
        raise
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    # Initialize async metadata handler (reuses kvrocks_service.client for KVRocks lookups)
    metadata_handler = AsyncMetadataHandler(kvrocks_service.client)
    logger.info("Async metadata handler initialized")

    # Initialize async Redis layer
    redis_layer = AsyncRedisLayer(kvrocks_service)
    await redis_layer.initialize()
    logger.info("Async Redis layer initialized")

    # Initialize async mixer with kvrocks_service for following sync
    video_mixer = AsyncVideoMixer(redis_layer, kvrocks_service=kvrocks_service)
    logger.info("Async video mixer initialized")

    # Initialize scheduler
    scheduler = AsyncIOScheduler()

    # Add background jobs with BigQuery sync interval (6 hours)
    scheduler.add_job(
        refresh_global_pools,
        "interval",
        seconds=BIGQUERY_SYNC_INTERVAL,  # 6 hours
        id="refresh_global_pools",
        replace_existing=True,
        max_instances=1,  # Prevent concurrent execution
        misfire_grace_time=60,  # Allow 60s grace period for misfires
    )

    scheduler.add_job(
        sync_bloom_filters,
        "interval",
        seconds=BIGQUERY_SYNC_INTERVAL,  # 6 hours
        id="sync_bloom_filters",
        replace_existing=True,
        max_instances=1,  # Prevent concurrent execution
        misfire_grace_time=60,  # Allow 60s grace period for misfires
    )

    # Add global cache refresh job (every hour)
    # Pre-fetches 1000 videos with metadata for instant /global-cache responses
    scheduler.add_job(
        refresh_global_cache,
        "interval",
        seconds=3600,  # 1 hour
        id="refresh_global_cache",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    # Add exclude set sync job (every 5 minutes)
    # Syncs reported + NSFW videos from BigQuery to Redis for filtering
    scheduler.add_job(
        sync_exclude_set,
        "interval",
        seconds=EXCLUDE_SYNC_INTERVAL,  # 5 minutes
        id="sync_exclude_set",
        replace_existing=True,
        max_instances=1,
        misfire_grace_time=60,
    )

    # Add Google Chat metrics job (production only)
    # Sends metrics summary at 9 AM, 3 PM, 9 PM IST (3:30, 9:30, 15:30 UTC)
    if GCHAT_WEBHOOK_URL:
        scheduler.add_job(
            send_gchat_metrics_report,
            "cron",
            hour=",".join(str(h) for h in GCHAT_METRICS_HOURS_UTC),
            minute=GCHAT_METRICS_MINUTE_UTC,
            id="send_gchat_metrics",
            replace_existing=True,
            max_instances=1,
            misfire_grace_time=300,
        )
        logger.info(
            f"Google Chat metrics job scheduled at "
            f"{GCHAT_METRICS_HOURS_UTC}:{GCHAT_METRICS_MINUTE_UTC:02d} UTC"
        )
    else:
        logger.info("GCHAT_WEBHOOK_URL not set - metrics alerts disabled")

    # Note: User feed refills happen automatically via fetch_videos() auto-refill
    # No scheduled job needed for this

    # Add event listeners
    scheduler.add_listener(job_listener, EVENT_JOB_ERROR | EVENT_JOB_EXECUTED)

    # Start scheduler
    scheduler.start()
    logger.info("Background scheduler started with async jobs")

    # Run initial sync if BigQuery credentials are available
    if os.getenv("SERVICE_CRED"):
        logger.info("Running initial sync from BigQuery...")
        asyncio.create_task(refresh_global_pools())
        asyncio.create_task(sync_bloom_filters())
        # Populate global cache on startup for instant /global-cache responses
        logger.info("Populating global cache on startup...")
        asyncio.create_task(refresh_global_cache())
        # Sync exclude set (reported + NSFW videos) on startup
        logger.info("Syncing exclude set on startup...")
        asyncio.create_task(sync_exclude_set())
    else:
        logger.warning("SERVICE_CRED not set - BigQuery sync disabled")

    # Store start time for uptime calculation
    app.state.start_time = time.time()

    yield

    # Shutdown
    logger.info("Shutting down async recommendation system...")

    if scheduler:
        scheduler.shutdown(wait=True)
        logger.info("Background scheduler stopped")

    if kvrocks_service:
        await kvrocks_service.close()
        logger.info("Redis connection pool closed")


# Create FastAPI app
app = FastAPI(
    title="Feed API",
    description="Video Recommendation API",
    version="2.0.0",
    lifespan=lifespan,
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=CORS_ALLOW_CREDENTIALS,
    allow_methods=CORS_ALLOW_METHODS,
    allow_headers=CORS_ALLOW_HEADERS,
)


# ============================================================================
# Auth Dependencies
# ============================================================================


async def verify_admin_key(x_admin_key: str = Header(..., description="Admin API key")):
    """
    Dependency to verify admin API key for protected endpoints.

    Algorithm:
        1. Check if ADMIN_API_KEY is configured
        2. Compare provided key with configured key
        3. Raise 401 if invalid or missing

    Args:
        x_admin_key: API key from X-Admin-Key header

    Raises:
        HTTPException 401 if key is invalid or ADMIN_API_KEY not configured
    """
    if not ADMIN_API_KEY:
        raise HTTPException(
            status_code=500,
            detail={"error": "server_misconfigured", "message": "ADMIN_API_KEY not configured"}
        )
    if x_admin_key != ADMIN_API_KEY:
        raise HTTPException(
            status_code=401,
            detail={"error": "unauthorized", "message": "Invalid or missing admin key"}
        )
    return True


# ============================================================================
# API Endpoints (ALL ASYNC)
# ============================================================================


@app.get("/recommend/{user_id}", response_model=RecommendationResponse)
async def get_recommendations(
    user_id: str,
    count: int = Query(default=VIDEOS_PER_REQUEST, ge=1, le=500),
    rec_type: str = Query(
        default="mixed", description="Type: mixed, popularity, freshness"
    ),
):
    """
    ASYNC - Get personalized video recommendations for a user.

    This endpoint returns a curated selection of video recommendations
    tailored to the user's preferences and viewing history.
    """
    try:
        # Attach user context to Sentry for Release Health tracking
        sentry_sdk.set_user({"id": str(user_id)})
        # Check if user exists, if not bootstrap
        if not await redis_layer.db.exists(redis_layer._key_bloom_permanent(user_id)):
            logger.info(f"Bootstrapping new user: {user_id}")
            await redis_layer.bootstrap_fresh_user(user_id)

        # Get recommendations based on type
        if rec_type == "mixed":
            # Use mixer for blended recommendations (60/40 split)
            result = await video_mixer.get_mixed_recommendations(user_id, count)
            videos = result.videos
            sources = result.sources
        else:
            # Get from specific feed type (auto-consumes and auto-refills)
            videos = await redis_layer.fetch_videos(user_id, rec_type, count)
            sources = {rec_type: len(videos)}

        return RecommendationResponse(
            user_id=user_id,
            videos=videos,
            count=len(videos),
            sources=sources,
            timestamp=int(time.time()),
        )

    except Exception as e:
        logger.error(f"Error getting recommendations for {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/recommend-with-metadata/{user_id}",
    response_model=RecommendationWithMetadataResponse,
)
async def get_recommendations_with_metadata(
    user_id: str,
    count: int = Query(default=100, ge=1, le=500),
    rec_type: str = Query(
        default="mixed", description="Type: mixed, popularity, freshness"
    ),
):
    """
    ASYNC - Get personalized video recommendations with canister and post IDs.

    This endpoint returns video recommendations enriched with metadata
    including canister_id and post_id for each video.
    """
    try:
        # Attach user context to Sentry for Release Health tracking
        sentry_sdk.set_user({"id": str(user_id)})
        # Check if user exists, if not bootstrap
        if not await redis_layer.db.exists(redis_layer._key_bloom_permanent(user_id)):
            logger.info(f"Bootstrapping new user: {user_id}")
            await redis_layer.bootstrap_fresh_user(user_id)

        # Get recommendations based on type
        if rec_type == "mixed":
            # Use mixer for blended recommendations (60/40 split)
            result = await video_mixer.get_mixed_recommendations(user_id, count)
            videos = result.videos
            sources = result.sources
        else:
            # Get from specific feed type (auto-consumes and auto-refills)
            videos = await redis_layer.fetch_videos(user_id, rec_type, count)
            sources = {rec_type: len(videos)}

        # Convert video IDs to metadata (async, reuses shared KVRocks client)
        video_metadata = await metadata_handler.batch_convert_video_ids(videos)

        return RecommendationWithMetadataResponse(
            user_id=user_id,
            videos=video_metadata,
            count=len(video_metadata),
            sources=sources,
            timestamp=int(time.time()),
        )

    except Exception as e:
        logger.error(f"Error getting recommendations with metadata for {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get(
    "/v2/recommend-with-metadata/{user_id}",
    response_model=RecommendationWithMetadataV2Response,
)
async def get_recommendations_with_metadata_v2(
    user_id: str,
    count: int = Query(default=100, ge=1, le=500),
    rec_type: str = Query(
        default="mixed", description="Type: mixed, popularity, freshness"
    ),
):
    """
    ASYNC - Get personalized video recommendations with metadata and view counts (V2).

    This endpoint returns video recommendations enriched with:
    - canister_id and post_id for each video
    - num_views_loggedin: Number of logged-in user views
    - num_views_all: Total number of views

    This is the V2 version that includes view counts from redis-impressions.
    """
    try:
        # Attach user context to Sentry for Release Health tracking
        sentry_sdk.set_user({"id": str(user_id)})
        if not await redis_layer.db.exists(redis_layer._key_bloom_permanent(user_id)):
            logger.info(f"Bootstrapping new user: {user_id}")
            await redis_layer.bootstrap_fresh_user(user_id)

        if rec_type == "mixed":
            result = await video_mixer.get_mixed_recommendations(user_id, count)
            videos = result.videos
            sources = result.sources
        else:
            videos = await redis_layer.fetch_videos(user_id, rec_type, count)
            sources = {rec_type: len(videos)}

        video_metadata = await metadata_handler.batch_convert_video_ids_v2(videos)

        return RecommendationWithMetadataV2Response(
            user_id=user_id,
            videos=video_metadata,
            count=len(video_metadata),
            sources=sources,
            timestamp=int(time.time()),
        )

    except Exception as e:
        logger.error(
            f"Error getting recommendations with metadata v2 for {user_id}: {e}"
        )
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/global-cache")
async def get_global_cache(
    count: int = Query(default=1000, ge=1, le=10000),
    include_metadata: bool = Query(
        default=False, description="Include canister_id, post_id, publisher_user_id"
    ),
):
    """
    Get videos from global pools with in-memory caching.

    Returns randomly sampled videos from a pre-cached pool of 1000 videos.
    Cache is refreshed every hour by background job, providing near-instant responses.

    Args:
        count: Number of videos to fetch (max 10000, samples from 1000 cached)
        include_metadata: If True, include canister_id, post_id, publisher_user_id

    Returns:
        GlobalCacheResponse or GlobalCacheWithMetadataResponse

    Algorithm:
        1. Check if in-memory cache is valid (populated and < 1 hour old)
        2. If valid: randomly sample N videos from cached 1000
        3. If invalid/cold: fetch fresh from Redis (fallback for cold start)
        4. Return response with videos, count, sources, timestamp
    """
    global global_cache

    try:
        # Check if cache is valid (populated and within TTL)
        if global_cache.is_valid():
            # Serve from in-memory cache - near instant response
            if include_metadata:
                sampled = global_cache.sample_videos_with_metadata(count)
                return GlobalCacheWithMetadataResponse(
                    videos=sampled,
                    count=len(sampled),
                    sources=global_cache.sources,
                    timestamp=int(global_cache.timestamp),
                )
            else:
                sampled = global_cache.sample_videos(count)
                return GlobalCacheResponse(
                    videos=sampled,
                    count=len(sampled),
                    sources=global_cache.sources,
                    timestamp=int(global_cache.timestamp),
                )

        # Cache miss/expired - fetch fresh (cold start only, rare)
        logger.info("Global cache miss, fetching fresh from Redis...")
        result = await redis_layer.fetch_from_global_pools_raw(count)

        if include_metadata:
            # Enrich with metadata (async, reuses shared KVRocks client)
            video_metadata = await metadata_handler.batch_convert_video_ids(result["videos"])
            return GlobalCacheWithMetadataResponse(
                videos=video_metadata,
                count=len(video_metadata),
                sources=result["sources"],
                timestamp=result["timestamp"],
            )
        else:
            return GlobalCacheResponse(
                videos=result["videos"],
                count=result["count"],
                sources=result["sources"],
                timestamp=result["timestamp"],
            )

    except Exception as e:
        logger.error(f"Error fetching global cache: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# @app.post("/feedback/{user_id}", response_model=FeedbackResponse)
# async def submit_feedback(user_id: str, feedback: FeedbackRequest):
#     """
#     ASYNC - Mark videos as watched for a user.
#
#     This endpoint marks videos as consumed, adding them to the cooldown set
#     and optionally to the permanent bloom filter.
#     """
#     try:
#         # Mark videos in cooldown (1-day expiry)
#         videos_marked = await redis_layer.mark_videos_cooldown(
#             user_id, feedback.video_ids
#         )
#
#         # Optionally add to permanent bloom filter
#         if feedback.mark_permanent:
#             await redis_layer.mark_videos_permanently_watched(
#                 user_id, feedback.video_ids
#             )
#
#         logger.info(
#             f"Marked {videos_marked} videos for user {user_id} (permanent: {feedback.mark_permanent})"
#         )
#
#         return FeedbackResponse(
#             success=True,
#             videos_marked=videos_marked,
#             message=f"Marked {videos_marked} videos as {feedback.interaction_type}",
#         )
#
#     except Exception as e:
#         logger.error(f"Error submitting feedback for {user_id}: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


@app.get("/health", response_model=HealthResponse)
async def health_check():
    """
    ASYNC - Health check endpoint.

    Returns the current health status of the system including
    Redis connection, scheduler status, and uptime.
    """
    try:
        redis_connected = (
            await kvrocks_service.verify_connection() if kvrocks_service else False
        )
        scheduler_running = scheduler.running if scheduler else False
        uptime = (
            time.time() - app.state.start_time
            if hasattr(app.state, "start_time")
            else 0
        )

        return HealthResponse(
            status="healthy" if redis_connected and scheduler_running else "degraded",
            timestamp=int(time.time()),
            redis_connected=redis_connected,
            scheduler_running=scheduler_running,
            uptime_seconds=uptime,
        )
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return HealthResponse(
            status="unhealthy",
            timestamp=int(time.time()),
            redis_connected=False,
            scheduler_running=False,
            uptime_seconds=0,
        )


@app.get("/metrics")
async def metrics():
    """
    Expose Prometheus metrics for Fly.io scraping.

    Algorithm:
        1. Check if running in multiprocess mode (PROMETHEUS_MULTIPROC_DIR set)
        2. If multiprocess: create registry with MultiProcessCollector to aggregate
        3. If single process: use default registry
        4. Return as plain text in Prometheus exposition format

    Returns:
        PlainTextResponse with Prometheus metrics
    """
    # Check if running in multiprocess mode
    if 'PROMETHEUS_MULTIPROC_DIR' in os.environ or 'prometheus_multiproc_dir' in os.environ:
        # Multiprocess mode - aggregate metrics from all workers
        registry = CollectorRegistry()
        MultiProcessCollector(registry)
        output = generate_latest(registry)
    else:
        # Single process mode - use default registry
        output = generate_latest(REGISTRY)

    return PlainTextResponse(
        content=output,
        media_type=CONTENT_TYPE_LATEST
    )


@app.get("/metric_summary")
async def metric_summary():
    """
    Get 24-hour metrics summary with percentiles.

    Algorithm:
        1. Aggregate hourly bucket data from last 24 hours
        2. Compute p50, p90, p99 latencies per endpoint
        3. Return formatted JSON summary

    Returns:
        JSON with window, hours_with_data, and per-endpoint stats
        including total_requests, avg_latency_ms, p50_ms, p90_ms, p99_ms
    """
    return hourly_metrics.get_summary()


@app.get("/gchat-metrics-preview")
async def gchat_metrics_preview():
    """
    Preview the Google Chat metrics message format.

    Returns the exact text that would be sent to Google Chat webhook.
    Useful for testing/debugging the message format without waiting for scheduled job.

    Returns:
        PlainTextResponse with formatted metrics message
    """
    summary = hourly_metrics.get_summary()
    message = format_gchat_message(summary)
    return PlainTextResponse(content=message, media_type="text/plain")


@app.get("/feed-stats/{user_id}")
async def get_feed_stats(user_id: str):
    """
    ASYNC - Get feed statistics for a user (debugging endpoint).

    Returns detailed information about the user's feed status,
    including valid counts, refill needs, and percentile pointers.
    """
    try:
        stats = await video_mixer.get_feed_stats(user_id)
        return stats
    except Exception as e:
        logger.error(f"Error getting feed stats for {user_id}: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/status")
async def system_status():
    """
    ASYNC - Comprehensive system status endpoint.

    Returns detailed information about system components and configuration.
    """
    try:
        status = {
            "timestamp": datetime.utcnow().isoformat(),
            "version": "2.0.0-async",
            "environment": os.getenv("ENV", "production"),
            "components": {
                "redis": {
                    "connected": (
                        await kvrocks_service.verify_connection()
                        if kvrocks_service
                        else False
                    ),
                    "host": os.getenv("KVROCKS_HOST", "unknown"),
                    "port": os.getenv("KVROCKS_PORT", "unknown"),
                    "pool": {
                        "max_connections": (
                            kvrocks_service.max_connections
                            if kvrocks_service
                            else 0
                        ),
                        "created_connections": (
                            len(kvrocks_service.pool._created_connections)
                            if kvrocks_service
                            and hasattr(kvrocks_service.pool, "_created_connections")
                            else 0
                        ),
                        "available_connections": (
                            len(kvrocks_service.pool._available_connections)
                            if kvrocks_service
                            and hasattr(
                                kvrocks_service.pool, "_available_connections"
                            )
                            else 0
                        ),
                        "in_use_connections": (
                            len(kvrocks_service.pool._in_use_connections)
                            if kvrocks_service
                            and hasattr(kvrocks_service.pool, "_in_use_connections")
                            else 0
                        ),
                    },
                    "semaphore": {
                        "size": REDIS_SEMAPHORE_SIZE,
                        "available": (
                            redis_layer.redis_semaphore._value
                            if redis_layer
                            and hasattr(redis_layer.redis_semaphore, "_value")
                            else 0
                        ),
                        "waiters": (
                            len(redis_layer.redis_semaphore._waiters)
                            if redis_layer
                            and hasattr(redis_layer.redis_semaphore, "_waiters")
                            else 0
                        ),
                    },
                },
                "scheduler": {
                    "running": scheduler.running if scheduler else False,
                    "jobs": ["refresh_global_pools", "sync_bloom_filters"],
                    "note": "User feed refills happen automatically via fetch_videos() auto-refill",
                },
                "mixer": {"initialized": video_mixer is not None},
            },
            "uptime_seconds": (
                time.time() - app.state.start_time
                if hasattr(app.state, "start_time")
                else 0
            ),
        }

        return status

    except Exception as e:
        logger.error(f"Error getting system status: {e}")
        return JSONResponse(
            status_code=500,
            content={"error": "Failed to get system status", "details": str(e)},
        )


@app.get("/debug/user/{user_id}")
async def debug_user(user_id: str):
    """
    ASYNC - Debug endpoint for user state inspection.

    Returns detailed information about a user's recommendation state.
    """
    try:
        if not redis_layer:
            return JSONResponse(
                status_code=503, content={"error": "Redis not available"}
            )

        # Get comprehensive user state
        debug_info = {
            "user_id": user_id,
            "exists": await redis_layer.db.exists(
                redis_layer._key_bloom_permanent(user_id)
            ),
            "bloom_filter": {
                "exists": await redis_layer.db.exists(
                    redis_layer._key_bloom_permanent(user_id)
                ),
                "key": redis_layer._key_bloom_permanent(user_id),
            },
            "watched_set": {
                "count": await redis_layer.db.zcard(
                    redis_layer._key_watched_short_lived(user_id)
                ),
                "key": redis_layer._key_watched_short_lived(user_id),
            },
            "percentile_pointer": await redis_layer.get_pop_percentile_pointer(user_id),
            "feeds": {},
        }

        # Check each feed type (only production feed types)
        for rec_type in ["popularity", "freshness", "fallback"]:
            debug_info["feeds"][rec_type] = {
                "valid_count": await redis_layer.count_valid_videos_to_show(
                    user_id, rec_type
                ),
                "needs_refill": await redis_layer.needs_refill(user_id, rec_type),
                "total_count": await redis_layer.db.zcard(
                    redis_layer._key_videos_to_show(user_id, rec_type)
                ),
            }

        # Get debug state string
        debug_info["state_dump"] = await redis_layer.debug_user_state(user_id)

        return debug_info

    except Exception as e:
        logger.error(f"Error debugging user {user_id}: {e}")
        return JSONResponse(
            status_code=500, content={"error": str(e), "user_id": user_id}
        )



# @app.post("/admin/reset-user/{user_id}")
# async def reset_user(user_id: str):
#     """
#     ASYNC - Admin endpoint to reset a user's state.
#
#     Clears all user data and allows fresh start.
#     """
#     try:
#         if not redis_layer:
#             return JSONResponse(
#                 status_code=503, content={"error": "Redis not available"}
#             )
#
#         # Delete all user keys
#         keys_deleted = 0
#
#         # Delete bloom filter
#         if await redis_layer.db.delete(redis_layer._key_bloom_permanent(user_id)):
#             keys_deleted += 1
#
#         # Delete watched set
#         if await redis_layer.db.delete(redis_layer._key_watched_short_lived(user_id)):
#             keys_deleted += 1
#
#         # Delete percentile pointer
#         if await redis_layer.db.delete(
#             redis_layer._key_pop_percentile_pointer(user_id)
#         ):
#             keys_deleted += 1
#
#         # Delete all feed types (only production feed types)
#         for rec_type in ["popularity", "freshness", "fallback"]:
#             if await redis_layer.db.delete(
#                 redis_layer._key_videos_to_show(user_id, rec_type)
#             ):
#                 keys_deleted += 1
#
#         logger.info(f"Reset user {user_id}: deleted {keys_deleted} keys")
#
#         return {
#             "success": True,
#             "user_id": user_id,
#             "keys_deleted": keys_deleted,
#             "message": f"User {user_id} has been reset",
#         }
#
#     except Exception as e:
#         logger.error(f"Error resetting user {user_id}: {e}")
#         return JSONResponse(
#             status_code=500, content={"error": str(e), "user_id": user_id}
#         )
#
#
# @app.post("/admin/sync/popularity")
# async def trigger_popularity_sync():
#     """
#     Admin endpoint to manually trigger popularity pool sync.
#
#     Returns job status and whether it was triggered or skipped.
#     """
#     try:
#         if not os.getenv("SERVICE_CRED"):
#             raise HTTPException(status_code=503, detail="SERVICE_CRED not configured")
#
#         # Check if job is already running
#         lock_key = f"{JOB_LOCK_KEY_PREFIX}popularity_sync"
#         is_locked = await kvrocks_service.client.exists(lock_key)
#
#         if is_locked:
#             current_holder = await kvrocks_service.client.get(lock_key)
#             return {
#                 "triggered": False,
#                 "status": "already_running",
#                 "locked_by": current_holder if current_holder else "unknown",
#                 "message": "Popularity sync is already running",
#             }
#
#         # Trigger sync in background
#         asyncio.create_task(
#             sync_global_popularity_pools(redis_layer, kvrocks_service)
#         )
#
#         return {
#             "triggered": True,
#             "status": "started",
#             "message": "Popularity sync triggered successfully",
#         }
#
#     except Exception as e:
#         logger.error(f"Error triggering popularity sync: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.post("/admin/sync/freshness")
# async def trigger_freshness_sync():
#     """
#     Admin endpoint to manually trigger freshness windows sync.
#     """
#     try:
#         if not os.getenv("SERVICE_CRED"):
#             raise HTTPException(status_code=503, detail="SERVICE_CRED not configured")
#
#         lock_key = f"{JOB_LOCK_KEY_PREFIX}freshness_sync"
#         is_locked = await kvrocks_service.client.exists(lock_key)
#
#         if is_locked:
#             current_holder = await kvrocks_service.client.get(lock_key)
#             return {
#                 "triggered": False,
#                 "status": "already_running",
#                 "locked_by": current_holder if current_holder else "unknown",
#                 "message": "Freshness sync is already running",
#             }
#
#         asyncio.create_task(sync_freshness_windows(redis_layer, kvrocks_service))
#
#         return {
#             "triggered": True,
#             "status": "started",
#             "message": "Freshness sync triggered successfully",
#         }
#
#     except Exception as e:
#         logger.error(f"Error triggering freshness sync: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.post("/admin/sync/bloom")
# async def trigger_bloom_sync():
#     """
#     Admin endpoint to manually trigger user bloom filter sync.
#     """
#     try:
#         if not os.getenv("SERVICE_CRED"):
#             raise HTTPException(status_code=503, detail="SERVICE_CRED not configured")
#
#         lock_key = f"{JOB_LOCK_KEY_PREFIX}bloom_sync"
#         is_locked = await kvrocks_service.client.exists(lock_key)
#
#         if is_locked:
#             current_holder = await kvrocks_service.client.get(lock_key)
#             return {
#                 "triggered": False,
#                 "status": "already_running",
#                 "locked_by": current_holder if current_holder else "unknown",
#                 "message": "Bloom sync is already running",
#             }
#
#         asyncio.create_task(sync_user_bloom_filters(redis_layer, kvrocks_service))
#
#         return {
#             "triggered": True,
#             "status": "started",
#             "message": "Bloom filter sync triggered successfully",
#         }
#
#     except Exception as e:
#         logger.error(f"Error triggering bloom sync: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.post("/admin/sync/all")
# async def trigger_all_syncs():
#     """
#     Admin endpoint to trigger all sync jobs at once.
#     """
#     try:
#         if not os.getenv("SERVICE_CRED"):
#             raise HTTPException(status_code=503, detail="SERVICE_CRED not configured")
#
#         results = {}
#
#         # Check each lock
#         for job_type in ["popularity", "freshness", "bloom"]:
#             lock_key = f"{JOB_LOCK_KEY_PREFIX}{job_type}_sync"
#             is_locked = await kvrocks_service.client.exists(lock_key)
#
#             if is_locked:
#                 current_holder = await kvrocks_service.client.get(lock_key)
#                 results[job_type] = {
#                     "triggered": False,
#                     "status": "already_running",
#                     "locked_by": current_holder if current_holder else "unknown",
#                 }
#             else:
#                 # Trigger the appropriate sync
#                 if job_type == "popularity":
#                     asyncio.create_task(
#                         sync_global_popularity_pools(redis_layer, kvrocks_service)
#                     )
#                 elif job_type == "freshness":
#                     asyncio.create_task(
#                         sync_freshness_windows(redis_layer, kvrocks_service)
#                     )
#                 elif job_type == "bloom":
#                     asyncio.create_task(
#                         sync_user_bloom_filters(redis_layer, kvrocks_service)
#                     )
#
#                 results[job_type] = {"triggered": True, "status": "started"}
#
#         # Count triggered jobs
#         triggered_count = sum(1 for r in results.values() if r["triggered"])
#
#         return {
#             "message": f"Triggered {triggered_count}/3 sync jobs",
#             "results": results,
#         }
#
#     except Exception as e:
#         logger.error(f"Error triggering all syncs: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.get("/admin/sync/status")
# async def get_sync_status():
#     """
#     Admin endpoint to check the status of all sync jobs.
#     """
#     try:
#         status = {}
#
#         for job_type in ["popularity_sync", "freshness_sync", "bloom_sync"]:
#             lock_key = f"{JOB_LOCK_KEY_PREFIX}{job_type}"
#
#             # Check if lock exists
#             is_locked = await kvrocks_service.client.exists(lock_key)
#
#             if is_locked:
#                 # Get lock holder and TTL
#                 holder = await kvrocks_service.client.get(lock_key)
#                 ttl = await kvrocks_service.client.ttl(lock_key)
#
#                 status[job_type] = {
#                     "running": True,
#                     "locked_by": holder if holder else "unknown",
#                     "lock_ttl_seconds": ttl,
#                 }
#             else:
#                 status[job_type] = {
#                     "running": False,
#                     "locked_by": None,
#                     "lock_ttl_seconds": 0,
#                 }
#
#         # Add scheduler info
#         scheduler_jobs = []
#         if scheduler:
#             for job in scheduler.get_jobs():
#                 next_run = job.next_run_time.isoformat() if job.next_run_time else None
#                 scheduler_jobs.append(
#                     {"id": job.id, "next_run": next_run, "interval": str(job.trigger)}
#                 )
#
#         return {
#             "sync_jobs": status,
#             "scheduled_jobs": scheduler_jobs,
#             "bigquery_configured": bool(os.getenv("SERVICE_CRED")),
#         }
#
#     except Exception as e:
#         logger.error(f"Error getting sync status: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.get("/admin/sync/logs/{job_type}")
# async def get_sync_logs(job_type: str, limit: int = Query(default=100, ge=1, le=1000)):
#     """
#     Get recent logs for a specific sync job.
#
#     Args:
#         job_type: One of "popularity", "freshness", "bloom"
#         limit: Maximum number of log entries to return
#
#     Returns:
#         Recent log entries with metadata
#     """
#     try:
#         valid_types = ["popularity", "freshness", "bloom"]
#         if job_type not in valid_types:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Invalid job type. Must be one of: {valid_types}",
#             )
#
#         # Get logs from Redis
#         job_name = f"{job_type}_sync"
#         logs = await get_job_logs(kvrocks_service.client, job_name, limit)
#
#         return {"job_type": job_name, "log_count": len(logs), "logs": logs}
#
#     except Exception as e:
#         logger.error(f"Error retrieving logs for {job_type}: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.delete("/admin/sync/logs/{job_type}")
# async def clear_sync_logs(job_type: str):
#     """
#     Clear logs for a specific sync job.
#
#     Args:
#         job_type: One of "popularity", "freshness", "bloom"
#
#     Returns:
#         Success status
#     """
#     try:
#         valid_types = ["popularity", "freshness", "bloom"]
#         if job_type not in valid_types:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Invalid job type. Must be one of: {valid_types}",
#             )
#
#         job_name = f"{job_type}_sync"
#         cleared = await clear_job_logs(kvrocks_service.client, job_name)
#
#         return {
#             "success": cleared,
#             "message": f"Logs {'cleared' if cleared else 'not found'} for {job_name}",
#         }
#
#     except Exception as e:
#         logger.error(f"Error clearing logs for {job_type}: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.get("/admin/sync/logs")
# async def get_all_sync_logs(limit: int = Query(default=50, ge=1, le=500)):
#     """
#     Get recent logs from all sync jobs.
#
#     Args:
#         limit: Maximum number of log entries per job
#
#     Returns:
#         Dictionary with logs from all jobs
#     """
#     try:
#         all_logs = {}
#
#         for job_type in ["popularity", "freshness", "bloom"]:
#             job_name = f"{job_type}_sync"
#             logs = await get_job_logs(kvrocks_service.client, job_name, limit)
#             stats = await get_job_log_stats(kvrocks_service.client, job_name)
#
#             all_logs[job_name] = {
#                 "count": len(logs),
#                 "stats": stats,
#                 "recent_logs": logs,
#             }
#
#         return all_logs
#
#     except Exception as e:
#         logger.error(f"Error retrieving all logs: {e}")
#         raise HTTPException(status_code=500, detail=str(e))
#
#
# @app.get("/admin/sync/logs/{job_type}/stats")
# async def get_sync_log_stats(job_type: str):
#     """
#     Get statistics about logs for a specific sync job.
#
#     Args:
#         job_type: One of "popularity", "freshness", "bloom"
#
#     Returns:
#         Log statistics including count, TTL, most recent log
#     """
#     try:
#         valid_types = ["popularity", "freshness", "bloom"]
#         if job_type not in valid_types:
#             raise HTTPException(
#                 status_code=400,
#                 detail=f"Invalid job type. Must be one of: {valid_types}",
#             )
#
#         job_name = f"{job_type}_sync"
#         stats = await get_job_log_stats(kvrocks_service.client, job_name)
#
#         return stats
#
#     except Exception as e:
#         logger.error(f"Error getting log stats for {job_type}: {e}")
#         raise HTTPException(status_code=500, detail=str(e))


# ============================================================================
# Tournament Endpoints
# ============================================================================


@app.post(
    "/tournament/register",
    response_model=TournamentRegisterResponse,
    responses={
        401: {"model": TournamentErrorResponse, "description": "Unauthorized"},
        409: {"model": TournamentErrorResponse, "description": "Tournament already exists"},
        503: {"model": TournamentErrorResponse, "description": "Insufficient videos available"},
    },
)
async def register_tournament(
    request: TournamentRegisterRequest,
    _: bool = Depends(verify_admin_key),
):
    """
    Register a new tournament and generate its video set (Admin Only).

    This endpoint creates a tournament with a set of top-performing bot_uploaded/AI
    videos. Videos are selected from BigQuery based on popularity score, excluding
    recently-used tournament videos to prevent overlap.

    Algorithm:
        1. Check if tournament already exists (return 409 if so)
        2. Fetch tournament-eligible videos from BigQuery
        3. Filter out recently-used videos (7-day cooldown)
        4. If not enough, allow partial overlap with oldest used videos
        5. Store video set in Redis with 3-day TTL
        6. Track videos as recently-used for future overlap prevention

    Args:
        request: TournamentRegisterRequest with tournament_id and video_count
        _: Admin key verification (via Depends)

    Returns:
        TournamentRegisterResponse with tournament_id, video_count, created_at

    Raises:
        HTTPException 401: Invalid or missing admin key
        HTTPException 409: Tournament already exists
        HTTPException 503: Insufficient videos available
    """
    import uuid
    from utils.bigquery_client import BigQueryClient

    video_count = request.video_count

    # Generate tournament_id if not provided, otherwise use client's ID
    if request.tournament_id:
        tournament_id = request.tournament_id
        # Only check for duplicates when client provides ID
        try:
            if await redis_layer.tournament_exists(tournament_id):
                raise HTTPException(
                    status_code=409,
                    detail={
                        "error": "tournament_exists",
                        "message": f"Tournament {tournament_id} already exists"
                    }
                )
        except HTTPException:
            raise
    else:
        # Server-generated UUID - guaranteed unique, no duplicate check needed
        tournament_id = str(uuid.uuid4())

    try:
        # Fetch large pool of tournament-eligible videos WITH metadata from BigQuery
        # Using 10000 pool enables natural cascade through popularity tiers
        bq_client = BigQueryClient()
        candidates_df = bq_client.fetch_tournament_eligible_videos(
            limit=TOURNAMENT_VIDEO_POOL_SIZE
        )

        # Deduplicate by video_id at source - prevents all downstream duplicate issues
        # BigQuery LEFT JOINs can return multiple rows per video_id if metadata differs
        candidates_df = candidates_df.drop_duplicates(subset=['video_id'], keep='first')

        if candidates_df.empty:
            raise HTTPException(
                status_code=503,
                detail={
                    "error": "insufficient_videos",
                    "message": "No tournament-eligible videos found in BigQuery",
                    "available_count": 0,
                    "requested_count": video_count
                }
            )

        # Get recently-used tournament videos (within 7-day cooldown window)
        recently_used = await redis_layer.get_recently_used_tournament_videos()

        # Keep original DataFrame for fallback before filtering
        original_candidates_df = candidates_df.copy()

        # Filter out recently-used videos using pandas for efficiency
        # Videos are already ordered by popularity score (DESC) from BigQuery
        fresh_df = candidates_df[~candidates_df["video_id"].isin(recently_used)]

        # Take top N from filtered results (natural cascade through popularity)
        selected_df = fresh_df.head(video_count)
        # Defense-in-depth: deduplicate by video_id in case BigQuery returns duplicates
        selected_df = selected_df.drop_duplicates(subset=['video_id'], keep='first')

        # If not enough fresh videos after filtering all 10000, allow partial overlap
        if len(selected_df) < video_count:
            needed = video_count - len(selected_df)

            # Use cached original DataFrame to get recently-used videos (ordered by popularity)
            overlapping_df = original_candidates_df[
                original_candidates_df["video_id"].isin(recently_used)
            ].head(needed)

            # Combine fresh and overlapping DataFrames
            selected_df = pd.concat([selected_df, overlapping_df], ignore_index=True)
            # Defense-in-depth: deduplicate after concat to prevent same video in both sets
            selected_df = selected_df.drop_duplicates(subset=['video_id'], keep='first')

            logger.warning(
                f"Tournament {tournament_id}: Using {len(overlapping_df)} overlapping videos "
                f"due to insufficient fresh videos in pool of {TOURNAMENT_VIDEO_POOL_SIZE}"
            )

        # Graceful degradation: return what we have instead of failing
        if len(selected_df) < video_count:
            logger.warning(
                f"Tournament {tournament_id}: Only {len(selected_df)} unique videos available, "
                f"requested {video_count}. Proceeding with available videos."
            )

        videos_with_metadata = selected_df[
            ["video_id", "canister_id", "post_id", "publisher_user_id"]
        ].to_dict("records")

        for video in videos_with_metadata:
            video["canister_id"] = STUBBED_CANISTER_ID

        result = await redis_layer.store_tournament_videos(tournament_id, videos_with_metadata)

        logger.info(
            f"Tournament {tournament_id} registered with {len(videos_with_metadata)} videos (with metadata)"
        )

        return TournamentRegisterResponse(
            tournament_id=tournament_id,
            video_count=len(videos_with_metadata),
            created_at=result["created_at"]
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error registering tournament {tournament_id}: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": str(e)}
        )


@app.get(
    "/tournament/{tournament_id}/videos",
    responses={
        200: {"description": "Tournament videos returned successfully"},
        404: {"model": TournamentErrorResponse, "description": "Tournament not found"},
    },
)
async def get_tournament_videos(
    tournament_id: str,
    with_metadata: bool = Query(default=False, description="Include canister_id, post_id, publisher_user_id"),
):
    """
    Get the video set for a tournament.

    This endpoint returns the pre-generated video set for a tournament.
    Videos are returned in the order they were selected (by popularity score).
    Metadata is pre-stored 

    Algorithm:
        1. Fetch videos from Redis (with or without metadata)
        2. If not found, return 404
        3. Return video list

    Args:
        tournament_id: Tournament identifier
        with_metadata: Include video metadata (canister_id, post_id, publisher_user_id)

    Returns:
        TournamentVideosResponse or TournamentVideosWithMetadataResponse

    Raises:
        HTTPException 404: Tournament not found
    """
    try:
        if with_metadata:
            # Fetch videos with full metadata from Redis (pre-stored at registration)
            videos_data = await redis_layer.get_tournament_videos(tournament_id, with_metadata=True)

            if videos_data is None:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "tournament_not_found",
                        "message": f"Tournament {tournament_id} does not exist"
                    }
                )

            return TournamentVideosWithMetadataResponse(
                tournament_id=tournament_id,
                video_count=len(videos_data),
                videos=[
                    VideoMetadata(
                        video_id=str(v["video_id"]),
                        canister_id=str(v["canister_id"]),
                        post_id=str(v["post_id"]),
                        publisher_user_id=str(v["publisher_user_id"])
                    )
                    for v in videos_data
                ]
            )
        else:
            # Fetch just video IDs from Redis
            video_ids = await redis_layer.get_tournament_videos(tournament_id, with_metadata=False)

            if video_ids is None:
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "tournament_not_found",
                        "message": f"Tournament {tournament_id} does not exist"
                    }
                )

            return TournamentVideosResponse(
                tournament_id=tournament_id,
                video_count=len(video_ids),
                videos=video_ids
            )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching tournament {tournament_id} videos: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": str(e)}
        )


@app.get(
    "/tournament/overlap",
    response_model=TournamentOverlapResponse,
    responses={
        400: {"model": TournamentErrorResponse, "description": "Invalid request (same tournament)"},
        404: {"model": TournamentErrorResponse, "description": "Tournament not found"},
    },
)
async def check_tournament_overlap(
    tournament_a: str = Query(..., description="First tournament ID"),
    tournament_b: str = Query(..., description="Second tournament ID"),
    include_video_ids: bool = Query(
        default=True,
        description="Include list of overlapping video IDs in response"
    ),
):
    """
    Check video overlap between two tournaments.

    This endpoint compares the video sets of two tournaments and returns
    overlap statistics including count, percentages, and optionally the
    list of overlapping video IDs.

    Algorithm:
        1. Validate tournament IDs are different
        2. Fetch and compare video sets from both tournaments
        3. Calculate intersection and percentages
        4. Return overlap statistics

    Args:
        tournament_a: First tournament identifier
        tournament_b: Second tournament identifier
        include_video_ids: If True, include list of overlapping video IDs

    Returns:
        TournamentOverlapResponse with overlap statistics

    Raises:
        HTTPException 400: Same tournament ID provided for both
        HTTPException 404: One or both tournaments not found
    """
    try:
        result = await redis_layer.get_tournament_overlap(
            tournament_a,
            tournament_b,
            include_video_ids
        )

        # Handle error cases from utility method
        if result and "error" in result:
            if result["error"] == "same_tournament":
                raise HTTPException(
                    status_code=400,
                    detail={
                        "error": "same_tournament",
                        "message": "Cannot compare a tournament with itself"
                    }
                )
            elif result["error"] == "tournament_not_found":
                raise HTTPException(
                    status_code=404,
                    detail={
                        "error": "tournament_not_found",
                        "message": result["message"],
                        "tournament_id": result.get("tournament_id")
                    }
                )

        return TournamentOverlapResponse(**result)

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error checking tournament overlap: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": str(e)}
        )


@app.get(
    "/tournament/list",
    response_model=TournamentListResponse,
    responses={401: {"model": TournamentErrorResponse}},
)
async def list_tournaments(
    _: str = Depends(verify_admin_key)
):
    """
    List all tournaments (active and expired) with their status (Admin Only).

    Algorithm:
        1. Fetch all tournaments from Redis registry
        2. Calculate status for each based on created_at + duration
        3. Return sorted by created_at descending

    Returns:
        TournamentListResponse with count and list of tournaments

    Raises:
        HTTPException 401: Invalid/missing admin key
    """
    try:
        tournaments = await redis_layer.list_all_tournaments()

        # Count active and expired tournaments
        active_count = sum(1 for t in tournaments if t["status"] == "active")
        expired_count = sum(1 for t in tournaments if t["status"] == "expired")

        return TournamentListResponse(
            count=len(tournaments),
            active_count=active_count,
            expired_count=expired_count,
            tournaments=[
                TournamentListItem(
                    tournament_id=t["tournament_id"],
                    status=t["status"],
                    video_count=t["video_count"],
                    created_at=t["created_at"],
                    expires_at=t["expires_at"]
                )
                for t in tournaments
            ]
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error listing tournaments: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": str(e)}
        )


@app.delete(
    "/tournament/{tournament_id}",
    response_model=TournamentDeleteResponse,
    responses={
        401: {"model": TournamentErrorResponse},
        404: {"model": TournamentErrorResponse},
    },
)
async def delete_tournament_endpoint(
    tournament_id: str,
    _: str = Depends(verify_admin_key)
):
    """
    Delete a tournament and clear its videos from cooldown (Admin Only).

    This immediately removes:
    - Tournament video list
    - Tournament metadata
    - Videos from the 7-day cooldown pool (making them available for new tournaments)

    Algorithm:
        1. Verify tournament exists
        2. Remove videos from cooldown ZSET
        3. Delete tournament data
        4. Remove from registry

    Args:
        tournament_id: Tournament to delete

    Returns:
        TournamentDeleteResponse with deletion summary

    Raises:
        HTTPException 401: Invalid/missing admin key
        HTTPException 404: Tournament not found
    """
    try:
        result = await redis_layer.delete_tournament(tournament_id)

        if result is None:
            raise HTTPException(
                status_code=404,
                detail={
                    "error": "tournament_not_found",
                    "message": f"Tournament {tournament_id} does not exist"
                }
            )

        return TournamentDeleteResponse(
            tournament_id=result["tournament_id"],
            deleted=result["deleted"],
            status_before_deletion=result["status_before_deletion"],
            videos_removed_from_cooldown=result["videos_removed_from_cooldown"],
            message=result["message"]
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error deleting tournament {tournament_id}: {e}")
        sentry_sdk.capture_exception(e)
        raise HTTPException(
            status_code=500,
            detail={"error": "internal_error", "message": str(e)}
        )


@app.get("/")
async def root():
    """Root endpoint with API information."""
    return {
        "service": "Feed API",
        "version": "2.0.0-async",
        "mode": "production",
        "endpoints": [
            "/recommend/{user_id}",
            "/recommend-with-metadata/{user_id}",
            "/v2/recommend-with-metadata/{user_id}",
            "/global-cache",
            "/feedback/{user_id}",
            "/tournament/register",
            "/tournament/{tournament_id}/videos",
            "/tournament/overlap",
            "/tournament/list",
            "/tournament/{tournament_id}",  # DELETE
            "/health",
            "/status",
            "/metrics",
            "/feed-stats/{user_id}",
            "/debug/user/{user_id}",
            "/docs",
            "/redoc",
        ],
    }


# ============================================================================
# Error Handlers
# ============================================================================


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """Handle validation errors with better formatting."""
    return JSONResponse(
        status_code=422,
        content={
            "error": "Validation Error",
            "details": exc.errors(),
            "body": exc.body if hasattr(exc, "body") else None,
        },
    )


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Handle HTTP exceptions with consistent formatting."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "error": exc.detail,
            "status_code": exc.status_code,
            "path": request.url.path,
        },
    )


# ============================================================================
# Middleware for Request Logging and Prometheus Metrics
# ============================================================================


def _normalize_endpoint(path: str) -> str:
    """
    Normalize URL paths to reduce metric label cardinality.

    Replaces dynamic segments (user IDs) with placeholders to prevent
    metric explosion from unique user IDs.

    Args:
        path: Raw URL path like /recommend/user123

    Returns:
        Normalized path like /recommend/{user_id}
    """
    import re
    path = re.sub(r'/recommend/[^/]+', '/recommend/{user_id}', path)
    path = re.sub(r'/recommend-with-metadata/[^/]+', '/recommend-with-metadata/{user_id}', path)
    path = re.sub(r'/v2/recommend-with-metadata/[^/]+', '/v2/recommend-with-metadata/{user_id}', path)
    path = re.sub(r'/feedback/[^/]+', '/feedback/{user_id}', path)
    path = re.sub(r'/feed-stats/[^/]+', '/feed-stats/{user_id}', path)
    path = re.sub(r'/debug/user/[^/]+', '/debug/user/{user_id}', path)
    return path


def _extract_user_id(path: str) -> str:
    """
    Extract user_id from request path if present.

    Args:
        path: Raw URL path like /recommend/abc123

    Returns:
        User ID if found, None otherwise
    """
    import re
    patterns = [
        r'/recommend/([^/]+)',
        r'/recommend-with-metadata/([^/]+)',
        r'/v2/recommend-with-metadata/([^/]+)',
        r'/feedback/([^/]+)',
        r'/feed-stats/([^/]+)',
        r'/debug/user/([^/]+)',
    ]
    for pattern in patterns:
        match = re.search(pattern, path)
        if match:
            return match.group(1)
    return None


@app.middleware("http")
async def log_requests(request: Request, call_next):
    """
    Middleware for request logging and Prometheus metrics.

    Algorithm:
        1. Skip metrics recording for /metrics endpoint itself
        2. Increment active requests gauge
        3. Record start time and process request
        4. Record latency histogram and request counter
        5. Log response at DEBUG level (respects LOG_LEVEL)

    Args:
        request: Incoming HTTP request
        call_next: Next middleware/handler in chain

    Returns:
        HTTP response with timing headers
    """
    # Skip metrics for /metrics endpoint to avoid recursion
    if request.url.path == "/metrics":
        return await call_next(request)

    endpoint = _normalize_endpoint(request.url.path)
    ACTIVE_REQUESTS.inc()
    start_time = time.time()

    try:
        # Log request at debug level
        logger.debug(f"Request: {request.method} {request.url.path}")

        # Process request
        response = await call_next(request)

        # Calculate duration
        duration = time.time() - start_time

        # Record Prometheus metrics
        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status_code=response.status_code
        ).inc()

        REQUEST_LATENCY.labels(
            method=request.method,
            endpoint=endpoint
        ).observe(duration)

        # Record to hourly metrics for /metric_summary endpoint (includes error tracking)
        hourly_metrics.record(f"{request.method} {endpoint}", duration, response.status_code)

        # Send error alert to Google Chat (if configured and within rate limit)
        if (response.status_code >= 400
            and GCHAT_WEBHOOK_URL
            and response.status_code not in GCHAT_EXCLUDED_STATUS_CODES):
            full_endpoint = f"{request.method} {endpoint}"
            if error_alert_limiter.should_alert(full_endpoint):
                user_id = _extract_user_id(request.url.path)
                # Fire-and-forget: don't await, don't block response
                asyncio.create_task(
                    send_error_alert_to_gchat(
                        GCHAT_WEBHOOK_URL,
                        full_endpoint,
                        response.status_code,
                        user_id
                    )
                )

        # Log response at DEBUG level (respects LOG_LEVEL setting)
        logger.debug(
            f"Response: {request.method} {request.url.path} "
            f"status={response.status_code} duration={duration:.3f}s"
        )

        # Add custom headers
        response.headers["X-Process-Time"] = str(duration)
        response.headers["X-Server-Version"] = "2.0.0-async"

        return response
    finally:
        ACTIVE_REQUESTS.dec()


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import uvicorn

    logger.info("=" * 80)
    logger.info("Starting Feed API")
    logger.info(f"Environment: {os.getenv('ENV', 'development')}")
    logger.info(f"KVRocks Host: {os.getenv('KVROCKS_HOST', 'localhost')}")
    logger.info(f"Log Level: {LOG_LEVEL}")
    logger.info("=" * 80)

    # Run the server
    uvicorn.run(
        "async_api_server:app",
        host="0.0.0.0",
        port=8000,
        workers=2,  # Single worker for development; scale in production
        reload=False,
        log_level=LOG_LEVEL.lower(),
    )
