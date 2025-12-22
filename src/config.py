"""
Configuration constants for the recommendation system.

This module contains all configuration values including TTLs, thresholds,
mixer ratios, job schedules, and system settings. Centralizing these
values makes the system easier to tune and maintain.
"""

import os
from typing import Dict, List

# ============================================================================
# REDIS CONNECTION
# ============================================================================

# Hardcoded connection pool settings (optimized for 2-worker setup)
# Each worker gets its own pool of max_connections
REDIS_MAX_CONNECTIONS = 100  # Per worker
REDIS_SOCKET_TIMEOUT = 60  # Seconds

REDIS_CONFIG = {
    "host": os.getenv("DRAGONFLY_HOST", "localhost"),
    "port": int(os.getenv("DRAGONFLY_PORT", "6379")),
    "password": os.getenv("DRAGONFLY_PASSWORD", "redispass"),
    "cluster_enabled": os.getenv("DRAGONFLY_CLUSTER_ENABLED", "false").lower() == "true",
    "ssl_enabled": os.getenv("DRAGONFLY_TLS_ENABLED", "false").lower() == "true",
    "max_connections": REDIS_MAX_CONNECTIONS,
    "socket_timeout": REDIS_SOCKET_TIMEOUT,
}

# ============================================================================
# SEMAPHORE CONFIGURATION (Connection Pool Protection)
# ============================================================================

# Limit concurrent Redis operations to prevent pool exhaustion
# Reserve 20% of connections for health checks, monitoring, and background ops
# When using multiple workers, each worker gets its own pool and semaphore
REDIS_SEMAPHORE_SIZE = int(REDIS_MAX_CONNECTIONS * 0.8)  # 80 for 100 max connections

# ============================================================================
# TTL VALUES (in seconds)
# ============================================================================

TTL_WATCHED_SET = 24 * 60 * 60  # 1 day - short-lived watched videos
TTL_VIDEOS_TO_SHOW = 3 * 24 * 60 * 60  # 3 days - videos in to_show sets
TTL_GLOBAL_FRESHNESS = 7 * 24 * 60 * 60  # 7 days - global freshness pools
TTL_FALLBACK_VIDEOS = 7 * 24 * 60 * 60  # 7 days - fallback pool

# ============================================================================
# CAPACITY AND THRESHOLDS
# ============================================================================

VIDEOS_TO_SHOW_CAPACITY = 500  # Max videos in user's to_show set
REFILL_THRESHOLD = 200  # Refill when valid videos < this
VIDEOS_PER_REQUEST = 100  # Default videos to return per recommendation request
REFILL_BATCH_SIZE = 1000  # Videos to fetch per refill iteration (reduced from 5000)
REFILL_MAX_ITERATIONS = 10  # Max iterations for refill operations

# Bloom filter configuration
BLOOM_ERROR_RATE = 0.01  # 1% false positive rate
BLOOM_INITIAL_CAPACITY = 1000  # Start with 1K, will auto-expand
BLOOM_EXPANSION = 2  # Double capacity on each expansion
BLOOM_TTL_DAYS = 30  # Auto-expire inactive users after 30 days of inactivity

# ============================================================================
# POPULARITY PERCENTILE BUCKETS
# ============================================================================

PERCENTILE_BUCKETS = [
    "99_100",  # Top 1% most popular
    "90_99",   # Top 10%
    "80_90",   # Top 20%
    "70_80",   # Top 30%
    "60_70",   # Top 40%
    "50_60",   # Top 50%
    "40_50",   # Bottom 50%
    "30_40",   # Bottom 40%
    "20_30",   # Bottom 30%
    "10_20",   # Bottom 20%
    "0_10",    # Bottom 10% least popular
]

# Videos per bucket (for dummy data generation)
VIDEOS_PER_BUCKET = {
    "99_100": 1000,
    "90_99": 1000,
    "80_90": 1000,
    "70_80": 1000,
    "60_70": 1000,
    "50_60": 1000,
    "40_50": 1000,
    "30_40": 1000,
    "20_30": 1000,
    "10_20": 1000,
    "0_10": 1000,
}

# ============================================================================
# FRESHNESS WINDOWS
# ============================================================================

FRESHNESS_WINDOWS = [
    "l1d",   # Last 1 day
    "l7d",   # Last 7 days
    "l14d",  # Last 14 days
    "l30d",  # Last 30 days
    "l90d",  # Last 90 days
]

# Videos per window (for dummy data generation)
VIDEOS_PER_WINDOW = {
    "l1d": 200,
    "l7d": 400,
    "l14d": 300,
    "l30d": 200,
    "l90d": 100,
}

# ============================================================================
# MIXER CONFIGURATION
# ============================================================================

# Default mixing ratios for different feed types
MIXER_RATIOS = {
    "similarity": 0.40,  # 40% - Initially random, future: semantic search
    "popularity": 0.35,  # 35% - From popularity percentile buckets
    "freshness": 0.25,   # 25% - From freshness windows
}

# Production mixing ratios (no simulation, no similarity)
PRODUCTION_MIXER_RATIOS = {
    "popularity": 0.60,  # 60% - From popularity percentile buckets
    "freshness": 0.40,   # 40% - From freshness windows
}

# Fallback ratios when primary feeds are insufficient
FALLBACK_RATIOS = {
    "popularity": 0.50,
    "freshness": 0.30,
    "fallback": 0.20,
}

# ============================================================================
# UGC (USER-GENERATED CONTENT) CONFIGURATION
# ============================================================================

# UGC mixing ratio - 5% of non-following slots reserved for UGC content
# This promotes content from real platform users (ai_ugc + ugc_content_approval)
UGC_RATIO = 0.05  # 5% of remaining slots after following

# UGC pool TTL (same as other feeds)
TTL_UGC_VIDEOS = 3 * 24 * 60 * 60  # 3 days

# Maximum UGC videos to keep in global pool
UGC_POOL_CAPACITY = 10000  # 10K videos, ordered by freshness

# ============================================================================
# FOLLOWING CONTENT CONFIGURATION
# ============================================================================

# Following content TTL (same as other feeds)
TTL_FOLLOWING_VIDEOS = 3 * 24 * 60 * 60  # 3 days

# Following sync rate limiting (on-demand only, NOT periodic)
FOLLOWING_SYNC_COOLDOWN = 10 * 60  # 10 minutes between sync attempts
FOLLOWING_REFILL_THRESHOLD = 10  # Trigger sync if pool below this (< 10 videos)

# Following content mixing - position-based distribution
FOLLOWING_SEGMENT_1_RANGE = (1, 10)   # First 10 positions
FOLLOWING_SEGMENT_1_MIN = 3           # At least 3 from following in segment 1
FOLLOWING_SEGMENT_1_MAX = 5           # At most 5 from following in segment 1

FOLLOWING_SEGMENT_2_RANGE = (11, 50)  # Positions 11-50
FOLLOWING_SEGMENT_2_MIN = 10          # At least 10 from following in segment 2

# ============================================================================
# TOURNAMENT CONFIGURATION
# ============================================================================

# Tournament TTL - auto-expire tournament data after this period
TOURNAMENT_TTL_DAYS = 3
TOURNAMENT_TTL_SECONDS = TOURNAMENT_TTL_DAYS * 24 * 60 * 60  # 3 days in seconds

# Video reuse cooldown - prevent same videos appearing in tournaments too frequently
TOURNAMENT_VIDEO_REUSE_COOLDOWN_DAYS = 7
TOURNAMENT_VIDEO_REUSE_COOLDOWN_SECONDS = TOURNAMENT_VIDEO_REUSE_COOLDOWN_DAYS * 24 * 60 * 60

# Admin API key for tournament registration endpoint
ADMIN_API_KEY = os.getenv("ADMIN_API_KEY", "")

# BigQuery table for tournament-eligible videos
TOURNAMENT_VIDEO_TABLE = "yral_ds.global_popular_videos_l7d"

# Large pool size for fetching tournament videos - enables natural cascade through popularity
# When filtering out recently-used videos, having a large pool ensures we can still
# select the required number of videos from lower popularity tiers
TOURNAMENT_VIDEO_POOL_SIZE = 10000

# ============================================================================
# BACKGROUND JOB SCHEDULES (in seconds)
# ============================================================================

# How often to refresh global pools from BigQuery
GLOBAL_REFRESH_INTERVAL = 3600  # 1 hour

# How often to sync user watch history to bloom filters
BLOOM_SYNC_INTERVAL = 1800  # 30 minutes

# DEPRECATED: User feed refills now happen automatically via fetch_videos() auto-refill
# This constant is kept for backward compatibility but is no longer used
REFILL_CHECK_INTERVAL = 900  # 15 minutes (UNUSED - auto-refill handles this)

# How often to cleanup expired entries
CLEANUP_INTERVAL = 3600  # 1 hour

# ============================================================================
# BIGQUERY SYNC CONFIGURATION
# ============================================================================

# Sync interval: 6 hours (matches user requirement)
BIGQUERY_SYNC_INTERVAL = 6 * 60 * 60  # 6 hours in seconds

# Jitter configuration to prevent thundering herd
# Each job will add random delay between min and max before starting
SYNC_JITTER_MIN = 0  # Minimum jitter in seconds
SYNC_JITTER_MAX = 300  # Maximum jitter in seconds (5 minutes)

# Distributed lock configuration
JOB_LOCK_TTL = 60 * 60  # 1 hour - locks expire after this time to prevent deadlocks
JOB_LOCK_KEY_PREFIX = "job:lock:"

# Batch processing configuration for BigQuery data
BQ_BATCH_SIZE = 10000  # Process BigQuery results in chunks of this size
BQ_PIPELINE_SIZE = 1000  # Redis pipeline batch size for bulk operations
BQ_MAX_WORKERS = 3  # Maximum concurrent batch processors

# BigQuery specific settings
BQ_PROJECT_ID = "hot-or-not-feed-intelligence"
BQ_DATASET = "yral_ds"
BQ_TIMEOUT = 300  # Query timeout in seconds (5 minutes)

# ============================================================================
# DATA SIMULATOR CONFIGURATION
# ============================================================================

# Total number of videos to simulate
TOTAL_VIDEOS = 10000

# Number of simulated users
TOTAL_USERS = 1000

# Average interactions per user
AVG_INTERACTIONS_PER_USER = 50

# Video metadata ranges
VIEW_COUNT_RANGE = (0, 1000000)  # Min and max view counts
ENGAGEMENT_SCORE_RANGE = (0.0, 100.0)  # Engagement score range

# ============================================================================
# API CONFIGURATION
# ============================================================================

# API server settings
API_HOST = "0.0.0.0"
API_PORT = 8000
API_WORKERS = 4
API_RELOAD = os.getenv("ENV", "development") == "development"

# CORS settings
CORS_ORIGINS = ["*"]  # In production, specify actual origins
CORS_ALLOW_CREDENTIALS = True
CORS_ALLOW_METHODS = ["*"]
CORS_ALLOW_HEADERS = ["*"]

# Rate limiting
RATE_LIMIT_REQUESTS = 100  # Requests per minute
RATE_LIMIT_WINDOW = 60  # Window in seconds

# ============================================================================
# GOOGLE CHAT WEBHOOK (PRODUCTION ONLY)
# ============================================================================

# Webhook URL for metrics alerts - only set in production CI
# Stage servers will not send alerts (safe fallback when unset)
GCHAT_WEBHOOK_URL = os.getenv("GCHAT_WEBHOOK_URL", "")

# Schedule: 9 AM, 3 PM, 9 PM IST = 3:30, 9:30, 15:30 UTC
GCHAT_METRICS_HOURS_UTC = [3, 9, 15]
GCHAT_METRICS_MINUTE_UTC = 30

# Error alert rate limiting
GCHAT_ERROR_ALERT_LIMIT = 4  # Max alerts per endpoint per minute
GCHAT_ERROR_ALERT_WINDOW = 60  # Window in seconds
GCHAT_ALERT_MENTION = "<users/jay@gobazzinga.io>"  # User to mention on errors
GCHAT_EXCLUDED_STATUS_CODES = {404}  # Client errors to exclude from alerts (noise)

# ============================================================================
# MONITORING AND METRICS
# ============================================================================

# Prometheus metrics configuration
METRICS_ENABLED = True
METRICS_PORT = 9090
METRICS_PATH = "/metrics"

# Metric buckets for histograms
LATENCY_BUCKETS = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0]

# ============================================================================
# LOGGING CONFIGURATION
# ============================================================================

# Master switch for debug/info logging (set to False for production/stress testing)
ENABLE_DEBUG_LOGGING = False

# Lua script logging (set to False for production/stress testing)
ENABLE_LUA_SCRIPT_LOGGING = False

# Set log level based on debug flag
LOG_LEVEL = "INFO" if ENABLE_DEBUG_LOGGING else "WARNING"
LOG_FORMAT = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
LOG_FILE = "recommendation_system.log"

# ============================================================================
# FEATURE FLAGS
# ============================================================================

# Stubbed canister ID - all metadata endpoints return this value
STUBBED_CANISTER_ID = "ivkka-7qaaa-aaaas-qbg3q-cai"

# Enable/disable specific features
FEATURES = {
    "enable_semantic_search": False,  # Not implemented yet
    "enable_collaborative_filtering": False,  # Not implemented yet
    "enable_real_bigquery": False,  # Use dummy data for now
    "enable_cache_warming": True,  # Proactively warm caches
    "enable_auto_refill": True,  # Automatically refill low feeds
    "enable_metrics": True,  # Export Prometheus metrics
}

# ============================================================================
# SYSTEM LIMITS
# ============================================================================

# Maximum concurrent refill operations
MAX_CONCURRENT_REFILLS = 10

# Maximum videos to fetch in a single operation
MAX_VIDEOS_PER_FETCH = 500

# Maximum retries for failed operations
MAX_RETRIES = 3

# Timeout for Redis operations (seconds)
REDIS_TIMEOUT = 5

# ============================================================================
# DUMMY DATA PATTERNS
# ENV for adhoc tests, not prod facing
# ============================================================================

# Patterns for generating realistic video IDs
VIDEO_ID_PATTERN = "vid_{category}_{index:06d}"

# Categories for video classification
VIDEO_CATEGORIES = [
    "entertainment",
    "education",
    "gaming",
    "music",
    "sports",
    "news",
    "cooking",
    "travel",
    "tech",
    "lifestyle",
]

# User ID pattern
USER_ID_PATTERN = "user_{index:06d}"