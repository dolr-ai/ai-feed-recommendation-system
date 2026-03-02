# Knowledge Transfer: Recsys

---

## 1. System Overview

**What it does:** Serves personalized short-video recommendations to users. Each API call returns ~100 videos mixed from popularity-ranked content, fresh uploads, followed-user content, and user-generated content (UGC).

**Tech stack:**

| Layer         | Technology                                              |
|---------------|---------------------------------------------------------|
| API           | FastAPI (fully async), uvicorn (2 workers in production)|
| Data store    | KVRocks (Redis-compatible, cluster mode with TLS)       |
| Data source   | BigQuery (GCP project: `hot-or-not-feed-intelligence`)  |
| Deployment    | Fly.io (`fly deploy`)                                   |
| Monitoring    | Sentry (errors), Prometheus (metrics), Google Chat (alerts) |

**Core guarantee:** The API must *always* return the requested number of videos. If a user's personal pool runs dry, the system auto-refills from global pools. If that fails, it uses fallback pool. If even that fails, it logs the exhaustion. The returned videos must always be unique.

---

## 2. Architecture

### Component Hierarchy

```
async_api_server.py          <-- FastAPI app, endpoints, lifespan, scheduler
    |
    +-- async_mixer.py        <-- Feed mixing logic ( pop/fresh, following, UGC)
    |       |
    |       +-- async_main.py <-- AsyncRedisLayer: all Redis ops, Lua scripts, refill logic
    |               |
    |               +-- utils/async_redis_utils.py  <-- Connection pooling, cluster/TLS
    |
    +-- background_jobs.py    <-- BigQuery sync jobs (popularity, freshness, bloom, UGC, exclude)
    |       |
    |       +-- utils/bigquery_client.py  <-- SQL queries against BigQuery
    |
    +-- metadata_handler.py   <-- Converts video_id -> canister_id/post_id via KVRocks lookup
    |
    +-- config.py             <-- All constants (TTLs, thresholds, ratios, schedules)
    |
    +-- job_logger.py         <-- Per-job logging stored in Redis for admin inspection
    |
    +-- utils/metrics_utils.py <-- Hourly metrics aggregation (latency percentiles, error rates)
    +-- utils/common_utils.py  <-- Shared utilities (Sentry error filtering, etc.)
```

### File Responsibilities

| File | Responsibility |
|------|---------------|
| `src/async_api_server.py` | FastAPI app. Defines all endpoints. Manages lifespan (startup: connect Redis, init mixer, start scheduler; shutdown: close connections). Registers background jobs with APScheduler. Contains Prometheus metrics middleware. |
| `src/async_main.py` | `AsyncRedisLayer` class. ALL Redis operations. Contains 5 Lua scripts for atomic operations. Implements refill logic for all feed types (popularity cascade, freshness cascade, fallback, following, UGC discovery). Manages bloom filters, cooldown sets, percentile pointers. |
| `src/async_mixer.py` | `AsyncVideoMixer` class. Implements the feed mixing algorithm. Decides how many videos come from each source (following, UGC, popularity, freshness). Handles position-based following placement and UGC interspersing. Triggers on-demand following sync. |
| `src/background_jobs.py` | All BigQuery-to-Redis sync jobs. Distributed locking via SET NX EX. Jobs: popularity pools, freshness windows, bloom filters, UGC pool, UGC discovery pool, exclude set, Google Chat metrics. |
| `src/config.py` | Every tunable constant. TTLs, thresholds, ratios, job schedules, Redis settings, BigQuery config, feature flags. Change system behavior here. |
| `src/metadata_handler.py` | `AsyncMetadataHandler`. Converts internal `video_id` to `canister_id` + `post_id` by looking up `offchain:metadata:video_details:{video_id}` hashes in KVRocks. These hashes are written by an external off-chain-agent process. |
| `src/utils/async_redis_utils.py` | `AsyncKVRocksService`. Connection pooling, standalone/cluster mode, TLS/mTLS support. Wraps `redis.asyncio`. Provides convenience methods for ZSET, bloom filter, and Lua script operations. |
| `src/utils/bigquery_client.py` | `BigQueryClient`. SQL queries for fetching popular videos, fresh videos, user watch history, UGC videos, UGC discovery eligible videos, followed-user content, tournament videos, and excluded (reported/NSFW) videos. All queries validate against `video_unique_v2` and respect the `ugc_content_approval` override filter. |

### Workers and Concurrency

Production runs **2 uvicorn workers**. Each worker:
- Has its own `AsyncKVRocksService` instance (own connection pool of 100 connections)
- Runs its own APScheduler instance (distributed locking prevents duplicate job execution)

---

## 3. Data Flow

### 3.1 Recommendation Request Lifecycle

```
GET /recommend/{user_id}?count=100&rec_type=mixed
    |
    v
[async_api_server.py: recommend endpoint]
    |
    +-- Is user fresh? (no bloom, no watched:short)
    |       YES -> bootstrap_fresh_user()
    |              (refill popularity pool with 1000 videos, freshness with 400)
    |
    +-- mixer.getMixedRecommendation(user_id, count=100)
            |
            +-- Step 0: _maybe_trigger_following_sync()
            |       Check if following pool < 10 videos AND last sync > 10 min ago
            |       If yes: fire-and-forget BigQuery sync
            |
            +-- Step 1: fetch_videos(user_id, "following", 30)
            |       Lua script: fetch_and_consume (atomic fetch + remove + cooldown)
            |       Python: filter excluded videos (SMISMEMBER against exclude set)
            |       If pool low: auto-refill is skipped (following requires BigQuery)
            |
            +-- Step 2: fetch_videos(user_id, "ugc", N)
            |       N = 30% of remaining slots after following
            |       Same Lua + exclude filter flow
            |       If pool low: auto-refill from UGC Discovery Pool (priority-based)
            |
            +-- Step 3: fetch_videos(user_id, "popularity", M)
            |       M = 60% of remaining slots after following+ugc
            |       Same Lua + exclude filter flow
            |       If pool low: auto-refill by sampling global popularity ZSETs
            |                    cascade through buckets 99_100 -> 90_99 -> ... -> 0_10
            |
            +-- Step 4: fetch_videos(user_id, "freshness", K)
            |       K = 40% of remaining slots after following+ugc
            |       If pool low: auto-refill by cascading l1d -> l7d -> ... -> l90d
            |
            +-- Step 5: Construct segments
            |       Segment 1 (positions 1-10): 3-5 following + pop/fresh fill
            |       Segment 2 (positions 11-50): remaining following + pop/fresh fill
            |       Shuffle within each segment
            |
            +-- Step 6: Intersperse UGC at even intervals
            |
            +-- Step 7: If still short, fetch from fallback pool
            |
            +-- Return MixerResult(videos, sources, insufficient_feeds, used_fallback)
    |
    v
[Convert video_ids to canister_id/post_id via metadata_handler]
    |
    v
JSON response to client
```

### 3.2 fetch_videos() Internal Flow (the core operation)

This is the most critical function. It lives in `async_main.py:AsyncRedisLayer.fetch_videos()` (line ~949).

```
fetch_videos(user_id, rec_type, count)
    |
    +-- Ensure bloom filter exists for user
    |
    +-- Execute fetch_and_consume Lua script (up to 3 times for exclude filtering)
    |       Lua atomically:
    |       1. Cleans expired entries from pool and watched set
    |       2. Iterates candidates from pool (score > now)
    |       3. For each: check watched:short -> check bloom -> if clean: add to result
    |       4. Removes sent videos from pool (ZREM)
    |       5. Adds sent videos to watched:short with cooldown (ZADD)
    |       Returns: [videos, removed_count, filtered_by_watched, filtered_by_bloom]
    |
    +-- Python: _filter_excluded_videos() via SMISMEMBER against {GLOBAL}:exclude:reported_nsfw
    |
    +-- If got enough: return
    |
    +-- If insufficient + auto_refill:
    |       Refill appropriate pool (popularity/freshness/ugc/fallback)
    |       Retry fetch
    |       Repeat up to 3 times
    |
    +-- If STILL insufficient: try fallback pool as last resort
    |
    +-- If rec_type == "ugc": increment push counts for served videos
    |
    +-- Return videos (trimmed to count)
```

### 3.3 Background Sync Lifecycle

```
[APScheduler triggers job every N hours/minutes]
    |
    v
acquire_job_lock(redis_client, job_name)
    |   Uses SET NX EX (only one worker runs the job)
    |   Lock key: job:lock:{job_name}
    |   Lock TTL: 1 hour (prevents deadlocks)
    |
    v
BigQueryClient().fetch_xxx()
    |   Executes SQL query with retry (3 attempts, exponential backoff)
    |   Returns pandas DataFrame
    |
    v
filter_videos_with_metadata(video_ids, kvrocks_client)
    |   Pipeline HGETALL on offchain:metadata:video_details:{vid}
    |   Drops videos without metadata (external off-chain-agent hasn't indexed them)
    |
    v
Write to Redis (clear + batch insert)
    |   Global pools: DELETE key, then ZADD in batches of 1000
    |   Score = expiry timestamp (now + TTL)
    |
    v
release_job_lock()
```

### 3.4 Refill Cascade (Popularity Example)

When a user's popularity pool drops below REFILL_THRESHOLD (200 videos):

```
refill_popularity(user_id)
    |
    +-- Acquire lock: {user:ID}:refill:lock:popularity
    |
    +-- Read percentile pointer: {user:ID}:pop_percentile_pointer
    |       Default: "99_100" (highest bucket)
    |       Resets every 24 hours
    |
    +-- For each bucket starting from pointer:
    |       ZRANDMEMBER from {GLOBAL}:pool:pop_{bucket}
    |       Filter expired (score <= now)
    |       Accumulate until 2x target candidates
    |
    +-- filter_and_add_videos Lua script:
    |       Check each candidate against watched:short + bloom
    |       Add unwatched to user pool with TTL score
    |       Cap pool at VIDEOS_TO_SHOW_CAPACITY (500)
    |
    +-- Update pointer to final bucket reached
    |
    +-- Release lock
```

---

## 4. API Reference

All endpoints defined in `src/async_api_server.py`.

| Method | Path | Auth | Purpose |
|--------|------|------|---------|
| GET | `/` | None | Service info + available endpoints list |
| GET | `/health` | None | Health check (Redis connectivity, scheduler status, uptime) |
| GET | `/metrics` | None | Prometheus metrics (for scraping) |
| GET | `/metric_summary` | None | 24-hour metrics summary with latency percentiles |
| GET | `/recommend/{user_id}` | None | Get personalized recommendations (returns video_ids). Query: `count`, `rec_type` |
| GET | `/recommend-with-metadata/{user_id}` | None | Recommendations with canister_id/post_id. Query: `count`, `rec_type` |
| GET | `/v2/recommend-with-metadata/{user_id}` | None | Recommendations with metadata + view counts. Query: `count`, `rec_type`, `profile` |
| GET | `/global-cache` | None | Random sample from pre-cached global pool. Query: `count`, `include_metadata` |
| GET | `/feed-stats/{user_id}` | None | Feed statistics per user (valid counts, refill status, sync status) |
| GET | `/status` | None | System status (Redis, scheduler, config, pool sizes) |
| GET | `/debug/user/{user_id}` | None | User state inspection (bloom, watched, per-feed stats) |
| GET | `/gchat-metrics-preview` | None | Preview Google Chat metrics message format |
| POST | `/tournament/register` | Admin | Register tournament, generate video set. Body: `tournament_id`, `video_count` |
| GET | `/tournament/{id}/videos` | None | Get tournament video set. Query: `with_metadata` |
| GET | `/tournament/overlap` | None | Check video overlap between two tournaments |
| GET | `/tournament/list` | Admin | List all tournaments with status |
| DELETE | `/tournament/{id}` | Admin | Delete tournament + clear cooldown |


---


## 5. Design Decisions

### Why Lua scripts instead of Python-level Redis calls?

**Atomicity.** The `fetch_and_consume` script must check if a video is watched, remove it from the pool, and add it to cooldown in a single atomic operation. Without Lua, a race condition exists: two concurrent requests could fetch the same video (ZRANGEBYSCORE returns it to both), and both would send it to different users before either removes it from the pool.

Lua scripts execute on the Redis server as a single atomic operation. No interleaving possible.

### Why expiry timestamps as sorted set scores (not engagement scores)?

**Individual video TTLs.** Redis `EXPIRE` only works at the key level (entire set). By using `score = now + TTL`, each individual video has its own expiry. `ZREMRANGEBYSCORE key -inf now` cleans up in O(log N + M) where M is expired entries. This also enables `ZRANGEBYSCORE key now +inf` to fetch only valid entries.

The alternative (separate TTL tracking) would require an additional data structure and more Redis round-trips.

### Why bloom filters instead of Redis SETs for watch history?

**Memory efficiency.** A Redis SET storing 100K video IDs per user would consume ~6.4MB per user. A bloom filter with 1% false positive rate and 100K entries uses ~120KB. At 100K users, that is 640GB vs 12GB.

The 1% false positive rate means ~1% of unwatched videos may be incorrectly filtered. This is acceptable: users have millions of videos available, losing 1% is invisible.

Bloom filters use `BF.RESERVE` with `EXPANSION=2` for auto-scaling. They have a 30-day sliding expiry (refreshed on every access) to auto-delete inactive users.

### Why the two-tier deduplication (bloom + watched:short)?

**Different TTLs for different guarantees.**

- `bloom:permanent` (30-day sliding): Prevents lifetime re-sends. Once a user watches a video, it never appears again (within bloom filter accuracy). The 30-day expiry is per-user inactivity -- active users have their bloom refreshed indefinitely.
- `watched:short` (24-hour fixed): Prevents rapid re-sends. A video sent to a user enters a 24-hour cooldown even if they haven't watched it yet. This covers the case where the bloom filter hasn't been synced yet (sync runs every 6 hours) and prevents the same video appearing in consecutive feed loads.

**Rapid iteration.** Every Python file with an `if __name__ == "__main__"` block has hardcoded test values that serve as both documentation and runnable examples. Changing a variable and running the file is faster than typing CLI arguments, and the hardcoded values show you what valid input looks like.
