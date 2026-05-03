# Feed Recsys System Migration Plan

## Scope

- Migrate the legacy curated feed flow into the active `src` codebase.
- Current API scope is only `recommend-with-metadata/{user_id}`.
- Do not carry `v2` into module names or internal code structure.
- Keep the legacy feed-supporting jobs, but move their logic into the active `src` architecture.
- Replace all BigQuery reads with ClickHouse reads.
- Keep KVRocks as the runtime state and cache layer.
- Add `from_ai_influencer` to every video in the final feed response.
- Keep the final response assembled on request, but reduce request-path latency with short-lived KVRocks count caching.

## Serving Strategy

- Treat the per-request cooldown state as `served_recent`, not true watched.
- Keep `served_recent` at `24h` to avoid showing the same video back to a user too soon.
- Keep the long-term dedupe layer in the bloom filter populated from historical watch data.
- Serve smaller batches by default and refill ahead of the next request instead of waiting for the pool to go empty.
- After serving a batch, check remaining pool size.
- If the pool is running low, trigger a background refill and return the current response immediately.
- Only block on refill when the current request cannot be satisfied from the existing pool.
- Cache view counts per video in KVRocks with a short TTL and only call the off-chain bulk stats API for cache misses.

## Design Rules

- Use the existing top-level `src/{routers,services,repository,jobs,schemas,utils,clients}` structure.
- Do not create one file per tiny helper.
- Small final-response helpers stay in the same service file that builds the response.
- Repository files only read/write storage.
- Service files own business rules and orchestration.
- Router files should stay thin.
- Job files should stay thin and call service methods.
- No BigQuery code should remain in the new feed recsys path.

## What Changes From My Earlier Draft

- `recommendation_service.py` is too vague. Replace it with `recommend_with_metadata_service.py`.
- Do not create `ai_influencer_tag_service.py`.
- Do not create `response_enricher.py`.
- Do not create a separate metadata repository file and a separate AI influencer repository file unless the code grows enough to justify it later.
- Keep small tagging logic in the same file that assembles final video metadata.

## Target File Layout

```text
src/
  clients/
    clickhouse_client.py

  routers/
    feed_recsys.py

  repository/
    clickhouse_feed_repository.py
    kv_feed_repository.py

  services/
    recommend_with_metadata_service.py
    feed_pool_service.py
    video_metadata_service.py
    feed_sync_service.py

  jobs/
    feed_recsys_jobs.py

  schemas/
    feed_recsys.py

  utils/
    feed_recsys_keys.py
```

## Why Each File Exists

### `src/clients/clickhouse_client.py`

Purpose:
- Own the ClickHouse connection setup and query execution wrapper.

Why this file is needed:
- ClickHouse access is an infrastructure concern, not business logic.
- The repository layer should not know how credentials, TLS, or retries are wired.

What goes here:
- client construction
- connection lifecycle
- basic query execution helpers

What does **not** go here:
- SQL for popularity, freshness, bloom, or following logic

### `src/routers/feed_recsys.py`

Purpose:
- Expose the feed recsys HTTP route for `recommend-with-metadata/{user_id}`.

Why this file is needed:
- Keeps FastAPI route handling separate from feed selection and metadata logic.

What goes here:
- route definitions
- request parameter parsing
- dependency injection
- HTTP error mapping

What does **not** go here:
- KVRocks key logic
- ClickHouse queries
- pool refill logic

### `src/repository/clickhouse_feed_repository.py`

Purpose:
- Own every ClickHouse read used by feed recsys jobs and on-demand feed refill paths.

Why this file is needed:
- All source-of-truth feed inputs come from ClickHouse now.
- One repository keeps the SQL surface easy to audit and test.

What goes here:
- methods that read from:
  - `video_unique_v2`
  - `global_popular_videos_l7d`
  - `ai_ugc`
  - `bot_uploaded_content`
  - `ugc_content_approval`
  - `user_video_relation`
  - `follower_graph`
  - `video_statistics`
  - `excluded_videos`

Suggested method names:
- `get_global_popular_videos()`
- `get_fresh_videos()`
- `get_user_watch_history()`
- `get_following_video_candidates()`
- `get_ugc_discovery_videos()`
- `get_excluded_video_ids()`
- `get_valid_video_ids()`

### `src/repository/kv_feed_repository.py`

Purpose:
- Own every KVRocks read/write for feed recsys runtime state.

Why this file is needed:
- The endpoint and the jobs both depend heavily on KVRocks.
- Putting all feed recsys keys and storage calls behind one repository avoids key-name duplication.

What goes here:
- user pools
- global pools
- watched-video state
- bloom filters
- following pool sync timestamps
- exclude sets
- AI influencer ID set
- KVRocks metadata hash reads for video IDs

Suggested method names:
- `get_user_pool()`
- `save_user_pool()`
- `get_global_pool()`
- `replace_global_pool()`
- `user_bloom_exists()`
- `replace_user_bloom()`
- `add_watched_videos()`
- `get_video_metadata_batch()`
- `replace_ai_influencer_ids()`
- `check_ai_influencer_ids()`
- `get_following_sync_time()`
- `set_following_sync_time()`

### `src/services/recommend_with_metadata_service.py`

Purpose:
- Orchestrate the endpoint flow from user request to final response.

Why this file is needed:
- This is the top-level use case for the current migration scope.
- It keeps the router thin and keeps the feed flow readable in one place.

What goes here:
- the main `recommend_with_metadata()` method
- user bootstrap checks
- pool read orchestration
- call into metadata assembly
- return final schema objects

What does **not** go here:
- raw SQL
- raw KVRocks commands
- job scheduling

### `src/services/feed_pool_service.py`

Purpose:
- Manage user feed pools and refill behavior.

Why this file is needed:
- Pool refill logic is large enough to deserve its own file.
- It is a separate concern from HTTP handling and final metadata shaping.

What goes here:
- choosing which pools to read
- checking whether a user needs bootstrap
- refilling from global or following sources
- filtering out already-watched items
- writing updated pool state back to KVRocks

What does **not** go here:
- final response schema building
- AI influencer flag decoration

### `src/services/video_metadata_service.py`

Purpose:
- Turn selected video IDs into the final response rows.

Why this file is needed:
- Metadata assembly is a real responsibility, but the AI influencer flagging is too small to justify its own file.

What goes here:
- batch KVRocks metadata lookup
- response row building
- the small helper that adds `from_ai_influencer`

Suggested internal helper name:
- `_attach_ai_influencer_flags()`

Why the AI influencer flag belongs here:
- It depends on `publisher_user_id`, which is already available at metadata assembly time.
- It should be evaluated late so it stays fresh when the influencer roster changes.
- It avoids duplicating AI influencer state inside every pool.

### `src/services/feed_sync_service.py`

Purpose:
- Hold the actual business logic for background jobs that populate KVRocks from ClickHouse and `chat-ai`.

Why this file is needed:
- Job logic is large enough that it should not live directly in the job entrypoint file.
- It centralizes all “build caches from source-of-truth data” behavior.

What goes here:
- global popularity sync
- freshness sync
- user bloom sync
- following pool sync
- UGC discovery sync
- exclude-set sync
- AI influencer ID sync

Suggested method names:
- `sync_global_popularity_pools()`
- `sync_fresh_pools()`
- `sync_user_bloom_filters()`
- `sync_user_following_pool(user_id)`
- `sync_ugc_discovery_pool()`
- `sync_excluded_videos()`
- `sync_ai_influencer_ids()`

### `src/jobs/feed_recsys_jobs.py`

Purpose:
- Expose scheduler-friendly async entrypoints.

Why this file is needed:
- The scheduler needs stable job functions.
- The file should stay thin and delegate to `feed_sync_service.py`.

What goes here:
- acquire lock
- call the correct `FeedSyncService` method
- log success/failure
- release lock

What does **not** go here:
- SQL
- feed ranking rules
- metadata transformation

### `src/schemas/feed_recsys.py`

Purpose:
- Define request and response schema models for the active feed recsys API.

Why this file is needed:
- The response shape is non-trivial and now includes `from_ai_influencer`.

What goes here:
- response models for videos
- top-level feed response model
- query parameter schema if needed

### `src/utils/feed_recsys_keys.py`

Purpose:
- Keep all feed recsys KVRocks key naming in one place.

Why this file is needed:
- The current `src/utils/kvrocks.py` is strongly influencer-feed-oriented.
- Feed recsys should not scatter literal key strings across repositories and services.

What goes here:
- namespaced key builders only

Suggested key helpers:
- `user_pool_key(user_id, pool_name)`
- `user_bloom_key(user_id)`
- `user_watched_key(user_id)`
- `global_pool_key(pool_name)`
- `excluded_videos_key()`
- `ai_influencer_ids_key()`
- `job_lock_key(job_name)`

## Existing Files To Update

### `src/core/settings.py`

Add:
- ClickHouse settings:
  - `clickhouse_host`
  - `clickhouse_port`
  - `clickhouse_database`
  - `clickhouse_username`
  - `clickhouse_password`
  - `clickhouse_secure`
  - `clickhouse_connect_timeout_sec`
  - `clickhouse_query_timeout_sec`
- feed recsys settings:
  - per-job intervals
  - feed limits
  - namespace/prefix settings if needed

Initial credential plan:
- use the current Airflow ClickHouse credentials first
- later swap to a dedicated read-only ClickHouse user without changing repository code

### `src/core/dependencies.py`

Add builders/getters for:
- `clickhouse_client`
- `clickhouse_feed_repository`
- `kv_feed_repository`
- `recommend_with_metadata_service`
- `feed_sync_service`

### `src/core/app.py`

Add:
- `feed_recsys` router registration
- feed recsys scheduler jobs

Also:
- keep scheduler entrypoints thin
- do not inline job logic into `app.py`

## API Request Flow

1. `feed_recsys.py` receives `recommend-with-metadata/{user_id}`.
2. `RecommendWithMetadataService` validates the request and asks `FeedPoolService` for video IDs.
3. `FeedPoolService`:
   - checks whether user state exists
   - bootstraps if required
   - reads user pools from KVRocks
   - refills if the active pool is short or stale
   - updates watched/shown state
4. `VideoMetadataService` reads KVRocks metadata hashes for the selected video IDs.
5. `VideoMetadataService` calls `_attach_ai_influencer_flags()` before returning rows.
6. Final response is returned through `schemas/feed_recsys.py`.

## Background Jobs To Keep

Keep these as active feed recsys jobs:
- popularity pool sync
- freshness pool sync
- user bloom sync
- following pool sync
- UGC discovery sync
- excluded video sync
- AI influencer ID sync

Compatibility note:
- `sync_ugc_pool` can be migrated if you want strict parity with legacy storage.
- The currently active UGC read path appears to use the discovery pool, so `sync_ugc_pool` should not drive the new endpoint unless we confirm a real consumer.

## Moderation And NSFW Plan

### Current legacy behavior

There are two separate moderation filters in the legacy system:

1. `ugc_content_approval.is_approved = FALSE` is treated as a hard rejection override in the source queries.
2. `excluded_videos` is synced into a KVRocks set and checked again at serve time for reported/NSFW removals.

What that means in practice:
- rejected videos are filtered while building popularity, freshness, following, bloom, and UGC candidate sources
- reported/NSFW videos are filtered again when the endpoint is about to return results

Legacy weakness:
- the serve-time exclude check is fail-open if the exclude set is missing or the lookup fails
- this is acceptable for availability, but weak for safety

### Active feed recsys behavior

The new active path should use defense in depth with three layers:

1. Query-time moderation filtering in ClickHouse.
2. Sync-time cache building into KVRocks using only already-filtered results.
3. Serve-time final exclude check before returning the response.

This is the target rule:
- a video must survive both `ugc_content_approval` rejection filtering and `excluded_videos` filtering to be eligible for the feed

### Layer 1: query-time filtering in ClickHouse

All feed-building queries in `clickhouse_feed_repository.py` should:
- exclude videos rejected in `ugc_content_approval`
- exclude videos present in `excluded_videos`
- validate the video against `video_unique_v2`

This keeps bad content out of newly built pools instead of relying only on request-time cleanup.

### Layer 2: sync-time filtered KVRocks pools

`feed_sync_service.py` should write only moderation-clean results into:
- global popularity pools
- freshness pools
- following pools
- UGC discovery pools
- user bloom inputs where applicable

Also:
- `sync_excluded_videos()` should still copy `excluded_videos` into KVRocks
- this remains useful for fast request-time checks and for catching newly excluded videos before the next full pool rebuild

### Layer 3: serve-time final gate

`feed_pool_service.py` should do one last membership check against the KVRocks excluded-video set before returning video IDs to `recommend_with_metadata_service.py`.

This catches:
- videos that entered a pool before they were later reported
- videos excluded after the last ClickHouse sync
- stale pool entries

### Failure mode in the new active path

Do **not** keep the legacy fail-open behavior as-is.

Recommended active behavior:
- if the final excluded-video membership check fails, drop the unverified batch and try to refill from another clean source
- if we still cannot verify enough videos, return a short feed or a retryable error instead of serving unverified content

Reason:
- this is a moderation boundary, so safety should win over perfect availability

### Data sources used for moderation

`ugc_content_approval`:
- approval override
- `is_approved = FALSE` means the video must not appear anywhere

`excluded_videos`:
- reported/NSFW/banned exclusion source
- this is the canonical source for the active exclude set

### Migration decision

The active feed recsys implementation will keep both moderation layers:
- approval-rejection filtering from `ugc_content_approval`
- reported/NSFW filtering from `excluded_videos`

But unlike legacy, it will apply `excluded_videos` twice:
- once while building pools in ClickHouse
- once again before serving the final response

## ClickHouse Source Mapping

### `video_unique_v2`

Use for:
- valid-video filtering
- dedup-safe source of truth

### `global_popular_videos_l7d`

Use for:
- popularity pools
- popularity percentile ordering

### `ai_ugc` and `bot_uploaded_content`

Use for:
- upload timestamps
- core video metadata needed during pool construction

### `ugc_content_approval`

Use for:
- approval override filtering
- removing rejected videos from all pools

### `user_video_relation`

Use for:
- recent watch history
- bloom/filter state rebuild

### `follower_graph`

Use for:
- following-based feed pool construction

### `video_statistics`

Use for:
- UGC discovery selection
- prioritizing low-impression videos

### `excluded_videos`

Use for:
- reported/NSFW exclusions
- global safety filter before writing pools

## KVRocks Data Plan

Suggested namespace pattern:
- `{namespace}:feed_recsys:*`
- user keys should use a per-user Redis hash slot tag
- global keys should use a shared `{GLOBAL}` hash slot tag

Suggested keys:
- `{namespace}:feed_recsys:{user:{user_id}}:pool:{pool_name}`
- `{namespace}:feed_recsys:{user:{user_id}}:watched`
- `{namespace}:feed_recsys:{user:{user_id}}:bloom`
- `{namespace}:feed_recsys:{user:{user_id}}:following:last_sync`
- `{namespace}:feed_recsys:{user:{user_id}}:pop_percentile_pointer`
- `{namespace}:feed_recsys:{GLOBAL}:pool:popular:{window}`
- `{namespace}:feed_recsys:{GLOBAL}:pool:fresh:{window}`
- `{namespace}:feed_recsys:{GLOBAL}:pool:ugc_discovery`
- `{namespace}:feed_recsys:{GLOBAL}:ugc_discovery:timestamps`
- `{namespace}:feed_recsys:{GLOBAL}:ugc_discovery:pushes`
- `{namespace}:feed_recsys:{GLOBAL}:exclude:videos`
- `{namespace}:feed_recsys:{GLOBAL}:lookup:ai_influencer_ids`
- `{namespace}:feed_recsys:{GLOBAL}:jobs:lock:{job_name}`

Metadata read source stays:
- `offchain:metadata:video_details:{video_id}`

## Where To Add `from_ai_influencer`

Add it in `src/services/video_metadata_service.py`, after metadata lookup and before final schema conversion.

Reason:
- `publisher_user_id` is available there.
- the check is cheap if AI influencer IDs are stored in one KVRocks set.
- the tag stays fresh without rebuilding feed pools.
- we avoid writing duplicate tag state into popularity, freshness, following, and UGC caches.

Implementation shape:
- fetch all `publisher_user_id` values for the current page
- batch-check them against `{namespace}:feed_recsys:{GLOBAL}:lookup:ai_influencer_ids`
- write `from_ai_influencer: true/false` into each final video object

## AI Influencer Sync Plan

Phase 1 recommendation:
- add `sync_ai_influencer_ids()` inside `feed_sync_service.py`
- fetch from `https://chat-ai.rishi.yral.com/api/v1/influencers`
- store the creator principal IDs in one KVRocks set

Why this is the safest first cut:
- the new feed recsys flow gets an explicit owner for this lookup set
- rollout does not depend on hidden assumptions inside the current influencer-feed pipeline

Later cleanup option:
- if both systems need the exact same canonical set, move the sync to a shared writer path after the migration is stable

## Implementation Sequence

### Phase 1: Foundation

1. Add ClickHouse settings to `src/core/settings.py`.
2. Add `clickhouse_client.py`.
3. Add `feed_recsys_keys.py`.
4. Add dependency wiring in `src/core/dependencies.py`.

### Phase 2: Storage Layer

1. Build `clickhouse_feed_repository.py`.
2. Build `kv_feed_repository.py`.
3. Write unit tests for both repositories.

### Phase 3: Background Sync

1. Build `feed_sync_service.py`.
2. Build `feed_recsys_jobs.py`.
3. Register jobs in `src/core/app.py`.
4. Run sync jobs into namespaced KVRocks keys first.

### Phase 4: API Path

1. Build `feed_pool_service.py`.
2. Build `video_metadata_service.py`.
3. Build `recommend_with_metadata_service.py`.
4. Add `schemas/feed_recsys.py`.
5. Add `routers/feed_recsys.py`.

### Phase 5: Validation And Cutover

1. Compare old and new feed outputs for the same users.
2. Check that excluded videos are removed in the new path.
3. Check that `from_ai_influencer` matches expected creators.
4. Keep view counts at `0` for parity with the current live legacy behavior.
5. Cut traffic to the new router after parity is acceptable.
6. Archive or remove the migrated legacy path only after cutover.

## Test Plan

Add:
- `tests/unit/test_clickhouse_feed_repository.py`
- `tests/unit/test_kv_feed_repository.py`
- `tests/unit/test_feed_pool_service.py`
- `tests/unit/test_video_metadata_service.py`
- `tests/unit/test_recommend_with_metadata_service.py`
- `tests/integration/test_feed_recsys_api.py`

Critical assertions:
- rejected videos never appear in the final feed
- invalid or duplicate videos are filtered out
- watched/bloom logic prevents obvious repeats
- following refill only uses allowed source videos
- `from_ai_influencer` is set correctly from `publisher_user_id`
- view counts remain `0`

## Decisions Locked For This Migration

- Only `recommend-with-metadata/{user_id}` is in scope for the new API path.
- No BigQuery reads in the new feed recsys implementation.
- KVRocks remains the serving and cache layer.
- `from_ai_influencer` is added late, in final metadata assembly.
- Small response decoration logic stays in `video_metadata_service.py`, not in a separate service file.
