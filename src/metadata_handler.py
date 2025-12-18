"""
Metadata handler for converting video_id to canister_id and post_id.

This module handles the conversion from video_id to the current canister_id
and post_id by:
1. Fetching the "previous" canister_id and post_id from BigQuery tables:
   - ai_ugc (highest priority)
   - bot_uploaded_content (medium priority)
   - ugc_content_approval (lowest priority, approved videos only)
2. Looking up the Redis mapping to get the "current" values
3. Returning the mapped values or keeping the previous values if no mapping exists

This is completely separate from the main recommendation Redis/Dragonfly
and uses a different Redis instance for metadata mappings.
"""

import os
import json
import logging
import time
from typing import Dict, List, Optional, Tuple, Union
import redis
from google.cloud import bigquery
from google.oauth2 import service_account
from config import STUBBED_CANISTER_ID

logger = logging.getLogger(__name__)


class MetadataHandler:
    """
    Handler for video metadata conversion and mapping.

    This class manages the conversion from video_id to canister_id/post_id pairs
    by querying BigQuery for the base metadata and applying Redis mappings for
    the current values.
    """

    def __init__(self):
        """
        Initialize metadata handler with BigQuery and Redis connections.

        Algorithm:
            1. Initialize BigQuery client using SERVICE_CRED
            2. Initialize Redis client for metadata mappings
            3. Initialize Redis impressions client for view counts (optional)
            4. Set up default configurations and caching
        """
        # Initialize BigQuery client
        self._init_bigquery()

        # Initialize Redis client for metadata mappings
        self._init_redis()

        # Initialize Redis impressions client (optional)
        self._init_redis_impressions()

        # Cache for video metadata to reduce BigQuery calls
        self._metadata_cache = {}
        self._cache_ttl = 3600  # 1 hour cache TTL
        self._cache_timestamps = {}

        logger.info("MetadataHandler initialized with BigQuery and Redis connections")

    def _init_bigquery(self):
        """
        Initialize BigQuery client for fetching video metadata.

        Algorithm:
            1. Read SERVICE_CRED from environment
            2. Parse JSON credentials
            3. Create service account credentials
            4. Initialize BigQuery client
            5. Set default table reference for video_index
        """
        service_cred = os.getenv("SERVICE_CRED")
        if not service_cred:
            raise ValueError("SERVICE_CRED environment variable not set")

        try:
            service_acc_creds = json.loads(service_cred)
            credentials = service_account.Credentials.from_service_account_info(
                service_acc_creds
            )
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid SERVICE_CRED JSON: {e}")
        except Exception as e:
            raise ValueError(f"Failed to create credentials from SERVICE_CRED: {e}")

        # Initialize BigQuery client
        self.bq_client = bigquery.Client(
            credentials=credentials, project="hot-or-not-feed-intelligence"
        )

        # Default bot_uploaded_content table
        self.video_table = os.getenv(
            "VIDEO_TABLE", "hot-or-not-feed-intelligence.yral_ds.bot_uploaded_content"
        )
        # AI UGC table (fixed, not configurable via env)
        self.ai_ugc_table = "hot-or-not-feed-intelligence.yral_ds.ai_ugc"
        # Video unique table (required for all videos)
        self.video_unique_table = "hot-or-not-feed-intelligence.yral_ds.video_unique"

        logger.info(f"BigQuery client initialized with table: {self.video_table}")
        logger.info(f"AI UGC table: {self.ai_ugc_table}")
        logger.info(f"Video unique table: {self.video_unique_table}")

    def _init_redis(self):
        """
        Initialize Redis client for metadata mappings.

        Algorithm:
            1. Read Redis connection details from environment
            2. Prefer METADATA_REDIS_HOST over fallbacks
            3. Create Redis client with auth
            4. Test connection with ping
            5. Set socket timeout for resilience

        Environment variables:
            - METADATA_REDIS_HOST: Primary host for metadata Redis
            - METADATA_REDIS_PORT: Port (default 6379)
            - METADATA_REDIS_AUTHKEY: Authentication key/password
        """
        # Get Redis connection details for metadata
        host = os.environ.get("METADATA_REDIS_HOST")
        if not host:
            raise RuntimeError(
                "METADATA_REDIS_HOST not set. This should point to the metadata Redis instance "
                "(different from your Dragonfly instance)"
            )

        port = int(os.environ.get("METADATA_REDIS_PORT", 6379))
        auth = os.environ.get("METADATA_REDIS_AUTHKEY")

        if not auth:
            raise RuntimeError(
                "METADATA_REDIS_AUTHKEY not set. This is required for metadata Redis connection"
            )

        # Create Redis client
        self.redis_client = redis.Redis(
            host=host,
            port=port,
            password=auth,
            decode_responses=True,  # Get strings back, not bytes
            socket_timeout=10,
            socket_connect_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
        )

        # Test connection
        try:
            self.redis_client.ping()
            logger.info(f"Connected to metadata Redis at {host}:{port}")
        except redis.ConnectionError as e:
            logger.error(f"Failed to connect to metadata Redis: {e}")
            raise

    def _init_redis_impressions(self):
        """
        Initialize Redis client for impressions/view counts.

        Algorithm:
            1. Read Redis connection details from environment
            2. Create Redis client with auth
            3. Test connection with ping
            4. Set socket timeout for resilience

        Environment variables:
            - REDIS_IMPRESSIONS_HOST: Host for impressions Redis
            - REDIS_IMPRESSIONS_PORT: Port (default 6379)
            - REDIS_IMPRESSIONS_AUTHKEY: Authentication key/password

        Note:
            This connection is optional. If not configured, view counts will be 0.
        """
        # Get Redis connection details for impressions
        host = os.environ.get("REDIS_IMPRESSIONS_HOST")
        if not host:
            logger.warning(
                "REDIS_IMPRESSIONS_HOST not set. View counts will not be available."
            )
            self.redis_impressions_client = None
            return

        port = int(os.environ.get("REDIS_IMPRESSIONS_PORT", 6379))
        auth = os.environ.get("REDIS_IMPRESSIONS_AUTHKEY")

        if not auth:
            logger.warning(
                "REDIS_IMPRESSIONS_AUTHKEY not set. View counts will not be available."
            )
            self.redis_impressions_client = None
            return

        # Check if TLS is enabled (default true for impressions.yral.com)
        tls_enabled = os.environ.get("REDIS_IMPRESSIONS_TLS_ENABLED", "true").lower() == "true"

        # Create Redis client
        self.redis_impressions_client = redis.Redis(
            host=host,
            port=port,
            password=auth,
            decode_responses=True,  # Get strings back, not bytes
            socket_timeout=10,
            socket_connect_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
            ssl=tls_enabled,
            ssl_cert_reqs=None if tls_enabled else None,
        )

        # Test connection
        try:
            self.redis_impressions_client.ping()
            logger.info(f"Connected to redis-impressions at {host}:{port}")
        except redis.ConnectionError as e:
            logger.warning(f"Failed to connect to redis-impressions: {e}")
            self.redis_impressions_client = None

    def _fetch_from_bigquery(
        self, video_ids: List[str]
    ) -> Dict[str, Dict[str, Union[str, int]]]:
        """
        Fetch video metadata from BigQuery tables.

        Queries three tables with priority ordering:
            1. ai_ugc (highest priority)
            2. bot_uploaded_content (medium priority)
            3. ugc_content_approval (lowest priority, approved videos only)

        Args:
            video_ids: List of video IDs to fetch

        Returns:
            Dictionary mapping video_id to {"canister_id": str, "post_id": int, "publisher_user_id": str}

        Algorithm:
            1. Check cache for existing metadata
            2. Query BigQuery for missing video IDs from all three tables
            3. Prioritize results (ai_ugc > bot_uploaded > ugc_content_approval)
            4. Parse results and extract canister_id, post_id, publisher_user_id
            5. Update cache with fresh data
            6. Return combined cached and fresh results
        """
        results = {}
        current_time = time.time()

        # Check cache first
        uncached_ids = []
        for video_id in video_ids:
            if video_id in self._metadata_cache:
                cache_time = self._cache_timestamps.get(video_id, 0)
                if current_time - cache_time < self._cache_ttl:
                    results[video_id] = self._metadata_cache[video_id]
                else:
                    uncached_ids.append(video_id)
            else:
                uncached_ids.append(video_id)

        if not uncached_ids:
            return results

        query = f"""
        WITH ai_ugc_data AS (
            -- Get AI UGC videos with proper field mapping (handle duplicates)
            SELECT DISTINCT
                video_id,
                FIRST_VALUE(upload_canister_id) OVER (
                    PARTITION BY video_id
                    ORDER BY upload_timestamp DESC
                ) as canister_id,
                FIRST_VALUE(post_id) OVER (
                    PARTITION BY video_id
                    ORDER BY upload_timestamp DESC
                ) as post_id,
                FIRST_VALUE(publisher_user_id) OVER (
                    PARTITION BY video_id
                    ORDER BY upload_timestamp DESC
                ) as publisher_user_id,
                1 as priority  -- Highest priority for AI UGC
            FROM `hot-or-not-feed-intelligence.yral_ds.ai_ugc`
            WHERE video_id IN UNNEST(@video_ids)
        ),
        bot_uploaded_data AS (
            -- Get bot uploaded content
            SELECT DISTINCT
                video_id,
                CASE
                    WHEN canister_id = '2vxsx-fae' THEN 'ivkka-7qaaa-aaaas-qbg3q-cai'
                    ELSE canister_id
                END as canister_id,
                post_id,
                publisher_user_id,
                2 as priority  -- Medium priority for bot uploaded
            FROM `{self.video_table}`
            WHERE video_id IN UNNEST(@video_ids)
        ),
        ugc_approval_data AS (
            -- Get UGC content approval data (approved videos only)
            SELECT DISTINCT
                video_id,
                FIRST_VALUE(canister_id) OVER (
                    PARTITION BY video_id
                    ORDER BY created_at DESC
                ) as canister_id,
                FIRST_VALUE(post_id) OVER (
                    PARTITION BY video_id
                    ORDER BY created_at DESC
                ) as post_id,
                FIRST_VALUE(user_id) OVER (
                    PARTITION BY video_id
                    ORDER BY created_at DESC
                ) as publisher_user_id,
                3 as priority  -- Lowest priority for ugc_content_approval
            FROM `hot-or-not-feed-intelligence.yral_ds.ugc_content_approval`
            WHERE video_id IN UNNEST(@video_ids)
              AND is_approved = TRUE
        ),
        combined_data AS (
            SELECT * FROM ai_ugc_data
            UNION ALL
            SELECT * FROM bot_uploaded_data
            UNION ALL
            SELECT * FROM ugc_approval_data
        ),
        prioritized_data AS (
            -- Select with priority: prefer AI UGC when video exists in both
            SELECT
                video_id,
                canister_id,
                post_id,
                publisher_user_id,
                ROW_NUMBER() OVER (PARTITION BY video_id ORDER BY priority) as rn
            FROM combined_data
        )
        -- Final selection: ensure video is in video_unique and return prioritized metadata
        SELECT
            pd.video_id,
            pd.canister_id,
            pd.post_id,
            pd.publisher_user_id
        FROM prioritized_data pd
        INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_unique` vu
            ON pd.video_id = vu.video_id
        WHERE pd.rn = 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ArrayQueryParameter("video_ids", "STRING", uncached_ids)
            ]
        )

        try:
            query_job = self.bq_client.query(query, job_config=job_config)
            rows = query_job.result()

            for row in rows:
                video_id = row.video_id
                metadata = {
                    "canister_id": row.canister_id,
                    "post_id": row.post_id,
                    "publisher_user_id": row.publisher_user_id,
                }
                results[video_id] = metadata

                # Update cache
                self._metadata_cache[video_id] = metadata
                self._cache_timestamps[video_id] = current_time

        except Exception as e:
            logger.error(f"Failed to fetch from BigQuery: {e}")
            # Return partial results if any

        return results

    def _apply_redis_mapping(
        self, old_canister_id: str, old_post_id: Union[str, int]
    ) -> Optional[Dict[str, str]]:
        """
        Apply Redis mapping to get current canister_id and post_id.

        Args:
            old_canister_id: Previous canister ID from BigQuery
            old_post_id: Previous post ID from BigQuery

        Returns:
            Dict with {"canister_id": str, "post_id": str} if mapping exists,
            None if no mapping found (keep previous values)

        Algorithm:
            1. Build composite key: "{old_canister_id}-{old_post_id}"
            2. HGETALL the key from Redis
            3. If hash exists with both fields, return mapped values
            4. If no mapping or incomplete, return None
            5. Handle connection errors gracefully
        """
        if not old_canister_id:
            return None

        # Build composite key
        key = f"{old_canister_id}-{old_post_id}"

        try:
            # Fetch mapping from Redis
            mapping = self.redis_client.hgetall(key)

            if not mapping:
                return None  # No mapping exists

            # Validate mapping has required fields
            canister_id = mapping.get("canister_id")
            post_id = mapping.get("post_id")

            if not canister_id or post_id is None:
                return None

            return {"canister_id": str(canister_id), "post_id": str(post_id)}

        except redis.RedisError as e:
            logger.warning(f"Redis mapping lookup failed for {key}: {e}")
            return None  # Treat as no mapping on error
        except Exception as e:
            logger.error(f"Unexpected error in Redis mapping: {e}")
            return None

    def _apply_redis_mappings_batch(
        self, mappings_to_check: List[Tuple[str, str, Union[str, int]]]
    ) -> Dict[str, Dict[str, str]]:
        """
        Batch apply Redis mappings using pipeline for all canister_id-post_id pairs.


        Args:
            mappings_to_check: List of (video_id, canister_id, post_id) tuples
                - video_id: str - the video identifier to map results back
                - canister_id: str - the canister ID from BigQuery
                - post_id: str or int - the post ID from BigQuery

        Returns:
            Dict[str, Dict[str, str]]: Mapping of video_id to {"canister_id": str, "post_id": str}
            Only includes entries where Redis mapping exists with valid data.

        Algorithm:
            1. Return empty if no mappings to check
            2. Build Redis keys as "{canister_id}-{post_id}" for each tuple
            3. Create Redis pipeline and queue all HGETALL commands
            4. Execute pipeline in single network round-trip
            5. Parse results, validate each has canister_id and post_id fields
            6. Return dict mapping video_id to validated mapping data
            7. Handle errors gracefully, return partial results on failure
        """
        if not mappings_to_check:
            return {}

        # Build all keys upfront: key format is "{canister_id}-{post_id}"
        keys = [f"{canister_id}-{post_id}" for _, canister_id, post_id in mappings_to_check]

        try:
            # Use pipeline for single network round-trip instead of N sequential calls
            pipe = self.redis_client.pipeline()
            for key in keys:
                pipe.hgetall(key)

            # Execute all HGETALL commands at once
            results_raw = pipe.execute()

            # Process results and map back to video_ids
            results = {}
            for i, (video_id, _, _) in enumerate(mappings_to_check):
                mapping = results_raw[i]
                # Validate mapping has required fields
                if mapping and mapping.get("canister_id") and mapping.get("post_id") is not None:
                    results[video_id] = {
                        "canister_id": str(mapping["canister_id"]),
                        "post_id": str(mapping["post_id"]),
                    }

            return results

        except redis.RedisError as e:
            logger.warning(f"Redis batch mapping lookup failed: {e}")
            return {}
        except Exception as e:
            logger.error(f"Unexpected error in Redis batch mapping: {e}")
            return {}

    def get_video_view_counts_from_redis_impressions(
        self, video_ids: List[str]
    ) -> Dict[str, tuple]:
        """
        Fetch view counts for video IDs from Redis impressions instance.

        Args:
            video_ids: List of video IDs to fetch view counts for

        Returns:
            Dictionary mapping video_id to (num_views_loggedin, num_views_all)

        Algorithm:
            1. Check if Redis impressions client is available
            2. Use pipeline for batch HGETALL operations
            3. Build pipeline with HGETALL for each video
            4. Execute pipeline in single round-trip
            5. Parse results and extract view counts
            6. Return dictionary with view counts
        """
        if not video_ids:
            return {}

        if not self.redis_impressions_client:
            logger.warning(
                "Redis-impressions client not available, returning empty view counts"
            )
            return {}

        try:
            # Use pipeline for batch operations
            pipe = self.redis_impressions_client.pipeline()

            # Build pipeline with HGETALL for each video
            video_hash_keys = []
            for video_id in video_ids:
                video_hash_key = f"rewards:video:{video_id}"
                video_hash_keys.append(video_hash_key)
                pipe.hgetall(video_hash_key)

            # Execute pipeline - all commands in single round-trip
            results = pipe.execute()

            # Parse results
            view_counts = {}
            for i, video_id in enumerate(video_ids):
                if i < len(results) and results[i]:
                    data = results[i]
                    num_views_loggedin = int(data.get("total_count_loggedin", 0))
                    num_views_all = int(data.get("total_count_all", 0))
                    view_counts[video_id] = (num_views_loggedin, num_views_all)
                    logger.debug(
                        f"View counts for {video_id}: loggedin={num_views_loggedin}, all={num_views_all}"
                    )

            logger.info(
                f"Retrieved view counts for {len(view_counts)}/{len(video_ids)} video IDs from redis-impressions"
            )
            return view_counts

        except Exception as e:
            logger.error(f"Error fetching view counts from redis-impressions: {e}")
            return {}

    def convert_video_id_to_canister_post(
        self, video_id: str
    ) -> Optional[Dict[str, Union[str, int]]]:
        """
        Convert a single video_id to current canister_id and post_id.

        Args:
            video_id: The video ID to convert

        Returns:
            Dictionary with {"video_id": str, "canister_id": str, "post_id": int, "publisher_user_id": str}
            or None if video not found

        Algorithm:
            1. Fetch metadata from BigQuery (with caching)
            2. Get previous canister_id, post_id, and publisher_user_id
            3. Apply Redis mapping to get current canister_id/post_id values
            4. Return mapped values or previous if no mapping
            5. publisher_user_id always comes from BigQuery (no mapping needed)
        """
        # Fetch from BigQuery
        metadata = self._fetch_from_bigquery([video_id])

        if video_id not in metadata:
            logger.warning(f"Video {video_id} not found in BigQuery")
            return None

        prev_metadata = metadata[video_id]
        prev_canister_id = prev_metadata["canister_id"]
        prev_post_id = prev_metadata["post_id"]

        # Apply Redis mapping
        mapping = self._apply_redis_mapping(prev_canister_id, prev_post_id)

        # publisher_user_id always comes from BigQuery (doesn't need mapping)
        publisher_user_id = str(prev_metadata.get("publisher_user_id", ""))

        if mapping:
            # Use mapped (current) values for canister_id and post_id
            return {
                "video_id": video_id,
                "canister_id": mapping["canister_id"],
                "post_id": str(mapping["post_id"]),
                "publisher_user_id": publisher_user_id,
            }
        else:
            # No mapping, use previous values
            return {
                "video_id": video_id,
                "canister_id": str(prev_canister_id),
                "post_id": str(prev_post_id),
                "publisher_user_id": publisher_user_id,
            }

    def batch_convert_video_ids(
        self, video_ids: List[str]
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Convert multiple video_ids to canister_id and post_id pairs.

        Args:
            video_ids: List of video IDs to convert

        Returns:
            List of dictionaries with video metadata including current
            canister_id, post_id, and publisher_user_id values

        Algorithm:
            1. Batch fetch from BigQuery for efficiency
            2. Apply Redis mappings for canister_id/post_id pairs
            3. Combine results maintaining order
            4. Skip videos not found in BigQuery
            5. publisher_user_id always comes from BigQuery (no mapping needed)
            6. Return list of converted metadata
        """
        if not video_ids:
            return []

        # Batch fetch from BigQuery
        all_metadata = self._fetch_from_bigquery(video_ids)

        # Prepare batch lookup data: (video_id, canister_id, post_id) for videos found in BQ
        mappings_to_check = [
            (video_id, all_metadata[video_id]["canister_id"], all_metadata[video_id]["post_id"])
            for video_id in video_ids
            if video_id in all_metadata
        ]

        # Single pipelined call for all Redis mappings (instead of N sequential calls)
        batch_mappings = self._apply_redis_mappings_batch(mappings_to_check)

        # Build results using batch mappings
        results = []
        for video_id in video_ids:
            if video_id not in all_metadata:
                logger.warning(f"Video {video_id} not found in BigQuery")
                continue

            prev_metadata = all_metadata[video_id]
            prev_canister_id = prev_metadata["canister_id"]
            prev_post_id = prev_metadata["post_id"]

            # Get mapping from batch results (or None if not found)
            mapping = batch_mappings.get(video_id)

            # publisher_user_id always comes from BigQuery (doesn't need mapping)
            publisher_user_id = str(prev_metadata.get("publisher_user_id", ""))

            if mapping:
                # Use mapped (current) values for canister_id and post_id
                results.append(
                    {
                        "video_id": video_id,
                        "canister_id": STUBBED_CANISTER_ID,
                        "post_id": str(mapping["post_id"]),
                        "publisher_user_id": publisher_user_id,
                    }
                )
            else:
                # No mapping, use previous values
                results.append(
                    {
                        "video_id": video_id,
                        "canister_id": STUBBED_CANISTER_ID,
                        "post_id": str(prev_post_id),
                        "publisher_user_id": publisher_user_id,
                    }
                )

        return results

    def batch_convert_video_ids_v2(
        self, video_ids: List[str]
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Convert multiple video_ids to canister_id, post_id, and view counts.

        This is the V2 version that includes num_views_loggedin and num_views_all
        fields from redis-impressions.

        Args:
            video_ids: List of video IDs to convert

        Returns:
            List of dictionaries with video metadata including current
            canister_id, post_id, publisher_user_id, num_views_loggedin, and num_views_all

        Algorithm:
            1. Batch fetch metadata from BigQuery (v1 data)
            2. Apply Redis mappings for canister_id/post_id pairs
            3. Fetch view counts from redis-impressions
            4. Combine all data into results
            5. Skip videos not found in BigQuery
            6. Return list of converted metadata with view counts
        """
        if not video_ids:
            return []

        # Get v1 metadata (canister_id, post_id, publisher_user_id)
        v1_results = self.batch_convert_video_ids(video_ids)

        # If no v1 results, return empty
        if not v1_results:
            return []

        # Extract video IDs that were successfully converted
        successful_video_ids = [r["video_id"] for r in v1_results]

        # Fetch view counts from redis-impressions
        # muting view counts: frontend to fetch this async
        # view_counts = self.get_video_view_counts_from_redis_impressions(
        #     successful_video_ids
        # )
        view_counts = {}

        # Combine v1 data with view counts
        results = []
        for v1_data in v1_results:
            video_id = v1_data["video_id"]

            # Get view counts (default to 0 if not found)
            # num_views_loggedin, num_views_all = view_counts.get(video_id, (0, 0))

            # Build v2 result (canister_id already stubbed from v1)
            v2_data = {
                "video_id": video_id,
                "canister_id": v1_data["canister_id"],
                "post_id": v1_data["post_id"],
                "publisher_user_id": v1_data["publisher_user_id"],
                "num_views_loggedin": 0,
                "num_views_all": 0
            }
            results.append(v2_data)

        return results

    def test_connection(self) -> Dict[str, bool]:
        """
        Test connections to BigQuery and Redis.

        Returns:
            Dictionary with connection status for each service

        Algorithm:
            1. Test BigQuery with simple query
            2. Test Redis with ping
            3. Return status for both connections
        """
        status = {"bigquery": False, "redis": False}

        # Test BigQuery
        try:
            query = f"SELECT 1 as test FROM `{self.video_table}` LIMIT 1"
            list(self.bq_client.query(query).result())
            status["bigquery"] = True
        except Exception as e:
            logger.error(f"BigQuery connection test failed: {e}")

        # Test Redis
        try:
            self.redis_client.ping()
            status["redis"] = True
        except Exception as e:
            logger.error(f"Redis connection test failed: {e}")

        return status


# Singleton instance for easy import
_handler = None


def get_metadata_handler() -> MetadataHandler:
    """
    Get or create singleton metadata handler.

    Returns:
        MetadataHandler instance

    Algorithm:
        1. Check if handler already exists
        2. If not, create new handler
        3. Return singleton instance
    """
    global _handler
    if _handler is None:
        _handler = MetadataHandler()
    return _handler


def convert_video_id_to_canister_post(
    video_id: str,
) -> Optional[Dict[str, Union[str, int]]]:
    """
    Convenience function to convert a single video_id.

    Args:
        video_id: The video ID to convert

    Returns:
        Dictionary with video_id, canister_id, post_id, and publisher_user_id

    Algorithm:
        1. Get singleton handler
        2. Call conversion method
        3. Return result

    Example:
        >>> result = convert_video_id_to_canister_post("video_12345")
        >>> if result:
        ...     print(f"Canister: {result['canister_id']}, Post: {result['post_id']}, Publisher: {result['publisher_user_id']}")
    """
    handler = get_metadata_handler()
    return handler.convert_video_id_to_canister_post(video_id)


def batch_convert_video_ids(video_ids: List[str]) -> List[Dict[str, Union[str, int]]]:
    """
    Convenience function to convert multiple video_ids.

    Args:
        video_ids: List of video IDs to convert

    Returns:
        List of dictionaries with metadata (video_id, canister_id, post_id, publisher_user_id)

    Algorithm:
        1. Get singleton handler
        2. Call batch conversion method
        3. Return results

    Example:
        >>> videos = ["video_123", "video_456", "video_789"]
        >>> results = batch_convert_video_ids(videos)
        >>> for r in results:
        ...     print(f"{r['video_id']} -> {r['canister_id']}-{r['post_id']} by {r['publisher_user_id']}")
    """
    handler = get_metadata_handler()
    return handler.batch_convert_video_ids(video_ids)


def batch_convert_video_ids_v2(
    video_ids: List[str],
) -> List[Dict[str, Union[str, int]]]:
    """
    Convenience function to convert multiple video_ids with view counts (V2).

    This version includes num_views_loggedin and num_views_all fields.

    Args:
        video_ids: List of video IDs to convert

    Returns:
        List of dictionaries with metadata including view counts

    Algorithm:
        1. Get singleton handler
        2. Call batch conversion v2 method
        3. Return results with view counts

    Example:
        >>> videos = ["video_123", "video_456", "video_789"]
        >>> results = batch_convert_video_ids_v2(videos)
        >>> for r in results:
        ...     print(f"{r['video_id']}: views_loggedin={r['num_views_loggedin']}, views_all={r['num_views_all']}")
    """
    handler = get_metadata_handler()
    return handler.batch_convert_video_ids_v2(video_ids)


if __name__ == "__main__":
    """
    Test the metadata handler functionality.
    """
    logging.basicConfig(level=logging.INFO)

    # Test initialization
    print("\n=== Testing MetadataHandler ===")

    try:
        handler = MetadataHandler()
        print("✓ Handler initialized successfully")

        # Test connections
        print("\n=== Testing Connections ===")
        status = handler.test_connection()
        print(f"BigQuery: {'✓' if status['bigquery'] else '✗'}")
        print(f"Redis: {'✓' if status['redis'] else '✗'}")

        # Test single video conversion
        print("\n=== Testing Single Video Conversion ===")
        test_video = "video_000001"  # Replace with actual video_id
        result = handler.convert_video_id_to_canister_post(test_video)

        if result:
            print(f"Video: {result['video_id']}")
            print(f"Canister ID: {result['canister_id']}")
            print(f"Post ID: {result['post_id']}")
            print(f"Publisher User ID: {result['publisher_user_id']}")
        else:
            print(f"No metadata found for {test_video}")

        # Test batch conversion
        print("\n=== Testing Batch Conversion ===")
        test_videos = ["video_000001", "video_000002", "video_000003"]
        results = handler.batch_convert_video_ids(test_videos)

        print(f"Converted {len(results)} out of {len(test_videos)} videos")
        for r in results[:3]:  # Show first 3
            print(
                f"  {r['video_id']} -> {r['canister_id']}-{r['post_id']} (publisher: {r['publisher_user_id']})"
            )

    except Exception as e:
        print(f"✗ Error: {e}")
        import traceback

        traceback.print_exc()
