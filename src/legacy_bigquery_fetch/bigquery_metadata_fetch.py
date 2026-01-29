"""
ARCHIVED LEGACY CODE - BigQuery Metadata Fetch

This file contains the legacy BigQuery-based metadata fetching code that was
replaced by KVRocks lookups. Preserved for reference in case business logic is needed.

Replacement: _fetch_from_kvrocks() in metadata_handler.py
"""

import os
import json
import logging
import time
from typing import Dict, List, Union
from google.cloud import bigquery
from google.oauth2 import service_account

logger = logging.getLogger(__name__)


def _init_bigquery():
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
    bq_client = bigquery.Client(
        credentials=credentials, project="hot-or-not-feed-intelligence"
    )

    # Default bot_uploaded_content table
    video_table = os.getenv(
        "VIDEO_TABLE", "hot-or-not-feed-intelligence.yral_ds.bot_uploaded_content"
    )
    # AI UGC table (fixed, not configurable via env)
    ai_ugc_table = "hot-or-not-feed-intelligence.yral_ds.ai_ugc"
    # Video unique table (required for all videos)
    video_unique_table = "hot-or-not-feed-intelligence.yral_ds.video_unique_v2"

    return bq_client, video_table, ai_ugc_table, video_unique_table


def _fetch_from_bigquery(
    bq_client,
    video_table: str,
    video_ids: List[str],
    metadata_cache: Dict,
    cache_timestamps: Dict,
    cache_ttl: int = 3600,
) -> Dict[str, Dict[str, Union[str, int]]]:
    """
    Fetch video metadata from BigQuery tables.

    ARCHIVED: This method has been replaced by _fetch_from_kvrocks() for production use.
    Kept for reference and potential rollback scenarios.

    Queries three tables with priority ordering:
        1. ai_ugc (highest priority)
        2. bot_uploaded_content (medium priority)
        3. ugc_content_approval (lowest priority, approved videos only)

    Args:
        bq_client: BigQuery client instance
        video_table: Default video table path
        video_ids: List of video IDs to fetch
        metadata_cache: Cache dictionary for metadata
        cache_timestamps: Cache timestamps dictionary
        cache_ttl: Cache TTL in seconds (default 3600)

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
        if video_id in metadata_cache:
            cache_time = cache_timestamps.get(video_id, 0)
            if current_time - cache_time < cache_ttl:
                results[video_id] = metadata_cache[video_id]
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
        FROM `{video_table}`
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
    INNER JOIN `hot-or-not-feed-intelligence.yral_ds.video_unique_v2` vu
        ON pd.video_id = vu.video_id
    WHERE pd.rn = 1
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("video_ids", "STRING", uncached_ids)
        ]
    )

    try:
        query_job = bq_client.query(query, job_config=job_config)
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
            metadata_cache[video_id] = metadata
            cache_timestamps[video_id] = current_time

    except Exception as e:
        logger.error(f"Failed to fetch from BigQuery: {e}")
        # Return partial results if any

    return results


if __name__ == "__main__":
    """
    This is archived code. Do not run directly.
    Use metadata_handler.py with KVRocks instead.
    """
    print("ARCHIVED: This code has been replaced by KVRocks-based metadata fetching.")
    print("See metadata_handler.py:_fetch_from_kvrocks() for current implementation.")
