"""
BigQuery client for fetching recommendation data.

This module provides a client for querying BigQuery tables to fetch:
1. Popular videos with DS scores for percentile bucketing
2. Fresh videos bucketed by upload time windows
3. User watch history from the last 12 hours

Uses SERVICE_CRED environment variable for authentication with the
hot-or-not-feed-intelligence project.
"""

import os
import json
import logging
import time
from typing import List, Dict, Optional, Generator, Tuple
from datetime import datetime, timedelta

import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger(__name__)


class BigQueryClient:
    """
    Client for fetching recommendation data from BigQuery.

    This class handles authentication using SERVICE_CRED environment variable
    and provides methods to fetch popular videos, fresh videos, and user
    watch history with proper error handling and retry logic.
    """

    def __init__(self, project_id: str = "hot-or-not-feed-intelligence", dataset: str = "yral_ds"):
        """
        Initialize BigQuery client with service account credentials.

        Args:
            project_id: GCP project ID
            dataset: BigQuery dataset name

        Algorithm:
            1. Parse SERVICE_CRED from environment
            2. Create service account credentials
            3. Initialize BigQuery client
            4. Set default query job config with timeout
        """
        self.project_id = project_id
        self.dataset = dataset

        # Parse service account credentials from environment
        service_cred = os.getenv("SERVICE_CRED")
        if not service_cred:
            raise ValueError("SERVICE_CRED environment variable not set")

        try:
            service_acc_creds = json.loads(service_cred)
            credentials = service_account.Credentials.from_service_account_info(service_acc_creds)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid SERVICE_CRED JSON: {e}")
        except Exception as e:
            raise ValueError(f"Failed to create credentials from SERVICE_CRED: {e}")

        # Initialize BigQuery client
        self.client = bigquery.Client(credentials=credentials, project=project_id)

        # Set default query job config
        self.default_job_config = bigquery.QueryJobConfig(
            use_query_cache=True,
            query_parameters=[],
            maximum_bytes_billed=10 * 1024 * 1024 * 1024  # 10GB limit
        )

        logger.info(f"BigQueryClient initialized for project {project_id}, dataset {dataset}")

    def fetch_popular_videos(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch popular videos with DS scores for percentile bucketing.

        Args:
            limit: Optional limit on number of records to fetch

        Returns:
            DataFrame with columns: video_id, global_popularity_score

        Algorithm:
            1. Start with video_unique as source of truth
            2. Ensure video exists in at least one source:
               - ai_ugc
               - bot_uploaded_content
               - ugc_content_approval (is_approved = TRUE)
            3. ugc_content_approval acts as OVERRIDE filter:
               - If is_approved = FALSE, video is excluded from ALL sources
               - Videos in ai_ugc/bot_uploaded but rejected in ugc_content_approval are invalid
            4. Join with global_popular_videos_l7d for popularity scores
            5. Return video_id and popularity score
            6. Apply limit if specified
        """
        query = f"""
        WITH valid_videos AS (
            -- Start with video_unique as source of truth
            -- ugc_content_approval acts as override: is_approved=FALSE excludes video from ALL sources
            SELECT DISTINCT vu.video_id
            FROM `{self.project_id}.{self.dataset}.video_unique` vu
            LEFT JOIN `{self.project_id}.{self.dataset}.ai_ugc` aug
                ON vu.video_id = aug.video_id
            LEFT JOIN `{self.project_id}.{self.dataset}.bot_uploaded_content` buc
                ON vu.video_id = buc.video_id
            LEFT JOIN `{self.project_id}.{self.dataset}.ugc_content_approval` uca
                ON vu.video_id = uca.video_id
            WHERE
                -- Must be in at least one valid source
                (aug.video_id IS NOT NULL
                 OR buc.video_id IS NOT NULL
                 OR (uca.video_id IS NOT NULL AND uca.is_approved = TRUE))
                -- AND must NOT be rejected in ugc_content_approval
                AND (uca.video_id IS NULL OR uca.is_approved IS NOT FALSE)
        )
        SELECT DISTINCT
            gpv.video_id,
            gpv.global_popularity_score
        FROM `{self.project_id}.{self.dataset}.global_popular_videos_l7d` gpv
        INNER JOIN valid_videos vv
            ON gpv.video_id = vv.video_id
        WHERE gpv.global_popularity_score IS NOT NULL
        ORDER BY gpv.global_popularity_score DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        logger.info(f"Fetching popular videos from BigQuery (limit={limit})")
        return self._execute_query_with_retry(query)

    def fetch_fresh_videos(self) -> pd.DataFrame:
        """
        Fetch fresh videos bucketed by upload time windows.

        Returns:
            DataFrame with columns: video_id, bucket (l1d/l7d/l14d/l30d/l90d)

        Algorithm:
            1. Get timestamps from content sources (deduplicated by video_id):
               - ai_ugc: upload_timestamp
               - bot_uploaded_content: timestamp
               - ugc_content_approval (is_approved = TRUE): created_at
            2. ugc_content_approval acts as OVERRIDE filter:
               - If is_approved = FALSE, video is excluded from ALL sources
               - Videos in ai_ugc/bot_uploaded but rejected in ugc_content_approval are invalid
            3. Join with video_unique to ensure video exists there
            4. Calculate time boundaries for each window
            5. Bucket videos based on age:
               - l1d: last 1 day
               - l7d: 1-7 days ago
               - l14d: 7-14 days ago
               - l30d: 14-30 days ago
               - l90d: 30-90 days ago
            6. Return only videos from last 90 days
        """
        query = f"""
        WITH video_timestamps AS (
            -- Get timestamps from AI UGC (deduplicated by video_id)
            SELECT
                video_id,
                MAX(upload_timestamp) AS ts,
                'ai_ugc' as source
            FROM `{self.project_id}.{self.dataset}.ai_ugc`
            WHERE upload_timestamp IS NOT NULL
            GROUP BY video_id

            UNION ALL

            -- Get timestamps from bot uploaded content
            SELECT
                video_id,
                MAX(`timestamp`) AS ts,
                'bot_uploaded' as source
            FROM `{self.project_id}.{self.dataset}.bot_uploaded_content`
            WHERE `timestamp` IS NOT NULL
            GROUP BY video_id

            UNION ALL

            -- Get timestamps from ugc_content_approval (approved only)
            SELECT
                video_id,
                MAX(created_at) AS ts,
                'ugc_approved' as source
            FROM `{self.project_id}.{self.dataset}.ugc_content_approval`
            WHERE is_approved = TRUE AND created_at IS NOT NULL
            GROUP BY video_id
        ),
        rejected_videos AS (
            -- Videos explicitly rejected in ugc_content_approval (excludes from ALL sources)
            SELECT DISTINCT video_id
            FROM `{self.project_id}.{self.dataset}.ugc_content_approval`
            WHERE is_approved = FALSE
        ),
        per_video AS (
            -- Get most recent timestamp for each video, ensuring it's in video_unique
            -- and NOT rejected in ugc_content_approval
            SELECT
                vu.video_id,
                MAX(vt.ts) as ts
            FROM `{self.project_id}.{self.dataset}.video_unique` vu
            INNER JOIN video_timestamps vt
                ON vu.video_id = vt.video_id
            LEFT JOIN rejected_videos rv
                ON vu.video_id = rv.video_id
            WHERE rv.video_id IS NULL  -- Exclude rejected videos
            GROUP BY vu.video_id
        ),
        bounds AS (
            SELECT
                MAX(ts) AS max_ts,
                MAX(ts) - INTERVAL 1 DAY AS c1,
                MAX(ts) - INTERVAL 7 DAY AS c7,
                MAX(ts) - INTERVAL 14 DAY AS c14,
                MAX(ts) - INTERVAL 30 DAY AS c30,
                MAX(ts) - INTERVAL 90 DAY AS c90
            FROM per_video
        )
        SELECT
            p.video_id,
            CASE
                WHEN p.ts > b.c1  AND p.ts <= b.max_ts THEN 'l1d'
                WHEN p.ts > b.c7  AND p.ts <= b.c1 THEN 'l7d'
                WHEN p.ts > b.c14 AND p.ts <= b.c7 THEN 'l14d'
                WHEN p.ts > b.c30 AND p.ts <= b.c14 THEN 'l30d'
                WHEN p.ts > b.c90 AND p.ts <= b.c30 THEN 'l90d'
                ELSE NULL
            END AS bucket
        FROM per_video p
        CROSS JOIN bounds b
        WHERE p.ts > b.c90
            AND p.ts <= b.max_ts
        ORDER BY bucket, video_id
        """

        logger.info("Fetching fresh videos from BigQuery with window bucketing")
        return self._execute_query_with_retry(query)

    def fetch_user_watch_history(self, hours_back: int = 12) -> pd.DataFrame:
        """
        Fetch user watch history from the last N hours.

        Args:
            hours_back: Number of hours to look back (default 12)

        Returns:
            DataFrame with columns: user_id, video_id

        Algorithm:
            1. Get all valid videos from video_unique that exist in at least one source:
               - ai_ugc
               - bot_uploaded_content
               - ugc_content_approval (is_approved = TRUE)
            2. ugc_content_approval acts as OVERRIDE filter:
               - If is_approved = FALSE, video is excluded from ALL sources
               - Videos in ai_ugc/bot_uploaded but rejected in ugc_content_approval are invalid
            3. Join with userVideoRelation table
            4. Get max timestamp from userVideoRelation
            5. Filter for videos watched in last N hours
            6. Return distinct user-video pairs
            7. Exclude null timestamps
        """
        query = f"""
        WITH valid_videos AS (
            -- Get all valid videos from video_unique
            -- ugc_content_approval acts as override: is_approved=FALSE excludes video from ALL sources
            SELECT DISTINCT vu.video_id
            FROM `{self.project_id}.{self.dataset}.video_unique` vu
            LEFT JOIN `{self.project_id}.{self.dataset}.ai_ugc` aug
                ON vu.video_id = aug.video_id
            LEFT JOIN `{self.project_id}.{self.dataset}.bot_uploaded_content` buc
                ON vu.video_id = buc.video_id
            LEFT JOIN `{self.project_id}.{self.dataset}.ugc_content_approval` uca
                ON vu.video_id = uca.video_id
            WHERE
                -- Must be in at least one valid source
                (aug.video_id IS NOT NULL
                 OR buc.video_id IS NOT NULL
                 OR (uca.video_id IS NOT NULL AND uca.is_approved = TRUE))
                -- AND must NOT be rejected in ugc_content_approval
                AND (uca.video_id IS NULL OR uca.is_approved IS NOT FALSE)
        ),
        t AS (
            SELECT
                uvr.user_id,
                uvr.video_id,
                uvr.last_watched_timestamp,
                MAX(uvr.last_watched_timestamp) OVER () AS max_ts
            FROM `{self.project_id}.{self.dataset}.userVideoRelation` uvr
            INNER JOIN valid_videos vv
                ON uvr.video_id = vv.video_id
        )
        SELECT DISTINCT
            user_id,
            video_id
        FROM t
        WHERE last_watched_timestamp >= TIMESTAMP_SUB(max_ts, INTERVAL {hours_back} HOUR)
            AND last_watched_timestamp IS NOT NULL
        """

        logger.info(f"Fetching user watch history from last {hours_back} hours")
        return self._execute_query_with_retry(query)

    def fetch_ugc_videos(self, limit: Optional[int] = None) -> pd.DataFrame:
        """
        Fetch UGC (User-Generated Content) videos ordered by freshness.

        This function returns videos from ai_ugc table only (for 5% UGC bias).
        Note: ugc_content_approval is still a valid source for freshness/popularity
        pools, but not for the UGC bias feature.

        Algorithm:
            1. Get UGC videos from ai_ugc table with upload_timestamp
            2. Validate against video_unique table (source of truth)
            3. Exclude any videos rejected in ugc_content_approval (OVERRIDE filter)
            4. Filter to videos from last 90 days
            5. Order by upload_timestamp DESC (freshest first)
            6. Apply limit if specified

        Args:
            limit: Optional limit on number of records (default: None for all)

        Returns:
            DataFrame with columns:
                - video_id (STRING): The video identifier
                - created_at (TIMESTAMP): Video creation timestamp

        Example:
            >>> bq_client = BigQueryClient()
            >>> df = bq_client.fetch_ugc_videos(limit=1000)
            >>> print(df.head())
               video_id                  created_at
            0  vid_001   2024-12-04 10:30:00+00:00
            1  vid_002   2024-12-04 09:15:00+00:00
        """
        query = f"""
        WITH rejected_videos AS (
            -- Videos explicitly rejected in ugc_content_approval (OVERRIDE filter)
            -- Even ai_ugc videos are excluded if rejected
            SELECT DISTINCT video_id
            FROM `{self.project_id}.{self.dataset}.ugc_content_approval`
            WHERE is_approved = FALSE
        )
        SELECT DISTINCT
            aug.video_id,
            aug.upload_timestamp AS created_at
        FROM `{self.project_id}.{self.dataset}.ai_ugc` aug
        INNER JOIN `{self.project_id}.{self.dataset}.video_unique` vu
            ON aug.video_id = vu.video_id
        LEFT JOIN rejected_videos rv
            ON aug.video_id = rv.video_id
        WHERE aug.upload_timestamp IS NOT NULL
          AND aug.publisher_user_id IS NOT NULL
          AND rv.video_id IS NULL  -- Exclude rejected videos
          AND aug.upload_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 90 DAY)
        ORDER BY aug.upload_timestamp DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        logger.info(f"Fetching UGC videos from BigQuery (limit={limit})")
        return self._execute_query_with_retry(query)

    def fetch_followed_users_content(
        self,
        user_id: str,
        num_videos: int = 1000
    ) -> pd.DataFrame:
        """
        Fetch popular and fresh content from users that a given user follows.

        This function returns videos uploaded by followed users, prioritizing
        popular content first and filling the remainder with fresh content
        (last 30 days).

        Algorithm:
            1. Get all active users that the given user follows from follower_graph
            2. Get all content from followed users across sources:
               - bot_uploaded_content (publisher_user_id)
               - ai_ugc (publisher_user_id)
               - ugc_content_approval (user_id, is_approved = TRUE)
            3. Validate against video_unique table (source of truth)
            4. ugc_content_approval acts as OVERRIDE filter:
               - If is_approved = FALSE, video is excluded from ALL sources
               - Videos in ai_ugc/bot_uploaded but rejected in ugc_content_approval are invalid
            5. Split into popular (has score in global_popular_videos_l7d) and
               fresh (last 30 days, excluding videos already in popular)
            6. Order popular by score DESC, fresh by upload_time DESC
            7. Return popular first, fill remainder with fresh up to num_videos

        Args:
            user_id: The user whose followed users' content we want
            num_videos: Total number of videos to return (default 1000)

        Returns:
            DataFrame with columns:
                - video_id (STRING): The video identifier
                - global_popularity_score (FLOAT64): Popularity score, NULL if fresh-only

        Example:
            >>> bq_client = BigQueryClient()
            >>> df = bq_client.fetch_followed_users_content("user_123", num_videos=100)
            >>> print(df.head())
               video_id  global_popularity_score
            0  vid_001                     0.95
            1  vid_002                     0.87
            2  vid_003                     None  # Fresh video, no popularity score
        """
        query = f"""
        WITH followed_users AS (
            -- Get all active users that the given user follows
            SELECT DISTINCT following_id AS user_id
            FROM `{self.project_id}.{self.dataset}.follower_graph`
            WHERE follower_id = '{user_id}'
              AND active = TRUE
        ),

        followed_content AS (
            -- Get content from bot_uploaded_content
            SELECT DISTINCT
                buc.video_id,
                buc.publisher_user_id,
                buc.timestamp AS upload_time
            FROM `{self.project_id}.{self.dataset}.bot_uploaded_content` buc
            INNER JOIN followed_users fu ON buc.publisher_user_id = fu.user_id
            WHERE buc.video_id IS NOT NULL

            UNION DISTINCT

            -- Get content from ai_ugc
            SELECT DISTINCT
                aug.video_id,
                aug.publisher_user_id,
                aug.upload_timestamp AS upload_time
            FROM `{self.project_id}.{self.dataset}.ai_ugc` aug
            INNER JOIN followed_users fu ON aug.publisher_user_id = fu.user_id
            WHERE aug.video_id IS NOT NULL

            UNION DISTINCT

            -- Get content from ugc_content_approval (approved only)
            SELECT DISTINCT
                uca.video_id,
                uca.user_id AS publisher_user_id,
                uca.created_at AS upload_time
            FROM `{self.project_id}.{self.dataset}.ugc_content_approval` uca
            INNER JOIN followed_users fu ON uca.user_id = fu.user_id
            WHERE uca.video_id IS NOT NULL
              AND uca.is_approved = TRUE
              AND uca.user_id IS NOT NULL
        ),

        valid_followed_content AS (
            -- Validate against video_unique (source of truth)
            -- Also exclude videos rejected in ugc_content_approval
            SELECT
                fc.video_id,
                fc.publisher_user_id,
                MAX(fc.upload_time) AS upload_time
            FROM followed_content fc
            INNER JOIN `{self.project_id}.{self.dataset}.video_unique` vu
                ON fc.video_id = vu.video_id
            LEFT JOIN `{self.project_id}.{self.dataset}.ugc_content_approval` uca
                ON fc.video_id = uca.video_id
            WHERE uca.video_id IS NULL OR uca.is_approved IS NOT FALSE
            GROUP BY fc.video_id, fc.publisher_user_id
        ),

        popular_videos AS (
            -- Videos with popularity scores (priority 1)
            SELECT
                vfc.video_id,
                gpv.global_popularity_score,
                1 AS priority
            FROM valid_followed_content vfc
            INNER JOIN `{self.project_id}.{self.dataset}.global_popular_videos_l7d` gpv
                ON vfc.video_id = gpv.video_id
            WHERE gpv.global_popularity_score IS NOT NULL
        ),

        fresh_videos AS (
            -- Fresh videos without popularity scores (priority 2)
            -- Excludes videos already in popular_videos for deduplication
            SELECT
                vfc.video_id,
                CAST(NULL AS FLOAT64) AS global_popularity_score,
                vfc.upload_time,
                2 AS priority
            FROM valid_followed_content vfc
            WHERE vfc.upload_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 30 DAY)
              AND vfc.video_id NOT IN (SELECT video_id FROM popular_videos)
        ),

        combined AS (
            -- Rank within each priority group
            SELECT
                video_id,
                global_popularity_score,
                priority,
                ROW_NUMBER() OVER (
                    PARTITION BY priority
                    ORDER BY
                        CASE WHEN priority = 1 THEN -global_popularity_score ELSE 0 END,
                        CASE WHEN priority = 2 THEN upload_time END DESC NULLS LAST
                ) AS rank_in_group
            FROM (
                SELECT video_id, global_popularity_score, CAST(NULL AS TIMESTAMP) AS upload_time, priority FROM popular_videos
                UNION ALL
                SELECT video_id, global_popularity_score, upload_time, priority FROM fresh_videos
            )
        )

        -- Final selection: popular first, then fresh
        SELECT
            video_id,
            global_popularity_score
        FROM combined
        ORDER BY priority, rank_in_group
        LIMIT {num_videos}
        """

        logger.info(f"Fetching followed users content for user_id={user_id}, num_videos={num_videos}")
        return self._execute_query_with_retry(query)

    def fetch_tournament_eligible_videos(self, limit: int = 10000) -> pd.DataFrame:
        """
        Fetch tournament-eligible videos WITH metadata (canister_id, post_id, publisher_user_id).

        This function returns a large pool of videos suitable for tournaments,
        prioritizing bot-uploaded and AI-generated content with high popularity scores.
        Only videos with valid metadata (present in ai_ugc or bot_uploaded_content tables)
        are returned, ensuring all tournament videos can be resolved to canister/post IDs.

        Algorithm:
            1. Query global_popular_videos_l7d table for popularity-ranked videos
            2. LEFT JOIN with ai_ugc table for AI content metadata
            3. LEFT JOIN with bot_uploaded_content table for bot content metadata
            4. Filter to is_bot_uploaded = TRUE (bot/AI content only)
            5. Filter to is_nsfw = FALSE (no NSFW content)
            6. Filter to videos that have metadata in at least one table
            7. COALESCE metadata from ai_ugc (priority) or bot_uploaded_content
            8. Order by global_popularity_score DESC (best performers first)
            9. Return top N videos with full metadata

        Args:
            limit: Number of videos to fetch (default 10000 for cascade support)

        Returns:
            DataFrame with columns:
                - video_id (STRING): The video identifier
                - global_popularity_score (FLOAT64): Popularity score for ranking
                - canister_id (STRING): Canister ID for the video
                - post_id (INT64): Post ID within the canister
                - publisher_user_id (STRING): User ID of the publisher

        Example:
            >>> bq_client = BigQueryClient()
            >>> df = bq_client.fetch_tournament_eligible_videos(limit=10000)
            >>> print(df.columns.tolist())
            ['video_id', 'global_popularity_score', 'canister_id', 'post_id', 'publisher_user_id']
        """
        query = f"""
        SELECT
            p.video_id,
            p.global_popularity_score,
            COALESCE(a.upload_canister_id, b.canister_id) as canister_id,
            COALESCE(a.post_id, b.post_id) as post_id,
            COALESCE(a.publisher_user_id, b.publisher_user_id) as publisher_user_id
        FROM `{self.project_id}.{self.dataset}.global_popular_videos_l7d` p
        LEFT JOIN `{self.project_id}.{self.dataset}.ai_ugc` a ON p.video_id = a.video_id
        LEFT JOIN `{self.project_id}.{self.dataset}.bot_uploaded_content` b ON p.video_id = b.video_id
        WHERE p.is_bot_uploaded = TRUE
          AND (p.is_nsfw = FALSE OR p.is_nsfw IS NULL)
          AND p.global_popularity_score IS NOT NULL
          AND (a.video_id IS NOT NULL OR b.video_id IS NOT NULL)
        ORDER BY p.global_popularity_score DESC
        LIMIT {limit}
        """

        logger.info(f"Fetching tournament-eligible videos with metadata (pool_size={limit})")
        return self._execute_query_with_retry(query)

    def _execute_query_with_retry(
        self,
        query: str,
        max_retries: int = 3,
        initial_delay: float = 1.0
    ) -> pd.DataFrame:
        """
        Execute BigQuery query with exponential backoff retry logic.

        Args:
            query: SQL query to execute
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay between retries (seconds)

        Returns:
            Query results as DataFrame

        Algorithm:
            1. Try to execute query
            2. If transient error, wait with exponential backoff
            3. Retry up to max_retries times
            4. Convert results to DataFrame
            5. Log timing and row count
        """
        delay = initial_delay
        last_error = None

        for attempt in range(max_retries):
            try:
                start_time = time.time()

                # Execute query
                query_job = self.client.query(query, job_config=self.default_job_config)
                results = query_job.result(timeout=300)  # 5 minute timeout

                # Convert to dataframe
                df = results.to_dataframe()

                elapsed = time.time() - start_time
                logger.info(f"Query completed in {elapsed:.2f}s, returned {len(df)} rows")

                return df

            except gcp_exceptions.DeadlineExceeded as e:
                last_error = e
                logger.warning(f"Query timeout on attempt {attempt + 1}: {e}")

            except gcp_exceptions.BadRequest as e:
                # Non-retryable error
                logger.error(f"Bad query request: {e}")
                raise

            except (gcp_exceptions.ServiceUnavailable, gcp_exceptions.InternalServerError) as e:
                last_error = e
                logger.warning(f"Transient error on attempt {attempt + 1}: {e}")

            except Exception as e:
                last_error = e
                logger.error(f"Unexpected error on attempt {attempt + 1}: {e}")

            # Exponential backoff
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {delay:.1f} seconds...")
                time.sleep(delay)
                delay *= 2  # Double the delay for next retry

        # All retries exhausted
        logger.error(f"Query failed after {max_retries} attempts")
        raise last_error or Exception("Query failed after all retries")

    def batch_process_dataframe(
        self,
        df: pd.DataFrame,
        batch_size: int = 10000
    ) -> Generator[pd.DataFrame, None, None]:
        """
        Process DataFrame in chunks for memory efficiency.

        Args:
            df: DataFrame to process
            batch_size: Size of each batch

        Yields:
            DataFrame chunks of specified size

        Algorithm:
            1. Calculate total number of batches
            2. Yield chunks using iloc
            3. Log progress for large datasets
        """
        total_rows = len(df)
        num_batches = (total_rows + batch_size - 1) // batch_size

        logger.info(f"Processing {total_rows} rows in {num_batches} batches of {batch_size}")

        for i in range(0, total_rows, batch_size):
            batch = df.iloc[i:i + batch_size]
            batch_num = i // batch_size + 1

            if batch_num % 10 == 0 or batch_num == num_batches:
                logger.debug(f"Processing batch {batch_num}/{num_batches}")

            yield batch


if __name__ == "__main__":
    # Test the BigQuery client
    import asyncio

    logging.basicConfig(level=logging.INFO)

    # Initialize client
    try:
        client = BigQueryClient()

        # Test fetching popular videos
        logger.info("Testing popular videos fetch...")
        popular_df = client.fetch_popular_videos(limit=10)
        logger.info(f"Sample popular videos:\n{popular_df.head()}")

        # Test fetching fresh videos
        logger.info("\nTesting fresh videos fetch...")
        fresh_df = client.fetch_fresh_videos()
        logger.info(f"Fresh videos by bucket:\n{fresh_df['bucket'].value_counts()}")

        # Test fetching user watch history
        logger.info("\nTesting user watch history fetch...")
        history_df = client.fetch_user_watch_history(hours_back=1)
        logger.info(f"User watch history stats: {len(history_df)} interactions from {history_df['user_id'].nunique()} users")

        # Test fetching followed users content
        logger.info("\nTesting followed users content fetch...")
        test_user_id = "test_user_123"  # Replace with real user_id for testing
        following_df = client.fetch_followed_users_content(test_user_id, num_videos=50)
        logger.info(f"Followed users content: {len(following_df)} videos")
        if not following_df.empty:
            popular_count = following_df['global_popularity_score'].notna().sum()
            fresh_count = following_df['global_popularity_score'].isna().sum()
            logger.info(f"  Popular videos: {popular_count}, Fresh videos: {fresh_count}")

        # Test batch processing
        logger.info("\nTesting batch processing...")
        for batch_num, batch in enumerate(client.batch_process_dataframe(popular_df, batch_size=3)):
            logger.info(f"Batch {batch_num}: {len(batch)} rows")

    except Exception as e:
        logger.error(f"Test failed: {e}")