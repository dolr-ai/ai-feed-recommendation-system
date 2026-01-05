"""
Async metadata handler for converting video_id to canister_id and post_id.

This module handles the conversion from video_id to canister_id/post_id pairs
by looking up metadata in KVRocks.

Key pattern: offchain:metadata:video_details:{video_id}
Storage format: Redis HASH with fields {video_id, post_id, publisher_user_id}
    (stored by off-chain-agent using HSETMULTIPLE)

Uses STUBBED_CANISTER_ID for all canister_id values.
"""

import os
import logging
from typing import Dict, List, Optional, Union
import redis
from config import STUBBED_CANISTER_ID

logger = logging.getLogger(__name__)


class AsyncMetadataHandler:
    """
    Async handler for video metadata conversion.

    Accepts an existing async KVRocks client (from kvrocks_service.client) to reuse
    the connection pool with mTLS already configured.

    Fetches video metadata from KVRocks using key pattern:
    offchain:metadata:video_details:{video_id}

    Returns canister_id, post_id, and publisher_user_id for videos.
    """

    def __init__(self, kvrocks_client):
        """
        Initialize async metadata handler with existing KVRocks client.

        Args:
            kvrocks_client: Async Redis client from kvrocks_service.client.
                            Already has connection pool and mTLS configured.
        """
        self.kvrocks_client = kvrocks_client

        # Initialize Redis impressions client (optional - for view counts)
        self._init_redis_impressions()

        logger.info("AsyncMetadataHandler initialized with shared KVRocks client")

    async def _fetch_from_kvrocks(
        self, video_ids: List[str]
    ) -> Dict[str, Dict[str, Union[str, int]]]:
        """
        Fetch video metadata from KVRocks using pipelined HGETALL.

        Uses pipelined HGETALL instead of GET because off-chain-agent stores
        metadata as Redis HASH (using HSETMULTIPLE), not as JSON STRING.

        Uses pipeline for cluster-mode compatibility - routes each HGETALL
        to the correct node based on key hash slot.

        Args:
            video_ids: List of video IDs to fetch metadata for

        Returns:
            Dictionary mapping video_id to metadata dict containing:
            - canister_id: str (may be None, caller uses STUBBED_CANISTER_ID)
            - post_id: str or int
            - publisher_user_id: str

        Key pattern: offchain:metadata:video_details:{video_id}
        Storage format: Redis HASH with fields {video_id, post_id, publisher_user_id}
        """
        if not video_ids:
            return {}

        try:
            # Use pipeline for cluster-mode compatibility (routes each HGETALL to correct node)
            pipe = self.kvrocks_client.pipeline()
            for vid in video_ids:
                pipe.hgetall(f"offchain:metadata:video_details:{vid}")
            results_raw = await pipe.execute()

            metadata = {}
            for vid, data in zip(video_ids, results_raw):
                # HGETALL returns empty dict {} for missing keys
                if not data:
                    continue

                # data is already a dict from HGETALL, no JSON parsing needed
                # Handle alternate field name: upload_canister_id vs canister_id
                canister_id = data.get("canister_id") or data.get("upload_canister_id")
                metadata[vid] = {
                    "canister_id": canister_id,
                    "post_id": data.get("post_id"),
                    "publisher_user_id": data.get("publisher_user_id", ""),
                }

            logger.debug(
                f"Fetched metadata for {len(metadata)}/{len(video_ids)} videos from KVRocks"
            )
            return metadata

        except Exception as e:
            logger.error(f"KVRocks fetch failed: {e}")
            return {}

    def _init_redis_impressions(self):
        """
        Initialize Redis client for impressions/view counts.

        Environment variables:
            - REDIS_IMPRESSIONS_HOST: Host for impressions Redis
            - REDIS_IMPRESSIONS_PORT: Port (default 6379)
            - REDIS_IMPRESSIONS_AUTHKEY: Authentication key/password

        Note:
            This connection is optional. If not configured, view counts will be 0.
        """
        host = os.environ.get("REDIS_IMPRESSIONS_HOST")
        if not host:
            self.redis_impressions_client = None
            return

        port = int(os.environ.get("REDIS_IMPRESSIONS_PORT", 6379))
        auth = os.environ.get("REDIS_IMPRESSIONS_AUTHKEY")

        if not auth:
            self.redis_impressions_client = None
            return

        tls_enabled = os.environ.get("REDIS_IMPRESSIONS_TLS_ENABLED", "true").lower() == "true"

        self.redis_impressions_client = redis.Redis(
            host=host,
            port=port,
            password=auth,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10,
            retry_on_timeout=True,
            health_check_interval=30,
            ssl=tls_enabled,
            ssl_cert_reqs=None if tls_enabled else None,
        )

        try:
            self.redis_impressions_client.ping()
        except redis.ConnectionError:
            self.redis_impressions_client = None

    def get_video_view_counts_from_redis_impressions(
        self, video_ids: List[str]
    ) -> Dict[str, tuple]:
        """
        Fetch view counts for video IDs from Redis impressions instance.

        Args:
            video_ids: List of video IDs to fetch view counts for

        Returns:
            Dictionary mapping video_id to (num_views_loggedin, num_views_all)
        """
        if not video_ids:
            return {}

        if not self.redis_impressions_client:
            return {}

        try:
            pipe = self.redis_impressions_client.pipeline()

            video_hash_keys = []
            for video_id in video_ids:
                video_hash_key = f"rewards:video:{video_id}"
                video_hash_keys.append(video_hash_key)
                pipe.hgetall(video_hash_key)

            results = pipe.execute()

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

    async def convert_video_id_to_canister_post(
        self, video_id: str
    ) -> Optional[Dict[str, Union[str, int]]]:
        """
        Convert a single video_id to canister_id and post_id.

        Args:
            video_id: The video ID to convert

        Returns:
            Dictionary with {"video_id": str, "canister_id": str, "post_id": str, "publisher_user_id": str}
        """
        metadata = await self._fetch_from_kvrocks([video_id])

        if video_id not in metadata:
            return None

        data = metadata[video_id]
        return {
            "video_id": video_id,
            "canister_id": STUBBED_CANISTER_ID,
            "post_id": str(data.get("post_id", "")),
            "publisher_user_id": str(data.get("publisher_user_id", "")),
        }

    async def batch_convert_video_ids(
        self, video_ids: List[str]
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Convert multiple video_ids to canister_id and post_id pairs.

        Args:
            video_ids: List of video IDs to convert

        Returns:
            List of dictionaries with video metadata (video_id, canister_id, post_id, publisher_user_id).
            Videos not found in KVRocks are skipped.
        """
        if not video_ids:
            return []

        all_metadata = await self._fetch_from_kvrocks(video_ids)

        results = []
        for video_id in video_ids:
            if video_id not in all_metadata:
                continue

            data = all_metadata[video_id]
            results.append({
                "video_id": video_id,
                "canister_id": STUBBED_CANISTER_ID,
                "post_id": str(data.get("post_id", "")),
                "publisher_user_id": str(data.get("publisher_user_id", "")),
            })

        return results

    async def batch_convert_video_ids_v2(
        self, video_ids: List[str]
    ) -> List[Dict[str, Union[str, int]]]:
        """
        Convert multiple video_ids to canister_id, post_id, and view counts.

        V2 version that includes num_views_loggedin and num_views_all fields.
        Currently view counts are stubbed to 0 (frontend fetches async).

        Args:
            video_ids: List of video IDs to convert

        Returns:
            List of dictionaries with video metadata including
            canister_id, post_id, publisher_user_id, num_views_loggedin, num_views_all
        """
        if not video_ids:
            return []

        v1_results = await self.batch_convert_video_ids(video_ids)

        if not v1_results:
            return []

        # View counts stubbed to 0 (frontend fetches async)
        results = []
        for v1_data in v1_results:
            v2_data = {
                "video_id": v1_data["video_id"],
                "canister_id": v1_data["canister_id"],
                "post_id": v1_data["post_id"],
                "publisher_user_id": v1_data["publisher_user_id"],
                "num_views_loggedin": 0,
                "num_views_all": 0
            }
            results.append(v2_data)

        return results


if __name__ == "__main__":
    """
    Test the async metadata handler functionality.
    """
    import asyncio
    logging.basicConfig(level=logging.INFO)

    async def test():
        print("\n=== Testing AsyncMetadataHandler ===")
        print("Note: Requires a running KVRocks instance with video metadata.")
        print("In production, the handler receives kvrocks_service.client.")
        print("This test is for documentation purposes only.")

    asyncio.run(test())
