"""
Test helper functions for seeding Redis and generating test data.

Simple, reusable functions - no classes, no factories.
"""

import time
from typing import Dict, List


def generate_video_ids(prefix: str, count: int) -> List[str]:
    """
    Generate deterministic video IDs for testing.

    Args:
        prefix: String prefix (e.g., 'vid_pop_99_100')
        count: Number of IDs to generate

    Returns:
        List of video IDs in format '{prefix}_{index:06d}'
    """
    return [f"{prefix}_{i:06d}" for i in range(1, count + 1)]


def seed_global_popularity(client, videos_by_bucket: Dict[str, List[str]], ttl: int = 86400):
    """
    Seed global popularity buckets with test videos.

    Args:
        client: Redis client
        videos_by_bucket: Dict mapping bucket name -> list of video IDs
            Example: {'99_100': ['vid_001', 'vid_002'], '90_99': ['vid_003']}
        ttl: TTL in seconds for video expiry scores (default 1 day)

    Returns:
        Dict mapping bucket -> count of videos added
    """
    now = int(time.time())
    expiry = float(now + ttl)
    added = {}

    for bucket, videos in videos_by_bucket.items():
        key = f"{{GLOBAL}}:pool:pop_{bucket}"
        client.delete(key)
        if videos:
            mapping = {vid: expiry for vid in videos}
            client.zadd(key, mapping)
        added[bucket] = len(videos)

    return added


def seed_global_freshness(client, videos_by_window: Dict[str, List[str]], ttl: int = 86400):
    """
    Seed global freshness windows with test videos.

    Args:
        client: Redis client
        videos_by_window: Dict mapping window name -> list of video IDs
            Example: {'l1d': ['vid_001'], 'l7d': ['vid_002', 'vid_003']}
        ttl: TTL in seconds for video expiry scores (default 1 day)

    Returns:
        Dict mapping window -> count of videos added
    """
    now = int(time.time())
    expiry = float(now + ttl)
    added = {}

    for window, videos in videos_by_window.items():
        key = f"{{GLOBAL}}:pool:fresh_{window}"
        client.delete(key)
        if videos:
            mapping = {vid: expiry for vid in videos}
            client.zadd(key, mapping)
        added[window] = len(videos)

    return added


def seed_user_pool(client, user_id: str, rec_type: str, videos: List[str], ttl: int = 86400):
    """
    Seed a user's videos_to_show pool.

    Args:
        client: Redis client
        user_id: User identifier
        rec_type: Pool type ('popularity', 'freshness', 'following', 'ugc')
        videos: List of video IDs to add
        ttl: TTL in seconds for video expiry scores

    Returns:
        Number of videos added
    """
    key = f"{{user:{user_id}}}:videos_to_show:{rec_type}"
    client.delete(key)

    if not videos:
        return 0

    now = int(time.time())
    expiry = float(now + ttl)
    mapping = {vid: expiry for vid in videos}
    return client.zadd(key, mapping)


def clear_user_keys(client, user_id: str) -> int:
    """
    Delete all keys for a user.

    Args:
        client: Redis client
        user_id: User identifier

    Returns:
        Number of keys deleted
    """
    # Match cluster-safe key pattern with hash tags
    pattern = f"{{user:{user_id}}}:*"
    keys = client.keys(pattern)
    if not keys:
        return 0
    return client.delete(*keys)


def clear_global_pools(client):
    """
    Delete all global popularity and freshness pools.

    Args:
        client: Redis client

    Returns:
        Number of keys deleted
    """
    deleted = 0
    for pattern in ["{GLOBAL}:pool:pop_*", "{GLOBAL}:pool:fresh_*", "{GLOBAL}:pool:ugc"]:
        keys = client.keys(pattern)
        if keys:
            deleted += client.delete(*keys)
    return deleted
