from __future__ import annotations

import time
from typing import Iterable, Optional

from src.utils.feed_recsys_keys import (
    ai_influencer_ids_key,
    excluded_videos_key,
    global_pool_key,
    ugc_discovery_pushes_key,
    ugc_discovery_timestamps_key,
    user_bloom_key,
    user_following_sync_key,
    user_pool_key,
    user_popularity_pointer_key,
    user_watched_key,
    video_metadata_key,
)


class KVFeedRepository:
    def __init__(self, client, settings):
        self._client = client
        self._settings = settings

    async def get_user_pool(
        self,
        user_id: str,
        pool_name: str,
        limit: int,
        current_time: Optional[int] = None,
    ) -> list[str]:
        return await self._get_pool(
            user_pool_key(self._settings, user_id, pool_name),
            limit,
            current_time,
        )

    async def add_user_pool_videos(
        self,
        user_id: str,
        pool_name: str,
        video_ids: list[str],
        ttl_sec: Optional[int] = None,
    ) -> int:
        return await self._add_pool_videos(
            user_pool_key(self._settings, user_id, pool_name),
            video_ids,
            ttl_sec,
        )

    async def replace_user_pool(
        self,
        user_id: str,
        pool_name: str,
        video_ids: list[str],
        ttl_sec: Optional[int] = None,
    ) -> int:
        return await self._replace_pool(
            user_pool_key(self._settings, user_id, pool_name),
            video_ids,
            ttl_sec,
        )

    async def clear_user_pool(self, user_id: str, pool_name: str) -> int:
        return int(await self._client.delete(user_pool_key(self._settings, user_id, pool_name)))

    async def remove_user_pool_videos(
        self,
        user_id: str,
        pool_name: str,
        video_ids: list[str],
    ) -> int:
        return await self._remove_pool_videos(
            user_pool_key(self._settings, user_id, pool_name),
            video_ids,
        )

    async def get_global_pool(
        self,
        pool_name: str,
        limit: int,
        current_time: Optional[int] = None,
    ) -> list[str]:
        return await self._get_pool(
            global_pool_key(self._settings, pool_name),
            limit,
            current_time,
        )

    async def replace_global_pool(
        self,
        pool_name: str,
        video_ids: list[str],
        ttl_sec: Optional[int] = None,
    ) -> int:
        return await self._replace_pool(
            global_pool_key(self._settings, pool_name),
            video_ids,
            ttl_sec,
        )

    async def remove_global_pool_videos(self, pool_name: str, video_ids: list[str]) -> int:
        return await self._remove_pool_videos(
            global_pool_key(self._settings, pool_name),
            video_ids,
        )

    async def replace_excluded_videos(self, video_ids: list[str]) -> int:
        return await self._replace_set(
            excluded_videos_key(self._settings),
            video_ids,
        )

    async def filter_excluded_videos(self, video_ids: list[str]) -> list[str]:
        if not video_ids:
            return []

        key = excluded_videos_key(self._settings)
        if not await self._client.exists(key):
            return video_ids

        membership = await self._smismember(key, video_ids)
        return [
            video_id
            for video_id, is_excluded in zip(video_ids, membership)
            if not is_excluded
        ]

    async def replace_ai_influencer_ids(self, user_ids: list[str]) -> int:
        return await self._replace_set(
            ai_influencer_ids_key(self._settings),
            user_ids,
        )

    async def check_ai_influencer_ids(self, user_ids: list[str]) -> dict[str, bool]:
        if not user_ids:
            return {}

        membership = await self._smismember(
            ai_influencer_ids_key(self._settings),
            user_ids,
        )
        return {
            user_id: bool(is_member)
            for user_id, is_member in zip(user_ids, membership)
        }

    async def get_video_metadata_batch(self, video_ids: list[str]) -> dict[str, dict]:
        if not video_ids:
            return {}

        pipe = self._client.pipeline()
        for video_id in video_ids:
            pipe.hgetall(video_metadata_key(video_id))
        results = await pipe.execute()

        metadata: dict[str, dict] = {}
        for video_id, row in zip(video_ids, results):
            if not row:
                continue
            metadata[video_id] = {
                "canister_id": row.get("canister_id") or row.get("upload_canister_id"),
                "post_id": row.get("post_id"),
                "publisher_user_id": row.get("publisher_user_id", ""),
            }
        return metadata

    async def user_bloom_exists(self, user_id: str) -> bool:
        return bool(await self._client.exists(user_bloom_key(self._settings, user_id)))

    async def ensure_user_bloom(self, user_id: str) -> bool:
        key = user_bloom_key(self._settings, user_id)

        if await self._client.exists(key):
            await self._client.expire(key, self._settings.feed_recsys_bloom_ttl_sec)
            return False

        try:
            await self._client.execute_command(
                "BF.RESERVE",
                key,
                self._settings.feed_recsys_bloom_error_rate,
                self._settings.feed_recsys_bloom_initial_capacity,
                "EXPANSION",
                self._settings.feed_recsys_bloom_expansion,
            )
            await self._client.expire(key, self._settings.feed_recsys_bloom_ttl_sec)
            return True
        except Exception as exc:
            if "item exists" in str(exc).lower():
                await self._client.expire(key, self._settings.feed_recsys_bloom_ttl_sec)
                return False
            raise

    async def add_to_user_bloom(self, user_id: str, video_ids: list[str]) -> int:
        if not video_ids:
            return 0

        key = user_bloom_key(self._settings, user_id)
        await self.ensure_user_bloom(user_id)
        results = await self._client.execute_command("BF.MADD", key, *video_ids)
        await self._client.expire(key, self._settings.feed_recsys_bloom_ttl_sec)
        return sum(int(bool(result)) for result in results)

    async def check_user_bloom(self, user_id: str, video_ids: list[str]) -> dict[str, bool]:
        if not video_ids:
            return {}

        key = user_bloom_key(self._settings, user_id)
        if not await self._client.exists(key):
            return {video_id: False for video_id in video_ids}

        results = await self._client.execute_command("BF.MEXISTS", key, *video_ids)
        await self._client.expire(key, self._settings.feed_recsys_bloom_ttl_sec)
        return {
            video_id: bool(result)
            for video_id, result in zip(video_ids, results)
        }

    async def add_watched_videos(
        self,
        user_id: str,
        video_ids: list[str],
        ttl_sec: Optional[int] = None,
    ) -> int:
        if not video_ids:
            return 0

        ttl = ttl_sec or self._settings.feed_recsys_watched_ttl_sec
        expiry = self._expiry_timestamp(ttl)
        mapping = {
            video_id: float(expiry)
            for video_id in dict.fromkeys(video_ids)
        }
        key = user_watched_key(self._settings, user_id)
        added = await self._client.zadd(key, mapping)
        await self._client.expire(key, ttl)
        return int(added)

    async def check_user_watched(self, user_id: str, video_ids: list[str]) -> dict[str, bool]:
        if not video_ids:
            return {}

        key = user_watched_key(self._settings, user_id)
        now = self._now()
        await self._client.zremrangebyscore(key, "-inf", now)

        pipe = self._client.pipeline()
        for video_id in video_ids:
            pipe.zscore(key, video_id)
        scores = await pipe.execute()
        return {
            video_id: score is not None
            for video_id, score in zip(video_ids, scores)
        }

    async def get_watched_count(self, user_id: str) -> int:
        key = user_watched_key(self._settings, user_id)
        now = self._now()
        await self._client.zremrangebyscore(key, "-inf", now)
        return int(await self._client.zcount(key, now, "+inf"))

    async def get_following_sync_time(self, user_id: str) -> Optional[int]:
        value = await self._client.get(user_following_sync_key(self._settings, user_id))
        if value is None:
            return None
        return int(value)

    async def set_following_sync_time(
        self,
        user_id: str,
        unix_ts: Optional[int] = None,
        ttl_sec: Optional[int] = None,
    ) -> bool:
        timestamp = unix_ts if unix_ts is not None else self._now()
        kwargs = {}
        if ttl_sec is not None:
            kwargs["ex"] = ttl_sec
        return bool(
            await self._client.set(
                user_following_sync_key(self._settings, user_id),
                timestamp,
                **kwargs,
            )
        )

    async def get_popularity_pointer(
        self,
        user_id: str,
        default_bucket: str = "99_100",
    ) -> str:
        key = user_popularity_pointer_key(self._settings, user_id)
        value = await self._client.get(key)
        if value:
            return value

        await self._client.set(
            key,
            default_bucket,
            ex=self._settings.feed_recsys_percentile_pointer_ttl_sec,
        )
        return default_bucket

    async def set_popularity_pointer(self, user_id: str, bucket: str) -> bool:
        key = user_popularity_pointer_key(self._settings, user_id)
        ttl_seconds = int(await self._client.ttl(key))
        expiry = (
            ttl_seconds
            if ttl_seconds > 0
            else self._settings.feed_recsys_percentile_pointer_ttl_sec
        )
        return bool(await self._client.set(key, bucket, ex=expiry))

    async def get_ugc_discovery_push_counts(self) -> dict[str, int]:
        raw = await self._client.hgetall(ugc_discovery_pushes_key(self._settings))
        return {
            key: int(value)
            for key, value in raw.items()
        }

    async def replace_ugc_discovery_pool(
        self,
        videos: list[dict],
        ttl_sec: Optional[int] = None,
    ) -> int:
        pool_key = global_pool_key(self._settings, "ugc_discovery")
        timestamps_key = ugc_discovery_timestamps_key(self._settings)
        pushes_key = ugc_discovery_pushes_key(self._settings)
        existing_push_counts = await self.get_ugc_discovery_push_counts()
        ttl = ttl_sec or self._settings.feed_recsys_ugc_discovery_pool_ttl_sec
        expiry = self._expiry_timestamp(ttl)
        new_video_ids = {
            row["video_id"]
            for row in videos
            if row.get("video_id")
        }

        pipe = self._client.pipeline()
        pipe.delete(pool_key)
        pipe.delete(timestamps_key)

        for row in videos:
            video_id = row["video_id"]
            upload_timestamp = int(row["upload_timestamp"])
            pipe.zadd(pool_key, {video_id: float(expiry)})
            pipe.hset(timestamps_key, video_id, upload_timestamp)
            if video_id not in existing_push_counts:
                pipe.hset(pushes_key, video_id, 0)

        stale_video_ids = sorted(set(existing_push_counts) - new_video_ids)
        if stale_video_ids:
            pipe.hdel(pushes_key, *stale_video_ids)

        pipe.expire(pool_key, ttl)
        await pipe.execute()
        return len(new_video_ids)

    async def _get_pool(
        self,
        key: str,
        limit: int,
        current_time: Optional[int],
    ) -> list[str]:
        min_score = current_time if current_time is not None else self._now()
        return await self._client.zrangebyscore(
            key,
            min_score,
            "+inf",
            start=0,
            num=limit,
        )

    async def _replace_pool(
        self,
        key: str,
        video_ids: list[str],
        ttl_sec: Optional[int],
    ) -> int:
        ttl = ttl_sec or self._settings.feed_recsys_pool_ttl_sec
        expiry = self._expiry_timestamp(ttl)
        unique_video_ids = list(dict.fromkeys(video_ids))

        pipe = self._client.pipeline()
        pipe.delete(key)
        if unique_video_ids:
            pipe.zadd(
                key,
                {
                    video_id: float(expiry)
                    for video_id in unique_video_ids
                },
            )
            pipe.expire(key, ttl)
        await pipe.execute()
        return len(unique_video_ids)

    async def _remove_pool_videos(self, key: str, video_ids: list[str]) -> int:
        unique_video_ids = list(dict.fromkeys(video_ids))
        if not unique_video_ids:
            return 0
        return int(await self._client.zrem(key, *unique_video_ids))

    async def _add_pool_videos(
        self,
        key: str,
        video_ids: list[str],
        ttl_sec: Optional[int],
    ) -> int:
        if not video_ids:
            return 0

        ttl = ttl_sec or self._settings.feed_recsys_pool_ttl_sec
        expiry = self._expiry_timestamp(ttl)
        mapping = {
            video_id: float(expiry)
            for video_id in dict.fromkeys(video_ids)
        }
        added = await self._client.zadd(key, mapping)
        await self._client.expire(key, ttl)
        return int(added)

    async def _replace_set(self, key: str, values: Iterable[str]) -> int:
        unique_values = list(dict.fromkeys(value for value in values if value))
        temp_key = f"{key}:tmp"

        if not unique_values:
            await self._client.delete(temp_key)
            await self._client.delete(key)
            return 0

        pipe = self._client.pipeline()
        pipe.delete(temp_key)
        pipe.sadd(temp_key, *unique_values)
        pipe.rename(temp_key, key)
        await pipe.execute()
        return len(unique_values)

    async def _smismember(self, key: str, values: list[str]) -> list[int]:
        if hasattr(self._client, "smismember"):
            return await self._client.smismember(key, values)
        return await self._client.execute_command("SMISMEMBER", key, *values)

    @staticmethod
    def _now() -> int:
        return int(time.time())

    def _expiry_timestamp(self, ttl_sec: int) -> int:
        return self._now() + ttl_sec
