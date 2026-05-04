from __future__ import annotations

import json
import time
from typing import Iterable, Optional

from src.utils.feed_recsys_keys import (
    ai_influencer_ids_key,
    excluded_videos_key,
    following_sync_users_key,
    global_pool_key,
    user_bloom_key,
    user_pool_key,
    user_popularity_pointer_key,
    user_refill_lock_key,
    user_served_recent_key,
    video_view_count_key,
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

    async def track_following_sync_user(self, user_id: str) -> bool:
        normalized_user_id = str(user_id or "").strip()
        if not normalized_user_id:
            return False
        return bool(
            await self._client.sadd(
                following_sync_users_key(self._settings),
                normalized_user_id,
            )
        )

    async def get_tracked_following_sync_users(self) -> list[str]:
        if hasattr(self._client, "smembers"):
            user_ids = await self._client.smembers(following_sync_users_key(self._settings))
        else:
            user_ids = await self._client.execute_command(
                "SMEMBERS",
                following_sync_users_key(self._settings),
            )
        return sorted(user_id for user_id in user_ids if user_id)

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

    async def add_served_recent_videos(
        self,
        user_id: str,
        video_ids: list[str],
        ttl_sec: Optional[int] = None,
    ) -> int:
        if not video_ids:
            return 0

        ttl = ttl_sec or self._settings.feed_recsys_served_recent_ttl_sec
        expiry = self._expiry_timestamp(ttl)
        mapping = {
            video_id: float(expiry)
            for video_id in dict.fromkeys(video_ids)
        }
        key = user_served_recent_key(self._settings, user_id)
        added = await self._client.zadd(key, mapping)
        await self._client.expire(key, ttl)
        return int(added)

    async def check_user_served_recent(self, user_id: str, video_ids: list[str]) -> dict[str, bool]:
        if not video_ids:
            return {}

        key = user_served_recent_key(self._settings, user_id)
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

    async def get_served_recent_count(self, user_id: str) -> int:
        key = user_served_recent_key(self._settings, user_id)
        now = self._now()
        await self._client.zremrangebyscore(key, "-inf", now)
        return int(await self._client.zcount(key, now, "+inf"))

    async def get_user_pool_size(
        self,
        user_id: str,
        pool_name: str,
        current_time: Optional[int] = None,
    ) -> int:
        key = user_pool_key(self._settings, user_id, pool_name)
        now = current_time if current_time is not None else self._now()
        await self._client.zremrangebyscore(key, "-inf", now)
        return int(await self._client.zcount(key, now, "+inf"))

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

    async def acquire_refill_lock(
        self,
        user_id: str,
        pool_name: str,
        ttl_sec: Optional[int] = None,
    ) -> bool:
        ttl = ttl_sec or self._settings.feed_recsys_refill_lock_ttl_sec
        return bool(
            await self._client.set(
                user_refill_lock_key(self._settings, user_id, pool_name),
                "1",
                ex=ttl,
                nx=True,
            )
        )

    async def release_refill_lock(self, user_id: str, pool_name: str) -> None:
        await self._client.delete(user_refill_lock_key(self._settings, user_id, pool_name))

    async def get_cached_video_view_counts(self, video_ids: list[str]) -> dict[str, dict[str, int]]:
        unique_video_ids = list(dict.fromkeys(video_id for video_id in video_ids if video_id))
        if not unique_video_ids:
            return {}

        keys = [video_view_count_key(self._settings, video_id) for video_id in unique_video_ids]
        if hasattr(self._client, "mget"):
            values = await self._client.mget(keys)
        else:
            pipe = self._client.pipeline()
            for key in keys:
                pipe.get(key)
            values = await pipe.execute()

        result: dict[str, dict[str, int]] = {}
        for video_id, raw in zip(unique_video_ids, values):
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            result[video_id] = {
                "num_views_loggedin": int(payload.get("num_views_loggedin") or 0),
                "num_views_all": int(payload.get("num_views_all") or 0),
            }
        return result

    async def cache_video_view_counts(
        self,
        view_counts: dict[str, dict[str, int]],
        ttl_sec: Optional[int] = None,
    ) -> int:
        if not view_counts:
            return 0

        ttl = ttl_sec or self._settings.feed_recsys_view_count_ttl_sec
        pipe = self._client.pipeline()
        count = 0
        for video_id, payload in view_counts.items():
            if not video_id:
                continue
            pipe.set(
                video_view_count_key(self._settings, video_id),
                json.dumps(
                    {
                        "num_views_loggedin": int(payload.get("num_views_loggedin") or 0),
                        "num_views_all": int(payload.get("num_views_all") or 0),
                    }
                ),
                ex=ttl,
            )
            count += 1
        await pipe.execute()
        return count

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
        if not unique_values:
            await self._client.delete(key)
            return 0

        pipe = self._client.pipeline()
        pipe.delete(key)
        pipe.sadd(key, *unique_values)
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
