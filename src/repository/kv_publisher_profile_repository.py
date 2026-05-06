from __future__ import annotations

import json
import random
import uuid

from src.utils.feed_recsys_keys import (
    publisher_profile_key,
    publisher_profile_refresh_lock_key,
    publisher_profile_refresh_queue_key,
    publisher_profile_warmup_queue_key,
)


class KVPublisherProfileRepository:
    _COMPARE_AND_DELETE_SCRIPT = """
    if redis.call('GET', KEYS[1]) == ARGV[1] then
        return redis.call('DEL', KEYS[1])
    end
    return 0
    """

    def __init__(self, client, settings):
        self._client = client
        self._settings = settings

    async def get_profiles_batch(self, publisher_user_ids: list[str]) -> dict[str, dict]:
        unique_ids = self._normalize_ids(publisher_user_ids)
        if not unique_ids or self._client is None:
            return {}

        keys = [publisher_profile_key(self._settings, publisher_id) for publisher_id in unique_ids]
        if hasattr(self._client, "mget"):
            values = await self._client.mget(keys)
        else:
            pipe = self._client.pipeline()
            for key in keys:
                pipe.get(key)
            values = await pipe.execute()

        profiles: dict[str, dict] = {}
        for publisher_id, raw in zip(unique_ids, values):
            if not raw:
                continue
            try:
                payload = json.loads(raw)
            except (TypeError, ValueError):
                continue
            profiles[publisher_id] = self._normalize_profile_payload(payload)
        return profiles

    async def cache_profiles_batch(self, profiles_by_publisher_id: dict[str, dict]) -> int:
        if not profiles_by_publisher_id or self._client is None:
            return 0

        base_ttl = self._settings.feed_recsys_publisher_profile_hard_ttl_sec
        jitter_sec = max(0, self._settings.feed_recsys_publisher_profile_ttl_jitter_sec)
        pipe = self._client.pipeline()
        count = 0
        for publisher_id, payload in profiles_by_publisher_id.items():
            normalized_id = str(publisher_id or "").strip()
            if not normalized_id:
                continue
            normalized_payload = self._normalize_profile_payload(payload)
            ttl = base_ttl + random.randint(0, jitter_sec) if jitter_sec else base_ttl
            pipe.set(
                publisher_profile_key(self._settings, normalized_id),
                json.dumps(normalized_payload, separators=(",", ":")),
                ex=ttl,
            )
            count += 1

        if count > 0:
            await pipe.execute()
        return count

    async def enqueue_refresh(self, publisher_user_ids: list[str]) -> int:
        return await self._enqueue_set(
            publisher_profile_refresh_queue_key(self._settings),
            publisher_user_ids,
        )

    async def enqueue_warmup(self, publisher_user_ids: list[str]) -> int:
        return await self._enqueue_set(
            publisher_profile_warmup_queue_key(self._settings),
            publisher_user_ids,
        )

    async def dequeue_refresh_batch(self, limit: int) -> list[str]:
        return await self._spop_batch(
            publisher_profile_refresh_queue_key(self._settings),
            limit,
        )

    async def dequeue_warmup_batch(self, limit: int) -> list[str]:
        return await self._spop_batch(
            publisher_profile_warmup_queue_key(self._settings),
            limit,
        )

    async def acquire_refresh_lock(self, publisher_user_id: str, ttl_sec: int) -> str | None:
        normalized_id = str(publisher_user_id or "").strip()
        if not normalized_id or self._client is None:
            return None

        token = uuid.uuid4().hex
        acquired = await self._client.set(
            publisher_profile_refresh_lock_key(self._settings, normalized_id),
            token,
            ex=ttl_sec,
            nx=True,
        )
        return token if acquired else None

    async def release_refresh_lock(self, publisher_user_id: str, token: str) -> None:
        normalized_id = str(publisher_user_id or "").strip()
        normalized_token = str(token or "").strip()
        if not normalized_id or not normalized_token or self._client is None:
            return

        key = publisher_profile_refresh_lock_key(self._settings, normalized_id)
        if hasattr(self._client, "execute_command"):
            await self._client.execute_command(
                "EVAL",
                self._COMPARE_AND_DELETE_SCRIPT,
                1,
                key,
                normalized_token,
            )
            return

        current = None
        if hasattr(self._client, "get"):
            current = await self._client.get(key)
        if current == normalized_token:
            await self._client.delete(key)

    async def _enqueue_set(self, key: str, publisher_user_ids: list[str]) -> int:
        unique_ids = self._normalize_ids(publisher_user_ids)
        if not unique_ids or self._client is None:
            return 0
        return int(await self._client.sadd(key, *unique_ids))

    async def _spop_batch(self, key: str, limit: int) -> list[str]:
        batch_size = max(0, int(limit or 0))
        if batch_size <= 0 or self._client is None:
            return []

        if hasattr(self._client, "spop"):
            raw = await self._client.spop(key, batch_size)
        else:
            raw = await self._client.execute_command("SPOP", key, batch_size)

        if raw is None:
            return []
        if isinstance(raw, str):
            return [raw] if raw else []
        return [value for value in raw if value]

    def _normalize_profile_payload(self, payload: dict | None) -> dict:
        payload = payload or {}
        return {
            "username": str(payload.get("username") or "").strip(),
            "profile_image_url": str(payload.get("profile_image_url") or "").strip(),
            "is_pro_user": self._to_bool(payload.get("is_pro_user")),
            "username_fetched_at": int(payload.get("username_fetched_at") or 0),
            "profile_fetched_at": int(payload.get("profile_fetched_at") or 0),
        }

    @staticmethod
    def _normalize_ids(values: list[str]) -> list[str]:
        return list(dict.fromkeys(str(value or "").strip() for value in values if value))

    @staticmethod
    def _to_bool(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y"}
        return False
