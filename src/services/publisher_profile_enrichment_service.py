from __future__ import annotations

import asyncio
import time

from src.schemas.feed_recsys import FeedVideoMetadata
from src.services.logger_service import LoggerService

MAX_PUBLISHER_BATCH_SIZE = 100


class PublisherProfileEnrichmentService:
    def __init__(
        self,
        clickhouse_feed_repository,
        kv_publisher_profile_repository,
        request_metadata_service_client,
        background_metadata_service_client,
        request_canister_client,
        background_canister_client,
        settings,
    ):
        self._clickhouse_feed_repository = clickhouse_feed_repository
        self._kv_publisher_profile_repository = kv_publisher_profile_repository
        self._request_metadata_service_client = request_metadata_service_client
        self._background_metadata_service_client = background_metadata_service_client
        self._request_canister_client = request_canister_client
        self._background_canister_client = background_canister_client
        self._settings = settings
        self._follow_lookup_semaphore = asyncio.Semaphore(
            max(1, settings.feed_recsys_follow_lookup_max_concurrency)
        )
        self._log = LoggerService().get("feed_recsys_publisher_profile")

    async def enrich_rows(
        self,
        viewer_user_id: str,
        rows: list[FeedVideoMetadata],
    ) -> list[FeedVideoMetadata]:
        if not rows:
            return rows

        publisher_user_ids = self._dedupe_publisher_ids(
            row.publisher_user_id for row in rows if row.publisher_user_id
        )
        if not publisher_user_ids:
            return rows

        now = int(time.time())
        cached_profiles_task = asyncio.create_task(
            self._get_cached_profiles(publisher_user_ids)
        )
        following_status_task = asyncio.create_task(
            self._get_following_status_map(viewer_user_id, publisher_user_ids)
        )

        cached_profiles = await cached_profiles_task
        fresh_ids, stale_ids, miss_ids = self._partition_cached_profiles(
            publisher_user_ids,
            cached_profiles,
            now,
        )

        resolved_profiles = {
            publisher_id: self._normalize_profile_payload(cached_profiles.get(publisher_id))
            for publisher_id in fresh_ids + stale_ids
            if publisher_id in cached_profiles
        }
        if stale_ids:
            self._schedule_refresh_enqueue(stale_ids)

        if miss_ids:
            request_profiles = await self._resolve_profiles(
                miss_ids,
                existing_profiles={},
                metadata_client=self._request_metadata_service_client,
                canister_client=self._request_canister_client,
            )
            if request_profiles:
                await self._cache_profiles(request_profiles)
                resolved_profiles.update(request_profiles)

        following_status = await following_status_task
        for row in rows:
            profile = resolved_profiles.get(row.publisher_user_id, {})
            row.is_following = following_status.get(row.publisher_user_id, False)
            row.username = str(profile.get("username") or "")
            row.is_pro_user = bool(profile.get("is_pro_user") or False)
            row.profile_image_url = str(profile.get("profile_image_url") or "")

        return rows

    async def queue_warmup_publishers(self, publisher_user_ids: list[str]) -> int:
        unique_ids = self._dedupe_publisher_ids(publisher_user_ids)
        if not unique_ids:
            return 0
        try:
            return await self._kv_publisher_profile_repository.enqueue_warmup(unique_ids)
        except Exception:
            self._log.warning(
                "Publisher profile warmup queue write failed",
                extra={"publisher_count": len(unique_ids)},
                exc_info=True,
            )
            return 0

    async def refresh_queued_profiles(self) -> dict[str, int]:
        batch_size = self._publisher_batch_size(
            self._settings.feed_recsys_publisher_profile_refresh_batch_size
        )
        publisher_user_ids = (
            await self._kv_publisher_profile_repository.dequeue_refresh_batch(batch_size)
        )
        if not publisher_user_ids:
            return {"dequeued": 0, "refreshed": 0}

        try:
            refreshed, failed_ids = await self._refresh_profiles(
                publisher_user_ids,
                metadata_client=self._background_metadata_service_client,
                canister_client=self._background_canister_client,
            )
        except Exception:
            await self._enqueue_refresh(publisher_user_ids)
            raise
        if failed_ids:
            await self._enqueue_refresh(failed_ids)
        return {"dequeued": len(publisher_user_ids), "refreshed": refreshed}

    async def warmup_publisher_profiles(self) -> dict[str, int]:
        batch_size = self._publisher_batch_size(
            self._settings.feed_recsys_publisher_profile_warmup_batch_size
        )
        publisher_user_ids = (
            await self._kv_publisher_profile_repository.dequeue_warmup_batch(batch_size)
        )
        if not publisher_user_ids:
            return {"dequeued": 0, "warmed": 0}

        try:
            warmed, failed_ids = await self._refresh_profiles(
                publisher_user_ids,
                metadata_client=self._background_metadata_service_client,
                canister_client=self._background_canister_client,
            )
        except Exception:
            await self._kv_publisher_profile_repository.enqueue_warmup(publisher_user_ids)
            raise
        if failed_ids:
            await self._kv_publisher_profile_repository.enqueue_warmup(failed_ids)
        return {"dequeued": len(publisher_user_ids), "warmed": warmed}

    async def backfill_recent_publishers(self) -> dict[str, int]:
        batch_size = self._publisher_batch_size(
            self._settings.feed_recsys_publisher_profile_backfill_batch_size
        )
        lookback_days = (
            self._settings.feed_recsys_publisher_profile_backfill_lookback_days
        )
        publisher_user_ids = (
            await self._clickhouse_feed_repository.get_recent_active_publisher_user_ids(
                limit=batch_size,
                lookback_days=lookback_days,
            )
        )
        if not publisher_user_ids:
            return {"selected": 0, "refreshed": 0}

        refreshed, failed_ids = await self._refresh_profiles(
            publisher_user_ids,
            metadata_client=self._background_metadata_service_client,
            canister_client=self._background_canister_client,
            raise_on_total_failure=True,
        )
        if failed_ids:
            await self._enqueue_refresh(failed_ids)
        return {"selected": len(publisher_user_ids), "refreshed": refreshed}

    async def _refresh_profiles(
        self,
        publisher_user_ids: list[str],
        metadata_client,
        canister_client,
        raise_on_total_failure: bool = True,
    ) -> tuple[int, list[str]]:
        unique_ids = self._dedupe_publisher_ids(publisher_user_ids)
        if not unique_ids:
            return 0, []

        acquired_locks = await self._acquire_refresh_locks(unique_ids)
        locked_ids = list(acquired_locks)
        if not locked_ids:
            return 0, []

        try:
            existing_profiles = await self._get_cached_profiles(locked_ids)
            merged_profiles, failed_ids, refreshed_ids = await self._resolve_profiles_with_status(
                locked_ids,
                existing_profiles=existing_profiles,
                metadata_client=metadata_client,
                canister_client=canister_client,
                raise_on_total_failure=raise_on_total_failure,
            )
            if not refreshed_ids:
                return 0, failed_ids
            profiles_to_cache = {
                publisher_id: merged_profiles[publisher_id]
                for publisher_id in refreshed_ids
                if publisher_id in merged_profiles
            }
            cached_count = await self._cache_profiles(profiles_to_cache)
            if cached_count <= 0:
                failed_ids = self._dedupe_publisher_ids(
                    failed_ids + list(profiles_to_cache)
                )
                return 0, failed_ids
            return cached_count, failed_ids
        finally:
            await self._release_refresh_locks(acquired_locks)

    async def _resolve_profiles(
        self,
        publisher_user_ids: list[str],
        existing_profiles: dict[str, dict],
        metadata_client,
        canister_client,
    ) -> dict[str, dict]:
        resolved, _failed_ids, _refreshed_ids = await self._resolve_profiles_with_status(
            publisher_user_ids,
            existing_profiles=existing_profiles,
            metadata_client=metadata_client,
            canister_client=canister_client,
            raise_on_total_failure=False,
        )
        return resolved

    async def _resolve_profiles_with_status(
        self,
        publisher_user_ids: list[str],
        existing_profiles: dict[str, dict],
        metadata_client,
        canister_client,
        raise_on_total_failure: bool,
    ) -> tuple[dict[str, dict], list[str], list[str]]:
        unique_ids = self._dedupe_publisher_ids(publisher_user_ids)
        if not unique_ids:
            return {}, [], []

        usernames_task = asyncio.create_task(
            self._fetch_usernames(unique_ids, metadata_client)
        )
        profiles_task = asyncio.create_task(
            self._fetch_profiles(unique_ids, canister_client)
        )
        (username_map, username_failed_ids, usernames_succeeded), (
            profile_map,
            profile_failed_ids,
            profiles_succeeded,
        ) = await asyncio.gather(usernames_task, profiles_task)

        if raise_on_total_failure and not usernames_succeeded and not profiles_succeeded:
            raise RuntimeError("publisher profile upstreams unavailable")

        now = int(time.time())
        resolved: dict[str, dict] = {}
        refreshed_ids: list[str] = []
        for publisher_id in unique_ids:
            base_profile = existing_profiles.get(publisher_id)
            merged_profile, refreshed = self._merge_profile_payload(
                base_profile=base_profile,
                username=username_map.get(publisher_id),
                username_found=publisher_id in username_map,
                profile=profile_map.get(publisher_id),
                profile_found=publisher_id in profile_map,
                now=now,
            )
            if self._has_usable_cached_value(merged_profile):
                resolved[publisher_id] = merged_profile
            if refreshed:
                refreshed_ids.append(publisher_id)

        return (
            resolved,
            self._dedupe_publisher_ids(username_failed_ids + profile_failed_ids),
            refreshed_ids,
        )

    async def _fetch_usernames(self, publisher_user_ids: list[str], metadata_client):
        return await self._fetch_chunked_source_map(
            publisher_user_ids,
            metadata_client.get_usernames_bulk,
            "Metadata service bulk username lookup failed",
        )

    async def _fetch_profiles(self, publisher_user_ids: list[str], canister_client):
        return await self._fetch_chunked_source_map(
            publisher_user_ids,
            canister_client.get_users_profile_details,
            "User info service bulk profile lookup failed",
        )

    async def _get_cached_profiles(self, publisher_user_ids: list[str]) -> dict[str, dict]:
        try:
            return await self._kv_publisher_profile_repository.get_profiles_batch(
                publisher_user_ids
            )
        except Exception:
            self._log.warning(
                "Publisher profile cache lookup failed",
                extra={"publisher_count": len(publisher_user_ids)},
                exc_info=True,
            )
            return {}

    async def _cache_profiles(self, profiles_by_publisher_id: dict[str, dict]) -> int:
        try:
            return await self._kv_publisher_profile_repository.cache_profiles_batch(
                profiles_by_publisher_id
            )
        except Exception:
            self._log.warning(
                "Publisher profile cache write failed",
                extra={"publisher_count": len(profiles_by_publisher_id)},
                exc_info=True,
            )
            return 0

    async def _enqueue_refresh(self, publisher_user_ids: list[str]) -> int:
        unique_ids = self._dedupe_publisher_ids(publisher_user_ids)
        if not unique_ids:
            return 0
        try:
            return await self._kv_publisher_profile_repository.enqueue_refresh(unique_ids)
        except Exception:
            self._log.warning(
                "Publisher profile refresh queue write failed",
                extra={"publisher_count": len(unique_ids)},
                exc_info=True,
            )
            return 0

    async def _get_following_status_map(
        self,
        viewer_user_id: str,
        publisher_user_ids: list[str],
    ) -> dict[str, bool]:
        unique_ids = self._dedupe_publisher_ids(publisher_user_ids)
        if not viewer_user_id or not unique_ids:
            return {publisher_id: False for publisher_id in unique_ids}

        try:
            async with self._follow_lookup_semaphore:
                return await self._clickhouse_feed_repository.get_following_status_batch(
                    viewer_user_id,
                    unique_ids,
                )
        except Exception:
            self._log.warning(
                "Request-time follow status lookup failed",
                extra={
                    "viewer_user_id": viewer_user_id,
                    "publisher_count": len(unique_ids),
                },
                exc_info=True,
            )
            return {publisher_id: False for publisher_id in unique_ids}

    async def _acquire_refresh_locks(
        self,
        publisher_user_ids: list[str],
    ) -> dict[str, str]:
        lock_ttl = self._settings.feed_recsys_publisher_profile_refresh_lock_ttl_sec
        acquired: dict[str, str] = {}
        for publisher_id in publisher_user_ids:
            try:
                token = await self._kv_publisher_profile_repository.acquire_refresh_lock(
                    publisher_id,
                    lock_ttl,
                )
                if token:
                    acquired[publisher_id] = token
            except Exception:
                self._log.warning(
                    "Publisher profile refresh lock acquisition failed",
                    extra={"publisher_user_id": publisher_id},
                    exc_info=True,
                )
        return acquired

    async def _release_refresh_locks(self, lock_tokens: dict[str, str]) -> None:
        for publisher_id, token in lock_tokens.items():
            try:
                await self._kv_publisher_profile_repository.release_refresh_lock(
                    publisher_id,
                    token,
                )
            except Exception:
                self._log.warning(
                    "Publisher profile refresh lock release failed",
                    extra={"publisher_user_id": publisher_id},
                    exc_info=True,
                )

    def _partition_cached_profiles(
        self,
        publisher_user_ids: list[str],
        cached_profiles: dict[str, dict],
        now: int,
    ) -> tuple[list[str], list[str], list[str]]:
        fresh_ids: list[str] = []
        stale_ids: list[str] = []
        miss_ids: list[str] = []

        for publisher_id in publisher_user_ids:
            profile = cached_profiles.get(publisher_id)
            if not profile:
                miss_ids.append(publisher_id)
                continue

            normalized = self._normalize_profile_payload(profile)
            if self._is_profile_fresh(normalized, now):
                fresh_ids.append(publisher_id)
            elif self._has_usable_cached_value(normalized):
                stale_ids.append(publisher_id)
            else:
                miss_ids.append(publisher_id)

        return fresh_ids, stale_ids, miss_ids

    def _is_profile_fresh(self, payload: dict, now: int) -> bool:
        username_fetched_at = int(payload.get("username_fetched_at") or 0)
        profile_fetched_at = int(payload.get("profile_fetched_at") or 0)
        if username_fetched_at <= 0 or profile_fetched_at <= 0:
            return False

        username_age = max(0, now - username_fetched_at)
        profile_age = max(0, now - profile_fetched_at)
        return (
            username_age
            <= self._settings.feed_recsys_publisher_username_stale_after_sec
            and profile_age
            <= self._settings.feed_recsys_publisher_profile_stale_after_sec
        )

    def _merge_profile_payload(
        self,
        base_profile: dict | None,
        username: str | None,
        username_found: bool,
        profile: dict | None,
        profile_found: bool,
        now: int,
    ) -> tuple[dict, bool]:
        merged = self._normalize_profile_payload(base_profile)
        refreshed = False
        if username_found:
            merged["username"] = str(username or "").strip()
            merged["username_fetched_at"] = now
            refreshed = True
        if profile_found:
            profile = profile or {}
            merged["profile_image_url"] = str(
                profile.get("profile_image_url") or ""
            ).strip()
            merged["is_pro_user"] = bool(profile.get("is_pro_user") or False)
            merged["profile_fetched_at"] = now
            refreshed = True
        return merged, refreshed

    async def _fetch_chunked_source_map(
        self,
        publisher_user_ids: list[str],
        fetcher,
        error_message: str,
    ) -> tuple[dict[str, object], list[str], bool]:
        batches = self._chunk_publisher_ids(publisher_user_ids)
        if not batches:
            return {}, [], False

        tasks = [asyncio.create_task(fetcher(batch)) for batch in batches]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        merged: dict[str, object] = {}
        failed_ids: list[str] = []
        succeeded = False
        for batch, result in zip(batches, results):
            if isinstance(result, Exception):
                self._log.warning(
                    error_message,
                    extra={
                        "publisher_count": len(batch),
                        "error": str(result),
                    },
                )
                failed_ids.extend(batch)
                continue

            succeeded = True
            if isinstance(result, dict):
                merged.update(result)

        return merged, self._dedupe_publisher_ids(failed_ids), succeeded

    @staticmethod
    def _normalize_profile_payload(profile: dict | None) -> dict:
        profile = profile or {}
        return {
            "username": str(profile.get("username") or "").strip(),
            "profile_image_url": str(profile.get("profile_image_url") or "").strip(),
            "is_pro_user": bool(profile.get("is_pro_user") or False),
            "username_fetched_at": int(profile.get("username_fetched_at") or 0),
            "profile_fetched_at": int(profile.get("profile_fetched_at") or 0),
        }

    @staticmethod
    def _has_usable_cached_value(profile: dict) -> bool:
        return bool(
            profile.get("username")
            or profile.get("profile_image_url")
            or profile.get("is_pro_user")
            or profile.get("username_fetched_at")
            or profile.get("profile_fetched_at")
        )

    @staticmethod
    def _dedupe_publisher_ids(publisher_user_ids) -> list[str]:
        return list(
            dict.fromkeys(
                str(publisher_id or "").strip()
                for publisher_id in publisher_user_ids
                if publisher_id
            )
        )

    def _schedule_refresh_enqueue(self, publisher_user_ids: list[str]) -> None:
        asyncio.create_task(self._enqueue_refresh(publisher_user_ids))

    def _chunk_publisher_ids(self, publisher_user_ids: list[str]) -> list[list[str]]:
        unique_ids = self._dedupe_publisher_ids(publisher_user_ids)
        if not unique_ids:
            return []

        chunk_size = self._publisher_batch_size(
            getattr(
                self._settings,
                "feed_recsys_publisher_profile_upstream_chunk_size",
                MAX_PUBLISHER_BATCH_SIZE,
            )
        )
        return [
            unique_ids[index:index + chunk_size]
            for index in range(0, len(unique_ids), chunk_size)
        ]

    @staticmethod
    def _publisher_batch_size(configured_size: int) -> int:
        return min(MAX_PUBLISHER_BATCH_SIZE, max(1, int(configured_size or 0)))
