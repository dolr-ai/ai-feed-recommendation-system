from __future__ import annotations

import asyncio
import random
import time

from src.services.logger_service import LoggerService


class FeedPoolService:
    def __init__(self, kv_feed_repository, feed_sync_service, settings):
        self._kv_feed_repository = kv_feed_repository
        self._feed_sync_service = feed_sync_service
        self._settings = settings
        self._log = LoggerService().get("feed_recsys_pool")
        self._background_refill_tasks: set[asyncio.Task] = set()

    async def get_video_ids(
        self,
        user_id: str,
        count: int,
        rec_type: str,
    ) -> tuple[list[str], dict[str, int]]:
        await self._bootstrap_if_needed(user_id)

        if rec_type == "mixed":
            return await self._get_mixed_video_ids(user_id, count)

        videos = await self._fetch_pool_videos(user_id, rec_type, count)
        return videos, {rec_type: len(videos)}

    async def _get_mixed_video_ids(self, user_id: str, count: int) -> tuple[list[str], dict[str, int]]:
        sources: dict[str, int] = {}

        following_needed = min(
            self._settings.feed_recsys_following_max_per_request,
            count // 3,
        )
        following_videos = await self._fetch_pool_videos(user_id, "following", following_needed)
        sources["following"] = len(following_videos)

        remaining_slots = max(0, count - len(following_videos))
        ugc_needed = max(1, int(remaining_slots * self._settings.feed_recsys_ugc_ratio))
        ugc_videos = await self._fetch_pool_videos(user_id, "ugc", ugc_needed)
        sources["ugc"] = len(ugc_videos)

        slots_for_pop_fresh = max(0, remaining_slots - len(ugc_videos))
        pop_needed = int(slots_for_pop_fresh * self._settings.feed_recsys_popularity_ratio)
        fresh_needed = max(0, slots_for_pop_fresh - pop_needed)

        popularity_videos = await self._fetch_pool_videos(user_id, "popularity", pop_needed)
        freshness_videos = await self._fetch_pool_videos(user_id, "freshness", fresh_needed)
        sources["popularity"] = len(popularity_videos)
        sources["freshness"] = len(freshness_videos)

        other_videos = popularity_videos + freshness_videos
        random.shuffle(other_videos)

        seg1_size = min(self._settings.feed_recsys_following_first_segment_size, count)
        seg1_following_count = min(
            len(following_videos),
            self._settings.feed_recsys_following_first_segment_max,
        )
        seg1_following_count = max(
            seg1_following_count,
            min(
                self._settings.feed_recsys_following_first_segment_min,
                len(following_videos),
            ),
        )
        segment_1 = list(following_videos[:seg1_following_count])
        seg1_remaining = max(0, seg1_size - len(segment_1))
        segment_1.extend(other_videos[:seg1_remaining])
        random.shuffle(segment_1)

        segment_2 = list(following_videos[seg1_following_count:])
        segment_2.extend(other_videos[seg1_remaining:])
        random.shuffle(segment_2)

        all_videos = segment_1 + segment_2
        all_videos = self._intersperse_ugc(all_videos, ugc_videos)

        remaining_needed = max(0, count - len(all_videos))
        if remaining_needed > 0:
            fallback_videos = await self._fetch_pool_videos(user_id, "fallback", remaining_needed)
            all_videos.extend(fallback_videos)
            sources["fallback"] = len(fallback_videos)

        return all_videos[:count], sources

    async def _fetch_pool_videos(self, user_id: str, pool_name: str, count: int) -> list[str]:
        if count <= 0:
            return []

        selected: list[str] = []
        attempts = 0
        candidate_limit = max(
            count * 4,
            self._settings.feed_recsys_refill_threshold,
        )

        while len(selected) < count and attempts <= self._settings.feed_recsys_refill_max_attempts:
            candidates = await self._kv_feed_repository.get_user_pool(
                user_id,
                pool_name,
                candidate_limit,
                current_time=int(time.time()),
            )
            if not candidates:
                attempts += 1
                refill_target = max(count * 2, self._settings.feed_recsys_refill_threshold)
                await self._refill_pool(user_id, pool_name, refill_target)
                continue

            filtered = await self._filter_unseen_videos(user_id, candidates)
            filtered_set = set(filtered)
            stale_ids = [video_id for video_id in candidates if video_id not in filtered_set]
            if stale_ids:
                await self._kv_feed_repository.remove_user_pool_videos(user_id, pool_name, stale_ids)

            if filtered:
                needed = count - len(selected)
                batch = filtered[:needed]
                await self._kv_feed_repository.remove_user_pool_videos(user_id, pool_name, batch)
                await self._kv_feed_repository.add_served_recent_videos(user_id, batch)
                selected.extend(batch)

                remaining_count = await self._kv_feed_repository.get_user_pool_size(
                    user_id,
                    pool_name,
                    current_time=int(time.time()),
                )
                await self._maybe_schedule_background_refill(
                    user_id,
                    pool_name,
                    request_count=count,
                    remaining_count=remaining_count,
                )

            if len(selected) < count:
                attempts += 1
                refill_target = max(count * 2, self._settings.feed_recsys_refill_threshold)
                await self._refill_pool(user_id, pool_name, refill_target)

        if not selected:
            self._log.debug(
                "Feed recsys pool returned no videos",
                extra={
                    "user_id": user_id,
                    "pool_name": pool_name,
                    "requested_count": count,
                    "attempts": attempts,
                },
            )
        elif len(selected) < count:
            self._log.debug(
                "Feed recsys pool returned partial batch",
                extra={
                    "user_id": user_id,
                    "pool_name": pool_name,
                    "requested_count": count,
                    "selected_count": len(selected),
                    "attempts": attempts,
                },
            )
        return selected[:count]

    async def _bootstrap_if_needed(self, user_id: str) -> None:
        bloom_exists = await self._kv_feed_repository.user_bloom_exists(user_id)
        served_recent_count = await self._kv_feed_repository.get_served_recent_count(user_id)
        if bloom_exists or served_recent_count > 0:
            return

        await self._kv_feed_repository.ensure_user_bloom(user_id)
        await self._kv_feed_repository.set_popularity_pointer(
            user_id,
            self._settings.feed_recsys_popularity_buckets[0],
        )
        popularity_added = await self.refill_popularity(
            user_id,
            self._settings.feed_recsys_refill_threshold * 5,
        )
        freshness_added = await self.refill_freshness(
            user_id,
            self._settings.feed_recsys_refill_threshold * 2,
        )
        ugc_added = await self.refill_ugc(
            user_id,
            max(50, self._settings.feed_recsys_refill_threshold // 2),
        )
        self._log.debug(
            "Feed recsys user bootstrap completed",
            extra={
                "user_id": user_id,
                "popularity_added": popularity_added,
                "freshness_added": freshness_added,
                "ugc_added": ugc_added,
            },
        )

    async def _refill_pool(self, user_id: str, pool_name: str, target: int) -> None:
        if pool_name == "popularity":
            await self.refill_popularity(user_id, target)
            return
        if pool_name == "freshness":
            await self.refill_freshness(user_id, target)
            return
        if pool_name == "ugc":
            await self.refill_ugc(user_id, target)
            return
        if pool_name == "following":
            await self.refill_following(user_id)
            return
        if pool_name == "fallback":
            await self.refill_fallback(user_id, target)

    async def refill_popularity(self, user_id: str, target: int) -> int:
        await self._kv_feed_repository.ensure_user_bloom(user_id)
        buckets = self._settings.feed_recsys_popularity_buckets
        pointer = await self._kv_feed_repository.get_popularity_pointer(user_id, buckets[0])
        start_index = buckets.index(pointer) if pointer in buckets else 0
        added_total = 0
        last_bucket = buckets[start_index]

        for bucket in buckets[start_index:]:
            last_bucket = bucket
            global_ids = await self._kv_feed_repository.get_global_pool(
                f"popular:{bucket}",
                max(target * 3, self._settings.feed_recsys_refill_threshold),
                current_time=int(time.time()),
            )
            if not global_ids:
                continue
            candidates = await self._filter_unseen_videos(user_id, global_ids)
            if not candidates:
                continue
            added_total += await self._kv_feed_repository.add_user_pool_videos(
                user_id,
                "popularity",
                candidates[: max(0, target - added_total)],
                ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
            )
            if added_total >= target:
                break

        await self._kv_feed_repository.set_popularity_pointer(user_id, last_bucket)
        return added_total

    async def refill_freshness(self, user_id: str, target: int) -> int:
        await self._kv_feed_repository.ensure_user_bloom(user_id)
        added_total = 0
        for window in self._settings.feed_recsys_freshness_windows:
            global_ids = await self._kv_feed_repository.get_global_pool(
                f"fresh:{window}",
                max(target * 3, self._settings.feed_recsys_refill_threshold),
                current_time=int(time.time()),
            )
            if not global_ids:
                continue
            candidates = await self._filter_unseen_videos(user_id, global_ids)
            if not candidates:
                continue
            added_total += await self._kv_feed_repository.add_user_pool_videos(
                user_id,
                "freshness",
                candidates[: max(0, target - added_total)],
                ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
            )
            if added_total >= target:
                break
        return added_total

    async def refill_ugc(self, user_id: str, target: int) -> int:
        await self._kv_feed_repository.ensure_user_bloom(user_id)
        global_ids = await self._kv_feed_repository.get_global_pool(
            "ugc_discovery",
            max(target * 3, self._settings.feed_recsys_refill_threshold),
            current_time=int(time.time()),
        )
        candidates = await self._filter_unseen_videos(user_id, global_ids)
        return await self._kv_feed_repository.add_user_pool_videos(
            user_id,
            "ugc",
            candidates[:target],
            ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
        )

    async def refill_following(self, user_id: str) -> None:
        now = int(time.time())
        last_sync = await self._kv_feed_repository.get_following_sync_time(user_id)
        if last_sync is not None and (now - last_sync) < self._settings.feed_recsys_following_sync_cooldown_sec:
            return

        await self._kv_feed_repository.set_following_sync_time(
            user_id,
            now,
            ttl_sec=self._settings.feed_recsys_following_sync_cooldown_sec * 2,
        )
        await self._feed_sync_service.sync_user_following_pool(user_id)

    async def refill_fallback(self, user_id: str, target: int) -> int:
        fallback_sources = [
            "popular:99_100",
            "popular:90_99",
            "fresh:l1d",
            "fresh:l7d",
        ]
        added_total = 0
        for source in fallback_sources:
            global_ids = await self._kv_feed_repository.get_global_pool(
                source,
                max(target * 3, self._settings.feed_recsys_refill_threshold),
                current_time=int(time.time()),
            )
            if not global_ids:
                continue
            candidates = await self._filter_unseen_videos(user_id, global_ids)
            if not candidates:
                continue
            added_total += await self._kv_feed_repository.add_user_pool_videos(
                user_id,
                "fallback",
                candidates[: max(0, target - added_total)],
                ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
            )
            if added_total >= target:
                break
        return added_total

    async def _filter_unseen_videos(self, user_id: str, video_ids: list[str]) -> list[str]:
        if not video_ids:
            return []

        unique_ids = list(dict.fromkeys(video_ids))
        filtered_ids = await self._kv_feed_repository.filter_excluded_videos(unique_ids)
        served_recent_status = await self._kv_feed_repository.check_user_served_recent(
            user_id,
            filtered_ids,
        )
        unserved_ids = [
            video_id
            for video_id in filtered_ids
            if not served_recent_status.get(video_id, False)
        ]
        bloom_status = await self._kv_feed_repository.check_user_bloom(user_id, unserved_ids)
        return [video_id for video_id in unserved_ids if not bloom_status.get(video_id, False)]

    async def close(self) -> None:
        tasks = list(self._background_refill_tasks)
        if not tasks:
            return

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self._background_refill_tasks.clear()

    async def _maybe_schedule_background_refill(
        self,
        user_id: str,
        pool_name: str,
        request_count: int,
        remaining_count: int,
    ) -> None:
        if pool_name == "following":
            threshold = max(
                self._settings.feed_recsys_following_refill_threshold,
                request_count,
            )
        else:
            threshold = max(
                self._settings.feed_recsys_background_refill_threshold,
                request_count * 2,
            )

        if remaining_count >= threshold:
            return

        target = max(
            self._settings.feed_recsys_background_refill_target,
            request_count * 4,
        )
        task = asyncio.create_task(self._run_background_refill(user_id, pool_name, target))
        self._log.debug(
            "Feed recsys background refill scheduled",
            extra={
                "user_id": user_id,
                "pool_name": pool_name,
                "remaining_count": remaining_count,
                "request_count": request_count,
                "target": target,
            },
        )
        self._background_refill_tasks.add(task)
        task.add_done_callback(self._background_refill_tasks.discard)

    async def _run_background_refill(self, user_id: str, pool_name: str, target: int) -> None:
        acquired = await self._kv_feed_repository.acquire_refill_lock(user_id, pool_name)
        if not acquired:
            return

        try:
            remaining_count = await self._kv_feed_repository.get_user_pool_size(
                user_id,
                pool_name,
                current_time=int(time.time()),
            )
            if remaining_count >= self._settings.feed_recsys_background_refill_threshold:
                return
            await self._refill_pool(user_id, pool_name, target)
        except Exception as exc:
            self._log.exception(
                "Feed recsys background refill failed",
                extra={"user_id": user_id, "pool_name": pool_name},
            )
        finally:
            await self._kv_feed_repository.release_refill_lock(user_id, pool_name)

    @staticmethod
    def _intersperse_ugc(videos: list[str], ugc_videos: list[str]) -> list[str]:
        if not ugc_videos:
            return videos

        result = list(videos)
        interval = max(1, len(result) // (len(ugc_videos) + 1))
        for index, ugc_video in enumerate(ugc_videos):
            insert_pos = min((index + 1) * interval, len(result))
            result.insert(insert_pos, ugc_video)
        return result
