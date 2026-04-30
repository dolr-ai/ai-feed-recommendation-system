from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from typing import Iterable, Optional

from src.services.logger_service import LoggerService


class FeedSyncService:
    def __init__(
        self,
        clickhouse_feed_repository,
        clickhouse_video_metadata_repository,
        kv_feed_repository,
        chat_api_client,
        settings,
    ):
        self._clickhouse_feed_repository = clickhouse_feed_repository
        self._clickhouse_video_metadata_repository = clickhouse_video_metadata_repository
        self._kv_feed_repository = kv_feed_repository
        self._chat_api_client = chat_api_client
        self._settings = settings
        self._log = LoggerService().get("feed_recsys_sync")

    async def sync_global_popularity_pools(self) -> dict[str, int]:
        rows = await self._clickhouse_feed_repository.get_global_popular_videos()
        grouped = self._bucket_popular_video_ids(rows)
        inserted: dict[str, int] = {}
        for bucket, video_ids in grouped.items():
            filtered_ids = await self._filter_video_ids_with_metadata(video_ids)
            inserted[bucket] = await self._kv_feed_repository.replace_global_pool(
                f"popular:{bucket}",
                filtered_ids,
                ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
            )
        self._log.info(
            "Feed recsys popularity sync completed",
            extra={"bucket_count": len(inserted), "video_count": sum(inserted.values())},
        )
        return inserted

    async def sync_fresh_pools(self) -> dict[str, int]:
        rows = await self._clickhouse_feed_repository.get_fresh_videos()
        grouped: dict[str, list[str]] = defaultdict(list)
        for row in rows:
            bucket = row.get("bucket")
            video_id = row.get("video_id")
            if not bucket or not video_id:
                continue
            grouped[bucket].append(video_id)

        inserted: dict[str, int] = {}
        for window in self._settings.feed_recsys_freshness_windows:
            filtered_ids = await self._filter_video_ids_with_metadata(grouped.get(window, []))
            inserted[window] = await self._kv_feed_repository.replace_global_pool(
                f"fresh:{window}",
                filtered_ids,
                ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
            )
        self._log.info(
            "Feed recsys freshness sync completed",
            extra={"window_count": len(inserted), "video_count": sum(inserted.values())},
        )
        return inserted

    async def sync_user_bloom_filters(self) -> dict[str, int]:
        rows = await self._clickhouse_feed_repository.get_user_watch_history()
        grouped: dict[str, list[str]] = defaultdict(list)
        for row in rows:
            user_id = row.get("user_id")
            video_id = row.get("video_id")
            if not user_id or not video_id:
                continue
            grouped[user_id].append(video_id)

        synced_users = 0
        synced_videos = 0
        for user_id, video_ids in grouped.items():
            await self._kv_feed_repository.ensure_user_bloom(user_id)
            synced_videos += await self._kv_feed_repository.add_to_user_bloom(
                user_id,
                list(dict.fromkeys(video_ids)),
            )
            synced_users += 1

        self._log.info(
            "Feed recsys bloom sync completed",
            extra={"user_count": synced_users, "video_count": synced_videos},
        )
        return {"users": synced_users, "videos": synced_videos}

    async def sync_user_following_pool(self, user_id: str) -> dict[str, int]:
        rows = await self._clickhouse_feed_repository.get_following_video_candidates(
            user_id=user_id,
            num_videos=self._settings.feed_recsys_following_fetch_limit,
        )
        video_ids = [
            row["video_id"]
            for row in rows
            if row.get("video_id")
        ]
        filtered_ids = await self._filter_video_ids_with_metadata(video_ids)
        added = await self._kv_feed_repository.replace_user_pool(
            user_id,
            "following",
            filtered_ids,
            ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
        )
        await self._kv_feed_repository.set_following_sync_time(
            user_id,
            ttl_sec=self._settings.feed_recsys_following_sync_cooldown_sec * 2,
        )
        self._log.info(
            "Feed recsys following sync completed",
            extra={"user_id": user_id, "video_count": added},
        )
        return {"fetched": len(rows), "added": added}

    async def sync_ugc_pool(self) -> dict[str, int]:
        rows = await self._clickhouse_feed_repository.get_ugc_videos(
            limit=self._settings.feed_recsys_ugc_pool_limit,
        )
        filtered_ids = await self._filter_video_ids_with_metadata(
            [row["video_id"] for row in rows if row.get("video_id")]
        )
        inserted = await self._kv_feed_repository.replace_global_pool(
            "ugc",
            filtered_ids,
            ttl_sec=self._settings.feed_recsys_pool_ttl_sec,
        )
        self._log.info(
            "Feed recsys ugc sync completed",
            extra={"video_count": inserted},
        )
        return {"videos": inserted}

    async def sync_ugc_discovery_pool(self) -> dict[str, int]:
        rows = await self._clickhouse_feed_repository.get_ugc_discovery_videos(
            max_views=self._settings.feed_recsys_ugc_discovery_max_views,
            max_age_days=self._settings.feed_recsys_ugc_discovery_max_age_days,
            limit=self._settings.feed_recsys_ugc_discovery_pool_limit,
        )
        filtered_ids = await self._filter_video_ids_with_metadata(
            [row["video_id"] for row in rows if row.get("video_id")]
        )
        filtered_set = set(filtered_ids)
        discovery_rows = [
            {
                "video_id": row["video_id"],
                "upload_timestamp": self._to_unix_timestamp(row.get("upload_timestamp")),
            }
            for row in rows
            if row.get("video_id") in filtered_set
        ]
        inserted = await self._kv_feed_repository.replace_ugc_discovery_pool(
            discovery_rows,
            ttl_sec=self._settings.feed_recsys_ugc_discovery_pool_ttl_sec,
        )
        self._log.info(
            "Feed recsys ugc discovery sync completed",
            extra={"video_count": inserted},
        )
        return {"videos": inserted}

    async def sync_excluded_videos(self) -> dict[str, int]:
        video_ids = await self._clickhouse_feed_repository.get_excluded_video_ids()
        inserted = await self._kv_feed_repository.replace_excluded_videos(video_ids)
        self._log.info(
            "Feed recsys exclude sync completed",
            extra={"video_count": inserted},
        )
        return {"videos": inserted}

    async def sync_ai_influencer_ids(self) -> dict[str, int]:
        influencers = await self._chat_api_client.get_all_influencers()
        user_ids = self._extract_ai_influencer_ids(influencers)
        inserted = await self._kv_feed_repository.replace_ai_influencer_ids(user_ids)
        self._log.info(
            "Feed recsys AI influencer sync completed",
            extra={"influencer_count": inserted},
        )
        return {"influencers": inserted}

    async def _filter_video_ids_with_metadata(self, video_ids: Iterable[str]) -> list[str]:
        unique_video_ids = list(dict.fromkeys(video_id for video_id in video_ids if video_id))
        if not unique_video_ids:
            return []

        result: list[str] = []
        batch_size = 500
        for index in range(0, len(unique_video_ids), batch_size):
            batch = unique_video_ids[index:index + batch_size]
            metadata = await self._clickhouse_video_metadata_repository.get_video_metadata_batch(
                batch
            )
            result.extend(
                [
                    video_id
                    for video_id in batch
                    if self._has_required_metadata(metadata.get(video_id))
                ]
            )
        return result

    @staticmethod
    def _has_required_metadata(metadata: Optional[dict]) -> bool:
        if not metadata:
            return False
        return bool(metadata.get("post_id") and metadata.get("publisher_user_id"))

    def _bucket_popular_video_ids(self, rows: list[dict]) -> dict[str, list[str]]:
        buckets = {bucket: [] for bucket in self._settings.feed_recsys_popularity_buckets}
        total = len(rows)
        if total == 0:
            return buckets

        thresholds = [
            (0.01, "99_100"),
            (0.10, "90_99"),
            (0.20, "80_90"),
            (0.30, "70_80"),
            (0.40, "60_70"),
            (0.50, "50_60"),
            (0.60, "40_50"),
            (0.70, "30_40"),
            (0.80, "20_30"),
            (0.90, "10_20"),
            (1.01, "0_10"),
        ]

        for index, row in enumerate(rows):
            video_id = row.get("video_id")
            if not video_id:
                continue
            fraction = (index + 1) / total
            for threshold, bucket in thresholds:
                if fraction <= threshold:
                    buckets[bucket].append(video_id)
                    break
        return buckets

    @staticmethod
    def _extract_ai_influencer_ids(records: list[dict]) -> list[str]:
        user_ids: list[str] = []
        for item in records:
            raw_id = item.get("Id") or item.get("id")
            if raw_id:
                user_ids.append(str(raw_id))
        return list(dict.fromkeys(user_ids))

    @staticmethod
    def _to_unix_timestamp(value) -> int:
        if value is None:
            return 0
        if isinstance(value, int):
            return value
        if isinstance(value, float):
            return int(value)
        if isinstance(value, datetime):
            if value.tzinfo is None:
                value = value.replace(tzinfo=timezone.utc)
            return int(value.timestamp())
        return int(datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp())
