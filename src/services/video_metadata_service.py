from __future__ import annotations

import asyncio

from src.schemas.feed_recsys import FeedVideoMetadata
from src.services.logger_service import LoggerService


class VideoMetadataService:
    def __init__(
        self,
        clickhouse_video_metadata_repository,
        kv_video_metadata_repository,
        kv_feed_repository,
        offchain_rewards_client,
        settings,
    ):
        self._clickhouse_video_metadata_repository = clickhouse_video_metadata_repository
        self._kv_video_metadata_repository = kv_video_metadata_repository
        self._kv_feed_repository = kv_feed_repository
        self._offchain_rewards_client = offchain_rewards_client
        self._settings = settings
        self._log = LoggerService().get("feed_recsys_metadata")

    async def build_video_rows(self, video_ids: list[str]) -> list[FeedVideoMetadata]:
        if not video_ids:
            return []

        metadata, view_counts = await asyncio.gather(
            self._get_resolved_metadata(video_ids),
            self._get_view_counts(video_ids),
        )
        ordered_rows = []
        for video_id in video_ids:
            row = metadata.get(video_id)
            if not self._has_required_metadata(row):
                continue
            counts = view_counts.get(video_id, {})
            ordered_rows.append(
                {
                    "video_id": video_id,
                    "canister_id": str(
                        row.get("canister_id") or self._settings.profile_canister_id
                    ),
                    "post_id": str(row.get("post_id") or ""),
                    "publisher_user_id": str(row.get("publisher_user_id") or ""),
                    "num_views_all": int(counts.get("num_views_all") or 0),
                }
            )

        await self._attach_ai_influencer_flags(ordered_rows)
        return [FeedVideoMetadata.model_validate(row) for row in ordered_rows]

    async def _attach_ai_influencer_flags(self, rows: list[dict]) -> None:
        publisher_ids = list(
            dict.fromkeys(
                row["publisher_user_id"]
                for row in rows
                if row.get("publisher_user_id")
            )
        )
        membership = await self._kv_feed_repository.check_ai_influencer_ids(publisher_ids)
        for row in rows:
            row["from_ai_influencer"] = membership.get(row.get("publisher_user_id", ""), False)

    async def _get_resolved_metadata(self, video_ids: list[str]) -> dict[str, dict]:
        unique_video_ids = list(dict.fromkeys(video_id for video_id in video_ids if video_id))
        if not unique_video_ids:
            return {}

        cached_metadata: dict[str, dict] = {}
        if self._kv_video_metadata_repository is not None:
            try:
                cached_metadata = await self._kv_video_metadata_repository.get_video_metadata_batch(
                    unique_video_ids
                )
            except Exception:
                self._log.warning(
                    "Central video metadata cache lookup failed",
                    extra={"video_count": len(unique_video_ids)},
                    exc_info=True,
                )

        fallback_video_ids = [
            video_id
            for video_id in unique_video_ids
            if not self._has_required_metadata(cached_metadata.get(video_id))
        ]
        if not fallback_video_ids:
            return {
                video_id: self._normalize_metadata(cached_metadata.get(video_id))
                for video_id in unique_video_ids
            }

        try:
            clickhouse_metadata = (
                await self._clickhouse_video_metadata_repository.get_video_metadata_batch(
                    fallback_video_ids
                )
            )
        except Exception:
            self._log.error(
                "ClickHouse video metadata lookup failed",
                extra={"video_count": len(fallback_video_ids)},
                exc_info=True,
            )
            raise
        if clickhouse_metadata and self._kv_video_metadata_repository is not None:
            try:
                await self._kv_video_metadata_repository.cache_video_metadata_batch(
                    {
                        video_id: row
                        for video_id, row in clickhouse_metadata.items()
                        if self._has_required_metadata(row)
                    }
                )
            except Exception:
                self._log.warning(
                    "Central video metadata cache backfill failed",
                    extra={"video_count": len(clickhouse_metadata)},
                    exc_info=True,
                )

        return {
            video_id: self._merge_metadata_rows(
                cached_metadata.get(video_id),
                clickhouse_metadata.get(video_id),
            )
            for video_id in unique_video_ids
        }

    def _merge_metadata_rows(
        self,
        primary: dict | None,
        fallback: dict | None,
    ) -> dict:
        primary_row = self._normalize_metadata(primary)
        fallback_row = self._normalize_metadata(fallback)
        return {
            "canister_id": primary_row["canister_id"] or fallback_row["canister_id"],
            "post_id": primary_row["post_id"] or fallback_row["post_id"],
            "publisher_user_id": (
                primary_row["publisher_user_id"] or fallback_row["publisher_user_id"]
            ),
        }

    @staticmethod
    def _has_required_metadata(metadata: dict | None) -> bool:
        if not metadata:
            return False
        return bool(metadata.get("post_id") and metadata.get("publisher_user_id"))

    @staticmethod
    def _normalize_metadata(metadata: dict | None) -> dict:
        if not metadata:
            return {
                "canister_id": "",
                "post_id": "",
                "publisher_user_id": "",
            }
        return {
            "canister_id": str(metadata.get("canister_id") or "").strip(),
            "post_id": str(metadata.get("post_id") or "").strip(),
            "publisher_user_id": str(metadata.get("publisher_user_id") or "").strip(),
        }

    async def _get_view_counts(self, video_ids: list[str]) -> dict[str, dict[str, int]]:
        unique_video_ids = list(dict.fromkeys(video_id for video_id in video_ids if video_id))
        if not unique_video_ids:
            return {}

        cached_counts = await self._kv_feed_repository.get_cached_video_view_counts(unique_video_ids)
        missing_video_ids = [
            video_id
            for video_id in unique_video_ids
            if video_id not in cached_counts
        ]
        if not missing_video_ids or self._offchain_rewards_client is None:
            return cached_counts

        fresh_counts: dict[str, dict[str, int]] = {}
        try:
            fresh_counts = await self._offchain_rewards_client.get_bulk_video_stats(missing_video_ids)
        except Exception:
            self._log.warning(
                "Offchain rewards view-count lookup failed",
                extra={"video_count": len(missing_video_ids)},
                exc_info=True,
            )
            return cached_counts

        if fresh_counts:
            await self._kv_feed_repository.upsert_video_view_counts(fresh_counts)
        return {**cached_counts, **fresh_counts}
