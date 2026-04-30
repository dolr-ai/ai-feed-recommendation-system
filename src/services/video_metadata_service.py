from __future__ import annotations

from src.schemas.feed_recsys import FeedVideoMetadata
from src.services.logger_service import LoggerService


class VideoMetadataService:
    def __init__(
        self,
        clickhouse_video_metadata_repository,
        kv_video_metadata_repository,
        kv_feed_repository,
        settings,
    ):
        self._clickhouse_video_metadata_repository = clickhouse_video_metadata_repository
        self._kv_video_metadata_repository = kv_video_metadata_repository
        self._kv_feed_repository = kv_feed_repository
        self._settings = settings
        self._log = LoggerService().get("feed_recsys_metadata")

    async def build_video_rows(self, video_ids: list[str]) -> list[FeedVideoMetadata]:
        if not video_ids:
            return []

        metadata = await self._get_resolved_metadata(video_ids)
        ordered_rows = []
        for video_id in video_ids:
            row = metadata.get(video_id)
            if not self._has_required_metadata(row):
                continue
            ordered_rows.append(
                {
                    "video_id": video_id,
                    "canister_id": str(
                        row.get("canister_id") or self._settings.profile_canister_id
                    ),
                    "post_id": str(row.get("post_id") or ""),
                    "publisher_user_id": str(row.get("publisher_user_id") or ""),
                    "num_views_loggedin": 0,
                    "num_views_all": 0,
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
        clickhouse_metadata = (
            await self._clickhouse_video_metadata_repository.get_video_metadata_batch(
                unique_video_ids
            )
        )
        fallback_video_ids = [
            video_id
            for video_id in unique_video_ids
            if not self._has_required_metadata(clickhouse_metadata.get(video_id))
        ]
        if not fallback_video_ids or self._kv_video_metadata_repository is None:
            return {
                video_id: self._normalize_metadata(clickhouse_metadata.get(video_id))
                for video_id in unique_video_ids
            }

        fallback_metadata: dict[str, dict] = {}
        try:
            fallback_metadata = await self._kv_video_metadata_repository.get_video_metadata_batch(
                fallback_video_ids
            )
        except Exception as exc:
            self._log.warning(
                "Central video metadata fallback lookup failed",
                extra={"error": str(exc), "video_count": len(fallback_video_ids)},
            )

        return {
            video_id: self._merge_metadata_rows(
                clickhouse_metadata.get(video_id),
                fallback_metadata.get(video_id),
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
