from __future__ import annotations

from src.schemas.feed_recsys import FeedVideoMetadata
from src.services.logger_service import LoggerService


class VideoMetadataService:
    def __init__(self, kv_feed_repository):
        self._kv_feed_repository = kv_feed_repository
        self._log = LoggerService().get("feed_recsys_metadata")

    async def build_video_rows(self, video_ids: list[str]) -> list[FeedVideoMetadata]:
        if not video_ids:
            return []

        metadata = await self._kv_feed_repository.get_video_metadata_batch(video_ids)
        ordered_rows = []
        for video_id in video_ids:
            if video_id not in metadata:
                continue
            row = metadata[video_id]
            ordered_rows.append(
                {
                    "video_id": video_id,
                    "canister_id": str(row.get("canister_id") or ""),
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
