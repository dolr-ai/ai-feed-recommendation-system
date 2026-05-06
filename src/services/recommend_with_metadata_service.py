from __future__ import annotations

import time

from src.schemas.feed_recsys import FeedRecommendationWithMetadataResponse
from src.services.logger_service import LoggerService


class RecommendWithMetadataService:
    def __init__(
        self,
        feed_pool_service,
        video_metadata_service,
        publisher_profile_enrichment_service,
    ):
        self._feed_pool_service = feed_pool_service
        self._video_metadata_service = video_metadata_service
        self._publisher_profile_enrichment_service = publisher_profile_enrichment_service
        self._log = LoggerService().get("feed_recsys_request")

    async def recommend_with_metadata(
        self,
        user_id: str,
        count: int,
        rec_type: str,
    ) -> FeedRecommendationWithMetadataResponse:
        try:
            video_ids, sources = await self._feed_pool_service.get_video_ids(
                user_id,
                count,
                rec_type,
            )
            videos = await self._video_metadata_service.build_video_rows(video_ids)
            videos = await self._publisher_profile_enrichment_service.enrich_rows(
                user_id,
                videos,
            )
            if not videos:
                self._log.warning(
                    "Feed recsys request returned no videos",
                    extra={
                        "user_id": user_id,
                        "rec_type": rec_type,
                        "requested_count": count,
                        "selected_count": len(video_ids),
                        "returned_count": len(videos),
                        "sources": sources,
                    },
                )
            elif len(video_ids) < count or len(videos) < len(video_ids):
                self._log.info(
                    "Feed recsys request returned a partial feed",
                    extra={
                        "user_id": user_id,
                        "rec_type": rec_type,
                        "requested_count": count,
                        "selected_count": len(video_ids),
                        "returned_count": len(videos),
                        "sources": sources,
                    },
                )
            return FeedRecommendationWithMetadataResponse(
                user_id=user_id,
                videos=videos,
                count=len(videos),
                sources=sources,
                timestamp=int(time.time()),
            )
        except Exception:
            self._log.exception(
                "Feed recsys request failed",
                extra={
                    "user_id": user_id,
                    "rec_type": rec_type,
                    "requested_count": count,
                },
            )
            raise
