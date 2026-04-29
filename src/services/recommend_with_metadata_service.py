from __future__ import annotations

import time

from src.schemas.feed_recsys import FeedRecommendationWithMetadataResponse


class RecommendWithMetadataService:
    def __init__(self, feed_pool_service, video_metadata_service):
        self._feed_pool_service = feed_pool_service
        self._video_metadata_service = video_metadata_service

    async def recommend_with_metadata(
        self,
        user_id: str,
        count: int,
        rec_type: str,
    ) -> FeedRecommendationWithMetadataResponse:
        video_ids, sources = await self._feed_pool_service.get_video_ids(
            user_id,
            count,
            rec_type,
        )
        videos = await self._video_metadata_service.build_video_rows(video_ids)
        return FeedRecommendationWithMetadataResponse(
            user_id=user_id,
            videos=videos,
            count=len(videos),
            sources=sources,
            timestamp=int(time.time()),
        )
