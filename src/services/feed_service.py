from datetime import datetime, timezone

from src.schemas.influencer import FeedResponse, InfluencerResponse
from src.services.logger_service import LoggerService


class FeedNotReadyError(Exception):
    pass


class FeedService:
    def __init__(self, repo):
        self._repo = repo
        self._log = LoggerService().get("feed")

    async def get_feed(
        self,
        offset: int,
        limit: int,
        with_metadata: bool,
    ) -> FeedResponse:
        blobs, total_count, feed_generated_at = await self._repo.get_feed(offset, limit)
        if total_count == 0:
            raise FeedNotReadyError()

        influencers = [
            self._build_response(blob, with_metadata)
            for blob in blobs
        ]
        has_more = (offset + len(influencers)) < total_count
        return FeedResponse(
            influencers=influencers,
            total_count=total_count,
            offset=offset,
            limit=limit,
            has_more=has_more,
            feed_generated_at=feed_generated_at or datetime.now(timezone.utc),
        )

    @staticmethod
    def _build_response(blob: dict, with_metadata: bool) -> InfluencerResponse:
        base = {
            "id": blob["id"],
            "name": blob["name"],
            "display_name": blob["display_name"],
            "avatar_url": blob["avatar_url"],
            "description": blob["description"],
            "category": blob["category"],
            "created_at": blob["created_at"],
        }
        if with_metadata:
            base["scores"] = {
                "engagement_score": blob["engagement_score"],
                "discovery_score": blob["discovery_score"],
                "momentum_score": blob["momentum_score"],
                "newness_score": blob["newness_score"],
                "underexposure_score": blob["underexposure_score"],
                "content_activity_score": blob["content_activity_score"],
                "depth_quality_score": blob["depth_quality_score"],
            }
            base["ranking"] = {
                "final_rank": blob["final_rank"],
                "engagement_rank": blob["engagement_rank"],
                "discovery_rank": blob["discovery_rank"],
                "selected_rank_source": blob["selected_rank_source"],
                "eligible_for_discovery": blob["eligible_for_discovery"],
            }
            base["signals"] = {
                "conversation_count": blob["conversation_count"],
                "message_count": blob["message_count"],
                "followers_count": blob["followers_count"],
                "total_video_views": blob["total_video_views"],
                "share_count_total": blob["share_count_total"],
                "likes_count_total": blob["likes_count_total"],
                "avg_watch_pct_mean": blob["avg_watch_pct_mean"],
                "ready_post_count": blob["ready_post_count"],
                "last_post_at": blob["last_post_at"],
                "depth_ratio": blob["depth_ratio"],
                "share_rate": blob["share_rate"],
                "views_per_post": blob["views_per_post"],
                "likes_per_post": blob["likes_per_post"],
                "posting_density": blob["posting_density"],
                "conv_velocity": blob["conv_velocity"],
            }
        return InfluencerResponse.model_validate(base)
