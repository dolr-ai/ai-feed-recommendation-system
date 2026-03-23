from datetime import datetime, timezone

import pytest

from src.services.feed_service import FeedNotReadyError, FeedService


class StubFeedRepo:
    def __init__(self, blobs, total_count):
        self._blobs = blobs
        self._total_count = total_count

    async def get_feed(self, offset: int, limit: int):
        return self._blobs, self._total_count, datetime.now(timezone.utc)


@pytest.mark.asyncio
async def test_feed_service_raises_when_feed_missing():
    service = FeedService(StubFeedRepo([], 0))
    with pytest.raises(FeedNotReadyError):
        await service.get_feed(offset=0, limit=50, with_metadata=False)


@pytest.mark.asyncio
async def test_feed_service_shapes_metadata_response():
    service = FeedService(
        StubFeedRepo(
            [
                {
                    "id": "abc",
                    "name": "abc",
                    "display_name": "ABC",
                    "avatar_url": "https://example.com/x.png",
                    "description": "desc",
                    "category": "gaming",
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "engagement_score": 0.9,
                    "discovery_score": 0.4,
                    "momentum_score": 0.4,
                    "newness_score": 0.8,
                    "underexposure_score": 0.7,
                    "content_activity_score": 0.6,
                    "depth_quality_score": 0.5,
                    "final_rank": 1,
                    "engagement_rank": 1,
                    "discovery_rank": 2,
                    "selected_rank_source": "engagement",
                    "eligible_for_discovery": True,
                    "conversation_count": 10,
                    "message_count": 50,
                    "followers_count": 20,
                    "total_video_views": 100,
                    "share_count_total": 5,
                    "likes_count_total": 15,
                    "avg_watch_pct_mean": 66.0,
                    "ready_post_count": 3,
                    "last_post_at": datetime.now(timezone.utc).isoformat(),
                    "depth_ratio": 5.0,
                    "share_rate": 0.05,
                    "views_per_post": 33.3,
                    "likes_per_post": 5.0,
                    "posting_density": 0.6,
                    "conv_velocity": 2.5,
                }
            ],
            1,
        )
    )

    response = await service.get_feed(offset=0, limit=50, with_metadata=True)

    assert response.total_count == 1
    assert response.influencers[0].scores is not None
    assert response.influencers[0].ranking is not None
    assert response.influencers[0].signals is not None
