from datetime import datetime, timedelta, timezone

import pytest

from src.services.feed_mixer_service import FeedMixerService
from src.services.discovery_boost_service import DiscoveryBoostService
from src.services.scoring_service import ScoringService


class StubRepo:
    def __init__(self, now: datetime):
        self.now = now
        self.rewritten = None
        self.metadata = [
            {
                "id": "eligible",
                "created_at": (now - timedelta(days=1)).isoformat(),
                "conversation_count": 40,
                "message_count": 100,
                "followers_count": 4,
                "ready_post_count": 1,
                "engagement_score": 0.60,
                "discovery_score": 0.30,
                "momentum_score": 0.8,
                "newness_score": 0.0,
                "underexposure_score": 0.0,
                "content_activity_score": 0.25,
                "depth_quality_score": 0.20,
                "eligible_for_discovery": True,
                "engagement_rank": 1,
                "discovery_rank": 1,
                "final_rank": 1,
                "selected_rank_source": "engagement",
            },
            {
                "id": "expired",
                "created_at": (now - timedelta(days=20)).isoformat(),
                "conversation_count": 50,
                "message_count": 60,
                "followers_count": 2,
                "ready_post_count": 0,
                "engagement_score": 0.70,
                "discovery_score": 0.20,
                "momentum_score": 0.2,
                "newness_score": 0.0,
                "underexposure_score": 0.0,
                "content_activity_score": 0.0,
                "depth_quality_score": 0.10,
                "eligible_for_discovery": True,
                "engagement_rank": 2,
                "discovery_rank": 2,
                "final_rank": 2,
                "selected_rank_source": "engagement",
            },
        ]
        self.serve_counts = {"eligible": 5.0, "expired": 1.0}

    async def get_all_metadata(self):
        return [dict(row) for row in self.metadata]

    async def get_all_serve_counts(self, influencer_ids):
        return {influencer_id: self.serve_counts.get(influencer_id, 0.0) for influencer_id in influencer_ids}

    async def rewrite_ranked_feed_rows(self, rows):
        self.rewritten = rows


@pytest.mark.asyncio
async def test_discovery_boost_refresh_updates_eligible_and_resets_expired(settings):
    now = datetime.now(timezone.utc)
    settings.curated_top_influencer_ids = ["expired"]
    repo = StubRepo(now)
    service = DiscoveryBoostService(
        repo,
        ScoringService(settings),
        FeedMixerService(settings),
        settings,
    )

    await service.refresh()

    assert repo.rewritten is not None
    assert repo.rewritten[0]["id"] == "expired"
    row_map = {row["id"]: row for row in repo.rewritten}
    assert row_map["eligible"]["eligible_for_discovery"] is True
    assert row_map["eligible"]["discovery_score"] > 0.0
    assert row_map["expired"]["eligible_for_discovery"] is False
    assert row_map["expired"]["discovery_score"] == 0.0
    assert row_map["expired"]["selected_rank_source"] == "curated"
