from src.models.influencer import InfluencerScore
from src.services.feed_mixer_service import FeedMixerService


def _score(
    influencer_id: str,
    engagement_score: float,
    discovery_score: float,
    eligible_for_discovery: bool,
) -> InfluencerScore:
    return InfluencerScore(
        influencer_id=influencer_id,
        engagement_score=engagement_score,
        discovery_score=discovery_score,
        momentum_score=0.5 if eligible_for_discovery else 0.0,
        newness_score=0.8 if eligible_for_discovery else 0.0,
        underexposure_score=0.7 if eligible_for_discovery else 0.0,
        content_activity_score=0.3 if eligible_for_discovery else 0.0,
        depth_quality_score=0.2,
        eligible_for_discovery=eligible_for_discovery,
    )


def test_feed_mixer_applies_expected_60_40_pattern(settings):
    service = FeedMixerService(settings)
    scores = [
        _score("e1", 0.99, 0.0, False),
        _score("e2", 0.98, 0.0, False),
        _score("e3", 0.97, 0.0, False),
        _score("e4", 0.96, 0.0, False),
        _score("e5", 0.95, 0.0, False),
        _score("e6", 0.94, 0.0, False),
        _score("d1", 0.50, 0.99, True),
        _score("d2", 0.49, 0.98, True),
        _score("d3", 0.48, 0.97, True),
        _score("d4", 0.47, 0.96, True),
    ]

    ranked = service.rank_scores(scores)

    assert [score.selected_rank_source for score in ranked] == [
        "engagement",
        "engagement",
        "discovery",
        "engagement",
        "discovery",
        "engagement",
        "engagement",
        "discovery",
        "engagement",
        "discovery",
    ]
    assert [score.influencer_id for score in ranked] == [
        "e1",
        "e2",
        "d1",
        "e3",
        "d2",
        "e4",
        "e5",
        "d3",
        "e6",
        "d4",
    ]
    assert [score.final_rank for score in ranked] == list(range(1, 11))
