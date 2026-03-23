import math

from src.services.scoring_service import ScoringService


def test_scoring_service_computes_scores_and_boosts(
    settings,
    sample_influencers,
    sample_snapshot_map,
):
    service = ScoringService(settings)
    scores = service.score(
        influencers=sample_influencers,
        snapshot_map=sample_snapshot_map,
        serve_counts_map={"new-hot": 5.0, "new-overexposed": 30.0},
    )

    score_map = {score.influencer_id: score for score in scores}
    assert len(scores) == 3
    assert score_map["new-hot"].eligible_for_discovery is True
    assert score_map["new-hot"].discovery_score > 0.0
    assert score_map["old-stable"].eligible_for_discovery is False
    assert score_map["old-stable"].discovery_score == 0.0
    assert score_map["new-overexposed"].eligible_for_discovery is False
    assert score_map["new-overexposed"].discovery_score == 0.0
    assert score_map["old-stable"].engagement_score > score_map["new-hot"].engagement_score
    assert sample_influencers[0].depth_ratio == 10.0
    assert math.isclose(sample_influencers[0].posting_density, 1.0)


def test_scoring_service_first_run_defaults_momentum_to_zero(settings, sample_influencers):
    service = ScoringService(settings)
    scores = service.score(
        influencers=sample_influencers,
        snapshot_map={},
        serve_counts_map={},
    )

    assert all(score.momentum_score == 0.0 for score in scores)
