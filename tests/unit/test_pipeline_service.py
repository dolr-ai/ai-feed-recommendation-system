from types import SimpleNamespace

import pytest

from src.services.pipeline_service import EmptyInfluencerUniverseError, PipelineService


class _StubChatApiClient:
    async def get_all_influencers(self):
        return [
            {
                "id": "i1",
                "name": "inactive",
                "display_name": "Inactive",
                "avatar_url": "",
                "description": "",
                "category": "",
                "created_at": "2026-01-01T00:00:00+00:00",
                "is_active": "inactive",
            }
        ]

    async def get_trending(self):
        raise AssertionError("get_trending should not be called when no active influencers exist")


class _StubRepo:
    def __init__(self) -> None:
        self.write_ranked_feed_called = False

    async def load_stats_snapshot(self):
        return {}

    async def get_all_serve_counts(self, influencer_ids):
        return {influencer_id: 0.0 for influencer_id in influencer_ids}

    async def write_ranked_feed(self, scores, influencers):
        self.write_ranked_feed_called = True

    async def write_stats_snapshot(self, influencers):
        return None


class _StubCheckpointRepo:
    def __init__(self) -> None:
        self.saved = []

    async def save(self, state) -> None:
        self.saved.append(state)

    async def delete_all(self) -> None:
        return None


@pytest.mark.asyncio
async def test_pipeline_raises_when_no_active_influencers_are_available():
    repo = _StubRepo()
    checkpoint_repo = _StubCheckpointRepo()
    service = PipelineService(
        chat_api_client=_StubChatApiClient(),
        canister_client=SimpleNamespace(fetch_all=None, get_stats=lambda: {}),
        scoring_service=SimpleNamespace(score=None),
        feed_mixer_service=SimpleNamespace(rank_scores=None),
        influencer_repo=repo,
        checkpoint_repo=checkpoint_repo,
        settings=SimpleNamespace(chat_api_base_url="https://chat-ai.rishi.yral.com"),
    )

    with pytest.raises(
        EmptyInfluencerUniverseError,
        match=r"zero active influencers",
    ):
        await service.run()

    assert repo.write_ranked_feed_called is False
    assert len(checkpoint_repo.saved) == 1
    assert checkpoint_repo.saved[0].step == "fetch_influencers"
    assert checkpoint_repo.saved[0].success is False
