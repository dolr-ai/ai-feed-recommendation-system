import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.routers.health import router as health_router
from src.routers.influencer_feed import router as influencer_feed_router
from src.services.feed_service import FeedService


def _build_test_app(kvrocks_client, repo) -> FastAPI:
    app = FastAPI()
    app.include_router(influencer_feed_router)
    app.include_router(health_router)
    app.state.kvrocks = kvrocks_client
    app.state.repo = repo
    app.state.feed_service = FeedService(repo)
    return app


@pytest.mark.asyncio
async def test_feed_api_and_health(kvrocks_client, repo, sample_feed_rows):
    influencers, scores = sample_feed_rows

    await repo.write_ranked_feed(scores, influencers)
    app = _build_test_app(kvrocks_client, repo)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        health = await client.get("/health")
        assert health.status_code == 200
        assert health.json()["status"] == "ok"

        response = await client.get(
            "/api/v1/influencer-feed?offset=0&limit=2&with_metadata=true"
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["total_count"] == 2
        assert payload["has_more"] is False
        assert payload["influencers"][0]["id"] == "i1"
        assert payload["influencers"][0]["scores"]["engagement_score"] == 0.7
        assert payload["influencers"][0]["ranking"]["final_rank"] == 1


@pytest.mark.asyncio
async def test_feed_api_omits_metadata_fields_when_not_requested(
    kvrocks_client, repo, sample_feed_rows
):
    influencers, scores = sample_feed_rows

    await repo.write_ranked_feed(scores, influencers)
    app = _build_test_app(kvrocks_client, repo)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.get(
            "/api/v1/influencer-feed?offset=0&limit=1&with_metadata=false"
        )
        assert response.status_code == 200
        payload = response.json()
        assert "scores" not in payload["influencers"][0]
        assert "signals" not in payload["influencers"][0]
