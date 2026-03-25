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


def _base_row(influencer_id: str, final_rank: int) -> dict:
    return {
        "id": influencer_id,
        "name": influencer_id,
        "display_name": influencer_id.upper(),
        "avatar_url": f"https://example.com/{influencer_id}.png",
        "description": influencer_id,
        "category": "test",
        "created_at": "2026-01-01T00:00:00+00:00",
        "final_rank": final_rank,
    }


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


@pytest.mark.asyncio
async def test_feed_api_paginates_persisted_curated_prefix_order(kvrocks_client, repo):
    await repo.rewrite_ranked_feed_rows(
        [
            _base_row("curated-1", 1),
            _base_row("curated-2", 2),
            _base_row("algo-1", 3),
        ]
    )
    app = _build_test_app(kvrocks_client, repo)

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        first_page = await client.get(
            "/api/v1/influencer-feed?offset=0&limit=2&with_metadata=false"
        )
        assert first_page.status_code == 200
        assert [item["id"] for item in first_page.json()["influencers"]] == [
            "curated-1",
            "curated-2",
        ]

        second_page = await client.get(
            "/api/v1/influencer-feed?offset=2&limit=1&with_metadata=false"
        )
        assert second_page.status_code == 200
        assert [item["id"] for item in second_page.json()["influencers"]] == ["algo-1"]
