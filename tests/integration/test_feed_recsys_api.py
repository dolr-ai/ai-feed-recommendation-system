import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.routers.feed_recsys import router as feed_recsys_router
from src.routers.health import router as health_router
from src.schemas.feed_recsys import FeedRecommendationWithMetadataResponse


class StubRecommendWithMetadataService:
    async def recommend_with_metadata(self, user_id: str, count: int, rec_type: str):
        return FeedRecommendationWithMetadataResponse(
            user_id=user_id,
            videos=[
                {
                    "video_id": "video-1",
                    "canister_id": "cid-1",
                    "post_id": "11",
                    "publisher_user_id": "publisher-1",
                    "num_views_loggedin": 0,
                    "num_views_all": 0,
                    "from_ai_influencer": True,
                }
            ],
            count=1,
            sources={rec_type: 1},
            timestamp=1710000000,
        )


class StubKVRocksClient:
    async def ping(self):
        return True


def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(feed_recsys_router)
    app.include_router(health_router)
    app.state.kvrocks = StubKVRocksClient()
    app.state.recommend_with_metadata_service = StubRecommendWithMetadataService()
    return app


@pytest.mark.asyncio
async def test_feed_recsys_api_returns_metadata_and_ai_influencer_flag():
    app = _build_test_app()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        health = await client.get("/health")
        assert health.status_code == 200

        response = await client.get(
            "/api/v1/recommend-with-metadata/user-123?count=5&rec_type=mixed"
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["user_id"] == "user-123"
        assert payload["count"] == 1
        assert payload["sources"] == {"mixed": 1}
        assert payload["videos"][0]["video_id"] == "video-1"
        assert payload["videos"][0]["from_ai_influencer"] is True
        assert payload["videos"][0]["num_views_loggedin"] == 0
