import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.routers.feed_recsys import router as feed_recsys_router
from src.routers.health import router as health_router
from src.schemas.feed_recsys import FeedRecommendationWithMetadataResponse
from src.services.recommend_with_metadata_service import RecommendWithMetadataService
from src.services.video_metadata_service import VideoMetadataService


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


class StubKVFeedRepository:
    def __init__(self):
        self.store = {}
        self.upsert_calls = []

    async def upsert_video_view_counts(self, view_counts):
        self.upsert_calls.append(view_counts)
        for video_id, payload in view_counts.items():
            existing = self.store.get(
                video_id,
                {"num_views_loggedin": 0, "num_views_all": 0},
            )
            self.store[video_id] = {
                "num_views_loggedin": max(
                    int(existing.get("num_views_loggedin") or 0),
                    int(payload.get("num_views_loggedin") or 0),
                ),
                "num_views_all": max(
                    int(existing.get("num_views_all") or 0),
                    int(payload.get("num_views_all") or 0),
                ),
            }
        return len(view_counts)

    async def get_cached_video_view_counts(self, video_ids):
        return {
            video_id: dict(self.store[video_id])
            for video_id in video_ids
            if video_id in self.store
        }

    async def check_ai_influencer_ids(self, user_ids):
        return {user_id: user_id == "publisher-1" for user_id in user_ids}


class StubKVVideoMetadataRepository:
    async def get_video_metadata_batch(self, video_ids):
        return {}

    async def cache_video_metadata_batch(self, metadata_by_video_id):
        return len(metadata_by_video_id)


class StubClickHouseVideoMetadataRepository:
    async def get_video_metadata_batch(self, video_ids):
        return {
            "video-1": {
                "canister_id": "cid-1",
                "post_id": "11",
                "publisher_user_id": "publisher-1",
            }
        }


class StubOffchainRewardsClient:
    def __init__(self):
        self.calls = []

    async def get_bulk_video_stats(self, video_ids):
        self.calls.append(video_ids)
        return {}


class StubFeedPoolService:
    async def get_video_ids(self, user_id: str, count: int, rec_type: str):
        return ["video-1"], {rec_type: 1}


class StubSettings:
    profile_canister_id = "profile-id"


def _build_test_app() -> FastAPI:
    app = FastAPI()
    app.include_router(feed_recsys_router)
    app.include_router(health_router)
    app.state.kvrocks = StubKVRocksClient()
    app.state.kv_feed_repository = StubKVFeedRepository()
    app.state.recommend_with_metadata_service = StubRecommendWithMetadataService()
    return app


def _build_warm_cache_test_app() -> tuple[FastAPI, StubKVFeedRepository, StubOffchainRewardsClient]:
    app = FastAPI()
    app.include_router(feed_recsys_router)
    app.include_router(health_router)
    kv_feed_repository = StubKVFeedRepository()
    offchain_rewards_client = StubOffchainRewardsClient()
    app.state.kvrocks = StubKVRocksClient()
    app.state.kv_feed_repository = kv_feed_repository
    app.state.recommend_with_metadata_service = RecommendWithMetadataService(
        feed_pool_service=StubFeedPoolService(),
        video_metadata_service=VideoMetadataService(
            clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
            kv_video_metadata_repository=StubKVVideoMetadataRepository(),
            kv_feed_repository=kv_feed_repository,
            offchain_rewards_client=offchain_rewards_client,
            settings=StubSettings(),
        ),
    )
    return app, kv_feed_repository, offchain_rewards_client


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


@pytest.mark.asyncio
async def test_feed_view_count_push_api_merges_duplicate_rows_and_ignores_extra_fields():
    app = _build_test_app()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/internal/feed-recsys/view-counts",
            json=[
                {
                    "video_id": "video-1",
                    "total_count_loggedin": 3,
                    "total_count_all": 9,
                    "count": 999,
                    "last_milestone": 100,
                },
                {
                    "video_id": "video-1",
                    "total_count_loggedin": 5,
                    "total_count_all": 7,
                },
                {
                    "video_id": "video-2",
                    "total_count_loggedin": 1,
                    "total_count_all": 2,
                },
            ],
        )

        assert response.status_code == 200
        assert response.json() == {"received": 3, "upserted": 2}
        assert app.state.kv_feed_repository.upsert_calls == [
            {
                "video-1": {"num_views_loggedin": 5, "num_views_all": 9},
                "video-2": {"num_views_loggedin": 1, "num_views_all": 2},
            }
        ]
        assert app.state.kv_feed_repository.store == {
            "video-1": {"num_views_loggedin": 5, "num_views_all": 9},
            "video-2": {"num_views_loggedin": 1, "num_views_all": 2},
        }


@pytest.mark.asyncio
async def test_feed_view_count_push_api_rejects_invalid_rows():
    app = _build_test_app()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.post(
            "/api/v1/internal/feed-recsys/view-counts",
            json=[
                {
                    "video_id": " ",
                    "total_count_loggedin": -1,
                    "total_count_all": 2,
                }
            ],
        )

        assert response.status_code == 422


@pytest.mark.asyncio
async def test_feed_view_count_push_warms_recommendation_cache_without_offchain_lookup():
    app, _, offchain_rewards_client = _build_warm_cache_test_app()

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        push_response = await client.post(
            "/api/v1/internal/feed-recsys/view-counts",
            json=[
                {
                    "video_id": "video-1",
                    "total_count_loggedin": 15,
                    "total_count_all": 20,
                }
            ],
        )
        assert push_response.status_code == 200

        response = await client.get(
            "/api/v1/recommend-with-metadata/user-123?count=1&rec_type=mixed"
        )
        assert response.status_code == 200
        payload = response.json()
        assert payload["videos"][0]["video_id"] == "video-1"
        assert payload["videos"][0]["num_views_loggedin"] == 15
        assert payload["videos"][0]["num_views_all"] == 20
        assert offchain_rewards_client.calls == []
