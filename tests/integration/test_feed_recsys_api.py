import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from src.routers.feed_recsys import router as feed_recsys_router
from src.routers.health import router as health_router
from src.schemas.feed_recsys import FeedRecommendationWithMetadataResponse
from src.services.publisher_profile_enrichment_service import (
    PublisherProfileEnrichmentService,
)
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


class StubPublisherProfileRepository:
    def __init__(self):
        self.cached_profiles = {}
        self.cache_writes = []
        self.refresh_enqueues = []

    async def get_profiles_batch(self, publisher_user_ids):
        return {
            publisher_id: self.cached_profiles[publisher_id]
            for publisher_id in publisher_user_ids
            if publisher_id in self.cached_profiles
        }

    async def cache_profiles_batch(self, profiles_by_publisher_id):
        self.cache_writes.append(profiles_by_publisher_id)
        return len(profiles_by_publisher_id)

    async def enqueue_refresh(self, publisher_user_ids):
        self.refresh_enqueues.append(list(publisher_user_ids))
        return len(publisher_user_ids)

    async def enqueue_warmup(self, publisher_user_ids):
        return len(publisher_user_ids)

    async def dequeue_refresh_batch(self, _limit):
        return []

    async def dequeue_warmup_batch(self, _limit):
        return []

    async def acquire_refresh_lock(self, publisher_user_id, _ttl_sec):
        return f"token-{publisher_user_id}"

    async def release_refresh_lock(self, _publisher_user_id, _token):
        return None


class StubMetadataServiceClient:
    def __init__(self, usernames=None):
        self.usernames = usernames or {}

    async def get_usernames_bulk(self, publisher_user_ids):
        return {
            publisher_id: self.usernames[publisher_id]
            for publisher_id in publisher_user_ids
            if publisher_id in self.usernames
        }


class StubCanisterProfileClient:
    def __init__(self, profiles=None):
        self.profiles = profiles or {}

    async def get_users_profile_details(self, publisher_user_ids):
        return {
            publisher_id: self.profiles[publisher_id]
            for publisher_id in publisher_user_ids
            if publisher_id in self.profiles
        }


class StubClickHouseFeedRepository:
    async def get_following_status_batch(self, viewer_user_id, publisher_user_ids):
        return {
            publisher_id: publisher_id == "publisher-1"
            for publisher_id in publisher_user_ids
        }


class StubSettings:
    profile_canister_id = "profile-id"
    feed_recsys_follow_lookup_max_concurrency = 2
    feed_recsys_publisher_username_stale_after_sec = 21600
    feed_recsys_publisher_profile_stale_after_sec = 3600
    feed_recsys_publisher_profile_refresh_batch_size = 100
    feed_recsys_publisher_profile_warmup_batch_size = 100
    feed_recsys_publisher_profile_upstream_chunk_size = 100
    feed_recsys_publisher_profile_refresh_lock_ttl_sec = 60


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
    publisher_profile_enrichment_service = PublisherProfileEnrichmentService(
        clickhouse_feed_repository=StubClickHouseFeedRepository(),
        kv_publisher_profile_repository=StubPublisherProfileRepository(),
        request_metadata_service_client=StubMetadataServiceClient(),
        background_metadata_service_client=StubMetadataServiceClient(),
        request_canister_client=StubCanisterProfileClient(),
        background_canister_client=StubCanisterProfileClient(),
        settings=StubSettings(),
    )
    app.state.recommend_with_metadata_service = RecommendWithMetadataService(
        feed_pool_service=StubFeedPoolService(),
        video_metadata_service=VideoMetadataService(
            clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
            kv_video_metadata_repository=StubKVVideoMetadataRepository(),
            kv_feed_repository=kv_feed_repository,
            offchain_rewards_client=offchain_rewards_client,
            settings=StubSettings(),
        ),
        publisher_profile_enrichment_service=publisher_profile_enrichment_service,
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
        assert payload["videos"][0]["is_following"] is False
        assert "username" not in payload["videos"][0]
        assert payload["videos"][0]["is_pro_user"] is False
        assert "profile_image_url" not in payload["videos"][0]


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


@pytest.mark.asyncio
async def test_feed_recsys_api_handles_mixed_profile_cache_hit_stale_and_miss():
    app = FastAPI()
    app.include_router(feed_recsys_router)
    app.include_router(health_router)
    kv_feed_repository = StubKVFeedRepository()
    publisher_profile_repository = StubPublisherProfileRepository()
    publisher_profile_repository.cached_profiles = {
        "publisher-1": {
            "username": "fresh-name",
            "profile_image_url": "https://img/fresh.png",
            "is_pro_user": False,
            "username_fetched_at": 9999999999,
            "profile_fetched_at": 9999999999,
        }
    }
    app.state.kvrocks = StubKVRocksClient()
    app.state.kv_feed_repository = kv_feed_repository
    app.state.recommend_with_metadata_service = RecommendWithMetadataService(
        feed_pool_service=StubFeedPoolService(),
        video_metadata_service=VideoMetadataService(
            clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
            kv_video_metadata_repository=StubKVVideoMetadataRepository(),
            kv_feed_repository=kv_feed_repository,
            offchain_rewards_client=StubOffchainRewardsClient(),
            settings=StubSettings(),
        ),
        publisher_profile_enrichment_service=PublisherProfileEnrichmentService(
            clickhouse_feed_repository=StubClickHouseFeedRepository(),
            kv_publisher_profile_repository=publisher_profile_repository,
            request_metadata_service_client=StubMetadataServiceClient(
                usernames={"publisher-1": "fresh-name"}
            ),
            background_metadata_service_client=StubMetadataServiceClient(
                usernames={"publisher-1": "fresh-name"}
            ),
            request_canister_client=StubCanisterProfileClient(
                profiles={
                    "publisher-1": {
                        "profile_image_url": "https://img/fresh.png",
                        "is_pro_user": False,
                    }
                }
            ),
            background_canister_client=StubCanisterProfileClient(
                profiles={
                    "publisher-1": {
                        "profile_image_url": "https://img/fresh.png",
                        "is_pro_user": False,
                    }
                }
            ),
            settings=StubSettings(),
        ),
    )

    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://testserver",
    ) as client:
        response = await client.get(
            "/api/v1/recommend-with-metadata/user-123?count=1&rec_type=mixed"
        )

    assert response.status_code == 200
    payload = response.json()
    assert payload["videos"][0]["username"] == "fresh-name"
    assert payload["videos"][0]["profile_image_url"] == "https://img/fresh.png"
    assert payload["videos"][0]["is_following"] is True
