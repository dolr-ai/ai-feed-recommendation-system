import asyncio

from src.schemas.feed_recsys import FeedVideoMetadata
from src.services.publisher_profile_enrichment_service import (
    PublisherProfileEnrichmentService,
)


class StubClickHouseFeedRepository:
    def __init__(self, recent_publisher_ids=None):
        self.calls = []
        self.recent_publisher_ids = recent_publisher_ids or []

    async def get_following_status_batch(self, viewer_user_id, publisher_ids):
        self.calls.append((viewer_user_id, list(publisher_ids)))
        return {
            "publisher-fresh": True,
            "publisher-stale": False,
        }

    async def get_recent_active_publisher_user_ids(self, limit, lookback_days):
        self.calls.append(("recent_publishers", limit, lookback_days))
        return list(self.recent_publisher_ids)


class StubKVPublisherProfileRepository:
    def __init__(self):
        self.cached_profiles = {}
        self.refresh_enqueues = []
        self.warmup_enqueues = []
        self.cache_writes = []
        self.refresh_locks = []
        self.released_locks = []

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
        self.warmup_enqueues.append(list(publisher_user_ids))
        return len(publisher_user_ids)

    async def dequeue_refresh_batch(self, _limit):
        return []

    async def dequeue_warmup_batch(self, _limit):
        return []

    async def acquire_refresh_lock(self, publisher_user_id, ttl_sec):
        self.refresh_locks.append((publisher_user_id, ttl_sec))
        return f"token-{publisher_user_id}"

    async def release_refresh_lock(self, publisher_user_id, token):
        self.released_locks.append((publisher_user_id, token))


class StubMetadataServiceClient:
    def __init__(self, usernames=None, should_fail=False):
        self.usernames = usernames or {}
        self.should_fail = should_fail
        self.calls = []

    async def get_usernames_bulk(self, publisher_user_ids):
        self.calls.append(list(publisher_user_ids))
        if self.should_fail:
            raise RuntimeError("metadata unavailable")
        return dict(self.usernames)


class StubCanisterClient:
    def __init__(self, profiles=None, should_fail=False):
        self.profiles = profiles or {}
        self.should_fail = should_fail
        self.calls = []

    async def get_users_profile_details(self, publisher_user_ids):
        self.calls.append(list(publisher_user_ids))
        if self.should_fail:
            raise RuntimeError("canister unavailable")
        return dict(self.profiles)


class StubSettings:
    feed_recsys_follow_lookup_max_concurrency = 2
    feed_recsys_publisher_username_stale_after_sec = 21600
    feed_recsys_publisher_profile_stale_after_sec = 3600
    feed_recsys_publisher_profile_refresh_batch_size = 100
    feed_recsys_publisher_profile_warmup_batch_size = 100
    feed_recsys_publisher_profile_backfill_batch_size = 100
    feed_recsys_publisher_profile_upstream_chunk_size = 100
    feed_recsys_publisher_profile_backfill_lookback_days = 90
    feed_recsys_publisher_profile_refresh_lock_ttl_sec = 60


def build_service(
    repo,
    metadata_client,
    canister_client,
    clickhouse_repo=None,
):
    return PublisherProfileEnrichmentService(
        clickhouse_feed_repository=clickhouse_repo or StubClickHouseFeedRepository(),
        kv_publisher_profile_repository=repo,
        request_metadata_service_client=metadata_client,
        background_metadata_service_client=metadata_client,
        request_canister_client=canister_client,
        background_canister_client=canister_client,
        settings=StubSettings(),
    )


async def test_partition_cached_profiles_distinguishes_fresh_stale_and_miss():
    service = build_service(
        repo=StubKVPublisherProfileRepository(),
        metadata_client=StubMetadataServiceClient(),
        canister_client=StubCanisterClient(),
    )
    cached_profiles = {
        "publisher-fresh": {
            "username": "fresh",
            "profile_image_url": "https://img/fresh.png",
            "is_pro_user": False,
            "username_fetched_at": 49000,
            "profile_fetched_at": 49500,
        },
        "publisher-stale": {
            "username": "stale",
            "profile_image_url": "https://img/stale.png",
            "is_pro_user": True,
            "username_fetched_at": 10,
            "profile_fetched_at": 10,
        },
    }

    fresh_ids, stale_ids, miss_ids = service._partition_cached_profiles(
        ["publisher-fresh", "publisher-stale", "publisher-miss"],
        cached_profiles,
        now=50000,
    )

    assert fresh_ids == ["publisher-fresh"]
    assert stale_ids == ["publisher-stale"]
    assert miss_ids == ["publisher-miss"]


async def test_resolve_profiles_preserves_stale_cache_when_username_fetch_fails():
    repo = StubKVPublisherProfileRepository()
    service = build_service(
        repo=repo,
        metadata_client=StubMetadataServiceClient(should_fail=True),
        canister_client=StubCanisterClient(
            profiles={
                "publisher-1": {
                    "profile_image_url": "",
                    "is_pro_user": False,
                }
            }
        ),
    )

    profiles = await service._resolve_profiles(
        ["publisher-1"],
        existing_profiles={
            "publisher-1": {
                "username": "cached-name",
                "profile_image_url": "https://img/cached.png",
                "is_pro_user": True,
                "username_fetched_at": 1,
                "profile_fetched_at": 1,
            }
        },
        metadata_client=service._request_metadata_service_client,
        canister_client=service._request_canister_client,
    )

    assert profiles == {
        "publisher-1": {
            "username": "cached-name",
            "profile_image_url": "",
            "is_pro_user": False,
            "username_fetched_at": 1,
            "profile_fetched_at": profiles["publisher-1"]["profile_fetched_at"],
        }
    }
    assert profiles["publisher-1"]["profile_fetched_at"] > 1


async def test_enrich_rows_handles_mixed_fresh_stale_and_miss_profiles():
    repo = StubKVPublisherProfileRepository()
    repo.cached_profiles = {
        "publisher-fresh": {
            "username": "fresh-name",
            "profile_image_url": "https://img/fresh.png",
            "is_pro_user": False,
            "username_fetched_at": 9999999999,
            "profile_fetched_at": 9999999999,
        },
        "publisher-stale": {
            "username": "stale-name",
            "profile_image_url": "https://img/stale.png",
            "is_pro_user": True,
            "username_fetched_at": 1,
            "profile_fetched_at": 1,
        },
    }
    service = build_service(
        repo=repo,
        metadata_client=StubMetadataServiceClient(
            usernames={"publisher-miss": "miss-name"}
        ),
        canister_client=StubCanisterClient(
            profiles={
                "publisher-miss": {
                    "profile_image_url": "https://img/miss.png",
                    "is_pro_user": True,
                }
            }
        ),
    )
    rows = [
        FeedVideoMetadata(
            video_id="video-1",
            canister_id="cid-1",
            post_id="11",
            publisher_user_id="publisher-fresh",
        ),
        FeedVideoMetadata(
            video_id="video-2",
            canister_id="cid-2",
            post_id="22",
            publisher_user_id="publisher-stale",
        ),
        FeedVideoMetadata(
            video_id="video-3",
            canister_id="cid-3",
            post_id="33",
            publisher_user_id="publisher-miss",
        ),
    ]

    enriched = await service.enrich_rows("viewer-1", rows)
    await asyncio.sleep(0)

    assert [(row.publisher_user_id, row.username) for row in enriched] == [
        ("publisher-fresh", "fresh-name"),
        ("publisher-stale", "stale-name"),
        ("publisher-miss", "miss-name"),
    ]
    assert enriched[0].is_following is True
    assert enriched[1].is_following is False
    assert enriched[2].is_following is False
    assert enriched[1].profile_image_url == "https://img/stale.png"
    assert enriched[2].profile_image_url == "https://img/miss.png"
    assert enriched[2].is_pro_user is True
    assert repo.refresh_enqueues == [["publisher-stale"]]
    assert repo.cache_writes == [
        {
            "publisher-miss": {
                "username": "miss-name",
                "profile_image_url": "https://img/miss.png",
                "is_pro_user": True,
                "username_fetched_at": repo.cache_writes[0]["publisher-miss"]["username_fetched_at"],
                "profile_fetched_at": repo.cache_writes[0]["publisher-miss"]["profile_fetched_at"],
            }
        }
    ]
    assert repo.cache_writes[0]["publisher-miss"]["username_fetched_at"] > 0
    assert repo.cache_writes[0]["publisher-miss"]["profile_fetched_at"] > 0


async def test_enrich_rows_does_not_block_on_stale_refresh_enqueue():
    class BlockingRefreshRepo(StubKVPublisherProfileRepository):
        def __init__(self):
            super().__init__()
            self.enqueue_started = asyncio.Event()
            self.release_enqueue = asyncio.Event()

        async def enqueue_refresh(self, publisher_user_ids):
            self.enqueue_started.set()
            await self.release_enqueue.wait()
            return await super().enqueue_refresh(publisher_user_ids)

    repo = BlockingRefreshRepo()
    repo.cached_profiles = {
        "publisher-stale": {
            "username": "stale-name",
            "profile_image_url": "https://img/stale.png",
            "is_pro_user": True,
            "username_fetched_at": 1,
            "profile_fetched_at": 1,
        }
    }
    service = build_service(
        repo=repo,
        metadata_client=StubMetadataServiceClient(),
        canister_client=StubCanisterClient(),
    )
    rows = [
        FeedVideoMetadata(
            video_id="video-1",
            canister_id="cid-1",
            post_id="11",
            publisher_user_id="publisher-stale",
        )
    ]

    enriched = await asyncio.wait_for(service.enrich_rows("viewer-1", rows), timeout=0.1)

    assert enriched[0].username == "stale-name"
    await asyncio.wait_for(repo.enqueue_started.wait(), timeout=0.1)
    repo.release_enqueue.set()
    await asyncio.sleep(0)


async def test_backfill_recent_publishers_uses_background_clients_and_refreshes_cache():
    repo = StubKVPublisherProfileRepository()
    clickhouse_repo = StubClickHouseFeedRepository(
        recent_publisher_ids=["publisher-2", "publisher-1"]
    )
    metadata_client = StubMetadataServiceClient(
        usernames={
            "publisher-1": "name-1",
            "publisher-2": "name-2",
        }
    )
    canister_client = StubCanisterClient(
        profiles={
            "publisher-1": {
                "profile_image_url": "https://img/1.png",
                "is_pro_user": False,
            },
            "publisher-2": {
                "profile_image_url": "https://img/2.png",
                "is_pro_user": True,
            },
        }
    )
    service = build_service(
        repo=repo,
        metadata_client=metadata_client,
        canister_client=canister_client,
        clickhouse_repo=clickhouse_repo,
    )

    result = await service.backfill_recent_publishers()

    assert result == {"selected": 2, "refreshed": 2}
    assert clickhouse_repo.calls == [("recent_publishers", 100, 90)]
    assert metadata_client.calls == [["publisher-2", "publisher-1"]]
    assert canister_client.calls == [["publisher-2", "publisher-1"]]
    assert repo.refresh_locks == [("publisher-2", 60), ("publisher-1", 60)]
    assert repo.released_locks == [
        ("publisher-2", "token-publisher-2"),
        ("publisher-1", "token-publisher-1"),
    ]
    assert repo.cache_writes == [
        {
            "publisher-2": {
                "username": "name-2",
                "profile_image_url": "https://img/2.png",
                "is_pro_user": True,
                "username_fetched_at": repo.cache_writes[0]["publisher-2"]["username_fetched_at"],
                "profile_fetched_at": repo.cache_writes[0]["publisher-2"]["profile_fetched_at"],
            },
            "publisher-1": {
                "username": "name-1",
                "profile_image_url": "https://img/1.png",
                "is_pro_user": False,
                "username_fetched_at": repo.cache_writes[0]["publisher-1"]["username_fetched_at"],
                "profile_fetched_at": repo.cache_writes[0]["publisher-1"]["profile_fetched_at"],
            },
        }
    ]


async def test_resolve_profiles_preserves_stale_values_when_bulk_response_omits_publisher():
    repo = StubKVPublisherProfileRepository()
    service = build_service(
        repo=repo,
        metadata_client=StubMetadataServiceClient(usernames={"publisher-2": "name-2"}),
        canister_client=StubCanisterClient(
            profiles={
                "publisher-2": {
                    "profile_image_url": "https://img/2.png",
                    "is_pro_user": True,
                }
            }
        ),
    )

    profiles = await service._resolve_profiles(
        ["publisher-1", "publisher-2"],
        existing_profiles={
            "publisher-1": {
                "username": "cached-name",
                "profile_image_url": "https://img/cached.png",
                "is_pro_user": True,
                "username_fetched_at": 10,
                "profile_fetched_at": 20,
            }
        },
        metadata_client=service._request_metadata_service_client,
        canister_client=service._request_canister_client,
    )

    assert profiles["publisher-1"] == {
        "username": "cached-name",
        "profile_image_url": "https://img/cached.png",
        "is_pro_user": True,
        "username_fetched_at": 10,
        "profile_fetched_at": 20,
    }
    assert profiles["publisher-2"]["username"] == "name-2"
    assert profiles["publisher-2"]["profile_image_url"] == "https://img/2.png"
    assert profiles["publisher-2"]["is_pro_user"] is True


async def test_refresh_queued_profiles_reenqueues_failed_batch_when_both_upstreams_fail():
    repo = StubKVPublisherProfileRepository()

    async def fake_dequeue_refresh_batch(_limit):
        return ["publisher-1", "publisher-2"]

    repo.dequeue_refresh_batch = fake_dequeue_refresh_batch
    service = build_service(
        repo=repo,
        metadata_client=StubMetadataServiceClient(should_fail=True),
        canister_client=StubCanisterClient(should_fail=True),
    )

    try:
        await service.refresh_queued_profiles()
    except RuntimeError as exc:
        assert str(exc) == "publisher profile upstreams unavailable"
    else:
        raise AssertionError("expected refresh_queued_profiles to raise")

    assert repo.refresh_enqueues == [["publisher-1", "publisher-2"]]


async def test_refresh_queued_profiles_reenqueues_only_failed_chunks():
    repo = StubKVPublisherProfileRepository()

    async def fake_dequeue_refresh_batch(_limit):
        return ["publisher-1", "publisher-2", "publisher-3"]

    repo.dequeue_refresh_batch = fake_dequeue_refresh_batch

    class PartialFailureMetadataClient(StubMetadataServiceClient):
        async def get_usernames_bulk(self, publisher_user_ids):
            self.calls.append(list(publisher_user_ids))
            if "publisher-3" in publisher_user_ids:
                raise RuntimeError("metadata unavailable")
            return {
                publisher_id: f"name-{publisher_id[-1]}"
                for publisher_id in publisher_user_ids
            }

    class PartialFailureCanisterClient(StubCanisterClient):
        async def get_users_profile_details(self, publisher_user_ids):
            self.calls.append(list(publisher_user_ids))
            if "publisher-3" in publisher_user_ids:
                raise RuntimeError("canister unavailable")
            return {
                publisher_id: {
                    "profile_image_url": f"https://img/{publisher_id[-1]}.png",
                    "is_pro_user": publisher_id == "publisher-2",
                }
                for publisher_id in publisher_user_ids
            }

    class ChunkedSettings(StubSettings):
        feed_recsys_publisher_profile_upstream_chunk_size = 2

    service = PublisherProfileEnrichmentService(
        clickhouse_feed_repository=StubClickHouseFeedRepository(),
        kv_publisher_profile_repository=repo,
        request_metadata_service_client=PartialFailureMetadataClient(),
        background_metadata_service_client=PartialFailureMetadataClient(),
        request_canister_client=PartialFailureCanisterClient(),
        background_canister_client=PartialFailureCanisterClient(),
        settings=ChunkedSettings(),
    )

    result = await service.refresh_queued_profiles()

    assert result == {"dequeued": 3, "refreshed": 2}
    assert repo.refresh_enqueues == [["publisher-3"]]
    assert repo.cache_writes == [
        {
            "publisher-1": {
                "username": "name-1",
                "profile_image_url": "https://img/1.png",
                "is_pro_user": False,
                "username_fetched_at": repo.cache_writes[0]["publisher-1"]["username_fetched_at"],
                "profile_fetched_at": repo.cache_writes[0]["publisher-1"]["profile_fetched_at"],
            },
            "publisher-2": {
                "username": "name-2",
                "profile_image_url": "https://img/2.png",
                "is_pro_user": True,
                "username_fetched_at": repo.cache_writes[0]["publisher-2"]["username_fetched_at"],
                "profile_fetched_at": repo.cache_writes[0]["publisher-2"]["profile_fetched_at"],
            },
        }
    ]


async def test_refresh_queued_profiles_does_not_extend_stale_cache_for_omitted_publishers():
    repo = StubKVPublisherProfileRepository()
    repo.cached_profiles = {
        "publisher-1": {
            "username": "stale-name",
            "profile_image_url": "https://img/stale.png",
            "is_pro_user": True,
            "username_fetched_at": 10,
            "profile_fetched_at": 20,
        }
    }

    async def fake_dequeue_refresh_batch(_limit):
        return ["publisher-1"]

    repo.dequeue_refresh_batch = fake_dequeue_refresh_batch
    service = build_service(
        repo=repo,
        metadata_client=StubMetadataServiceClient(usernames={}),
        canister_client=StubCanisterClient(profiles={}),
    )

    result = await service.refresh_queued_profiles()

    assert result == {"dequeued": 1, "refreshed": 0}
    assert repo.cache_writes == []
    assert repo.refresh_enqueues == []
