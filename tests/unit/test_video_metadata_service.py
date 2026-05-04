from src.services.video_metadata_service import VideoMetadataService


class StubClickHouseVideoMetadataRepository:
    def __init__(self):
        self.calls = []

    async def get_video_metadata_batch(self, video_ids):
        self.calls.append(video_ids)
        return {
            "video-1": {
                "canister_id": "cid-1",
                "post_id": "11",
                "publisher_user_id": "publisher-1",
            },
            "video-2": {
                "canister_id": "",
                "post_id": "22",
                "publisher_user_id": "",
            },
        }


class StubKVVideoMetadataRepository:
    def __init__(self):
        self.read_calls = []
        self.write_calls = []

    async def get_video_metadata_batch(self, video_ids):
        self.read_calls.append(video_ids)
        return {
            "video-2": {
                "canister_id": "cid-2",
                "post_id": "fallback-22",
                "publisher_user_id": "publisher-2",
            },
            "video-3": {
                "canister_id": "",
                "post_id": "33",
                "publisher_user_id": "publisher-3",
            },
        }

    async def cache_video_metadata_batch(self, metadata_by_video_id):
        self.write_calls.append(metadata_by_video_id)
        return len(metadata_by_video_id)


class StubKVFeedRepository:
    def __init__(self):
        self.cached_reads = []
        self.cached_writes = []
        self.cached_result = {
            "video-1": {"num_views_loggedin": 12, "num_views_all": 34},
        }

    async def check_ai_influencer_ids(self, user_ids):
        return {"publisher-1": True, "publisher-2": False, "publisher-3": True}

    async def get_cached_video_view_counts(self, video_ids):
        self.cached_reads.append(video_ids)
        return dict(self.cached_result)

    async def upsert_video_view_counts(self, view_counts):
        self.cached_writes.append(view_counts)
        return len(view_counts)


class StubOffchainRewardsClient:
    def __init__(self):
        self.calls = []

    async def get_bulk_video_stats(self, video_ids):
        self.calls.append(video_ids)
        return {
            "video-2": {"num_views_loggedin": 56, "num_views_all": 78},
            "video-3": {"num_views_loggedin": 0, "num_views_all": 11},
        }


class StubSettings:
    profile_canister_id = "profile-id"


async def test_video_metadata_service_builds_rows_and_attaches_ai_influencer_flags():
    clickhouse_repo = StubClickHouseVideoMetadataRepository()
    fallback_repo = StubKVVideoMetadataRepository()
    kv_feed_repository = StubKVFeedRepository()
    offchain_rewards_client = StubOffchainRewardsClient()
    service = VideoMetadataService(
        clickhouse_video_metadata_repository=clickhouse_repo,
        kv_video_metadata_repository=fallback_repo,
        kv_feed_repository=kv_feed_repository,
        offchain_rewards_client=offchain_rewards_client,
        settings=StubSettings(),
    )

    rows = await service.build_video_rows(["video-2", "video-1", "video-3", "missing"])

    assert fallback_repo.read_calls == [["video-2", "video-1", "video-3", "missing"]]
    assert clickhouse_repo.calls == [["video-1", "missing"]]
    assert fallback_repo.write_calls == [
        {
            "video-1": {
                "canister_id": "cid-1",
                "post_id": "11",
                "publisher_user_id": "publisher-1",
            }
        }
    ]
    assert kv_feed_repository.cached_reads == [["video-2", "video-1", "video-3", "missing"]]
    assert kv_feed_repository.cached_writes == [
        {
            "video-2": {"num_views_loggedin": 56, "num_views_all": 78},
            "video-3": {"num_views_loggedin": 0, "num_views_all": 11},
        }
    ]
    assert offchain_rewards_client.calls == [["video-2", "video-3", "missing"]]
    assert [row.video_id for row in rows] == ["video-2", "video-1", "video-3"]
    assert rows[0].canister_id == "cid-2"
    assert rows[0].post_id == "fallback-22"
    assert rows[0].publisher_user_id == "publisher-2"
    assert rows[0].from_ai_influencer is False
    assert rows[0].num_views_loggedin == 56
    assert rows[0].num_views_all == 78
    assert rows[1].publisher_user_id == "publisher-1"
    assert rows[1].from_ai_influencer is True
    assert rows[1].num_views_loggedin == 12
    assert rows[1].num_views_all == 34
    assert rows[2].canister_id == "profile-id"
    assert rows[2].from_ai_influencer is True
    assert rows[2].num_views_loggedin == 0
    assert rows[2].num_views_all == 11


async def test_video_metadata_service_uses_cached_view_counts_without_offchain_lookup():
    clickhouse_repo = StubClickHouseVideoMetadataRepository()
    fallback_repo = StubKVVideoMetadataRepository()
    kv_feed_repository = StubKVFeedRepository()
    kv_feed_repository.cached_result = {
        "video-1": {"num_views_loggedin": 12, "num_views_all": 34},
        "video-2": {"num_views_loggedin": 56, "num_views_all": 78},
    }
    offchain_rewards_client = StubOffchainRewardsClient()
    service = VideoMetadataService(
        clickhouse_video_metadata_repository=clickhouse_repo,
        kv_video_metadata_repository=fallback_repo,
        kv_feed_repository=kv_feed_repository,
        offchain_rewards_client=offchain_rewards_client,
        settings=StubSettings(),
    )

    rows = await service.build_video_rows(["video-2", "video-1"])

    assert offchain_rewards_client.calls == []
    assert kv_feed_repository.cached_writes == []
    assert rows[0].num_views_loggedin == 56
    assert rows[0].num_views_all == 78
    assert rows[1].num_views_loggedin == 12
    assert rows[1].num_views_all == 34
