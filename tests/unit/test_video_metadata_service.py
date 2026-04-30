from src.services.video_metadata_service import VideoMetadataService


class StubClickHouseVideoMetadataRepository:
    async def get_video_metadata_batch(self, video_ids):
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
        self.calls = []

    async def get_video_metadata_batch(self, video_ids):
        self.calls.append(video_ids)
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


class StubKVFeedRepository:
    async def check_ai_influencer_ids(self, user_ids):
        return {"publisher-1": True, "publisher-2": False, "publisher-3": True}


class StubSettings:
    profile_canister_id = "profile-id"


async def test_video_metadata_service_builds_rows_and_attaches_ai_influencer_flags():
    fallback_repo = StubKVVideoMetadataRepository()
    service = VideoMetadataService(
        clickhouse_video_metadata_repository=StubClickHouseVideoMetadataRepository(),
        kv_video_metadata_repository=fallback_repo,
        kv_feed_repository=StubKVFeedRepository(),
        settings=StubSettings(),
    )

    rows = await service.build_video_rows(["video-2", "video-1", "video-3", "missing"])

    assert fallback_repo.calls == [["video-2", "video-3", "missing"]]
    assert [row.video_id for row in rows] == ["video-2", "video-1", "video-3"]
    assert rows[0].canister_id == "cid-2"
    assert rows[0].post_id == "22"
    assert rows[0].publisher_user_id == "publisher-2"
    assert rows[0].from_ai_influencer is False
    assert rows[0].num_views_loggedin == 0
    assert rows[0].num_views_all == 0
    assert rows[1].publisher_user_id == "publisher-1"
    assert rows[1].from_ai_influencer is True
    assert rows[2].canister_id == "profile-id"
    assert rows[2].from_ai_influencer is True
