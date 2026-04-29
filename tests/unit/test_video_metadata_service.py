from src.services.video_metadata_service import VideoMetadataService


class StubKVFeedRepository:
    async def get_video_metadata_batch(self, video_ids):
        return {
            "video-1": {
                "canister_id": "cid-1",
                "post_id": "11",
                "publisher_user_id": "publisher-1",
            },
            "video-2": {
                "canister_id": "cid-2",
                "post_id": "22",
                "publisher_user_id": "publisher-2",
            },
        }

    async def check_ai_influencer_ids(self, user_ids):
        return {"publisher-1": True, "publisher-2": False}


async def test_video_metadata_service_builds_rows_and_attaches_ai_influencer_flags():
    service = VideoMetadataService(StubKVFeedRepository())

    rows = await service.build_video_rows(["video-2", "video-1", "missing"])

    assert [row.video_id for row in rows] == ["video-2", "video-1"]
    assert rows[0].publisher_user_id == "publisher-2"
    assert rows[0].from_ai_influencer is False
    assert rows[0].num_views_loggedin == 0
    assert rows[0].num_views_all == 0
    assert rows[1].publisher_user_id == "publisher-1"
    assert rows[1].from_ai_influencer is True
