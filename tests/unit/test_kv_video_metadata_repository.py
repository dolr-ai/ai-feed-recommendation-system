from src.core.settings import Settings
from src.repository.kv_video_metadata_repository import KVVideoMetadataRepository


class FakePipeline:
    def __init__(self, results=None):
        self.results = results or []
        self.ops = []

    def hgetall(self, key):
        self.ops.append(("hgetall", key))
        return self

    def hset(self, key, mapping=None):
        self.ops.append(("hset", key, mapping))
        return self

    async def execute(self):
        return self.results


class FakeClient:
    def __init__(self, pipeline_results=None):
        self.pipeline_results = list(pipeline_results or [])
        self.pipelines = []

    def pipeline(self):
        results = self.pipeline_results.pop(0) if self.pipeline_results else []
        pipe = FakePipeline(results)
        self.pipelines.append(pipe)
        return pipe


def build_settings(**overrides) -> Settings:
    return Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        **overrides,
    )


async def test_get_video_metadata_batch_normalizes_canister_fields():
    client = FakeClient(
        pipeline_results=[
            [
                {
                    "upload_canister_id": "cid-1",
                    "post_id": "11",
                    "publisher_user_id": "user-1",
                },
                {},
                {
                    "post_id": "33",
                    "publisher_user_id": "user-3",
                },
            ]
        ]
    )
    repo = KVVideoMetadataRepository(client, build_settings())

    metadata = await repo.get_video_metadata_batch(["video-1", "video-2", "video-3"])

    assert metadata == {
        "video-1": {
            "canister_id": "cid-1",
            "post_id": "11",
            "publisher_user_id": "user-1",
        },
        "video-3": {
            "canister_id": "profile-id",
            "post_id": "33",
            "publisher_user_id": "user-3",
        },
    }


async def test_get_video_metadata_batch_returns_empty_when_client_missing():
    repo = KVVideoMetadataRepository(None, build_settings())

    metadata = await repo.get_video_metadata_batch(["video-1"])

    assert metadata == {}


async def test_cache_video_metadata_batch_writes_persistent_shared_hash_payload():
    client = FakeClient(pipeline_results=[[]])
    repo = KVVideoMetadataRepository(client, build_settings())

    inserted = await repo.cache_video_metadata_batch(
        {
            "video-1": {
                "canister_id": "cid-1",
                "post_id": "11",
                "publisher_user_id": "user-1",
            },
            "video-2": {
                "upload_canister_id": "2vxsx-fae",
                "post_id": "22",
                "publisher_user_id": "user-2",
            },
        }
    )

    assert inserted == 2
    assert client.pipelines[0].ops == [
        (
            "hset",
            "offchain:metadata:video_details:video-1",
            {
                "video_id": "video-1",
                "canister_id": "cid-1",
                "post_id": "11",
                "publisher_user_id": "user-1",
            },
        ),
        (
            "hset",
            "offchain:metadata:video_details:video-2",
            {
                "video_id": "video-2",
                "canister_id": "profile-id",
                "post_id": "22",
                "publisher_user_id": "user-2",
            },
        ),
    ]
