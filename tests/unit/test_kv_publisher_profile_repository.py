import json

from src.core.settings import Settings
from src.repository.kv_publisher_profile_repository import KVPublisherProfileRepository


class FakePipeline:
    def __init__(self, results=None):
        self.results = results or []
        self.ops = []

    def get(self, key):
        self.ops.append(("get", key))
        return self

    def set(self, key, value, ex=None):
        self.ops.append(("set", key, json.loads(value), ex))
        return self

    async def execute(self):
        return self.results


class FakeClient:
    def __init__(self, mget_results=None, spop_results=None):
        self.mget_results = list(mget_results or [])
        self.spop_results = list(spop_results or [])
        self.pipeline_results = []
        self.pipelines = []
        self.sadd_calls = []
        self.set_calls = []
        self.deleted = []
        self.execute_command_calls = []
        self.get_values = {}

    async def mget(self, keys):
        self.mget_keys = keys
        return self.mget_results

    def pipeline(self):
        results = self.pipeline_results.pop(0) if self.pipeline_results else []
        pipe = FakePipeline(results)
        self.pipelines.append(pipe)
        return pipe

    async def sadd(self, key, *values):
        self.sadd_calls.append((key, values))
        return len(values)

    async def spop(self, key, count):
        self.spop_calls = getattr(self, "spop_calls", [])
        self.spop_calls.append((key, count))
        return self.spop_results.pop(0) if self.spop_results else []

    async def set(self, key, value, ex=None, nx=None):
        self.set_calls.append((key, value, ex, nx))
        return True

    async def get(self, key):
        return self.get_values.get(key)

    async def execute_command(self, *args):
        self.execute_command_calls.append(args)
        return 1

    async def delete(self, key):
        self.deleted.append(key)


def build_settings(**overrides) -> Settings:
    return Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        feed_recsys_publisher_profile_ttl_jitter_sec=0,
        **overrides,
    )


async def test_get_profiles_batch_normalizes_json_blob_payloads():
    client = FakeClient(
        mget_results=[
            json.dumps(
                {
                    "username": "alice",
                    "profile_image_url": "https://img/alice.png",
                    "is_pro_user": "true",
                    "username_fetched_at": 10,
                    "profile_fetched_at": 20,
                }
            ),
            None,
        ]
    )
    repo = KVPublisherProfileRepository(client, build_settings())

    profiles = await repo.get_profiles_batch(["publisher-1", "publisher-2"])

    assert profiles == {
        "publisher-1": {
            "username": "alice",
            "profile_image_url": "https://img/alice.png",
            "is_pro_user": True,
            "username_fetched_at": 10,
            "profile_fetched_at": 20,
        }
    }


async def test_cache_profiles_batch_writes_one_json_blob_per_publisher_with_ttl():
    client = FakeClient()
    repo = KVPublisherProfileRepository(
        client,
        build_settings(feed_recsys_publisher_profile_hard_ttl_sec=600),
    )

    inserted = await repo.cache_profiles_batch(
        {
            "publisher-1": {
                "username": "alice",
                "profile_image_url": "https://img/alice.png",
                "is_pro_user": True,
                "username_fetched_at": 10,
                "profile_fetched_at": 20,
            }
        }
    )

    assert inserted == 1
    assert client.pipelines[0].ops == [
        (
            "set",
            "prod:feed_recsys:{PUBLISHER_PROFILE}:publisher:publisher-1",
            {
                "username": "alice",
                "profile_image_url": "https://img/alice.png",
                "is_pro_user": True,
                "username_fetched_at": 10,
                "profile_fetched_at": 20,
            },
            600,
        )
    ]


async def test_queue_and_lock_helpers_use_deduped_publisher_ids():
    client = FakeClient(spop_results=[["publisher-1", "publisher-2"]])
    repo = KVPublisherProfileRepository(client, build_settings())

    queued = await repo.enqueue_refresh(["publisher-1", "publisher-1", "publisher-2"])
    popped = await repo.dequeue_refresh_batch(10)
    token = await repo.acquire_refresh_lock("publisher-1", ttl_sec=30)
    await repo.release_refresh_lock("publisher-1", token)

    assert queued == 2
    assert popped == ["publisher-1", "publisher-2"]
    assert isinstance(token, str) and token
    assert client.sadd_calls == [
        (
            "prod:feed_recsys:{PUBLISHER_PROFILE}:queue:refresh",
            ("publisher-1", "publisher-2"),
        )
    ]
    assert client.set_calls == [
        (
            "prod:feed_recsys:{PUBLISHER_PROFILE}:refresh:lock:publisher-1",
            token,
            30,
            True,
        )
    ]
    assert client.execute_command_calls == [
        (
            "EVAL",
            repo._COMPARE_AND_DELETE_SCRIPT,
            1,
            "prod:feed_recsys:{PUBLISHER_PROFILE}:refresh:lock:publisher-1",
            token,
        )
    ]
    assert client.deleted == []
