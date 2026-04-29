import src.repository.kv_feed_repository as kv_feed_repository_module
from src.core.settings import Settings
from src.repository.kv_feed_repository import KVFeedRepository
from src.utils.feed_recsys_keys import (
    ai_influencer_ids_key,
    global_pool_key,
    ugc_discovery_pushes_key,
    ugc_discovery_timestamps_key,
)


class FakePipeline:
    def __init__(self, results=None):
        self.results = results or []
        self.ops = []

    def delete(self, *keys):
        self.ops.append(("delete", keys))
        return self

    def zadd(self, key, mapping):
        self.ops.append(("zadd", key, mapping))
        return self

    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self

    def hgetall(self, key):
        self.ops.append(("hgetall", key))
        return self

    def sadd(self, key, *values):
        self.ops.append(("sadd", key, values))
        return self

    def rename(self, source, destination):
        self.ops.append(("rename", source, destination))
        return self

    def hset(self, key, field, value):
        self.ops.append(("hset", key, field, value))
        return self

    def hdel(self, key, *fields):
        self.ops.append(("hdel", key, fields))
        return self

    async def execute(self):
        return self.results


class FakeClient:
    def __init__(self, pipeline_results=None):
        self.pipeline_results = list(pipeline_results or [])
        self.pipelines = []
        self.exists_value = 0
        self.smismember_result = []
        self.get_value = None
        self.ttl_value = -2
        self.hgetall_result = {}
        self.zrangebyscore_result = []
        self.zadd_return = 0
        self.expire_calls = []
        self.set_calls = []
        self.delete_calls = []
        self.exists_calls = []
        self.smismember_calls = []
        self.execute_command_calls = []

    def pipeline(self):
        results = self.pipeline_results.pop(0) if self.pipeline_results else []
        pipe = FakePipeline(results)
        self.pipelines.append(pipe)
        return pipe

    async def exists(self, key):
        self.exists_calls.append(key)
        return self.exists_value

    async def smismember(self, key, values):
        self.smismember_calls.append((key, values))
        return self.smismember_result

    async def get(self, key):
        return self.get_value

    async def set(self, key, value, ex=None):
        self.set_calls.append((key, value, ex))
        return True

    async def ttl(self, key):
        return self.ttl_value

    async def hgetall(self, key):
        return self.hgetall_result

    async def zrangebyscore(self, key, min_score, max_score, start=0, num=None):
        return self.zrangebyscore_result

    async def zadd(self, key, mapping):
        self.last_zadd = (key, mapping)
        return self.zadd_return

    async def expire(self, key, ttl):
        self.expire_calls.append((key, ttl))
        return True

    async def delete(self, *keys):
        self.delete_calls.append(keys)
        return len(keys)

    async def execute_command(self, *args):
        self.execute_command_calls.append(args)
        return []


def build_settings(**overrides) -> Settings:
    return Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        **overrides,
    )


async def test_replace_global_pool_rewrites_pool_with_expiry_scores(monkeypatch):
    monkeypatch.setattr(kv_feed_repository_module.time, "time", lambda: 1000)
    client = FakeClient()
    repo = KVFeedRepository(client, build_settings(storage_namespace="staging"))

    count = await repo.replace_global_pool("popular:99_100", ["v1", "v2", "v1"], ttl_sec=60)

    assert count == 2
    pipe = client.pipelines[0]
    assert pipe.ops[0] == (
        "delete",
        ("staging:feed_recsys:{GLOBAL}:pool:popular:99_100",),
    )
    assert pipe.ops[1] == (
        "zadd",
        "staging:feed_recsys:{GLOBAL}:pool:popular:99_100",
        {"v1": 1060.0, "v2": 1060.0},
    )
    assert pipe.ops[2] == (
        "expire",
        "staging:feed_recsys:{GLOBAL}:pool:popular:99_100",
        60,
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
                    "canister_id": "cid-3",
                    "post_id": "33",
                },
            ]
        ]
    )
    repo = KVFeedRepository(client, build_settings())

    metadata = await repo.get_video_metadata_batch(["video-1", "video-2", "video-3"])

    assert metadata == {
        "video-1": {
            "canister_id": "cid-1",
            "post_id": "11",
            "publisher_user_id": "user-1",
        },
        "video-3": {
            "canister_id": "cid-3",
            "post_id": "33",
            "publisher_user_id": "",
        },
    }


async def test_check_ai_influencer_ids_uses_set_membership():
    client = FakeClient()
    client.smismember_result = [1, 0]
    settings = build_settings(storage_namespace="staging")
    repo = KVFeedRepository(client, settings)

    result = await repo.check_ai_influencer_ids(["user-1", "user-2"])

    assert result == {"user-1": True, "user-2": False}
    assert client.smismember_calls == [
        (
            ai_influencer_ids_key(settings),
            ["user-1", "user-2"],
        )
    ]


async def test_replace_ai_influencer_ids_swaps_temp_set():
    client = FakeClient()
    settings = build_settings(storage_namespace="staging")
    repo = KVFeedRepository(client, settings)

    count = await repo.replace_ai_influencer_ids(["user-1", "user-2", "user-1"])

    assert count == 2
    pipe = client.pipelines[0]
    target_key = ai_influencer_ids_key(settings)
    assert pipe.ops == [
        ("delete", (f"{target_key}:tmp",)),
        ("sadd", f"{target_key}:tmp", ("user-1", "user-2")),
        ("rename", f"{target_key}:tmp", target_key),
    ]


async def test_replace_ugc_discovery_pool_preserves_push_counts_and_cleans_stale(monkeypatch):
    monkeypatch.setattr(kv_feed_repository_module.time, "time", lambda: 2000)
    client = FakeClient()
    settings = build_settings(storage_namespace="staging")
    client.hgetall_result = {"video-1": "7", "stale-video": "3"}
    repo = KVFeedRepository(client, settings)

    count = await repo.replace_ugc_discovery_pool(
        [
            {"video_id": "video-1", "upload_timestamp": 101},
            {"video_id": "video-2", "upload_timestamp": 202},
        ],
        ttl_sec=120,
    )

    assert count == 2
    pipe = client.pipelines[0]
    pool_key = global_pool_key(settings, "ugc_discovery")
    timestamps_key = ugc_discovery_timestamps_key(settings)
    pushes_key = ugc_discovery_pushes_key(settings)
    assert pipe.ops[0] == ("delete", (pool_key,))
    assert pipe.ops[1] == ("delete", (timestamps_key,))
    assert ("hset", pushes_key, "video-2", 0) in pipe.ops
    assert ("hdel", pushes_key, ("stale-video",)) in pipe.ops
    assert ("expire", pool_key, 120) in pipe.ops


async def test_set_popularity_pointer_preserves_existing_ttl():
    client = FakeClient()
    client.ttl_value = 45
    repo = KVFeedRepository(client, build_settings(storage_namespace="staging"))

    await repo.set_popularity_pointer("user-7", "95_99")

    assert client.set_calls == [
        (
            "staging:feed_recsys:{user:user-7}:pop_percentile_pointer",
            "95_99",
            45,
        )
    ]
