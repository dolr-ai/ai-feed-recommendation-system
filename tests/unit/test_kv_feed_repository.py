import src.repository.kv_feed_repository as kv_feed_repository_module
from src.core.settings import Settings
from src.repository.kv_feed_repository import KVFeedRepository
from src.utils.feed_recsys_keys import (
    ai_influencer_ids_key,
    global_pool_key,
    ugc_discovery_pushes_key,
    ugc_discovery_timestamps_key,
    user_refill_lock_key,
    user_served_recent_key,
    video_view_count_key,
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

    def get(self, key):
        self.ops.append(("get", key))
        return self

    def set(self, key, value, ex=None):
        self.ops.append(("set", key, value, ex))
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
        self.mget_values = []
        self.ttl_value = -2
        self.hgetall_result = {}
        self.zrangebyscore_result = []
        self.zadd_return = 0
        self.zcount_value = 0
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

    async def set(self, key, value, ex=None, nx=False):
        self.set_calls.append((key, value, ex, nx))
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

    async def zcount(self, key, min_score, max_score):
        return self.zcount_value

    async def expire(self, key, ttl):
        self.expire_calls.append((key, ttl))
        return True

    async def delete(self, *keys):
        self.delete_calls.append(keys)
        return len(keys)

    async def mget(self, keys):
        self.mget_calls = getattr(self, "mget_calls", [])
        self.mget_calls.append(keys)
        return self.mget_values

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
            False,
        )
    ]


async def test_add_served_recent_videos_uses_served_recent_key(monkeypatch):
    monkeypatch.setattr(kv_feed_repository_module.time, "time", lambda: 3000)
    client = FakeClient()
    client.zadd_return = 2
    settings = build_settings(storage_namespace="staging")
    repo = KVFeedRepository(client, settings)

    count = await repo.add_served_recent_videos("user-1", ["video-1", "video-2", "video-1"])

    assert count == 2
    assert client.last_zadd == (
        user_served_recent_key(settings, "user-1"),
        {"video-1": 89400.0, "video-2": 89400.0},
    )
    assert client.expire_calls == [
        (user_served_recent_key(settings, "user-1"), settings.feed_recsys_served_recent_ttl_sec)
    ]


async def test_acquire_refill_lock_uses_nx_set():
    client = FakeClient()
    settings = build_settings(storage_namespace="staging")
    repo = KVFeedRepository(client, settings)

    acquired = await repo.acquire_refill_lock("user-9", "ugc", ttl_sec=25)

    assert acquired is True
    assert client.set_calls == [
        (
            user_refill_lock_key(settings, "user-9", "ugc"),
            "1",
            25,
            True,
        )
    ]


async def test_get_cached_video_view_counts_reads_json_payloads():
    client = FakeClient()
    client.mget_values = [
        '{"num_views_loggedin": 7, "num_views_all": 19}',
        None,
        '{"num_views_loggedin": 0, "num_views_all": 5}',
    ]
    settings = build_settings(storage_namespace="staging")
    repo = KVFeedRepository(client, settings)

    result = await repo.get_cached_video_view_counts(["video-1", "video-2", "video-3"])

    assert result == {
        "video-1": {"num_views_loggedin": 7, "num_views_all": 19},
        "video-3": {"num_views_loggedin": 0, "num_views_all": 5},
    }
    assert client.mget_calls == [[
        video_view_count_key(settings, "video-1"),
        video_view_count_key(settings, "video-2"),
        video_view_count_key(settings, "video-3"),
    ]]


async def test_cache_video_view_counts_sets_ttl_payloads():
    client = FakeClient()
    settings = build_settings(storage_namespace="staging")
    repo = KVFeedRepository(client, settings)

    count = await repo.cache_video_view_counts(
        {
            "video-1": {"num_views_loggedin": 3, "num_views_all": 8},
            "video-2": {"num_views_loggedin": 0, "num_views_all": 1},
        },
        ttl_sec=120,
    )

    assert count == 2
    pipe = client.pipelines[0]
    assert pipe.ops == [
        (
            "set",
            video_view_count_key(settings, "video-1"),
            '{"num_views_loggedin": 3, "num_views_all": 8}',
            120,
        ),
        (
            "set",
            video_view_count_key(settings, "video-2"),
            '{"num_views_loggedin": 0, "num_views_all": 1}',
            120,
        ),
    ]
