from types import SimpleNamespace

import pytest

from src.jobs import feed_recsys_jobs


class _StubLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass

    def debug(self, *_args, **_kwargs) -> None:
        pass

    def exception(self, *_args, **_kwargs) -> None:
        pass


class _FailingFeedSyncService:
    async def sync_global_popularity_pools(self) -> None:
        raise RuntimeError("clickhouse unavailable")


class _StubKVClient:
    def __init__(self) -> None:
        self.deleted = []
        self.set_calls = []

    async def set(self, *_args, **_kwargs) -> bool:
        self.set_calls.append((_args, _kwargs))
        return True

    async def delete(self, key: str) -> None:
        self.deleted.append(key)


@pytest.mark.asyncio
async def test_feed_recsys_job_captures_failures_to_sentry(monkeypatch):
    captured: list[Exception] = []
    kvrocks_client = _StubKVClient()
    settings = SimpleNamespace(storage_namespace="test")
    runtime = {"feed_sync_service": _FailingFeedSyncService()}

    async def fake_close_runtime_objects(_runtime) -> None:
        pass

    monkeypatch.setattr(feed_recsys_jobs, "get_settings", lambda: settings)
    monkeypatch.setattr(
        feed_recsys_jobs,
        "build_runtime_objects",
        lambda client, resolved_settings: runtime,
    )
    monkeypatch.setattr(
        feed_recsys_jobs,
        "close_runtime_objects",
        fake_close_runtime_objects,
    )
    monkeypatch.setattr(
        feed_recsys_jobs.LoggerService,
        "get",
        lambda self, _name: _StubLogger(),
    )
    monkeypatch.setattr(
        feed_recsys_jobs.sentry_sdk,
        "capture_exception",
        lambda exc: captured.append(exc),
    )

    with pytest.raises(RuntimeError, match="clickhouse unavailable"):
        await feed_recsys_jobs._run_locked_job(
            kvrocks_client,
            "popularity_sync",
            60,
            "sync_global_popularity_pools",
        )

    assert [str(exc) for exc in captured] == ["clickhouse unavailable"]
    assert kvrocks_client.deleted == [
        "test:feed_recsys:{GLOBAL}:jobs:lock:popularity_sync"
    ]


@pytest.mark.asyncio
async def test_publisher_profile_backfill_job_uses_enrichment_runtime(monkeypatch):
    calls: list[str] = []
    kvrocks_client = _StubKVClient()
    settings = SimpleNamespace(
        storage_namespace="test",
        feed_recsys_publisher_profile_backfill_lock_ttl_sec=1800,
    )

    class _StubPublisherProfileEnrichmentService:
        async def backfill_recent_publishers(self):
            calls.append("backfill_recent_publishers")
            return {"selected": 1, "refreshed": 1}

    runtime = {
        "publisher_profile_enrichment_service": _StubPublisherProfileEnrichmentService()
    }

    async def fake_close_runtime_objects(_runtime) -> None:
        pass

    monkeypatch.setattr(feed_recsys_jobs, "get_settings", lambda: settings)
    monkeypatch.setattr(
        feed_recsys_jobs,
        "build_runtime_objects",
        lambda client, resolved_settings: runtime,
    )
    monkeypatch.setattr(
        feed_recsys_jobs,
        "close_runtime_objects",
        fake_close_runtime_objects,
    )
    monkeypatch.setattr(
        feed_recsys_jobs.LoggerService,
        "get",
        lambda self, _name: _StubLogger(),
    )

    await feed_recsys_jobs.run_feed_recsys_publisher_profile_backfill_sync(
        kvrocks_client
    )

    assert calls == ["backfill_recent_publishers"]
    assert kvrocks_client.deleted == [
        "test:feed_recsys:{GLOBAL}:jobs:lock:publisher_profile_backfill_sync"
    ]
