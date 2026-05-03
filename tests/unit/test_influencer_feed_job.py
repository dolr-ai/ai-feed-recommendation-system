from types import SimpleNamespace

import pytest

from src.jobs import influencer_feed_job


class _StubLogger:
    def info(self, *_args, **_kwargs) -> None:
        pass

    def error(self, *_args, **_kwargs) -> None:
        pass


class _StubPipelineService:
    async def run(self) -> None:
        raise RuntimeError("upstream 500")


class _StubChatApiClient:
    def __init__(self) -> None:
        self.closed = False

    async def close(self) -> None:
        self.closed = True


@pytest.mark.asyncio
async def test_run_influencer_feed_sync_captures_pipeline_failures(monkeypatch):
    kvrocks_client = object()
    captured: list[Exception] = []
    released: list[tuple[object, str]] = []
    chat_api_client = _StubChatApiClient()
    runtime = {
        "pipeline_service": _StubPipelineService(),
        "chat_api_client": chat_api_client,
    }

    async def fake_acquire_job_lock(*_args, **_kwargs) -> bool:
        return True

    async def fake_release_job_lock(client, job_name: str) -> None:
        released.append((client, job_name))

    monkeypatch.setattr(
        influencer_feed_job,
        "get_settings",
        lambda: SimpleNamespace(feed_sync_interval_sec=60),
    )
    monkeypatch.setattr(
        influencer_feed_job,
        "build_runtime_objects",
        lambda client, settings: runtime,
    )
    monkeypatch.setattr(
        influencer_feed_job,
        "acquire_job_lock",
        fake_acquire_job_lock,
    )
    monkeypatch.setattr(
        influencer_feed_job,
        "release_job_lock",
        fake_release_job_lock,
    )
    monkeypatch.setattr(
        influencer_feed_job.LoggerService,
        "get",
        lambda self, _name: _StubLogger(),
    )
    monkeypatch.setattr(
        influencer_feed_job.sentry_sdk,
        "capture_exception",
        lambda exc: captured.append(exc),
    )

    await influencer_feed_job.run_influencer_feed_sync(kvrocks_client)

    assert [str(exc) for exc in captured] == ["upstream 500"]
    assert chat_api_client.closed is True
    assert released == [(kvrocks_client, "influencer_feed_sync")]
