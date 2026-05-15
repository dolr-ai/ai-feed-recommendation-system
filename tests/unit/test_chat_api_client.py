import asyncio
from types import SimpleNamespace

import aiohttp
import pytest

from src.clients.chat_api_client import ChatApiClient, ChatApiResponseError


class _FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        if self.status >= 400:
            raise aiohttp.ClientResponseError(
                request_info=None,
                history=(),
                status=self.status,
                message="upstream error",
            )
        return None

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)
        self.calls = []

    def get(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _settings(**overrides):
    values = {
        "chat_api_base_url": "https://chat-ai.rishi.yral.com",
        "chat_api_timeout": 30,
        "chat_api_max_retries": 2,
        "chat_api_retry_backoff_sec": 0,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


@pytest.mark.asyncio
async def test_get_all_influencers_raises_when_upstream_returns_empty_list():
    session = _FakeSession([_FakeResponse({"influencers": [], "total": 0})])
    client = ChatApiClient(_settings(), session=session)

    with pytest.raises(
        ChatApiResponseError,
        match=r"https://chat-ai\.rishi\.yral\.com/api/v1/influencers returned zero influencers",
    ):
        await client.get_all_influencers()


@pytest.mark.asyncio
async def test_get_trending_allows_empty_upstream_list():
    session = _FakeSession([_FakeResponse({"influencers": [], "total": 0})])
    client = ChatApiClient(_settings(), session=session)

    assert await client.get_trending() == []


@pytest.mark.asyncio
async def test_get_all_influencers_retries_timeout_and_logs_attempt(caplog):
    session = _FakeSession(
        [
            asyncio.TimeoutError(),
            _FakeResponse({"influencers": [{"id": "i1"}], "total": 1}),
        ]
    )
    client = ChatApiClient(_settings(chat_api_max_retries=1), session=session)

    with caplog.at_level("WARNING", logger="influencer.chat_api_client"):
        assert await client.get_all_influencers() == [{"id": "i1"}]

    assert len(session.calls) == 2
    assert session.calls[0][1]["params"] == {"offset": 0, "limit": 100}
    retry_log = next(
        record
        for record in caplog.records
        if record.message == "Chat API page request failed, retrying"
    )
    assert retry_log.offset == 0
    assert retry_log.limit == 100
    assert retry_log.attempt == 1
    assert retry_log.will_retry is True
    assert retry_log.error_type == "TimeoutError"


@pytest.mark.asyncio
async def test_get_all_influencers_retries_5xx_response():
    session = _FakeSession(
        [
            _FakeResponse({"error": "temporary"}, status=503),
            _FakeResponse({"influencers": [{"id": "i1"}], "total": 1}),
        ]
    )
    client = ChatApiClient(_settings(chat_api_max_retries=1), session=session)

    assert await client.get_all_influencers() == [{"id": "i1"}]
    assert len(session.calls) == 2


@pytest.mark.asyncio
async def test_get_all_influencers_does_not_retry_4xx_response():
    session = _FakeSession(
        [
            _FakeResponse({"error": "bad request"}, status=400),
            _FakeResponse({"influencers": [{"id": "i1"}], "total": 1}),
        ]
    )
    client = ChatApiClient(_settings(chat_api_max_retries=2), session=session)

    with pytest.raises(aiohttp.ClientResponseError):
        await client.get_all_influencers()

    assert len(session.calls) == 1
