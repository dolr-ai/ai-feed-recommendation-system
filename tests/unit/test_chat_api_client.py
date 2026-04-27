from types import SimpleNamespace

import pytest

from src.clients.chat_api_client import ChatApiClient, ChatApiResponseError


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self) -> None:
        return None

    async def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = list(responses)

    def get(self, *_args, **_kwargs):
        return self._responses.pop(0)


@pytest.mark.asyncio
async def test_get_all_influencers_raises_when_upstream_returns_empty_list():
    settings = SimpleNamespace(
        chat_api_base_url="https://chat-ai.rishi.yral.com",
        chat_api_timeout=30,
    )
    session = _FakeSession([_FakeResponse({"influencers": [], "total": 0})])
    client = ChatApiClient(settings, session=session)

    with pytest.raises(
        ChatApiResponseError,
        match=r"https://chat-ai\.rishi\.yral\.com/api/v1/influencers returned zero influencers",
    ):
        await client.get_all_influencers()


@pytest.mark.asyncio
async def test_get_trending_allows_empty_upstream_list():
    settings = SimpleNamespace(
        chat_api_base_url="https://chat-ai.rishi.yral.com",
        chat_api_timeout=30,
    )
    session = _FakeSession([_FakeResponse({"influencers": [], "total": 0})])
    client = ChatApiClient(settings, session=session)

    assert await client.get_trending() == []
