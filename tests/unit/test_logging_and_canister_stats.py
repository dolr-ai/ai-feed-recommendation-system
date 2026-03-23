import json
import logging
from datetime import datetime, timezone

import httpx
import pytest

from src.clients.canister_client import CanisterClient, _ConfiguredIcClient
from src.models.influencer import Influencer
from src.services.logger_service import JsonFormatter


def _build_influencer(influencer_id: str) -> Influencer:
    return Influencer(
        id=influencer_id,
        name=influencer_id,
        display_name=influencer_id.upper(),
        avatar_url="https://example.com/avatar.png",
        description="desc",
        category="test",
        created_at=datetime.now(timezone.utc),
        is_active="active",
    )


def test_json_formatter_omits_none_fields():
    formatter = JsonFormatter()
    record = logging.makeLogRecord(
        {
            "name": "influencer.test",
            "levelname": "INFO",
            "levelno": logging.INFO,
            "msg": "hello",
            "args": (),
            "taskName": None,
            "step": "fetch_canister_data",
        }
    )

    payload = json.loads(formatter.format(record))

    assert payload["message"] == "hello"
    assert payload["step"] == "fetch_canister_data"
    assert "taskName" not in payload


@pytest.mark.asyncio
async def test_canister_client_collects_summary_stats(settings, monkeypatch):
    client = CanisterClient(settings)
    influencers = [
        _build_influencer("ok-user"),
        _build_influencer("missing-user"),
        _build_influencer("timeout-user"),
    ]

    async def fake_get_profile(principal: str) -> dict:
        if principal == "missing-user":
            return {"Err": "User not found"}
        return {"Ok": {"followers_count": 7}}

    async def fake_get_posts(principal: str, cursor: int = 0, limit: int = 10) -> list[dict]:
        if principal == "timeout-user":
            raise TimeoutError("The read operation timed out")
        return [
            {
                "status": "ReadyToView",
                "share_count": 2,
                "likes": ["a", "b"],
                "view_stats": {
                    "total_view_count": 10,
                    "average_watch_percentage": 75.0,
                },
                "created_at": {"secs_since_epoch": 1_700_000_000},
            }
        ]

    monkeypatch.setattr(client, "get_profile", fake_get_profile)
    monkeypatch.setattr(client, "get_posts", fake_get_posts)

    enriched = await client.fetch_all(influencers)
    stats = client.get_stats()

    assert len(enriched) == 3
    assert stats["profile_ok"] == 2
    assert stats["profile_user_not_found"] == 1
    assert stats["profile_timeout"] == 0
    assert stats["profile_other_err"] == 0
    assert stats["posts_ok"] == 2
    assert stats["posts_timeout"] == 1
    assert stats["posts_other_err"] == 0
    assert enriched[0].followers_count == 7
    assert enriched[1].followers_count == 0
    assert enriched[2].followers_count == 7
    assert enriched[0].ready_post_count == 1
    assert enriched[2].ready_post_count == 0


def test_configured_ic_client_retries_timeout(monkeypatch):
    client = _ConfiguredIcClient(
        url="https://ic0.app",
        timeout_sec=15.0,
        retries=2,
        retry_backoff_sec=0.0,
    )
    calls = {"count": 0}

    class StubResponse:
        content = b"ok"

    def fake_post(*args, **kwargs):
        calls["count"] += 1
        if calls["count"] < 3:
            raise httpx.ReadTimeout("timed out")
        return StubResponse()

    monkeypatch.setattr(httpx, "post", fake_post)

    result = client.query("abc", b"payload")

    assert result == b"ok"
    assert calls["count"] == 3
