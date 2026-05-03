from types import SimpleNamespace

import pytest

from src.clients.clickhouse_client import build_clickhouse_client
from src.core.settings import Settings


class _FakeQueryResult:
    def __init__(self, rows, column_names):
        self.result_rows = rows
        self.column_names = column_names


class _FakeClickHouseConnectClient:
    def __init__(self):
        self.query_calls = []
        self.command_calls = []
        self.closed = False

    def query(self, query, parameters=None):
        self.query_calls.append((query, parameters))
        return _FakeQueryResult(
            rows=[("video-1", 10), ("video-2", 20)],
            column_names=["video_id", "score"],
        )

    def command(self, query, parameters=None):
        self.command_calls.append((query, parameters))
        return "ok"

    def close(self):
        self.closed = True


@pytest.mark.asyncio
async def test_clickhouse_client_uses_clickhouse_connect(monkeypatch):
    fake_client = _FakeClickHouseConnectClient()
    captured = {}

    def fake_get_client(**kwargs):
        captured.update(kwargs)
        return fake_client

    monkeypatch.setitem(
        __import__("sys").modules,
        "clickhouse_connect",
        SimpleNamespace(get_client=fake_get_client),
    )

    settings = Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        clickhouse_host="host.docker.internal",
        clickhouse_port=8443,
        clickhouse_database="yral",
        clickhouse_username="reader-user",
        clickhouse_password="reader-pass",
        clickhouse_secure=True,
        clickhouse_verify=False,
        clickhouse_connect_timeout_sec=11.5,
        clickhouse_query_timeout_sec=44.0,
    )

    client = build_clickhouse_client(settings)

    rows = await client.fetch_all(
        "SELECT video_id, score FROM some_table WHERE bucket = %(bucket)s",
        {"bucket": "99_100"},
    )
    command_result = await client.execute(
        "SYSTEM FLUSH LOGS WHERE bucket = %(bucket)s",
        {"bucket": "99_100"},
    )
    await client.close()

    assert rows == [
        {"video_id": "video-1", "score": 10},
        {"video_id": "video-2", "score": 20},
    ]
    assert command_result == "ok"
    assert captured == {
        "host": "host.docker.internal",
        "port": 8443,
        "database": "yral",
        "username": "reader-user",
        "password": "reader-pass",
        "secure": True,
        "verify": False,
        "connect_timeout": 11.5,
        "send_receive_timeout": 44.0,
    }
    assert fake_client.query_calls == [
        (
            "SELECT video_id, score FROM some_table WHERE bucket = %(bucket)s",
            {"bucket": "99_100"},
        )
    ]
    assert fake_client.command_calls == [
        (
            "SYSTEM FLUSH LOGS WHERE bucket = %(bucket)s",
            {"bucket": "99_100"},
        )
    ]
    assert fake_client.closed is True
