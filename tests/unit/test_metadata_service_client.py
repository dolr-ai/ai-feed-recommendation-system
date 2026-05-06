import pytest

from src.clients.metadata_service_client import MetadataServiceClient


@pytest.mark.asyncio
async def test_metadata_service_client_raises_on_unexpected_bulk_payload(settings, monkeypatch):
    client = MetadataServiceClient(settings, timeout_sec=0.5)

    async def fake_post_json(_path, _payload):
        return []

    monkeypatch.setattr(client, "_post_json", fake_post_json)

    with pytest.raises(RuntimeError, match="unexpected bulk payload type"):
        await client.get_usernames_bulk(["publisher-1"])
