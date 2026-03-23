import json
from dataclasses import asdict

from src.models.influencer import PipelineState
from src.repository.base import BaseCheckpointRepo
from src.utils.kvrocks import checkpoint_key, checkpoint_pattern


class CheckpointRepository(BaseCheckpointRepo):
    def __init__(self, client, settings):
        self._client = client
        self._settings = settings

    async def save(self, state: PipelineState) -> None:
        payload = asdict(state)
        payload["timestamp"] = state.timestamp.isoformat()
        await self._client.set(
            checkpoint_key(state.step),
            json.dumps(payload),
            ex=self._settings.checkpoint_ttl_sec,
        )

    async def delete(self, step: str) -> None:
        await self._client.delete(checkpoint_key(step))

    async def delete_all(self) -> None:
        keys = [key async for key in self._client.scan_iter(match=checkpoint_pattern())]
        if keys:
            await self._client.delete(*keys)
