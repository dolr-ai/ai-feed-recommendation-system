from __future__ import annotations

from typing import Optional

from aiohttp import ClientSession

from src.clients.base import BaseApiClient
from src.utils.http_client import HttpClientFactory


class OffchainRewardsClient(BaseApiClient):
    def __init__(self, settings, session: Optional[ClientSession] = None):
        self._settings = settings
        self._session = session
        self._owns_session = session is None

    async def get_bulk_video_stats(self, video_ids: list[str]) -> dict[str, dict[str, int]]:
        unique_video_ids = list(dict.fromkeys(video_id for video_id in video_ids if video_id))
        
        if not unique_video_ids:
            return {}

        session = await self._get_session()
        base_url = self._settings.offchain_agent_base_url.rstrip("/")
        payload = {"video_ids": unique_video_ids}

        async with session.post(
            f"{base_url}/api/v1/rewards/videos/bulk-stats-v2",
            json=payload,
            headers={"accept": "application/json"},
        ) as response:
            response.raise_for_status()
            rows = await response.json()

        result: dict[str, dict[str, int]] = {}
        
        for row in rows:
            video_id = str(row.get("video_id") or "").strip()
            if not video_id:
                continue
            result[video_id] = {
                "num_views_loggedin": int(row.get("total_count_loggedin") or 0),
                "num_views_all": int(row.get("total_count_all") or 0),
            }
        return result

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = HttpClientFactory.create(self._settings.offchain_agent_timeout)
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None
