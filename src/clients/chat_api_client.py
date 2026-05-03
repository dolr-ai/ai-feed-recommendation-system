from __future__ import annotations

from typing import List, Optional

from aiohttp import ClientSession

from src.clients.base import BaseApiClient
from src.utils.http_client import HttpClientFactory


class ChatApiResponseError(RuntimeError):
    pass


class ChatApiClient(BaseApiClient):
    def __init__(self, settings, session: Optional[ClientSession] = None):
        self._settings = settings
        self._session = session
        self._owns_session = session is None

    async def get_all_influencers(self) -> List[dict]:
        return await self._fetch_paginated(
            "/api/v1/influencers",
            require_non_empty=True,
        )

    async def get_trending(self) -> List[dict]:
        return await self._fetch_paginated("/api/v1/influencers/trending")

    async def _fetch_paginated(self, path: str, require_non_empty: bool = False) -> List[dict]:
        session = await self._get_session()
        base_url = self._settings.chat_api_base_url.rstrip("/")
        limit = 100
        offset = 0
        total = None
        items: List[dict] = []

        while True:
            async with session.get(
                f"{base_url}{path}",
                params={"offset": offset, "limit": limit},
                headers={"accept": "application/json"},
            ) as response:
                response.raise_for_status()
                payload = await response.json()

            if not isinstance(payload, dict):
                raise ChatApiResponseError(
                    f"{base_url}{path} returned {type(payload).__name__}, expected JSON object"
                )

            batch = payload.get("influencers")
            if not isinstance(batch, list):
                raise ChatApiResponseError(
                    f"{base_url}{path} missing list field 'influencers'"
                )

            items.extend(batch)
            if total is None:
                total = int(payload.get("total", len(batch)))
            offset += limit
            if len(batch) < limit or offset >= total:
                break

        if require_non_empty and not items:
            raise ChatApiResponseError(f"{base_url}{path} returned zero influencers")

        return items

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = HttpClientFactory.create(self._settings.chat_api_timeout)
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None
