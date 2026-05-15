from __future__ import annotations

import asyncio
import time
from typing import List, Optional

import aiohttp
from aiohttp import ClientSession

from src.clients.base import BaseApiClient
from src.services.logger_service import LoggerService
from src.utils.http_client import HttpClientFactory


class ChatApiResponseError(RuntimeError):
    pass


class ChatApiClient(BaseApiClient):
    def __init__(self, settings, session: Optional[ClientSession] = None):
        self._settings = settings
        self._session = session
        self._owns_session = session is None
        self._max_retries = max(0, int(getattr(settings, "chat_api_max_retries", 2)))
        self._retry_backoff_sec = max(
            0.0,
            float(getattr(settings, "chat_api_retry_backoff_sec", 1.0)),
        )
        self._log = LoggerService().get("chat_api_client")

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
            payload = await self._fetch_page_with_retry(
                session=session,
                base_url=base_url,
                path=path,
                offset=offset,
                limit=limit,
            )

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

    async def _fetch_page_with_retry(
        self,
        session: ClientSession,
        base_url: str,
        path: str,
        offset: int,
        limit: int,
    ):
        url = f"{base_url}{path}"
        attempts = self._max_retries + 1
        last_error: Exception | None = None

        for attempt in range(1, attempts + 1):
            started_at = time.perf_counter()
            status: int | None = None
            try:
                async with session.get(
                    url,
                    params={"offset": offset, "limit": limit},
                    headers={"accept": "application/json"},
                ) as response:
                    status = response.status
                    response.raise_for_status()
                    payload = await response.json()

                elapsed_sec = time.perf_counter() - started_at
                self._log.debug(
                    "Chat API page fetched",
                    extra={
                        "path": path,
                        "offset": offset,
                        "limit": limit,
                        "attempt": attempt,
                        "elapsed_sec": round(elapsed_sec, 3),
                        "status": status,
                    },
                )
                return payload
            except Exception as exc:
                last_error = exc
                elapsed_sec = time.perf_counter() - started_at
                retryable = self._is_retryable_error(exc)
                will_retry = retryable and attempt < attempts
                log_extra = {
                    "path": path,
                    "offset": offset,
                    "limit": limit,
                    "attempt": attempt,
                    "max_attempts": attempts,
                    "elapsed_sec": round(elapsed_sec, 3),
                    "status": getattr(exc, "status", status),
                    "error_type": type(exc).__name__,
                    "retryable": retryable,
                    "will_retry": will_retry,
                }
                if will_retry:
                    self._log.warning(
                        "Chat API page request failed, retrying",
                        extra=log_extra,
                    )
                    await asyncio.sleep(self._retry_backoff_sec * attempt)
                    continue

                self._log.error(
                    "Chat API page request failed",
                    extra=log_extra,
                    exc_info=True,
                )
                raise

        raise last_error or RuntimeError("Chat API page request failed")

    @staticmethod
    def _is_retryable_error(exc: Exception) -> bool:
        if isinstance(exc, asyncio.TimeoutError):
            return True
        if isinstance(exc, aiohttp.ClientResponseError):
            return 500 <= exc.status < 600
        return False

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = HttpClientFactory.create(self._settings.chat_api_timeout)
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None
