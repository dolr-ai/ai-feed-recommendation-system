from __future__ import annotations

import asyncio
from typing import Optional

from aiohttp import ClientSession

from src.clients.base import BaseApiClient
from src.services.logger_service import LoggerService
from src.utils.http_client import HttpClientFactory


class MetadataServiceClient(BaseApiClient):
    def __init__(
        self,
        settings,
        timeout_sec: float,
        max_retries: int = 0,
        retry_backoff_sec: float = 0.0,
        session: Optional[ClientSession] = None,
    ):
        self._settings = settings
        self._timeout_sec = timeout_sec
        self._max_retries = max(0, max_retries)
        self._retry_backoff_sec = max(0.0, retry_backoff_sec)
        self._session = session
        self._owns_session = session is None
        self._log = LoggerService().get("metadata_service_client")

    async def get_usernames_bulk(self, user_ids: list[str]) -> dict[str, str]:
        unique_user_ids = list(
            dict.fromkeys(str(user_id or "").strip() for user_id in user_ids if user_id)
        )
        if not unique_user_ids:
            return {}

        payload = await self._post_json(
            "metadata-bulk",
            {"users": unique_user_ids},
        )
        if not isinstance(payload, dict):
            raise RuntimeError(
                "metadata service returned unexpected bulk payload type: "
                f"{type(payload).__name__}"
            )

        usernames: dict[str, str] = {}
        for user_id in unique_user_ids:
            row = payload.get(user_id)
            if not isinstance(row, dict):
                continue
            usernames[user_id] = str(row.get("user_name") or "").strip()
        return usernames

    async def _post_json(self, path: str, payload: dict):
        session = await self._get_session()
        url = self._settings.metadata_service_base_url.rstrip("/") + f"/{path.lstrip('/')}"
        headers = {"accept": "application/json"}
        auth_token = str(self._settings.metadata_service_auth_token or "").strip()
        if auth_token:
            headers["authorization"] = auth_token

        attempts = self._max_retries + 1
        last_error: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                async with session.post(
                    url,
                    json=payload,
                    headers=headers,
                ) as response:
                    response.raise_for_status()
                    body = await response.json()
                return self._unwrap_api_result(body)
            except Exception as exc:
                last_error = exc
                if attempt >= attempts:
                    raise
                await asyncio.sleep(self._retry_backoff_sec * attempt)

        raise last_error or RuntimeError("Metadata service request failed")

    @staticmethod
    def _unwrap_api_result(body):
        if isinstance(body, dict):
            if "Ok" in body:
                return body["Ok"]
            if "Err" in body:
                raise RuntimeError(str(body["Err"]))
        return body

    async def _get_session(self) -> ClientSession:
        if self._session is None:
            self._session = HttpClientFactory.create(self._timeout_sec)
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None
