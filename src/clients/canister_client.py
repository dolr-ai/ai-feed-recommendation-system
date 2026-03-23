from __future__ import annotations

import asyncio
import base64
import math
from collections import Counter
from datetime import datetime, timezone
from typing import Any, Optional

import httpx

from src.clients.base import BaseApiClient
from src.models.influencer import Influencer
from src.services.logger_service import LoggerService
from src.utils.rate_limiter import AsyncRateLimiter


def candid_hash(name: str) -> str:
    value = 0
    for char in name:
        value = (value * 223 + ord(char)) % (2**32)
    return f"_{value}"


class CanisterClient(BaseApiClient):
    def __init__(self, settings):
        self._settings = settings
        self._log = LoggerService().get("canister_client")
        self._limiter = AsyncRateLimiter(settings.canister_rps)
        self._semaphore = asyncio.Semaphore(settings.canister_concurrency)
        self._agent = None
        self._types = None
        self._encode = None
        self._principal_ready = False
        self._ok_key = candid_hash("Ok")
        self._err_key = candid_hash("Err")
        self._followers_key = candid_hash("followers_count")
        self._status_key = candid_hash("status")
        self._ready_key = candid_hash("ReadyToView")
        self._share_count_key = candid_hash("share_count")
        self._likes_key = candid_hash("likes")
        self._created_at_key = candid_hash("created_at")
        self._secs_since_epoch_key = candid_hash("secs_since_epoch")
        self._view_stats_key = candid_hash("view_stats")
        self._total_views_key = candid_hash("total_view_count")
        self._avg_watch_key = candid_hash("average_watch_percentage")
        self._stats = Counter()

    async def get_profile(self, principal: str) -> dict:
        await self._ensure_agent()
        await self._limiter.acquire()
        raw = await asyncio.to_thread(
            self._agent.query_raw,
            self._settings.profile_canister_id,
            "get_user_profile_details_v7",
            self._encode([{"type": self._types.Principal, "value": principal}]),
        )
        return self._normalize_profile(raw)

    async def get_posts(self, principal: str, cursor: int = 0, limit: int = 10) -> list[dict]:
        await self._ensure_agent()
        await self._limiter.acquire()
        raw = await asyncio.to_thread(
            self._agent.query_raw,
            self._settings.posts_canister_id,
            "get_posts_of_this_user_profile_with_pagination_cursor",
            self._encode(
                [
                    {"type": self._types.Principal, "value": principal},
                    {"type": self._types.Nat64, "value": cursor},
                    {"type": self._types.Nat64, "value": limit},
                ]
            ),
        )
        return self._normalize_posts(raw)

    async def fetch_all(self, influencers: list[Influencer]) -> list[Influencer]:
        self._stats = Counter(
            {
                "profile_ok": 0,
                "profile_user_not_found": 0,
                "profile_timeout": 0,
                "profile_other_err": 0,
                "posts_ok": 0,
                "posts_timeout": 0,
                "posts_other_err": 0,
            }
        )

        async def _enrich_one(influencer: Influencer) -> Influencer:
            try:
                async with self._semaphore:
                    result = await self.get_profile(influencer.id)
            except Exception as exc:
                if self._is_timeout_error(exc):
                    self._stats["profile_timeout"] += 1
                else:
                    self._stats["profile_other_err"] += 1
                self._log.warning(
                    "Profile fetch failed",
                    extra={
                        "influencer_id": influencer.id,
                        "error": str(exc),
                        "error_type": self._classify_error(exc),
                    },
                )
                return influencer

            if "Ok" in result:
                self._stats["profile_ok"] += 1
                influencer.followers_count = int(
                    result["Ok"].get("followers_count", 0)
                )
            else:
                error = str(result.get("Err") or "unknown profile error")
                if error == "User not found":
                    self._stats["profile_user_not_found"] += 1
                else:
                    self._stats["profile_other_err"] += 1
                    self._log.warning(
                        "Profile Err",
                        extra={
                            "influencer_id": influencer.id,
                            "error": error,
                        },
                    )

            try:
                async with self._semaphore:
                    posts = await self.get_posts(influencer.id, cursor=0, limit=10)
            except Exception as exc:
                if self._is_timeout_error(exc):
                    self._stats["posts_timeout"] += 1
                else:
                    self._stats["posts_other_err"] += 1
                self._log.warning(
                    "Posts fetch failed",
                    extra={
                        "influencer_id": influencer.id,
                        "error": str(exc),
                        "error_type": self._classify_error(exc),
                    },
                )
                return influencer

            self._stats["posts_ok"] += 1
            ready = [post for post in posts if post.get("status") == "ReadyToView"]
            influencer.ready_post_count = len(ready)
            if ready:
                influencer.total_video_views = sum(
                    int(post["view_stats"]["total_view_count"]) for post in ready
                )
                influencer.share_count_total = sum(
                    int(post.get("share_count", 0)) for post in ready
                )
                influencer.likes_count_total = sum(
                    len(post.get("likes", [])) for post in ready
                )
                watch_pcts = [
                    float(post["view_stats"]["average_watch_percentage"])
                    for post in ready
                    if "view_stats" in post
                ]
                influencer.avg_watch_pct_mean = (
                    sum(watch_pcts) / len(watch_pcts) if watch_pcts else 0.0
                )
                influencer.last_post_at = datetime.fromtimestamp(
                    max(
                        int(post["created_at"]["secs_since_epoch"])
                        for post in ready
                    ),
                    tz=timezone.utc,
                )
            return influencer

        return await asyncio.gather(*[_enrich_one(influencer) for influencer in influencers])

    def get_stats(self) -> dict[str, int]:
        return dict(self._stats)

    async def close(self) -> None:
        return None

    async def _ensure_agent(self) -> None:
        if self._agent is not None:
            return

        from ic.agent import Agent
        from ic.candid import Types, encode
        from ic.identity import Identity
        from ic.principal import CRC_LENGTH_IN_BYTES, Principal

        if not self._principal_ready:
            def _fixed_from_str(value: str):
                normalized = value.replace("-", "")
                pad_len = math.ceil(len(normalized) / 8) * 8 - len(normalized)
                raw_bytes = base64.b32decode(
                    normalized.upper().encode() + b"=" * pad_len
                )
                if len(raw_bytes) < CRC_LENGTH_IN_BYTES:
                    raise ValueError("principal length error")
                return Principal(bytes=raw_bytes[CRC_LENGTH_IN_BYTES:])

            Principal.from_str = staticmethod(_fixed_from_str)
            self._principal_ready = True

        self._types = Types
        self._encode = encode
        self._agent = Agent(
            Identity(),
            _ConfiguredIcClient(
                url=self._settings.ic_gateway_base_url,
                timeout_sec=self._settings.canister_http_timeout_sec,
                retries=self._settings.canister_query_retries,
                retry_backoff_sec=self._settings.canister_retry_backoff_sec,
            ),
        )

    def _normalize_profile(self, raw: Any) -> dict:
        if not raw:
            return {"Err": "empty response"}
        payload = self._unwrap_value(raw[0])
        if not isinstance(payload, dict):
            return {"Err": f"unexpected profile payload type: {type(payload).__name__}"}
        if self._ok_key in payload:
            ok_payload = payload[self._ok_key]
            return {
                "Ok": {
                    "followers_count": int(
                        ok_payload.get("followers_count", ok_payload.get(self._followers_key, 0))
                    )
                }
            }
        if self._err_key in payload:
            return {"Err": payload[self._err_key]}
        if "Ok" in payload or "Err" in payload:
            return payload
        return {"Err": f"unknown profile payload keys: {list(payload.keys())}"}

    def _normalize_posts(self, raw: Any) -> list[dict]:
        if not raw:
            return []
        posts = self._unwrap_value(raw[0])
        if not isinstance(posts, list):
            self._log.warning(
                "Unexpected posts payload",
                extra={"payload_type": type(posts).__name__},
            )
            return []
        normalized = []
        for post in posts:
            item = self._unwrap_value(post)
            if not isinstance(item, dict):
                self._log.warning(
                    "Skipping malformed post payload",
                    extra={"payload_type": type(item).__name__},
                )
                continue
            status = self._extract_status(item)
            view_stats = item.get("view_stats", item.get(self._view_stats_key, {}))
            created_at = item.get("created_at", item.get(self._created_at_key, {}))
            normalized.append(
                {
                    "status": status,
                    "share_count": int(
                        item.get("share_count", item.get(self._share_count_key, 0))
                    ),
                    "likes": item.get("likes", item.get(self._likes_key, [])) or [],
                    "view_stats": {
                        "total_view_count": int(
                            view_stats.get(
                                "total_view_count",
                                view_stats.get(self._total_views_key, 0),
                            )
                        ),
                        "average_watch_percentage": float(
                            view_stats.get(
                                "average_watch_percentage",
                                view_stats.get(self._avg_watch_key, 0),
                            )
                        ),
                    },
                    "created_at": {
                        "secs_since_epoch": int(
                            created_at.get(
                                "secs_since_epoch",
                                created_at.get(self._secs_since_epoch_key, 0),
                            )
                        )
                    },
                }
            )
        return normalized

    def _extract_status(self, post: dict) -> str:
        status = post.get("status", post.get(self._status_key, {}))
        if isinstance(status, str):
            return status
        if isinstance(status, dict):
            if "ReadyToView" in status or self._ready_key in status:
                return "ReadyToView"
            if status:
                return next(iter(status.keys()))
        return "Unknown"

    @staticmethod
    def _unwrap_value(payload: Any) -> Any:
        if isinstance(payload, dict) and "value" in payload:
            return payload["value"]
        return payload

    @staticmethod
    def _is_timeout_error(exc: Exception) -> bool:
        return "timed out" in str(exc).lower()

    @classmethod
    def _classify_error(cls, exc: Exception) -> str:
        if cls._is_timeout_error(exc):
            return "timeout"
        return type(exc).__name__


class _ConfiguredIcClient:
    def __init__(
        self,
        url: str,
        timeout_sec: float,
        retries: int,
        retry_backoff_sec: float,
    ) -> None:
        self.url = url
        self._timeout = httpx.Timeout(timeout_sec)
        self._retries = max(0, retries)
        self._retry_backoff_sec = max(0.0, retry_backoff_sec)

    def query(self, canister_id, data):
        endpoint = self.url + "/api/v2/canister/" + canister_id + "/query"
        headers = {"Content-Type": "application/cbor"}
        return self._post_with_retry(endpoint, data, headers).content

    def call(self, canister_id, req_id, data):
        endpoint = self.url + "/api/v2/canister/" + canister_id + "/call"
        headers = {"Content-Type": "application/cbor"}
        self._post_with_retry(endpoint, data, headers)
        return req_id

    def read_state(self, canister_id, data):
        endpoint = self.url + "/api/v2/canister/" + canister_id + "/read_state"
        headers = {"Content-Type": "application/cbor"}
        return self._post_with_retry(endpoint, data, headers).content

    def status(self):
        endpoint = self.url + "/api/v2/status"
        return httpx.get(endpoint, timeout=self._timeout).content

    def _post_with_retry(self, endpoint: str, data, headers: dict) -> httpx.Response:
        attempts = self._retries + 1
        last_error: Exception | None = None
        for attempt in range(attempts):
            try:
                return httpx.post(
                    endpoint,
                    data=data,
                    headers=headers,
                    timeout=self._timeout,
                )
            except httpx.TimeoutException as exc:
                last_error = exc
                if attempt == attempts - 1:
                    raise
                if self._retry_backoff_sec > 0:
                    delay = self._retry_backoff_sec * (attempt + 1)
                    import time

                    time.sleep(delay)
        if last_error is not None:
            raise last_error
        raise RuntimeError("unreachable")
