import argparse
import asyncio
import base64
import json
import math
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import httpx

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clients.canister_client import candid_hash
from src.clients.chat_api_client import ChatApiClient
from src.core.settings import get_settings
from src.utils.rate_limiter import AsyncRateLimiter


def _parse_csv_floats(value: str) -> list[float]:
    return [float(item.strip()) for item in value.split(",") if item.strip()]


def _parse_csv_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[index]


@dataclass
class ProbeResult:
    endpoint: str
    principal_id: str
    status: str
    total_latency_ms: float
    request_latency_ms: float
    queue_wait_ms: float
    detail: str | None = None
    http_status: int | None = None
    retry_after: str | None = None


@dataclass
class RoundSummary:
    endpoint: str
    rps: float
    concurrency: int
    request_count: int
    cooldown_sec: float
    timeout_sec: float
    counts: dict[str, int]
    total_latency_ms_p50: float | None
    total_latency_ms_p95: float | None
    total_latency_ms_max: float | None
    request_latency_ms_p50: float | None
    request_latency_ms_p95: float | None
    request_latency_ms_max: float | None
    queue_wait_ms_p50: float | None
    queue_wait_ms_p95: float | None
    queue_wait_ms_max: float | None
    retry_after_values: list[str]


class ProbeHttpError(Exception):
    def __init__(self, status_code: int, body: str, retry_after: str | None) -> None:
        super().__init__(f"HTTP {status_code}: {body[:200]}")
        self.status_code = status_code
        self.body = body
        self.retry_after = retry_after


class ProbeIcHttpClient:
    def __init__(self, url: str, timeout_sec: float) -> None:
        self.url = url.rstrip("/")
        self._timeout = httpx.Timeout(timeout_sec)

    def query(self, canister_id, data):
        endpoint = f"{self.url}/api/v2/canister/{canister_id}/query"
        response = httpx.post(
            endpoint,
            data=data,
            headers={"Content-Type": "application/cbor"},
            timeout=self._timeout,
        )
        if response.status_code != 200:
            raise ProbeHttpError(
                status_code=response.status_code,
                body=response.text,
                retry_after=response.headers.get("Retry-After"),
            )
        return response.content


class ProbeCanisterAdapter:
    def __init__(self, settings, timeout_sec: float) -> None:
        self._settings = settings
        self._ok_key = candid_hash("Ok")
        self._err_key = candid_hash("Err")
        self._followers_key = candid_hash("followers_count")
        self._ready_key = candid_hash("ReadyToView")
        self._status_key = candid_hash("status")
        self._view_stats_key = candid_hash("view_stats")
        self._created_at_key = candid_hash("created_at")
        self._ensure_agent(timeout_sec)

    def _ensure_agent(self, timeout_sec: float) -> None:
        from ic.agent import Agent
        from ic.candid import Types, encode
        from ic.identity import Identity
        from ic.principal import CRC_LENGTH_IN_BYTES, Principal

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
        self._types = Types
        self._encode = encode
        self._agent = Agent(
            Identity(),
            ProbeIcHttpClient(self._settings.ic_gateway_base_url, timeout_sec),
        )

    async def query_profile(self, principal: str) -> dict:
        raw = await asyncio.to_thread(
            self._agent.query_raw,
            self._settings.profile_canister_id,
            "get_user_profile_details_v7",
            self._encode([{"type": self._types.Principal, "value": principal}]),
        )
        return self._normalize_profile(raw)

    async def query_posts(self, principal: str, limit: int) -> list[dict]:
        raw = await asyncio.to_thread(
            self._agent.query_raw,
            self._settings.posts_canister_id,
            "get_posts_of_this_user_profile_with_pagination_cursor",
            self._encode(
                [
                    {"type": self._types.Principal, "value": principal},
                    {"type": self._types.Nat64, "value": 0},
                    {"type": self._types.Nat64, "value": limit},
                ]
            ),
        )
        return self._normalize_posts(raw)

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
        payload = self._unwrap_value(raw[0])
        if not isinstance(payload, list):
            return []
        normalized = []
        for post in payload:
            item = self._unwrap_value(post)
            if not isinstance(item, dict):
                continue
            status = item.get("status", item.get(self._status_key, {}))
            if isinstance(status, dict) and self._ready_key in status:
                status = "ReadyToView"
            elif not isinstance(status, str):
                status = "Unknown"
            normalized.append(
                {
                    "status": status,
                    "view_stats": item.get("view_stats", item.get(self._view_stats_key, {})),
                    "created_at": item.get("created_at", item.get(self._created_at_key, {})),
                }
            )
        return normalized

    @staticmethod
    def _unwrap_value(payload: Any) -> Any:
        if isinstance(payload, dict) and "value" in payload:
            return payload["value"]
        return payload


class CanisterRateProbe:
    def __init__(
        self,
        settings,
        principals: list[str],
        endpoint: str,
        posts_limit: int,
        request_count: int,
        timeout_sec: float,
    ) -> None:
        self._settings = settings
        self._principals = principals
        self._endpoint = endpoint
        self._posts_limit = posts_limit
        self._request_count = request_count
        self._timeout_sec = timeout_sec

    async def run_round(self, rps: float, concurrency: int, cooldown_sec: float) -> RoundSummary:
        adapter = ProbeCanisterAdapter(self._settings, self._timeout_sec)
        limiter = AsyncRateLimiter(rps)
        semaphore = asyncio.Semaphore(concurrency)

        async def _invoke(index: int) -> ProbeResult:
            principal_id = self._principals[index % len(self._principals)]
            started = time.perf_counter()
            request_started = started
            try:
                await limiter.acquire()
                async with semaphore:
                    request_started = time.perf_counter()
                    if self._endpoint == "profile":
                        payload = await adapter.query_profile(principal_id)
                        status, detail = self._classify_profile_payload(payload)
                    else:
                        payload = await adapter.query_posts(principal_id, self._posts_limit)
                        status, detail = self._classify_posts_payload(payload)
                    finished = time.perf_counter()
                    return ProbeResult(
                        endpoint=self._endpoint,
                        principal_id=principal_id,
                        status=status,
                        total_latency_ms=(finished - started) * 1000,
                        request_latency_ms=(finished - request_started) * 1000,
                        queue_wait_ms=(request_started - started) * 1000,
                        detail=detail,
                    )
            except ProbeHttpError as exc:
                finished = time.perf_counter()
                status = "rate_limited" if exc.status_code == 429 else f"http_{exc.status_code}"
                return ProbeResult(
                    endpoint=self._endpoint,
                    principal_id=principal_id,
                    status=status,
                    total_latency_ms=(finished - started) * 1000,
                    request_latency_ms=(finished - request_started) * 1000,
                    queue_wait_ms=(request_started - started) * 1000,
                    detail=exc.body[:200],
                    http_status=exc.status_code,
                    retry_after=exc.retry_after,
                )
            except Exception as exc:
                finished = time.perf_counter()
                status = self._classify_exception(exc)
                return ProbeResult(
                    endpoint=self._endpoint,
                    principal_id=principal_id,
                    status=status,
                    total_latency_ms=(finished - started) * 1000,
                    request_latency_ms=(finished - request_started) * 1000,
                    queue_wait_ms=(request_started - started) * 1000,
                    detail=str(exc),
                )

        results = await asyncio.gather(*[_invoke(index) for index in range(self._request_count)])
        counts = Counter(result.status for result in results)
        total_latencies = [result.total_latency_ms for result in results]
        request_latencies = [result.request_latency_ms for result in results]
        queue_waits = [result.queue_wait_ms for result in results]
        retry_after_values = sorted(
            {
                value
                for value in (result.retry_after for result in results)
                if value
            }
        )
        summary = RoundSummary(
            endpoint=self._endpoint,
            rps=rps,
            concurrency=concurrency,
            request_count=self._request_count,
            cooldown_sec=cooldown_sec,
            timeout_sec=self._timeout_sec,
            counts=dict(counts),
            total_latency_ms_p50=_percentile(total_latencies, 50),
            total_latency_ms_p95=_percentile(total_latencies, 95),
            total_latency_ms_max=max(total_latencies) if total_latencies else None,
            request_latency_ms_p50=_percentile(request_latencies, 50),
            request_latency_ms_p95=_percentile(request_latencies, 95),
            request_latency_ms_max=max(request_latencies) if request_latencies else None,
            queue_wait_ms_p50=_percentile(queue_waits, 50),
            queue_wait_ms_p95=_percentile(queue_waits, 95),
            queue_wait_ms_max=max(queue_waits) if queue_waits else None,
            retry_after_values=retry_after_values,
        )
        if cooldown_sec > 0:
            await asyncio.sleep(cooldown_sec)
        return summary

    @staticmethod
    def _classify_profile_payload(payload: dict) -> tuple[str, str | None]:
        if "Ok" in payload:
            return "ok", None
        error = str(payload.get("Err") or "unknown")
        if error == "User not found":
            return "user_not_found", error
        return "logical_err", error

    @staticmethod
    def _classify_posts_payload(payload: list[dict]) -> tuple[str, str | None]:
        return "ok", f"posts={len(payload)}"

    @staticmethod
    def _classify_exception(exc: Exception) -> str:
        message = str(exc).lower()
        if "timed out" in message:
            return "timeout"
        if "429" in message or "rate limit" in message:
            return "rate_limited"
        return type(exc).__name__


async def _load_principals(settings, sample_size: int) -> list[str]:
    chat_client = ChatApiClient(settings)
    try:
        influencers = await chat_client.get_all_influencers()
    finally:
        await chat_client.close()
    principals = [
        item["id"]
        for item in influencers
        if item.get("is_active") == "active" and item.get("id")
    ]
    return principals[:sample_size]


def _load_principals_from_file(path: str, sample_size: int) -> list[str]:
    raw = json.loads(Path(path).read_text())
    if not isinstance(raw, list):
        raise ValueError("principal file must contain a JSON list")
    principals = [str(item) for item in raw if str(item).strip()]
    return principals[:sample_size]


def _best_clean_round(rounds: list[RoundSummary]) -> dict | None:
    clean_rounds = [
        round_summary
        for round_summary in rounds
        if all(
            round_summary.counts.get(key, 0) == 0
            for key in ("timeout", "rate_limited")
        )
        and all(
            not key.startswith("http_") or round_summary.counts.get(key, 0) == 0
            for key in round_summary.counts
        )
    ]
    if not clean_rounds:
        return None
    best = sorted(
        clean_rounds,
        key=lambda item: (
            item.rps,
            item.concurrency,
            -(item.request_latency_ms_p95 or 0),
        ),
        reverse=True,
    )[0]
    return {
        "endpoint": best.endpoint,
        "rps": best.rps,
        "concurrency": best.concurrency,
        "request_latency_ms_p95": best.request_latency_ms_p95,
        "request_count": best.request_count,
    }


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", choices=["profile", "posts", "both"], default="both")
    parser.add_argument("--rps-values", default="3,5,7,10")
    parser.add_argument("--concurrency-values", default="1,3,5,10")
    parser.add_argument("--sample-size", type=int, default=100)
    parser.add_argument("--requests-per-round", type=int, default=100)
    parser.add_argument("--cooldown-sec", type=float, default=10.0)
    parser.add_argument("--timeout-sec", type=float, default=5.0)
    parser.add_argument("--posts-limit", type=int, default=10)
    parser.add_argument("--principal-file", default="")
    parser.add_argument("--output", default="")
    args = parser.parse_args()

    get_settings.cache_clear()
    settings = get_settings()
    if args.principal_file:
        principals = _load_principals_from_file(args.principal_file, args.sample_size)
    else:
        principals = await _load_principals(settings, args.sample_size)
    if not principals:
        raise RuntimeError("No active influencer principals available for probing")

    endpoints = ["profile", "posts"] if args.endpoint == "both" else [args.endpoint]
    rps_values = _parse_csv_floats(args.rps_values)
    concurrency_values = _parse_csv_ints(args.concurrency_values)

    print(
        json.dumps(
            {
                "message": "Starting canister rate-limit probe",
                "endpoint": args.endpoint,
                "sample_size": len(principals),
                "requests_per_round": args.requests_per_round,
                "rps_values": rps_values,
                "concurrency_values": concurrency_values,
                "cooldown_sec": args.cooldown_sec,
                "timeout_sec": args.timeout_sec,
            }
        )
    )

    all_rounds: list[RoundSummary] = []
    for endpoint in endpoints:
        for rps in rps_values:
            for concurrency in concurrency_values:
                probe = CanisterRateProbe(
                    settings=settings,
                    principals=principals,
                    endpoint=endpoint,
                    posts_limit=args.posts_limit,
                    request_count=args.requests_per_round,
                    timeout_sec=args.timeout_sec,
                )
                summary = await probe.run_round(
                    rps=rps,
                    concurrency=concurrency,
                    cooldown_sec=args.cooldown_sec,
                )
                all_rounds.append(summary)
                print(json.dumps(asdict(summary), default=str))

    recommendations = [_best_clean_round([item for item in all_rounds if item.endpoint == endpoint]) for endpoint in endpoints]
    report = {
        "rounds": [asdict(item) for item in all_rounds],
        "recommended_clean_rounds": [item for item in recommendations if item],
    }

    if args.output:
        Path(args.output).write_text(json.dumps(report, indent=2, default=str))

    print(json.dumps({"message": "Probe completed", **report}, default=str))


if __name__ == "__main__":
    asyncio.run(main())
