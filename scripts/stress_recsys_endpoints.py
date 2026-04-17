import argparse
import asyncio
import json
import math
import statistics
import sys
import time
from collections import Counter
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import aiohttp

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


DEFAULT_ENDPOINTS = {
    "influencer_feed": "https://recsys.ansuman.yral.com/api/v1/influencer-feed?offset=0&limit=50&with_metadata=false",
    "metrics": "https://recsys.ansuman.yral.com/metrics",
    "health": "https://recsys.ansuman.yral.com/health",
}


def _parse_csv_ints(value: str) -> list[int]:
    return [int(item.strip()) for item in value.split(",") if item.strip()]


def _parse_csv_strings(value: str) -> list[str]:
    return [item.strip() for item in value.split(",") if item.strip()]


def _percentile(values: list[float], pct: float) -> float | None:
    if not values:
        return None
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil((pct / 100.0) * len(ordered)) - 1))
    return ordered[index]


@dataclass
class RequestSample:
    latency_ms: float
    status_code: int | None
    response_bytes: int
    error: str | None = None


@dataclass
class StageSummary:
    endpoint: str
    url: str
    concurrency: int
    target_duration_sec: float
    wall_time_sec: float
    total_requests: int
    ok_requests: int
    non_2xx_requests: int
    exception_requests: int
    error_rate: float
    rps: float
    success_rps: float
    avg_latency_ms: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    p99_latency_ms: float | None
    max_latency_ms: float | None
    avg_response_bytes: float | None
    status_counts: dict[str, int]
    error_counts: dict[str, int]


async def _issue_request(session: aiohttp.ClientSession, url: str) -> RequestSample:
    started = time.perf_counter()
    try:
        async with session.get(url) as response:
            payload = await response.read()
            latency_ms = (time.perf_counter() - started) * 1000.0
            return RequestSample(
                latency_ms=latency_ms,
                status_code=response.status,
                response_bytes=len(payload),
            )
    except asyncio.TimeoutError:
        latency_ms = (time.perf_counter() - started) * 1000.0
        return RequestSample(
            latency_ms=latency_ms,
            status_code=None,
            response_bytes=0,
            error="timeout",
        )
    except aiohttp.ClientError as exc:
        latency_ms = (time.perf_counter() - started) * 1000.0
        return RequestSample(
            latency_ms=latency_ms,
            status_code=None,
            response_bytes=0,
            error=exc.__class__.__name__,
        )
    except Exception as exc:  # pragma: no cover - defensive
        latency_ms = (time.perf_counter() - started) * 1000.0
        return RequestSample(
            latency_ms=latency_ms,
            status_code=None,
            response_bytes=0,
            error=exc.__class__.__name__,
        )


async def _worker(
    session: aiohttp.ClientSession,
    url: str,
    stop_at: float,
) -> list[RequestSample]:
    results: list[RequestSample] = []
    while time.perf_counter() < stop_at:
        results.append(await _issue_request(session, url))
    return results


def _summarize_stage(
    endpoint: str,
    url: str,
    concurrency: int,
    target_duration_sec: float,
    wall_time_sec: float,
    results: list[RequestSample],
) -> StageSummary:
    total_requests = len(results)
    ok_requests = sum(
        1 for sample in results if sample.status_code is not None and 200 <= sample.status_code < 300
    )
    non_2xx_requests = sum(
        1 for sample in results if sample.status_code is not None and not (200 <= sample.status_code < 300)
    )
    exception_requests = sum(1 for sample in results if sample.error is not None)
    error_rate = (
        (non_2xx_requests + exception_requests) / total_requests if total_requests else 0.0
    )
    latencies = [sample.latency_ms for sample in results]
    response_sizes = [sample.response_bytes for sample in results if sample.response_bytes > 0]
    status_counts = Counter(str(sample.status_code) for sample in results if sample.status_code is not None)
    error_counts = Counter(sample.error for sample in results if sample.error is not None)
    rps = total_requests / wall_time_sec if wall_time_sec > 0 else 0.0
    success_rps = ok_requests / wall_time_sec if wall_time_sec > 0 else 0.0
    avg_latency_ms = statistics.fmean(latencies) if latencies else None
    avg_response_bytes = statistics.fmean(response_sizes) if response_sizes else None

    return StageSummary(
        endpoint=endpoint,
        url=url,
        concurrency=concurrency,
        target_duration_sec=target_duration_sec,
        wall_time_sec=wall_time_sec,
        total_requests=total_requests,
        ok_requests=ok_requests,
        non_2xx_requests=non_2xx_requests,
        exception_requests=exception_requests,
        error_rate=error_rate,
        rps=rps,
        success_rps=success_rps,
        avg_latency_ms=avg_latency_ms,
        p50_latency_ms=_percentile(latencies, 50),
        p95_latency_ms=_percentile(latencies, 95),
        p99_latency_ms=_percentile(latencies, 99),
        max_latency_ms=max(latencies) if latencies else None,
        avg_response_bytes=avg_response_bytes,
        status_counts=dict(status_counts),
        error_counts=dict(error_counts),
    )


async def run_stage(
    endpoint: str,
    url: str,
    concurrency: int,
    duration_sec: float,
    timeout_sec: float,
) -> StageSummary:
    timeout = aiohttp.ClientTimeout(total=timeout_sec)
    connector = aiohttp.TCPConnector(limit=0, ttl_dns_cache=300)
    headers = {"accept": "application/json"}
    stage_started = time.perf_counter()
    stop_at = stage_started + duration_sec

    async with aiohttp.ClientSession(timeout=timeout, connector=connector, headers=headers) as session:
        tasks = [
            asyncio.create_task(_worker(session=session, url=url, stop_at=stop_at))
            for _ in range(concurrency)
        ]
        task_results = await asyncio.gather(*tasks)

    wall_time_sec = time.perf_counter() - stage_started
    flat_results = [sample for batch in task_results for sample in batch]
    return _summarize_stage(
        endpoint=endpoint,
        url=url,
        concurrency=concurrency,
        target_duration_sec=duration_sec,
        wall_time_sec=wall_time_sec,
        results=flat_results,
    )


def _format_stage(summary: StageSummary) -> str:
    return (
        f"[{summary.endpoint}] c={summary.concurrency} total={summary.total_requests} "
        f"ok={summary.ok_requests} non2xx={summary.non_2xx_requests} exc={summary.exception_requests} "
        f"err={summary.error_rate * 100:.2f}% rps={summary.rps:.2f} "
        f"p50={summary.p50_latency_ms:.1f}ms p95={summary.p95_latency_ms:.1f}ms "
        f"p99={summary.p99_latency_ms:.1f}ms max={summary.max_latency_ms:.1f}ms "
        f"statuses={summary.status_counts or {}} errors={summary.error_counts or {}}"
    )


def _analyze_endpoint(
    endpoint: str,
    stages: list[StageSummary],
    error_rate_threshold: float,
    latency_multiplier: float,
) -> dict[str, Any]:
    baseline = stages[0] if stages else None
    baseline_p95 = baseline.p95_latency_ms if baseline else None
    stable_stage = None
    first_error_stage = None
    first_latency_cliff_stage = None

    for stage in stages:
        if first_error_stage is None and stage.error_rate > error_rate_threshold:
            first_error_stage = stage
        if (
            baseline_p95 is not None
            and first_latency_cliff_stage is None
            and stage.p95_latency_ms is not None
            and stage.p95_latency_ms > baseline_p95 * latency_multiplier
        ):
            first_latency_cliff_stage = stage
        is_stable = stage.error_rate <= error_rate_threshold
        if baseline_p95 is not None and stage.p95_latency_ms is not None:
            is_stable = is_stable and stage.p95_latency_ms <= baseline_p95 * latency_multiplier
        if is_stable:
            stable_stage = stage

    return {
        "endpoint": endpoint,
        "baseline_p95_latency_ms": baseline_p95,
        "stable_through_concurrency": stable_stage.concurrency if stable_stage else None,
        "first_error_concurrency": first_error_stage.concurrency if first_error_stage else None,
        "first_latency_cliff_concurrency": (
            first_latency_cliff_stage.concurrency if first_latency_cliff_stage else None
        ),
    }


async def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run bounded stress tests against recsys endpoints and report latency/error behavior."
    )
    parser.add_argument(
        "--concurrency",
        default="1,2,5,10,20,40,80",
        help="Comma-separated concurrency stages to run for every endpoint.",
    )
    parser.add_argument(
        "--duration-sec",
        type=float,
        default=10.0,
        help="Target duration for each stage.",
    )
    parser.add_argument(
        "--timeout-sec",
        type=float,
        default=15.0,
        help="Per-request client timeout.",
    )
    parser.add_argument(
        "--error-rate-threshold",
        type=float,
        default=0.01,
        help="Maximum error rate considered stable when estimating concurrency headroom.",
    )
    parser.add_argument(
        "--latency-multiplier",
        type=float,
        default=2.5,
        help="p95 growth over baseline treated as a latency cliff when estimating concurrency headroom.",
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional JSON output path. Defaults to /tmp with a timestamp.",
    )
    parser.add_argument(
        "--endpoints",
        default=",".join(DEFAULT_ENDPOINTS.keys()),
        help="Comma-separated endpoint keys to test. Available: "
        + ", ".join(DEFAULT_ENDPOINTS.keys()),
    )
    args = parser.parse_args()

    concurrency_levels = _parse_csv_ints(args.concurrency)
    selected_endpoints = _parse_csv_strings(args.endpoints)
    unknown_endpoints = [name for name in selected_endpoints if name not in DEFAULT_ENDPOINTS]
    if unknown_endpoints:
        raise SystemExit(f"Unknown endpoints: {', '.join(unknown_endpoints)}")
    timestamp = time.strftime("%Y%m%d-%H%M%S")
    output_path = (
        Path(args.output)
        if args.output
        else Path(f"/tmp/recsys_stress_results_{timestamp}.json")
    )

    endpoint_results: dict[str, list[StageSummary]] = {}
    for endpoint in selected_endpoints:
        url = DEFAULT_ENDPOINTS[endpoint]
        print(f"\n== {endpoint} == {url}", flush=True)
        endpoint_results[endpoint] = []
        for concurrency in concurrency_levels:
            summary = await run_stage(
                endpoint=endpoint,
                url=url,
                concurrency=concurrency,
                duration_sec=args.duration_sec,
                timeout_sec=args.timeout_sec,
            )
            endpoint_results[endpoint].append(summary)
            print(_format_stage(summary), flush=True)

    analysis = {
        endpoint: _analyze_endpoint(
            endpoint=endpoint,
            stages=stages,
            error_rate_threshold=args.error_rate_threshold,
            latency_multiplier=args.latency_multiplier,
        )
        for endpoint, stages in endpoint_results.items()
    }
    payload = {
        "config": {
            "concurrency": concurrency_levels,
            "duration_sec": args.duration_sec,
            "timeout_sec": args.timeout_sec,
            "error_rate_threshold": args.error_rate_threshold,
            "latency_multiplier": args.latency_multiplier,
        },
        "analysis": analysis,
        "results": {
            endpoint: [asdict(stage) for stage in stages]
            for endpoint, stages in endpoint_results.items()
        },
    }
    output_path.write_text(json.dumps(payload, indent=2))
    print(f"\nSaved JSON results to {output_path}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
