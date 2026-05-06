#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import math
import time
from collections import Counter
from pathlib import Path
from typing import Any

import httpx


DEFAULT_USER_ID = "jjhwf-vqja5-n5ds4-d5pnp-i4zva-xgr3n-qspvp-g4k4e-yfabm-qlm7q-jae"
REQUIRED_VIDEO_FIELDS = {
    "video_id",
    "canister_id",
    "post_id",
    "publisher_user_id",
    "num_views_loggedin",
    "num_views_all",
    "from_ai_influencer",
    "is_following",
    "is_pro_user",
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Benchmark the feed recsys recommend-with-metadata endpoint and report "
            "latency percentiles plus concurrency behavior."
        )
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--user-id", default=DEFAULT_USER_ID)
    parser.add_argument("--count", type=int, default=10)
    parser.add_argument("--rec-type", default="mixed")
    parser.add_argument(
        "--requests",
        type=int,
        default=100,
        help="Requests to send per concurrency phase.",
    )
    parser.add_argument(
        "--concurrency",
        action="append",
        type=int,
        help=(
            "Concurrent workers for a phase. Can be repeated. "
            "Default phases are 1, 5, and 10."
        ),
    )
    parser.add_argument("--warmup-requests", type=int, default=5)
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    parser.add_argument("--output-json", type=Path)
    parser.add_argument("--json", action="store_true", help="Print JSON only.")
    return parser


def endpoint_url(base_url: str, user_id: str) -> str:
    return (
        f"{base_url.rstrip('/')}/api/v1/recommend-with-metadata/"
        f"{user_id}"
    )


def percentile(values: list[float], percentile_value: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil((percentile_value / 100.0) * len(ordered)) - 1)
    return round(ordered[index], 2)


def latency_summary(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {
            "min_ms": None,
            "avg_ms": None,
            "p50_ms": None,
            "p90_ms": None,
            "p95_ms": None,
            "max_ms": None,
        }
    return {
        "min_ms": round(min(values), 2),
        "avg_ms": round(sum(values) / len(values), 2),
        "p50_ms": percentile(values, 50),
        "p90_ms": percentile(values, 90),
        "p95_ms": percentile(values, 95),
        "max_ms": round(max(values), 2),
    }


def validate_payload(payload: Any, expected_user_id: str) -> tuple[list[str], dict[str, int]]:
    errors: list[str] = []
    counters = {
        "videos": 0,
        "username_present": 0,
        "profile_image_url_present": 0,
        "profile_image_url_nonblank": 0,
    }

    if not isinstance(payload, dict):
        return ["payload_not_object"], counters
    if payload.get("user_id") != expected_user_id:
        errors.append("user_id_mismatch")

    videos = payload.get("videos")
    if not isinstance(videos, list):
        return errors + ["videos_not_list"], counters

    counters["videos"] = len(videos)
    if payload.get("count") != len(videos):
        errors.append("count_mismatch")

    for video in videos:
        if not isinstance(video, dict):
            errors.append("video_not_object")
            continue
        missing = REQUIRED_VIDEO_FIELDS - set(video)
        if missing:
            errors.append("missing_required_video_fields")
        if "username" in video:
            counters["username_present"] += 1
        if "profile_image_url" in video:
            counters["profile_image_url_present"] += 1
            if video.get("profile_image_url"):
                counters["profile_image_url_nonblank"] += 1

    return errors, counters


async def send_one(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    expected_user_id: str,
) -> dict[str, Any]:
    started_at = time.perf_counter()
    try:
        response = await client.get(url, params=params)
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        result: dict[str, Any] = {
            "ok": 200 <= response.status_code < 300,
            "status_code": response.status_code,
            "elapsed_ms": elapsed_ms,
            "response_bytes": len(response.content),
        }
        try:
            payload = response.json()
        except ValueError:
            result["validation_errors"] = ["response_not_json"]
            return result

        validation_errors, counters = validate_payload(payload, expected_user_id)
        result["validation_errors"] = validation_errors
        result.update(counters)
        return result
    except Exception as exc:
        elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
        return {
            "ok": False,
            "status_code": None,
            "elapsed_ms": elapsed_ms,
            "response_bytes": 0,
            "error_type": type(exc).__name__,
            "error": str(exc),
            "validation_errors": [],
            "videos": 0,
            "username_present": 0,
            "profile_image_url_present": 0,
            "profile_image_url_nonblank": 0,
        }


async def run_warmup(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    expected_user_id: str,
    warmup_requests: int,
) -> None:
    for _ in range(max(0, warmup_requests)):
        await send_one(client, url, params, expected_user_id)


async def run_phase(
    client: httpx.AsyncClient,
    url: str,
    params: dict[str, Any],
    expected_user_id: str,
    request_count: int,
    concurrency: int,
) -> dict[str, Any]:
    queue: asyncio.Queue[int] = asyncio.Queue()
    for index in range(request_count):
        queue.put_nowait(index)

    results: list[dict[str, Any]] = []

    async def worker() -> None:
        while True:
            try:
                queue.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                results.append(await send_one(client, url, params, expected_user_id))
            finally:
                queue.task_done()

    started_at = time.perf_counter()
    workers = [
        asyncio.create_task(worker())
        for _ in range(max(1, min(concurrency, request_count)))
    ]
    await asyncio.gather(*workers)
    duration_sec = time.perf_counter() - started_at

    return summarize_phase(results, request_count, concurrency, duration_sec)


def summarize_phase(
    results: list[dict[str, Any]],
    request_count: int,
    concurrency: int,
    duration_sec: float,
) -> dict[str, Any]:
    latencies = [result["elapsed_ms"] for result in results]
    success_latencies = [
        result["elapsed_ms"]
        for result in results
        if result["ok"] and not result.get("validation_errors")
    ]
    status_counts = Counter(
        str(result["status_code"]) if result["status_code"] is not None else "EXCEPTION"
        for result in results
    )
    error_counts = Counter(
        result.get("error_type")
        for result in results
        if result.get("error_type")
    )
    validation_error_counts = Counter(
        error
        for result in results
        for error in result.get("validation_errors", [])
    )

    successful_results = [
        result
        for result in results
        if result["ok"] and not result.get("validation_errors")
    ]
    total_videos = sum(result.get("videos", 0) for result in successful_results)
    response_bytes = [result["response_bytes"] for result in results]

    return {
        "concurrency": concurrency,
        "requests": request_count,
        "completed": len(results),
        "successes": len(successful_results),
        "failures": request_count - len(successful_results),
        "success_rate": round(len(successful_results) / request_count, 4)
        if request_count
        else 0.0,
        "duration_sec": round(duration_sec, 3),
        "throughput_rps": round(len(results) / duration_sec, 2)
        if duration_sec > 0
        else None,
        "latency_all": latency_summary(latencies),
        "latency_success": latency_summary(success_latencies),
        "status_counts": dict(status_counts),
        "error_counts": dict(error_counts),
        "validation_error_counts": dict(validation_error_counts),
        "response_bytes": latency_summary(response_bytes),
        "avg_videos_per_success": round(total_videos / len(successful_results), 2)
        if successful_results
        else 0.0,
        "username_present_per_success_response_avg": round(
            sum(result.get("username_present", 0) for result in successful_results)
            / len(successful_results),
            2,
        )
        if successful_results
        else 0.0,
        "profile_image_present_per_success_response_avg": round(
            sum(result.get("profile_image_url_present", 0) for result in successful_results)
            / len(successful_results),
            2,
        )
        if successful_results
        else 0.0,
        "profile_image_nonblank_per_success_response_avg": round(
            sum(result.get("profile_image_url_nonblank", 0) for result in successful_results)
            / len(successful_results),
            2,
        )
        if successful_results
        else 0.0,
    }


def print_phase(phase: dict[str, Any]) -> None:
    all_latency = phase["latency_all"]
    success_latency = phase["latency_success"]
    print(
        "phase "
        f"concurrency={phase['concurrency']} requests={phase['requests']} "
        f"successes={phase['successes']} failures={phase['failures']} "
        f"success_rate={phase['success_rate']} throughput_rps={phase['throughput_rps']}"
    )
    print(
        "latency_all_ms "
        f"min={all_latency['min_ms']} avg={all_latency['avg_ms']} "
        f"p50={all_latency['p50_ms']} p90={all_latency['p90_ms']} "
        f"p95={all_latency['p95_ms']} max={all_latency['max_ms']}"
    )
    print(
        "latency_success_ms "
        f"min={success_latency['min_ms']} avg={success_latency['avg_ms']} "
        f"p50={success_latency['p50_ms']} p90={success_latency['p90_ms']} "
        f"p95={success_latency['p95_ms']} max={success_latency['max_ms']}"
    )
    print(
        "response "
        f"status_counts={phase['status_counts']} "
        f"errors={phase['error_counts']} "
        f"validation_errors={phase['validation_error_counts']} "
        f"avg_videos={phase['avg_videos_per_success']} "
        f"avg_username_fields={phase['username_present_per_success_response_avg']} "
        f"avg_profile_image_fields={phase['profile_image_present_per_success_response_avg']}"
    )


async def run() -> int:
    args = build_parser().parse_args()
    concurrency_levels = args.concurrency or [1, 5, 10]
    request_count = max(1, args.requests)
    url = endpoint_url(args.base_url, args.user_id)
    params = {"count": args.count, "rec_type": args.rec_type}

    timeout = httpx.Timeout(args.timeout_sec)
    async with httpx.AsyncClient(timeout=timeout, headers={"accept": "application/json"}) as client:
        await run_warmup(client, url, params, args.user_id, args.warmup_requests)
        phases = []
        for concurrency in concurrency_levels:
            phases.append(
                await run_phase(
                    client,
                    url,
                    params,
                    args.user_id,
                    request_count,
                    max(1, concurrency),
                )
            )

    report = {
        "url": url,
        "params": params,
        "warmup_requests": max(0, args.warmup_requests),
        "timeout_sec": args.timeout_sec,
        "phases": phases,
    }

    if args.output_json:
        args.output_json.write_text(json.dumps(report, indent=2) + "\n")
    if args.json:
        print(json.dumps(report, indent=2))
    else:
        print(f"benchmark url={url} params={params}")
        for phase in phases:
            print_phase(phase)
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
