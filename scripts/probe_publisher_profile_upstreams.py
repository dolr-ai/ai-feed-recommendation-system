#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.clients.canister_client import CanisterClient
from src.clients.metadata_service_client import MetadataServiceClient
from src.core.settings import get_settings


SAMPLE_PUBLISHER_USER_IDS = [
    "txfih-vet4g-lvtgo-6nle2-xmlte-5pmow-bjeei-hpwzs-bidi7-qmaoy-mqe",
    "jyfii-2znct-fkemi-hsyv2-gpbi7-aatoo-pet7i-anbyg-pyazx-2ucf5-6ae",
    "ajqky-usehk-cuk3x-7euws-wc7or-nexuj-xigsk-cbinh-kxond-pwmm3-tae",
    "dinty-gwiqo-nplm5-ldujw-6yzco-bmjmb-7ytgh-mrpc7-f5p72-uleee-gae",
    "jovus-ytdu6-ut6i2-4rdj7-xblwf-apsmq-dk2zu-lsgxl-og24d-nu472-aqe",
    "jtkog-ia6ya-gpi7e-berg7-5nshy-gilhg-drmcn-oi4tp-ajf7x-2vn5v-pqe",
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Probe publisher-profile upstreams used by feed recsys enrichment. "
            "By default this uses the publisher_user_id values from the reported sample response."
        )
    )
    parser.add_argument(
        "--publisher-user-id",
        action="append",
        default=[],
        help="Publisher principal to probe. Can be passed multiple times.",
    )
    parser.add_argument(
        "--response-json",
        type=Path,
        help="Optional feed response JSON file. Publisher IDs are extracted from videos[].publisher_user_id.",
    )
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--sleep-sec", type=float, default=0.0)
    parser.add_argument(
        "--metadata-timeout-sec",
        type=float,
        help="Override metadata service timeout. Defaults to FEED_RECSYS_REQUEST_METADATA_TIMEOUT_SEC.",
    )
    parser.add_argument(
        "--canister-timeout-sec",
        type=float,
        help="Override canister HTTP timeout. Defaults to FEED_RECSYS_REQUEST_IC_PROFILE_TIMEOUT_SEC.",
    )
    parser.add_argument(
        "--canister-retries",
        type=int,
        help="Override canister query retries. Defaults to FEED_RECSYS_REQUEST_IC_PROFILE_RETRIES.",
    )
    parser.add_argument("--skip-metadata", action="store_true")
    parser.add_argument("--skip-canister", action="store_true")
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit machine-readable JSON only.",
    )
    return parser


def extract_publishers_from_response(path: Path) -> list[str]:
    payload = json.loads(path.read_text())
    videos = payload.get("videos") if isinstance(payload, dict) else None
    if not isinstance(videos, list):
        raise ValueError(f"{path} does not look like a feed response JSON payload")
    return [
        str(video.get("publisher_user_id") or "").strip()
        for video in videos
        if isinstance(video, dict) and video.get("publisher_user_id")
    ]


def dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(str(value or "").strip() for value in values if value))


async def fetch_metadata_usernames(
    client: MetadataServiceClient,
    publisher_user_ids: list[str],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    payload = await client._post_json(  # noqa: SLF001 - diagnostic script, intentionally inspects raw row shape.
        "metadata-bulk",
        {"users": publisher_user_ids},
    )
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    if not isinstance(payload, dict):
        raise RuntimeError(f"metadata payload type was {type(payload).__name__}")

    usernames: dict[str, str] = {}
    row_keys: dict[str, list[str]] = {}
    row_count = 0
    for publisher_id in publisher_user_ids:
        row = payload.get(publisher_id)
        if not isinstance(row, dict):
            continue
        row_count += 1
        row_keys[publisher_id] = sorted(str(key) for key in row)
        usernames[publisher_id] = str(row.get("user_name") or "").strip()

    return {
        "elapsed_ms": elapsed_ms,
        "row_count": row_count,
        "usernames": usernames,
        "row_keys": row_keys,
    }


async def fetch_canister_profiles(
    client: CanisterClient,
    publisher_user_ids: list[str],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    profiles = await client.get_users_profile_details(publisher_user_ids)
    elapsed_ms = round((time.perf_counter() - started_at) * 1000, 2)
    if not isinstance(profiles, dict):
        raise RuntimeError(f"canister profile payload type was {type(profiles).__name__}")
    return {
        "elapsed_ms": elapsed_ms,
        "profiles": profiles,
    }


def exception_payload(exc: Exception) -> dict[str, str]:
    return {
        "type": type(exc).__name__,
        "message": str(exc),
    }


def print_attempt(attempt: dict[str, Any]) -> None:
    prefix = f"attempt={attempt['attempt']}"
    metadata = attempt.get("metadata")
    if metadata:
        if metadata["ok"]:
            print(
                f"{prefix} metadata ok elapsed_ms={metadata['elapsed_ms']} "
                f"rows={metadata['row_count']} usernames_nonblank={metadata['nonblank_usernames']}"
            )
        else:
            error = metadata["error"]
            print(
                f"{prefix} metadata failed elapsed_ms={metadata['elapsed_ms']} "
                f"error_type={error['type']} error={error['message']!r}"
            )

    canister = attempt.get("canister")
    if canister:
        if canister["ok"]:
            print(
                f"{prefix} canister ok elapsed_ms={canister['elapsed_ms']} "
                f"rows={canister['row_count']} profile_images_nonblank={canister['nonblank_profile_images']}"
            )
        else:
            error = canister["error"]
            print(
                f"{prefix} canister failed elapsed_ms={canister['elapsed_ms']} "
                f"error_type={error['type']} error={error['message']!r}"
            )


def build_summary(
    publisher_user_ids: list[str],
    attempts: list[dict[str, Any]],
) -> dict[str, Any]:
    per_publisher: dict[str, dict[str, Any]] = {
        publisher_id: {
            "metadata_rows": 0,
            "username_nonblank": 0,
            "last_username": "",
            "metadata_row_keys": [],
            "canister_rows": 0,
            "profile_image_nonblank": 0,
            "last_profile_image_url": "",
            "is_pro_true": 0,
        }
        for publisher_id in publisher_user_ids
    }
    source_counts: dict[str, defaultdict[str, int]] = {
        "metadata": defaultdict(int),
        "canister": defaultdict(int),
    }
    source_latencies: dict[str, list[float]] = {"metadata": [], "canister": []}
    source_errors: dict[str, list[dict[str, str]]] = {"metadata": [], "canister": []}

    for attempt in attempts:
        metadata = attempt.get("metadata")
        if metadata:
            source_counts["metadata"]["attempts"] += 1
            source_latencies["metadata"].append(metadata["elapsed_ms"])
            if metadata["ok"]:
                source_counts["metadata"]["successes"] += 1
                for publisher_id, username in metadata["usernames"].items():
                    per_publisher[publisher_id]["metadata_rows"] += 1
                    per_publisher[publisher_id]["metadata_row_keys"] = metadata["row_keys"].get(
                        publisher_id,
                        [],
                    )
                    if username:
                        per_publisher[publisher_id]["username_nonblank"] += 1
                        per_publisher[publisher_id]["last_username"] = username
            else:
                source_counts["metadata"]["failures"] += 1
                source_errors["metadata"].append(metadata["error"])

        canister = attempt.get("canister")
        if canister:
            source_counts["canister"]["attempts"] += 1
            source_latencies["canister"].append(canister["elapsed_ms"])
            if canister["ok"]:
                source_counts["canister"]["successes"] += 1
                for publisher_id, profile in canister["profiles"].items():
                    per_publisher[publisher_id]["canister_rows"] += 1
                    profile_image_url = str(profile.get("profile_image_url") or "")
                    if profile_image_url:
                        per_publisher[publisher_id]["profile_image_nonblank"] += 1
                        per_publisher[publisher_id]["last_profile_image_url"] = profile_image_url
                    if bool(profile.get("is_pro_user") or False):
                        per_publisher[publisher_id]["is_pro_true"] += 1
            else:
                source_counts["canister"]["failures"] += 1
                source_errors["canister"].append(canister["error"])

    def latency_summary(values: list[float]) -> dict[str, float | None]:
        if not values:
            return {"min_ms": None, "avg_ms": None, "max_ms": None}
        return {
            "min_ms": round(min(values), 2),
            "avg_ms": round(sum(values) / len(values), 2),
            "max_ms": round(max(values), 2),
        }

    return {
        "publisher_count": len(publisher_user_ids),
        "publishers": publisher_user_ids,
        "sources": {
            source: {
                **dict(source_counts[source]),
                **latency_summary(source_latencies[source]),
                "errors": source_errors[source],
            }
            for source in ("metadata", "canister")
            if source_counts[source]["attempts"] > 0
        },
        "per_publisher": per_publisher,
    }


async def run() -> int:
    args = build_parser().parse_args()
    publisher_user_ids = list(args.publisher_user_id)
    if args.response_json:
        publisher_user_ids.extend(extract_publishers_from_response(args.response_json))
    if not publisher_user_ids:
        publisher_user_ids = SAMPLE_PUBLISHER_USER_IDS
    publisher_user_ids = dedupe(publisher_user_ids)

    settings = get_settings()
    metadata_timeout = (
        args.metadata_timeout_sec
        if args.metadata_timeout_sec is not None
        else settings.feed_recsys_request_metadata_timeout_sec
    )
    canister_timeout = (
        args.canister_timeout_sec
        if args.canister_timeout_sec is not None
        else settings.feed_recsys_request_ic_profile_timeout_sec
    )
    canister_retries = (
        args.canister_retries
        if args.canister_retries is not None
        else settings.feed_recsys_request_ic_profile_retries
    )

    metadata_client = MetadataServiceClient(settings, timeout_sec=metadata_timeout)
    canister_client = CanisterClient(
        settings,
        http_timeout_sec=canister_timeout,
        query_retries=canister_retries,
    )
    attempts: list[dict[str, Any]] = []

    if not args.json:
        print(
            "probe_config "
            f"publisher_count={len(publisher_user_ids)} repeats={args.repeats} "
            f"metadata_timeout_sec={metadata_timeout} "
            f"canister_timeout_sec={canister_timeout} canister_retries={canister_retries}"
        )

    try:
        for attempt_num in range(1, max(1, args.repeats) + 1):
            attempt: dict[str, Any] = {"attempt": attempt_num}
            if not args.skip_metadata:
                started_at = time.perf_counter()
                try:
                    metadata = await fetch_metadata_usernames(
                        metadata_client,
                        publisher_user_ids,
                    )
                    usernames = metadata["usernames"]
                    attempt["metadata"] = {
                        "ok": True,
                        "elapsed_ms": metadata["elapsed_ms"],
                        "row_count": metadata["row_count"],
                        "nonblank_usernames": sum(1 for value in usernames.values() if value),
                        "usernames": usernames,
                        "row_keys": metadata["row_keys"],
                    }
                except Exception as exc:
                    attempt["metadata"] = {
                        "ok": False,
                        "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
                        "error": exception_payload(exc),
                    }

            if not args.skip_canister:
                started_at = time.perf_counter()
                try:
                    canister = await fetch_canister_profiles(
                        canister_client,
                        publisher_user_ids,
                    )
                    profiles = canister["profiles"]
                    attempt["canister"] = {
                        "ok": True,
                        "elapsed_ms": canister["elapsed_ms"],
                        "row_count": len(profiles),
                        "nonblank_profile_images": sum(
                            1
                            for profile in profiles.values()
                            if profile.get("profile_image_url")
                        ),
                        "profiles": profiles,
                    }
                except Exception as exc:
                    attempt["canister"] = {
                        "ok": False,
                        "elapsed_ms": round((time.perf_counter() - started_at) * 1000, 2),
                        "error": exception_payload(exc),
                    }

            attempts.append(attempt)
            if not args.json:
                print_attempt(attempt)
            if args.sleep_sec > 0 and attempt_num < args.repeats:
                await asyncio.sleep(args.sleep_sec)
    finally:
        await metadata_client.close()
        await canister_client.close()

    summary = build_summary(publisher_user_ids, attempts)
    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print("summary")
        print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(run()))
