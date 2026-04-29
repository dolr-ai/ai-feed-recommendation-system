#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

import httpx


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Compare the legacy curated-feed response with the new feed recsys response "
            "for one or more users."
        )
    )
    parser.add_argument("--legacy-base-url", required=True)
    parser.add_argument("--new-base-url", required=True)
    parser.add_argument("--user-id", action="append", required=True)
    parser.add_argument("--count", type=int, default=100)
    parser.add_argument("--rec-type", default="mixed")
    parser.add_argument("--timeout-sec", type=float, default=30.0)
    return parser


def extract_video_map(payload: dict[str, Any]) -> dict[str, dict[str, Any]]:
    videos = payload.get("videos", [])
    return {
        video["video_id"]: video
        for video in videos
        if isinstance(video, dict) and video.get("video_id")
    }


def compare_payloads(user_id: str, legacy_payload: dict[str, Any], new_payload: dict[str, Any]) -> dict[str, Any]:
    legacy_videos = extract_video_map(legacy_payload)
    new_videos = extract_video_map(new_payload)

    legacy_ids = set(legacy_videos)
    new_ids = set(new_videos)
    overlap = legacy_ids & new_ids

    return {
        "user_id": user_id,
        "legacy_count": legacy_payload.get("count", 0),
        "new_count": new_payload.get("count", 0),
        "legacy_unique_videos": len(legacy_ids),
        "new_unique_videos": len(new_ids),
        "overlap_count": len(overlap),
        "overlap_ratio_vs_legacy": round(len(overlap) / len(legacy_ids), 4) if legacy_ids else 0.0,
        "overlap_ratio_vs_new": round(len(overlap) / len(new_ids), 4) if new_ids else 0.0,
        "legacy_missing_in_new": sorted(legacy_ids - new_ids)[:20],
        "new_missing_in_legacy": sorted(new_ids - legacy_ids)[:20],
        "new_ai_influencer_true_count": sum(
            1
            for video in new_videos.values()
            if video.get("from_ai_influencer") is True
        ),
        "new_missing_metadata_count": sum(
            1
            for video in new_videos.values()
            if not video.get("post_id") or not video.get("publisher_user_id")
        ),
    }


def fetch_json(client: httpx.Client, url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = client.get(url, params=params)
    response.raise_for_status()
    return response.json()


def main() -> int:
    args = build_parser().parse_args()
    params = {"count": args.count, "rec_type": args.rec_type}

    with httpx.Client(timeout=args.timeout_sec) as client:
        results = []
        for user_id in args.user_id:
            legacy_url = f"{args.legacy_base_url.rstrip('/')}/v2/recommend-with-metadata/{user_id}"
            new_url = f"{args.new_base_url.rstrip('/')}/api/v1/recommend-with-metadata/{user_id}"
            legacy_payload = fetch_json(client, legacy_url, params)
            new_payload = fetch_json(client, new_url, params)
            results.append(compare_payloads(user_id, legacy_payload, new_payload))

    print(json.dumps(results, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
