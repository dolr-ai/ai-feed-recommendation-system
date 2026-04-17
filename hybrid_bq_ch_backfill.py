#!/usr/bin/env python3
"""
Hybrid backfill for the yral ClickHouse cluster.

Why this exists:
- Phase A should not depend on Cloudflare/orange-cloud ingress.
- We want an immutable local snapshot first, then a resumable local load.
- The earlier one-shot migration script is useful, but it does not persist
  progress and it assumes the old single-node ClickHouse target.

Workflow:
1. snapshot:
   - reads BigQuery at a single snapshot timestamp
   - writes chunked Parquet files to a local state directory
   - records raw/compare row counts and chunk metadata in a manifest

2. load:
   - loads those Parquet chunks into local ClickHouse
   - truncates each target table exactly once
   - records completed chunks in the manifest
   - can resume from the last successful chunk

3. verify:
   - compares ClickHouse row counts to the snapshot compare counts
   - uses FINAL for ReplacingMergeTree-backed tables

This script intentionally targets local/native ClickHouse by default.
That keeps the cluster bootstrap independent from HAProxy/Cloudflare decisions.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import pathlib
import re
import sys
import tempfile
from dataclasses import dataclass, field
from typing import Any, Iterable

import pyarrow as pa
import pyarrow.parquet as pq
from clickhouse_driver import Client
from google.cloud import bigquery
from google.oauth2 import service_account


BQ_PROJECT = "hot-or-not-feed-intelligence"
BQ_DATASET = "yral_ds"

CH_HOST = os.getenv("CH_HOST", "127.0.0.1")
CH_PORT = int(os.getenv("CH_PORT", "9000"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "")
CH_DATABASE = os.getenv("CH_DATABASE", "yral")
CH_SECURE = os.getenv("CH_SECURE", "false").strip().lower() in {"1", "true", "yes", "on"}
CH_VERIFY = os.getenv("CH_VERIFY", "false").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_SNAPSHOT_DIR = pathlib.Path(
    os.getenv("BACKFILL_STATE_DIR", pathlib.Path(__file__).parent / ".clickhouse_backfill")
)
DEFAULT_CHUNK_ROWS = int(os.getenv("BACKFILL_CHUNK_ROWS", "50000"))
DEFAULT_LOAD_BATCH_ROWS = int(os.getenv("BACKFILL_LOAD_BATCH_ROWS", "10000"))


@dataclass(frozen=True)
class SourceCandidate:
    source: str
    transform: str = "identity"


@dataclass(frozen=True)
class TableSpec:
    bq_table: str
    ch_table: str
    key_columns: tuple[str, ...]
    target_columns: tuple[str, ...]
    watermark_column: str | None = None
    include_updated_at: bool = True
    source_candidates: dict[str, tuple[SourceCandidate, ...]] = field(default_factory=dict)


TABLES: tuple[TableSpec, ...] = (
    TableSpec(
        bq_table="excluded_videos",
        ch_table="excluded_videos",
        key_columns=("video_id",),
        target_columns=("video_id", "excluded_at", "exclusion_reason"),
        watermark_column="excluded_at",
        source_candidates={},
    ),
    TableSpec(
        bq_table="global_popular_videos_l7d",
        ch_table="global_popular_videos_l7d",
        key_columns=("video_id",),
        target_columns=(
            "video_id",
            "normalized_like_perc_p",
            "normalized_watch_perc_p",
            "global_popularity_score",
            "is_nsfw",
            "nsfw_ec",
            "nsfw_gore",
            "nsfw_probability",
            "upload_type",
            "is_bot_uploaded",
            "user_uploaded_ai_content",
        ),
        watermark_column=None,
        include_updated_at=False,
        source_candidates={
            "is_nsfw": (
                SourceCandidate("is_nsfw"),
                SourceCandidate("isNsfw"),
            ),
        },
    ),
    TableSpec(
        bq_table="ai_ugc",
        ch_table="ai_ugc",
        key_columns=("video_id",),
        target_columns=(
            "video_id",
            "publisher_user_id",
            "user_id",
            "ai_canister_id",
            "event_timestamp_str",
            "publish_time",
            "received_timestamp",
            "event_json_data",
            "event_type",
            "type_ext",
            "upload_timestamp",
            "post_id",
            "upload_canister_id",
            "country",
            "display_name",
        ),
        watermark_column="upload_timestamp",
        source_candidates={
            "publisher_user_id": (
                SourceCandidate("publisher_user_id"),
                SourceCandidate("publisherId"),
            ),
            "event_json_data": (
                SourceCandidate("event_json_data", transform="json"),
                SourceCandidate("eventJsonData", transform="json"),
            ),
        },
    ),
    TableSpec(
        bq_table="follower_graph",
        ch_table="follower_graph",
        key_columns=("follower_id", "following_id"),
        target_columns=("follower_id", "following_id", "active", "last_updated_timestamp"),
        watermark_column="last_updated_timestamp",
        source_candidates={
            "follower_id": (
                SourceCandidate("follower_id"),
                SourceCandidate("followerId"),
            ),
            "following_id": (
                SourceCandidate("following_id"),
                SourceCandidate("followingId"),
            ),
        },
    ),
    TableSpec(
        bq_table="ugc_content_approval",
        ch_table="ugc_content_approval",
        key_columns=("video_id",),
        target_columns=("video_id", "post_id", "canister_id", "user_id", "is_approved", "created_at"),
        watermark_column="created_at",
        source_candidates={
            "is_approved": (
                SourceCandidate("is_approved"),
                SourceCandidate("isApproved"),
            ),
            "user_id": (
                SourceCandidate("user_id"),
                SourceCandidate("userId"),
            ),
        },
    ),
    TableSpec(
        bq_table="video_statistics",
        ch_table="video_statistics",
        key_columns=("video_id",),
        target_columns=(
            "video_id",
            "user_normalized_like_perc",
            "user_normalized_share_perc",
            "user_normalized_watch_percentage_perc",
            "total_impressions",
            "last_update_timestamp",
        ),
        watermark_column="last_update_timestamp",
        source_candidates={},
    ),
    TableSpec(
        bq_table="video_unique_v2",
        ch_table="video_unique_v2",
        key_columns=("video_id",),
        target_columns=("video_id", "videohash", "created_at"),
        watermark_column="created_at",
        source_candidates={},
    ),
    TableSpec(
        bq_table="bot_uploaded_content",
        ch_table="bot_uploaded_content",
        key_columns=("video_id",),
        target_columns=(
            "user_id",
            "publisher_user_id",
            "canister_id",
            "video_id",
            "post_id",
            "country",
            "display_name",
            "timestamp",
        ),
        watermark_column="timestamp",
        source_candidates={
            "user_id": (
                SourceCandidate("user_id"),
                SourceCandidate("userId"),
            ),
            "publisher_user_id": (
                SourceCandidate("publisher_user_id"),
                SourceCandidate("publisherId"),
            ),
        },
    ),
    TableSpec(
        bq_table="userVideoRelation",
        ch_table="user_video_relation",
        key_columns=("user_id", "video_id"),
        target_columns=(
            "user_id",
            "video_id",
            "last_watched_timestamp",
            "mean_percentage_watched",
            "last_liked_timestamp",
            "liked",
            "last_shared_timestamp",
            "shared",
        ),
        watermark_column="last_watched_timestamp",
        source_candidates={
            "user_id": (
                SourceCandidate("user_id"),
                SourceCandidate("userId"),
            ),
            "video_id": (
                SourceCandidate("video_id"),
                SourceCandidate("videoId"),
            ),
            "last_watched_timestamp": (
                SourceCandidate("last_watched_timestamp"),
                SourceCandidate("lastWatchedTimestamp"),
            ),
            "mean_percentage_watched": (
                SourceCandidate("mean_percentage_watched"),
                SourceCandidate("meanPercentageWatched"),
            ),
            "last_liked_timestamp": (
                SourceCandidate("last_liked_timestamp"),
                SourceCandidate("lastLikedTimestamp"),
            ),
            "last_shared_timestamp": (
                SourceCandidate("last_shared_timestamp"),
                SourceCandidate("lastSharedTimestamp"),
            ),
        },
    ),
)


def load_service_cred() -> str:
    env_val = os.environ.get("SERVICE_CRED", "").strip()
    if env_val:
        return env_val

    env_file = pathlib.Path(__file__).parent / ".env"
    if env_file.exists():
        text = env_file.read_text()
        match = re.search(r"SERVICE_CRED\s*=\s*'(\{.*?\})\s*'", text, re.DOTALL)
        if match:
            return match.group(1).strip()

    raise RuntimeError("SERVICE_CRED not found in env or .env file")


def get_bigquery_client() -> bigquery.Client:
    try:
        cred_json = load_service_cred()
        cred_dict = json.loads(cred_json)
        credentials = service_account.Credentials.from_service_account_info(cred_dict)
        return bigquery.Client(project=BQ_PROJECT, credentials=credentials)
    except RuntimeError:
        return bigquery.Client(project=BQ_PROJECT)


def get_clickhouse_client() -> Client:
    return Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=CH_SECURE,
        verify=CH_VERIFY,
    )


def resolve_tables(table_names: list[str]) -> list[TableSpec]:
    if not table_names:
        return list(TABLES)

    wanted = set(table_names)
    resolved = [table for table in TABLES if table.bq_table in wanted or table.ch_table in wanted]
    missing = wanted - {table.bq_table for table in resolved} - {table.ch_table for table in resolved}
    if missing:
        raise SystemExit(f"Unknown tables requested: {sorted(missing)}")
    return resolved


def source_candidates_for(table: TableSpec, target_col: str) -> tuple[SourceCandidate, ...]:
    configured = table.source_candidates.get(target_col)
    if configured:
        return configured
    return (SourceCandidate(target_col),)


def choose_source_expr(table: TableSpec, target_col: str, bq_columns: set[str]) -> str:
    for candidate in source_candidates_for(table, target_col):
        if candidate.source not in bq_columns:
            continue

        quoted_source = f"`{candidate.source}`"
        quoted_target = f"`{target_col}`"

        if candidate.transform == "identity":
            if candidate.source == target_col:
                return quoted_source
            return f"{quoted_source} AS {quoted_target}"

        if candidate.transform == "json":
            return f"TO_JSON_STRING({quoted_source}) AS {quoted_target}"

        raise RuntimeError(f"Unsupported transform {candidate.transform!r} for {table.bq_table}.{target_col}")

    attempted = ", ".join(candidate.source for candidate in source_candidates_for(table, target_col))
    raise RuntimeError(
        f"Could not resolve source column for {table.bq_table}.{target_col}; "
        f"tried [{attempted}] against BQ schema {sorted(bq_columns)}"
    )


def snapshot_literal(snapshot_ts: dt.datetime) -> str:
    return snapshot_ts.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")


def bq_table_expr(table: str, snapshot_ts: dt.datetime) -> str:
    ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    return f"{ref} FOR SYSTEM_TIME AS OF TIMESTAMP('{snapshot_literal(snapshot_ts)}')"


def build_project_sql(table: TableSpec, bq_columns: set[str], snapshot_ts: dt.datetime) -> str:
    select_exprs = [choose_source_expr(table, target_col, bq_columns) for target_col in table.target_columns]
    return f"SELECT {', '.join(select_exprs)} FROM {bq_table_expr(table.bq_table, snapshot_ts)}"


def build_snapshot_sql(table: TableSpec, bq_columns: set[str], snapshot_ts: dt.datetime) -> str:
    project_sql = build_project_sql(table, bq_columns, snapshot_ts)

    if table.watermark_column is None:
        return f"SELECT * FROM ({project_sql})"

    key_cols = ", ".join(f"`{column}`" for column in table.key_columns)
    order_parts = [f"`{table.watermark_column}` DESC NULLS LAST"]
    order_parts.extend(f"`{column}` DESC NULLS LAST" for column in table.key_columns)
    return (
        "SELECT * EXCEPT(_row_num) "
        "FROM ("
        f"SELECT *, ROW_NUMBER() OVER (PARTITION BY {key_cols} ORDER BY {', '.join(order_parts)}) AS _row_num "
        f"FROM ({project_sql})"
        ") "
        "WHERE _row_num = 1"
    )


def get_bq_columns(bq: bigquery.Client, table_name: str) -> list[str]:
    table_ref = bq.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}")
    return [field.name for field in table_ref.schema]


def get_bq_raw_count(bq: bigquery.Client, table: TableSpec, snapshot_ts: dt.datetime) -> int:
    query = f"SELECT COUNT(*) AS cnt FROM {bq_table_expr(table.bq_table, snapshot_ts)}"
    return int(list(bq.query(query).result())[0]["cnt"])


def get_bq_compare_count(bq: bigquery.Client, table: TableSpec, bq_columns: set[str], snapshot_ts: dt.datetime) -> int:
    query = f"SELECT COUNT(*) AS cnt FROM ({build_snapshot_sql(table, bq_columns, snapshot_ts)})"
    return int(list(bq.query(query).result())[0]["cnt"])


def ensure_dir(path: pathlib.Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def atomic_write_json(path: pathlib.Path, payload: dict[str, Any]) -> None:
    ensure_dir(path.parent)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False) as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        temp_path = pathlib.Path(handle.name)
    temp_path.replace(path)


def read_json(path: pathlib.Path) -> dict[str, Any]:
    return json.loads(path.read_text()) if path.exists() else {}


def default_snapshot_id() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def latest_snapshot_dir(root: pathlib.Path) -> pathlib.Path | None:
    candidates: list[tuple[int, str, pathlib.Path]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        manifest_path = child / "manifest.json"
        if not manifest_path.exists():
            continue

        manifest = read_json(manifest_path)
        has_tables = 1 if manifest.get("tables") else 0
        candidates.append((has_tables, child.name, child))

    if not candidates:
        return None

    return sorted(candidates)[-1][2]


def save_manifest(root: pathlib.Path, manifest: dict[str, Any]) -> None:
    atomic_write_json(root / "manifest.json", manifest)


def ensure_manifest(root: pathlib.Path, snapshot_id: str | None) -> tuple[pathlib.Path, dict[str, Any]]:
    if snapshot_id:
        run_dir = root / snapshot_id
    else:
        existing_run_dir = latest_snapshot_dir(root)
        run_dir = existing_run_dir if existing_run_dir is not None else root / default_snapshot_id()

    ensure_dir(run_dir)
    manifest_path = run_dir / "manifest.json"
    if manifest_path.exists():
        manifest = read_json(manifest_path)
    else:
        manifest = {
            "snapshot_id": run_dir.name,
            "snapshot_ts": None,
            "tables": {},
        }
        save_manifest(run_dir, manifest)
    return run_dir, manifest


def serialize_row(row: dict[str, Any], table: TableSpec, snapshot_ts: dt.datetime) -> dict[str, Any]:
    result = dict(row)
    if table.include_updated_at:
        result["_updated_at"] = snapshot_ts
    return result


def write_chunk_parquet(path: pathlib.Path, rows: list[dict[str, Any]]) -> None:
    ensure_dir(path.parent)
    table = pa.Table.from_pylist(rows)
    pq.write_table(table, path)


def iter_chunk_rows(path: pathlib.Path) -> Iterable[dict[str, Any]]:
    return pq.read_table(path).to_pylist()


def get_ch_engine(ch: Client, table_name: str) -> str:
    rows = ch.execute(
        """
        SELECT engine
        FROM system.tables
        WHERE database = %(database)s AND name = %(table)s
        """,
        {"database": CH_DATABASE, "table": table_name},
    )
    if not rows:
        raise RuntimeError(f"ClickHouse table not found: {CH_DATABASE}.{table_name}")
    return str(rows[0][0])


def get_ch_count(ch: Client, table_name: str, *, use_final: bool) -> int:
    suffix = " FINAL" if use_final else ""
    return int(ch.execute(f"SELECT count() FROM {CH_DATABASE}.{table_name}{suffix}")[0][0])


def table_insert_columns(table: TableSpec) -> list[str]:
    columns = list(table.target_columns)
    if table.include_updated_at:
        columns.append("_updated_at")
    return columns


def snapshot_table(
    bq: bigquery.Client,
    table: TableSpec,
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    *,
    chunk_rows: int,
    snapshot_ts: dt.datetime,
    force: bool,
) -> None:
    tables_state = manifest.setdefault("tables", {})
    table_state = tables_state.setdefault(table.ch_table, {})
    if table_state.get("snapshot_complete") and not force:
        print(f"[snapshot] {table.ch_table}: already complete, skipping")
        return

    table_dir = run_dir / table.ch_table
    if force and table_dir.exists():
        for file in table_dir.glob("*.parquet"):
            file.unlink()
    ensure_dir(table_dir)

    bq_columns = set(get_bq_columns(bq, table.bq_table))
    raw_count = get_bq_raw_count(bq, table, snapshot_ts)
    compare_count = get_bq_compare_count(bq, table, bq_columns, snapshot_ts)
    query = build_snapshot_sql(table, bq_columns, snapshot_ts)

    table_state.update(
        {
            "bq_table": table.bq_table,
            "ch_table": table.ch_table,
            "key_columns": list(table.key_columns),
            "watermark_column": table.watermark_column,
            "target_columns": list(table_insert_columns(table)),
            "raw_count": raw_count,
            "compare_count": compare_count,
            "snapshot_complete": False,
            "load_started": False,
            "load_complete": False,
            "loaded_chunks": [],
        }
    )
    save_manifest(run_dir, manifest)

    print(f"[snapshot] {table.ch_table}: raw={raw_count:,} compare={compare_count:,}")

    query_job = bq.query(query)
    chunk_index = 0
    rows_seen = 0
    chunk: list[dict[str, Any]] = []

    for row in query_job.result(page_size=chunk_rows):
        chunk.append(serialize_row(dict(row), table, snapshot_ts))
        if len(chunk) >= chunk_rows:
            chunk_path = table_dir / f"chunk-{chunk_index:05d}.parquet"
            write_chunk_parquet(chunk_path, chunk)
            rows_seen += len(chunk)
            chunk_index += 1
            print(f"[snapshot] {table.ch_table}: wrote {rows_seen:,}/{compare_count:,}", flush=True)
            chunk = []

    if chunk:
        chunk_path = table_dir / f"chunk-{chunk_index:05d}.parquet"
        write_chunk_parquet(chunk_path, chunk)
        rows_seen += len(chunk)
        chunk_index += 1

    table_state.update(
        {
            "snapshot_complete": True,
            "snapshot_rows": rows_seen,
            "chunk_count": chunk_index,
        }
    )
    save_manifest(run_dir, manifest)
    print(f"[snapshot] {table.ch_table}: complete rows={rows_seen:,} chunks={chunk_index}")


def load_table(
    ch: Client,
    table: TableSpec,
    run_dir: pathlib.Path,
    manifest: dict[str, Any],
    *,
    load_batch_rows: int,
) -> None:
    tables_state = manifest.setdefault("tables", {})
    table_state = tables_state.get(table.ch_table)
    if not table_state or not table_state.get("snapshot_complete"):
        raise RuntimeError(f"Snapshot not complete for {table.ch_table}")

    if table_state.get("load_complete"):
        print(f"[load] {table.ch_table}: already complete, skipping")
        return

    if not table_state.get("load_started"):
        ch.execute(f"TRUNCATE TABLE {CH_DATABASE}.{table.ch_table}")
        table_state["load_started"] = True
        table_state["loaded_chunks"] = []
        save_manifest(run_dir, manifest)
    elif not table_state.get("loaded_chunks"):
        # Recovery path for a failed first chunk: some rows may have been inserted
        # before the crash, but no chunk was checkpointed yet.
        ch.execute(f"TRUNCATE TABLE {CH_DATABASE}.{table.ch_table}")
        save_manifest(run_dir, manifest)

    loaded_chunks = set(table_state.get("loaded_chunks", []))
    table_dir = run_dir / table.ch_table
    columns = table_state["target_columns"]

    for chunk_path in sorted(table_dir.glob("chunk-*.parquet")):
        if chunk_path.name in loaded_chunks:
            continue

        rows = list(iter_chunk_rows(chunk_path))
        for offset in range(0, len(rows), load_batch_rows):
            batch_rows = rows[offset : offset + load_batch_rows]
            values = [[row.get(column) for column in columns] for row in batch_rows]
            ch.execute(
                f"INSERT INTO {CH_DATABASE}.{table.ch_table} ({', '.join(columns)}) VALUES",
                values,
            )

        table_state.setdefault("loaded_chunks", []).append(chunk_path.name)
        table_state["loaded_rows"] = len(table_state["loaded_chunks"])  # low-signal checkpoint marker
        save_manifest(run_dir, manifest)
        print(f"[load] {table.ch_table}: loaded {chunk_path.name}")

    table_state["load_complete"] = True
    save_manifest(run_dir, manifest)
    print(f"[load] {table.ch_table}: complete")


def verify_table(ch: Client, table: TableSpec, manifest: dict[str, Any]) -> tuple[int, int, int]:
    table_state = manifest["tables"][table.ch_table]
    raw = get_ch_count(ch, table.ch_table, use_final=False)
    compare = get_ch_count(ch, table.ch_table, use_final=bool(table.watermark_column))
    expected = int(table_state["compare_count"])
    return raw, compare, expected


def print_status(run_dir: pathlib.Path, manifest: dict[str, Any], tables: list[TableSpec]) -> None:
    print(f"Snapshot dir: {run_dir}")
    print(f"Snapshot id:  {manifest.get('snapshot_id')}")
    print(f"Snapshot ts:  {manifest.get('snapshot_ts')}")
    for table in tables:
        state = manifest.get("tables", {}).get(table.ch_table, {})
        print(
            f"- {table.ch_table}: "
            f"snapshot_complete={state.get('snapshot_complete', False)} "
            f"chunks={state.get('chunk_count', 0)} "
            f"load_complete={state.get('load_complete', False)} "
            f"loaded_chunks={len(state.get('loaded_chunks', []))}"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Hybrid BigQuery -> ClickHouse cluster backfill")
    parser.add_argument("mode", choices=("snapshot", "load", "verify", "status"))
    parser.add_argument("--tables", nargs="*", default=[])
    parser.add_argument("--snapshot-dir", default=str(DEFAULT_SNAPSHOT_DIR))
    parser.add_argument("--snapshot-id")
    parser.add_argument("--chunk-rows", type=int, default=DEFAULT_CHUNK_ROWS)
    parser.add_argument("--load-batch-rows", type=int, default=DEFAULT_LOAD_BATCH_ROWS)
    parser.add_argument("--force", action="store_true", help="recreate snapshots for selected tables")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    tables = resolve_tables(args.tables)
    root = pathlib.Path(args.snapshot_dir)
    ensure_dir(root)
    run_dir, manifest = ensure_manifest(root, args.snapshot_id)

    if manifest.get("snapshot_ts") is None:
        manifest["snapshot_ts"] = snapshot_literal(dt.datetime.now(dt.timezone.utc).replace(microsecond=0))
        save_manifest(run_dir, manifest)

    snapshot_ts = dt.datetime.fromisoformat(manifest["snapshot_ts"].replace("Z", "+00:00"))

    if args.mode == "status":
        print_status(run_dir, manifest, tables)
        return 0

    if args.mode == "snapshot":
        bq = get_bigquery_client()
        for table in tables:
            snapshot_table(
                bq,
                table,
                run_dir,
                manifest,
                chunk_rows=args.chunk_rows,
                snapshot_ts=snapshot_ts,
                force=args.force,
            )
        return 0

    ch = get_clickhouse_client()

    if args.mode == "load":
        for table in tables:
            load_table(
                ch,
                table,
                run_dir,
                manifest,
                load_batch_rows=args.load_batch_rows,
            )
        return 0

    if args.mode == "verify":
        failures = 0
        for table in tables:
            raw, compare, expected = verify_table(ch, table, manifest)
            status = "OK" if compare == expected else "MISMATCH"
            print(
                f"{table.ch_table:<28} raw={raw:>10,} compare={compare:>10,} "
                f"expected={expected:>10,} {status}"
            )
            if compare != expected:
                failures += 1
        return 1 if failures else 0

    raise SystemExit(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
    sys.exit(main())
