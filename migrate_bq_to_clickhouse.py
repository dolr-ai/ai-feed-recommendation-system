#!/usr/bin/env python3
"""
One-time migration script: BigQuery → ClickHouse
Reads each of the 9 yral_ds tables from BQ and bulk-loads into ClickHouse.

Usage:
    pip install google-cloud-bigquery clickhouse-driver pandas pyarrow
    python migrate_bq_to_clickhouse.py
"""

import json
import os
import time
import pathlib
import datetime as dt
from dataclasses import dataclass
from google.cloud import bigquery
from google.oauth2 import service_account
from clickhouse_driver import Client

def load_service_cred() -> str:
    """Load SERVICE_CRED from env var, or fall back to reading .env file."""
    import re
    env_val = os.environ.get("SERVICE_CRED", "").strip()
    if env_val:
        return env_val
    # Try reading .env in the same directory as this script
    env_file = pathlib.Path(__file__).parent / ".env"
    if env_file.exists():
        text = env_file.read_text()
        # Extract everything between SERVICE_CRED = ' and the closing '
        match = re.search(r"SERVICE_CRED\s*=\s*'(\{.*?\})\s*'", text, re.DOTALL)
        if match:
            return match.group(1).strip()
    raise RuntimeError("SERVICE_CRED not found in env or .env file")

# ── Config ────────────────────────────────────────────────────────────────────

BQ_PROJECT  = "hot-or-not-feed-intelligence"
BQ_DATASET  = "yral_ds"

CH_HOST     = os.getenv("CH_HOST", "138.201.196.246")
CH_PORT     = int(os.getenv("CH_PORT", "9440"))
CH_USER     = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "Yral-CH-2026-prod")
CH_DATABASE = os.getenv("CH_DATABASE", "yral")
CH_SECURE   = os.getenv("CH_SECURE", "true").strip().lower() in {"1", "true", "yes", "on"}
CH_VERIFY   = os.getenv("CH_VERIFY", "false").strip().lower() in {"1", "true", "yes", "on"}

BATCH_SIZE  = 50_000   # rows per insert batch — tune down if memory issues

# Credential loaded at runtime via load_service_cred()

# ── Table definitions ─────────────────────────────────────────────────────────
@dataclass(frozen=True)
class TableSpec:
    bq_table: str
    ch_table: str
    key_columns: tuple[str, ...]
    watermark_column: str | None = None


TABLES = [
    # Smallest first
    TableSpec("excluded_videos",           "excluded_videos",           ("video_id",),                  "excluded_at"),
    TableSpec("global_popular_videos_l7d", "global_popular_videos_l7d", ("video_id",)),
    TableSpec("ai_ugc",                    "ai_ugc",                    ("video_id",),                  "upload_timestamp"),
    TableSpec("follower_graph",            "follower_graph",            ("follower_id", "following_id"), "last_updated_timestamp"),
    TableSpec("ugc_content_approval",      "ugc_content_approval",      ("video_id",)),
    TableSpec("video_statistics",          "video_statistics",          ("video_id",),                  "last_update_timestamp"),
    TableSpec("video_unique_v2",           "video_unique_v2",           ("video_id",)),
    TableSpec("bot_uploaded_content",      "bot_uploaded_content",      ("video_id",),                  "timestamp"),
    # Largest last
    TableSpec("userVideoRelation",         "user_video_relation",       ("user_id", "video_id"),       "last_watched_timestamp"),
]

# ── BQ type → Python converter ────────────────────────────────────────────────
def convert_row(row, schema):
    """Convert a BQ Row into a plain list while preserving BigQuery NULL semantics."""
    result = []
    for field in schema:
        val = row[field.name]
        if field.field_type == "JSON":
            if val is None:
                result.append(None)
            elif isinstance(val, str):
                result.append(val)
            else:
                result.append(json.dumps(val, separators=(",", ":"), ensure_ascii=False))
        else:
            result.append(val)
    return result


def bq_table_expr(table: str, snapshot_ts: dt.datetime | None) -> str:
    ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    if snapshot_ts is None:
        return ref

    snapshot_literal = snapshot_ts.astimezone(dt.timezone.utc).isoformat().replace("+00:00", "Z")
    return f"{ref} FOR SYSTEM_TIME AS OF TIMESTAMP('{snapshot_literal}')"


def get_ch_engine(ch: Client, table: str) -> str:
    rows = ch.execute(
        """
        SELECT engine
        FROM system.tables
        WHERE database = %(database)s AND name = %(table)s
        """,
        {"database": CH_DATABASE, "table": table},
    )
    if not rows:
        raise RuntimeError(f"ClickHouse table not found: {CH_DATABASE}.{table}")
    return str(rows[0][0])


def uses_latest_per_key(spec: TableSpec, ch_engine: str) -> bool:
    return "ReplacingMergeTree" in ch_engine and spec.watermark_column is not None


def build_bq_source_query(
    spec: TableSpec,
    columns: list[str],
    *,
    snapshot_ts: dt.datetime | None,
    use_latest_per_key: bool,
) -> str:
    select_cols = ", ".join(columns)
    table_expr = bq_table_expr(spec.bq_table, snapshot_ts)

    if not use_latest_per_key:
        return f"SELECT {select_cols} FROM {table_expr}"

    partition = ", ".join(spec.key_columns)
    order_parts = [f"{spec.watermark_column} DESC NULLS LAST"]
    order_parts.extend(f"{column} DESC NULLS LAST" for column in spec.key_columns)
    order_parts.append(f"TO_JSON_STRING(STRUCT({select_cols})) DESC")

    return (
        f"SELECT {select_cols} "
        f"FROM ("
        f"SELECT {select_cols}, "
        f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {', '.join(order_parts)}) AS _row_num "
        f"FROM {table_expr}"
        f") "
        f"WHERE _row_num = 1"
    )


def get_bq_counts(
    bq: bigquery.Client,
    spec: TableSpec,
    *,
    snapshot_ts: dt.datetime | None,
    use_latest_per_key: bool,
) -> tuple[int, int]:
    raw_query = f"SELECT COUNT(*) AS cnt FROM {bq_table_expr(spec.bq_table, snapshot_ts)}"
    raw_count = int(list(bq.query(raw_query).result())[0]["cnt"])

    if not use_latest_per_key:
        return raw_count, raw_count

    key_expr = ", ".join(spec.key_columns)
    compare_query = (
        "SELECT COUNT(DISTINCT TO_JSON_STRING(STRUCT("
        f"{key_expr}"
        f"))) AS cnt FROM {bq_table_expr(spec.bq_table, snapshot_ts)}"
    )
    compare_count = int(list(bq.query(compare_query).result())[0]["cnt"])
    return raw_count, compare_count


def get_ch_count(ch: Client, table: str, *, use_final: bool) -> int:
    suffix = " FINAL" if use_final else ""
    return int(ch.execute(f"SELECT count() FROM {CH_DATABASE}.{table}{suffix}")[0][0])

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # ── Connect BQ ────────────────────────────────────────────────────────────
    try:
        cred_json = load_service_cred()
        cred_dict = json.loads(cred_json)
        credentials = service_account.Credentials.from_service_account_info(cred_dict)
        bq = bigquery.Client(project=BQ_PROJECT, credentials=credentials)
        print("  Auth: service account from .env")
    except RuntimeError:
        # Falls back to application default credentials (gcloud auth)
        bq = bigquery.Client(project=BQ_PROJECT)
        print("  Auth: application default credentials")

    # ── Connect ClickHouse ─────────────────────────────────────────────────────
    ch = Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
        secure=CH_SECURE,
        verify=CH_VERIFY,
    )

    print(f"\n{'='*60}")
    print(f"  BigQuery → ClickHouse Migration")
    print(f"  Source: {BQ_PROJECT}.{BQ_DATASET}")
    print(f"  Target: {CH_HOST}/{CH_DATABASE}")
    print(f"{'='*60}\n")

    results = []
    snapshot_ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)

    import sys
    target_tables = sys.argv[1:] if len(sys.argv) > 1 else [t.bq_table for t in TABLES]
    print(f"  Snapshot: {snapshot_ts.isoformat()}")

    for spec in TABLES:
        if spec.bq_table not in target_tables:
            continue
        print(f"\n── {spec.bq_table} ──────────────────────────────────")
        t_start = time.time()

        ch_engine = get_ch_engine(ch, spec.ch_table)
        use_latest_per_key = uses_latest_per_key(spec, ch_engine)

        bq_raw_count, bq_compare_count = get_bq_counts(
            bq,
            spec,
            snapshot_ts=snapshot_ts,
            use_latest_per_key=use_latest_per_key,
        )
        print(f"  BQ rows:  {bq_raw_count:,}")
        if use_latest_per_key:
            print(
                "  BQ view:  "
                f"compare={bq_compare_count:,} "
                f"dedup_delta={bq_raw_count - bq_compare_count:+,} "
                f"(latest row per key by {spec.watermark_column})"
            )

        # Get schema
        table_ref = bq.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{spec.bq_table}")
        schema = table_ref.schema
        columns = [f.name for f in schema]
        print(f"  Columns:  {columns}")
        print(f"  CH engine:{ch_engine}")

        # Truncate target (clean slate for re-runs)
        ch.execute(f"TRUNCATE TABLE IF EXISTS {CH_DATABASE}.{spec.ch_table}")

        # Stream from BQ in batches
        query_job = bq.query(
            build_bq_source_query(
                spec,
                columns,
                snapshot_ts=snapshot_ts,
                use_latest_per_key=use_latest_per_key,
            )
        )
        rows_inserted: int = 0
        batch = []

        for row in query_job.result(page_size=BATCH_SIZE):
            batch.append(convert_row(row, schema))
            if len(batch) >= BATCH_SIZE:
                ch.execute(
                    f"INSERT INTO {CH_DATABASE}.{spec.ch_table} ({', '.join(columns)}) VALUES",
                    batch
                )
                rows_inserted += len(batch)
                print(f"  Inserted: {rows_inserted:,} / {bq_compare_count:,}", end="\r")
                batch = []

        # Flush remainder
        if batch:
            ch.execute(
                f"INSERT INTO {CH_DATABASE}.{spec.ch_table} ({', '.join(columns)}) VALUES",
                batch
            )
            rows_inserted += len(batch)

        elapsed = time.time() - t_start

        # Verify count in ClickHouse
        ch_raw_count = get_ch_count(ch, spec.ch_table, use_final=False)
        ch_compare_count = get_ch_count(ch, spec.ch_table, use_final=use_latest_per_key)
        if use_latest_per_key:
            print(
                "  CH view:  "
                f"raw={ch_raw_count:,} compare={ch_compare_count:,} "
                f"dedup_delta={ch_raw_count - ch_compare_count:+,}"
            )
        status = "✅" if ch_compare_count == bq_compare_count else "⚠️ MISMATCH"
        print(
            f"\n  Compare: BQ={bq_compare_count:,}  →  CH={ch_compare_count:,}  "
            f"{status}  ({elapsed:.1f}s)"
        )
        results.append((spec.bq_table, bq_raw_count, bq_compare_count, ch_compare_count, status))

    # ── Summary ────────────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"  Migration Complete")
    print(f"{'='*60}")
    print(f"  {'Table':<35} {'BQ raw':>10} {'BQ cmp':>10} {'CH cmp':>10} Status")
    print(f"  {'-'*60}")
    for table, bq_raw, bq_cmp, ch_cmp, status in results:
        print(f"  {table:<35} {bq_raw:>10,} {bq_cmp:>10,} {ch_cmp:>10,} {status}")
    print()

if __name__ == "__main__":
    main()
