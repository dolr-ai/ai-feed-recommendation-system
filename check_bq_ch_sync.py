#!/usr/bin/env python3
"""
Compare BigQuery and ClickHouse copies of the DAG-written tables.

The checker is ClickHouse-aware:
- uses FINAL for ReplacingMergeTree tables
- normalizes ClickHouse naive datetimes using the server timezone
- compares floating-point values with tolerance

Modes:
1. summary (default)
   - compare row counts
   - compare watermarks
   - compare first and last N rows ordered by the table key

2. strict
   - stream the full table from both systems ordered by key
   - compare every normalized row with tolerances

Usage examples:
    python check_bq_ch_sync.py
    python check_bq_ch_sync.py --mode strict --tables excluded_videos ai_ugc
    python check_bq_ch_sync.py --sample-size 20 --show-mismatches 10

Current production note:
    The hardcoded CH defaults in this script are legacy values. For the
    current cluster, run it with CH_HOST/CH_PORT/CH_PASSWORD/CH_SECURE
    overrides, or use an SSH tunnel to ansuman-1 localhost native port.
"""

from __future__ import annotations

import argparse
import datetime as dt
import decimal
import json
import math
import os
import pathlib
import re
from dataclasses import dataclass
from typing import Iterator, Sequence
from zoneinfo import ZoneInfo

from clickhouse_driver import Client
from google.cloud import bigquery
from google.oauth2 import service_account


@dataclass(frozen=True)
class TableSpec:
    name: str
    bq_table: str
    ch_table: str
    key_columns: tuple[str, ...]
    watermark_column: str | None = None


@dataclass(frozen=True)
class ChTableInfo:
    engine: str
    use_final: bool


@dataclass(frozen=True)
class CompareOptions:
    float_rel_tol: float
    float_abs_tol: float
    timestamp_tol: dt.timedelta


TABLE_SPECS: tuple[TableSpec, ...] = (
    TableSpec(
        name="excluded_videos",
        bq_table="excluded_videos",
        ch_table="excluded_videos",
        key_columns=("video_id",),
        watermark_column="excluded_at",
    ),
    TableSpec(
        name="global_popular_videos_l7d",
        bq_table="global_popular_videos_l7d",
        ch_table="global_popular_videos_l7d",
        key_columns=("video_id",),
    ),
    TableSpec(
        name="ai_ugc",
        bq_table="ai_ugc",
        ch_table="ai_ugc",
        key_columns=("video_id",),
        watermark_column="upload_timestamp",
    ),
    TableSpec(
        name="follower_graph",
        bq_table="follower_graph",
        ch_table="follower_graph",
        key_columns=("follower_id", "following_id"),
        watermark_column="last_updated_timestamp",
    ),
    TableSpec(
        name="ugc_content_approval",
        bq_table="ugc_content_approval",
        ch_table="ugc_content_approval",
        key_columns=("video_id",),
        watermark_column="created_at",
    ),
    TableSpec(
        name="video_statistics",
        bq_table="video_statistics",
        ch_table="video_statistics",
        key_columns=("video_id",),
        watermark_column="last_update_timestamp",
    ),
    TableSpec(
        name="video_unique_v2",
        bq_table="video_unique_v2",
        ch_table="video_unique_v2",
        key_columns=("video_id",),
        watermark_column="created_at",
    ),
    TableSpec(
        name="bot_uploaded_content",
        bq_table="bot_uploaded_content",
        ch_table="bot_uploaded_content",
        key_columns=("video_id",),
        watermark_column="timestamp",
    ),
    TableSpec(
        name="user_video_relation",
        bq_table="userVideoRelation",
        ch_table="user_video_relation",
        key_columns=("user_id", "video_id"),
        watermark_column="last_watched_timestamp",
    ),
)

BQ_PROJECT = "hot-or-not-feed-intelligence"
BQ_DATASET = "yral_ds"

CH_HOST = os.getenv("CH_HOST", "138.201.196.246")
CH_PORT = int(os.getenv("CH_PORT", "9440"))
CH_USER = os.getenv("CH_USER", "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "Yral-CH-2026-prod")
CH_DATABASE = os.getenv("CH_DATABASE", "yral")
CH_SECURE = os.getenv("CH_SECURE", "true").strip().lower() in {"1", "true", "yes", "on"}
CH_VERIFY = os.getenv("CH_VERIFY", "false").strip().lower() in {"1", "true", "yes", "on"}

DEFAULT_FLOAT_REL_TOL = 1e-6
DEFAULT_FLOAT_ABS_TOL = 1e-6
DEFAULT_TIMESTAMP_TOL_MS = 1.0
MAX_DIFF_COLUMNS = 3


def load_service_cred() -> str:
    """Load SERVICE_CRED from env var, or fall back to reading .env file."""
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


def close_clickhouse_client(ch: Client) -> None:
    disconnect = getattr(ch, "disconnect_connection", None)
    if callable(disconnect):
        disconnect()


def get_ch_timezone_name(ch: Client) -> str:
    return str(ch.execute("SELECT timezone()")[0][0])


def get_ch_timezone(tz_name: str) -> dt.tzinfo:
    try:
        return ZoneInfo(tz_name)
    except Exception:
        return dt.timezone.utc


def get_bq_columns(bq: bigquery.Client, table: str) -> list[str]:
    table_ref = bq.get_table(f"{BQ_PROJECT}.{BQ_DATASET}.{table}")
    return [field.name for field in table_ref.schema]


def get_ch_columns(ch: Client, table: str) -> list[str]:
    rows = ch.execute(f"DESCRIBE TABLE {CH_DATABASE}.{table}")
    return [row[0] for row in rows]


def get_ch_table_info(ch: Client, table: str) -> ChTableInfo:
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

    engine = str(rows[0][0])
    use_final = "ReplacingMergeTree" in engine
    return ChTableInfo(engine=engine, use_final=use_final)


def ch_table_expr(spec: TableSpec, table_info: ChTableInfo, *, use_final: bool | None = None) -> str:
    if use_final is None:
        use_final = table_info.use_final
    suffix = " FINAL" if use_final else ""
    return f"{CH_DATABASE}.{spec.ch_table}{suffix}"


def shared_columns(bq_columns: Sequence[str], ch_columns: Sequence[str]) -> list[str]:
    ch_set = set(ch_columns)
    return [column for column in bq_columns if column in ch_set]


def normalize_json_like(value):
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                loaded = json.loads(value)
            except Exception:
                return value
            return json.dumps(loaded, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        return value
    return value


def normalize_value(value, *, naive_datetime_tz: dt.tzinfo | None = None):
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    if isinstance(value, decimal.Decimal):
        return value.normalize()
    if isinstance(value, dt.datetime):
        if value.tzinfo is None:
            base_tz = naive_datetime_tz or dt.timezone.utc
            value = value.replace(tzinfo=base_tz)
        return value.astimezone(dt.timezone.utc)
    if isinstance(value, (dt.date, dt.time)):
        return value.isoformat()
    return normalize_json_like(value)


def normalize_row(
    columns: Sequence[str],
    values: Sequence[object],
    *,
    naive_datetime_tz: dt.tzinfo | None = None,
) -> dict[str, object]:
    return {
        column: normalize_value(value, naive_datetime_tz=naive_datetime_tz)
        for column, value in zip(columns, values)
    }


def is_numeric(value: object) -> bool:
    return isinstance(value, (int, float, decimal.Decimal)) and not isinstance(value, bool)


def render_value(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, dt.datetime):
        return value.isoformat()
    if isinstance(value, float):
        return format(value, ".17g")
    if isinstance(value, decimal.Decimal):
        return format(value.normalize(), "f")
    return str(value)


def format_key(spec: TableSpec, row: dict[str, object]) -> str:
    payload = {column: render_value(row[column]) for column in spec.key_columns}
    return json.dumps(payload, sort_keys=True, ensure_ascii=False)


def values_equal(left: object, right: object, options: CompareOptions) -> bool:
    if left is None or right is None:
        return left is right

    if isinstance(left, dt.datetime) and isinstance(right, dt.datetime):
        return abs(left - right) <= options.timestamp_tol

    if is_numeric(left) and is_numeric(right):
        if isinstance(left, int) and isinstance(right, int):
            return left == right
        return math.isclose(
            float(left),
            float(right),
            rel_tol=options.float_rel_tol,
            abs_tol=options.float_abs_tol,
        )

    return left == right


def describe_value_difference(left: object, right: object) -> str:
    if left is None or right is None:
        return f"BQ={render_value(left)} CH={render_value(right)}"

    if isinstance(left, dt.datetime) and isinstance(right, dt.datetime):
        delta_ms = (right - left).total_seconds() * 1000
        return (
            f"BQ={render_value(left)} CH={render_value(right)} "
            f"delta={delta_ms:+.3f} ms"
        )

    if is_numeric(left) and is_numeric(right):
        if isinstance(left, int) and isinstance(right, int):
            delta = int(right) - int(left)
            return f"BQ={render_value(left)} CH={render_value(right)} delta={delta:+,}"
        delta = float(right) - float(left)
        return f"BQ={render_value(left)} CH={render_value(right)} delta={delta:+.9g}"

    return f"BQ={render_value(left)} CH={render_value(right)}"


def row_differences(
    columns: Sequence[str],
    bq_row: dict[str, object],
    ch_row: dict[str, object],
    options: CompareOptions,
) -> list[str]:
    diffs = []
    for column in columns:
        if not values_equal(bq_row[column], ch_row[column], options):
            diffs.append(f"{column}: {describe_value_difference(bq_row[column], ch_row[column])}")
    return diffs


def summarize_differences(differences: Sequence[str], *, limit: int = MAX_DIFF_COLUMNS) -> str:
    visible = list(differences[:limit])
    hidden = len(differences) - len(visible)
    summary = "; ".join(visible)
    if hidden > 0:
        summary = f"{summary}; +{hidden} more column mismatch(es)"
    return summary


def rows_equal(
    columns: Sequence[str],
    bq_row: dict[str, object],
    ch_row: dict[str, object],
    options: CompareOptions,
) -> bool:
    return not row_differences(columns, bq_row, ch_row, options)


def get_row_count_bq(bq: bigquery.Client, table: str) -> int:
    query = f"SELECT COUNT(*) AS cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    return int(list(bq.query(query).result())[0]["cnt"])


def bq_compare_uses_latest_per_key(spec: TableSpec, table_info: ChTableInfo) -> bool:
    return table_info.use_final and spec.watermark_column is not None


def build_bq_compare_select(
    spec: TableSpec,
    columns: Sequence[str],
    *,
    use_latest_per_key: bool,
) -> str:
    select_cols = ", ".join(columns)
    table_ref = f"`{BQ_PROJECT}.{BQ_DATASET}.{spec.bq_table}`"

    if not use_latest_per_key:
        return f"SELECT {select_cols} FROM {table_ref}"

    partition = ", ".join(spec.key_columns)
    order_parts = [f"{spec.watermark_column} DESC NULLS LAST"]
    order_parts.extend(f"{column} DESC NULLS LAST" for column in spec.key_columns)
    order_parts.append(f"TO_JSON_STRING(STRUCT({select_cols})) DESC")

    return (
        f"SELECT {select_cols} "
        f"FROM ("
        f"SELECT {select_cols}, "
        f"ROW_NUMBER() OVER (PARTITION BY {partition} ORDER BY {', '.join(order_parts)}) AS _row_num "
        f"FROM {table_ref}"
        f") "
        f"WHERE _row_num = 1"
    )


def get_row_count_bq_compare(
    bq: bigquery.Client,
    spec: TableSpec,
    *,
    use_latest_per_key: bool,
) -> int:
    if not use_latest_per_key:
        return get_row_count_bq(bq, spec.bq_table)

    key_expr = ", ".join(spec.key_columns)
    query = (
        "SELECT COUNT(DISTINCT TO_JSON_STRING(STRUCT("
        f"{key_expr}"
        f"))) AS cnt FROM `{BQ_PROJECT}.{BQ_DATASET}.{spec.bq_table}`"
    )
    return int(list(bq.query(query).result())[0]["cnt"])


def get_row_count_ch(ch: Client, spec: TableSpec, table_info: ChTableInfo, *, use_final: bool) -> int:
    return int(ch.execute(f"SELECT count() FROM {ch_table_expr(spec, table_info, use_final=use_final)}")[0][0])


def get_watermark_bq(bq: bigquery.Client, table: str, column: str):
    query = f"SELECT MAX({column}) AS max_value FROM `{BQ_PROJECT}.{BQ_DATASET}.{table}`"
    return normalize_value(list(bq.query(query).result())[0]["max_value"])


def get_watermark_ch(
    ch: Client,
    spec: TableSpec,
    table_info: ChTableInfo,
    column: str,
    *,
    ch_timezone: dt.tzinfo,
):
    value = ch.execute(
        f"SELECT max({column}) FROM {ch_table_expr(spec, table_info, use_final=table_info.use_final)}"
    )[0][0]
    return normalize_value(value, naive_datetime_tz=ch_timezone)


def fetch_bq_sample(
    bq: bigquery.Client,
    spec: TableSpec,
    columns: Sequence[str],
    order_columns: Sequence[str],
    limit: int,
    descending: bool,
    *,
    use_latest_per_key: bool,
) -> list[dict[str, object]]:
    order = ", ".join(f"{column} {'DESC' if descending else 'ASC'}" for column in order_columns)
    select_cols = ", ".join(columns)
    base_query = build_bq_compare_select(
        spec,
        columns,
        use_latest_per_key=use_latest_per_key,
    )
    query = (
        f"SELECT {select_cols} "
        f"FROM ({base_query}) "
        f"ORDER BY {order} "
        f"LIMIT {limit}"
    )
    rows = bq.query(query).result()
    return [normalize_row(columns, [row[column] for column in columns]) for row in rows]


def fetch_ch_sample(
    ch: Client,
    spec: TableSpec,
    table_info: ChTableInfo,
    columns: Sequence[str],
    order_columns: Sequence[str],
    limit: int,
    descending: bool,
    *,
    ch_timezone: dt.tzinfo,
) -> list[dict[str, object]]:
    order = ", ".join(f"{column} {'DESC' if descending else 'ASC'}" for column in order_columns)
    select_cols = ", ".join(columns)
    query = (
        f"SELECT {select_cols} "
        f"FROM {ch_table_expr(spec, table_info, use_final=table_info.use_final)} "
        f"ORDER BY {order} "
        f"LIMIT {limit}"
    )
    rows = ch.execute(query)
    return [normalize_row(columns, row, naive_datetime_tz=ch_timezone) for row in rows]


def iter_bq_rows(
    bq: bigquery.Client,
    spec: TableSpec,
    columns: Sequence[str],
    order_columns: Sequence[str],
    page_size: int,
    *,
    use_latest_per_key: bool,
) -> Iterator[dict[str, object]]:
    select_cols = ", ".join(columns)
    order = ", ".join(order_columns)
    base_query = build_bq_compare_select(
        spec,
        columns,
        use_latest_per_key=use_latest_per_key,
    )
    query = (
        f"SELECT {select_cols} "
        f"FROM ({base_query}) "
        f"ORDER BY {order}"
    )
    result_iter = bq.query(query).result(page_size=page_size)
    for row in result_iter:
        yield normalize_row(columns, [row[column] for column in columns])


def iter_ch_rows(
    ch: Client,
    spec: TableSpec,
    table_info: ChTableInfo,
    columns: Sequence[str],
    order_columns: Sequence[str],
    block_size: int,
    *,
    ch_timezone: dt.tzinfo,
) -> Iterator[dict[str, object]]:
    select_cols = ", ".join(columns)
    order = ", ".join(order_columns)
    query = (
        f"SELECT {select_cols} "
        f"FROM {ch_table_expr(spec, table_info, use_final=table_info.use_final)} "
        f"ORDER BY {order}"
    )
    for row in ch.execute_iter(query, settings={"max_block_size": block_size}):
        yield normalize_row(columns, row, naive_datetime_tz=ch_timezone)


def compare_row_lists(
    label: str,
    spec: TableSpec,
    columns: Sequence[str],
    bq_rows: Sequence[dict[str, object]],
    ch_rows: Sequence[dict[str, object]],
    options: CompareOptions,
) -> tuple[bool, str]:
    if len(bq_rows) != len(ch_rows):
        return False, f"{label}: length mismatch BQ={len(bq_rows)} CH={len(ch_rows)}"

    for index, (bq_row, ch_row) in enumerate(zip(bq_rows, ch_rows), start=1):
        diffs = row_differences(columns, bq_row, ch_row, options)
        if diffs:
            key = format_key(spec, bq_row)
            return False, (
                f"{label}: mismatch at sample #{index} key {key}; "
                f"{summarize_differences(diffs)}"
            )

    return True, f"{label}: match"


def compare_samples(
    bq: bigquery.Client,
    ch: Client,
    spec: TableSpec,
    table_info: ChTableInfo,
    columns: Sequence[str],
    sample_size: int,
    options: CompareOptions,
    *,
    ch_timezone: dt.tzinfo,
    use_latest_per_key: bool,
) -> tuple[bool, list[str]]:
    first_bq = fetch_bq_sample(
        bq,
        spec,
        columns,
        spec.key_columns,
        sample_size,
        descending=False,
        use_latest_per_key=use_latest_per_key,
    )
    first_ch = fetch_ch_sample(
        ch,
        spec,
        table_info,
        columns,
        spec.key_columns,
        sample_size,
        descending=False,
        ch_timezone=ch_timezone,
    )
    first_ok, first_msg = compare_row_lists("First rows", spec, columns, first_bq, first_ch, options)

    last_bq = fetch_bq_sample(
        bq,
        spec,
        columns,
        spec.key_columns,
        sample_size,
        descending=True,
        use_latest_per_key=use_latest_per_key,
    )
    last_ch = fetch_ch_sample(
        ch,
        spec,
        table_info,
        columns,
        spec.key_columns,
        sample_size,
        descending=True,
        ch_timezone=ch_timezone,
    )
    last_ok, last_msg = compare_row_lists("Last rows", spec, columns, last_bq, last_ch, options)

    return first_ok and last_ok, [first_msg, last_msg]


def strict_compare(
    bq: bigquery.Client,
    ch: Client,
    spec: TableSpec,
    table_info: ChTableInfo,
    columns: Sequence[str],
    page_size: int,
    max_mismatches: int,
    options: CompareOptions,
    *,
    ch_timezone: dt.tzinfo,
    use_latest_per_key: bool,
) -> tuple[bool, int, list[str]]:
    bq_iter = iter_bq_rows(
        bq,
        spec,
        columns,
        spec.key_columns,
        page_size,
        use_latest_per_key=use_latest_per_key,
    )
    ch_iter = iter_ch_rows(
        ch,
        spec,
        table_info,
        columns,
        spec.key_columns,
        page_size,
        ch_timezone=ch_timezone,
    )

    compared = 0
    mismatches: list[str] = []

    while True:
        bq_row = next(bq_iter, None)
        ch_row = next(ch_iter, None)

        if bq_row is None and ch_row is None:
            return not mismatches, compared, mismatches

        if bq_row is None or ch_row is None:
            mismatches.append(f"row stream length mismatch after {compared:,} rows")
            return False, compared, mismatches

        compared += 1
        diffs = row_differences(columns, bq_row, ch_row, options)
        if diffs:
            key = format_key(spec, bq_row)
            mismatches.append(
                f"row {compared:,} key {key}; {summarize_differences(diffs)}"
            )
            if len(mismatches) >= max_mismatches:
                return False, compared, mismatches


def resolve_tables(requested: Sequence[str]) -> list[TableSpec]:
    if not requested:
        return list(TABLE_SPECS)

    by_name = {spec.name: spec for spec in TABLE_SPECS}
    resolved = []
    for name in requested:
        if name not in by_name:
            valid = ", ".join(sorted(by_name))
            raise SystemExit(f"Unknown table '{name}'. Valid values: {valid}")
        resolved.append(by_name[name])
    return resolved


def print_table_header(spec: TableSpec, table_info: ChTableInfo):
    compare_mode = "FINAL" if table_info.use_final else "raw table"
    print(f"\n=== {spec.name} ===")
    print(f"  BQ: {BQ_PROJECT}.{BQ_DATASET}.{spec.bq_table}")
    print(f"  CH: {CH_DATABASE}.{spec.ch_table}")
    print(f"  CH engine: {table_info.engine} (comparison view: {compare_mode})")


def format_percent_delta(base: int, delta: int) -> str:
    if base == 0:
        return "n/a"
    return f"{(delta / base) * 100:+.3f}%"


def main() -> int:
    parser = argparse.ArgumentParser(description="Check BigQuery vs ClickHouse sync.")
    parser.add_argument(
        "--mode",
        choices=("summary", "strict"),
        default="summary",
        help="summary: counts + samples, strict: full-table compare with tolerances",
    )
    parser.add_argument(
        "--tables",
        nargs="*",
        default=[],
        help="Subset of logical table names to compare",
    )
    parser.add_argument(
        "--sample-size",
        type=int,
        default=10,
        help="Number of first and last rows to compare in summary mode",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=10_000,
        help="Streaming page size for strict mode",
    )
    parser.add_argument(
        "--show-mismatches",
        type=int,
        default=5,
        help="Maximum number of mismatch lines to print in strict mode",
    )
    parser.add_argument(
        "--float-rel-tol",
        type=float,
        default=DEFAULT_FLOAT_REL_TOL,
        help="Relative tolerance for float comparisons",
    )
    parser.add_argument(
        "--float-abs-tol",
        type=float,
        default=DEFAULT_FLOAT_ABS_TOL,
        help="Absolute tolerance for float comparisons",
    )
    parser.add_argument(
        "--timestamp-tol-ms",
        type=float,
        default=DEFAULT_TIMESTAMP_TOL_MS,
        help="Tolerance in milliseconds for timestamp comparisons",
    )
    args = parser.parse_args()

    options = CompareOptions(
        float_rel_tol=args.float_rel_tol,
        float_abs_tol=args.float_abs_tol,
        timestamp_tol=dt.timedelta(milliseconds=args.timestamp_tol_ms),
    )

    bq = get_bigquery_client()
    ch = get_clickhouse_client()
    ch_timezone_name = get_ch_timezone_name(ch)
    ch_timezone = get_ch_timezone(ch_timezone_name)
    specs = resolve_tables(args.tables)

    failures = 0
    failed_tables: list[tuple[str, list[str]]] = []

    print("BigQuery vs ClickHouse parity check")
    print(f"  Mode: {args.mode}")
    print(f"  BigQuery: {BQ_PROJECT}.{BQ_DATASET}")
    print(f"  ClickHouse: {CH_HOST}:{CH_PORT}/{CH_DATABASE}")
    print(f"  ClickHouse server timezone: {ch_timezone_name}")
    print(f"  Float tolerance: rel={args.float_rel_tol:g} abs={args.float_abs_tol:g}")
    print(f"  Timestamp tolerance: {args.timestamp_tol_ms:g} ms")

    for spec in specs:
        table_ch = ch if args.mode == "summary" else get_clickhouse_client()
        table_info = get_ch_table_info(table_ch, spec.ch_table)
        print_table_header(spec, table_info)

        try:
            bq_columns = get_bq_columns(bq, spec.bq_table)
            ch_columns = get_ch_columns(table_ch, spec.ch_table)
            columns = shared_columns(bq_columns, ch_columns)
            table_issues: list[str] = []
            use_latest_per_key = bq_compare_uses_latest_per_key(spec, table_info)

            if not columns:
                print("  FAIL: no shared columns between BigQuery and ClickHouse")
                failures += 1
                failed_tables.append((spec.name, ["shared_schema"]))
                continue

            missing_keys = [column for column in spec.key_columns if column not in columns]
            if missing_keys:
                print(f"  FAIL: missing key columns in shared schema: {missing_keys}")
                failures += 1
                failed_tables.append((spec.name, ["missing_keys"]))
                continue

            bq_raw_count = get_row_count_bq(bq, spec.bq_table)
            bq_compare_count = get_row_count_bq_compare(
                bq,
                spec,
                use_latest_per_key=use_latest_per_key,
            )
            if use_latest_per_key:
                print(
                    "  BQ row view: "
                    f"raw={bq_raw_count:,} compare={bq_compare_count:,} "
                    f"dedup_delta={bq_raw_count - bq_compare_count:+,} "
                    f"(latest row per key by {spec.watermark_column})"
                )

            raw_ch_count = get_row_count_ch(table_ch, spec, table_info, use_final=False)
            effective_ch_count = get_row_count_ch(
                table_ch,
                spec,
                table_info,
                use_final=table_info.use_final,
            )
            if table_info.use_final:
                print(
                    "  CH row view: "
                    f"raw={raw_ch_count:,} compare={effective_ch_count:,} "
                    f"dedup_delta={raw_ch_count - effective_ch_count:+,}"
                )

            count_delta = effective_ch_count - bq_compare_count
            count_ok = bq_compare_count == effective_ch_count
            bq_count_label = "BQ(compare)" if use_latest_per_key else "BQ"
            ch_count_label = "CH(compare)" if table_info.use_final else "CH"
            print(
                f"  Row count: {bq_count_label}={bq_compare_count:,} {ch_count_label}={effective_ch_count:,} "
                f"delta={count_delta:+,} ({format_percent_delta(bq_compare_count, count_delta)}) "
                f"{'OK' if count_ok else 'MISMATCH'}"
            )
            if not count_ok:
                failures += 1
                table_issues.append("row_count")

            if spec.watermark_column and spec.watermark_column in columns:
                bq_max = get_watermark_bq(bq, spec.bq_table, spec.watermark_column)
                ch_max = get_watermark_ch(
                    table_ch,
                    spec,
                    table_info,
                    spec.watermark_column,
                    ch_timezone=ch_timezone,
                )
                watermark_ok = values_equal(bq_max, ch_max, options)
                print(
                    f"  Watermark ({spec.watermark_column}): "
                    f"{'OK' if watermark_ok else 'MISMATCH'} | "
                    f"{describe_value_difference(bq_max, ch_max)}"
                )
                if not watermark_ok:
                    failures += 1
                    table_issues.append(f"watermark:{spec.watermark_column}")

            if args.mode == "summary":
                sample_ok, sample_lines = compare_samples(
                    bq=bq,
                    ch=table_ch,
                    spec=spec,
                    table_info=table_info,
                    columns=columns,
                    sample_size=args.sample_size,
                    options=options,
                    ch_timezone=ch_timezone,
                    use_latest_per_key=use_latest_per_key,
                )
                for line in sample_lines:
                    print(f"  {line}")
                if not sample_ok:
                    failures += 1
                    table_issues.append("samples")
            else:
                strict_ok, compared, mismatch_lines = strict_compare(
                    bq=bq,
                    ch=table_ch,
                    spec=spec,
                    table_info=table_info,
                    columns=columns,
                    page_size=args.page_size,
                    max_mismatches=args.show_mismatches,
                    options=options,
                    ch_timezone=ch_timezone,
                    use_latest_per_key=use_latest_per_key,
                )
                print(f"  Strict compare rows checked: {compared:,}")
                if strict_ok:
                    print("  Strict compare: exact match within configured tolerances")
                else:
                    print("  Strict compare: mismatch")
                    for line in mismatch_lines:
                        print(f"    - {line}")
                    failures += 1
                    table_issues.append("strict")

            if table_issues:
                print(f"  Status: FAIL ({', '.join(table_issues)})")
                failed_tables.append((spec.name, table_issues))
            else:
                print("  Status: PASS")
        finally:
            if table_ch is not ch:
                close_clickhouse_client(table_ch)

    print()
    if failures:
        print(f"Completed with {failures} mismatched check(s).")
        print("Failed tables:")
        for table_name, issues in failed_tables:
            print(f"  - {table_name}: {', '.join(issues)}")
        return 1

    print("All requested checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
