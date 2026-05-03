#!/usr/bin/env python3
"""Safely replay a BigQuery date range into ClickHouse for user_video_relation."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Sequence

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_BQ_PROJECT = "hot-or-not-feed-intelligence"
DEFAULT_BQ_DATASET = "yral_ds"
DEFAULT_SSH_USER = "ansuman"
DEFAULT_SSH_TARGET = "ansuman-1"
DEFAULT_CLICKHOUSE_DATABASE = "yral"
DEFAULT_TABLE = "user_video_relation"
DEFAULT_START = "2026-04-01T00:00:00+00:00"
DEFAULT_END = "2026-05-01T00:00:00+00:00"


class BackfillError(RuntimeError):
    """Raised when the range replay cannot complete."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Replay a BigQuery time range into ClickHouse for yral.user_video_relation "
            "without deleting existing ClickHouse rows."
        )
    )
    parser.add_argument(
        "--env-file",
        default=str(DEFAULT_ENV_PATH),
        help=f"dotenv file to load before reading SERVICE_CRED (default: {DEFAULT_ENV_PATH})",
    )
    parser.add_argument(
        "--bq-project",
        default=DEFAULT_BQ_PROJECT,
        help=f"BigQuery project id (default: {DEFAULT_BQ_PROJECT})",
    )
    parser.add_argument(
        "--bq-dataset",
        default=DEFAULT_BQ_DATASET,
        help=f"BigQuery dataset name (default: {DEFAULT_BQ_DATASET})",
    )
    parser.add_argument(
        "--ssh-user",
        default=DEFAULT_SSH_USER,
        help=f"SSH user for the ClickHouse node (default: {DEFAULT_SSH_USER})",
    )
    parser.add_argument(
        "--ssh-target",
        default=DEFAULT_SSH_TARGET,
        help=f"SSH target for the ClickHouse node (default: {DEFAULT_SSH_TARGET})",
    )
    parser.add_argument(
        "--ssh-port",
        type=int,
        default=None,
        help="Optional SSH port override",
    )
    parser.add_argument(
        "--clickhouse-database",
        default=DEFAULT_CLICKHOUSE_DATABASE,
        help=f"ClickHouse database name (default: {DEFAULT_CLICKHOUSE_DATABASE})",
    )
    parser.add_argument(
        "--start",
        default=DEFAULT_START,
        help=f"UTC start timestamp, inclusive (default: {DEFAULT_START})",
    )
    parser.add_argument(
        "--end",
        default=DEFAULT_END,
        help=f"UTC end timestamp, exclusive (default: {DEFAULT_END})",
    )
    parser.add_argument(
        "--page-size",
        type=int,
        default=5000,
        help="BigQuery page size when streaming rows (default: 5000)",
    )
    parser.add_argument(
        "--execute",
        action="store_true",
        help="Actually perform the ClickHouse insert. Without this flag the script is dry-run only.",
    )
    return parser.parse_args()


def load_environment(env_file: str) -> None:
    env_path = Path(env_file).expanduser()
    if env_path.exists():
        load_dotenv(env_path, override=False)


def parse_utc_timestamp(value: str) -> datetime:
    candidate = value.strip().replace("Z", "+00:00")
    dt = datetime.fromisoformat(candidate)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def build_bigquery_client(project_id: str) -> bigquery.Client:
    service_cred = os.getenv("SERVICE_CRED")
    if not service_cred:
        raise BackfillError("SERVICE_CRED is not set after loading the environment")

    try:
        credentials_info = json.loads(service_cred)
    except json.JSONDecodeError:
        credential_path = Path(service_cred).expanduser()
        if not credential_path.exists():
            raise BackfillError(
                "SERVICE_CRED must contain valid JSON or a path to a service-account JSON file"
            ) from None
        credentials_info = json.loads(credential_path.read_text(encoding="utf-8"))

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(project=project_id, credentials=credentials)


def format_clickhouse_timestamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


def run_ssh_command(
    ssh_user: str,
    ssh_target: str,
    ssh_port: int | None,
    remote_argv: Sequence[str],
) -> str:
    ssh_cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=15",
    ]
    if ssh_port is not None:
        ssh_cmd.extend(["-p", str(ssh_port)])
    ssh_cmd.append(f"{ssh_user}@{ssh_target}")
    ssh_cmd.append(shlex.join(remote_argv))

    try:
        completed = subprocess.run(
            ssh_cmd,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        message = exc.stderr.strip() or exc.stdout.strip() or str(exc)
        raise BackfillError(
            f"SSH command failed for {ssh_user}@{ssh_target}: {message}"
        ) from exc

    return completed.stdout


def fetch_bigquery_range_stats(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    start: datetime,
    end: datetime,
) -> tuple[int, int]:
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", end),
        ]
    )
    query = f"""
    SELECT
      COUNT(*) AS raw_count,
      COUNT(DISTINCT TO_JSON_STRING(STRUCT(user_id, video_id))) AS logical_count
    FROM `{project_id}.{dataset}.userVideoRelation`
    WHERE last_watched_timestamp >= @start_ts
      AND last_watched_timestamp < @end_ts
    """
    row = next(iter(client.query(query, job_config=job_config).result()))
    return int(row["raw_count"]), int(row["logical_count"])


def fetch_clickhouse_range_stats(
    ssh_user: str,
    ssh_target: str,
    ssh_port: int | None,
    database: str,
    table: str,
    start: datetime,
    end: datetime,
) -> tuple[int, int]:
    start_ch = format_clickhouse_timestamp(start)
    end_ch = format_clickhouse_timestamp(end)
    where_clause = (
        f"last_watched_timestamp >= toDateTime64('{start_ch}', 3, 'UTC') "
        f"AND last_watched_timestamp < toDateTime64('{end_ch}', 3, 'UTC')"
    )
    sql = (
        "SELECT "
        f"(SELECT count() FROM {database}.{table} WHERE {where_clause}) AS raw_count, "
        f"(SELECT count() FROM {database}.{table} FINAL WHERE {where_clause}) AS logical_count "
        "FORMAT TSVRaw"
    )
    output = run_ssh_command(
        ssh_user,
        ssh_target,
        ssh_port,
        ["clickhouse-client", "--query", sql],
    ).strip()
    raw_count, logical_count = output.split("\t", 1)
    return int(raw_count), int(logical_count)


def build_backfill_query(project_id: str, dataset: str) -> str:
    return f"""
    WITH ranked AS (
      SELECT
        user_id,
        video_id,
        last_watched_timestamp,
        mean_percentage_watched,
        last_liked_timestamp,
        liked,
        last_shared_timestamp,
        shared,
        ROW_NUMBER() OVER (
          PARTITION BY user_id, video_id
          ORDER BY
            last_watched_timestamp DESC,
            COALESCE(last_liked_timestamp, TIMESTAMP('1970-01-01 00:00:00+00')) DESC,
            CAST(liked AS INT64) DESC,
            COALESCE(last_shared_timestamp, TIMESTAMP('1970-01-01 00:00:00+00')) DESC,
            CAST(shared AS INT64) DESC
        ) AS rn
      FROM `{project_id}.{dataset}.userVideoRelation`
      WHERE last_watched_timestamp >= @start_ts
        AND last_watched_timestamp < @end_ts
    )
    SELECT
      user_id,
      video_id,
      last_watched_timestamp,
      mean_percentage_watched,
      last_liked_timestamp,
      liked,
      last_shared_timestamp,
      shared
    FROM ranked
    WHERE rn = 1
    ORDER BY last_watched_timestamp, user_id, video_id
    """


def start_clickhouse_insert(
    ssh_user: str,
    ssh_target: str,
    ssh_port: int | None,
    database: str,
    table: str,
) -> subprocess.Popen[str]:
    insert_sql = (
        f"INSERT INTO {database}.{table} "
        "(user_id, video_id, last_watched_timestamp, mean_percentage_watched, "
        "last_liked_timestamp, liked, last_shared_timestamp, shared) "
        "FORMAT TabSeparated"
    )
    ssh_cmd = [
        "ssh",
        "-o",
        "BatchMode=yes",
        "-o",
        "StrictHostKeyChecking=accept-new",
        "-o",
        "ConnectTimeout=15",
    ]
    if ssh_port is not None:
        ssh_cmd.extend(["-p", str(ssh_port)])
    ssh_cmd.append(f"{ssh_user}@{ssh_target}")
    ssh_cmd.append(shlex.join(["clickhouse-client", "--query", insert_sql]))
    return subprocess.Popen(
        ssh_cmd,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )


def escape_tsv(value: object) -> str:
    if value is None:
        return r"\N"
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, datetime):
        return format_clickhouse_timestamp(value)
    if isinstance(value, float):
        return repr(value)
    text = str(value)
    return (
        text.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
    )


def row_to_tsv(row: bigquery.table.Row) -> str:
    values = [
        row["user_id"],
        row["video_id"],
        row["last_watched_timestamp"],
        row["mean_percentage_watched"],
        row["last_liked_timestamp"],
        row["liked"],
        row["last_shared_timestamp"],
        row["shared"],
    ]
    return "\t".join(escape_tsv(value) for value in values) + "\n"


def stream_backfill_rows(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    start: datetime,
    end: datetime,
    page_size: int,
    insert_process: subprocess.Popen[str],
) -> int:
    if insert_process.stdin is None:
        raise BackfillError("ClickHouse insert process did not expose stdin")

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", start),
            bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", end),
        ]
    )
    query = build_backfill_query(project_id, dataset)
    rows = client.query(query, job_config=job_config).result(page_size=page_size)

    streamed_rows = 0
    try:
        for row in rows:
            insert_process.stdin.write(row_to_tsv(row))
            streamed_rows += 1
        insert_process.stdin.close()
    except Exception:
        insert_process.kill()
        raise

    return_code = insert_process.wait()
    stdout = ""
    stderr = ""
    if insert_process.stdout is not None:
        stdout = insert_process.stdout.read()
    if insert_process.stderr is not None:
        stderr = insert_process.stderr.read()
    if return_code != 0:
        message = stderr.strip() or stdout.strip() or f"exit code {return_code}"
        raise BackfillError(f"ClickHouse insert failed: {message}")
    return streamed_rows


def print_range_stats(
    label: str,
    bq_raw: int,
    bq_logical: int,
    ch_raw: int,
    ch_logical: int,
) -> None:
    print(label)
    print(f"  BigQuery raw:      {bq_raw}")
    print(f"  BigQuery logical:  {bq_logical}")
    print(f"  ClickHouse raw:    {ch_raw}")
    print(f"  ClickHouse logical:{ch_logical}")
    print(f"  Logical gap:       {bq_logical - ch_logical}")


def main() -> int:
    args = parse_args()
    load_environment(args.env_file)

    start = parse_utc_timestamp(args.start)
    end = parse_utc_timestamp(args.end)
    if end <= start:
        raise BackfillError("--end must be after --start")

    client = build_bigquery_client(args.bq_project)
    pre_bq_raw, pre_bq_logical = fetch_bigquery_range_stats(
        client, args.bq_project, args.bq_dataset, start, end
    )
    pre_ch_raw, pre_ch_logical = fetch_clickhouse_range_stats(
        ssh_user=args.ssh_user,
        ssh_target=args.ssh_target,
        ssh_port=args.ssh_port,
        database=args.clickhouse_database,
        table=DEFAULT_TABLE,
        start=start,
        end=end,
    )
    print_range_stats("Pre-backfill stats", pre_bq_raw, pre_bq_logical, pre_ch_raw, pre_ch_logical)

    if not args.execute:
        print("")
        print("Dry run only. Re-run with --execute to stream the deduped BigQuery range into ClickHouse.")
        return 0

    print("")
    print(
        f"Replaying deduped BigQuery rows for {start.isoformat()} to {end.isoformat()} "
        f"into {args.clickhouse_database}.{DEFAULT_TABLE} on {args.ssh_user}@{args.ssh_target}."
    )

    insert_process = start_clickhouse_insert(
        ssh_user=args.ssh_user,
        ssh_target=args.ssh_target,
        ssh_port=args.ssh_port,
        database=args.clickhouse_database,
        table=DEFAULT_TABLE,
    )
    streamed_rows = stream_backfill_rows(
        client=client,
        project_id=args.bq_project,
        dataset=args.bq_dataset,
        start=start,
        end=end,
        page_size=args.page_size,
        insert_process=insert_process,
    )
    print(f"Streamed {streamed_rows} deduped rows into ClickHouse.")

    post_ch_raw, post_ch_logical = fetch_clickhouse_range_stats(
        ssh_user=args.ssh_user,
        ssh_target=args.ssh_target,
        ssh_port=args.ssh_port,
        database=args.clickhouse_database,
        table=DEFAULT_TABLE,
        start=start,
        end=end,
    )
    print("")
    print_range_stats(
        "Post-backfill stats",
        pre_bq_raw,
        pre_bq_logical,
        post_ch_raw,
        post_ch_logical,
    )
    return 0 if pre_bq_logical == post_ch_logical else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except BackfillError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
