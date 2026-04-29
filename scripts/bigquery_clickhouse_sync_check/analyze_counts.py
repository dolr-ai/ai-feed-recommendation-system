#!/usr/bin/env python3
"""Analyze BigQuery vs ClickHouse counts with dedup-aware parity."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence, Tuple

from dotenv import load_dotenv
from google.cloud import bigquery
from google.oauth2 import service_account

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_ENV_PATH = REPO_ROOT / ".env"
DEFAULT_BQ_PROJECT = "hot-or-not-feed-intelligence"
DEFAULT_BQ_DATASET = "yral_ds"
DEFAULT_CLICKHOUSE_DATABASE = "yral"
DEFAULT_SSH_USER = "ansuman"
DEFAULT_SSH_TARGET = "ansuman-1"


@dataclass(frozen=True)
class TableSpec:
    bigquery_table: str
    clickhouse_table: str
    dedup_key: Tuple[str, ...]


@dataclass(frozen=True)
class ClickHouseTableMetadata:
    engine: str
    sorting_key: str


@dataclass
class TableComparison:
    bigquery_table: str
    clickhouse_table: str
    dedup_key: Tuple[str, ...]
    clickhouse_engine: str | None
    clickhouse_sorting_key: str | None
    bigquery_raw_count: int | None
    bigquery_logical_count: int | None
    bigquery_duplicate_rows: int | None
    clickhouse_raw_count: int | None
    clickhouse_logical_count: int | None
    clickhouse_duplicate_rows: int | None
    raw_delta: int | None
    logical_delta: int | None
    logical_delta_ratio: float | None
    status_basis: str
    status: str
    note: str = ""


class SyncCheckError(RuntimeError):
    """Raised when the parity check cannot complete."""


TABLE_SPECS: Dict[str, TableSpec] = {
    spec.bigquery_table: spec
    for spec in [
        TableSpec("ai_ugc", "ai_ugc", ("video_id",)),
        TableSpec("bot_uploaded_content", "bot_uploaded_content", ("video_id",)),
        TableSpec("excluded_videos", "excluded_videos", ("video_id",)),
        TableSpec("follower_graph", "follower_graph", ("follower_id", "following_id")),
        TableSpec("global_popular_videos_l7d", "global_popular_videos_l7d", ("video_id",)),
        TableSpec("ugc_content_approval", "ugc_content_approval", ("video_id",)),
        TableSpec("userVideoRelation", "user_video_relation", ("user_id", "video_id")),
        TableSpec("video_statistics", "video_statistics", ("video_id",)),
        TableSpec("video_unique_v2", "video_unique_v2", ("video_id",)),
    ]
}
CLICKHOUSE_TO_BIGQUERY = {
    spec.clickhouse_table: spec.bigquery_table for spec in TABLE_SPECS.values()
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare migrated yral tables between BigQuery and self-hosted "
            "ClickHouse, accounting for dedup semantics."
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
        "--clickhouse-database",
        default=DEFAULT_CLICKHOUSE_DATABASE,
        help=f"ClickHouse database name (default: {DEFAULT_CLICKHOUSE_DATABASE})",
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
        "--table",
        dest="tables",
        action="append",
        default=[],
        help=(
            "Optional table selector. Repeat the flag or pass a comma-separated list. "
            "Accepts either BigQuery table names or ClickHouse table names."
        ),
    )
    parser.add_argument(
        "--max-absolute-delta",
        type=int,
        default=0,
        help=(
            "Allowed logical row-count delta before marking a mismatch. "
            "Logical means distinct dedup-key count in BigQuery versus FINAL in ClickHouse. "
            "Default: 0"
        ),
    )
    parser.add_argument(
        "--max-delta-ratio",
        type=float,
        default=0.0,
        help=(
            "Allowed logical delta ratio before marking a mismatch. Ratio is "
            "abs(logical_delta) / max(bigquery_logical_count, 1). Default: 0.0"
        ),
    )
    parser.add_argument(
        "--output",
        choices=("table", "json"),
        default="table",
        help="Output format (default: table)",
    )
    return parser.parse_args()


def load_environment(env_file: str) -> None:
    env_path = Path(env_file).expanduser()
    if env_path.exists():
        load_dotenv(env_path, override=False)


def build_bigquery_client(project_id: str) -> bigquery.Client:
    service_cred = os.getenv("SERVICE_CRED")
    if not service_cred:
        raise SyncCheckError("SERVICE_CRED is not set after loading the environment")

    try:
        credentials_info = json.loads(service_cred)
    except json.JSONDecodeError:
        credential_path = Path(service_cred).expanduser()
        if not credential_path.exists():
            raise SyncCheckError(
                "SERVICE_CRED must contain valid JSON or a path to a service-account JSON file"
            ) from None
        credentials_info = json.loads(credential_path.read_text(encoding="utf-8"))

    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    return bigquery.Client(project=project_id, credentials=credentials)


def normalize_requested_tables(requested: Sequence[str]) -> List[str]:
    normalized: List[str] = []
    for item in requested:
        for token in item.split(","):
            name = token.strip()
            if name:
                normalized.append(name)
    return normalized


def resolve_table_specs(requested_tables: Sequence[str]) -> List[TableSpec]:
    if not requested_tables:
        return list(TABLE_SPECS.values())

    specs: List[TableSpec] = []
    seen: set[tuple[str, str]] = set()
    for table_name in requested_tables:
        if table_name in TABLE_SPECS:
            spec = TABLE_SPECS[table_name]
        elif table_name in CLICKHOUSE_TO_BIGQUERY:
            spec = TABLE_SPECS[CLICKHOUSE_TO_BIGQUERY[table_name]]
        else:
            raise SyncCheckError(
                f"Unknown table selector '{table_name}'. "
                "Use a mapped BigQuery or ClickHouse table name."
            )
        pair = (spec.bigquery_table, spec.clickhouse_table)
        if pair not in seen:
            seen.add(pair)
            specs.append(spec)
    return specs


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
        raise SyncCheckError(
            f"SSH command failed for {ssh_user}@{ssh_target}: {message}"
        ) from exc

    return completed.stdout


def fetch_clickhouse_metadata(
    ssh_user: str,
    ssh_target: str,
    ssh_port: int | None,
    database: str,
) -> Dict[str, ClickHouseTableMetadata]:
    sql = (
        "SELECT name, engine, sorting_key "
        "FROM system.tables "
        f"WHERE database = '{database}' "
        "ORDER BY name FORMAT TSVRaw"
    )
    output = run_ssh_command(
        ssh_user,
        ssh_target,
        ssh_port,
        ["clickhouse-client", "--query", sql],
    )
    metadata: Dict[str, ClickHouseTableMetadata] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) != 3:
            raise SyncCheckError(f"Unexpected ClickHouse metadata row: {line}")
        name, engine, sorting_key = parts
        metadata[name] = ClickHouseTableMetadata(engine=engine, sorting_key=sorting_key)
    return metadata


def fetch_bigquery_table_names(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
) -> set[str]:
    return {
        table.table_id
        for table in client.list_tables(f"{project_id}.{dataset}")
    }


def build_bigquery_key_expr(columns: Sequence[str]) -> str:
    quoted = ", ".join(f"`{column}`" for column in columns)
    return f"TO_JSON_STRING(STRUCT({quoted}))"


def build_bigquery_stats_query(
    project_id: str,
    dataset: str,
    specs: Iterable[TableSpec],
) -> str:
    statements = []
    for spec in specs:
        key_expr = build_bigquery_key_expr(spec.dedup_key)
        statements.append(
            "SELECT "
            f"'{spec.bigquery_table}' AS table_name, "
            "COUNT(*) AS raw_count, "
            f"COUNT(DISTINCT {key_expr}) AS logical_count "
            f"FROM `{project_id}.{dataset}.{spec.bigquery_table}`"
        )
    return "\nUNION ALL\n".join(statements)


def fetch_bigquery_stats(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    specs: Sequence[TableSpec],
) -> Dict[str, Dict[str, int]]:
    if not specs:
        return {}

    query_job = client.query(build_bigquery_stats_query(project_id, dataset, specs))
    rows = query_job.result()
    return {
        row["table_name"]: {
            "raw_count": int(row["raw_count"]),
            "logical_count": int(row["logical_count"]),
        }
        for row in rows
    }


def count_requires_final(engine: str | None) -> bool:
    return bool(engine) and "ReplacingMergeTree" in engine


def build_clickhouse_stats_query(
    database: str,
    specs: Sequence[TableSpec],
    clickhouse_metadata: Dict[str, ClickHouseTableMetadata],
) -> str:
    statements = []
    for spec in specs:
        metadata = clickhouse_metadata[spec.clickhouse_table]
        raw_select = f"(SELECT count() FROM {database}.{spec.clickhouse_table})"
        if count_requires_final(metadata.engine):
            logical_select = f"(SELECT count() FROM {database}.{spec.clickhouse_table} FINAL)"
        else:
            logical_select = raw_select
        statements.append(
            "SELECT "
            f"'{spec.clickhouse_table}' AS table_name, "
            f"{raw_select} AS raw_count, "
            f"{logical_select} AS logical_count"
        )
    union_query = "\nUNION ALL\n".join(statements)
    return f"SELECT * FROM (\n{union_query}\n)\nORDER BY table_name FORMAT TSVRaw"


def fetch_clickhouse_stats(
    ssh_user: str,
    ssh_target: str,
    ssh_port: int | None,
    database: str,
    specs: Sequence[TableSpec],
    clickhouse_metadata: Dict[str, ClickHouseTableMetadata],
) -> Dict[str, Dict[str, int]]:
    if not specs:
        return {}

    sql = build_clickhouse_stats_query(database, specs, clickhouse_metadata)
    output = run_ssh_command(
        ssh_user,
        ssh_target,
        ssh_port,
        ["clickhouse-client", "--query", sql],
    )
    stats: Dict[str, Dict[str, int]] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        table_name, raw_count, logical_count = line.split("\t", 2)
        stats[table_name] = {
            "raw_count": int(raw_count),
            "logical_count": int(logical_count),
        }
    return stats


def calculate_delta_ratio(logical_count: int, logical_delta: int) -> float:
    return abs(logical_delta) / max(logical_count, 1)


def build_note(
    uses_logical_status: bool,
    logical_delta: int,
    raw_delta: int,
    bigquery_duplicate_rows: int,
    clickhouse_duplicate_rows: int,
) -> str:
    duplicate_context = (
        f"bigquery_duplicate_rows={bigquery_duplicate_rows}, "
        f"clickhouse_pending_duplicate_rows={clickhouse_duplicate_rows}"
    )

    if not uses_logical_status:
        if raw_delta == 0:
            if bigquery_duplicate_rows == 0 and clickhouse_duplicate_rows == 0:
                return "Raw counts match and this table is not deduped by ClickHouse."
            return (
                "Raw counts match. Repeated dedup keys exist, but this table is "
                f"not deduped by ClickHouse by design: {duplicate_context}."
            )
        if raw_delta > 0:
            return (
                f"BigQuery has {raw_delta} more raw rows and this table is not deduped by "
                f"ClickHouse. Duplicate context: {duplicate_context}."
            )
        return (
            f"ClickHouse has {-raw_delta} more raw rows and this table is not deduped by "
            f"design. Duplicate context: {duplicate_context}."
        )

    if logical_delta == 0:
        if raw_delta == 0 and bigquery_duplicate_rows == 0 and clickhouse_duplicate_rows == 0:
            return "Raw and logical counts match."
        if raw_delta == 0:
            return f"Logical counts match. Duplicate pressure exists but cancels out: {duplicate_context}."
        return f"Logical counts match. Raw delta is explained by duplicate pressure: {duplicate_context}."

    if logical_delta > 0:
        return (
            f"BigQuery has {logical_delta} more logical rows after dedup. "
            f"Duplicate context: {duplicate_context}."
        )

    return (
        f"ClickHouse has {-logical_delta} more logical rows after dedup. "
        f"Duplicate context: {duplicate_context}."
    )


def compare_stats(
    requested_specs: Sequence[TableSpec],
    bigquery_tables: set[str],
    clickhouse_metadata: Dict[str, ClickHouseTableMetadata],
    bigquery_stats: Dict[str, Dict[str, int]],
    clickhouse_stats: Dict[str, Dict[str, int]],
    max_absolute_delta: int,
    max_delta_ratio: float,
) -> List[TableComparison]:
    results: List[TableComparison] = []

    for spec in requested_specs:
        bq_exists = spec.bigquery_table in bigquery_tables
        ch_exists = spec.clickhouse_table in clickhouse_metadata
        metadata = clickhouse_metadata.get(spec.clickhouse_table)

        if not bq_exists and not ch_exists:
            results.append(
                TableComparison(
                    bigquery_table=spec.bigquery_table,
                    clickhouse_table=spec.clickhouse_table,
                    dedup_key=spec.dedup_key,
                    clickhouse_engine=None,
                    clickhouse_sorting_key=None,
                    bigquery_raw_count=None,
                    bigquery_logical_count=None,
                    bigquery_duplicate_rows=None,
                    clickhouse_raw_count=None,
                    clickhouse_logical_count=None,
                    clickhouse_duplicate_rows=None,
                    raw_delta=None,
                    logical_delta=None,
                    logical_delta_ratio=None,
                    status_basis="logical",
                    status="missing_both",
                    note="Table is missing in both systems.",
                )
            )
            continue

        if not bq_exists:
            ch_stats = clickhouse_stats.get(spec.clickhouse_table, {})
            results.append(
                TableComparison(
                    bigquery_table=spec.bigquery_table,
                    clickhouse_table=spec.clickhouse_table,
                    dedup_key=spec.dedup_key,
                    clickhouse_engine=metadata.engine if metadata else None,
                    clickhouse_sorting_key=metadata.sorting_key if metadata else None,
                    bigquery_raw_count=None,
                    bigquery_logical_count=None,
                    bigquery_duplicate_rows=None,
                    clickhouse_raw_count=ch_stats.get("raw_count"),
                    clickhouse_logical_count=ch_stats.get("logical_count"),
                    clickhouse_duplicate_rows=None
                    if "raw_count" not in ch_stats or "logical_count" not in ch_stats
                    else ch_stats["raw_count"] - ch_stats["logical_count"],
                    raw_delta=None,
                    logical_delta=None,
                    logical_delta_ratio=None,
                    status_basis="logical",
                    status="missing_bigquery",
                    note="Table missing in BigQuery dataset.",
                )
            )
            continue

        if not ch_exists:
            bq_stats = bigquery_stats.get(spec.bigquery_table, {})
            results.append(
                TableComparison(
                    bigquery_table=spec.bigquery_table,
                    clickhouse_table=spec.clickhouse_table,
                    dedup_key=spec.dedup_key,
                    clickhouse_engine=None,
                    clickhouse_sorting_key=None,
                    bigquery_raw_count=bq_stats.get("raw_count"),
                    bigquery_logical_count=bq_stats.get("logical_count"),
                    bigquery_duplicate_rows=None
                    if "raw_count" not in bq_stats or "logical_count" not in bq_stats
                    else bq_stats["raw_count"] - bq_stats["logical_count"],
                    clickhouse_raw_count=None,
                    clickhouse_logical_count=None,
                    clickhouse_duplicate_rows=None,
                    raw_delta=None,
                    logical_delta=None,
                    logical_delta_ratio=None,
                    status_basis="logical",
                    status="missing_clickhouse",
                    note="Table missing in ClickHouse database.",
                )
            )
            continue

        bq_stats = bigquery_stats[spec.bigquery_table]
        ch_stats = clickhouse_stats[spec.clickhouse_table]
        bigquery_raw_count = bq_stats["raw_count"]
        bigquery_logical_count = bq_stats["logical_count"]
        clickhouse_raw_count = ch_stats["raw_count"]
        clickhouse_logical_count = ch_stats["logical_count"]
        bigquery_duplicate_rows = bigquery_raw_count - bigquery_logical_count
        clickhouse_duplicate_rows = clickhouse_raw_count - clickhouse_logical_count
        raw_delta = bigquery_raw_count - clickhouse_raw_count
        logical_delta = bigquery_logical_count - clickhouse_logical_count
        logical_delta_ratio = calculate_delta_ratio(bigquery_logical_count, logical_delta)
        uses_logical_status = count_requires_final(metadata.engine)
        status_basis = "logical" if uses_logical_status else "raw"

        status = "ok"
        if uses_logical_status:
            delta_for_status = abs(logical_delta)
            ratio_for_status = logical_delta_ratio
        else:
            delta_for_status = abs(raw_delta)
            ratio_for_status = abs(raw_delta) / max(bigquery_raw_count, 1)
        if delta_for_status > max_absolute_delta or ratio_for_status > max_delta_ratio:
            status = "mismatch"

        results.append(
            TableComparison(
                bigquery_table=spec.bigquery_table,
                clickhouse_table=spec.clickhouse_table,
                dedup_key=spec.dedup_key,
                clickhouse_engine=metadata.engine,
                clickhouse_sorting_key=metadata.sorting_key,
                bigquery_raw_count=bigquery_raw_count,
                bigquery_logical_count=bigquery_logical_count,
                bigquery_duplicate_rows=bigquery_duplicate_rows,
                clickhouse_raw_count=clickhouse_raw_count,
                clickhouse_logical_count=clickhouse_logical_count,
                clickhouse_duplicate_rows=clickhouse_duplicate_rows,
                raw_delta=raw_delta,
                logical_delta=logical_delta,
                logical_delta_ratio=logical_delta_ratio,
                status_basis=status_basis,
                status=status,
                note=build_note(
                    uses_logical_status=uses_logical_status,
                    logical_delta=logical_delta,
                    raw_delta=raw_delta,
                    bigquery_duplicate_rows=bigquery_duplicate_rows,
                    clickhouse_duplicate_rows=clickhouse_duplicate_rows,
                ),
            )
        )

    return sorted(results, key=lambda item: item.clickhouse_table)


def render_table(results: Sequence[TableComparison]) -> str:
    headers = [
        "bigquery_table",
        "clickhouse_table",
        "dedup_key",
        "bq_raw",
        "bq_logical",
        "bq_dups",
        "ch_raw",
        "ch_logical",
        "ch_dups",
        "raw_delta",
        "logical_delta",
        "basis",
        "status",
    ]

    rows = []
    for result in results:
        rows.append(
            [
                result.bigquery_table,
                result.clickhouse_table,
                ",".join(result.dedup_key),
                "-" if result.bigquery_raw_count is None else str(result.bigquery_raw_count),
                "-" if result.bigquery_logical_count is None else str(result.bigquery_logical_count),
                "-" if result.bigquery_duplicate_rows is None else str(result.bigquery_duplicate_rows),
                "-" if result.clickhouse_raw_count is None else str(result.clickhouse_raw_count),
                "-" if result.clickhouse_logical_count is None else str(result.clickhouse_logical_count),
                "-" if result.clickhouse_duplicate_rows is None else str(result.clickhouse_duplicate_rows),
                "-" if result.raw_delta is None else str(result.raw_delta),
                "-" if result.logical_delta is None else str(result.logical_delta),
                result.status_basis,
                result.status,
            ]
        )

    widths = [
        max(len(header), *(len(row[index]) for row in rows)) if rows else len(header)
        for index, header in enumerate(headers)
    ]

    def format_row(values: Sequence[str]) -> str:
        return "  ".join(
            value.ljust(widths[index]) for index, value in enumerate(values)
        )

    lines = [format_row(headers), format_row(["-" * width for width in widths])]
    lines.extend(format_row(row) for row in rows)

    details = []
    for result in results:
        engine = result.clickhouse_engine or "-"
        sorting_key = result.clickhouse_sorting_key or "-"
        if result.note:
            details.append(
                f"{result.clickhouse_table}: engine={engine}; sorting_key={sorting_key}; {result.note}"
            )
    if details:
        lines.append("")
        lines.extend(details)

    return "\n".join(lines)


def summarize_results(results: Sequence[TableComparison]) -> Dict[str, int]:
    summary = {
        "ok": 0,
        "mismatch": 0,
        "missing_bigquery": 0,
        "missing_clickhouse": 0,
        "missing_both": 0,
    }
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    summary["total"] = len(results)
    return summary


def main() -> int:
    args = parse_args()
    load_environment(args.env_file)

    requested_tables = normalize_requested_tables(args.tables)
    requested_specs = resolve_table_specs(requested_tables)
    bigquery_client = build_bigquery_client(args.bq_project)
    bigquery_tables = fetch_bigquery_table_names(
        bigquery_client,
        args.bq_project,
        args.bq_dataset,
    )
    clickhouse_metadata = fetch_clickhouse_metadata(
        ssh_user=args.ssh_user,
        ssh_target=args.ssh_target,
        ssh_port=args.ssh_port,
        database=args.clickhouse_database,
    )

    comparable_specs = [
        spec
        for spec in requested_specs
        if spec.bigquery_table in bigquery_tables and spec.clickhouse_table in clickhouse_metadata
    ]

    bigquery_stats = fetch_bigquery_stats(
        bigquery_client,
        args.bq_project,
        args.bq_dataset,
        comparable_specs,
    )
    clickhouse_stats = fetch_clickhouse_stats(
        ssh_user=args.ssh_user,
        ssh_target=args.ssh_target,
        ssh_port=args.ssh_port,
        database=args.clickhouse_database,
        specs=comparable_specs,
        clickhouse_metadata=clickhouse_metadata,
    )

    results = compare_stats(
        requested_specs=requested_specs,
        bigquery_tables=bigquery_tables,
        clickhouse_metadata=clickhouse_metadata,
        bigquery_stats=bigquery_stats,
        clickhouse_stats=clickhouse_stats,
        max_absolute_delta=args.max_absolute_delta,
        max_delta_ratio=args.max_delta_ratio,
    )
    summary = summarize_results(results)

    if args.output == "json":
        payload = {
            "ssh_target": args.ssh_target,
            "clickhouse_database": args.clickhouse_database,
            "bigquery_project": args.bq_project,
            "bigquery_dataset": args.bq_dataset,
            "status_basis": (
                "ReplacingMergeTree tables use logical parity "
                "(BigQuery distinct dedup-key count versus ClickHouse FINAL); "
                "non-dedup tables use raw parity"
            ),
            "summary": summary,
            "results": [asdict(result) for result in results],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"BigQuery dataset {args.bq_project}.{args.bq_dataset} vs "
            f"ClickHouse {args.clickhouse_database} on {args.ssh_user}@{args.ssh_target}"
        )
        print(
            "Status basis: ReplacingMergeTree tables use BigQuery distinct dedup-key "
            "count versus ClickHouse FINAL; non-dedup tables use raw parity."
        )
        print(render_table(results))
        print("")
        print(
            "Summary: "
            f"total={summary['total']} ok={summary.get('ok', 0)} "
            f"mismatch={summary.get('mismatch', 0)} "
            f"missing_bigquery={summary.get('missing_bigquery', 0)} "
            f"missing_clickhouse={summary.get('missing_clickhouse', 0)} "
            f"missing_both={summary.get('missing_both', 0)}"
        )

    return 0 if summary.get("mismatch", 0) == 0 and summary.get("missing_bigquery", 0) == 0 and summary.get("missing_clickhouse", 0) == 0 and summary.get("missing_both", 0) == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SyncCheckError as exc:
        print(f"error: {exc}", file=sys.stderr)
        raise SystemExit(2)
