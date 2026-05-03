#!/usr/bin/env python3
"""Compare BigQuery and self-hosted ClickHouse row counts for migrated tables."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

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

# Current production table mapping from the rollout docs.
TABLE_MAPPINGS: Dict[str, str] = {
    "ai_ugc": "ai_ugc",
    "bot_uploaded_content": "bot_uploaded_content",
    "excluded_videos": "excluded_videos",
    "follower_graph": "follower_graph",
    "global_popular_videos_l7d": "global_popular_videos_l7d",
    "ugc_content_approval": "ugc_content_approval",
    "userVideoRelation": "user_video_relation",
    "video_statistics": "video_statistics",
    "video_unique_v2": "video_unique_v2",
}
CLICKHOUSE_TO_BIGQUERY = {value: key for key, value in TABLE_MAPPINGS.items()}


@dataclass(frozen=True)
class TableMapping:
    bigquery_table: str
    clickhouse_table: str


@dataclass
class TableComparison:
    bigquery_table: str
    clickhouse_table: str
    clickhouse_engine: str | None
    bigquery_count: int | None
    clickhouse_count: int | None
    absolute_delta: int | None
    delta_ratio: float | None
    status: str
    note: str = ""


class SyncCheckError(RuntimeError):
    """Raised when the parity check cannot complete."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Compare row counts for the migrated yral tables between "
            "BigQuery and self-hosted ClickHouse."
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
        help="Allowed absolute row-count delta before marking a mismatch (default: 0)",
    )
    parser.add_argument(
        "--max-delta-ratio",
        type=float,
        default=0.0,
        help=(
            "Allowed delta ratio before marking a mismatch. Ratio is "
            "abs(delta) / max(bigquery_count, 1). Default: 0.0"
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


def resolve_table_mappings(requested_tables: Sequence[str]) -> List[TableMapping]:
    if not requested_tables:
        return [
            TableMapping(bigquery_table=bq_table, clickhouse_table=clickhouse_table)
            for bq_table, clickhouse_table in TABLE_MAPPINGS.items()
        ]

    mappings: List[TableMapping] = []
    seen: set[tuple[str, str]] = set()
    for table_name in requested_tables:
        if table_name in TABLE_MAPPINGS:
            pair = (table_name, TABLE_MAPPINGS[table_name])
        elif table_name in CLICKHOUSE_TO_BIGQUERY:
            pair = (CLICKHOUSE_TO_BIGQUERY[table_name], table_name)
        else:
            raise SyncCheckError(
                f"Unknown table selector '{table_name}'. "
                "Use a mapped BigQuery or ClickHouse table name."
            )
        if pair not in seen:
            seen.add(pair)
            mappings.append(TableMapping(bigquery_table=pair[0], clickhouse_table=pair[1]))
    return mappings


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
) -> Dict[str, str]:
    sql = (
        "SELECT name, engine "
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
    metadata: Dict[str, str] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        name, engine = line.split("\t", 1)
        metadata[name] = engine
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


def build_bigquery_count_query(
    project_id: str,
    dataset: str,
    mappings: Iterable[TableMapping],
) -> str:
    statements = []
    for mapping in mappings:
        statements.append(
            "SELECT "
            f"'{mapping.bigquery_table}' AS table_name, "
            "COUNT(*) AS row_count "
            f"FROM `{project_id}.{dataset}.{mapping.bigquery_table}`"
        )
    return "\nUNION ALL\n".join(statements)


def fetch_bigquery_counts(
    client: bigquery.Client,
    project_id: str,
    dataset: str,
    mappings: Sequence[TableMapping],
) -> Dict[str, int]:
    if not mappings:
        return {}

    query_job = client.query(build_bigquery_count_query(project_id, dataset, mappings))
    rows = query_job.result()
    return {row["table_name"]: int(row["row_count"]) for row in rows}


def count_requires_final(engine: str | None) -> bool:
    return bool(engine) and "ReplacingMergeTree" in engine


def build_clickhouse_count_query(
    database: str,
    mappings: Sequence[TableMapping],
    clickhouse_metadata: Dict[str, str],
) -> str:
    statements = []
    for mapping in mappings:
        engine = clickhouse_metadata[mapping.clickhouse_table]
        final_clause = " FINAL" if count_requires_final(engine) else ""
        statements.append(
            "SELECT "
            f"'{mapping.clickhouse_table}' AS table_name, "
            "count() AS row_count "
            f"FROM {database}.{mapping.clickhouse_table}{final_clause}"
        )
    return "\nUNION ALL\n".join(statements) + "\nORDER BY table_name FORMAT TSVRaw"


def fetch_clickhouse_counts(
    ssh_user: str,
    ssh_target: str,
    ssh_port: int | None,
    database: str,
    mappings: Sequence[TableMapping],
    clickhouse_metadata: Dict[str, str],
) -> Dict[str, int]:
    if not mappings:
        return {}

    sql = build_clickhouse_count_query(database, mappings, clickhouse_metadata)
    output = run_ssh_command(
        ssh_user,
        ssh_target,
        ssh_port,
        ["clickhouse-client", "--query", sql],
    )
    counts: Dict[str, int] = {}
    for line in output.splitlines():
        if not line.strip():
            continue
        table_name, row_count = line.split("\t", 1)
        counts[table_name] = int(row_count)
    return counts


def calculate_delta_ratio(bigquery_count: int, absolute_delta: int) -> float:
    return absolute_delta / max(bigquery_count, 1)


def compare_counts(
    requested_mappings: Sequence[TableMapping],
    bigquery_tables: set[str],
    clickhouse_metadata: Dict[str, str],
    bigquery_counts: Dict[str, int],
    clickhouse_counts: Dict[str, int],
    max_absolute_delta: int,
    max_delta_ratio: float,
) -> List[TableComparison]:
    results: List[TableComparison] = []

    for mapping in requested_mappings:
        bq_exists = mapping.bigquery_table in bigquery_tables
        ch_exists = mapping.clickhouse_table in clickhouse_metadata

        if not bq_exists and not ch_exists:
            results.append(
                TableComparison(
                    bigquery_table=mapping.bigquery_table,
                    clickhouse_table=mapping.clickhouse_table,
                    clickhouse_engine=None,
                    bigquery_count=None,
                    clickhouse_count=None,
                    absolute_delta=None,
                    delta_ratio=None,
                    status="missing_both",
                    note="Table is missing in both systems",
                )
            )
            continue

        if not bq_exists:
            results.append(
                TableComparison(
                    bigquery_table=mapping.bigquery_table,
                    clickhouse_table=mapping.clickhouse_table,
                    clickhouse_engine=clickhouse_metadata.get(mapping.clickhouse_table),
                    bigquery_count=None,
                    clickhouse_count=clickhouse_counts.get(mapping.clickhouse_table),
                    absolute_delta=None,
                    delta_ratio=None,
                    status="missing_bigquery",
                    note="Table missing in BigQuery dataset",
                )
            )
            continue

        if not ch_exists:
            results.append(
                TableComparison(
                    bigquery_table=mapping.bigquery_table,
                    clickhouse_table=mapping.clickhouse_table,
                    clickhouse_engine=None,
                    bigquery_count=bigquery_counts.get(mapping.bigquery_table),
                    clickhouse_count=None,
                    absolute_delta=None,
                    delta_ratio=None,
                    status="missing_clickhouse",
                    note="Table missing in ClickHouse database",
                )
            )
            continue

        bq_count = bigquery_counts[mapping.bigquery_table]
        ch_count = clickhouse_counts[mapping.clickhouse_table]
        absolute_delta = abs(bq_count - ch_count)
        delta_ratio = calculate_delta_ratio(bq_count, absolute_delta)
        status = "ok"
        note = ""
        if absolute_delta > max_absolute_delta or delta_ratio > max_delta_ratio:
            status = "mismatch"
            note = (
                f"delta={absolute_delta} exceeds allowed thresholds "
                f"(abs<={max_absolute_delta}, ratio<={max_delta_ratio})"
            )

        results.append(
            TableComparison(
                bigquery_table=mapping.bigquery_table,
                clickhouse_table=mapping.clickhouse_table,
                clickhouse_engine=clickhouse_metadata.get(mapping.clickhouse_table),
                bigquery_count=bq_count,
                clickhouse_count=ch_count,
                absolute_delta=absolute_delta,
                delta_ratio=delta_ratio,
                status=status,
                note=note,
            )
        )

    return sorted(results, key=lambda item: item.clickhouse_table)


def render_table(results: Sequence[TableComparison]) -> str:
    headers = [
        "bigquery_table",
        "clickhouse_table",
        "engine",
        "bigquery_count",
        "clickhouse_count",
        "absolute_delta",
        "delta_ratio",
        "status",
    ]

    rows = []
    for result in results:
        rows.append(
            [
                result.bigquery_table,
                result.clickhouse_table,
                result.clickhouse_engine or "-",
                "-" if result.bigquery_count is None else str(result.bigquery_count),
                "-" if result.clickhouse_count is None else str(result.clickhouse_count),
                "-" if result.absolute_delta is None else str(result.absolute_delta),
                "-" if result.delta_ratio is None else f"{result.delta_ratio:.6f}",
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

    notes = [result for result in results if result.note]
    if notes:
        lines.append("")
        for result in notes:
            lines.append(f"{result.clickhouse_table}: {result.note}")

    return "\n".join(lines)


def summarize_results(results: Sequence[TableComparison]) -> Dict[str, int]:
    summary = {"ok": 0, "mismatch": 0, "missing_bigquery": 0, "missing_clickhouse": 0, "missing_both": 0}
    for result in results:
        summary[result.status] = summary.get(result.status, 0) + 1
    summary["total"] = len(results)
    return summary


def main() -> int:
    args = parse_args()
    load_environment(args.env_file)

    requested_tables = normalize_requested_tables(args.tables)
    requested_mappings = resolve_table_mappings(requested_tables)
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

    comparable_mappings = [
        mapping
        for mapping in requested_mappings
        if mapping.bigquery_table in bigquery_tables and mapping.clickhouse_table in clickhouse_metadata
    ]

    bigquery_counts = fetch_bigquery_counts(
        bigquery_client,
        args.bq_project,
        args.bq_dataset,
        comparable_mappings,
    )
    clickhouse_counts = fetch_clickhouse_counts(
        ssh_user=args.ssh_user,
        ssh_target=args.ssh_target,
        ssh_port=args.ssh_port,
        database=args.clickhouse_database,
        mappings=comparable_mappings,
        clickhouse_metadata=clickhouse_metadata,
    )

    results = compare_counts(
        requested_mappings=requested_mappings,
        bigquery_tables=bigquery_tables,
        clickhouse_metadata=clickhouse_metadata,
        bigquery_counts=bigquery_counts,
        clickhouse_counts=clickhouse_counts,
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
            "summary": summary,
            "results": [asdict(result) for result in results],
        }
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            f"BigQuery dataset {args.bq_project}.{args.bq_dataset} vs "
            f"ClickHouse {args.clickhouse_database} on {args.ssh_user}@{args.ssh_target}"
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
