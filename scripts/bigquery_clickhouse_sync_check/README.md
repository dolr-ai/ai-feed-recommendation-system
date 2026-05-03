# BigQuery vs ClickHouse Sync Check

This folder contains parity tooling for the migrated `yral` tables in:

- BigQuery: `hot-or-not-feed-intelligence.yral_ds`
- self-hosted ClickHouse: `yral` on `ansuman-1` or `ansuman-2`

The script uses:

- `SERVICE_CRED` from the repo `.env` for BigQuery access
- SSH into `ansuman-1` or `ansuman-2` and runs `clickhouse-client` there directly

That avoids adding a second local secret path for ClickHouse.

There are now two scripts:

- `check_counts.py`: simple raw-count comparison
- `analyze_counts.py`: dedup-aware investigation view
- `backfill_user_video_relation_range.py`: safe range replay for `userVideoRelation`

Use `analyze_counts.py` when you need to understand whether a mismatch is caused by:

- duplicate keys in BigQuery
- pending duplicates in ClickHouse before `FINAL`
- a real logical drift after dedup

Use `backfill_user_video_relation_range.py` when the analyzer shows a historical hole in `userVideoRelation` and you want to replay a bounded BigQuery range into ClickHouse without deleting the existing partition first.

## Default mapped tables

- `ai_ugc`
- `bot_uploaded_content`
- `excluded_videos`
- `follower_graph`
- `global_popular_videos_l7d`
- `ugc_content_approval`
- `userVideoRelation` -> `user_video_relation`
- `video_statistics`
- `video_unique_v2`

`analyze_counts.py` uses the documented logical view:

- BigQuery logical count = `COUNT(DISTINCT dedup_key)`
- ClickHouse logical count = `COUNT(*) FINAL` for `ReplacingMergeTree`

`ReplacingMergeTree` tables are counted with `FINAL` on ClickHouse, because that is the safe parity view documented in the self-hosted migration notes.

## Usage

Simple raw-count run against `ansuman-1`:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/check_counts.py
```

Dedup-aware investigation against `ansuman-1`:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/analyze_counts.py
```

Run against `ansuman-2`:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/analyze_counts.py --ssh-target ansuman-2
```

Check only a subset:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/analyze_counts.py \
  --table userVideoRelation \
  --table video_statistics
```

Machine-readable output:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/analyze_counts.py --output json
```

Allow small logical drift before returning non-zero:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/analyze_counts.py \
  --max-absolute-delta 100 \
  --max-delta-ratio 0.001
```

Dry-run the April 2026 `userVideoRelation` replay:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/backfill_user_video_relation_range.py
```

Execute the April 2026 replay:

```bash
./venv/bin/python scripts/bigquery_clickhouse_sync_check/backfill_user_video_relation_range.py --execute
```

## Exit codes

- `0`: all compared tables are within logical threshold
- `1`: completed, but at least one table is missing or mismatched
- `2`: operational failure, such as SSH or credential errors
