from __future__ import annotations

from textwrap import dedent


class ClickHouseVideoMetadataRepository:
    def __init__(self, client, settings):
        self._client = client
        self._settings = settings

    async def get_video_metadata_batch(self, video_ids: list[str]) -> dict[str, dict]:
        unique_video_ids = tuple(dict.fromkeys(video_id for video_id in video_ids if video_id))
        if not unique_video_ids:
            return {}

        query = f"""
        WITH
            rejected_videos AS (
                SELECT DISTINCT video_id
                FROM {self._table("ugc_content_approval")} FINAL
                WHERE is_approved = 0
            ),
            excluded_videos AS (
                SELECT DISTINCT video_id
                FROM {self._table("excluded_videos")} FINAL
            ),
            ai_ugc_data AS (
                SELECT
                    aug.video_id,
                    coalesce(aug.upload_canister_id, '') AS canister_id,
                    coalesce(aug.post_id, '') AS post_id,
                    coalesce(aug.publisher_user_id, '') AS publisher_user_id,
                    aug.upload_timestamp AS source_timestamp,
                    1 AS priority
                FROM {self._table("ai_ugc")} AS aug FINAL
                WHERE aug.video_id IN %(video_ids)s
            ),
            bot_uploaded_data AS (
                SELECT
                    buc.video_id,
                    coalesce(buc.canister_id, '') AS canister_id,
                    coalesce(buc.post_id, '') AS post_id,
                    coalesce(buc.publisher_user_id, '') AS publisher_user_id,
                    buc.timestamp AS source_timestamp,
                    2 AS priority
                FROM {self._table("bot_uploaded_content")} AS buc FINAL
                WHERE buc.video_id IN %(video_ids)s
            ),
            ugc_approval_data AS (
                SELECT
                    uca.video_id,
                    coalesce(uca.canister_id, '') AS canister_id,
                    coalesce(uca.post_id, '') AS post_id,
                    coalesce(uca.user_id, '') AS publisher_user_id,
                    uca.created_at AS source_timestamp,
                    3 AS priority
                FROM {self._table("ugc_content_approval")} AS uca FINAL
                WHERE uca.video_id IN %(video_ids)s
                  AND uca.is_approved = 1
            ),
            combined_data AS (
                SELECT * FROM ai_ugc_data
                UNION ALL
                SELECT * FROM bot_uploaded_data
                UNION ALL
                SELECT * FROM ugc_approval_data
            ),
            prioritized_data AS (
                SELECT
                    video_id,
                    canister_id,
                    post_id,
                    publisher_user_id,
                    row_number() OVER (
                        PARTITION BY video_id
                        ORDER BY priority ASC, source_timestamp DESC
                    ) AS rn
                FROM combined_data
            )
        SELECT
            pd.video_id AS video_id,
            pd.canister_id AS canister_id,
            pd.post_id AS post_id,
            pd.publisher_user_id AS publisher_user_id
        FROM prioritized_data pd
        INNER JOIN {self._table("video_unique_v2")} AS vu FINAL
            ON pd.video_id = vu.video_id
        LEFT JOIN rejected_videos rv
            ON pd.video_id = rv.video_id
        LEFT JOIN excluded_videos ev
            ON pd.video_id = ev.video_id
        WHERE pd.rn = 1
          AND ifNull(rv.video_id, '') = ''
          AND ifNull(ev.video_id, '') = ''
        """
        rows = await self._client.fetch_all(
            dedent(query).strip(),
            {"video_ids": unique_video_ids},
        )

        metadata: dict[str, dict] = {}
        for row in rows:
            video_id = str(row.get("video_id") or "").strip()
            if not video_id:
                continue

            canister_id = str(row.get("canister_id") or "").strip()
            if canister_id == "2vxsx-fae":
                canister_id = self._settings.profile_canister_id

            metadata[video_id] = {
                "canister_id": canister_id,
                "post_id": str(row.get("post_id") or "").strip(),
                "publisher_user_id": str(row.get("publisher_user_id") or "").strip(),
            }
        return metadata

    def _table(self, name: str) -> str:
        return f"{self._settings.clickhouse_database}.{name}"
