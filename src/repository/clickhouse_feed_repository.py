from __future__ import annotations

from textwrap import dedent
from typing import Optional


class ClickHouseFeedRepository:
    def __init__(self, client, settings):
        self._client = client
        self._settings = settings

    async def get_global_popular_videos(
        self,
        limit: Optional[int] = None,
    ) -> list[dict]:
        query = f"""
        WITH
            {self._rejected_and_excluded_ctes()},
            {self._valid_videos_cte(include_approval_source=True)}
        SELECT
            gpv.video_id,
            gpv.global_popularity_score
        FROM {self._table("global_popular_videos_l7d")} AS gpv
        INNER JOIN valid_videos vv
            ON gpv.video_id = vv.video_id
        WHERE gpv.global_popularity_score IS NOT NULL
        ORDER BY gpv.global_popularity_score DESC, gpv.video_id
        {self._limit_clause(limit)}
        """
        return await self._client.fetch_all(
            dedent(query).strip(),
            self._limit_parameters(limit),
        )

    async def get_fresh_videos(self) -> list[dict]:
        query = f"""
        WITH
            {self._rejected_and_excluded_ctes()},
            {self._valid_videos_cte(include_approval_source=True)},
            video_timestamps AS (
                SELECT
                    video_id,
                    max(upload_timestamp) AS ts
                FROM {self._table("ai_ugc")} FINAL
                WHERE upload_timestamp IS NOT NULL
                GROUP BY video_id

                UNION ALL

                SELECT
                    video_id,
                    max(timestamp) AS ts
                FROM {self._table("bot_uploaded_content")} FINAL
                WHERE timestamp IS NOT NULL
                GROUP BY video_id

                UNION ALL

                SELECT
                    video_id,
                    max(created_at) AS ts
                FROM {self._table("ugc_content_approval")} FINAL
                WHERE is_approved = 1
                  AND created_at IS NOT NULL
                GROUP BY video_id
            ),
            per_video AS (
                SELECT
                    vt.video_id,
                    max(vt.ts) AS ts
                FROM video_timestamps vt
                INNER JOIN valid_videos vv
                    ON vt.video_id = vv.video_id
                GROUP BY vt.video_id
            ),
            bounds AS (
                SELECT
                    max(ts) AS max_ts,
                    max(ts) - INTERVAL 1 DAY AS c1,
                    max(ts) - INTERVAL 7 DAY AS c7,
                    max(ts) - INTERVAL 14 DAY AS c14,
                    max(ts) - INTERVAL 30 DAY AS c30,
                    max(ts) - INTERVAL 90 DAY AS c90
                FROM per_video
            )
        SELECT
            p.video_id,
            CASE
                WHEN p.ts > b.c1 AND p.ts <= b.max_ts THEN 'l1d'
                WHEN p.ts > b.c7 AND p.ts <= b.c1 THEN 'l7d'
                WHEN p.ts > b.c14 AND p.ts <= b.c7 THEN 'l14d'
                WHEN p.ts > b.c30 AND p.ts <= b.c14 THEN 'l30d'
                WHEN p.ts > b.c90 AND p.ts <= b.c30 THEN 'l90d'
                ELSE NULL
            END AS bucket
        FROM per_video p
        CROSS JOIN bounds b
        WHERE p.ts > b.c90
          AND p.ts <= b.max_ts
        ORDER BY bucket, p.video_id
        """
        return await self._client.fetch_all(dedent(query).strip())

    async def get_user_watch_history(
        self,
        hours_back: int = 12,
    ) -> list[dict]:
        query = f"""
        WITH
            {self._rejected_and_excluded_ctes()},
            {self._valid_videos_cte(include_approval_source=True)},
            watch_events AS (
                SELECT
                    uvr.user_id,
                    uvr.video_id,
                    uvr.last_watched_timestamp
                FROM {self._table("user_video_relation")} AS uvr FINAL
                INNER JOIN valid_videos vv
                    ON uvr.video_id = vv.video_id
                WHERE uvr.last_watched_timestamp IS NOT NULL
            ),
            bounds AS (
                SELECT max(last_watched_timestamp) AS max_ts
                FROM watch_events
            )
        SELECT DISTINCT
            we.user_id,
            we.video_id
        FROM watch_events we
        CROSS JOIN bounds b
        WHERE we.last_watched_timestamp >= b.max_ts - INTERVAL %(hours_back)s HOUR
        """
        return await self._client.fetch_all(
            dedent(query).strip(),
            {"hours_back": hours_back},
        )

    async def get_ugc_videos(
        self,
        limit: Optional[int] = None,
    ) -> list[dict]:
        query = f"""
        WITH
            {self._rejected_and_excluded_ctes()},
            {self._valid_videos_cte(include_approval_source=False)}
        SELECT DISTINCT
            aug.video_id,
            aug.upload_timestamp AS created_at
        FROM {self._table("ai_ugc")} AS aug FINAL
        INNER JOIN valid_videos vv
            ON aug.video_id = vv.video_id
        WHERE aug.upload_timestamp IS NOT NULL
          AND aug.publisher_user_id IS NOT NULL
          AND aug.upload_timestamp >= now() - INTERVAL 90 DAY
        ORDER BY aug.upload_timestamp DESC, aug.video_id
        {self._limit_clause(limit)}
        """
        return await self._client.fetch_all(
            dedent(query).strip(),
            self._limit_parameters(limit),
        )

    async def get_ugc_discovery_videos(
        self,
        max_views: int = 200,
        max_age_days: int = 7,
        limit: Optional[int] = None,
    ) -> list[dict]:
        query = f"""
        WITH
            {self._rejected_and_excluded_ctes()},
            {self._valid_videos_cte(include_approval_source=True)},
            ugc_sources AS (
                SELECT
                    video_id,
                    upload_timestamp
                FROM {self._table("ai_ugc")} FINAL
                WHERE upload_timestamp IS NOT NULL
                  AND publisher_user_id IS NOT NULL

                UNION ALL

                SELECT
                    video_id,
                    created_at AS upload_timestamp
                FROM {self._table("ugc_content_approval")} FINAL
                WHERE is_approved = 1
                  AND created_at IS NOT NULL
            )
        SELECT DISTINCT
            us.video_id,
            us.upload_timestamp,
            coalesce(vs.total_impressions, 0) AS impression_count
        FROM ugc_sources us
        INNER JOIN valid_videos vv
            ON us.video_id = vv.video_id
        LEFT JOIN {self._table("video_statistics")} AS vs FINAL
            ON us.video_id = vs.video_id
        WHERE us.upload_timestamp >= now() - INTERVAL %(max_age_days)s DAY
          AND coalesce(vs.total_impressions, 0) < %(max_views)s
        ORDER BY us.upload_timestamp DESC, us.video_id
        {self._limit_clause(limit)}
        """
        parameters = {
            "max_views": max_views,
            "max_age_days": max_age_days,
            **self._limit_parameters(limit),
        }
        return await self._client.fetch_all(dedent(query).strip(), parameters)

    async def get_following_video_candidates(
        self,
        user_id: str,
        num_videos: int = 1000,
    ) -> list[dict]:
        query = f"""
        WITH
            {self._rejected_and_excluded_ctes()},
            {self._valid_videos_cte(include_approval_source=True)},
            followed_users AS (
                SELECT DISTINCT following_id AS user_id
                FROM {self._table("follower_graph")} FINAL
                WHERE follower_id = %(user_id)s
                  AND active = 1
            ),
            followed_content AS (
                SELECT
                    buc.video_id,
                    buc.publisher_user_id,
                    buc.timestamp AS upload_time
                FROM {self._table("bot_uploaded_content")} AS buc FINAL
                INNER JOIN followed_users fu
                    ON buc.publisher_user_id = fu.user_id
                WHERE buc.video_id IS NOT NULL

                UNION ALL

                SELECT
                    aug.video_id,
                    aug.publisher_user_id,
                    aug.upload_timestamp AS upload_time
                FROM {self._table("ai_ugc")} AS aug FINAL
                INNER JOIN followed_users fu
                    ON aug.publisher_user_id = fu.user_id
                WHERE aug.video_id IS NOT NULL

                UNION ALL

                SELECT
                    uca.video_id,
                    uca.user_id AS publisher_user_id,
                    uca.created_at AS upload_time
                FROM {self._table("ugc_content_approval")} AS uca FINAL
                INNER JOIN followed_users fu
                    ON uca.user_id = fu.user_id
                WHERE uca.video_id IS NOT NULL
                  AND uca.is_approved = 1
                  AND uca.user_id IS NOT NULL
            ),
            valid_followed_content AS (
                SELECT
                    fc.video_id,
                    fc.publisher_user_id,
                    max(fc.upload_time) AS upload_time
                FROM followed_content fc
                INNER JOIN valid_videos vv
                    ON fc.video_id = vv.video_id
                GROUP BY fc.video_id, fc.publisher_user_id
            ),
            popular_videos AS (
                SELECT
                    vfc.video_id,
                    gpv.global_popularity_score,
                    CAST(NULL, 'Nullable(DateTime)') AS upload_time,
                    1 AS priority
                FROM valid_followed_content vfc
                INNER JOIN {self._table("global_popular_videos_l7d")} AS gpv
                    ON vfc.video_id = gpv.video_id
                WHERE gpv.global_popularity_score IS NOT NULL
            ),
            fresh_videos AS (
                SELECT
                    vfc.video_id,
                    CAST(NULL, 'Nullable(Float64)') AS global_popularity_score,
                    vfc.upload_time,
                    2 AS priority
                FROM valid_followed_content vfc
                WHERE vfc.upload_time >= now() - INTERVAL 30 DAY
                  AND vfc.video_id NOT IN (SELECT video_id FROM popular_videos)
            )
        SELECT
            video_id,
            global_popularity_score
        FROM (
            SELECT video_id, global_popularity_score, upload_time, priority
            FROM popular_videos

            UNION ALL

            SELECT video_id, global_popularity_score, upload_time, priority
            FROM fresh_videos
        )
        ORDER BY
            priority ASC,
            global_popularity_score DESC NULLS LAST,
            upload_time DESC NULLS LAST,
            video_id
        LIMIT %(num_videos)s
        """
        return await self._client.fetch_all(
            dedent(query).strip(),
            {"user_id": user_id, "num_videos": num_videos},
        )

    async def get_excluded_video_ids(self) -> list[str]:
        query = f"""
        SELECT DISTINCT video_id
        FROM {self._table("excluded_videos")} FINAL
        """
        rows = await self._client.fetch_all(dedent(query).strip())
        return [
            row["video_id"]
            for row in rows
            if row.get("video_id")
        ]

    def _table(self, name: str) -> str:
        return f"{self._settings.clickhouse_database}.{name}"

    def _rejected_and_excluded_ctes(self) -> str:
        return dedent(
            f"""
            rejected_videos AS (
                SELECT DISTINCT video_id
                FROM {self._table("ugc_content_approval")} FINAL
                WHERE is_approved = 0
            ),
            excluded_videos AS (
                SELECT DISTINCT video_id
                FROM {self._table("excluded_videos")} FINAL
            )
            """
        ).strip()

    def _valid_videos_cte(self, include_approval_source: bool) -> str:
        source_conditions = [
            "aug.video_id IS NOT NULL",
            "buc.video_id IS NOT NULL",
        ]
        if include_approval_source:
            source_conditions.append("uca_approved.video_id IS NOT NULL")

        valid_source_clause = "\n          OR ".join(source_conditions)
        return dedent(
            f"""
            valid_videos AS (
                SELECT DISTINCT vu.video_id AS video_id
                FROM {self._table("video_unique_v2")} AS vu FINAL
                LEFT JOIN {self._table("ai_ugc")} AS aug FINAL
                    ON vu.video_id = aug.video_id
                LEFT JOIN {self._table("bot_uploaded_content")} AS buc FINAL
                    ON vu.video_id = buc.video_id
                LEFT JOIN {self._table("ugc_content_approval")} AS uca_approved FINAL
                    ON vu.video_id = uca_approved.video_id
                   AND uca_approved.is_approved = 1
                LEFT JOIN rejected_videos rv
                    ON vu.video_id = rv.video_id
                LEFT JOIN excluded_videos ev
                    ON vu.video_id = ev.video_id
                WHERE rv.video_id IS NULL
                  AND ev.video_id IS NULL
                  AND (
                      {valid_source_clause}
                  )
            )
            """
        ).strip()

    @staticmethod
    def _limit_clause(limit: Optional[int]) -> str:
        return "LIMIT %(limit)s" if limit is not None else ""

    @staticmethod
    def _limit_parameters(limit: Optional[int]) -> dict:
        if limit is None:
            return {}
        return {"limit": limit}
