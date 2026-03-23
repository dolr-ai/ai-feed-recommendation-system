import json
from datetime import datetime, timezone
from typing import Dict, List, Optional

from src.models.influencer import Influencer, InfluencerScore, StatsSnapshot
from src.services.logger_service import LoggerService
from src.utils.kvrocks import (
    feed_meta_key,
    metadata_key,
    ranked_feed_key,
    serve_counts_key,
    stats_snapshot_key,
)


class InfluencerRepository:
    def __init__(self, client, settings):
        self._client = client
        self._settings = settings
        self._log = LoggerService().get("repository")

    async def write_ranked_feed(
        self,
        scores: List[InfluencerScore],
        influencers: List[Influencer],
    ) -> None:
        inf_map = {inf.id: inf for inf in influencers}
        rows: List[dict] = []
        for score in scores:
            inf = inf_map[score.influencer_id]
            rows.append(
                {
                    "id": inf.id,
                    "name": inf.name,
                    "display_name": inf.display_name,
                    "avatar_url": inf.avatar_url,
                    "description": inf.description,
                    "category": inf.category,
                    "created_at": inf.created_at.isoformat(),
                    "engagement_score": round(score.engagement_score, 6),
                    "discovery_score": round(score.discovery_score, 6),
                    "momentum_score": round(score.momentum_score, 6),
                    "newness_score": round(score.newness_score, 6),
                    "underexposure_score": round(score.underexposure_score, 6),
                    "content_activity_score": round(score.content_activity_score, 6),
                    "depth_quality_score": round(score.depth_quality_score, 6),
                    "eligible_for_discovery": score.eligible_for_discovery,
                    "engagement_rank": score.engagement_rank,
                    "discovery_rank": score.discovery_rank,
                    "final_rank": score.final_rank,
                    "selected_rank_source": score.selected_rank_source,
                    "conversation_count": inf.conversation_count,
                    "message_count": inf.message_count,
                    "followers_count": inf.followers_count,
                    "total_video_views": inf.total_video_views,
                    "share_count_total": inf.share_count_total,
                    "likes_count_total": inf.likes_count_total,
                    "avg_watch_pct_mean": round(inf.avg_watch_pct_mean, 2),
                    "ready_post_count": inf.ready_post_count,
                    "last_post_at": inf.last_post_at.isoformat() if inf.last_post_at else None,
                    "depth_ratio": round(inf.depth_ratio, 4),
                    "share_rate": round(inf.share_rate, 4),
                    "views_per_post": round(inf.views_per_post, 2),
                    "likes_per_post": round(inf.likes_per_post, 2),
                    "posting_density": round(inf.posting_density, 4),
                    "conv_velocity": round(score.conv_velocity, 4),
                }
            )
        await self.rewrite_ranked_feed_rows(rows)

    async def rewrite_ranked_feed_rows(self, rows: List[dict]) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        ordered_rows = sorted(rows, key=lambda row: int(row["final_rank"]))

        pipe = self._client.pipeline()
        pipe.delete(ranked_feed_key())
        pipe.delete(metadata_key())
        pipe.delete(feed_meta_key())

        total_count = len(ordered_rows)
        zset_members = {
            row["id"]: float(total_count - idx + 1)
            for idx, row in enumerate(ordered_rows, start=1)
        }
        if zset_members:
            pipe.zadd(ranked_feed_key(), zset_members)

        for row in ordered_rows:
            pipe.hset(metadata_key(), row["id"], json.dumps(row))

        pipe.set(feed_meta_key(), json.dumps({"feed_generated_at": now_str}))
        pipe.expire(ranked_feed_key(), self._settings.feed_ttl_sec)
        pipe.expire(metadata_key(), self._settings.feed_ttl_sec)
        pipe.expire(feed_meta_key(), self._settings.feed_ttl_sec)
        await pipe.execute()

    async def write_stats_snapshot(self, influencers: List[Influencer]) -> None:
        now_str = datetime.now(timezone.utc).isoformat()
        pipe = self._client.pipeline()
        pipe.delete(stats_snapshot_key())
        for inf in influencers:
            blob = {
                "conversation_count": inf.conversation_count,
                "message_count": inf.message_count,
                "followers_count": inf.followers_count,
                "total_video_views": inf.total_video_views,
                "share_count_total": inf.share_count_total,
                "captured_at": now_str,
            }
            pipe.hset(stats_snapshot_key(), inf.id, json.dumps(blob))
        pipe.expire(stats_snapshot_key(), self._settings.snapshot_ttl_sec)
        await pipe.execute()

    async def load_stats_snapshot(self) -> Dict[str, StatsSnapshot]:
        raw = await self._client.hgetall(stats_snapshot_key())
        if not raw:
            return {}
        result = {}
        for influencer_id, blob_str in raw.items():
            blob = json.loads(blob_str)
            result[influencer_id] = StatsSnapshot(
                influencer_id=influencer_id,
                conversation_count=blob["conversation_count"],
                message_count=blob["message_count"],
                followers_count=blob["followers_count"],
                total_video_views=blob["total_video_views"],
                share_count_total=blob["share_count_total"],
                captured_at=datetime.fromisoformat(blob["captured_at"]),
            )
        return result

    async def get_feed(
        self,
        offset: int,
        limit: int,
    ) -> tuple[list[dict], int, Optional[datetime]]:
        pipe = self._client.pipeline()
        pipe.zrevrange(ranked_feed_key(), offset, offset + limit - 1)
        pipe.zcard(ranked_feed_key())
        pipe.get(feed_meta_key())
        ids, total_count, meta_blob = await pipe.execute()

        feed_generated_at = None
        if meta_blob:
            feed_generated_at = datetime.fromisoformat(
                json.loads(meta_blob)["feed_generated_at"]
            )

        if not ids:
            return [], int(total_count), feed_generated_at

        pipe = self._client.pipeline()
        for influencer_id in ids:
            pipe.hget(metadata_key(), influencer_id)
        blobs = await pipe.execute()

        result = []
        for influencer_id, blob in zip(ids, blobs):
            if not blob:
                self._log.debug(
                    "Metadata missing for ranked influencer",
                    extra={"influencer_id": influencer_id},
                )
                continue
            result.append(json.loads(blob))
        return result, int(total_count), feed_generated_at

    async def get_serve_count(self, influencer_id: str) -> float:
        value = await self._client.hget(serve_counts_key(), influencer_id)
        return float(value) if value else 0.0

    async def get_all_serve_counts(
        self,
        influencer_ids: List[str],
    ) -> Dict[str, float]:
        pipe = self._client.pipeline()
        for influencer_id in influencer_ids:
            pipe.hget(serve_counts_key(), influencer_id)
        vals = await pipe.execute()
        return {
            influencer_id: float(value) if value else 0.0
            for influencer_id, value in zip(influencer_ids, vals)
        }

    async def increment_serve_counts(self, ids: List[str], offset: int) -> None:
        pipe = self._client.pipeline()
        for page_idx, influencer_id in enumerate(ids):
            absolute_rank = offset + page_idx + 1
            pipe.hincrbyfloat(
                serve_counts_key(),
                influencer_id,
                self._position_weight(absolute_rank),
            )
        pipe.expire(serve_counts_key(), self._settings.serve_count_ttl_sec)
        await pipe.execute()

    async def get_metadata(self, influencer_id: str) -> Optional[dict]:
        raw = await self._client.hget(metadata_key(), influencer_id)
        return json.loads(raw) if raw else None

    async def get_all_metadata(self) -> List[dict]:
        raw = await self._client.hgetall(metadata_key())
        if not raw:
            return []
        rows = [json.loads(blob) for blob in raw.values()]
        return sorted(rows, key=lambda row: int(row.get("final_rank") or 0))

    @staticmethod
    def _position_weight(rank: int) -> float:
        if rank <= 10:
            return 1.0
        if rank <= 20:
            return 0.6
        if rank <= 50:
            return 0.3
        return 0.1
