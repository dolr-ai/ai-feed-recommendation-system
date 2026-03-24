from datetime import datetime, timezone

from src.services.logger_service import LoggerService


class DiscoveryBoostService:
    def __init__(self, repo, scoring_service, feed_mixer_service, settings):
        self._repo = repo
        self._scoring_service = scoring_service
        self._feed_mixer_service = feed_mixer_service
        self._settings = settings
        self._log = LoggerService().get("discovery_boost")

    async def refresh(self) -> None:
        now = datetime.now(timezone.utc)
        rows = await self._repo.get_all_metadata()
        if not rows:
            return

        serve_counts = await self._repo.get_all_serve_counts([row["id"] for row in rows])
        for row in rows:
            eligible, discovery_score, newness_score, underexposure_score = (
                self._scoring_service.compute_discovery_metrics(
                    created_at=datetime.fromisoformat(row["created_at"]),
                    conversation_count=int(row["conversation_count"]),
                    message_count=int(row["message_count"]),
                    followers_count=int(row["followers_count"]),
                    ready_post_count=int(row["ready_post_count"]),
                    momentum_score=float(row["momentum_score"]),
                    content_activity_score=float(row["content_activity_score"]),
                    depth_quality_score=float(row["depth_quality_score"]),
                    serve_count=serve_counts.get(row["id"], 0.0),
                    now=now,
                )
            )
            row["eligible_for_discovery"] = eligible
            row["discovery_score"] = round(discovery_score, 6)
            row["newness_score"] = round(newness_score, 6)
            row["underexposure_score"] = round(underexposure_score, 6)

        ranked_rows = self._feed_mixer_service.rank_rows(rows)
        await self._repo.rewrite_ranked_feed_rows(ranked_rows)
        self._log.info(
            "Discovery refresh applied",
            extra={"updated_count": len(ranked_rows)},
        )
