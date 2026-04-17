from datetime import datetime, timezone

from src.models.influencer import Influencer, PipelineState
from src.services.logger_service import LoggerService


class PipelineService:
    def __init__(
        self,
        chat_api_client,
        canister_client,
        scoring_service,
        feed_mixer_service,
        influencer_repo,
        checkpoint_repo,
        settings,
    ):
        self._chat_api_client = chat_api_client
        self._canister_client = canister_client
        self._scoring_service = scoring_service
        self._feed_mixer_service = feed_mixer_service
        self._influencer_repo = influencer_repo
        self._checkpoint_repo = checkpoint_repo
        self._settings = settings
        self._log = LoggerService().get("pipeline")

    async def run(self) -> None:
        try:
            self._log.info("Pipeline step starting", extra={"step": "fetch_influencers"})
            base_records = await self._run_step(
                "fetch_influencers",
                self._chat_api_client.get_all_influencers,
            )
            influencers_map = self._build_influencer_map(base_records)
            await self._save_step("fetch_influencers", True, len(influencers_map))
            self._log.info(
                "Pipeline step completed",
                extra={"step": "fetch_influencers", "record_count": len(influencers_map)},
            )

            self._log.info("Pipeline step starting", extra={"step": "fetch_chat_stats"})
            trending_records = await self._run_step(
                "fetch_chat_stats",
                self._chat_api_client.get_trending,
            )
            self._merge_trending(influencers_map, trending_records)
            await self._save_step("fetch_chat_stats", True, len(trending_records))
            self._log.info(
                "Pipeline step completed",
                extra={"step": "fetch_chat_stats", "record_count": len(trending_records)},
            )

            self._log.info(
                "Pipeline step starting",
                extra={
                    "step": "fetch_canister_data",
                    "record_count": len(influencers_map),
                    "note": "This is the slowest step because canister calls are rate-limited.",
                },
            )
            influencers = await self._run_step(
                "fetch_canister_data",
                lambda: self._canister_client.fetch_all(list(influencers_map.values())),
            )
            await self._save_step("fetch_canister_data", True, len(influencers))
            canister_stats = self._canister_client.get_stats()
            self._log.info(
                "Pipeline step completed",
                extra={"step": "fetch_canister_data", "record_count": len(influencers)},
            )
            self._log.info(
                "Canister fetch summary",
                extra=canister_stats,
            )

            self._log.info("Pipeline step starting", extra={"step": "compute_scores"})
            snapshot_map = await self._run_step(
                "compute_scores",
                self._influencer_repo.load_stats_snapshot,
            )
            if not snapshot_map:
                self._log.info("No prior stats snapshot found; using zero velocities")

            serve_counts_map = await self._influencer_repo.get_all_serve_counts(
                [influencer.id for influencer in influencers]
            )
            lane_scores = self._scoring_service.score(
                influencers=influencers,
                snapshot_map=snapshot_map,
                serve_counts_map=serve_counts_map,
            )
            scores = self._feed_mixer_service.rank_scores(lane_scores)
            await self._save_step("compute_scores", True, len(scores))
            self._log.info(
                "Pipeline step completed",
                extra={"step": "compute_scores", "record_count": len(scores)},
            )

            self._log.info("Pipeline step starting", extra={"step": "write_to_kvrocks"})
            await self._run_step(
                "write_to_kvrocks",
                lambda: self._influencer_repo.write_ranked_feed(scores, influencers),
            )
            await self._run_step(
                "write_to_kvrocks",
                lambda: self._influencer_repo.write_stats_snapshot(influencers),
            )
            await self._save_step("write_to_kvrocks", True, len(scores))
            await self._checkpoint_repo.delete_all()
            self._log.info(
                "Pipeline step completed",
                extra={"step": "write_to_kvrocks", "record_count": len(scores)},
            )
        except Exception as exc:
            step = self._infer_failed_step(exc)
            self._log.exception(
                "Pipeline step failed",
                extra={"step": step, "error": str(exc)},
            )
            await self._save_step(step, False, 0, str(exc))
            raise

    def _build_influencer_map(self, records: list[dict]) -> dict[str, Influencer]:
        result: dict[str, Influencer] = {}
        for item in records:
            if item.get("is_active") != "active":
                continue
            created_at = self._parse_datetime(item["created_at"])
            influencer = Influencer(
                id=item["id"],
                name=item.get("name", ""),
                display_name=item.get("display_name", ""),
                avatar_url=item.get("avatar_url", ""),
                description=item.get("description", ""),
                category=item.get("category", ""),
                created_at=created_at,
                is_active=item["is_active"],
            )
            result[influencer.id] = influencer
        return result

    @staticmethod
    def _merge_trending(influencers_map: dict[str, Influencer], trending_records: list[dict]) -> None:
        trending_map = {item["id"]: item for item in trending_records}
        for influencer in influencers_map.values():
            stats = trending_map.get(influencer.id)
            if stats:
                influencer.conversation_count = int(stats.get("conversation_count") or 0)
                influencer.message_count = int(stats.get("message_count") or 0)

    async def _save_step(
        self,
        step: str,
        success: bool,
        record_count: int,
        error: str | None = None,
    ) -> None:
        await self._checkpoint_repo.save(
            PipelineState(
                step=step,
                timestamp=datetime.now(timezone.utc),
                success=success,
                record_count=record_count,
                error=error,
            )
        )

    async def _run_step(self, step: str, operation):
        try:
            return await operation()
        except Exception as exc:
            setattr(exc, "pipeline_step", step)
            raise

    @staticmethod
    def _parse_datetime(value: str) -> datetime:
        normalized = value.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        if parsed.tzinfo is None:
            return parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc)

    @staticmethod
    def _infer_failed_step(exc: Exception) -> str:
        return getattr(exc, "pipeline_step", "unknown")
