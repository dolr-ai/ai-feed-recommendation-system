from typing import Callable, List, Optional, Sequence, Tuple, TypeVar

from src.models.influencer import InfluencerScore

T = TypeVar("T")


class FeedMixerService:
    def __init__(self, settings):
        self._pattern = [
            token.strip().upper()
            for token in settings.mixer_pattern.split(",")
            if token.strip()
        ]

    def rank_scores(self, scores: List[InfluencerScore]) -> List[InfluencerScore]:
        engagement_sorted = sorted(
            scores,
            key=lambda score: (
                -score.engagement_score,
                -score.depth_quality_score,
                score.influencer_id,
            ),
        )
        discovery_sorted = sorted(
            [
                score
                for score in scores
                if score.eligible_for_discovery and score.discovery_score > 0.0
            ],
            key=lambda score: (
                -score.discovery_score,
                -score.momentum_score,
                -score.newness_score,
                score.influencer_id,
            ),
        )

        for idx, score in enumerate(engagement_sorted, start=1):
            score.engagement_rank = idx
            score.discovery_rank = None
            score.final_rank = 0
            score.selected_rank_source = ""

        for idx, score in enumerate(discovery_sorted, start=1):
            score.discovery_rank = idx

        mixed = self._mix(
            engagement_items=engagement_sorted,
            discovery_items=discovery_sorted,
            get_id=lambda score: score.influencer_id,
        )
        for idx, (score, source) in enumerate(mixed, start=1):
            score.final_rank = idx
            score.selected_rank_source = source

        return [score for score, _ in mixed]

    def rank_rows(self, rows: List[dict]) -> List[dict]:
        engagement_sorted = sorted(
            rows,
            key=lambda row: (
                -float(row["engagement_score"]),
                -float(row.get("depth_quality_score", 0.0)),
                row["id"],
            ),
        )
        discovery_sorted = sorted(
            [
                row
                for row in rows
                if row.get("eligible_for_discovery") and float(row["discovery_score"]) > 0.0
            ],
            key=lambda row: (
                -float(row["discovery_score"]),
                -float(row.get("momentum_score", 0.0)),
                -float(row.get("newness_score", 0.0)),
                row["id"],
            ),
        )

        for idx, row in enumerate(engagement_sorted, start=1):
            row["engagement_rank"] = idx
            row["discovery_rank"] = None
            row["final_rank"] = 0
            row["selected_rank_source"] = ""

        for idx, row in enumerate(discovery_sorted, start=1):
            row["discovery_rank"] = idx

        mixed = self._mix(
            engagement_items=engagement_sorted,
            discovery_items=discovery_sorted,
            get_id=lambda row: row["id"],
        )
        for idx, (row, source) in enumerate(mixed, start=1):
            row["final_rank"] = idx
            row["selected_rank_source"] = source

        return [row for row, _ in mixed]

    def _mix(
        self,
        engagement_items: Sequence[T],
        discovery_items: Sequence[T],
        get_id: Callable[[T], str],
    ) -> List[Tuple[T, str]]:
        seen: set[str] = set()
        mixed: List[Tuple[T, str]] = []
        engagement_idx = 0
        discovery_idx = 0
        total_items = len(engagement_items)

        while len(mixed) < total_items:
            progressed = False
            for slot in self._pattern:
                if len(mixed) >= total_items:
                    break

                if slot == "E":
                    candidate, engagement_idx = self._next_unseen(
                        engagement_items,
                        engagement_idx,
                        seen,
                        get_id,
                    )
                    source = "engagement"
                    if candidate is None:
                        candidate, discovery_idx = self._next_unseen(
                            discovery_items,
                            discovery_idx,
                            seen,
                            get_id,
                        )
                        source = "discovery"
                else:
                    candidate, discovery_idx = self._next_unseen(
                        discovery_items,
                        discovery_idx,
                        seen,
                        get_id,
                    )
                    source = "discovery"
                    if candidate is None:
                        candidate, engagement_idx = self._next_unseen(
                            engagement_items,
                            engagement_idx,
                            seen,
                            get_id,
                        )
                        source = "engagement"

                if candidate is None:
                    continue

                seen.add(get_id(candidate))
                mixed.append((candidate, source))
                progressed = True

            if not progressed:
                break

        while len(mixed) < total_items:
            candidate, engagement_idx = self._next_unseen(
                engagement_items,
                engagement_idx,
                seen,
                get_id,
            )
            if candidate is None:
                break
            seen.add(get_id(candidate))
            mixed.append((candidate, "engagement"))

        return mixed

    @staticmethod
    def _next_unseen(
        items: Sequence[T],
        start_idx: int,
        seen: set[str],
        get_id: Callable[[T], str],
    ) -> tuple[Optional[T], int]:
        idx = start_idx
        while idx < len(items):
            candidate = items[idx]
            idx += 1
            if get_id(candidate) in seen:
                continue
            return candidate, idx
        return None, idx
