from abc import ABC, abstractmethod
from datetime import datetime
from typing import Dict, List, Optional

from src.models.influencer import (
    Influencer,
    InfluencerScore,
    PipelineState,
    StatsSnapshot,
)


class BaseWriteRepo(ABC):
    @abstractmethod
    async def write_ranked_feed(
        self,
        scores: List[InfluencerScore],
        influencers: List[Influencer],
    ) -> None:
        ...

    @abstractmethod
    async def write_stats_snapshot(
        self,
        influencers: List[Influencer],
    ) -> None:
        ...

    @abstractmethod
    async def rewrite_ranked_feed_rows(
        self,
        rows: List[dict],
    ) -> None:
        ...


class BaseReadRepo(ABC):
    @abstractmethod
    async def get_feed(
        self,
        offset: int,
        limit: int,
    ) -> tuple[list[dict], int, Optional[datetime]]:
        ...

    @abstractmethod
    async def get_serve_count(self, influencer_id: str) -> float:
        ...

    @abstractmethod
    async def get_all_serve_counts(
        self,
        influencer_ids: List[str],
    ) -> Dict[str, float]:
        ...

    @abstractmethod
    async def increment_serve_counts(self, ids: List[str], offset: int) -> None:
        ...

    @abstractmethod
    async def load_stats_snapshot(self) -> Dict[str, StatsSnapshot]:
        ...

    @abstractmethod
    async def get_metadata(self, influencer_id: str) -> Optional[dict]:
        ...

    @abstractmethod
    async def get_all_metadata(self) -> List[dict]:
        ...


class BaseCheckpointRepo(ABC):
    @abstractmethod
    async def save(self, state: PipelineState) -> None:
        ...

    @abstractmethod
    async def delete(self, step: str) -> None:
        ...

    @abstractmethod
    async def delete_all(self) -> None:
        ...
