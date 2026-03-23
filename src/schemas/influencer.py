from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


class FeedQueryParams(BaseModel):
    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=50, ge=1, le=100)
    with_metadata: bool = False


class InfluencerScores(BaseModel):
    engagement_score: float
    discovery_score: float
    momentum_score: float
    newness_score: float
    underexposure_score: float
    content_activity_score: float
    depth_quality_score: float


class InfluencerRanking(BaseModel):
    final_rank: int
    engagement_rank: int
    discovery_rank: Optional[int]
    selected_rank_source: str
    eligible_for_discovery: bool


class InfluencerSignals(BaseModel):
    conversation_count: int
    message_count: int
    followers_count: int
    total_video_views: int
    share_count_total: int
    likes_count_total: int
    avg_watch_pct_mean: float
    ready_post_count: int
    last_post_at: Optional[datetime]
    depth_ratio: float
    share_rate: float
    views_per_post: float
    likes_per_post: float
    posting_density: float
    conv_velocity: float


class InfluencerResponse(BaseModel):
    id: str
    name: str
    display_name: str
    avatar_url: str
    description: str
    category: str
    created_at: datetime
    scores: Optional[InfluencerScores] = None
    ranking: Optional[InfluencerRanking] = None
    signals: Optional[InfluencerSignals] = None


class FeedResponse(BaseModel):
    influencers: List[InfluencerResponse]
    total_count: int
    offset: int
    limit: int
    has_more: bool
    feed_generated_at: datetime
