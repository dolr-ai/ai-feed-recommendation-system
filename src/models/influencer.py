from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class Influencer:
    id: str
    name: str
    display_name: str
    avatar_url: str
    description: str
    category: str
    created_at: datetime
    is_active: str
    conversation_count: int = 0
    message_count: int = 0
    followers_count: int = 0
    total_video_views: int = 0
    share_count_total: int = 0
    likes_count_total: int = 0
    avg_watch_pct_mean: float = 0.0
    ready_post_count: int = 0
    last_post_at: Optional[datetime] = None
    depth_ratio: float = 0.0
    share_rate: float = 0.0
    views_per_post: float = 0.0
    likes_per_post: float = 0.0
    posting_density: float = 0.0


@dataclass
class InfluencerScore:
    influencer_id: str
    engagement_score: float
    discovery_score: float
    momentum_score: float
    newness_score: float
    underexposure_score: float
    content_activity_score: float
    depth_quality_score: float
    eligible_for_discovery: bool
    engagement_rank: int = 0
    discovery_rank: Optional[int] = None
    final_rank: int = 0
    selected_rank_source: str = "engagement"
    depth_ratio: float = 0.0
    share_rate: float = 0.0
    conv_velocity: float = 0.0


@dataclass
class StatsSnapshot:
    influencer_id: str
    conversation_count: int
    message_count: int
    followers_count: int
    total_video_views: int
    share_count_total: int
    captured_at: datetime


@dataclass
class PipelineState:
    step: str
    timestamp: datetime
    success: bool
    record_count: int = 0
    error: Optional[str] = None
