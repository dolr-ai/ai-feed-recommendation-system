from typing import Dict, List, Literal

from pydantic import BaseModel, Field


FeedRecType = Literal["mixed", "popularity", "freshness", "following", "ugc", "fallback"]


class FeedRecommendationQueryParams(BaseModel):
    count: int = Field(default=100, ge=1, le=500)
    rec_type: FeedRecType = "mixed"


class FeedVideoMetadata(BaseModel):
    video_id: str
    canister_id: str
    post_id: str
    publisher_user_id: str
    num_views_loggedin: int = Field(default=0)
    num_views_all: int = Field(default=0)
    from_ai_influencer: bool = Field(default=False)


class FeedRecommendationWithMetadataResponse(BaseModel):
    user_id: str
    videos: List[FeedVideoMetadata]
    count: int
    sources: Dict[str, int]
    timestamp: int
