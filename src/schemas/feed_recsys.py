from typing import Dict, List, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


FeedRecType = Literal["mixed", "popularity", "freshness", "following", "ugc", "fallback"]


class FeedRecommendationQueryParams(BaseModel):
    count: int = Field(default=20, ge=1, le=500)
    rec_type: FeedRecType = "mixed"


class FeedVideoMetadata(BaseModel):
    video_id: str
    canister_id: str
    post_id: str
    publisher_user_id: str
    num_views_loggedin: int = Field(default=0)
    num_views_all: int = Field(default=0)
    from_ai_influencer: bool = Field(default=False)
    is_following: bool = Field(default=False)
    username: str = Field(default="")
    is_pro_user: bool = Field(default=False)
    profile_image_url: str = Field(default="")


class FeedRecommendationWithMetadataResponse(BaseModel):
    user_id: str
    videos: List[FeedVideoMetadata]
    count: int
    sources: Dict[str, int]
    timestamp: int


class FeedViewCountSnapshotRow(BaseModel):
    model_config = ConfigDict(extra="ignore")

    video_id: str = Field(min_length=1)
    total_count_loggedin: int = Field(ge=0)
    total_count_all: int = Field(ge=0)
    count: int | None = Field(default=None, ge=0)
    last_milestone: int | None = Field(default=None, ge=0)

    @field_validator("video_id")
    @classmethod
    def validate_video_id(cls, value: str) -> str:
        normalized = value.strip()
        if not normalized:
            raise ValueError("video_id must not be blank")
        return normalized


class FeedViewCountPushResponse(BaseModel):
    received: int
    upserted: int
