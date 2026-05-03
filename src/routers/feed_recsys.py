from fastapi import APIRouter, Depends, Query

from src.core.dependencies import get_recommend_with_metadata_service
from src.schemas.feed_recsys import FeedRecType, FeedRecommendationWithMetadataResponse
from src.services.recommend_with_metadata_service import RecommendWithMetadataService

router = APIRouter(prefix="/api/v1", tags=["feed-recsys"])


@router.get(
    "/recommend-with-metadata/{user_id}",
    response_model=FeedRecommendationWithMetadataResponse,
)
async def get_recommend_with_metadata(
    user_id: str,
    count: int = Query(default=20, ge=1, le=500),
    rec_type: FeedRecType = Query(default="mixed"),
    recommend_with_metadata_service: RecommendWithMetadataService = Depends(
        get_recommend_with_metadata_service
    ),
):
    return await recommend_with_metadata_service.recommend_with_metadata(
        user_id=user_id,
        count=count,
        rec_type=rec_type,
    )
