from fastapi import APIRouter, Depends, Query

from src.core.dependencies import get_kv_feed_repository, get_recommend_with_metadata_service
from src.schemas.feed_recsys import (
    FeedRecType,
    FeedRecommendationWithMetadataResponse,
    FeedViewCountPushResponse,
    FeedViewCountSnapshotRow,
)
from src.services.recommend_with_metadata_service import RecommendWithMetadataService

router = APIRouter(prefix="/api/v1", tags=["feed-recsys"])


@router.get(
    "/recommend-with-metadata/{user_id}",
    response_model=FeedRecommendationWithMetadataResponse,
    response_model_exclude_none=True,
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


@router.post(
    "/internal/feed-recsys/view-counts",
    response_model=FeedViewCountPushResponse,
)
async def push_view_counts(
    rows: list[FeedViewCountSnapshotRow],
    kv_feed_repository=Depends(get_kv_feed_repository),
):
    merged_rows: dict[str, dict[str, int]] = {}
    for row in rows:
        existing = merged_rows.setdefault(
            row.video_id,
            {
                "num_views_loggedin": 0,
                "num_views_all": 0,
            },
        )
        existing["num_views_loggedin"] = max(
            existing["num_views_loggedin"],
            row.total_count_loggedin,
        )
        existing["num_views_all"] = max(
            existing["num_views_all"],
            row.total_count_all,
        )

    upserted = await kv_feed_repository.upsert_video_view_counts(merged_rows)
    return FeedViewCountPushResponse(received=len(rows), upserted=upserted)
