from typing import Annotated

from fastapi import APIRouter, Depends, Header, Query

from src.core.dependencies import get_kv_feed_repository, get_recommend_with_metadata_service
from src.schemas.feed_recsys import (
    FeedRecType,
    FeedRecommendationWithMetadataResponse,
    FeedViewCountPushResponse,
    FeedViewCountSnapshotRow,
)
from src.services.recommend_with_metadata_service import RecommendWithMetadataService

FEED_RECSYS_ROUTER_PREFIX = "/api/v1"
INTERNAL_VIEW_COUNTS_PATH = "/internal/feed-recsys/view-counts"
INTERNAL_VIEW_COUNTS_FULL_PATH = f"{FEED_RECSYS_ROUTER_PREFIX}{INTERNAL_VIEW_COUNTS_PATH}"

router = APIRouter(prefix=FEED_RECSYS_ROUTER_PREFIX, tags=["feed-recsys"])

VIEW_COUNTS_PUSH_DESCRIPTION = """
Internal endpoint for offchain to warm recsys' cached aggregate video view counts.
Recsys stores `total_count_all` as `num_views_all`; do not send logged-in counts
or rewards fields.

Auth type: internal HMAC. Include:

- `x-internal-timestamp`: Unix epoch seconds.
- `x-internal-signature`: hex HMAC-SHA256 of:

"""


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
    INTERNAL_VIEW_COUNTS_PATH,
    summary="Push aggregate video view counts",
    description=VIEW_COUNTS_PUSH_DESCRIPTION,
    response_model=FeedViewCountPushResponse,
)
async def push_view_counts(
    rows: list[FeedViewCountSnapshotRow],
    x_internal_timestamp: Annotated[
        str,
        Header(
            alias="x-internal-timestamp",
            description="Unix epoch seconds included in the HMAC signature.",
        ),
    ],
    x_internal_signature: Annotated[
        str,
        Header(
            alias="x-internal-signature",
            description="Hex HMAC-SHA256 signature for this request.",
        ),
    ],
    kv_feed_repository=Depends(get_kv_feed_repository),
):
    merged_rows: dict[str, dict[str, int]] = {}
    for row in rows:
        existing = merged_rows.setdefault(
            row.video_id,
            {
                "num_views_all": 0,
            },
        )
        existing["num_views_all"] = max(
            existing["num_views_all"],
            row.total_count_all,
        )

    upserted = await kv_feed_repository.upsert_video_view_counts(merged_rows)
    return FeedViewCountPushResponse(received=len(rows), upserted=upserted)
