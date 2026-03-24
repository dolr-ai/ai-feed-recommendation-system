import asyncio

from fastapi import APIRouter, Depends, HTTPException

from src.core.dependencies import get_feed_service, get_repo
from src.schemas.influencer import FeedQueryParams, FeedResponse
from src.services.feed_service import FeedNotReadyError, FeedService

router = APIRouter(prefix="/api/v1", tags=["influencer-feed"])


@router.get(
    "/influencer-feed",
    response_model=FeedResponse,
    response_model_exclude_none=True,
    response_model_exclude_unset=True,
)
async def get_influencer_feed(
    params: FeedQueryParams = Depends(),
    feed_service: FeedService = Depends(get_feed_service),
    repo=Depends(get_repo),
):
    try:
        feed = await feed_service.get_feed(
            offset=params.offset,
            limit=params.limit,
            with_metadata=params.with_metadata,
        )
    except FeedNotReadyError as exc:
        raise HTTPException(
            status_code=503,
            detail="Feed not yet available. Pipeline has not completed.",
            headers={"Retry-After": "60"},
        ) from exc

    asyncio.create_task(
        repo.increment_serve_counts(
            ids=[influencer.id for influencer in feed.influencers],
            offset=params.offset,
        )
    )
    return feed
