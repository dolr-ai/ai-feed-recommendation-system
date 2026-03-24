from fastapi import APIRouter, Depends

from src.core.dependencies import get_kvrocks
from src.utils.kvrocks import ranked_feed_key

router = APIRouter(tags=["health"])


@router.get("/health")
async def health(kvrocks=Depends(get_kvrocks)):
    kv_ok = await kvrocks.ping()
    return {
        "status": "ok" if kv_ok else "degraded",
        "kvrocks": "ok" if kv_ok else "unreachable",
    }


@router.get("/metrics")
async def metrics(kvrocks=Depends(get_kvrocks)):
    total = await kvrocks.zcard(ranked_feed_key())
    return {
        "feed_size": total,
        "status": "ready" if total > 0 else "empty",
    }
