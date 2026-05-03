from src.core.dependencies import build_runtime_objects, close_runtime_objects
from src.core.settings import get_settings
from src.services.logger_service import LoggerService
from src.utils.feed_recsys_keys import job_lock_key


async def run_feed_recsys_popularity_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "popularity_sync",
        get_settings().feed_recsys_popularity_sync_interval_sec,
        "sync_global_popularity_pools",
    )


async def run_feed_recsys_freshness_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "freshness_sync",
        get_settings().feed_recsys_freshness_sync_interval_sec,
        "sync_fresh_pools",
    )


async def run_feed_recsys_bloom_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "bloom_sync",
        get_settings().feed_recsys_bloom_sync_interval_sec,
        "sync_user_bloom_filters",
    )


async def run_feed_recsys_ugc_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "ugc_sync",
        get_settings().feed_recsys_ugc_sync_interval_sec,
        "sync_ugc_pool",
    )


async def run_feed_recsys_ugc_discovery_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "ugc_discovery_sync",
        get_settings().feed_recsys_ugc_discovery_sync_interval_sec,
        "sync_ugc_discovery_pool",
    )


async def run_feed_recsys_exclude_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "exclude_sync",
        get_settings().feed_recsys_exclude_sync_interval_sec,
        "sync_excluded_videos",
    )


async def run_feed_recsys_ai_influencer_sync(kvrocks_client) -> None:
    await _run_locked_job(
        kvrocks_client,
        "ai_influencer_sync",
        get_settings().feed_recsys_ai_influencer_sync_interval_sec,
        "sync_ai_influencer_ids",
    )


async def _run_locked_job(
    kvrocks_client,
    job_name: str,
    ttl: int,
    service_method_name: str,
) -> None:
    settings = get_settings()
    lock_key = job_lock_key(settings, job_name)
    log = LoggerService().get(f"feed_recsys_job.{job_name}")

    acquired = await kvrocks_client.set(lock_key, "1", ex=ttl, nx=True)
    if not acquired:
        log.debug("Feed recsys job already running, skipping", extra={"job": job_name})
        return

    runtime = build_runtime_objects(kvrocks_client, settings)
    try:
        service_method = getattr(runtime["feed_sync_service"], service_method_name)
        await service_method()
    except Exception:
        log.exception("Feed recsys job failed", extra={"job": job_name})
        raise
    finally:
        await close_runtime_objects(runtime)
        await kvrocks_client.delete(lock_key)
