from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from src.core.dependencies import build_runtime_objects, close_runtime_objects
from src.core.observability import emit_sentry_startup_test_event, init_sentry
from src.core.settings import get_settings
from src.jobs.feed_recsys_jobs import (
    run_feed_recsys_ai_influencer_sync,
    run_feed_recsys_bloom_sync,
    run_feed_recsys_exclude_sync,
    run_feed_recsys_freshness_sync,
    run_feed_recsys_following_sync,
    run_feed_recsys_popularity_sync,
    run_feed_recsys_ugc_sync,
)
from src.jobs.discovery_boost_job import run_discovery_boost_refresh
from src.jobs.influencer_feed_job import run_influencer_feed_sync
from src.routers.feed_recsys import router as feed_recsys_router
from src.routers.health import router as health_router
from src.routers.influencer_feed import router as influencer_feed_router
from src.services.logger_service import LoggerService
from src.utils.kvrocks import build_kvrocks_client


def scheduler_next_run_time(
    run_on_startup: bool,
    delay_sec: int = 0,
) -> Optional[datetime]:
    if not run_on_startup:
        return None
    return datetime.now() + timedelta(seconds=max(0, delay_sec))


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger_service = LoggerService()
    logger_service.configure(settings.log_level)
    init_sentry(settings)
    log = logger_service.get("app")
    log.info(
        "Feed recsys app starting",
        extra={
            "storage_namespace": settings.storage_namespace,
            "kvrocks_host": settings.kvrocks_host,
            "kvrocks_port": settings.kvrocks_port,
            "clickhouse_host": settings.clickhouse_host,
            "clickhouse_port": settings.clickhouse_port,
            "scheduler_enabled": settings.scheduler_enabled,
            "feed_recsys_jobs_enabled": settings.feed_recsys_jobs_enabled,
            "feed_recsys_job_run_on_startup": settings.feed_recsys_job_run_on_startup,
        },
    )
    if emit_sentry_startup_test_event(settings):
        log.info("Sentry startup test message emitted")
    kvrocks = await build_kvrocks_client(settings)
    runtime = build_runtime_objects(kvrocks, settings)
    scheduler = None

    if settings.scheduler_enabled:
        scheduler = AsyncIOScheduler(
            job_defaults={
                "misfire_grace_time": settings.scheduler_misfire_grace_time_sec
            }
        )
        scheduler.add_job(
            run_influencer_feed_sync,
            "interval",
            seconds=settings.feed_sync_interval_sec,
            args=[kvrocks],
            id="influencer_feed_sync",
            next_run_time=scheduler_next_run_time(settings.feed_sync_run_on_startup),
        )
        scheduler.add_job(
            run_discovery_boost_refresh,
            "interval",
            seconds=settings.discovery_refresh_interval_sec,
            args=[kvrocks],
            id="influencer_discovery_boost",
            next_run_time=scheduler_next_run_time(
                settings.discovery_refresh_run_on_startup
            ),
        )
        scheduler.start()
        log.debug(
            "Background job startup flags resolved",
            extra={
                "feed_sync_run_on_startup": settings.feed_sync_run_on_startup,
                "discovery_refresh_run_on_startup": settings.discovery_refresh_run_on_startup,
            },
        )
    else:
        log.info("Scheduler disabled; no background jobs scheduled")

    if settings.scheduler_enabled and settings.feed_recsys_jobs_enabled:
        scheduler = scheduler or AsyncIOScheduler(
            job_defaults={
                "misfire_grace_time": settings.scheduler_misfire_grace_time_sec
            }
        )
        stagger_sec = max(0, settings.feed_recsys_startup_stagger_sec)
        scheduler.add_job(
            run_feed_recsys_popularity_sync,
            "interval",
            seconds=settings.feed_recsys_popularity_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_popularity_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=0,
            ),
        )
        scheduler.add_job(
            run_feed_recsys_freshness_sync,
            "interval",
            seconds=settings.feed_recsys_freshness_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_freshness_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=stagger_sec,
            ),
        )
        scheduler.add_job(
            run_feed_recsys_bloom_sync,
            "interval",
            seconds=settings.feed_recsys_bloom_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_bloom_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=stagger_sec * 2,
            ),
        )
        scheduler.add_job(
            run_feed_recsys_ugc_sync,
            "interval",
            seconds=settings.feed_recsys_ugc_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_ugc_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=stagger_sec * 3,
            ),
        )
        scheduler.add_job(
            run_feed_recsys_following_sync,
            "interval",
            seconds=settings.feed_recsys_following_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_following_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=stagger_sec * 4,
            ),
        )
        scheduler.add_job(
            run_feed_recsys_exclude_sync,
            "interval",
            seconds=settings.feed_recsys_exclude_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_exclude_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=stagger_sec * 5,
            ),
        )
        scheduler.add_job(
            run_feed_recsys_ai_influencer_sync,
            "interval",
            seconds=settings.feed_recsys_ai_influencer_sync_interval_sec,
            args=[kvrocks],
            id="feed_recsys_ai_influencer_sync",
            next_run_time=scheduler_next_run_time(
                settings.feed_recsys_job_run_on_startup,
                delay_sec=stagger_sec * 6,
            ),
        )
        if not scheduler.running:
            scheduler.start()
        log.info(
            "Feed recsys jobs scheduled",
            extra={
                "run_on_startup": settings.feed_recsys_job_run_on_startup,
                "startup_stagger_sec": stagger_sec,
            },
        )
    else:
        log.info(
            "Feed recsys jobs not scheduled",
            extra={
                "scheduler_enabled": settings.scheduler_enabled,
                "feed_recsys_jobs_enabled": settings.feed_recsys_jobs_enabled,
                "feed_recsys_job_run_on_startup": settings.feed_recsys_job_run_on_startup,
            },
        )

    app.state.kvrocks = kvrocks
    app.state.scheduler = scheduler
    for key, value in runtime.items():
        setattr(app.state, key, value)

    yield

    if scheduler is not None:
        scheduler.shutdown(wait=False)
    await close_runtime_objects(runtime)
    if hasattr(kvrocks, "aclose"):
        await kvrocks.aclose()
    elif hasattr(kvrocks, "close"):
        await kvrocks.close()


def create_app() -> FastAPI:
    try:
        settings = get_settings()
    except Exception:
        settings = None
    if settings is not None:
        LoggerService().configure(settings.log_level)
    init_sentry(settings)
    app = FastAPI(title="Feed Recsys API", lifespan=lifespan)
    app.include_router(influencer_feed_router)
    app.include_router(feed_recsys_router)
    app.include_router(health_router)
    return app


app = create_app()
