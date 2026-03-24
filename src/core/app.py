from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI

from src.core.dependencies import build_runtime_objects
from src.core.settings import get_settings
from src.jobs.discovery_boost_job import run_discovery_boost_refresh
from src.jobs.influencer_feed_job import run_influencer_feed_sync
from src.routers.health import router as health_router
from src.routers.influencer_feed import router as influencer_feed_router
from src.services.logger_service import LoggerService
from src.utils.kvrocks import build_kvrocks_client


def scheduler_next_run_time(run_on_startup: bool) -> Optional[datetime]:
    return datetime.now() if run_on_startup else None


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    logger_service = LoggerService()
    logger_service.configure(settings.log_level)
    log = logger_service.get("app")
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
        if settings.feed_sync_run_on_startup:
            log.info("Full sync startup run enabled")
        else:
            log.info("Full sync startup run disabled")

        if settings.discovery_refresh_run_on_startup:
            log.info("Discovery refresh startup run enabled")
        else:
            log.info("Discovery refresh startup run disabled")

        if (
            not settings.feed_sync_run_on_startup
            and not settings.discovery_refresh_run_on_startup
        ):
            log.info("Jobs scheduled for future intervals only")
    else:
        log.info("Scheduler disabled; no background jobs scheduled")

    app.state.kvrocks = kvrocks
    app.state.scheduler = scheduler
    for key, value in runtime.items():
        setattr(app.state, key, value)

    yield

    if scheduler is not None:
        scheduler.shutdown(wait=False)
    await runtime["chat_api_client"].close()
    if hasattr(kvrocks, "aclose"):
        await kvrocks.aclose()
    elif hasattr(kvrocks, "close"):
        await kvrocks.close()


def create_app() -> FastAPI:
    app = FastAPI(title="Influencer Feed API", lifespan=lifespan)
    app.include_router(influencer_feed_router)
    app.include_router(health_router)
    return app


app = create_app()
