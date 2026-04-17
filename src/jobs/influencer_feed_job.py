from src.core.dependencies import build_runtime_objects
from src.core.settings import get_settings
from src.services.logger_service import LoggerService
from src.utils.job_lock import acquire_job_lock, release_job_lock


async def run_influencer_feed_sync(kvrocks_client) -> None:
    settings = get_settings()
    log = LoggerService().get("influencer_feed_job")
    job_name = "influencer_feed_sync"

    acquired = await acquire_job_lock(
        kvrocks_client,
        job_name,
        ttl=settings.feed_sync_interval_sec,
    )
    if not acquired:
        log.info("Job already running, skipping", extra={"job": job_name})
        return

    runtime = build_runtime_objects(kvrocks_client, settings)
    try:
        await runtime["pipeline_service"].run()
    except Exception as exc:
        log.exception(
            "Pipeline failed",
            extra={"job": job_name, "error": str(exc)},
        )
    finally:
        await runtime["chat_api_client"].close()
        await release_job_lock(kvrocks_client, job_name)
