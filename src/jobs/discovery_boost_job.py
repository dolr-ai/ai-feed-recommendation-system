from src.core.dependencies import build_runtime_objects
from src.core.settings import get_settings
from src.services.logger_service import LoggerService
from src.utils.job_lock import acquire_job_lock, release_job_lock


async def run_discovery_boost_refresh(kvrocks_client) -> None:
    settings = get_settings()
    log = LoggerService().get("discovery_boost_job")
    job_name = "influencer_discovery_boost"

    acquired = await acquire_job_lock(kvrocks_client, job_name, ttl=300)
    if not acquired:
        log.info("Discovery refresh job already running, skipping", extra={"job": job_name})
        return

    runtime = build_runtime_objects(kvrocks_client, settings)
    try:
        await runtime["discovery_boost_service"].refresh()
    except Exception as exc:
        log.error("Discovery refresh failed", extra={"error": str(exc)})
    finally:
        await runtime["chat_api_client"].close()
        await release_job_lock(kvrocks_client, job_name)
