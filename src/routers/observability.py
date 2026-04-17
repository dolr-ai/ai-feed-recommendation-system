from __future__ import annotations

from uuid import uuid4

from fastapi import APIRouter, Depends, Header, HTTPException

from src.core.observability import capture_sentry_message, flush_sentry, sentry_is_active
from src.core.settings import Settings, get_settings
from src.services.logger_service import LoggerService

router = APIRouter(prefix="/ops/sentry", tags=["observability"])


def _require_sentry_verification_access(
    settings: Settings = Depends(get_settings),
    sentry_verification_token: str | None = Header(
        default=None,
        alias="X-Sentry-Verification-Token",
    ),
) -> Settings:
    if not settings.sentry_verification_enabled:
        raise HTTPException(status_code=404, detail="Not found")

    expected_token = settings.sentry_verification_token.strip()
    if expected_token and sentry_verification_token != expected_token:
        raise HTTPException(status_code=403, detail="Invalid verification token")

    if not settings.sentry_enabled or not sentry_is_active():
        raise HTTPException(status_code=503, detail="Sentry is not active")

    return settings


@router.post("/log-test")
async def sentry_log_test(
    settings: Settings = Depends(_require_sentry_verification_access),
):
    verification_id = str(uuid4())
    details = {
        "verification_id": verification_id,
        "environment": settings.sentry_environment,
        "route": "log-test",
    }
    log = LoggerService().get("observability")
    log.info("Sentry verification info log", extra=details)
    log.warning("Sentry verification warning log", extra=details)
    event_id = capture_sentry_message(
        "Sentry verification message",
        level="info",
        tags={
            "sentry_test_kind": "manual",
            "sentry_test_route": "log-test",
        },
        contexts={"sentry_verification": details},
        extras=details,
    )
    flush_sentry(settings.sentry_shutdown_timeout_sec)
    return {
        "status": "sent",
        "verification_id": verification_id,
        "message_event_id": event_id,
    }


@router.post("/error-test")
async def sentry_error_test(
    settings: Settings = Depends(_require_sentry_verification_access),
):
    verification_id = str(uuid4())
    details = {
        "verification_id": verification_id,
        "environment": settings.sentry_environment,
        "route": "error-test",
    }
    LoggerService().get("observability").warning(
        "Sentry verification error route invoked",
        extra=details,
    )
    raise RuntimeError(f"Sentry verification failure [{verification_id}]")
