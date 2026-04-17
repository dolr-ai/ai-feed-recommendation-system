from __future__ import annotations

import logging
from typing import Any, Optional

import sentry_sdk
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import (
    LoggingIntegration,
    ignore_logger,
    ignore_logger_for_sentry_logs,
)
from sentry_sdk.integrations.starlette import StarletteIntegration

from src.core.settings import Settings, get_settings

_sentry_initialized = False
_TRANSIENT_PATTERNS = (
    "refill lock already held",
    "key already exists",
)
_EXPECTED_HTTP_MESSAGES = (
    "feed not yet available. pipeline has not completed.",
)


def _get_event_message(event: dict[str, Any]) -> str:
    logentry = event.get("logentry") or {}
    if isinstance(logentry, dict):
        formatted = logentry.get("formatted")
        if isinstance(formatted, str) and formatted:
            return formatted.lower()
        message = logentry.get("message")
        if isinstance(message, str) and message:
            return message.lower()

    message = event.get("message")
    if isinstance(message, str):
        return message.lower()

    return ""


def _should_drop_exception(exc_type: type[BaseException], exc_value: BaseException) -> bool:
    message = str(exc_value).lower()
    status_code = getattr(exc_value, "status_code", None)

    if exc_type.__name__ in {"TimeoutError", "ConnectionResetError"}:
        return True

    if status_code == 503 and any(
        expected in message for expected in _EXPECTED_HTTP_MESSAGES
    ):
        return True

    return any(pattern in message for pattern in _TRANSIENT_PATTERNS)


def filter_sentry_event(event: dict[str, Any], hint: dict[str, Any]) -> Optional[dict[str, Any]]:
    exc_info = hint.get("exc_info")
    if exc_info:
        exc_type, exc_value, _ = exc_info
        if exc_type is not None and exc_value is not None:
            if _should_drop_exception(exc_type, exc_value):
                return None

    message = _get_event_message(event)
    if any(pattern in message for pattern in _TRANSIENT_PATTERNS):
        return None
    if any(expected in message for expected in _EXPECTED_HTTP_MESSAGES):
        return None

    return event


def _configure_sentry_logger_filters() -> None:
    ignore_logger("uvicorn.access")
    ignore_logger_for_sentry_logs("uvicorn.access")
    ignore_logger_for_sentry_logs("uvicorn.error")


def init_sentry(settings: Optional[Settings] = None) -> bool:
    global _sentry_initialized

    if _sentry_initialized:
        return False

    if settings is None:
        try:
            settings = get_settings()
        except Exception:
            # Allow app import in tests/tooling that do not provide full runtime env yet.
            return False

    if not settings.sentry_enabled:
        return False

    dsn = settings.sentry_dsn.strip()
    if not dsn:
        return False

    _configure_sentry_logger_filters()

    sentry_sdk.init(
        dsn=dsn,
        environment=settings.sentry_environment,
        release=settings.sentry_release.strip() or None,
        server_name=settings.sentry_server_name.strip() or None,
        send_default_pii=settings.sentry_send_default_pii,
        enable_logs=settings.sentry_enable_logs,
        debug=settings.sentry_debug,
        shutdown_timeout=settings.sentry_shutdown_timeout_sec,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        profiles_sample_rate=settings.sentry_profiles_sample_rate,
        before_send=filter_sentry_event,
        integrations=[
            StarletteIntegration(transaction_style="url"),
            FastApiIntegration(transaction_style="url"),
            LoggingIntegration(
                level=logging.INFO,
                event_level=logging.ERROR,
                sentry_logs_level=logging.INFO if settings.sentry_enable_logs else None,
            ),
        ],
    )
    _sentry_initialized = True
    return True


def sentry_is_active() -> bool:
    return sentry_sdk.get_client().is_active()


def flush_sentry(timeout: float) -> None:
    if sentry_is_active():
        sentry_sdk.flush(timeout=timeout)


def capture_sentry_message(
    message: str,
    *,
    level: str = "info",
    tags: Optional[dict[str, str]] = None,
    contexts: Optional[dict[str, Any]] = None,
    extras: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    if not sentry_is_active():
        return None

    with sentry_sdk.new_scope() as scope:
        if tags:
            for key, value in tags.items():
                scope.set_tag(key, value)
        if contexts:
            for key, value in contexts.items():
                scope.set_context(key, value)
        if extras:
            for key, value in extras.items():
                scope.set_extra(key, value)
        return scope.capture_message(message, level=level)


def emit_sentry_startup_test_event(settings: Settings) -> Optional[str]:
    message = settings.sentry_startup_test_message.strip()
    if not message:
        return None

    event_id = capture_sentry_message(
        message,
        level="info",
        tags={"sentry_test_kind": "startup"},
        contexts={
            "sentry_startup_test": {
                "environment": settings.sentry_environment,
            }
        },
    )
    flush_sentry(settings.sentry_shutdown_timeout_sec)
    return event_id
