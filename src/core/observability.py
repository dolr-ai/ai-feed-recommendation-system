import logging
from typing import Optional

import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration

from src.core.settings import Settings, get_settings

_sentry_initialized = False


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

    sentry_sdk.init(
        dsn=dsn,
        environment=settings.sentry_environment,
        send_default_pii=settings.sentry_send_default_pii,
        enable_logs=settings.sentry_enable_logs,
        traces_sample_rate=settings.sentry_traces_sample_rate,
        profiles_sample_rate=settings.sentry_profiles_sample_rate,
        integrations=[
            LoggingIntegration(level=logging.INFO, event_level=logging.ERROR),
        ],
    )
    _sentry_initialized = True
    return True


def emit_sentry_startup_test_event(settings: Settings) -> bool:
    message = settings.sentry_startup_test_message.strip()
    if not (_sentry_initialized and message):
        return False

    sentry_sdk.capture_message(message, level="info")
    return True
