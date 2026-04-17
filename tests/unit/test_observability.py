from __future__ import annotations

from fastapi import HTTPException

from src.core import observability


def test_init_sentry_configures_integrations(settings, monkeypatch):
    captured: dict[str, object] = {}
    ignored_loggers: list[str] = []
    ignored_sentry_loggers: list[str] = []

    def fake_init(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr(observability, "_sentry_initialized", False)
    monkeypatch.setattr(observability.sentry_sdk, "init", fake_init)
    monkeypatch.setattr(observability, "ignore_logger", ignored_loggers.append)
    monkeypatch.setattr(
        observability,
        "ignore_logger_for_sentry_logs",
        ignored_sentry_loggers.append,
    )

    configured = settings.model_copy(
        update={
            "sentry_enabled": True,
            "sentry_dsn": "https://public@example.com/24",
            "sentry_environment": "staging",
            "sentry_enable_logs": True,
            "sentry_debug": True,
            "sentry_release": "recommendation-system@2026.04.15",
            "sentry_server_name": "recsys-worker-01",
            "sentry_shutdown_timeout_sec": 7,
        }
    )

    initialized = observability.init_sentry(configured)

    assert initialized is True
    assert captured["dsn"] == "https://public@example.com/24"
    assert captured["environment"] == "staging"
    assert captured["debug"] is True
    assert captured["release"] == "recommendation-system@2026.04.15"
    assert captured["server_name"] == "recsys-worker-01"
    assert captured["shutdown_timeout"] == 7
    assert captured["before_send"] is observability.filter_sentry_event
    integration_names = {
        type(integration).__name__ for integration in captured["integrations"]
    }
    assert integration_names == {
        "FastApiIntegration",
        "StarletteIntegration",
        "LoggingIntegration",
    }
    assert ignored_loggers == ["uvicorn.access"]
    assert ignored_sentry_loggers == ["uvicorn.access", "uvicorn.error"]


def test_init_sentry_skips_when_disabled(settings, monkeypatch):
    monkeypatch.setattr(observability, "_sentry_initialized", False)

    configured = settings.model_copy(
        update={
            "sentry_enabled": False,
            "sentry_dsn": "https://public@example.com/24",
        }
    )

    assert observability.init_sentry(configured) is False


def test_filter_sentry_event_drops_expected_feed_not_ready_event():
    exc = HTTPException(
        status_code=503,
        detail="Feed not yet available. Pipeline has not completed.",
    )
    event = {
        "logentry": {
            "formatted": "Feed not yet available. Pipeline has not completed.",
        }
    }
    hint = {"exc_info": (HTTPException, exc, None)}

    assert observability.filter_sentry_event(event, hint) is None


def test_emit_sentry_startup_test_event_flushes(settings, monkeypatch):
    flushed: dict[str, object] = {}

    monkeypatch.setattr(
        observability,
        "capture_sentry_message",
        lambda *args, **kwargs: "startup-event-id",
    )
    monkeypatch.setattr(
        observability,
        "flush_sentry",
        lambda timeout: flushed.setdefault("timeout", timeout),
    )

    configured = settings.model_copy(
        update={
            "sentry_startup_test_message": "startup sentry test",
            "sentry_shutdown_timeout_sec": 9,
        }
    )

    event_id = observability.emit_sentry_startup_test_event(configured)

    assert event_id == "startup-event-id"
    assert flushed["timeout"] == 9
