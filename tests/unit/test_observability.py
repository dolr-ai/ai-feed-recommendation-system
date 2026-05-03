from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.logging import LoggingIntegration

from src.core import observability
from src.core.settings import Settings


def test_init_sentry_registers_fastapi_integration(monkeypatch):
    init_kwargs = {}
    settings = Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        sentry_enabled=True,
        sentry_dsn="https://public@example.com/1",
    )

    monkeypatch.setattr(observability, "_sentry_initialized", False)
    monkeypatch.setattr(
        observability.sentry_sdk,
        "init",
        lambda **kwargs: init_kwargs.update(kwargs),
    )

    assert observability.init_sentry(settings) is True
    assert any(
        isinstance(integration, FastApiIntegration)
        for integration in init_kwargs["integrations"]
    )
    logging_integration = next(
        integration
        for integration in init_kwargs["integrations"]
        if isinstance(integration, LoggingIntegration)
    )
    assert logging_integration._handler is None

    monkeypatch.setattr(observability, "_sentry_initialized", False)
