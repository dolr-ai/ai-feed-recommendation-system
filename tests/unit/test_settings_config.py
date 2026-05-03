from pathlib import Path

import pytest
from pydantic import ValidationError

from src.config import load_env_alias_overrides, load_env_overrides, load_file_config
from src.core.settings import Settings, get_settings


def test_load_file_config_reads_flat_toml(tmp_path: Path):
    config_path = tmp_path / "feed.toml"
    config_path.write_text(
        'kvrocks_port = 7777\n'
        'log_level = "DEBUG"\n'
        'chat_api_base_url = "https://example.com"\n'
        'ic_gateway_base_url = "https://ic0.app"\n'
        'profile_canister_id = "profile-id"\n'
        'posts_canister_id = "posts-id"\n'
    )

    config = load_file_config(str(config_path))

    assert config["kvrocks_port"] == 7777
    assert config["log_level"] == "DEBUG"


def test_env_overrides_beat_file_values(monkeypatch):
    monkeypatch.setenv("KVROCKS_PORT", "9999")
    overrides = load_env_overrides(["kvrocks_port"])

    merged = {
        "kvrocks_port": 7777,
        "chat_api_base_url": "https://example.com",
        "ic_gateway_base_url": "https://ic0.app",
        "profile_canister_id": "profile-id",
        "posts_canister_id": "posts-id",
        **overrides,
    }
    settings = Settings(**merged)

    assert settings.kvrocks_port == 9999


def test_load_env_alias_overrides_maps_reader_credentials(tmp_path: Path):
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        'CLICKHOUSE_READER_USERNAME="reader-user"\n'
        'CLICKHOUSE_READER_PASSWORD="reader-pass"\n'
    )

    overrides = load_env_alias_overrides(
        {
            "CLICKHOUSE_READER_USERNAME": "clickhouse_username",
            "CLICKHOUSE_READER_PASSWORD": "clickhouse_password",
        },
        dotenv_path=str(dotenv_path),
    )

    assert overrides == {
        "clickhouse_username": "reader-user",
        "clickhouse_password": "reader-pass",
    }


def test_get_settings_uses_reader_alias_when_direct_clickhouse_env_is_absent(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.delenv("CLICKHOUSE_USERNAME", raising=False)
    monkeypatch.delenv("CLICKHOUSE_PASSWORD", raising=False)
    monkeypatch.setenv("CLICKHOUSE_READER_USERNAME", "reader-user")
    monkeypatch.setenv("CLICKHOUSE_READER_PASSWORD", "reader-pass")
    monkeypatch.setenv("CHAT_API_BASE_URL", "https://example.com")
    monkeypatch.setenv("IC_GATEWAY_BASE_URL", "https://ic0.app")
    monkeypatch.setenv("PROFILE_CANISTER_ID", "profile-id")
    monkeypatch.setenv("POSTS_CANISTER_ID", "posts-id")

    settings = get_settings()

    assert settings.clickhouse_username == "reader-user"
    assert settings.clickhouse_password == "reader-pass"

    get_settings.cache_clear()


def test_required_endpoint_and_canister_values_must_come_from_config_or_env():
    with pytest.raises(ValidationError):
        Settings()


def test_curated_top_influencer_ids_accepts_csv_values():
    settings = Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        curated_top_influencer_ids="id-1, id-2 ,, id-3",
    )

    assert settings.curated_top_influencer_ids == ["id-1", "id-2", "id-3"]
