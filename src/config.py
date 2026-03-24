from __future__ import annotations

import os
import tomllib
from pathlib import Path
from typing import Any, Iterable

DEFAULT_CONFIG_PATH = "config/influencer_feed.toml"
CONFIG_PATH_ENV_VAR = "INFLUENCER_FEED_CONFIG_FILE"


def resolve_config_path(explicit_path: str | None = None) -> Path:
    raw_path = explicit_path or os.getenv(CONFIG_PATH_ENV_VAR, DEFAULT_CONFIG_PATH)
    return Path(raw_path)


def load_file_config(explicit_path: str | None = None) -> dict[str, Any]:
    path = resolve_config_path(explicit_path)
    if not path.exists():
        return {}
    with path.open("rb") as handle:
        payload = tomllib.load(handle)
    if not isinstance(payload, dict):
        return {}
    return {
        key: value
        for key, value in payload.items()
        if not isinstance(value, dict)
    }


def load_env_overrides(field_names: Iterable[str], dotenv_path: str = ".env") -> dict[str, Any]:
    env_source = _load_dotenv_file(dotenv_path)
    env_source.update(os.environ)
    overrides: dict[str, Any] = {}
    for field_name in field_names:
        env_key = field_name.upper()
        if env_key in env_source:
            overrides[field_name] = env_source[env_key]
    return overrides


def _load_dotenv_file(path_str: str) -> dict[str, str]:
    path = Path(path_str)
    if not path.exists():
        return {}
    result: dict[str, str] = {}
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        cleaned_value = value.strip().strip("'").strip('"')
        result[key.strip()] = cleaned_value
    return result
