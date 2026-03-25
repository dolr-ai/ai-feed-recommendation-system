import json
from functools import lru_cache
from typing import Any

from pydantic import ConfigDict, Field, field_validator, model_validator

from src.config import load_env_overrides, load_file_config

try:
    from pydantic_settings import BaseSettings, SettingsConfigDict
except ModuleNotFoundError:  # pragma: no cover - local fallback when extras are absent
    from pydantic import BaseModel

    class BaseSettings(BaseModel):
        model_config = ConfigDict(extra="ignore")

    def SettingsConfigDict(**kwargs):
        return ConfigDict(**kwargs)


class Settings(BaseSettings):
    storage_namespace: str = "prod"

    kvrocks_host: str = "localhost"
    kvrocks_port: int = 6379
    kvrocks_password: str = ""
    kvrocks_tls_enabled: bool = False
    kvrocks_cluster_enabled: bool = False
    kvrocks_ssl_ca_cert: str = ""
    kvrocks_ssl_client_cert: str = ""
    kvrocks_ssl_client_key: str = ""

    chat_api_base_url: str = Field(...)
    chat_api_timeout: int = 30

    ic_gateway_base_url: str = Field(...)
    profile_canister_id: str = Field(...)
    posts_canister_id: str = Field(...)
    canister_rps: float = 25.0
    canister_concurrency: int = 10
    canister_http_timeout_sec: float = 15.0
    canister_query_retries: int = 2
    canister_retry_backoff_sec: float = 0.5

    engagement_w_conversation: float = 0.45
    engagement_w_messages: float = 0.30
    engagement_w_followers: float = 0.15
    engagement_w_depth_quality: float = 0.10

    discovery_w_newness: float = 0.35
    discovery_w_momentum: float = 0.30
    discovery_w_underexposure: float = 0.20
    discovery_w_content_activity: float = 0.10
    discovery_w_depth_quality: float = 0.05

    content_w_post_recency: float = 0.40
    content_w_posting_density: float = 0.30
    content_w_views_per_post: float = 0.20
    content_w_share_rate: float = 0.10

    new_bot_age_threshold_days: int = 7
    content_post_recency_halflife_days: int = 7

    discovery_max_conversations: int = 200
    discovery_min_conversations: int = 3
    discovery_min_messages: int = 20
    discovery_min_followers: int = 2
    depth_confidence_full_at_conversations: int = 20

    mom_w_conv: float = 0.35
    mom_w_messages: float = 0.25
    mom_w_shares: float = 0.20
    mom_w_views: float = 0.10
    mom_w_followers: float = 0.10

    serve_penalty_per_count: float = 0.0015
    mixer_pattern: str = "E,E,D,E,D,E,E,D,E,D"
    curated_top_influencer_ids: list[str] = Field(default_factory=list)

    norm_lower_percentile: float = 5.0
    norm_upper_percentile: float = 95.0

    feed_sync_interval_sec: int = 21600
    discovery_refresh_interval_sec: int = 1800
    scheduler_enabled: bool = True
    feed_sync_run_on_startup: bool = True
    discovery_refresh_run_on_startup: bool = False
    scheduler_misfire_grace_time_sec: int = 300

    feed_ttl_sec: int = 90000
    snapshot_ttl_sec: int = 90000
    checkpoint_ttl_sec: int = 86400
    serve_count_ttl_sec: int = 2592000

    feed_default_limit: int = 50
    feed_max_limit: int = 100

    log_level: str = "INFO"

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    @field_validator("mixer_pattern")
    @classmethod
    def validate_mixer_pattern(cls, value: str) -> str:
        tokens = [token.strip().upper() for token in value.split(",") if token.strip()]
        if len(tokens) != 10:
            raise ValueError("mixer_pattern must contain exactly 10 comma-separated slots")
        if any(token not in {"E", "D"} for token in tokens):
            raise ValueError("mixer_pattern tokens must be only E or D")
        if tokens.count("E") != 6 or tokens.count("D") != 4:
            raise ValueError("mixer_pattern must contain 6 E slots and 4 D slots")
        return ",".join(tokens)

    @field_validator("curated_top_influencer_ids", mode="before")
    @classmethod
    def validate_curated_top_influencer_ids(cls, value: Any) -> list[str]:
        if value is None or value == "":
            return []

        raw_items: list[Any]
        if isinstance(value, str):
            stripped = value.strip()
            if not stripped:
                return []
            if stripped.startswith("["):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    raw_items = stripped.split(",")
                else:
                    raw_items = parsed if isinstance(parsed, list) else [parsed]
            else:
                raw_items = stripped.split(",")
        elif isinstance(value, (list, tuple)):
            raw_items = list(value)
        else:
            raise ValueError("curated_top_influencer_ids must be a list or comma-separated string")

        normalized = [str(item).strip() for item in raw_items if str(item).strip()]
        if len(normalized) != len(set(normalized)):
            raise ValueError("curated_top_influencer_ids must not contain duplicates")
        return normalized

    @model_validator(mode="after")
    def validate_weight_groups(self) -> "Settings":
        groups = {
            "engagement": [
                self.engagement_w_conversation,
                self.engagement_w_messages,
                self.engagement_w_followers,
                self.engagement_w_depth_quality,
            ],
            "discovery": [
                self.discovery_w_newness,
                self.discovery_w_momentum,
                self.discovery_w_underexposure,
                self.discovery_w_content_activity,
                self.discovery_w_depth_quality,
            ],
            "content_activity": [
                self.content_w_post_recency,
                self.content_w_posting_density,
                self.content_w_views_per_post,
                self.content_w_share_rate,
            ],
            "momentum": [
                self.mom_w_conv,
                self.mom_w_messages,
                self.mom_w_shares,
                self.mom_w_views,
                self.mom_w_followers,
            ],
        }
        for group_name, values in groups.items():
            total = sum(values)
            if abs(total - 1.0) > 1e-6:
                raise ValueError(f"{group_name} weights must sum to 1.0, got {total}")
        return self


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    file_values = load_file_config()
    env_values = load_env_overrides(Settings.model_fields.keys())
    merged = {**file_values, **env_values}
    return Settings(**merged)
