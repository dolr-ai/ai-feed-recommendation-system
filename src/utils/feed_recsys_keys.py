from __future__ import annotations

from src.core.settings import Settings


def feed_recsys_prefix(settings: Settings) -> str:
    namespace = (settings.storage_namespace or "prod").strip() or "prod"
    return f"{namespace}:feed_recsys"


def _user_slot(user_id: str) -> str:
    return f"{{user:{user_id}}}"


def _global_slot() -> str:
    return "{GLOBAL}"


def _publisher_profile_slot() -> str:
    return "{PUBLISHER_PROFILE}"


def user_pool_key(settings: Settings, user_id: str, pool_name: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_user_slot(user_id)}:pool:{pool_name}"


def user_bloom_key(settings: Settings, user_id: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_user_slot(user_id)}:bloom"


def user_served_recent_key(settings: Settings, user_id: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_user_slot(user_id)}:served_recent"


def following_sync_users_key(settings: Settings) -> str:
    return f"{feed_recsys_prefix(settings)}:{_global_slot()}:following:users"


def user_popularity_pointer_key(settings: Settings, user_id: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_user_slot(user_id)}:pop_percentile_pointer"


def user_refill_lock_key(settings: Settings, user_id: str, pool_name: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_user_slot(user_id)}:refill:lock:{pool_name}"


def global_pool_key(settings: Settings, pool_name: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_global_slot()}:pool:{pool_name}"


def excluded_videos_key(settings: Settings) -> str:
    return f"{feed_recsys_prefix(settings)}:{_global_slot()}:exclude:videos"


def ai_influencer_ids_key(settings: Settings) -> str:
    return f"{feed_recsys_prefix(settings)}:{_global_slot()}:lookup:ai_influencer_ids"


def job_lock_key(settings: Settings, job_name: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_global_slot()}:jobs:lock:{job_name}"


def video_view_count_key(settings: Settings, video_id: str) -> str:
    return f"{feed_recsys_prefix(settings)}:{_global_slot()}:view_counts:{video_id}"


def publisher_profile_key(settings: Settings, publisher_user_id: str) -> str:
    return (
        f"{feed_recsys_prefix(settings)}:{_publisher_profile_slot()}:"
        f"publisher:{publisher_user_id}"
    )


def publisher_profile_refresh_queue_key(settings: Settings) -> str:
    return f"{feed_recsys_prefix(settings)}:{_publisher_profile_slot()}:queue:refresh"


def publisher_profile_warmup_queue_key(settings: Settings) -> str:
    return f"{feed_recsys_prefix(settings)}:{_publisher_profile_slot()}:queue:warmup"


def publisher_profile_refresh_lock_key(
    settings: Settings,
    publisher_user_id: str,
) -> str:
    return (
        f"{feed_recsys_prefix(settings)}:{_publisher_profile_slot()}:"
        f"refresh:lock:{publisher_user_id}"
    )


def video_metadata_key(video_id: str) -> str:
    return f"offchain:metadata:video_details:{video_id}"
