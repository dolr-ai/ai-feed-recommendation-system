from src.core.settings import Settings
from src.utils.feed_recsys_keys import (
    ai_influencer_ids_key,
    excluded_videos_key,
    feed_recsys_prefix,
    global_pool_key,
    job_lock_key,
    user_bloom_key,
    user_pool_key,
    user_popularity_pointer_key,
    user_refill_lock_key,
    user_served_recent_key,
    video_metadata_key,
    video_view_count_key,
)


def build_settings(**overrides) -> Settings:
    return Settings(
        chat_api_base_url="https://example.com",
        ic_gateway_base_url="https://ic0.app",
        profile_canister_id="profile-id",
        posts_canister_id="posts-id",
        **overrides,
    )


def test_feed_recsys_key_builders_use_storage_namespace():
    settings = build_settings(storage_namespace="staging")

    assert feed_recsys_prefix(settings) == "staging:feed_recsys"
    assert user_pool_key(settings, "user-1", "popular") == (
        "staging:feed_recsys:{user:user-1}:pool:popular"
    )
    assert user_bloom_key(settings, "user-1") == "staging:feed_recsys:{user:user-1}:bloom"
    assert user_served_recent_key(settings, "user-1") == (
        "staging:feed_recsys:{user:user-1}:served_recent"
    )
    assert user_popularity_pointer_key(settings, "user-1") == (
        "staging:feed_recsys:{user:user-1}:pop_percentile_pointer"
    )
    assert user_refill_lock_key(settings, "user-1", "ugc") == (
        "staging:feed_recsys:{user:user-1}:refill:lock:ugc"
    )
    assert global_pool_key(settings, "popular:l7d") == (
        "staging:feed_recsys:{GLOBAL}:pool:popular:l7d"
    )
    assert excluded_videos_key(settings) == "staging:feed_recsys:{GLOBAL}:exclude:videos"
    assert ai_influencer_ids_key(settings) == "staging:feed_recsys:{GLOBAL}:lookup:ai_influencer_ids"
    assert video_view_count_key(settings, "video-1") == (
        "staging:feed_recsys:{GLOBAL}:view_counts:video-1"
    )
    assert job_lock_key(settings, "freshness_sync") == (
        "staging:feed_recsys:{GLOBAL}:jobs:lock:freshness_sync"
    )


def test_video_metadata_key_stays_on_shared_offchain_prefix():
    assert video_metadata_key("video-123") == "offchain:metadata:video_details:video-123"
