import os
from datetime import datetime, timedelta, timezone

import pytest

from src.config import load_file_config
from src.core.settings import Settings
from src.models.influencer import Influencer, InfluencerScore
from src.repository.influencer_repository import InfluencerRepository
from src.utils.kvrocks import (
    build_kvrocks_client,
    feed_meta_key,
    metadata_key,
    ranked_feed_key,
    serve_counts_key,
    stats_snapshot_key,
)


def _require_test_kvrocks() -> tuple[str, int]:
    host = os.getenv("TEST_KVROCKS_HOST")
    port = os.getenv("TEST_KVROCKS_PORT")
    if not host or not port:
        pytest.skip("TEST_KVROCKS_HOST/TEST_KVROCKS_PORT not set")
    return host, int(port)


@pytest.fixture
def integration_settings() -> Settings:
    host, port = _require_test_kvrocks()
    merged = {
        **load_file_config(),
        "kvrocks_host": host,
        "kvrocks_port": port,
        "kvrocks_password": os.getenv("TEST_KVROCKS_PASSWORD", ""),
        "kvrocks_tls_enabled": os.getenv("TEST_KVROCKS_TLS_ENABLED", "false").lower()
        == "true",
        "kvrocks_cluster_enabled": os.getenv(
            "TEST_KVROCKS_CLUSTER_ENABLED",
            "false",
        ).lower()
        == "true",
    }
    return Settings(**merged)


@pytest.fixture
async def kvrocks_client(integration_settings):
    client = await build_kvrocks_client(integration_settings)
    await client.delete(
        ranked_feed_key(),
        metadata_key(),
        feed_meta_key(),
        stats_snapshot_key(),
        serve_counts_key(),
    )
    yield client
    await client.delete(
        ranked_feed_key(),
        metadata_key(),
        feed_meta_key(),
        stats_snapshot_key(),
        serve_counts_key(),
    )
    if hasattr(client, "aclose"):
        await client.aclose()
    elif hasattr(client, "close"):
        await client.close()


@pytest.fixture
def repo(kvrocks_client, integration_settings):
    return InfluencerRepository(kvrocks_client, integration_settings)


@pytest.fixture
def sample_feed_rows():
    now = datetime.now(timezone.utc)
    influencers = [
        Influencer(
            id="i1",
            name="one",
            display_name="One",
            avatar_url="https://example.com/1.png",
            description="one",
            category="gaming",
            created_at=now - timedelta(days=1),
            is_active="active",
            conversation_count=100,
            message_count=500,
            followers_count=200,
            total_video_views=1000,
            share_count_total=20,
            likes_count_total=50,
            avg_watch_pct_mean=60.0,
            ready_post_count=4,
            last_post_at=now - timedelta(hours=5),
            depth_ratio=5.0,
            share_rate=0.02,
            views_per_post=250.0,
            likes_per_post=12.5,
            posting_density=0.8,
        ),
        Influencer(
            id="i2",
            name="two",
            display_name="Two",
            avatar_url="https://example.com/2.png",
            description="two",
            category="music",
            created_at=now - timedelta(days=2),
            is_active="active",
            conversation_count=80,
            message_count=320,
            followers_count=150,
            total_video_views=800,
            share_count_total=15,
            likes_count_total=40,
            avg_watch_pct_mean=55.0,
            ready_post_count=3,
            last_post_at=now - timedelta(hours=12),
            depth_ratio=4.0,
            share_rate=0.01875,
            views_per_post=266.67,
            likes_per_post=13.33,
            posting_density=0.6,
        ),
    ]
    scores = [
        InfluencerScore(
            influencer_id="i1",
            engagement_score=0.7,
            discovery_score=0.4,
            momentum_score=0.5,
            newness_score=0.8,
            underexposure_score=0.5,
            content_activity_score=0.4,
            depth_quality_score=0.3,
            eligible_for_discovery=True,
            engagement_rank=1,
            discovery_rank=1,
            final_rank=1,
            selected_rank_source="engagement",
            depth_ratio=5.0,
            share_rate=0.02,
            conv_velocity=1.5,
        ),
        InfluencerScore(
            influencer_id="i2",
            engagement_score=0.5,
            discovery_score=0.0,
            momentum_score=0.4,
            newness_score=0.4,
            underexposure_score=0.3,
            content_activity_score=0.2,
            depth_quality_score=0.2,
            eligible_for_discovery=False,
            engagement_rank=2,
            discovery_rank=None,
            final_rank=2,
            selected_rank_source="engagement",
            depth_ratio=4.0,
            share_rate=0.01875,
            conv_velocity=1.0,
        ),
    ]
    return influencers, scores
