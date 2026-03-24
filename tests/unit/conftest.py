from datetime import datetime, timedelta, timezone

import pytest

from src.config import load_file_config
from src.core.settings import Settings
from src.models.influencer import Influencer, StatsSnapshot


@pytest.fixture
def settings() -> Settings:
    return Settings(**load_file_config())


@pytest.fixture
def sample_influencers() -> list[Influencer]:
    now = datetime.now(timezone.utc)
    return [
        Influencer(
            id="new-hot",
            name="new-hot",
            display_name="New Hot",
            avatar_url="https://example.com/a.png",
            description="New influencer",
            category="gaming",
            created_at=now - timedelta(days=1),
            is_active="active",
            conversation_count=40,
            message_count=400,
            followers_count=120,
            total_video_views=10_000,
            share_count_total=400,
            likes_count_total=900,
            avg_watch_pct_mean=75.0,
            ready_post_count=5,
            last_post_at=now - timedelta(hours=8),
        ),
        Influencer(
            id="old-stable",
            name="old-stable",
            display_name="Old Stable",
            avatar_url="https://example.com/b.png",
            description="Old influencer",
            category="tech",
            created_at=now - timedelta(days=30),
            is_active="active",
            conversation_count=2_000,
            message_count=8_000,
            followers_count=20_000,
            total_video_views=5_000,
            share_count_total=80,
            likes_count_total=500,
            avg_watch_pct_mean=55.0,
            ready_post_count=4,
            last_post_at=now - timedelta(days=3),
        ),
        Influencer(
            id="new-overexposed",
            name="new-overexposed",
            display_name="New Overexposed",
            avatar_url="https://example.com/c.png",
            description="Too many convos",
            category="music",
            created_at=now - timedelta(days=2),
            is_active="active",
            conversation_count=700,
            message_count=1_400,
            followers_count=300,
            total_video_views=3_000,
            share_count_total=60,
            likes_count_total=100,
            avg_watch_pct_mean=30.0,
            ready_post_count=2,
            last_post_at=now - timedelta(days=1),
        ),
    ]


@pytest.fixture
def sample_snapshot_map(sample_influencers) -> dict[str, StatsSnapshot]:
    now = datetime.now(timezone.utc)
    return {
        "new-hot": StatsSnapshot(
            influencer_id="new-hot",
            conversation_count=10,
            message_count=100,
            followers_count=100,
            total_video_views=5_000,
            share_count_total=100,
            captured_at=now - timedelta(hours=6),
        ),
        "old-stable": StatsSnapshot(
            influencer_id="old-stable",
            conversation_count=1_800,
            message_count=7_500,
            followers_count=19_800,
            total_video_views=4_900,
            share_count_total=70,
            captured_at=now - timedelta(hours=6),
        ),
        "new-overexposed": StatsSnapshot(
            influencer_id="new-overexposed",
            conversation_count=650,
            message_count=1_300,
            followers_count=280,
            total_video_views=2_800,
            share_count_total=50,
            captured_at=now - timedelta(hours=6),
        ),
    }
