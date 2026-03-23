import argparse
import asyncio
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.clients.canister_client import CanisterClient
from src.clients.chat_api_client import ChatApiClient
from src.core.settings import get_settings
from src.repository.influencer_repository import InfluencerRepository
from src.services.scoring_service import ScoringService
from src.utils.kvrocks import build_kvrocks_client


async def main(principal_id: str) -> None:
    get_settings.cache_clear()
    settings = get_settings()
    chat = ChatApiClient(settings)
    canister = CanisterClient(settings)
    kvrocks = await build_kvrocks_client(settings)
    repo = InfluencerRepository(kvrocks, settings)
    scorer = ScoringService(settings)
    try:
        records = await chat.get_all_influencers()
        target = next((item for item in records if item["id"] == principal_id), None)
        if target is None:
            print(json.dumps({"error": "principal not found in chat api", "principal_id": principal_id}, indent=2))
            return

        profile = await canister.get_profile(principal_id)
        posts = await canister.get_posts(principal_id, cursor=0, limit=10)
        metadata = await repo.get_metadata(principal_id)

        from src.models.influencer import Influencer
        from src.services.pipeline_service import PipelineService

        influencer = Influencer(
            id=target["id"],
            name=target.get("name", ""),
            display_name=target.get("display_name", ""),
            avatar_url=target.get("avatar_url", ""),
            description=target.get("description", ""),
            category=target.get("category", ""),
            created_at=PipelineService._parse_datetime(target["created_at"]),
            is_active=target["is_active"],
        )

        profile_followers = profile.get("Ok", {}).get("followers_count", 0)
        influencer.followers_count = int(profile_followers) if "Ok" in profile else 0
        ready = [post for post in posts if post.get("status") == "ReadyToView"]
        influencer.ready_post_count = len(ready)
        if ready:
            influencer.total_video_views = sum(
                int(post["view_stats"]["total_view_count"]) for post in ready
            )
            influencer.share_count_total = sum(int(post.get("share_count", 0)) for post in ready)
            influencer.likes_count_total = sum(len(post.get("likes", [])) for post in ready)
            watch_pcts = [
                float(post["view_stats"]["average_watch_percentage"])
                for post in ready
                if "view_stats" in post
            ]
            influencer.avg_watch_pct_mean = sum(watch_pcts) / len(watch_pcts) if watch_pcts else 0.0

        score = scorer.score([influencer], snapshot_map={}, serve_counts_map={})[0]

        print(
            json.dumps(
                {
                    "chat_api_record": {
                        "id": target["id"],
                        "name": target.get("name"),
                        "display_name": target.get("display_name"),
                        "category": target.get("category"),
                        "created_at": target.get("created_at"),
                    },
                    "canister_profile": profile,
                    "ready_posts_count": len(ready),
                    "canister_posts_sample": posts[:2],
                    "computed_score": {
                        "effective_score": score.effective_score,
                        "composite_score": score.composite_score,
                        "discovery_boost": score.discovery_boost,
                        "popularity_score": score.popularity_score,
                        "virality_score": score.virality_score,
                        "freshness_activity_score": score.freshness_activity_score,
                    },
                    "stored_metadata": metadata,
                },
                indent=2,
                default=str,
            )
        )
    finally:
        await chat.close()
        if hasattr(kvrocks, "aclose"):
            await kvrocks.aclose()
        else:
            await kvrocks.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("principal_id")
    args = parser.parse_args()
    asyncio.run(main(args.principal_id))
