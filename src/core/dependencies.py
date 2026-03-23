from fastapi import Request

from src.clients.canister_client import CanisterClient
from src.clients.chat_api_client import ChatApiClient
from src.core.settings import get_settings
from src.repository.checkpoint_repository import CheckpointRepository
from src.repository.influencer_repository import InfluencerRepository
from src.services.discovery_boost_service import DiscoveryBoostService
from src.services.feed_service import FeedService
from src.services.feed_mixer_service import FeedMixerService
from src.services.pipeline_service import PipelineService
from src.services.scoring_service import ScoringService


def build_runtime_objects(kvrocks_client, settings=None) -> dict:
    resolved_settings = settings or get_settings()
    repo = InfluencerRepository(kvrocks_client, resolved_settings)
    checkpoint_repo = CheckpointRepository(kvrocks_client, resolved_settings)
    chat_api_client = ChatApiClient(resolved_settings)
    canister_client = CanisterClient(resolved_settings)
    scoring_service = ScoringService(resolved_settings)
    feed_mixer_service = FeedMixerService(resolved_settings)
    feed_service = FeedService(repo)
    discovery_boost_service = DiscoveryBoostService(
        repo,
        scoring_service,
        feed_mixer_service,
        resolved_settings,
    )
    pipeline_service = PipelineService(
        chat_api_client=chat_api_client,
        canister_client=canister_client,
        scoring_service=scoring_service,
        feed_mixer_service=feed_mixer_service,
        influencer_repo=repo,
        checkpoint_repo=checkpoint_repo,
        settings=resolved_settings,
    )
    return {
        "settings": resolved_settings,
        "repo": repo,
        "checkpoint_repo": checkpoint_repo,
        "chat_api_client": chat_api_client,
        "canister_client": canister_client,
        "scoring_service": scoring_service,
        "feed_mixer_service": feed_mixer_service,
        "feed_service": feed_service,
        "discovery_boost_service": discovery_boost_service,
        "pipeline_service": pipeline_service,
    }


def get_kvrocks(request: Request):
    return request.app.state.kvrocks


def get_repo(request: Request):
    return request.app.state.repo


def get_feed_service(request: Request):
    return request.app.state.feed_service


def get_discovery_boost_service(request: Request):
    return request.app.state.discovery_boost_service


def get_pipeline_service(request: Request):
    return request.app.state.pipeline_service
