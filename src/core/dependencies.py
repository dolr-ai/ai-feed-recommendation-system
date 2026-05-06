from fastapi import Request

from src.clients.canister_client import CanisterClient
from src.clients.chat_api_client import ChatApiClient
from src.clients.clickhouse_client import build_clickhouse_client
from src.clients.metadata_service_client import MetadataServiceClient
from src.clients.offchain_rewards_client import OffchainRewardsClient
from src.core.settings import get_settings
from src.repository.checkpoint_repository import CheckpointRepository
from src.repository.clickhouse_feed_repository import ClickHouseFeedRepository
from src.repository.clickhouse_video_metadata_repository import (
    ClickHouseVideoMetadataRepository,
)
from src.repository.influencer_repository import InfluencerRepository
from src.repository.kv_feed_repository import KVFeedRepository
from src.repository.kv_publisher_profile_repository import KVPublisherProfileRepository
from src.repository.kv_video_metadata_repository import KVVideoMetadataRepository
from src.services.feed_pool_service import FeedPoolService
from src.services.feed_sync_service import FeedSyncService
from src.services.discovery_boost_service import DiscoveryBoostService
from src.services.feed_service import FeedService
from src.services.feed_mixer_service import FeedMixerService
from src.services.pipeline_service import PipelineService
from src.services.publisher_profile_enrichment_service import (
    PublisherProfileEnrichmentService,
)
from src.services.recommend_with_metadata_service import RecommendWithMetadataService
from src.services.scoring_service import ScoringService
from src.services.video_metadata_service import VideoMetadataService


def build_runtime_objects(kvrocks_client, settings=None) -> dict:
    resolved_settings = settings or get_settings()
    clickhouse_client = build_clickhouse_client(resolved_settings)
    clickhouse_feed_repository = ClickHouseFeedRepository(
        clickhouse_client,
        resolved_settings,
    )
    clickhouse_video_metadata_repository = ClickHouseVideoMetadataRepository(
        clickhouse_client,
        resolved_settings,
    )
    kv_feed_repository = KVFeedRepository(kvrocks_client, resolved_settings)
    kv_publisher_profile_repository = KVPublisherProfileRepository(
        kvrocks_client,
        resolved_settings,
    )
    kv_video_metadata_repository = KVVideoMetadataRepository(
        kvrocks_client,
        resolved_settings,
    )
    repo = InfluencerRepository(kvrocks_client, resolved_settings)
    checkpoint_repo = CheckpointRepository(kvrocks_client, resolved_settings)
    chat_api_client = ChatApiClient(resolved_settings)
    offchain_rewards_client = OffchainRewardsClient(resolved_settings)
    canister_client = CanisterClient(resolved_settings)
    request_metadata_service_client = MetadataServiceClient(
        resolved_settings,
        timeout_sec=resolved_settings.feed_recsys_request_metadata_timeout_sec,
    )
    background_metadata_service_client = MetadataServiceClient(
        resolved_settings,
        timeout_sec=resolved_settings.feed_recsys_background_metadata_timeout_sec,
        max_retries=resolved_settings.feed_recsys_background_metadata_max_retries,
        retry_backoff_sec=resolved_settings.feed_recsys_background_metadata_retry_backoff_sec,
    )
    request_canister_client = CanisterClient(
        resolved_settings,
        http_timeout_sec=resolved_settings.feed_recsys_request_ic_profile_timeout_sec,
        query_retries=resolved_settings.feed_recsys_request_ic_profile_retries,
    )
    scoring_service = ScoringService(resolved_settings)
    feed_mixer_service = FeedMixerService(resolved_settings)
    feed_service = FeedService(repo)
    publisher_profile_enrichment_service = PublisherProfileEnrichmentService(
        clickhouse_feed_repository=clickhouse_feed_repository,
        kv_publisher_profile_repository=kv_publisher_profile_repository,
        request_metadata_service_client=request_metadata_service_client,
        background_metadata_service_client=background_metadata_service_client,
        request_canister_client=request_canister_client,
        background_canister_client=canister_client,
        settings=resolved_settings,
    )
    feed_sync_service = FeedSyncService(
        clickhouse_feed_repository=clickhouse_feed_repository,
        clickhouse_video_metadata_repository=clickhouse_video_metadata_repository,
        kv_video_metadata_repository=kv_video_metadata_repository,
        kv_feed_repository=kv_feed_repository,
        chat_api_client=chat_api_client,
        settings=resolved_settings,
        publisher_profile_enrichment_service=publisher_profile_enrichment_service,
    )
    video_metadata_service = VideoMetadataService(
        clickhouse_video_metadata_repository=clickhouse_video_metadata_repository,
        kv_video_metadata_repository=kv_video_metadata_repository,
        kv_feed_repository=kv_feed_repository,
        offchain_rewards_client=offchain_rewards_client,
        settings=resolved_settings,
    )
    feed_pool_service = FeedPoolService(
        kv_feed_repository=kv_feed_repository,
        feed_sync_service=feed_sync_service,
        settings=resolved_settings,
    )
    recommend_with_metadata_service = RecommendWithMetadataService(
        feed_pool_service=feed_pool_service,
        video_metadata_service=video_metadata_service,
        publisher_profile_enrichment_service=publisher_profile_enrichment_service,
    )
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
        "clickhouse_client": clickhouse_client,
        "clickhouse_feed_repository": clickhouse_feed_repository,
        "clickhouse_video_metadata_repository": clickhouse_video_metadata_repository,
        "kv_feed_repository": kv_feed_repository,
        "kv_publisher_profile_repository": kv_publisher_profile_repository,
        "kv_video_metadata_repository": kv_video_metadata_repository,
        "repo": repo,
        "checkpoint_repo": checkpoint_repo,
        "chat_api_client": chat_api_client,
        "request_metadata_service_client": request_metadata_service_client,
        "background_metadata_service_client": background_metadata_service_client,
        "offchain_rewards_client": offchain_rewards_client,
        "canister_client": canister_client,
        "request_canister_client": request_canister_client,
        "scoring_service": scoring_service,
        "feed_mixer_service": feed_mixer_service,
        "feed_service": feed_service,
        "publisher_profile_enrichment_service": publisher_profile_enrichment_service,
        "feed_sync_service": feed_sync_service,
        "video_metadata_service": video_metadata_service,
        "feed_pool_service": feed_pool_service,
        "recommend_with_metadata_service": recommend_with_metadata_service,
        "discovery_boost_service": discovery_boost_service,
        "pipeline_service": pipeline_service,
    }


async def close_runtime_objects(runtime: dict) -> None:
    feed_pool_service = runtime.get("feed_pool_service")
    if feed_pool_service is not None and hasattr(feed_pool_service, "close"):
        await feed_pool_service.close()

    await runtime["chat_api_client"].close()
    request_metadata_service_client = runtime.get("request_metadata_service_client")
    if request_metadata_service_client is not None:
        await request_metadata_service_client.close()
    background_metadata_service_client = runtime.get("background_metadata_service_client")
    if background_metadata_service_client is not None:
        await background_metadata_service_client.close()
    await runtime["offchain_rewards_client"].close()
    await runtime["clickhouse_client"].close()


def get_kvrocks(request: Request):
    return request.app.state.kvrocks


def get_clickhouse_client(request: Request):
    return request.app.state.clickhouse_client


def get_clickhouse_feed_repository(request: Request):
    return request.app.state.clickhouse_feed_repository


def get_kv_feed_repository(request: Request):
    return request.app.state.kv_feed_repository


def get_feed_sync_service(request: Request):
    return request.app.state.feed_sync_service


def get_recommend_with_metadata_service(request: Request):
    return request.app.state.recommend_with_metadata_service


def get_repo(request: Request):
    return request.app.state.repo


def get_feed_service(request: Request):
    return request.app.state.feed_service


def get_discovery_boost_service(request: Request):
    return request.app.state.discovery_boost_service


def get_pipeline_service(request: Request):
    return request.app.state.pipeline_service
