import math
from datetime import datetime, timezone
from typing import Dict, List

from src.models.influencer import Influencer, InfluencerScore, StatsSnapshot
from src.utils.math_utils import log_safe, robust_normalize


class ScoringService:
    def __init__(self, settings):
        self._settings = settings

    def score(
        self,
        influencers: List[Influencer],
        snapshot_map: Dict[str, StatsSnapshot],
        serve_counts_map: Dict[str, float],
    ) -> List[InfluencerScore]:
        now = datetime.now(timezone.utc)

        for inf in influencers:
            inf.depth_ratio = inf.message_count / max(inf.conversation_count, 1)
            inf.share_rate = inf.share_count_total / max(inf.total_video_views, 1)
            inf.views_per_post = inf.total_video_views / max(inf.ready_post_count, 1)
            inf.likes_per_post = inf.likes_count_total / max(inf.ready_post_count, 1)
            inf.posting_density = min(inf.ready_post_count / 5.0, 1.0)

        velocities = [
            self._compute_velocities(influencer, snapshot_map, now)
            for influencer in influencers
        ]
        conv_raw = [log_safe(influencer.conversation_count) for influencer in influencers]
        msg_raw = [log_safe(influencer.message_count) for influencer in influencers]
        follower_raw = [log_safe(influencer.followers_count) for influencer in influencers]
        depth_raw = [self._depth_quality_raw(influencer) for influencer in influencers]
        views_per_post_raw = [log_safe(influencer.views_per_post) for influencer in influencers]
        mom_raw = [self._momentum_raw(velocity) for velocity in velocities]

        p05 = self._settings.norm_lower_percentile
        p95 = self._settings.norm_upper_percentile
        conv_n = robust_normalize(conv_raw, p05, p95)
        msg_n = robust_normalize(msg_raw, p05, p95)
        follower_n = robust_normalize(follower_raw, p05, p95)
        depth_n = robust_normalize(depth_raw, p05, p95)
        views_per_post_n = robust_normalize(views_per_post_raw, p05, p95)
        mom_n = robust_normalize(mom_raw, p05, p95)

        scores: List[InfluencerScore] = []
        for index, influencer in enumerate(influencers):
            engagement_score = (
                conv_n[index] * self._settings.engagement_w_conversation
                + msg_n[index] * self._settings.engagement_w_messages
                + follower_n[index] * self._settings.engagement_w_followers
                + depth_n[index] * self._settings.engagement_w_depth_quality
            )
            content_activity_score = self._content_activity_score(
                influencer=influencer,
                post_recency_score=self._post_recency_score(
                    influencer.last_post_at,
                    now,
                ),
                views_per_post_score=views_per_post_n[index],
            )
            eligible_for_discovery, discovery_score, newness_score, underexposure_score = (
                self.compute_discovery_metrics(
                    created_at=influencer.created_at,
                    conversation_count=influencer.conversation_count,
                    message_count=influencer.message_count,
                    followers_count=influencer.followers_count,
                    ready_post_count=influencer.ready_post_count,
                    momentum_score=mom_n[index],
                    content_activity_score=content_activity_score,
                    depth_quality_score=depth_n[index],
                    serve_count=serve_counts_map.get(influencer.id, 0.0),
                    now=now,
                )
            )

            scores.append(
                InfluencerScore(
                    influencer_id=influencer.id,
                    engagement_score=engagement_score,
                    discovery_score=discovery_score,
                    momentum_score=mom_n[index],
                    newness_score=newness_score,
                    underexposure_score=underexposure_score,
                    content_activity_score=content_activity_score,
                    depth_quality_score=depth_n[index],
                    eligible_for_discovery=eligible_for_discovery,
                    depth_ratio=influencer.depth_ratio,
                    share_rate=influencer.share_rate,
                    conv_velocity=velocities[index]["conv_velocity"],
                )
            )
        return scores

    def compute_discovery_metrics(
        self,
        *,
        created_at: datetime,
        conversation_count: int,
        message_count: int,
        followers_count: int,
        ready_post_count: int,
        momentum_score: float,
        content_activity_score: float,
        depth_quality_score: float,
        serve_count: float,
        now: datetime,
    ) -> tuple[bool, float, float, float]:
        bot_age_days = max((now - created_at).days, 0)
        newness_score = math.exp(
            -bot_age_days / max(self._settings.new_bot_age_threshold_days, 1)
        )
        underexposure_score = 1.0 - min(
            conversation_count / max(self._settings.discovery_max_conversations, 1),
            1.0,
        )
        eligible_for_discovery = self._is_discovery_candidate(
            bot_age_days=bot_age_days,
            conversation_count=conversation_count,
            message_count=message_count,
            followers_count=followers_count,
            ready_post_count=ready_post_count,
            momentum_score=momentum_score,
        )
        if not eligible_for_discovery:
            return False, 0.0, newness_score, underexposure_score

        discovery_score = (
            newness_score * self._settings.discovery_w_newness
            + momentum_score * self._settings.discovery_w_momentum
            + underexposure_score * self._settings.discovery_w_underexposure
            + content_activity_score * self._settings.discovery_w_content_activity
            + depth_quality_score * self._settings.discovery_w_depth_quality
        )
        discovery_score = max(
            0.0,
            discovery_score - serve_count * self._settings.serve_penalty_per_count,
        )
        return True, discovery_score, newness_score, underexposure_score

    def _compute_velocities(
        self,
        influencer: Influencer,
        snapshot_map: Dict[str, StatsSnapshot],
        now: datetime,
    ) -> dict:
        snapshot = snapshot_map.get(influencer.id)
        if snapshot is None:
            return {
                "conv_velocity": 0.0,
                "message_velocity": 0.0,
                "follower_velocity": 0.0,
                "view_velocity": 0.0,
                "share_velocity": 0.0,
            }

        delta_hrs = max((now - snapshot.captured_at).total_seconds() / 3600, 1.0)
        return {
            "conv_velocity": max(0, influencer.conversation_count - snapshot.conversation_count)
            / delta_hrs,
            "message_velocity": max(0, influencer.message_count - snapshot.message_count)
            / delta_hrs,
            "follower_velocity": max(0, influencer.followers_count - snapshot.followers_count)
            / delta_hrs,
            "view_velocity": max(0, influencer.total_video_views - snapshot.total_video_views)
            / delta_hrs,
            "share_velocity": max(0, influencer.share_count_total - snapshot.share_count_total)
            / delta_hrs,
        }

    def _momentum_raw(self, velocities: dict) -> float:
        return (
            log_safe(velocities["conv_velocity"]) * self._settings.mom_w_conv
            + log_safe(velocities["message_velocity"]) * self._settings.mom_w_messages
            + log_safe(velocities["share_velocity"]) * self._settings.mom_w_shares
            + log_safe(velocities["view_velocity"]) * self._settings.mom_w_views
            + log_safe(velocities["follower_velocity"]) * self._settings.mom_w_followers
        )

    def _depth_quality_raw(
        self,
        influencer: Influencer,
    ) -> float:
        depth_confidence = min(
            influencer.conversation_count
            / max(self._settings.depth_confidence_full_at_conversations, 1),
            1.0,
        )
        return log_safe(influencer.depth_ratio) * depth_confidence

    def _post_recency_score(
        self,
        last_post_at: datetime | None,
        now: datetime,
    ) -> float:
        if last_post_at is None:
            return 0.0
        days_since_post = max((now - last_post_at).days, 0)
        return math.exp(
            -days_since_post
            / max(self._settings.content_post_recency_halflife_days, 1),
        )

    def _content_activity_score(
        self,
        *,
        influencer: Influencer,
        post_recency_score: float,
        views_per_post_score: float,
    ) -> float:
        return (
            post_recency_score * self._settings.content_w_post_recency
            + influencer.posting_density * self._settings.content_w_posting_density
            + views_per_post_score * self._settings.content_w_views_per_post
            + min(influencer.share_rate, 1.0) * self._settings.content_w_share_rate
        )

    def _is_discovery_candidate(
        self,
        *,
        bot_age_days: int,
        conversation_count: int,
        message_count: int,
        followers_count: int,
        ready_post_count: int,
        momentum_score: float,
    ) -> bool:
        if bot_age_days > self._settings.new_bot_age_threshold_days:
            return False
        if conversation_count >= self._settings.discovery_max_conversations:
            return False
        return any(
            [
                conversation_count >= self._settings.discovery_min_conversations,
                message_count >= self._settings.discovery_min_messages,
                followers_count >= self._settings.discovery_min_followers,
                ready_post_count > 0,
                momentum_score > 0.0,
            ]
        )
