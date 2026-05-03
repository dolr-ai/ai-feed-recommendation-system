from src.clients.offchain_rewards_client import OffchainRewardsClient


class StubResponse:
    def __init__(self, payload):
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self._payload


class StubSession:
    def __init__(self, payload):
        self._payload = payload
        self.calls = []

    def post(self, url, json=None, headers=None):
        self.calls.append((url, json, headers))
        return StubResponse(self._payload)


class StubSettings:
    offchain_agent_base_url = "https://offchain.yral.com"
    offchain_agent_timeout = 15


async def test_get_bulk_video_stats_maps_response_fields():
    session = StubSession(
        [
            {
                "video_id": "video-1",
                "total_count_loggedin": 7,
                "total_count_all": 19,
            },
            {
                "video_id": "video-2",
                "total_count_loggedin": 0,
                "total_count_all": 5,
            },
        ]
    )
    client = OffchainRewardsClient(StubSettings(), session=session)

    result = await client.get_bulk_video_stats(["video-1", "video-2", "video-1"])

    assert result == {
        "video-1": {"num_views_loggedin": 7, "num_views_all": 19},
        "video-2": {"num_views_loggedin": 0, "num_views_all": 5},
    }
    assert session.calls == [
        (
            "https://offchain.yral.com/api/v1/rewards/videos/bulk-stats-v2",
            {"video_ids": ["video-1", "video-2"]},
            {"accept": "application/json"},
        )
    ]
