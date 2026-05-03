from __future__ import annotations

from src.utils.feed_recsys_keys import video_metadata_key


class KVVideoMetadataRepository:
    def __init__(self, client, settings):
        self._client = client
        self._settings = settings

    async def get_video_metadata_batch(self, video_ids: list[str]) -> dict[str, dict]:
        unique_video_ids = list(dict.fromkeys(video_id for video_id in video_ids if video_id))
        if not unique_video_ids or self._client is None:
            return {}

        pipe = self._client.pipeline()
        for video_id in unique_video_ids:
            pipe.hgetall(video_metadata_key(video_id))
        results = await pipe.execute()

        metadata: dict[str, dict] = {}
        for video_id, row in zip(unique_video_ids, results):
            if not row:
                continue
            metadata[video_id] = {
                "canister_id": (
                    row.get("canister_id")
                    or row.get("upload_canister_id")
                    or self._settings.profile_canister_id
                ),
                "post_id": str(row.get("post_id") or "").strip(),
                "publisher_user_id": str(row.get("publisher_user_id") or "").strip(),
            }
        return metadata
