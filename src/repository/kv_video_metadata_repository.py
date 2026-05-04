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

    async def cache_video_metadata_batch(self, metadata_by_video_id: dict[str, dict]) -> int:
        if not metadata_by_video_id or self._client is None:
            return 0

        pipe = self._client.pipeline()
        count = 0
        for video_id, row in metadata_by_video_id.items():
            normalized = self._normalize_metadata_payload(video_id, row)
            if normalized is None:
                continue

            # This shared offchain metadata hash is intentionally persistent.
            pipe.hset(video_metadata_key(normalized["video_id"]), mapping=normalized)
            count += 1

        if count > 0:
            await pipe.execute()
        return count

    def _normalize_metadata_payload(self, video_id: str, row: dict | None) -> dict | None:
        normalized_video_id = str(video_id or "").strip()
        if not normalized_video_id or not row:
            return None

        canister_id = (
            str(
                row.get("canister_id")
                or row.get("upload_canister_id")
                or self._settings.profile_canister_id
            ).strip()
            or self._settings.profile_canister_id
        )
        if canister_id == "2vxsx-fae":
            canister_id = self._settings.profile_canister_id

        return {
            "video_id": normalized_video_id,
            "canister_id": canister_id,
            "post_id": str(row.get("post_id") or "").strip(),
            "publisher_user_id": str(row.get("publisher_user_id") or "").strip(),
        }
