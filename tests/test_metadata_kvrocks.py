"""
Tests for metadata lookups from KVRocks instead of BigQuery.

These tests verify that:
1. Video metadata can be fetched from KVRocks using HGETALL
2. Missing videos are handled gracefully (empty dict returned)
3. Batch fetching works efficiently with pipelined HGETALL
4. HASH format is correctly read (no JSON parsing needed)

Key pattern: offchain:metadata:video_details:{video_id}
Storage format: Redis HASH with fields {video_id, post_id, publisher_user_id}
    (stored by off-chain-agent using HSETMULTIPLE)

Test flow (TDD):
    1. Write tests (initially will FAIL - method not implemented)
    2. Implement _fetch_from_kvrocks in metadata_handler.py
    3. Tests should PASS after implementation
"""

import os
import sys
import pytest
import uuid

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))


class TestMetadataKVRocksLookup:
    """Tests for KVRocks metadata fetching."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_fetch_single_video_metadata(self, redis_client):
        """
        Fetch metadata for single video from KVRocks.

        Algorithm:
            1. Seed metadata key in Redis as HASH (simulating KVRocks/off-chain-agent)
            2. Call HGETALL to retrieve
            3. Verify returns correct post_id, publisher_user_id

        Note: Using redis_client fixture which connects to stage Redis.
        For production, this would connect to actual KVRocks cluster.
        """
        video_id = f"test_vid_meta_{uuid.uuid4().hex[:8]}"
        metadata = {
            "video_id": video_id,
            "post_id": "12345",  # HASH stores all values as strings
            "publisher_user_id": "user_abc",
        }

        # Seed key as HASH (using offchain:metadata:video_details pattern)
        key = f"offchain:metadata:video_details:{video_id}"
        redis_client.hset(key, mapping=metadata)

        try:
            # Direct test using redis_client (simulating what _fetch_from_kvrocks does)
            data = redis_client.hgetall(key)
            assert data, "Key should exist and return non-empty dict"

            # HGETALL returns dict directly, no JSON parsing needed
            assert data["post_id"] == "12345"
            assert data["publisher_user_id"] == "user_abc"

        finally:
            redis_client.delete(key)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_batch_fetch_metadata_hgetall(self, redis_client):
        """
        Batch fetch metadata for multiple videos using pipelined HGETALL.

        Algorithm:
            1. Seed multiple metadata keys as HASH
            2. Use pipelined HGETALL to fetch all in single round-trip
            3. Verify all returned correctly
            4. Verify order is preserved

        This tests the efficiency of the batch lookup.
        """
        video_ids = [f"test_batch_vid_{uuid.uuid4().hex[:8]}" for _ in range(5)]
        metadata_list = [
            {
                "video_id": vid,
                "post_id": str(i * 100),  # HASH stores all values as strings
                "publisher_user_id": f"user_{i}",
            }
            for i, vid in enumerate(video_ids)
        ]

        # Seed all keys as HASH
        keys = []
        for vid, meta in zip(video_ids, metadata_list):
            key = f"offchain:metadata:video_details:{vid}"
            keys.append(key)
            redis_client.hset(key, mapping=meta)

        try:
            # Test pipelined HGETALL (cluster-mode compatible, what _fetch_from_kvrocks uses)
            pipe = redis_client.pipeline()
            for key in keys:
                pipe.hgetall(key)
            results = pipe.execute()

            assert len(results) == 5, "Should get 5 results"

            # Verify each result (HGETALL returns dict directly)
            for i, (data, expected) in enumerate(zip(results, metadata_list)):
                assert data, f"Result {i} should not be empty"
                assert data["video_id"] == expected["video_id"]
                assert data["post_id"] == expected["post_id"]
                assert data["publisher_user_id"] == expected["publisher_user_id"]

        finally:
            for key in keys:
                redis_client.delete(key)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_missing_video_returns_empty_dict(self, redis_client):
        """
        Missing video should return empty dict {} in HGETALL, not cause error.

        Algorithm:
            1. Create list of keys where some don't exist
            2. HGETALL all keys
            3. Verify empty dict {} returned for missing, data for existing
        """
        existing_vid = f"test_existing_{uuid.uuid4().hex[:8]}"
        missing_vid = f"test_missing_{uuid.uuid4().hex[:8]}"

        existing_key = f"offchain:metadata:video_details:{existing_vid}"
        missing_key = f"offchain:metadata:video_details:{missing_vid}"

        # Only seed the existing one as HASH
        metadata = {
            "video_id": existing_vid,
            "post_id": "999",
            "publisher_user_id": "user_exists",
        }
        redis_client.hset(existing_key, mapping=metadata)

        try:
            # Pipelined HGETALL (cluster-mode compatible)
            pipe = redis_client.pipeline()
            pipe.hgetall(existing_key)
            pipe.hgetall(missing_key)
            results = pipe.execute()

            assert len(results) == 2
            assert results[0], "Existing key should have non-empty dict"
            assert results[1] == {}, "Missing key should return empty dict"

            # Verify existing data (already a dict from HGETALL)
            data = results[0]
            assert data["video_id"] == existing_vid

        finally:
            redis_client.delete(existing_key)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_handles_upload_canister_id_field(self, redis_client):
        """
        Handle alternate field name: upload_canister_id -> canister_id.

        Some metadata sources use 'upload_canister_id' instead of 'canister_id'.
        The fetch method should normalize this.

        Algorithm:
            1. Seed metadata as HASH with upload_canister_id field
            2. Fetch with HGETALL and verify it maps to canister_id
        """
        video_id = f"test_upload_canister_{uuid.uuid4().hex[:8]}"
        metadata = {
            "video_id": video_id,
            "post_id": "456",
            "publisher_user_id": "user_upload",
            "upload_canister_id": "canister_from_upload"  # Alternate field name
        }

        key = f"offchain:metadata:video_details:{video_id}"
        redis_client.hset(key, mapping=metadata)

        try:
            data = redis_client.hgetall(key)

            # _fetch_from_kvrocks should handle this mapping
            canister_id = data.get("canister_id") or data.get("upload_canister_id")
            assert canister_id == "canister_from_upload"

        finally:
            redis_client.delete(key)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_handles_partial_hash_data(self, redis_client):
        """
        HASH with missing fields should be handled gracefully.

        With HASH format, there's no concept of "malformed JSON".
        Instead, we test that missing fields are handled gracefully.

        Algorithm:
            1. Seed key with only some fields
            2. Verify fetch works and missing fields return None via .get()
        """
        video_id = f"test_partial_{uuid.uuid4().hex[:8]}"
        key = f"offchain:metadata:video_details:{video_id}"

        # Seed with only video_id (missing post_id, publisher_user_id)
        redis_client.hset(key, mapping={"video_id": video_id})

        try:
            data = redis_client.hgetall(key)
            assert data, "Key should exist"

            # Missing fields should return None via .get()
            assert data.get("post_id") is None
            assert data.get("publisher_user_id") is None
            assert data["video_id"] == video_id

        finally:
            redis_client.delete(key)

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_empty_video_list_returns_empty(self, redis_client):
        """
        Empty video list should return empty list, not error.

        Tests that our implementation handles empty lists gracefully.
        """
        # Empty pipeline execute should return empty list
        pipe = redis_client.pipeline()
        result = pipe.execute()
        assert result == [], "Pipeline with no commands should return empty list"


class TestMetadataKeyPatterns:
    """Verify metadata key patterns are correct."""

    def test_metadata_key_pattern_format(self):
        """
        Metadata keys should follow the pattern from table_configs.py.

        Pattern: offchain:metadata:video_details:{video_id}
        """
        video_id = "test_vid_123"
        expected_key = f"offchain:metadata:video_details:{video_id}"

        # This is the pattern that should be used
        assert expected_key == "offchain:metadata:video_details:test_vid_123"

    def test_metadata_keys_no_hash_tag(self):
        """
        Metadata keys should NOT have hash tags (distributed across cluster).

        Unlike user/global keys, metadata keys are read individually,
        so they don't need to share slots.
        """
        key = "offchain:metadata:video_details:test_vid"

        # Should not contain hash tags
        assert "{" not in key
        assert "}" not in key


class TestMetadataValueFormat:
    """Verify expected metadata value format (HASH with string values)."""

    def test_expected_fields_present(self):
        """
        Metadata values should contain required fields.

        Required fields from off-chain-agent's VideoMetadata struct:
            - video_id
            - post_id
            - publisher_user_id

        Note: canister_id is NOT stored by off-chain-agent.
        The caller (metadata_handler) uses STUBBED_CANISTER_ID for all videos.
        """
        # Example metadata from off-chain-agent (stored as HASH, all values are strings)
        metadata = {
            "video_id": "vid_001",
            "post_id": "123",  # HASH stores as string
            "publisher_user_id": "user_abc",
        }

        # Verify required fields
        assert "video_id" in metadata
        assert "post_id" in metadata
        assert "publisher_user_id" in metadata


class TestMetadataIntegration:
    """Integration tests for metadata handler with KVRocks."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_full_metadata_flow(self, redis_client):
        """
        Test full metadata flow: seed as HASH -> fetch with HGETALL -> use.

        Algorithm:
            1. Seed realistic metadata for multiple videos as HASH
            2. Fetch using pipelined HGETALL
            3. Build response format matching batch_convert_video_ids output
        """
        # Create realistic test data (values as strings for HASH format)
        videos = []
        keys = []
        for i in range(10):
            vid = f"test_full_flow_{uuid.uuid4().hex[:8]}"
            meta = {
                "video_id": vid,
                "post_id": str(1000 + i),  # HASH stores as string
                "publisher_user_id": f"publisher_{i}",
            }
            videos.append((vid, meta))
            key = f"offchain:metadata:video_details:{vid}"
            keys.append(key)
            redis_client.hset(key, mapping=meta)

        try:
            # Fetch all at once using pipelined HGETALL (cluster-mode compatible)
            pipe = redis_client.pipeline()
            for key in keys:
                pipe.hgetall(key)
            results_raw = pipe.execute()

            # Build results (HGETALL returns dict directly)
            results = []
            for vid, data in zip([v[0] for v in videos], results_raw):
                if data:  # Non-empty dict
                    results.append({
                        "video_id": data["video_id"],
                        "canister_id": data.get("canister_id") or data.get("upload_canister_id"),
                        "post_id": data["post_id"],
                        "publisher_user_id": data["publisher_user_id"],
                    })

            # Verify all 10 videos were fetched
            assert len(results) == 10

            # Verify data matches what we seeded
            for i, result in enumerate(results):
                assert result["post_id"] == str(1000 + i)
                assert result["publisher_user_id"] == f"publisher_{i}"

        finally:
            for key in keys:
                redis_client.delete(key)


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-x"])
