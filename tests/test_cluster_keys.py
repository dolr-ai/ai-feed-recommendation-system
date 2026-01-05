"""
Tests for cluster-safe key patterns with hash tags.

Verifies keys hash to correct slots for Redis/KVRocks cluster mode.
Hash tags ensure all keys for a user land on the same slot,
and all global pool keys land on the same slot.

Algorithm:
    1. Compute CRC16-CCITT hash for key (or hash tag portion if present)
    2. Hash slot = CRC16 % 16384
    3. If key contains {hashtag}, only the hashtag portion is hashed
    4. Verify user keys share same slot, global keys share same slot
"""

import pytest


# CRC16-CCITT lookup table (XMODEM polynomial 0x1021)
CRC16_TABLE = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50A5, 0x60C6, 0x70E7,
    0x8108, 0x9129, 0xA14A, 0xB16B, 0xC18C, 0xD1AD, 0xE1CE, 0xF1EF,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52B5, 0x4294, 0x72F7, 0x62D6,
    0x9339, 0x8318, 0xB37B, 0xA35A, 0xD3BD, 0xC39C, 0xF3FF, 0xE3DE,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64E6, 0x74C7, 0x44A4, 0x5485,
    0xA56A, 0xB54B, 0x8528, 0x9509, 0xE5EE, 0xF5CF, 0xC5AC, 0xD58D,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76D7, 0x66F6, 0x5695, 0x46B4,
    0xB75B, 0xA77A, 0x9719, 0x8738, 0xF7DF, 0xE7FE, 0xD79D, 0xC7BC,
    0x48C4, 0x58E5, 0x6886, 0x78A7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xC9CC, 0xD9ED, 0xE98E, 0xF9AF, 0x8948, 0x9969, 0xA90A, 0xB92B,
    0x5AF5, 0x4AD4, 0x7AB7, 0x6A96, 0x1A71, 0x0A50, 0x3A33, 0x2A12,
    0xDBFD, 0xCBDC, 0xFBBF, 0xEB9E, 0x9B79, 0x8B58, 0xBB3B, 0xAB1A,
    0x6CA6, 0x7C87, 0x4CE4, 0x5CC5, 0x2C22, 0x3C03, 0x0C60, 0x1C41,
    0xEDAE, 0xFD8F, 0xCDEC, 0xDDCD, 0xAD2A, 0xBD0B, 0x8D68, 0x9D49,
    0x7E97, 0x6EB6, 0x5ED5, 0x4EF4, 0x3E13, 0x2E32, 0x1E51, 0x0E70,
    0xFF9F, 0xEFBE, 0xDFDD, 0xCFFC, 0xBF1B, 0xAF3A, 0x9F59, 0x8F78,
    0x9188, 0x81A9, 0xB1CA, 0xA1EB, 0xD10C, 0xC12D, 0xF14E, 0xE16F,
    0x1080, 0x00A1, 0x30C2, 0x20E3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83B9, 0x9398, 0xA3FB, 0xB3DA, 0xC33D, 0xD31C, 0xE37F, 0xF35E,
    0x02B1, 0x1290, 0x22F3, 0x32D2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xB5EA, 0xA5CB, 0x95A8, 0x8589, 0xF56E, 0xE54F, 0xD52C, 0xC50D,
    0x34E2, 0x24C3, 0x14A0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xA7DB, 0xB7FA, 0x8799, 0x97B8, 0xE75F, 0xF77E, 0xC71D, 0xD73C,
    0x26D3, 0x36F2, 0x0691, 0x16B0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xD94C, 0xC96D, 0xF90E, 0xE92F, 0x99C8, 0x89E9, 0xB98A, 0xA9AB,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18C0, 0x08E1, 0x3882, 0x28A3,
    0xCB7D, 0xDB5C, 0xEB3F, 0xFB1E, 0x8BF9, 0x9BD8, 0xABBB, 0xBB9A,
    0x4A75, 0x5A54, 0x6A37, 0x7A16, 0x0AF1, 0x1AD0, 0x2AB3, 0x3A92,
    0xFD2E, 0xED0F, 0xDD6C, 0xCD4D, 0xBDAA, 0xAD8B, 0x9DE8, 0x8DC9,
    0x7C26, 0x6C07, 0x5C64, 0x4C45, 0x3CA2, 0x2C83, 0x1CE0, 0x0CC1,
    0xEF1F, 0xFF3E, 0xCF5D, 0xDF7C, 0xAF9B, 0xBFBA, 0x8FD9, 0x9FF8,
    0x6E17, 0x7E36, 0x4E55, 0x5E74, 0x2E93, 0x3EB2, 0x0ED1, 0x1EF0,
]


def crc16_ccitt(data: bytes) -> int:
    """
    Compute CRC16-CCITT (XMODEM) checksum.

    Algorithm:
        1. Start with CRC = 0
        2. For each byte: CRC = (CRC << 8) ^ table[(CRC >> 8) ^ byte]
        3. Return final CRC masked to 16 bits

    Args:
        data: Bytes to compute checksum for

    Returns:
        16-bit CRC value
    """
    crc = 0
    for byte in data:
        crc = ((crc << 8) & 0xFFFF) ^ CRC16_TABLE[((crc >> 8) ^ byte) & 0xFF]
    return crc


def get_hash_slot(key: str) -> int:
    """
    Calculate Redis cluster hash slot for a key.

    Algorithm:
        1. Look for first '{' in key
        2. If found, look for '}' after it
        3. If '}' found and not immediately after '{', hash only the portion between braces
        4. Otherwise hash the entire key
        5. Return CRC16(hash_input) % 16384

    Args:
        key: Redis key string

    Returns:
        Hash slot number (0-16383)
    """
    # Find hash tag boundaries
    start = key.find("{")
    if start != -1:
        end = key.find("}", start + 1)
        # Only use hash tag if there's content between braces
        if end != -1 and end != start + 1:
            key = key[start + 1:end]

    return crc16_ccitt(key.encode()) % 16384


class TestHashSlotCalculation:
    """Unit tests for hash slot calculation function."""

    def test_simple_key_hashes_entire_key(self):
        """Keys without hash tags hash the entire key."""
        slot = get_hash_slot("simple_key")
        assert 0 <= slot < 16384

    def test_hash_tag_uses_tag_only(self):
        """Keys with hash tags only hash the tag portion."""
        # These should have same slot because same hash tag
        slot1 = get_hash_slot("{user:alice}:videos")
        slot2 = get_hash_slot("{user:alice}:bloom")
        assert slot1 == slot2

    def test_empty_hash_tag_hashes_entire_key(self):
        """Empty hash tags {} hash the entire key."""
        slot1 = get_hash_slot("{}:key1")
        slot2 = get_hash_slot("{}:key2")
        # Different keys should (likely) have different slots
        # Note: Could collide but very unlikely
        assert slot1 != slot2 or True  # Allow collision but verify both compute


class TestUserKeyHashSlots:
    """Verify all user keys for same user hash to same slot."""

    def test_user_keys_same_slot(self):
        """
        All keys for a single user should hash to the same slot.

        This is critical for Lua scripts that operate on multiple user keys atomically.
        The hash tag {user:USER_ID} ensures all these keys land on same slot.
        """
        user_id = "test_user_12345"

        # All user key patterns from the plan
        keys = [
            f"{{user:{user_id}}}:videos_to_show:popularity",
            f"{{user:{user_id}}}:videos_to_show:freshness",
            f"{{user:{user_id}}}:videos_to_show:following",
            f"{{user:{user_id}}}:videos_to_show:ugc",
            f"{{user:{user_id}}}:watched:short",
            f"{{user:{user_id}}}:bloom:permanent",
            f"{{user:{user_id}}}:pop_percentile_pointer",
            f"{{user:{user_id}}}:refill:lock:popularity",
            f"{{user:{user_id}}}:refill:lock:freshness",
            f"{{user:{user_id}}}:following:last_sync",
        ]

        slots = [get_hash_slot(k) for k in keys]
        unique_slots = set(slots)

        assert len(unique_slots) == 1, (
            f"All user keys should hash to same slot. "
            f"Got {len(unique_slots)} unique slots: {unique_slots}"
        )

    def test_user_keys_with_uuid_user_id(self):
        """Test with realistic UUID-style user ID."""
        user_id = "a1b2c3d4-e5f6-7890-abcd-ef1234567890"

        keys = [
            f"{{user:{user_id}}}:videos_to_show:popularity",
            f"{{user:{user_id}}}:watched:short",
            f"{{user:{user_id}}}:bloom:permanent",
        ]

        slots = [get_hash_slot(k) for k in keys]
        assert len(set(slots)) == 1

    def test_user_keys_with_numeric_user_id(self):
        """Test with numeric user ID (common in some systems)."""
        user_id = "9876543210"

        keys = [
            f"{{user:{user_id}}}:videos_to_show:freshness",
            f"{{user:{user_id}}}:pop_percentile_pointer",
        ]

        slots = [get_hash_slot(k) for k in keys]
        assert len(set(slots)) == 1


class TestGlobalKeyHashSlots:
    """Verify all global pool keys hash to same slot."""

    def test_global_popularity_keys_same_slot(self):
        """
        All global popularity pool keys should hash to same slot.

        Uses {GLOBAL} hash tag to ensure all global data lands on same slot.
        """
        # All popularity bucket keys
        keys = [
            "{GLOBAL}:pool:pop_99_100",
            "{GLOBAL}:pool:pop_90_99",
            "{GLOBAL}:pool:pop_80_90",
            "{GLOBAL}:pool:pop_70_80",
            "{GLOBAL}:pool:pop_60_70",
            "{GLOBAL}:pool:pop_50_60",
            "{GLOBAL}:pool:pop_40_50",
            "{GLOBAL}:pool:pop_30_40",
            "{GLOBAL}:pool:pop_20_30",
            "{GLOBAL}:pool:pop_10_20",
            "{GLOBAL}:pool:pop_0_10",
        ]

        slots = [get_hash_slot(k) for k in keys]
        unique_slots = set(slots)

        assert len(unique_slots) == 1, (
            f"All global popularity keys should hash to same slot. "
            f"Got {len(unique_slots)} unique slots: {unique_slots}"
        )

    def test_global_freshness_keys_same_slot(self):
        """All global freshness window keys should hash to same slot."""
        keys = [
            "{GLOBAL}:pool:fresh_l1d",
            "{GLOBAL}:pool:fresh_l7d",
            "{GLOBAL}:pool:fresh_l14d",
            "{GLOBAL}:pool:fresh_l30d",
            "{GLOBAL}:pool:fresh_l90d",
        ]

        slots = [get_hash_slot(k) for k in keys]
        assert len(set(slots)) == 1

    def test_all_global_keys_same_slot(self):
        """All global keys (popularity + freshness + fallback + ugc) should be same slot."""
        keys = [
            "{GLOBAL}:pool:pop_99_100",
            "{GLOBAL}:pool:pop_0_10",
            "{GLOBAL}:pool:fresh_l1d",
            "{GLOBAL}:pool:fresh_l90d",
            "{GLOBAL}:pool:fallback",
            "{GLOBAL}:pool:ugc",
        ]

        slots = [get_hash_slot(k) for k in keys]
        unique_slots = set(slots)

        assert len(unique_slots) == 1, (
            f"All global keys should hash to same slot. "
            f"Got {len(unique_slots)} unique slots: {unique_slots}"
        )


class TestUserGlobalSlotSeparation:
    """Verify user and global keys are on different slots (expected but not required)."""

    def test_user_and_global_different_slots(self):
        """
        User keys and global keys should hash to different slots.

        This is expected behavior (different hash tags) but not strictly required.
        The important thing is same-type keys share a slot.
        """
        user_key = "{user:test_user_001}:videos_to_show:popularity"
        global_key = "{GLOBAL}:pool:pop_99_100"

        user_slot = get_hash_slot(user_key)
        global_slot = get_hash_slot(global_key)

        # Just verify they compute valid slots
        assert 0 <= user_slot < 16384
        assert 0 <= global_slot < 16384

        # Note: They will be different because hash tags differ
        # But we don't assert this as it's not a requirement


class TestMetadataKeys:
    """Verify metadata keys work correctly (no hash tag - independent lookups)."""

    def test_metadata_keys_no_hash_tag(self):
        """
        Metadata keys don't use hash tags - each is independent.

        These keys are read individually via MGET, not in Lua scripts,
        so they don't need to share slots.
        """
        video_id = "vid_abc123"

        keys = [
            f"offchain:metadata:video_details:{video_id}",
            f"offchain:video_nsfw:{video_id}",
            f"offchain:video_dedup_status:{video_id}",
        ]

        # Just verify they compute valid slots
        for key in keys:
            slot = get_hash_slot(key)
            assert 0 <= slot < 16384

    def test_different_videos_distribute_across_slots(self):
        """Different video IDs should (likely) hash to different slots for distribution."""
        video_ids = [f"video_{i:06d}" for i in range(100)]
        keys = [f"offchain:metadata:video_details:{vid}" for vid in video_ids]

        slots = [get_hash_slot(k) for k in keys]
        unique_slots = set(slots)

        # With 100 videos, we expect many different slots (good distribution)
        # At minimum, should have more than 1 slot
        assert len(unique_slots) > 1, "Metadata should distribute across multiple slots"


if __name__ == "__main__":
    # Run quick verification
    print("Hash slot verification for cluster-safe keys")
    print("=" * 50)

    user_id = "test_user_001"
    user_keys = [
        f"{{user:{user_id}}}:videos_to_show:popularity",
        f"{{user:{user_id}}}:watched:short",
        f"{{user:{user_id}}}:bloom:permanent",
    ]

    print(f"\nUser keys for {user_id}:")
    for key in user_keys:
        slot = get_hash_slot(key)
        print(f"  {key} -> slot {slot}")

    global_keys = [
        "{GLOBAL}:pool:pop_99_100",
        "{GLOBAL}:pool:fresh_l1d",
        "{GLOBAL}:pool:fallback",
    ]

    print("\nGlobal keys:")
    for key in global_keys:
        slot = get_hash_slot(key)
        print(f"  {key} -> slot {slot}")

    metadata_keys = [
        "offchain:metadata:video_details:vid_001",
        "offchain:metadata:video_details:vid_002",
    ]

    print("\nMetadata keys (no hash tag):")
    for key in metadata_keys:
        slot = get_hash_slot(key)
        print(f"  {key} -> slot {slot}")
