"""
Background job tests.

Tests distributed locking and job execution.
Uses real Redis for locking tests (no BigQuery mocking).
"""

import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
sys.path.insert(0, os.path.dirname(__file__))

import pytest
from helpers import clear_user_keys


class TestDistributedLocking:
    """Tests for distributed job locking."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_lock_acquisition_succeeds(self, redis_client, redis_config):
        """
        First lock acquisition succeeds.

        Algorithm:
            1. Acquire lock for test job
            2. Verify lock is acquired
            3. Release lock
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from background_jobs import acquire_job_lock, release_job_lock

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        job_name = "test_job_lock_001"

        try:
            # Clear any existing lock
            redis_client.delete(f"job:lock:{job_name}")

            # Acquire lock
            acquired = await acquire_job_lock(service.client, job_name, ttl=60)
            assert acquired is True, "First lock acquisition should succeed"

            # Release lock
            await release_job_lock(service.client, job_name)

        finally:
            redis_client.delete(f"job:lock:{job_name}")
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_second_lock_fails_when_held(self, redis_client, redis_config):
        """
        Second lock acquisition fails when lock is held.

        Algorithm:
            1. Clear any stale lock
            2. Acquire lock
            3. Try to acquire same lock again
            4. Verify second acquisition fails
            5. Release lock
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from background_jobs import acquire_job_lock, release_job_lock
        import asyncio

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        job_name = "test_job_lock_002"

        try:
            # Clear any existing lock first (ensure clean state)
            redis_client.delete(f"job:lock:{job_name}")
            await asyncio.sleep(0.1)  # Small delay to ensure deletion propagates

            # First acquisition
            first = await acquire_job_lock(service.client, job_name, ttl=60)
            assert first is True, "First lock acquisition should succeed"

            # Second acquisition should fail
            second = await acquire_job_lock(service.client, job_name, ttl=60)
            assert second is False, "Second lock acquisition should fail"

            # Release lock
            await release_job_lock(service.client, job_name)

        finally:
            redis_client.delete(f"job:lock:{job_name}")
            await service.close()

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_lock_released_after_release(self, redis_client, redis_config):
        """
        Lock can be re-acquired after release.

        Algorithm:
            1. Acquire lock
            2. Release lock
            3. Acquire lock again
            4. Verify second acquisition succeeds
        """
        from utils.async_redis_utils import AsyncKVRocksService
        from background_jobs import acquire_job_lock, release_job_lock

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        job_name = "test_job_lock_003"

        try:
            # Clear any existing lock
            redis_client.delete(f"job:lock:{job_name}")

            # Acquire and release
            await acquire_job_lock(service.client, job_name, ttl=60)
            await release_job_lock(service.client, job_name)

            # Re-acquire should succeed
            acquired = await acquire_job_lock(service.client, job_name, ttl=60)
            assert acquired is True, "Lock should be available after release"

            await release_job_lock(service.client, job_name)

        finally:
            redis_client.delete(f"job:lock:{job_name}")
            await service.close()


class TestLockExpiry:
    """Tests for lock expiry behavior."""

    @pytest.mark.integration
    @pytest.mark.asyncio
    async def test_lock_expires_after_ttl(self, redis_client, redis_config):
        """
        Lock expires after TTL and can be re-acquired.

        Algorithm:
            1. Acquire lock with short TTL (1 second)
            2. Wait for TTL to expire
            3. Acquire lock again
            4. Verify second acquisition succeeds
        """
        import asyncio
        from utils.async_redis_utils import AsyncKVRocksService
        from background_jobs import acquire_job_lock

        service = AsyncKVRocksService(**redis_config)
        await service.connect()

        job_name = "test_job_lock_expiry"

        try:
            # Clear any existing lock
            redis_client.delete(f"job:lock:{job_name}")

            # Acquire with 1 second TTL
            first = await acquire_job_lock(service.client, job_name, ttl=1)
            assert first is True

            # Wait for expiry
            await asyncio.sleep(1.5)

            # Should be able to re-acquire
            second = await acquire_job_lock(service.client, job_name, ttl=60)
            assert second is True, "Lock should be available after expiry"

        finally:
            redis_client.delete(f"job:lock:{job_name}")
            await service.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
