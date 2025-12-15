"""
Async Redis/Dragonfly service with connection pooling for high-performance operations.

This module provides an async wrapper for Redis operations with:
- Connection pooling for efficient resource usage
- Automatic retries and health checks
- Support for both standalone and cluster modes
- Optimized settings for high concurrency

The async implementation ensures non-blocking I/O operations,
allowing the FastAPI server to handle thousands of concurrent requests.
"""

import os
import ssl
import logging
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool, Redis
from redis.exceptions import RedisError, ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


class AsyncDragonflyService:
    """
    Async Dragonfly/Redis service with connection pooling.

    This class provides async Redis operations with optimized connection pooling
    for production workloads. All operations are non-blocking, allowing
    concurrent request handling in the FastAPI server.

    Algorithm:
    1. Initialize connection pool with optimal settings
    2. Create Redis client using the pool
    3. Provide async methods for all Redis operations
    4. Handle connection failures with retries
    5. Maintain connection health with keepalive
    """

    def __init__(
        self,
        host: str = None,
        port: int = 6379,
        password: Optional[str] = None,
        instance_id: Optional[str] = None,
        max_connections: int = None,
        min_idle_connections: int = 10,
        socket_connect_timeout: int = 5,
        socket_timeout: int = 60,
        decode_responses: bool = True,
        ssl_enabled: bool = False,
        cluster_enabled: bool = False,
        **kwargs,
    ):
        """
        Initialize async Dragonfly service with connection pool parameters.

        Args:
            host: Redis host (defaults to env var or localhost)
            port: Redis port (defaults to env var or 6379)
            password: Redis password (optional)
            instance_id: Instance identifier for logging
            max_connections: Maximum connections in pool (default: 100)
            min_idle_connections: Minimum idle connections to maintain (default: 10)
            socket_connect_timeout: Connection timeout in seconds (default: 5)
            socket_timeout: Socket timeout in seconds (default: 10)
            decode_responses: Decode responses to strings (default: True)
            ssl_enabled: Enable TLS/SSL (default: False)
            cluster_enabled: Enable cluster mode (default: False)
            **kwargs: Additional Redis client parameters
        """
        # Get configuration from environment variables
        self.host = (
            host
            or os.environ.get("DRAGONFLY_HOST")
            or os.environ.get("REDIS_HOST")
            or "localhost"
        )
        self.port = port or int(
            os.environ.get("DRAGONFLY_PORT", os.environ.get("REDIS_PORT", 6379))
        )
        self.password = (
            password
            or os.environ.get("DRAGONFLY_PASSWORD")
            or os.environ.get("REDIS_PASSWORD")
        )
        self.instance_id = (
            instance_id
            or os.environ.get("DRAGONFLY_INSTANCE_ID")
            or f"{self.host}:{self.port}"
        )

        # Connection pool settings - hardcoded default (config.py should provide this)
        self.max_connections = max_connections or 100
        self.min_idle_connections = min_idle_connections
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_timeout = socket_timeout
        self.decode_responses = decode_responses
        self.ssl_enabled = ssl_enabled
        self.cluster_enabled = cluster_enabled
        self.kwargs = kwargs

        # Will be initialized in connect()
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[Redis] = None

        logger.info(
            f"Initialized AsyncDragonflyService: host={self.host}, port={self.port}, "
            f"max_connections={self.max_connections}, cluster={self.cluster_enabled}"
        )

    async def connect(self) -> Redis:
        """
        Establish async connection to Redis with connection pooling.

        Creates a connection pool with optimal settings for high concurrency
        and returns an async Redis client using that pool.

        Returns:
            Redis async client instance

        Raises:
            ConnectionError: If connection fails after retries
        """
        try:
            # Create connection pool with optimized settings
            if self.ssl_enabled:
                # For SSL connections, use URL-based approach which supports connection pooling
                # Based on testing, this approach works with connection pooling
                url = f"rediss://:{self.password}@{self.host}:{self.port}/0"

                # Create pool from URL with SSL parameters
                self.pool = ConnectionPool.from_url(
                    url,
                    max_connections=self.max_connections,
                    socket_connect_timeout=self.socket_connect_timeout,
                    socket_timeout=self.socket_timeout,
                    socket_keepalive=True,
                    decode_responses=self.decode_responses,
                    retry_on_timeout=True,
                    ssl_cert_reqs="none",  # Don't require certificate validation
                    ssl_check_hostname=False,  # Don't verify hostname
                    **self.kwargs
                )
            else:
                # For non-SSL connections, use regular connection pool
                pool_params = {
                    "host": self.host,
                    "port": self.port,
                    "password": self.password,
                    "max_connections": self.max_connections,
                    "socket_connect_timeout": self.socket_connect_timeout,
                    "socket_timeout": self.socket_timeout,
                    "socket_keepalive": True,
                    "decode_responses": self.decode_responses,
                    "retry_on_timeout": True,
                    "retry_on_error": [ConnectionError, TimeoutError],
                    "health_check_interval": 30,  # Check connection health every 30 seconds
                }
                self.pool = ConnectionPool(**pool_params, **self.kwargs)

            # Create Redis client using the pool
            self.client = aioredis.Redis(connection_pool=self.pool)

            # Test connection
            await self.client.ping()

            logger.info(f"Successfully connected to Redis at {self.host}:{self.port}")
            logger.info(f"Connection pool created with max_connections={self.max_connections}")

            return self.client

        except RedisError as e:
            logger.error(f"Failed to connect to Redis: {e}")
            raise ConnectionError(f"Cannot connect to Redis at {self.host}:{self.port}: {e}")

    async def get_client(self) -> Redis:
        """
        Get the async Redis client, connecting if necessary.

        Returns:
            Redis async client instance
        """
        if not self.client:
            await self.connect()
        return self.client

    async def disconnect(self):
        """
        Disconnect from Redis and cleanup resources.
        """
        if self.client:
            await self.client.aclose()
            self.client = None

        if self.pool:
            await self.pool.disconnect()
            self.pool = None

        logger.info(f"Disconnected from Redis at {self.host}:{self.port}")

    async def close(self):
        """
        Close all connections in the pool gracefully.

        Should be called during application shutdown to clean up resources.
        """
        if self.client:
            await self.client.close()
            logger.info("Closed Redis connection pool")

        if self.pool:
            await self.pool.disconnect()
            logger.info("Disconnected connection pool")

    async def verify_connection(self) -> bool:
        """
        Verify Redis connection is healthy.

        Returns:
            True if connection is healthy, False otherwise
        """
        try:
            if not self.client:
                logger.debug("Connection verification: client not initialized")
                return False

            # Attempt ping
            result = await self.client.ping()

            # Redis ping returns True or b'PONG' depending on decode_responses setting
            if result is True or result == b'PONG' or result == 'PONG':
                logger.debug("Connection verification: SUCCESS")
                return True
            else:
                logger.warning(f"Connection verification: unexpected ping result: {result}")
                return False

        except Exception as e:
            logger.error(f"Connection verification failed: {e}")
            return False

    # Utility methods for common operations

    async def exists(self, *keys) -> int:
        """Check if keys exist (async)."""
        return await self.client.exists(*keys)

    async def get(self, key: str) -> Any:
        """Get value for key (async)."""
        return await self.client.get(key)

    async def set(self, key: str, value: Any, ex: int = None, nx: bool = False) -> bool:
        """Set key-value with optional expiry (async)."""
        return await self.client.set(key, value, ex=ex, nx=nx)

    async def delete(self, *keys) -> int:
        """Delete keys (async)."""
        return await self.client.delete(*keys)

    async def keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern (async)."""
        return await self.client.keys(pattern)

    async def incr(self, key: str) -> int:
        """Increment key value (async)."""
        return await self.client.incr(key)

    async def ttl(self, key: str) -> int:
        """Get TTL for key (async)."""
        return await self.client.ttl(key)

    async def expire(self, key: str, seconds: int) -> bool:
        """Set expiry for key (async)."""
        return await self.client.expire(key, seconds)

    # ZSET operations
    async def zadd(self, key: str, mapping: Dict, nx=False, xx=False) -> int:
        """Add members to sorted set (async)."""
        return await self.client.zadd(key, mapping, nx=nx, xx=xx)

    async def zrem(self, key: str, *members) -> int:
        """Remove members from sorted set (async)."""
        return await self.client.zrem(key, *members)

    async def zcard(self, key: str) -> int:
        """Get sorted set cardinality (async)."""
        return await self.client.zcard(key)

    async def zrange(self, key: str, start: int, stop: int, withscores: bool = False):
        """Get range from sorted set (async)."""
        return await self.client.zrange(key, start, stop, withscores=withscores)

    async def zrangebyscore(self, key: str, min_score: float, max_score: float, start=None, num=None, withscores=False):
        """Get range by score from sorted set (async)."""
        return await self.client.zrangebyscore(key, min_score, max_score, start=start, num=num, withscores=withscores)

    async def zremrangebyscore(self, key: str, min_score: float, max_score: float) -> int:
        """Remove range by score from sorted set (async)."""
        return await self.client.zremrangebyscore(key, min_score, max_score)

    async def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """Count members in score range (async)."""
        return await self.client.zcount(key, min_score, max_score)

    async def zscore(self, key: str, member: str) -> Optional[float]:
        """Get score of member in sorted set (async)."""
        return await self.client.zscore(key, member)

    # Bloom filter operations (with fallback for unsupported bulk operations)
    async def bf_reserve(self, key: str, error_rate: float, capacity: int, expansion: int = None):
        """
        Create bloom filter with specified parameters (async).

        Args:
            key: Redis key for the bloom filter
            error_rate: Desired false positive rate (e.g. 0.01 for 1%)
            capacity: Initial capacity for expected number of items
            expansion: Optional expansion factor for auto-scaling (e.g. 2 for doubling)

        Returns:
            Result of the BF.RESERVE command
        """
        if expansion:
            # Use execute_command for EXPANSION parameter
            return await self.client.execute_command(
                'BF.RESERVE', key, error_rate, capacity, 'EXPANSION', expansion
            )
        else:
            # Use standard method without expansion
            return await self.client.bf().reserve(key, error_rate, capacity)

    async def bf_add(self, key: str, item: str) -> int:
        """Add item to bloom filter (async)."""
        return await self.client.bf().add(key, item)

    async def bf_exists(self, key: str, item: str) -> int:
        """Check if item exists in bloom filter (async)."""
        return await self.client.bf().exists(key, item)

    async def bf_madd(self, key: str, *items) -> List[int]:
        """
        Add multiple items to bloom filter (async).

        Uses pipelined operations as fallback if bulk operation not supported.
        """
        try:
            # Try bulk operation first
            return await self.client.bf().madd(key, *items)
        except Exception:
            # Fallback to pipelined individual adds
            async with self.client.pipeline() as pipe:
                for item in items:
                    pipe.bf().add(key, item)
                results = await pipe.execute()
                return results

    async def bf_mexists(self, key: str, *items) -> List[int]:
        """
        Check if multiple items exist in bloom filter (async).

        Uses pipelined operations as fallback if bulk operation not supported.
        """
        try:
            # Try bulk operation first
            return await self.client.bf().mexists(key, *items)
        except Exception:
            # Fallback to pipelined individual checks
            async with self.client.pipeline() as pipe:
                for item in items:
                    pipe.bf().exists(key, item)
                results = await pipe.execute()
                return results

    # Script operations for Lua
    async def register_script(self, script: str):
        """Register a Lua script and return the script object (async)."""
        return self.client.register_script(script)

    async def script_load(self, script: str) -> str:
        """Load a Lua script and return its SHA (async)."""
        return await self.client.script_load(script)

    async def evalsha(self, sha: str, numkeys: int, *keys_and_args):
        """Execute a Lua script by SHA (async)."""
        return await self.client.evalsha(sha, numkeys, *keys_and_args)

    async def eval(self, script: str, numkeys: int, *keys_and_args):
        """Execute a Lua script directly (async)."""
        return await self.client.eval(script, numkeys, *keys_and_args)