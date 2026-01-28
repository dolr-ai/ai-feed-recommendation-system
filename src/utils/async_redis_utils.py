"""
Async KVRocks service with connection pooling for high-performance operations.

This module provides an async wrapper for Redis-compatible operations with:
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
import tempfile
from typing import Any, Dict, List, Optional
import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool, Redis
from redis.asyncio.cluster import RedisCluster
from redis.exceptions import RedisError, ConnectionError, TimeoutError

logger = logging.getLogger(__name__)


class AsyncKVRocksService:
    """
    Async KVRocks service with connection pooling.

    This class provides async Redis-compatible operations with optimized connection pooling
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
        port: int = None,
        password: Optional[str] = None,
        instance_id: Optional[str] = None,
        max_connections: int = None,
        min_idle_connections: int = 10,
        socket_connect_timeout: int = 15,
        socket_timeout: int = 60,
        decode_responses: bool = True,
        ssl_enabled: bool = False,
        ssl_ca_certs: Optional[str] = None,
        ssl_certfile: Optional[str] = None,
        ssl_keyfile: Optional[str] = None,
        cluster_enabled: bool = False,
        **kwargs,
    ):
        """
        Initialize async KVRocks service with connection pool parameters.

        Args:
            host: Redis host (defaults to env var or localhost)
            port: Redis port (defaults to env var or 6379)
            password: Redis password (optional)
            instance_id: Instance identifier for logging
            max_connections: Maximum connections in pool (default: 100)
            min_idle_connections: Minimum idle connections to maintain (default: 10)
            socket_connect_timeout: Connection timeout in seconds (default: 15)
            socket_timeout: Socket timeout in seconds (default: 10)
            decode_responses: Decode responses to strings (default: True)
            ssl_enabled: Enable TLS/SSL (default: False)
            ssl_ca_certs: Path to CA certificate for mTLS (optional)
            ssl_certfile: Path to client certificate for mTLS (optional)
            ssl_keyfile: Path to client key for mTLS (optional)
            cluster_enabled: Enable cluster mode (default: False)
            **kwargs: Additional Redis client parameters
        """
        # Get configuration from environment variables
        self.host = host or os.environ.get("KVROCKS_HOST", "localhost")
        self.port = port if port is not None else int(os.environ.get("KVROCKS_PORT", 6379))
        self.password = password or os.environ.get("KVROCKS_PASSWORD")
        self.instance_id = instance_id or f"{self.host}:{self.port}"

        # Connection pool settings - hardcoded default (config.py should provide this)
        self.max_connections = max_connections or 100
        self.min_idle_connections = min_idle_connections
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_timeout = socket_timeout
        self.decode_responses = decode_responses
        self.ssl_enabled = ssl_enabled

        # mTLS certificates (PEM content from env vars)
        self._ssl_ca_cert_content = ssl_ca_certs or os.environ.get("KVROCKS_SSL_CA_CERT")
        self._ssl_client_cert_content = ssl_certfile or os.environ.get("KVROCKS_SSL_CLIENT_CERT")
        self._ssl_client_key_content = ssl_keyfile or os.environ.get("KVROCKS_SSL_CLIENT_KEY")

        # Temp file paths (created in connect() if needed)
        self.ssl_ca_certs = None
        self.ssl_certfile = None
        self.ssl_keyfile = None

        self.cluster_enabled = cluster_enabled
        self.kwargs = kwargs

        # Will be initialized in connect()
        self.pool: Optional[ConnectionPool] = None
        self.client: Optional[Redis] = None

        logger.info(
            f"Initialized AsyncKVRocksService: host={self.host}, port={self.port}, "
            f"max_connections={self.max_connections}, cluster={self.cluster_enabled}"
        )

    def _write_pem_to_temp(self, content: str) -> str:
        """Write PEM content to temp file, return path. redis-py needs file paths."""
        fd, path = tempfile.mkstemp(suffix=".pem")
        os.write(fd, content.encode("utf-8"))
        os.close(fd)
        return path

    def _setup_ssl_certs(self):
        """Write cert content to temp files for SSL connection."""
        if self._ssl_ca_cert_content:
            self.ssl_ca_certs = self._write_pem_to_temp(self._ssl_ca_cert_content)
        if self._ssl_client_cert_content:
            self.ssl_certfile = self._write_pem_to_temp(self._ssl_client_cert_content)
        if self._ssl_client_key_content:
            self.ssl_keyfile = self._write_pem_to_temp(self._ssl_client_key_content)

    async def connect(self) -> Redis:
        """
        Establish async connection to Redis with connection pooling.

        Creates a connection pool with optimal settings for high concurrency
        and returns an async Redis client using that pool.

        Algorithm:
            1. If cluster_enabled, use RedisCluster for cluster mode
            2. Otherwise, use Redis with ConnectionPool for standalone mode
            3. Both modes support mTLS via ssl_ca_certs, ssl_certfile, ssl_keyfile

        Returns:
            Redis async client instance (or RedisCluster in cluster mode)

        Raises:
            ConnectionError: If connection fails after retries
        """
        # Write PEM content to temp files for SSL (redis-py needs file paths)
        if self.ssl_enabled:
            self._setup_ssl_certs()

        try:
            if self.cluster_enabled:
                # Cluster mode - use RedisCluster
                cluster_params = {
                    "host": self.host,
                    "port": self.port,
                    "password": self.password,
                    "decode_responses": self.decode_responses,
                    "socket_timeout": self.socket_timeout,
                }

                if self.ssl_enabled:
                    cluster_params["ssl"] = True
                    if self.ssl_ca_certs and self.ssl_certfile and self.ssl_keyfile:
                        # mTLS with client certificates
                        cluster_params["ssl_ca_certs"] = self.ssl_ca_certs
                        cluster_params["ssl_certfile"] = self.ssl_certfile
                        cluster_params["ssl_keyfile"] = self.ssl_keyfile
                        logger.info(f"Using mTLS cluster mode for {self.host}:{self.port}")
                    else:
                        cluster_params["ssl_cert_reqs"] = None

                self.client = RedisCluster(**cluster_params)
                await self.client.ping()

                logger.info(f"Successfully connected to Redis cluster at {self.host}:{self.port}")
                return self.client

            # Standalone mode - use ConnectionPool
            if self.ssl_enabled:
                # For SSL connections, use URL-based approach which supports connection pooling
                url = f"rediss://:{self.password}@{self.host}:{self.port}/0"

                # Build SSL parameters based on whether we have mTLS certs
                ssl_params = {
                    "max_connections": self.max_connections,
                    "socket_connect_timeout": self.socket_connect_timeout,
                    "socket_timeout": self.socket_timeout,
                    "socket_keepalive": True,
                    "decode_responses": self.decode_responses,
                    "retry_on_timeout": True,
                }

                if self.ssl_ca_certs and self.ssl_certfile and self.ssl_keyfile:
                    # mTLS: Use client certificates for mutual TLS authentication
                    ssl_params.update({
                        "ssl_ca_certs": self.ssl_ca_certs,
                        "ssl_certfile": self.ssl_certfile,
                        "ssl_keyfile": self.ssl_keyfile,
                    })
                    logger.info(f"Using mTLS with client certs for {self.host}:{self.port}")
                else:
                    # Standard TLS: Skip certificate validation (for backwards compatibility)
                    ssl_params.update({
                        "ssl_cert_reqs": "none",
                        "ssl_check_hostname": False,
                    })

                # Create pool from URL with SSL parameters
                self.pool = ConnectionPool.from_url(
                    url,
                    **ssl_params,
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
            await self.client.aclose()
            self.client = None
            logger.info("Closed Redis connection pool")

        if self.pool:
            await self.pool.disconnect()
            self.pool = None
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

    async def zrandmember(self, key: str, count: int = 1, withscores: bool = False):
        """
        Return random members from sorted set (async).

        Algorithm:
            1. Call Redis ZRANDMEMBER command
            2. If count > 0, return that many unique random members
            3. If withscores=True, return interleaved [member, score, member, score, ...]

        Args:
            key: Redis key for the sorted set
            count: Number of random members to return (default: 1)
            withscores: If True, return scores along with members (default: False)

        Returns:
            List of members (or interleaved members+scores if withscores=True)
            Returns None/empty if key doesn't exist
        """
        return await self.client.zrandmember(key, count=count, withscores=withscores)

    async def zscore(self, key: str, member: str) -> Optional[float]:
        """Get score of member in sorted set (async)."""
        return await self.client.zscore(key, member)

    # Bloom filter operations (using execute_command for cluster compatibility)
    async def bf_reserve(self, key: str, error_rate: float, capacity: int, expansion: int = None):
        """
        Create bloom filter with specified parameters (async).

        Uses execute_command directly for cluster compatibility (RedisCluster doesn't have .bf()).

        Args:
            key: Redis key for the bloom filter
            error_rate: Desired false positive rate (e.g. 0.01 for 1%)
            capacity: Initial capacity for expected number of items
            expansion: Optional expansion factor for auto-scaling (e.g. 2 for doubling)

        Returns:
            Result of the BF.RESERVE command
        """
        if expansion:
            return await self.client.execute_command(
                'BF.RESERVE', key, error_rate, capacity, 'EXPANSION', expansion
            )
        else:
            return await self.client.execute_command(
                'BF.RESERVE', key, error_rate, capacity
            )

    async def bf_add(self, key: str, item: str) -> int:
        """Add item to bloom filter (async). Uses execute_command for cluster."""
        return await self.client.execute_command('BF.ADD', key, item)

    async def bf_exists(self, key: str, item: str) -> int:
        """Check if item exists in bloom filter (async). Uses execute_command for cluster."""
        return await self.client.execute_command('BF.EXISTS', key, item)

    async def bf_madd(self, key: str, *items) -> List[int]:
        """
        Add multiple items to bloom filter (async).

        Uses BF.MADD command directly via execute_command for cluster compatibility.

        Args:
            key: Redis key for the bloom filter
            items: Items to add to the bloom filter

        Returns:
            List of 1s (added) and 0s (already exists)
        """
        if not items:
            return []
        return await self.client.execute_command('BF.MADD', key, *items)

    async def bf_mexists(self, key: str, *items) -> List[int]:
        """
        Check if multiple items exist in bloom filter (async).

        Uses BF.MEXISTS command directly via execute_command for cluster compatibility.

        Args:
            key: Redis key for the bloom filter
            items: Items to check

        Returns:
            List of 1s (exists) and 0s (not found)
        """
        if not items:
            return []
        return await self.client.execute_command('BF.MEXISTS', key, *items)

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