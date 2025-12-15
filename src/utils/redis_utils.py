from typing import Any, Dict, List, Optional, Set
import os
import logging
import math

import redis
from redis.cluster import RedisCluster


def get_logger(name: str) -> logging.Logger:
    """Simple logger function"""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger


logger = get_logger(__name__)


class DragonflyService:
    """Dragonfly Redis service class for high-performance Redis operations"""

    def __init__(
        self,
        host: str = None,
        port: int = 6379,
        password: Optional[str] = None,
        instance_id: Optional[str] = None,
        socket_connect_timeout: int = 10,
        socket_timeout: int = 10,
        decode_responses: bool = True,
        ssl_enabled: bool = False,
        cluster_enabled: bool = False,
        **kwargs,
    ):
        """
        Initialize Dragonfly service with connection parameters

        Args:
            host: Dragonfly instance host/IP address
            port: Dragonfly instance port (default: 6379)
            password: Optional password for authentication
            instance_id: Optional instance ID for logging/identification
            socket_connect_timeout: Connection timeout in seconds
            socket_timeout: Socket timeout in seconds
            decode_responses: Whether to decode responses to strings
            ssl_enabled: Whether to use TLS/SSL encryption
            cluster_enabled: Whether to use cluster mode
            **kwargs: Additional redis client parameters
        """
        # Check environment variables for configuration
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

        # Get cluster mode from env if not provided
        if cluster_enabled is None:
            cluster_enabled_env = os.environ.get(
                "DRAGONFLY_CLUSTER_ENABLED", "false"
            ).lower()
            self.cluster_enabled = cluster_enabled_env == "true"
        else:
            self.cluster_enabled = cluster_enabled

        self.client = None
        self.ssl_enabled = ssl_enabled
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_timeout = socket_timeout
        self.decode_responses = decode_responses
        self.kwargs = kwargs

        logger.info(
            f"Initialized DragonflyService with host={self.host}, port={self.port}, "
            f"cluster_enabled={self.cluster_enabled}, ssl_enabled={self.ssl_enabled}"
        )

    def connect(self) -> redis.Redis:
        """
        Create and return a connected Dragonfly client

        Returns:
            Connected Redis client instance
        """
        try:
            # Basic connection parameters with connection pooling
            conn_params = {
                "host": self.host,
                "port": self.port,
                "socket_timeout": self.socket_timeout,
                "socket_connect_timeout": self.socket_connect_timeout,
                "decode_responses": self.decode_responses,
                "max_connections": 200,  # High concurrency support
                "health_check_interval": 30,
            }

            # Add password if provided
            if self.password:
                conn_params["password"] = self.password

            # Add SSL if enabled
            if self.ssl_enabled:
                conn_params.update(
                    {
                        "ssl": True,
                        "ssl_cert_reqs": None,
                        "ssl_check_hostname": False,
                    }
                )

            # Add any additional kwargs
            conn_params.update(self.kwargs)

            # Create client based on cluster mode
            if self.cluster_enabled:
                conn_params["skip_full_coverage_check"] = True
                self.client = RedisCluster(**conn_params)
                logger.debug(f"Using RedisCluster client for {self.instance_id}")
            else:
                self.client = redis.Redis(**conn_params)
                logger.debug(f"Using Redis client for {self.instance_id}")

            # Test connection
            self.client.ping()
            ssl_status = "with TLS" if self.ssl_enabled else "without TLS"
            cluster_status = (
                "in cluster mode" if self.cluster_enabled else "in standalone mode"
            )
            logger.info(
                f"Successfully connected to Dragonfly instance {self.instance_id} {ssl_status} {cluster_status}"
            )
            return self.client

        except Exception as e:
            logger.error(f"Failed to connect to Dragonfly {self.instance_id}: {e}")
            raise

    def get_client(self) -> redis.Redis:
        """
        Get the current client, connecting if necessary

        Returns:
            Redis client instance
        """
        if not self.client:
            return self.connect()

        try:
            # Test if current connection is still valid
            self.client.ping()
            return self.client
        except Exception:
            # Reconnect if connection is stale
            logger.debug(
                f"Reconnecting to Dragonfly {self.instance_id} due to stale connection"
            )
            return self.connect()

    def verify_connection(self) -> bool:
        """
        Verify connection to Dragonfly instance

        Returns:
            True if connection successful, False otherwise
        """
        try:
            if not self.client:
                self.connect()
            self.client.ping()
            return True
        except Exception as e:
            logger.error(
                f"Dragonfly connection verification failed for {self.instance_id}: {e}"
            )
            return False

    # Basic Redis operations (same as before, no changes needed)
    #
    def set(self, key: str, value: Any, ex: Optional[int] = None) -> bool:
        """Set a key-value pair"""
        try:
            client = self.get_client()
            if ex:
                return client.setex(key, ex, value)
            else:
                return client.set(key, value)
        except Exception as e:
            logger.error(f"Failed to set key {key} in {self.instance_id}: {e}")
            raise

    #
    def get(self, key: str) -> Any:
        """Get a value by key"""
        try:
            client = self.get_client()
            return client.get(key)
        except Exception as e:
            logger.error(f"Failed to get key {key} from {self.instance_id}: {e}")
            raise

    #
    def mset(self, mapping: Dict[str, Any]) -> bool:
        """Set multiple key-value pairs"""
        try:
            client = self.get_client()

            # In cluster mode, we need to handle mset differently
            if self.cluster_enabled:
                # Use pipeline to set multiple keys
                pipe = client.pipeline(transaction=False)
                for key, value in mapping.items():
                    pipe.set(key, value)
                results = pipe.execute()
                return all(results)
            else:
                return client.mset(mapping)
        except Exception as e:
            logger.error(f"Failed to set multiple keys in {self.instance_id}: {e}")
            raise

    #
    def mget(self, keys: List[str]) -> List[Any]:
        """Get multiple values by keys"""
        try:
            client = self.get_client()

            # In cluster mode, we need to handle mget differently
            if self.cluster_enabled:
                # Use pipeline to get multiple keys
                pipe = client.pipeline(transaction=False)
                for key in keys:
                    pipe.get(key)
                return pipe.execute()
            else:
                return client.mget(keys)
        except Exception as e:
            logger.error(f"Failed to get multiple keys from {self.instance_id}: {e}")
            raise

    def exists(self, key: str) -> bool:
        """Check if a key exists"""
        try:
            client = self.get_client()
            return bool(client.exists(key))
        except Exception as e:
            logger.error(
                f"Failed to check existence of key {key} in {self.instance_id}: {e}"
            )
            raise

    def delete(self, *keys: str) -> int:
        """Delete one or more keys"""
        try:
            client = self.get_client()

            # In cluster mode, we need to handle delete differently for multiple keys
            if self.cluster_enabled and len(keys) > 1:
                pipe = client.pipeline(transaction=False)
                for key in keys:
                    pipe.delete(key)
                results = pipe.execute()
                return sum(results)
            else:
                return client.delete(*keys)
        except Exception as e:
            logger.error(f"Failed to delete keys {keys} from {self.instance_id}: {e}")
            raise

    def keys(self, pattern: str = "*") -> List[str]:
        """Get all keys matching a pattern"""
        try:
            client = self.get_client()

            # Use scan_iter for better performance with large datasets
            result = []
            # Limit the number of keys to prevent memory issues
            max_keys = 10000
            count = 0

            for key in client.scan_iter(match=pattern, count=5000):
                result.append(key)
                count += 1
                if count >= max_keys:
                    logger.warning(
                        f"Reached maximum key limit ({max_keys}) for pattern: {pattern}"
                    )
                    break

            return result
        except Exception as e:
            logger.error(
                f"Failed to get keys with pattern {pattern} from {self.instance_id}: {e}"
            )
            raise

    def ttl(self, key: str) -> int:
        """Get the time-to-live for a key"""
        try:
            client = self.get_client()
            return client.ttl(key)
        except Exception as e:
            logger.error(
                f"Failed to get TTL for key {key} from {self.instance_id}: {e}"
            )
            raise

    def incr(self, key: str, amount: int = 1) -> int:
        """Increment a key's value"""
        try:
            client = self.get_client()
            return client.incr(key, amount)
        except Exception as e:
            logger.error(f"Failed to increment key {key} in {self.instance_id}: {e}")
            raise

    def expire(self, key: str, seconds: int) -> bool:
        """Set expiration time for a key"""
        try:
            client = self.get_client()
            return client.expire(key, seconds)
        except Exception as e:
            logger.error(
                f"Failed to set expiration for key {key} in {self.instance_id}: {e}"
            )
            raise

    def flushdb(self) -> bool:
        """
        Clear all keys in the current database and optimize memory.

        Safety: Only allows flushdb on localhost connections to prevent accidental
        data loss on production/remote instances.

        Raises:
            RuntimeError: If attempting to flushdb on a non-localhost instance
        """
        if self.host not in ['localhost', '127.0.0.1', '::1']:
            error_msg = (
                f"SAFETY CHECK FAILED: Cannot flushdb on non-localhost instance! "
                f"Current host: {self.host}. "
                f"flushdb is only allowed on localhost/127.0.0.1/::1 to prevent accidental data loss."
            )
            logger.error(error_msg)
            raise RuntimeError(error_msg)

        logger.warning(f"Performing flushdb on localhost instance: {self.host}")
        try:
            client = self.get_client()
            result = client.flushdb()

            # Force memory optimization
            try:
                # Try to run MEMORY PURGE command if available (Redis 4.0+)
                client.execute_command("MEMORY PURGE")
                logger.info("Executed MEMORY PURGE command")
            except Exception as e:
                logger.warning(f"MEMORY PURGE command not available: {e}")

            # Request garbage collection
            try:
                client.execute_command("FLUSHALL ASYNC")
                logger.info("Executed FLUSHALL ASYNC command")
            except Exception as e:
                logger.warning(f"FLUSHALL ASYNC command failed: {e}")

            return result
        except Exception as e:
            logger.error(f"Failed to flush database {self.instance_id}: {e}")
            raise

    def info(self, section: Optional[str] = None) -> Dict[str, Any]:
        """Get server information"""
        try:
            client = self.get_client()
            return client.info(section)
        except Exception as e:
            logger.error(f"Failed to get server info from {self.instance_id}: {e}")
            raise

    def pipeline(self):
        """Get a pipeline for batch operations"""
        try:
            client = self.get_client()
            return client.pipeline()
        except Exception as e:
            logger.error(f"Failed to create pipeline for {self.instance_id}: {e}")
            raise

    # Redis Set Operations
    def sadd(self, key: str, *members: str) -> int:
        """Add one or more members to a set"""
        try:
            client = self.get_client()
            return client.sadd(key, *members)
        except Exception as e:
            logger.error(
                f"Failed to add members to set {key} in {self.instance_id}: {e}"
            )
            raise

    def srem(self, key: str, *members: str) -> int:
        """Remove one or more members from a set"""
        try:
            client = self.get_client()
            return client.srem(key, *members)
        except Exception as e:
            logger.error(
                f"Failed to remove members from set {key} in {self.instance_id}: {e}"
            )
            raise

    def sismember(self, key: str, member: str) -> bool:
        """Check if a member exists in a set"""
        try:
            client = self.get_client()
            return bool(client.sismember(key, member))
        except Exception as e:
            logger.error(
                f"Failed to check membership of {member} in set {key} in {self.instance_id}: {e}"
            )
            raise

    def smembers(self, key: str) -> Set[str]:
        """Get all members of a set (use with caution for large sets)"""
        try:
            client = self.get_client()
            return client.smembers(key)
        except Exception as e:
            logger.error(
                f"Failed to get members of set {key} from {self.instance_id}: {e}"
            )
            raise

    def scard(self, key: str) -> int:
        """Get the cardinality (size) of a set"""
        try:
            client = self.get_client()
            return client.scard(key)
        except Exception as e:
            logger.error(
                f"Failed to get cardinality of set {key} from {self.instance_id}: {e}"
            )
            raise

    def sscan_iter(
        self, key: str, match: Optional[str] = None, count: Optional[int] = None
    ):
        """Iterate over set members using SSCAN for better performance with large sets"""
        try:
            client = self.get_client()
            return client.sscan_iter(key, match=match, count=count)
        except Exception as e:
            logger.error(f"Failed to scan set {key} in {self.instance_id}: {e}")
            raise

    def sdiff(self, *keys: str) -> Set[str]:
        """Return the difference between the first set and the other sets"""
        try:
            client = self.get_client()
            return client.sdiff(*keys)
        except Exception as e:
            logger.error(
                f"Failed to compute set difference for keys {keys} in {self.instance_id}: {e}"
            )
            raise

    def sinter(self, *keys: str) -> Set[str]:
        """Return the intersection of all sets"""
        try:
            client = self.get_client()
            return client.sinter(*keys)
        except Exception as e:
            logger.error(
                f"Failed to compute set intersection for keys {keys} in {self.instance_id}: {e}"
            )
            raise

    def sunion(self, *keys: str) -> Set[str]:
        """Return the union of all sets"""
        try:
            client = self.get_client()
            return client.sunion(*keys)
        except Exception as e:
            logger.error(
                f"Failed to compute set union for keys {keys} in {self.instance_id}: {e}"
            )
            raise

    # Redis Hash Operations
    def hset(self, key: str, mapping: Dict[str, Any] = None, **kwargs) -> int:
        """Set field in the hash stored at key to value"""
        try:
            client = self.get_client()
            if mapping:
                return client.hset(key, mapping=mapping)
            else:
                return client.hset(key, **kwargs)
        except Exception as e:
            logger.error(
                f"Failed to set hash fields for {key} in {self.instance_id}: {e}"
            )
            raise

    def hget(self, key: str, field: str) -> Any:
        """Get the value of a hash field"""
        try:
            client = self.get_client()
            return client.hget(key, field)
        except Exception as e:
            logger.error(
                f"Failed to get hash field {field} for {key} in {self.instance_id}: {e}"
            )
            raise

    def hgetall(self, key: str) -> Dict[str, Any]:
        """Get all fields and values in a hash"""
        try:
            client = self.get_client()
            return client.hgetall(key)
        except Exception as e:
            logger.error(
                f"Failed to get all hash fields for {key} in {self.instance_id}: {e}"
            )
            raise

    def hdel(self, key: str, *fields: str) -> int:
        """Delete one or more hash fields"""
        try:
            client = self.get_client()
            return client.hdel(key, *fields)
        except Exception as e:
            logger.error(
                f"Failed to delete hash fields for {key} in {self.instance_id}: {e}"
            )
            raise

    def hexists(self, key: str, field: str) -> bool:
        """Determine if a hash field exists"""
        try:
            client = self.get_client()
            return client.hexists(key, field)
        except Exception as e:
            logger.error(
                f"Failed to check hash field existence for {key} in {self.instance_id}: {e}"
            )
            raise

    def hkeys(self, key: str) -> List[str]:
        """Get all field names in a hash"""
        try:
            client = self.get_client()
            return client.hkeys(key)
        except Exception as e:
            logger.error(
                f"Failed to get hash keys for {key} in {self.instance_id}: {e}"
            )
            raise

    def hvals(self, key: str) -> List[Any]:
        """Get all values in a hash"""
        try:
            client = self.get_client()
            return client.hvals(key)
        except Exception as e:
            logger.error(
                f"Failed to get hash values for {key} in {self.instance_id}: {e}"
            )
            raise

    def hlen(self, key: str) -> int:
        """Get the number of fields in a hash"""
        try:
            client = self.get_client()
            return client.hlen(key)
        except Exception as e:
            logger.error(
                f"Failed to get hash length for {key} in {self.instance_id}: {e}"
            )
            raise

    # Redis Sorted Set Operations
    def zadd(self, key: str, mapping: Dict[str, float]) -> int:
        """Add one or more members to a sorted set, or update scores"""
        try:
            client = self.get_client()
            return client.zadd(key, mapping)
        except Exception as e:
            logger.error(
                f"Failed to add members to sorted set {key} in {self.instance_id}: {e}"
            )
            raise

    def zrem(self, key: str, *members: str) -> int:
        """Remove one or more members from a sorted set"""
        try:
            client = self.get_client()
            return client.zrem(key, *members)
        except Exception as e:
            logger.error(
                f"Failed to remove members from sorted set {key} in {self.instance_id}: {e}"
            )
            raise

    def zrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> List[Any]:
        """Return a range of members from a sorted set by index"""
        try:
            client = self.get_client()
            # Pass withscores as a keyword argument
            return client.zrange(key, start, stop, withscores=withscores)
        except Exception as e:
            logger.error(
                f"Failed to get range from sorted set {key} in {self.instance_id}: {e}"
            )
            raise

    def zrevrange(
        self, key: str, start: int, stop: int, withscores: bool = False
    ) -> List[Any]:
        """Return a range of members from a sorted set by index in reverse order"""
        try:
            client = self.get_client()
            # Pass withscores as a keyword argument
            return client.zrevrange(key, start, stop, withscores=withscores)
        except Exception as e:
            logger.error(
                f"Failed to get reverse range from sorted set {key} in {self.instance_id}: {e}"
            )
            raise

    def zrangebyscore(
        self,
        key: str,
        min_score: float,
        max_score: float,
        withscores: bool = False,
        start: int = None,
        num: int = None,
    ) -> List[Any]:
        """Return a range of members from a sorted set by score"""
        try:
            client = self.get_client()

            # Convert min_score and max_score to strings if they're special values
            if min_score == float("-inf"):
                min_score = "-inf"
            if max_score == float("inf"):
                max_score = "+inf"

            # Handle optional start and num parameters
            kwargs = {"withscores": withscores}
            if start is not None and num is not None:
                kwargs["start"] = start
                kwargs["num"] = num

            # Call the client method with appropriate parameters
            return client.zrangebyscore(key, min_score, max_score, **kwargs)
        except Exception as e:
            logger.error(
                f"Failed to get range by score from sorted set {key} in {self.instance_id}: {e}"
            )
            raise

    def zrevrangebyscore(
        self,
        key: str,
        max_score: float,
        min_score: float,
        withscores: bool = False,
        start: int = None,
        num: int = None,
    ) -> List[Any]:
        """Return a range of members from a sorted set by score in reverse order"""
        try:
            client = self.get_client()

            # Convert min_score and max_score to strings if they're special values
            if min_score == float("-inf"):
                min_score = "-inf"
            if max_score == float("inf"):
                max_score = "+inf"

            # Handle optional start and num parameters
            kwargs = {"withscores": withscores}
            if start is not None and num is not None:
                kwargs["start"] = start
                kwargs["num"] = num

            # Call the client method with appropriate parameters
            return client.zrevrangebyscore(key, max_score, min_score, **kwargs)
        except Exception as e:
            logger.error(
                f"Failed to get reverse range by score from sorted set {key} in {self.instance_id}: {e}"
            )
            raise

    def zcard(self, key: str) -> int:
        """Get the cardinality (size) of a sorted set"""
        try:
            client = self.get_client()
            return client.zcard(key)
        except Exception as e:
            logger.error(
                f"Failed to get cardinality of sorted set {key} from {self.instance_id}: {e}"
            )
            raise

    def zscore(self, key: str, member: str) -> Optional[float]:
        """Get the score of a member in a sorted set"""
        try:
            client = self.get_client()
            return client.zscore(key, member)
        except Exception as e:
            logger.error(
                f"Failed to get score of member {member} in sorted set {key} from {self.instance_id}: {e}"
            )
            raise

    def zincrby(self, key: str, amount: float, member: str) -> float:
        """Increment the score of a member in a sorted set"""
        try:
            client = self.get_client()
            return client.zincrby(key, amount, member)
        except Exception as e:
            logger.error(
                f"Failed to increment score of member {member} in sorted set {key} from {self.instance_id}: {e}"
            )
            raise

    def zrank(self, key: str, member: str) -> Optional[int]:
        """Get the rank of a member in a sorted set (0-based, ascending)"""
        try:
            client = self.get_client()
            return client.zrank(key, member)
        except Exception as e:
            logger.error(
                f"Failed to get rank of member {member} in sorted set {key} from {self.instance_id}: {e}"
            )
            raise

    def zrevrank(self, key: str, member: str) -> Optional[int]:
        """Get the rank of a member in a sorted set (0-based, descending)"""
        try:
            client = self.get_client()
            return client.zrevrank(key, member)
        except Exception as e:
            logger.error(
                f"Failed to get reverse rank of member {member} in sorted set {key} from {self.instance_id}: {e}"
            )
            raise

    def zcount(self, key: str, min_score: float, max_score: float) -> int:
        """Count members in a sorted set with scores within the given range"""
        try:
            client = self.get_client()
            return client.zcount(key, min_score, max_score)
        except Exception as e:
            logger.error(
                f"Failed to count members in sorted set {key} from {self.instance_id}: {e}"
            )
            raise

    def zscan_iter(
        self, key: str, match: Optional[str] = None, count: Optional[int] = None
    ):
        """Iterate over sorted set members using ZSCAN for better performance with large sets"""
        try:
            client = self.get_client()
            return client.zscan_iter(key, match=match, count=count)
        except Exception as e:
            logger.error(f"Failed to scan sorted set {key} in {self.instance_id}: {e}")
            raise

    # Redis List operations
    def lpush(self, key: str, *values: str) -> int:
        """
        Push one or more values to the left (head) of a list

        Args:
            key: The list key
            values: Values to push

        Returns:
            Length of the list after the push operation
        """
        try:
            client = self.get_client()
            return client.lpush(key, *values)
        except Exception as e:
            logger.error(f"Failed to lpush to list {key} in {self.instance_id}: {e}")
            raise

    def lrange(self, key: str, start: int, end: int) -> List[str]:
        """
        Get a range of elements from a list

        Args:
            key: The list key
            start: Start index (0-based)
            end: End index (inclusive, -1 means last element)

        Returns:
            List of elements in the specified range
        """
        try:
            client = self.get_client()
            result = client.lrange(key, start, end)
            return [
                item.decode("utf-8") if isinstance(item, bytes) else item
                for item in result
            ]
        except Exception as e:
            logger.error(f"Failed to lrange list {key} in {self.instance_id}: {e}")
            raise

    def llen(self, key: str) -> int:
        """
        Get the length of a list

        Args:
            key: The list key

        Returns:
            Length of the list
        """
        try:
            client = self.get_client()
            return client.llen(key)
        except Exception as e:
            logger.error(
                f"Failed to get length of list {key} in {self.instance_id}: {e}"
            )
            raise

    def lindex(self, key: str, index: int) -> Optional[str]:
        """
        Get an element from a list by its index

        Args:
            key: The list key
            index: Index (0-based, negative indices count from the end)

        Returns:
            Element at the specified index, or None if index is out of range
        """
        try:
            client = self.get_client()
            result = client.lindex(key, index)
            if result is None:
                return None
            return result.decode("utf-8") if isinstance(result, bytes) else result
        except Exception as e:
            logger.error(
                f"Failed to get index {index} from list {key} in {self.instance_id}: {e}"
            )
            raise

    # Redis Bloom Filter Operations
    def bf_reserve(
        self, key: str, error_rate: float = 0.01, capacity: int = 100000,
        expansion: int = None
    ) -> bool:
        """
        Create a new Bloom filter with specified error rate and capacity

        Args:
            key: The Bloom filter key
            error_rate: Desired false positive rate (0.0 < rate < 1.0, default: 0.01 = 1%)
            capacity: Expected number of items to store (default: 100,000)
            expansion: Optional expansion factor for auto-scaling (e.g. 2 for doubling)

        Returns:
            True if filter was created successfully

        Note:
            - Lower error_rate = more memory usage but fewer false positives
            - Higher capacity = more memory usage but can store more items
            - Memory usage approximately: capacity * ln(1/error_rate) * ln(2) / 8 bytes
            - With expansion: filter auto-scales when full (e.g. 1K→2K→4K→8K...)
        """
        try:
            # Calculate estimated memory usage
            # Formula: m = -n * ln(p) / (ln(2)^2)
            # Where: n = capacity, p = error_rate, m = number of bits
            bits_needed = -capacity * math.log(error_rate) / (math.log(2) ** 2)
            bytes_needed = bits_needed / 8

            # Convert to human readable format
            if bytes_needed < 1024:
                memory_str = f"{bytes_needed:.1f} bytes"
            elif bytes_needed < 1024 * 1024:
                memory_str = f"{bytes_needed / 1024:.1f} KB"
            elif bytes_needed < 1024 * 1024 * 1024:
                memory_str = f"{bytes_needed / (1024 * 1024):.1f} MB"
            else:
                memory_str = f"{bytes_needed / (1024 * 1024 * 1024):.1f} GB"

            client = self.get_client()

            # Build command based on whether expansion is specified
            if expansion:
                result = client.execute_command("BF.RESERVE", key, error_rate, capacity, "EXPANSION", expansion)
                logger.info(
                    f"Created Bloom filter '{key}' - Initial capacity: {capacity:,} items, "
                    f"Error rate: {error_rate:.4f} ({error_rate * 100:.2f}%), "
                    f"Expansion: {expansion}x, "
                    f"Initial memory: {memory_str}"
                )
            else:
                result = client.execute_command("BF.RESERVE", key, error_rate, capacity)
                logger.info(
                    f"Created Bloom filter '{key}' - Capacity: {capacity:,} items, "
                    f"Error rate: {error_rate:.4f} ({error_rate * 100:.2f}%), "
                    f"Estimated memory: {memory_str}"
                )

            return result == "OK"
        except Exception as e:
            logger.error(
                f"Failed to create Bloom filter {key} in {self.instance_id}: {e}"
            )
            raise

    def bf_add(self, key: str, item: str) -> bool:
        """
        Add an item to a Bloom filter

        Args:
            key: The Bloom filter key
            item: Item to add to the filter

        Returns:
            True if item was newly added, False if it may have existed before
        """
        try:
            client = self.get_client()
            result = client.execute_command("BF.ADD", key, item)
            return bool(result)
        except Exception as e:
            logger.error(
                f"Failed to add item to Bloom filter {key} in {self.instance_id}: {e}"
            )
            raise

    def bf_madd(self, key: str, *items: str) -> List[bool]:
        """
        Add multiple items to a Bloom filter (Dragonfly compatible implementation)

        Args:
            key: The Bloom filter key
            items: Items to add to the filter

        Returns:
            List of booleans indicating if each item was newly added

        Note:
            Dragonfly doesn't support BF.MADD, so this uses pipelined BF.ADD commands
        """
        try:
            client = self.get_client()

            # Use pipeline for better performance
            pipe = client.pipeline(transaction=False)
            for item in items:
                pipe.execute_command("BF.ADD", key, item)

            results = pipe.execute()
            return [bool(r) for r in results]
        except Exception as e:
            logger.error(
                f"Failed to add multiple items to Bloom filter {key} in {self.instance_id}: {e}"
            )
            raise

    def bf_exists(self, key: str, item: str) -> bool:
        """
        Check if an item exists in a Bloom filter

        Args:
            key: The Bloom filter key
            item: Item to check

        Returns:
            True if item might exist (possible false positive)
            False if item definitely does not exist
        """
        try:
            client = self.get_client()
            result = client.execute_command("BF.EXISTS", key, item)
            return bool(result)
        except Exception as e:
            logger.error(
                f"Failed to check item in Bloom filter {key} in {self.instance_id}: {e}"
            )
            raise

    def bf_mexists(self, key: str, *items: str) -> List[bool]:
        """
        Check if multiple items exist in a Bloom filter (Dragonfly compatible implementation)

        Args:
            key: The Bloom filter key
            items: Items to check

        Returns:
            List of booleans indicating if each item might exist

        Note:
            Dragonfly doesn't support BF.MEXISTS, so this uses pipelined BF.EXISTS commands
        """
        try:
            client = self.get_client()

            # Use pipeline for better performance
            pipe = client.pipeline(transaction=False)
            for item in items:
                pipe.execute_command("BF.EXISTS", key, item)

            results = pipe.execute()
            return [bool(r) for r in results]
        except Exception as e:
            logger.error(
                f"Failed to check multiple items in Bloom filter {key} in {self.instance_id}: {e}"
            )
            raise

    def bf_info(self, key: str) -> Dict[str, Any]:
        """
        Get information about a Bloom filter (Dragonfly compatible implementation)

        Args:
            key: The Bloom filter key

        Returns:
            Dictionary with available filter information

        Note:
            Dragonfly doesn't support BF.INFO, returns basic info only
        """
        try:
            client = self.get_client()

            # Try BF.INFO first (for Redis with RedisBloom)
            try:
                result = client.execute_command("BF.INFO", key)

                # Parse the result into a dictionary
                info = {}
                for i in range(0, len(result), 2):
                    key_name = (
                        result[i].decode()
                        if isinstance(result[i], bytes)
                        else result[i]
                    )
                    value = result[i + 1]
                    if isinstance(value, bytes):
                        value = value.decode()
                    info[key_name] = value

                return info
            except Exception as bf_info_error:
                if "unknown command" in str(bf_info_error).lower():
                    logger.warning(
                        f"BF.INFO not supported in {self.instance_id}, returning basic info"
                    )
                    # Return basic info that we can determine
                    return {
                        "filter_name": key,
                        "status": "exists" if self.exists(key) else "not_found",
                        "note": "BF.INFO not supported by this Redis instance (Dragonfly)",
                    }
                else:
                    raise bf_info_error

        except Exception as e:
            logger.error(
                f"Failed to get Bloom filter info for {key} in {self.instance_id}: {e}"
            )
            raise

    def bf_card(self, key: str) -> int:
        """
        Get the cardinality (estimated number of items) in a Bloom filter (Dragonfly compatible)

        Args:
            key: The Bloom filter key

        Returns:
            Estimated number of items in the filter

        Note:
            Dragonfly doesn't support BF.CARD, returns -1 as fallback
        """
        try:
            client = self.get_client()

            # Try BF.CARD first (for Redis with RedisBloom)
            try:
                result = client.execute_command("BF.CARD", key)
                return int(result)
            except Exception as bf_card_error:
                if "unknown command" in str(bf_card_error).lower():
                    logger.warning(f"BF.CARD not supported in {self.instance_id}")
                    return -1  # Indicate that cardinality is not available
                else:
                    raise bf_card_error

        except Exception as e:
            logger.error(
                f"Failed to get Bloom filter cardinality for {key} in {self.instance_id}: {e}"
            )
            raise
