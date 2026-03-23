from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

import redis.asyncio as redis_async
from redis.asyncio import Redis
from redis.asyncio.cluster import RedisCluster

from src.core.settings import Settings, get_settings

TLS_CERT_DIR = Path("/tmp/influencer-feed-kvrocks-certs")


def _namespace_prefix() -> str:
    namespace = (get_settings().storage_namespace or "prod").strip() or "prod"
    return f"{{{namespace}}}"


def _feed_key_prefix() -> str:
    return f"{_namespace_prefix()}:influencer_feed"


def _discovery_key_prefix() -> str:
    return f"{_namespace_prefix()}:influencer_discovery"


def _pipeline_key_prefix() -> str:
    return f"{_namespace_prefix()}:influencer_pipeline"


def _job_lock_key_prefix() -> str:
    return f"{_namespace_prefix()}:influencer_jobs"


def ranked_feed_key() -> str:
    return f"{_feed_key_prefix()}:ranked"


def metadata_key() -> str:
    return f"{_feed_key_prefix()}:metadata"


def feed_meta_key() -> str:
    return f"{_feed_key_prefix()}:meta"


def stats_snapshot_key() -> str:
    return f"{_feed_key_prefix()}:stats_snapshot"


def serve_counts_key() -> str:
    return f"{_discovery_key_prefix()}:serve_counts"


def checkpoint_key(step: str) -> str:
    return f"{_pipeline_key_prefix()}:checkpoint:{step}"


def checkpoint_pattern() -> str:
    return f"{_pipeline_key_prefix()}:checkpoint:*"


def job_lock_key(job_name: str) -> str:
    return f"{_job_lock_key_prefix()}:lock:{job_name}"


class AsyncKVRocksService:
    def __init__(self, settings: Settings):
        self._settings = settings
        self._client: Optional[Redis] = None

    async def connect(self):
        if self._client is not None:
            return self._client

        common_kwargs = {
            "host": self._settings.kvrocks_host,
            "port": self._settings.kvrocks_port,
            "password": self._settings.kvrocks_password or None,
            "decode_responses": True,
        }
        tls_kwargs = self._build_tls_kwargs()

        if self._settings.kvrocks_cluster_enabled:
            self._client = RedisCluster(
                **common_kwargs,
                **tls_kwargs,
            )
        else:
            self._client = redis_async.Redis(**common_kwargs, **tls_kwargs)

        await self._client.ping()
        return self._client

    async def get_client(self):
        return await self.connect()

    async def close(self) -> None:
        if self._client is not None:
            await self._client.close()
            self._client = None

    def _build_tls_kwargs(self) -> dict:
        if not self._settings.kvrocks_tls_enabled:
            return {}

        kwargs = {
            "ssl": True,
            "ssl_check_hostname": False,
        }
        cert_paths = self._materialize_tls_files()
        if cert_paths["ca"]:
            kwargs["ssl_ca_certs"] = cert_paths["ca"]
        if cert_paths["cert"]:
            kwargs["ssl_certfile"] = cert_paths["cert"]
        if cert_paths["key"]:
            kwargs["ssl_keyfile"] = cert_paths["key"]
        return kwargs

    def _materialize_tls_files(self) -> dict[str, str | None]:
        TLS_CERT_DIR.mkdir(parents=True, exist_ok=True)

        def _write_if_present(filename: str, content: str) -> str | None:
            normalized = (content or "").strip()
            if not normalized:
                return None
            path = TLS_CERT_DIR / filename
            path.write_text(normalized + "\n")
            os.chmod(path, 0o600)
            return str(path)

        return {
            "ca": _write_if_present("ca.crt", self._settings.kvrocks_ssl_ca_cert),
            "cert": _write_if_present("client.crt", self._settings.kvrocks_ssl_client_cert),
            "key": _write_if_present("client.key", self._settings.kvrocks_ssl_client_key),
        }


async def build_kvrocks_client(settings: Settings):
    service = AsyncKVRocksService(settings)
    return await service.get_client()
