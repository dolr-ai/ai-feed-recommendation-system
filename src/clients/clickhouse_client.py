from __future__ import annotations

import asyncio
from typing import Any, Optional


class ClickHouseClient:
    def __init__(self, settings):
        self._settings = settings
        self._client: Optional[Any] = None

    async def fetch_all(
        self,
        query: str,
        parameters: Optional[dict[str, Any]] = None,
    ) -> list[dict[str, Any]]:
        rows, column_names = await asyncio.to_thread(
            self._execute_with_columns,
            query,
            parameters or {},
        )
        return [
            dict(zip(column_names, row))
            for row in rows
        ]

    async def fetch_one(
        self,
        query: str,
        parameters: Optional[dict[str, Any]] = None,
    ) -> Optional[dict[str, Any]]:
        rows = await self.fetch_all(query, parameters)
        return rows[0] if rows else None

    async def fetch_scalar(
        self,
        query: str,
        parameters: Optional[dict[str, Any]] = None,
    ) -> Any:
        row = await self.fetch_one(query, parameters)
        if row is None:
            return None
        return next(iter(row.values()), None)

    async def execute(
        self,
        query: str,
        parameters: Optional[dict[str, Any]] = None,
    ) -> Any:
        return await asyncio.to_thread(
            self._get_or_create_client().execute,
            query,
            parameters or {},
        )

    async def ping(self) -> bool:
        return bool(await self.fetch_scalar("SELECT 1"))

    async def close(self) -> None:
        if self._client is None:
            return
        client = self._client
        self._client = None
        await asyncio.to_thread(client.disconnect_connection)

    async def aclose(self) -> None:
        await self.close()

    def _execute_with_columns(
        self,
        query: str,
        parameters: dict[str, Any],
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        rows, column_meta = self._get_or_create_client().execute(
            query,
            parameters,
            with_column_types=True,
        )
        column_names = [name for name, _ in column_meta]
        return rows, column_names

    def _get_or_create_client(self) -> Any:
        if self._client is not None:
            return self._client

        try:
            from clickhouse_driver import Client as SyncClickHouseClient
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional install state
            raise RuntimeError(
                "clickhouse-driver is required for ClickHouse access."
            ) from exc

        self._client = SyncClickHouseClient(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            database=self._settings.clickhouse_database,
            user=self._settings.clickhouse_username,
            password=self._settings.clickhouse_password or "",
            secure=self._settings.clickhouse_secure,
            verify=self._settings.clickhouse_verify,
            connect_timeout=self._settings.clickhouse_connect_timeout_sec,
            send_receive_timeout=self._settings.clickhouse_query_timeout_sec,
        )
        return self._client


def build_clickhouse_client(settings):
    return ClickHouseClient(settings)
