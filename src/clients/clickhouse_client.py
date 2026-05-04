from __future__ import annotations

import asyncio
import threading
import time
from typing import Any, Optional

from src.services.logger_service import LoggerService


class ClickHouseClient:
    def __init__(self, settings):
        self._settings = settings
        self._clients_by_thread: dict[int, Any] = {}
        self._clients_lock = threading.Lock()
        self._log = LoggerService().get("clickhouse_client")

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
            self._execute_command,
            query,
            parameters or {},
        )

    async def ping(self) -> bool:
        return bool(await self.fetch_scalar("SELECT 1"))

    async def close(self) -> None:
        clients = self._drain_clients()
        if not clients:
            return
        await asyncio.to_thread(self._close_clients_sync, clients)

    async def aclose(self) -> None:
        await self.close()

    def _execute_with_columns(
        self,
        query: str,
        parameters: dict[str, Any],
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        query_result = self._run_with_retry(
            operation="query",
            query=query,
            parameters=parameters,
        )
        rows = list(
            getattr(
                query_result,
                "result_rows",
                getattr(query_result, "result_set", []),
            )
        )
        column_names = list(getattr(query_result, "column_names", []))
        return rows, column_names

    def _execute_command(
        self,
        query: str,
        parameters: dict[str, Any],
    ) -> Any:
        return self._run_with_retry(
            operation="command",
            query=query,
            parameters=parameters,
        )

    def _run_with_retry(
        self,
        operation: str,
        query: str,
        parameters: dict[str, Any],
    ) -> Any:
        max_attempts = 1 + max(0, self._settings.clickhouse_max_retries)
        last_error: Exception | None = None

        for attempt in range(1, max_attempts + 1):
            try:
                client = self._get_or_create_client()
                if operation == "command":
                    return client.command(query, parameters=parameters)
                return client.query(query, parameters=parameters)
            except Exception as exc:
                last_error = exc
                retryable = self._is_retryable(exc)
                self._discard_current_thread_client()
                if not retryable or attempt >= max_attempts:
                    self._log.error(
                        "ClickHouse operation failed",
                        extra={
                            "operation": operation,
                            "attempt": attempt,
                            "max_attempts": max_attempts,
                            "retryable": retryable,
                            "error_type": type(exc).__name__,
                            "query_preview": self._query_preview(query),
                        },
                        exc_info=True,
                    )
                    raise

                sleep_sec = self._settings.clickhouse_retry_backoff_sec * attempt
                self._log.warning(
                    "ClickHouse operation retrying",
                    extra={
                        "operation": operation,
                        "attempt": attempt,
                        "max_attempts": max_attempts,
                        "sleep_sec": sleep_sec,
                        "error_type": type(exc).__name__,
                        "query_preview": self._query_preview(query),
                    },
                )
                time.sleep(sleep_sec)

        raise last_error or RuntimeError("ClickHouse operation failed without an exception")

    def _get_or_create_client(self) -> Any:
        thread_id = threading.get_ident()
        with self._clients_lock:
            client = self._clients_by_thread.get(thread_id)
        if client is not None:
            return client

        try:
            from clickhouse_connect import get_client
        except ModuleNotFoundError as exc:  # pragma: no cover - depends on optional install state
            raise RuntimeError(
                "clickhouse-connect is required for ClickHouse access."
            ) from exc

        client = get_client(
            host=self._settings.clickhouse_host,
            port=self._settings.clickhouse_port,
            database=self._settings.clickhouse_database,
            username=self._settings.clickhouse_username,
            password=self._settings.clickhouse_password or "",
            secure=self._settings.clickhouse_secure,
            verify=self._settings.clickhouse_verify,
            connect_timeout=self._settings.clickhouse_connect_timeout_sec,
            send_receive_timeout=self._settings.clickhouse_query_timeout_sec,
        )
        with self._clients_lock:
            self._clients_by_thread[thread_id] = client
        return client

    def _discard_current_thread_client(self) -> None:
        thread_id = threading.get_ident()
        with self._clients_lock:
            client = self._clients_by_thread.pop(thread_id, None)
        if client is None:
            return
        self._close_client_sync(client)

    def _drain_clients(self) -> list[Any]:
        with self._clients_lock:
            clients = list(self._clients_by_thread.values())
            self._clients_by_thread.clear()
        return clients

    def _close_clients_sync(self, clients: list[Any]) -> None:
        for client in clients:
            self._close_client_sync(client)

    @staticmethod
    def _close_client_sync(client: Any) -> None:
        if hasattr(client, "close"):
            client.close()

    @staticmethod
    def _query_preview(query: str) -> str:
        compact = " ".join(query.split())
        return compact[:160]

    @staticmethod
    def _is_retryable(exc: Exception) -> bool:
        message = str(exc).lower()
        retryable_markers = (
            "attempt to execute concurrent queries within the same session",
            "connection refused",
            "connect timeout",
            "connection timed out",
            "timed out",
            "max retries exceeded",
            "temporarily unavailable",
            "connection reset",
            "server disconnected",
            "network is unreachable",
            "broken pipe",
        )
        return any(marker in message for marker in retryable_markers)


def build_clickhouse_client(settings):
    return ClickHouseClient(settings)
