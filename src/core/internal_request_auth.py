from __future__ import annotations

import hashlib
import hmac
import secrets
import time
from collections.abc import Iterable
from typing import TypeAlias

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

ProtectedRoute: TypeAlias = tuple[str, str]


def _build_signature_message(
    timestamp: str,
    method: str,
    request_target: str,
    body: bytes,
) -> bytes:
    return b"\n".join(
        [
            timestamp.encode("utf-8"),
            method.upper().encode("utf-8"),
            request_target.encode("utf-8"),
            body,
        ]
    )


def build_internal_request_signature(
    secret: str,
    timestamp: str,
    method: str,
    request_target: str,
    body: bytes,
) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        _build_signature_message(timestamp, method, request_target, body),
        hashlib.sha256,
    ).hexdigest()


def _normalize_protected_routes(
    protected_routes: Iterable[ProtectedRoute],
) -> frozenset[ProtectedRoute]:
    return frozenset((method.upper(), path) for method, path in protected_routes)


def _build_request_target(scope: Scope) -> str:
    path = scope["path"]
    query_string = scope.get("query_string", b"")
    if not query_string:
        return path
    return f"{path}?{query_string.decode('latin-1')}"


class InternalRequestAuthMiddleware:
    def __init__(
        self,
        app: ASGIApp,
        *,
        secret: str,
        max_skew_sec: int,
        protected_routes: Iterable[ProtectedRoute],
    ):
        self.app = app
        self._secret = secret.encode("utf-8")
        self._max_skew_sec = max(0, int(max_skew_sec))
        self._protected_routes = _normalize_protected_routes(protected_routes)

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self._should_authenticate(scope):
            await self.app(scope, receive, send)
            return

        if not self._secret:
            await self._respond(
                scope,
                receive,
                send,
                status_code=503,
                detail="internal request auth is not configured",
            )
            return

        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        timestamp = headers.get("x-internal-timestamp")
        signature = headers.get("x-internal-signature")

        if not timestamp or not signature:
            await self._respond(
                scope,
                receive,
                send,
                status_code=401,
                detail="missing internal auth headers",
            )
            return

        try:
            request_time = int(timestamp)
        except ValueError:
            await self._respond(
                scope,
                receive,
                send,
                status_code=401,
                detail="invalid internal timestamp",
            )
            return

        if self._max_skew_sec and abs(int(time.time()) - request_time) > self._max_skew_sec:
            await self._respond(
                scope,
                receive,
                send,
                status_code=401,
                detail="stale internal request timestamp",
            )
            return

        body = await self._read_body(receive)
        expected_signature = hmac.new(
            self._secret,
            _build_signature_message(
                timestamp=timestamp,
                method=scope["method"],
                request_target=_build_request_target(scope),
                body=body,
            ),
            hashlib.sha256,
        ).hexdigest()

        if not secrets.compare_digest(signature, expected_signature):
            await self._respond(
                scope,
                receive,
                send,
                status_code=401,
                detail="invalid internal signature",
            )
            return

        await self.app(scope, self._replay_body(body), send)

    def _should_authenticate(self, scope: Scope) -> bool:
        return (
            scope["type"] == "http"
            and (scope["method"].upper(), scope["path"]) in self._protected_routes
        )

    @staticmethod
    async def _read_body(receive: Receive) -> bytes:
        chunks: list[bytes] = []
        more_body = True
        while more_body:
            message = await receive()
            if message["type"] != "http.request":
                continue
            chunks.append(message.get("body", b""))
            more_body = message.get("more_body", False)
        return b"".join(chunks)

    @staticmethod
    def _replay_body(body: bytes) -> Receive:
        sent = False

        async def receive() -> Message:
            nonlocal sent
            if sent:
                return {"type": "http.request", "body": b"", "more_body": False}
            sent = True
            return {"type": "http.request", "body": body, "more_body": False}

        return receive

    @staticmethod
    async def _respond(
        scope: Scope,
        receive: Receive,
        send: Send,
        *,
        status_code: int,
        detail: str,
    ) -> None:
        response = JSONResponse(status_code=status_code, content={"detail": detail})
        await response(scope, receive, send)


def add_internal_request_auth_middleware(
    app: FastAPI,
    settings,
    *,
    protected_routes: Iterable[ProtectedRoute],
) -> None:
    routes = _normalize_protected_routes(protected_routes)
    if not routes:
        return
    app.add_middleware(
        InternalRequestAuthMiddleware,
        secret=settings.internal_request_hmac_secret,
        max_skew_sec=settings.internal_request_max_skew_sec,
        protected_routes=routes,
    )
