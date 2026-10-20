"""A minimal asyncio HTTP server for /metrics and the console's JSON API.

GET only, one response per connection. It serves a handful of read-only
endpoints to Prometheus and the console on a trusted network. It is not a
general web server, and it does no authentication: bind it to localhost, or
put it behind something that authenticates.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Callable
from typing import Any

log = logging.getLogger(__name__)

type Handler = Callable[[], tuple[str, bytes]]

_REASONS = {
    200: "OK",
    404: "Not Found",
    405: "Method Not Allowed",
    500: "Internal Server Error",
    503: "Service Unavailable",
}


class Unavailable(Exception):
    """Raised by a handler: answer 503 with this message (a readiness probe
    that is not ready)."""


def json_handler(produce: Callable[[], Any]) -> Handler:
    return lambda: ("application/json", json.dumps(produce(), default=str).encode())


class MonitorServer:
    def __init__(
        self, routes: dict[str, Handler], host: str = "127.0.0.1", port: int = 9187
    ) -> None:
        self.routes = routes
        self.host = host
        self.port = port
        self._server: asyncio.Server | None = None

    async def start(self) -> None:
        self._server = await asyncio.start_server(self._handle, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]  # when 0 was asked for

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()

    async def _handle(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            async with asyncio.timeout(5):
                request = await reader.readline()
                while await reader.readline() not in (b"\r\n", b"\n", b""):
                    pass  # headers are not needed
            method, _, rest = request.decode("latin-1").partition(" ")
            path = rest.split(" ", 1)[0].split("?", 1)[0]
            if method != "GET":
                status, ctype, body = 405, "text/plain", b"GET only\n"
            elif (handler := self.routes.get(path)) is None:
                status, ctype, body = 404, "text/plain", b"not found\n"
            else:
                try:
                    ctype, body = handler()
                    status = 200
                except Unavailable as exc:
                    status, ctype, body = 503, "text/plain", f"{exc}\n".encode()
                except Exception:
                    log.exception("monitor endpoint %s failed", path)
                    status, ctype, body = 500, "text/plain", b"internal error\n"
            head = (
                f"HTTP/1.1 {status} {_REASONS[status]}\r\n"
                f"Content-Type: {ctype}\r\nContent-Length: {len(body)}\r\n"
                "Access-Control-Allow-Origin: *\r\nCache-Control: no-store\r\n"
                "Connection: close\r\n\r\n"
            )
            writer.write(head.encode("latin-1") + body)
            await writer.drain()
        except (TimeoutError, ConnectionError):
            pass
        finally:
            writer.close()
            with contextlib.suppress(ConnectionError):
                await writer.wait_closed()
