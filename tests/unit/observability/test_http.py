"""The monitoring server answers 200 from a handler, 503 when the handler
says it is not ready (a readiness probe), 404 for anything else."""

from __future__ import annotations

import asyncio
import urllib.error
import urllib.request

from deltaflow.aio import run
from deltaflow.observability.http import MonitorServer, Unavailable


def fetch(url: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode()


def not_ready() -> tuple[str, bytes]:
    raise Unavailable("loading")


def test_status_codes() -> None:
    async def go() -> list[tuple[int, str]]:
        server = MonitorServer(
            {"/ok": lambda: ("text/plain", b"fine\n"), "/readyz": not_ready}, port=0
        )
        await server.start()
        try:
            base = f"http://127.0.0.1:{server.port}"
            return [
                await asyncio.to_thread(fetch, f"{base}/ok"),
                await asyncio.to_thread(fetch, f"{base}/readyz"),
                await asyncio.to_thread(fetch, f"{base}/nope"),
            ]
        finally:
            await server.close()

    assert run(go()) == [(200, "fine\n"), (503, "loading\n"), (404, "not found\n")]
