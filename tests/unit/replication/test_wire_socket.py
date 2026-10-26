"""The replication connection's TCP socket."""

from __future__ import annotations

import asyncio
import socket

from deltaflow.aio import run
from deltaflow.replication.conninfo import resolve
from deltaflow.replication.wire import RECEIVE_BUFFER, _tcp_connect


def test_the_receive_buffer_is_fixed_not_autotuned() -> None:
    """A consumer that falls behind stops reading, and the walsender waits on
    TCP. With an autotuned receive buffer the kernel can run short of memory
    for data it already invited and drop some; from then on it ignores the
    server's acknowledgements until the consumer reads again, and heartbeats
    stall in retransmission backoff until wal_sender_timeout ends the
    connection (docs/operations.md). A fixed buffer closes the window
    without dropping anything."""

    async def go() -> int:
        server = await asyncio.start_server(lambda _r, w: w.close(), "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        try:
            sock = await _tcp_connect(resolve(f"host=127.0.0.1 port={port} user=x dbname=x"))
            try:
                return sock.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
            finally:
                sock.close()
        finally:
            server.close()
            await server.wait_closed()

    # What the kernel makes of the request: Linux doubles it, capped by
    # net.core.rmem_max; other systems keep it.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as scratch:
        default = scratch.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        scratch.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, RECEIVE_BUFFER)
        expected = scratch.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
    assert expected != default, "this host's kernel ignores SO_RCVBUF"
    assert run(go()) == expected
