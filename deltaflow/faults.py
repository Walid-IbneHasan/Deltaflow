"""Crash points for the chaos suite.

`crash_point(name)` does nothing unless the environment asks for a crash
there:

    DELTAFLOW_CRASH_AT=flush:after_commit:3

That crashes the process the 3rd time it passes `flush:after_commit`. The
crash is `os._exit`: no `finally` blocks, no atexit, no flushing of Python
buffers. To everything outside the process, Postgres included, it looks the
same as `kill -9`. The random-timing `kill -9` tests cover the rest. These
points exist because some windows are only a few microseconds wide, and
random timing would almost never land in them.
"""

from __future__ import annotations

import os
import sys
from collections import Counter

EXIT_CODE = 86
_hits: Counter[str] = Counter()


def _target() -> tuple[str, int] | None:
    raw = os.environ.get("DELTAFLOW_CRASH_AT")
    if not raw:
        return None
    name, _, n = raw.rpartition(":")
    return name, int(n)


_TARGET = _target()


def crash_point(name: str) -> None:
    if _TARGET is None or _TARGET[0] != name:
        return
    _hits[name] += 1
    if _hits[name] == _TARGET[1]:
        print(
            f"crash point {name} reached (hit {_hits[name]}): exiting", file=sys.stderr, flush=True
        )
        os._exit(EXIT_CODE)
