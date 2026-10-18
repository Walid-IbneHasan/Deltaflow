"""asyncio entry point shared by every CLI and test."""

from __future__ import annotations

import asyncio
import sys
from collections.abc import Coroutine
from typing import Any


def run[T](main: Coroutine[Any, Any, T]) -> T:
    """Run `main` to completion on a fresh event loop.

    psycopg's async mode cannot use the Proactor loop that is the default on
    Windows, so the selector loop is chosen explicitly there.
    """
    if sys.platform == "win32":
        return asyncio.run(main, loop_factory=asyncio.SelectorEventLoop)
    else:  # explicit else: mypy treats each platform branch as reachable
        return asyncio.run(main)
