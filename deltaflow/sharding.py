"""Sharding: which shard a row belongs to, and which worker owns a shard.

* **Rows to shards.** `shard_of(value, n)` hashes the canonical encoding of
  the shard key (state/codec.py), so equal values (`1`, `1.0`,
  `Decimal("1.00")`) always land on the same shard. The number of shards
  is fixed when the views are registered. Shards, not rows, are what move
  between workers.
* **Shards to workers.** `owner(shard, workers)` uses rendezvous (highest
  random weight) hashing. Each worker gets a pseudo-random score per shard,
  and the highest score wins. When a worker leaves, only the shards it
  owned move, and they spread over the remaining workers. When one joins,
  it takes about 1/(n+1) of the shards, from all the others. That is the
  consistent-hashing property, without a ring.
"""

from __future__ import annotations

import hashlib
from collections.abc import Iterable
from typing import Any

from deltaflow.state.codec import encode_key


def _h(data: bytes) -> int:
    return int.from_bytes(hashlib.blake2b(data, digest_size=8).digest(), "big")


def shard_of(value: Any, shards: int) -> int:
    return _h(encode_key((value,))) % shards


def owner(shard: int, workers: Iterable[str]) -> str | None:
    """The worker that should own `shard`, or None if there are no workers."""
    best: tuple[int, str] | None = None
    for worker in workers:
        score = (_h(f"{worker}\x00{shard}".encode()), worker)
        if best is None or score > best:
            best = score
    return None if best is None else best[1]
