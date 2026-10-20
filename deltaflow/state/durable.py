"""Checkpoints: how operator state survives a crash.

Layout under the state root:

    live/                     the database the engine writes to (WAL off)
    checkpoints/<LSN hex>/    a consistent copy taken at a transaction boundary
        ...RocksDB files...
        deltaflow-meta.json   engine metadata (LSN, schemas, counts, plan fingerprint)
        COMPLETE              written last; a checkpoint without it does not exist

A checkpoint is taken only between transactions, after the sink has
committed everything up to the same LSN. On restart `live/` is thrown away
and rebuilt from the newest complete checkpoint:

- SST and blob files are immutable, so they are hard-linked;
- the small mutable files (MANIFEST, CURRENT, OPTIONS) are copied.

Whatever the crashed process did after its last checkpoint is gone,
including a transaction applied halfway. It is replayed from the
replication slot, which is only acknowledged up to the checkpoint. See
docs/exactly-once.md.
"""

from __future__ import annotations

import json
import os
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from deltaflow.faults import crash_point
from deltaflow.replication.lsn import Lsn
from deltaflow.state.store import StateStore

_META = "deltaflow-meta.json"
_COMPLETE = "COMPLETE"
#: a checkpoint replacing one at the same LSN, before and after the swap
_NEXT, _OLD = ".next", ".old"
_IMMUTABLE = (".sst", ".blob")


@dataclass(frozen=True, slots=True)
class CheckpointInfo:
    lsn: Lsn
    path: Path
    meta: dict[str, Any]


class StateDirectory:
    def __init__(self, root: Path, *, keep: int = 2, live: str = "live") -> None:
        """`live` names the live database directory. Workers that take turns
        on one shard share its checkpoints but each use their own live
        directory, so a previous owner that is still alive is never wiped
        from under itself."""
        self.root = root
        self.keep = keep
        self._checkpoints = root / "checkpoints"
        self._live = root / live
        self._checkpoints.mkdir(parents=True, exist_ok=True)

    def checkpoints(self) -> list[CheckpointInfo]:
        """Complete checkpoints, oldest first. Incomplete ones are deleted."""
        self._finish_swaps()
        found = []
        for path in sorted(self._checkpoints.iterdir()):
            if not (path / _COMPLETE).exists():
                shutil.rmtree(path, ignore_errors=True)
                continue
            meta = json.loads((path / _META).read_text(encoding="utf-8"))
            found.append(CheckpointInfo(Lsn(int(path.name, 16)), path, meta))
        return found

    def latest(self) -> CheckpointInfo | None:
        found = self.checkpoints()
        return found[-1] if found else None

    def open_live(self, *, memory_budget: int, restore: CheckpointInfo | None = None) -> StateStore:
        """A fresh live database: empty, or a copy of `restore`."""
        if self._live.exists():
            shutil.rmtree(self._live)
        if restore is None:
            return StateStore(self._live, memory_budget=memory_budget)
        self._live.mkdir(parents=True)
        for src in restore.path.iterdir():
            if src.name in (_META, _COMPLETE):
                continue
            if src.suffix in _IMMUTABLE:
                os.link(src, self._live / src.name)
            else:
                shutil.copy2(src, self._live / src.name)
        return StateStore(self._live, memory_budget=memory_budget, fresh=False)

    def checkpoint(self, store: StateStore, lsn: Lsn, meta: dict[str, Any]) -> CheckpointInfo:
        latest = self.latest()
        if latest is not None and lsn < latest.lsn:
            raise ValueError(f"checkpoint at {lsn} is behind the latest one at {latest.lsn}")
        path = self._checkpoints / f"{int(lsn):016X}"
        # The same LSN again (new metadata, e.g. rebuilt views, at the same
        # position): written beside the old one and swapped in, so that one
        # complete checkpoint at this LSN exists at every moment. Deleting
        # the old one first would leave, after a crash, only an older one:
        # behind what the slot was acknowledged up to.
        target = path.with_name(path.name + _NEXT) if path.exists() else path
        shutil.rmtree(target, ignore_errors=True)
        store.checkpoint_to(target)
        (target / _META).write_text(json.dumps(meta), encoding="utf-8")
        crash_point("checkpoint:before_complete")
        _write_durably(target / _COMPLETE, str(lsn))
        if target != path:
            self._swap(path, target)
        info = CheckpointInfo(lsn, path, meta)
        for old in self.checkpoints()[: -self.keep]:
            shutil.rmtree(old.path, ignore_errors=True)
        return info

    def _swap(self, final: Path, nxt: Path) -> None:
        old = final.with_name(final.name + _OLD)
        shutil.rmtree(old, ignore_errors=True)
        final.rename(old)
        crash_point("checkpoint:swap")
        nxt.rename(final)
        shutil.rmtree(old, ignore_errors=True)

    def _finish_swaps(self) -> None:
        """Complete a swap a crash interrupted: a complete `.next` is the
        newer checkpoint at its LSN, whether or not the old one was moved
        aside yet; an incomplete one never counted."""
        for nxt in sorted(self._checkpoints.glob(f"*{_NEXT}")):
            final = nxt.with_name(nxt.name.removesuffix(_NEXT))
            if not (nxt / _COMPLETE).exists():
                shutil.rmtree(nxt, ignore_errors=True)
                continue
            if final.exists():
                self._swap(final, nxt)
            else:
                nxt.rename(final)
        for old in self._checkpoints.glob(f"*{_OLD}"):
            shutil.rmtree(old, ignore_errors=True)

    def reset(self) -> None:
        shutil.rmtree(self._checkpoints, ignore_errors=True)
        shutil.rmtree(self._live, ignore_errors=True)
        self._checkpoints.mkdir(parents=True, exist_ok=True)


def _write_durably(path: Path, text: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
