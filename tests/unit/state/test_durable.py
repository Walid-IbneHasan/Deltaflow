from __future__ import annotations

from pathlib import Path

import pytest

from deltaflow.replication.lsn import Lsn
from deltaflow.state.durable import StateDirectory

MB = 1 << 20


def test_checkpoint_and_restore_round_trip(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path)
    assert sd.latest() is None
    store = sd.open_live(memory_budget=8 * MB)
    ks = store.keyspace("k")
    ks.put(b"a", b"1")
    first = sd.checkpoint(store, Lsn(100), {"note": "first"})
    ks.put(b"b", b"2")
    second = sd.checkpoint(store, Lsn(200), {"note": "second"})
    ks.put(b"c", b"3")  # after the last checkpoint: lost on restore
    store.close()

    latest = sd.latest()
    assert latest is not None
    assert (latest.lsn, latest.meta) == (Lsn(200), {"note": "second"})
    assert first.lsn == Lsn(100)
    restored = sd.open_live(memory_budget=8 * MB, restore=latest)
    assert list(restored.keyspace("k").scan()) == [(b"a", b"1"), (b"b", b"2")]
    restored.keyspace("k").put(b"d", b"4")
    restored.close()
    # Writes after a restore never reach the checkpoint they came from.
    again = sd.open_live(memory_budget=8 * MB, restore=second)
    assert list(again.keyspace("k").scan()) == [(b"a", b"1"), (b"b", b"2")]
    again.close()


def test_only_the_newest_checkpoints_are_kept(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path, keep=2)
    store = sd.open_live(memory_budget=8 * MB)
    for lsn in (10, 20, 30, 40):
        store.keyspace("k").put(b"x", str(lsn).encode())
        sd.checkpoint(store, Lsn(lsn), {})
    store.close()
    assert [c.lsn for c in sd.checkpoints()] == [Lsn(30), Lsn(40)]


def test_an_incomplete_checkpoint_is_ignored(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path)
    store = sd.open_live(memory_budget=8 * MB)
    sd.checkpoint(store, Lsn(10), {"ok": True})
    store.close()
    # A crash while writing checkpoint 20: files there, completion marker not.
    (sd.root / "checkpoints" / f"{20:016X}").mkdir()
    latest = sd.latest()
    assert latest is not None
    assert latest.lsn == Lsn(10)
    assert not (sd.root / "checkpoints" / f"{20:016X}").exists()


def test_reset_forgets_everything(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path)
    store = sd.open_live(memory_budget=8 * MB)
    sd.checkpoint(store, Lsn(10), {})
    store.close()
    sd.reset()
    assert sd.latest() is None


def test_checkpoints_must_move_forward(tmp_path: Path) -> None:
    sd = StateDirectory(tmp_path)
    store = sd.open_live(memory_budget=8 * MB)
    sd.checkpoint(store, Lsn(10), {})
    with pytest.raises(ValueError, match="behind"):
        sd.checkpoint(store, Lsn(5), {})
    store.close()


def test_a_checkpoint_at_the_same_lsn_replaces_the_old_one_safely(tmp_path: Path) -> None:
    """A second checkpoint at the same LSN (a resume that rebuilt views
    has new state at the checkpoint's own position) is written beside the
    old one and swapped in: at every moment one complete checkpoint at that
    LSN exists. Deleting the old one first would leave, after a crash, only
    an older checkpoint, behind what the slot was acknowledged up to."""
    sd = StateDirectory(tmp_path)
    store = sd.open_live(memory_budget=8 * MB)
    store.keyspace("k").put(b"x", b"old")
    sd.checkpoint(store, Lsn(50), {"views": "old"})
    store.keyspace("k").put(b"x", b"new")
    sd.checkpoint(store, Lsn(50), {"views": "new"})
    store.close()
    latest = sd.latest()
    assert latest is not None
    assert (latest.lsn, latest.meta) == (Lsn(50), {"views": "new"})
    assert sorted(p.name for p in (sd.root / "checkpoints").iterdir()) == [f"{50:016X}"]


@pytest.mark.parametrize("crash", ["before_swap", "between_renames"])
def test_a_crash_while_swapping_keeps_a_checkpoint(tmp_path: Path, crash: str) -> None:
    sd = StateDirectory(tmp_path)
    store = sd.open_live(memory_budget=8 * MB)
    store.keyspace("k").put(b"x", b"old")
    sd.checkpoint(store, Lsn(50), {"views": "old"})
    store.keyspace("k").put(b"x", b"new")
    final = sd.root / "checkpoints" / f"{50:016X}"
    nxt = final.with_name(final.name + ".next")
    # what the swap leaves behind: the new one complete beside the old one,
    # or the old one already moved aside
    store.checkpoint_to(nxt)
    (nxt / "deltaflow-meta.json").write_text('{"views": "new"}', encoding="utf-8")
    (nxt / "COMPLETE").write_text("50", encoding="utf-8")
    if crash == "between_renames":
        final.rename(final.with_name(final.name + ".old"))
    store.close()
    latest = sd.latest()
    assert latest is not None
    assert (latest.lsn, latest.meta) == (Lsn(50), {"views": "new"})
    assert sorted(p.name for p in (sd.root / "checkpoints").iterdir()) == [f"{50:016X}"]
