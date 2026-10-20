"""Whether a checkpoint can be resumed on the server the runner reached:
the same cluster, and a timeline whose history still contains the
checkpoint's position (after a failover, the new primary's timeline
branched from the old one somewhere; WAL past that point is gone)."""

from __future__ import annotations

from deltaflow.replication.lsn import Lsn
from deltaflow.replication.slot import SystemInfo, _history, diverged

CK = Lsn.parse("0/5000000")


def now(
    timeline: int, history: tuple[tuple[int, Lsn], ...] = (), system: str = "7300"
) -> SystemInfo:
    return SystemInfo(system, timeline, Lsn.parse("0/9000000"), history)


def test_same_cluster_and_timeline_resumes() -> None:
    assert diverged(CK, {"id": "7300", "timeline": 1}, now(1)) is None


def test_a_checkpoint_older_than_this_check_resumes() -> None:
    assert diverged(CK, {}, now(3)) is None


def test_another_cluster_is_refused() -> None:
    reason = diverged(CK, {"id": "7300", "timeline": 1}, now(1, system="9999"))
    assert reason is not None
    assert "another cluster" in reason


def test_a_failover_after_the_checkpoint_resumes() -> None:
    """Timeline 1 ended at 0/6000000, after the checkpoint: everything the
    views applied is on the new primary too."""
    history = ((1, Lsn.parse("0/6000000")),)
    assert diverged(CK, {"id": "7300", "timeline": 1}, now(2, history)) is None


def test_a_failover_before_the_checkpoint_is_refused() -> None:
    """Timeline 1 ended at 0/4000000: the views applied WAL in (0/4000000,
    0/5000000] that the new primary never received."""
    history = ((1, Lsn.parse("0/4000000")),)
    reason = diverged(CK, {"id": "7300", "timeline": 1}, now(2, history))
    assert reason is not None
    assert "past where timeline 1 ended (0/4000000)" in reason


def test_several_failovers_follow_the_history() -> None:
    history = ((1, Lsn.parse("0/1000000")), (2, Lsn.parse("0/7000000")))
    assert diverged(CK, {"id": "7300", "timeline": 2}, now(3, history)) is None
    reason = diverged(CK, {"id": "7300", "timeline": 1}, now(3, history))
    assert reason is not None
    assert "timeline 1 ended" in reason


def test_a_timeline_outside_the_history_is_refused() -> None:
    reason = diverged(CK, {"id": "7300", "timeline": 5}, now(2, ((1, Lsn.parse("0/6000000")),)))
    assert reason is not None
    assert "not in the history" in reason


def test_history_files_parse_as_text_and_as_bytea_hex() -> None:
    text = "1\t0/3000148\tno recovery target specified\n\n2\t0/5000000\tno recovery target\n"
    expected = ((1, Lsn.parse("0/3000148")), (2, Lsn.parse("0/5000000")))
    assert _history(text) == expected
    assert _history("\\x" + text.encode().hex()) == expected
