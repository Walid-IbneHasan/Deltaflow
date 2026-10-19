"""The deltaflow command's pieces that need no database: finding the
registry a user names, and the arguments each subcommand takes."""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

from deltaflow.cli import CliError, load_views, parser

MODULE = """
from deltaflow.planner import Count, Deltaflow
from deltaflow.sinks import PostgresSink

def make() -> Deltaflow:
    df = Deltaflow()
    df.view("n").source("public.orders").agg(n=Count()).sink(PostgresSink("a.n"))
    return df

registry = make()
not_views = 42
"""


@pytest.fixture
def app(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    name = f"cli_app_{tmp_path.name.replace('-', '_')}"
    (tmp_path / f"{name}.py").write_text(MODULE, encoding="utf-8")
    monkeypatch.syspath_prepend(str(tmp_path))
    yield name
    sys.modules.pop(name, None)


def test_a_registry_or_a_function_returning_one(app: str) -> None:
    assert len(load_views(f"{app}:registry")().views) == 1
    factory = load_views(f"{app}:make")
    assert factory() is not factory()  # fresh each time


def test_a_cluster_needs_a_function(app: str) -> None:
    """Every shard runs its own copy of the views, with its own sinks."""
    with pytest.raises(CliError, match="function that returns"):
        load_views(f"{app}:registry", factory=True)
    load_views(f"{app}:make", factory=True)


@pytest.mark.parametrize(
    ("spec", "message"),
    [
        ("no_colon", "module:attribute"),
        ("cli_app_missing_xyz:views", "cannot import"),
        ("{app}:absent", "has no attribute"),
        ("{app}:not_views", "not a Deltaflow registry"),
    ],
)
def test_bad_specs_say_what_is_wrong(app: str, spec: str, message: str) -> None:
    with pytest.raises(CliError, match=message):
        load_views(spec.format(app=app))()


def test_the_current_directory_is_importable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """`deltaflow run views:registry` from a project directory, as with uvicorn."""
    (tmp_path / "views_in_cwd.py").write_text(MODULE, encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    try:
        assert len(load_views("views_in_cwd:registry")().views) == 1
    finally:
        sys.modules.pop("views_in_cwd", None)


def test_subcommands_and_their_required_arguments() -> None:
    p = parser()
    args = p.parse_args(["run", "app:views", "--slot", "app"])
    assert (args.command, args.views, args.slot, args.restart) == ("run", "app:views", "app", True)
    assert p.parse_args(["run", "app:views", "--slot", "s", "--no-restart"]).restart is False
    args = p.parse_args(["cluster", "app:make", "--worker-id", "w1", "--slot-prefix", "app"])
    assert (args.worker_id, args.slot_prefix) == ("w1", "app")
    assert p.parse_args(["status", "--json"]).json is True
    assert p.parse_args(["reset", "--slot", "app"]).slot == "app"
    with pytest.raises(SystemExit):
        p.parse_args(["run", "app:views"])  # --slot is required
