"""Running views: the engine (M2). Scheduling, backpressure and shard
assignment come in M5-M7."""

from deltaflow.runtime.engine import SNAPSHOT_TIME, Engine

__all__ = ["SNAPSHOT_TIME", "Engine"]
