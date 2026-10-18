"""Delta algebra: rows, signed multisets, deltas, consolidation, frontiers.

The timestamp of a delta is the commit LSN of the source transaction that
produced it. Commit LSNs are totally ordered and every replication consumer
sees transactions in that order, so there is no separate Timestamp type: a
timestamp is an `Lsn`.
"""

from deltaflow.core.delta import Delta, accumulate, consolidate
from deltaflow.core.frontier import (
    Frontier,
    FrontierRegressionError,
    FrontierViolationError,
    MutableFrontier,
)
from deltaflow.core.multiset import Multiset
from deltaflow.core.row import Row

__all__ = [
    "Delta",
    "Frontier",
    "FrontierRegressionError",
    "FrontierViolationError",
    "Multiset",
    "MutableFrontier",
    "Row",
    "accumulate",
    "consolidate",
]
