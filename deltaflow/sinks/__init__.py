"""Result sinks. M2: in memory. The Postgres sink with exactly-once
watermarks is M5."""

from deltaflow.sinks.base import Sink
from deltaflow.sinks.memory import MemorySink, NegativeMultiplicityError

__all__ = ["MemorySink", "NegativeMultiplicityError", "Sink"]
