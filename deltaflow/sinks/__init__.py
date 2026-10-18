"""Result sinks: in memory (tests, demos) and Postgres (exactly-once, M5)."""

from deltaflow.sinks.base import Sink
from deltaflow.sinks.memory import MemorySink, NegativeMultiplicityError
from deltaflow.sinks.postgres import PostgresSink, SinkError

__all__ = ["MemorySink", "NegativeMultiplicityError", "PostgresSink", "Sink", "SinkError"]
